defmodule ExAws.S3.Utils do
  ## Formatting and helpers
  @moduledoc false

  def ensure_slash("/" <> _ = path), do: path
  def ensure_slash(path), do: "/" <> path

  @headers [
    :cache_control,
    :content_disposition,
    :content_encoding,
    :content_length,
    :content_type,
    :expect,
    :expires,
    :content_md5
  ]

  @amz_headers [:website_redirect_location, :tagging, :tagging_directive]

  @etag_headers [:if_match, :if_none_match]

  def put_object_headers(opts) do
    opts = opts |> Map.new()

    regular_headers =
      opts
      |> format_and_take(@headers)

    amz_headers =
      opts
      |> format_and_take(@amz_headers)
      |> namespace("x-amz")

    etag_headers =
      opts
      |> format_and_take(@etag_headers)

    storage_class_headers = format_storage_class_headers(opts)

    acl_headers = format_acl_headers(opts)

    encryption_headers =
      opts
      |> Map.get(:encryption, %{})
      |> build_encryption_headers

    meta =
      opts
      |> Map.get(:meta, [])
      |> build_meta_headers

    regular_headers
    |> Map.merge(amz_headers)
    |> Map.merge(etag_headers)
    |> Map.merge(storage_class_headers)
    |> Map.merge(acl_headers)
    |> Map.merge(encryption_headers)
    |> Map.merge(meta)
  end

  def build_meta_headers(meta) do
    Enum.into(meta, %{}, fn {k, v} ->
      {"x-amz-meta-#{k}", v}
    end)
  end

  @doc """
  Validates that every header name and value is safe to put on the wire.

  Raises `ArgumentError` if a header name is not an RFC 9110 token, or if a
  header value contains control characters such as CR or LF. Without this check,
  headers built from untrusted input (for example `x-amz-meta-*`) could be used
  for HTTP header injection / request splitting.
  """
  def validate_headers!(headers) do
    Enum.each(headers, fn {name, value} ->
      name = header_to_binary(name)

      unless valid_header_name?(name) do
        raise ArgumentError,
              "invalid HTTP header name #{inspect(name)}: header names may only contain " <>
                "RFC 9110 token characters"
      end

      unless valid_header_value?(header_to_binary(value)) do
        raise ArgumentError,
              "invalid value for HTTP header #{inspect(name)}: #{inspect(value)} contains " <>
                "control characters (such as CR or LF), which are not allowed"
      end
    end)

    headers
  end

  @tchar_punctuation String.to_charlist("!#$%&'*+-.^_`|~")

  defp valid_header_name?(<<>>), do: false
  defp valid_header_name?(name), do: all_tchars?(name)

  defp all_tchars?(<<c, rest::binary>>)
       when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c in @tchar_punctuation,
       do: all_tchars?(rest)

  defp all_tchars?(<<>>), do: true
  defp all_tchars?(_), do: false

  # Field values may contain visible ASCII, space, horizontal tab and obs-text
  # (bytes >= 0x80). Everything else (CR, LF, NUL and other control characters)
  # is rejected.
  defp valid_header_value?(<<c, rest::binary>>) when c == ?\t or (c >= 0x20 and c != 0x7F),
    do: valid_header_value?(rest)

  defp valid_header_value?(<<>>), do: true
  defp valid_header_value?(_), do: false

  defp header_to_binary(value) when is_binary(value), do: value
  defp header_to_binary(value), do: to_string(value)

  @doc """
  format_and_take %{param_one: "v1", param_two: "v2"}, [:param_one]
  #=> %{"param-one" => "v1"}
  """
  def format_and_take(%{} = opts, param_list) do
    param_list
    |> Enum.map(&{&1, normalize_param(&1)})
    |> Enum.reduce(%{}, fn {elixir_opt, aws_opt}, params ->
      case Map.fetch(opts, elixir_opt) do
        :error -> params
        {:ok, nil} -> params
        {:ok, value} -> Map.put(params, aws_opt, value)
      end
    end)
  end

  def format_and_take(opts, param_list) do
    opts
    |> Map.new()
    |> format_and_take(param_list)
  end

  def format_storage_class_headers(%{storage_class: storage_class}) do
    formatted_storage_class =
      storage_class
      |> to_string()
      |> String.upcase()

    %{"x-amz-storage-class" => formatted_storage_class}
  end

  def format_storage_class_headers(_), do: %{}

  @acl_headers [
    :acl,
    :grant_read,
    :grant_write,
    :grant_read_acp,
    :grant_write_acp,
    :grant_full_control
  ]
  def format_acl_headers(%{acl: canned_acl}) do
    %{"x-amz-acl" => normalize_param(canned_acl)}
  end

  def format_acl_headers(grants), do: format_grant_headers(grants)

  def format_grant_headers(grants) do
    grants
    |> format_and_take(@acl_headers)
    |> namespace("x-amz")
    |> Enum.into(%{}, &format_grant_header/1)
  end

  def format_grant_header({permission, grantees}) do
    grants =
      grantees
      |> Enum.map(fn
        {:email, email} -> format_grantee("emailAddress", email)
        {type, identifier} -> format_grantee(type, identifier)
      end)
      |> Enum.join(", ")

    {permission, grants}
  end

  # A grant header is a comma-separated list of `type="identifier"` tokens and the
  # grammar has no escaping mechanism. If an identifier were interpolated verbatim, a
  # value such as `x", uri="http://acs.amazonaws.com/groups/global/AllUsers` would close
  # the quoted token and smuggle in a second grantee (GHSA-6fg2-v8g4-vfg2). Every legitimate
  # grantee identifier (canonical user id, email address or group URI) consists solely of
  # printable ASCII, so anything outside that range, or the structural `"` and `,`
  # characters, is rejected rather than escaped.
  defp format_grantee(type, identifier) do
    type = to_string(type)
    identifier = to_string(identifier)

    if safe_grantee_component?(type) and safe_grantee_component?(identifier) do
      ~s(#{type}="#{identifier}")
    else
      raise ArgumentError,
            "invalid S3 grantee #{type}=#{inspect(identifier)}: grantee types and identifiers " <>
              "may only contain printable ASCII characters other than `\"` and `,`"
    end
  end

  # 0x20 (space) through 0x7E (tilde) is exactly the printable ASCII range: everything
  # below 0x20 is a control character (including CR/LF, which would allow HTTP header
  # injection), 0x7F is DEL, and anything above is non-ASCII. Within that range only the
  # double quote and comma carry structural meaning in the grant header grammar.
  defp safe_grantee_component?(<<c, rest::binary>>)
       when c in 0x20..0x7E and c not in [?", ?,],
       do: safe_grantee_component?(rest)

  defp safe_grantee_component?(<<>>), do: true
  defp safe_grantee_component?(_), do: false

  def build_cors_rule(rule) do
    mapping = [
      allowed_origins: "AllowedOrigin",
      allowed_methods: "AllowedMethod",
      allowed_headers: "AllowedHeader",
      exposed_headers: "ExposeHeader"
    ]

    properties =
      mapping
      |> Enum.flat_map(fn {key, property} ->
        rule
        |> Map.get(key, [])
        |> Enum.map(&"<#{property}>#{escape_xml_string(&1)}</#{property}>")
      end)

    properties =
      case Map.fetch(rule, :max_age_seconds) do
        :error ->
          properties

        {:ok, nil} ->
          properties

        {:ok, value} ->
          ["<MaxAgeSeconds>#{escape_xml_string(value)}</MaxAgeSeconds>" | properties]
      end

    ["<CORSRule>", properties, "</CORSRule>"]
    |> IO.iodata_to_binary()
  end

  def build_lifecycle_rule(rule) do
    # ID
    properties = ["<ID>", escape_xml_string(rule.id), "</ID>"]

    # Status
    status_text = if rule.enabled, do: "Enabled", else: "Disabled"
    properties = [["<Status>", status_text, "</Status>"] | properties]

    # Filter
    filter_prefix =
      case Map.get(rule.filter, :prefix, nil) do
        prefix when is_binary(prefix) and prefix != "" ->
          [["<Prefix>", escape_xml_string(prefix), "</Prefix>"]]

        _ ->
          []
      end

    filter_tags =
      Enum.map(Map.get(rule.filter, :tags, []), fn {key, value} ->
        [
          "<Tag>",
          ["<Key>", escape_xml_string(key), "</Key>"],
          ["<Value>", escape_xml_string(value), "</Value>"],
          "</Tag>"
        ]
      end)

    filters =
      case filter_prefix ++ filter_tags do
        [] -> []
        [_] = filters -> filters
        many -> ["<And>", many, "</And>"]
      end

    properties = [["<Filter>", filters, "</Filter>"] | properties]

    # Actions
    mapping = [
      transition: %{
        tag: "Transition",
        action_tags: fn %{storage: storage} ->
          [["<StorageClass>", escape_xml_string(storage), "</StorageClass>"]]
        end
      },
      expiration: %{
        tag: "Expiration",
        action_tags: fn
          %{expired_object_delete_marker: marker} ->
            marker = if marker, do: "true", else: "false"
            [["<ExpiredObjectDeleteMarker>", marker, "</ExpiredObjectDeleteMarker>"]]

          _ ->
            []
        end
      },
      noncurrent_version_transition: %{
        tag: "NoncurrentVersionTransition",
        action_tags: fn %{storage: storage} ->
          [["<StorageClass>", escape_xml_string(storage), "</StorageClass>"]]
        end
      },
      noncurrent_version_expiration: %{
        tag: "NoncurrentVersionExpiration",
        action_tags: fn
          %{newer_noncurrent_versions: versions} ->
            [
              [
                "<NewerNoncurrentVersions>",
                escape_xml_string(versions),
                "</NewerNoncurrentVersions>"
              ]
            ]

          _ ->
            []
        end
      },
      abort_incomplete_multipart_upload: %{
        tag: "AbortIncompleteMultipartUpload",
        action_tags: fn _data -> [] end
      }
    ]

    properties =
      Enum.reduce(mapping, properties, fn {key, %{tag: tag, action_tags: fun}}, properties ->
        case rule.actions[key] do
          %{trigger: trigger} = config ->
            trigger = livecycle_trigger(key, trigger)
            action_tags = fun.(config)
            [["<#{tag}>", [trigger | action_tags], "</#{tag}>"] | properties]

          _ ->
            properties
        end
      end)

    ["<Rule>", properties, "</Rule>"]
    |> IO.iodata_to_binary()
  end

  defp livecycle_trigger(action, {:date, %Date{} = date})
       when action in [:transition, :expiration] do
    ["<Date>", Date.to_iso8601(date), "</Date>"]
  end

  defp livecycle_trigger(action, {:days, days})
       when action in [:transition, :expiration] and is_integer(days) and days >= 0 do
    ["<Days>", Integer.to_string(days), "</Days>"]
  end

  defp livecycle_trigger(action, {:days, days})
       when action in [:abort_incomplete_multipart_upload] and is_integer(days) and days >= 0 do
    ["<DaysAfterInitiation>", Integer.to_string(days), "</DaysAfterInitiation>"]
  end

  defp livecycle_trigger(action, {:days, days})
       when action in [:noncurrent_version_transition, :noncurrent_version_expiration] and
              is_integer(days) and days >= 0 do
    ["<NoncurrentDays>", Integer.to_string(days), "</NoncurrentDays>"]
  end

  @doc """
  Escapes a value for use as character data inside an XML element.

  Every caller-supplied value that is placed between XML tags in a request body
  must pass through this function. Without it, a value containing `<`, `&` or
  similar is read by S3 as markup rather than text, which both breaks the request
  and allows injection of additional elements.

  See https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html#object-key-xml-related-constraints
  """
  def escape_xml_string(value) do
    value
    |> to_string()
    |> String.replace(["'", "\"", "&", "<", ">", "\r", "\n"], fn
      "'" -> "&apos;"
      "\"" -> "&quot;"
      "&" -> "&amp;"
      "<" -> "&lt;"
      ">" -> "&gt;"
      "\r" -> "&#13;"
      "\n" -> "&#10;"
    end)
  end

  def normalize_param(:version_id), do: "versionId"

  def normalize_param(param) when is_atom(param) do
    param
    |> Atom.to_string()
    |> String.replace("_", "-")
  end

  def normalize_param(other), do: other

  def namespace(list, value) do
    list
    |> Enum.map(fn {k, v} -> {"#{value}-#{k}", v} end)
    |> Map.new()
  end

  def build_encryption_headers("AES256") do
    %{"x-amz-server-side-encryption" => "AES256"}
  end

  def build_encryption_headers(aws_kms_key_id: key_id) do
    %{
      "x-amz-server-side-encryption" => "aws:kms",
      "x-amz-server-side-encryption-aws-kms-key-id" => key_id
    }
  end

  def build_encryption_headers(headers) do
    headers
    |> Enum.map(fn {k, v} -> {normalize_param(k), v} end)
    |> namespace("x-amz-server-side-encryption")
  end

  # If we're using a standard port such as 80 or 443, then it needs to be excluded from the signed
  # headers. Including standard ports will cause AWS's signature validation to fail with a
  # SignatureDoesNotMatch error.
  @excluded_ports [80, "80", 443, "443"]
  def sanitized_port_component(%{port: nil}), do: ""
  def sanitized_port_component(%{port: port}) when port in @excluded_ports, do: ""
  def sanitized_port_component(%{port: port}), do: ":#{port}"
  def sanitized_port_component(_), do: ""

  def build_bucket_url(config, bucket_name) do
    "#{config[:scheme]}#{bucket_name}.#{config[:host]}"
  end

  defp acl_condition({:starts_with, starts_with}), do: [["starts-with", "$key", starts_with]]
  defp acl_condition(nil), do: []
  defp acl_condition(acl) when is_binary(acl), do: [%{"acl" => acl}]

  defp key_condition({:starts_with, starts_with}), do: [["starts-with", "$key", starts_with]]
  defp key_condition(nil), do: []
  defp key_condition(key) when is_binary(key), do: [%{"key" => key}]

  def content_length_condition([min, max]), do: [["content-length-range", min, max]]
  def content_length_condition(nil), do: []

  def build_amz_post_policy(
        datetime,
        expiration_date,
        bucket,
        credential,
        opts,
        exact_key \\ nil
      ) do
    key = Keyword.get(opts, :key, exact_key)
    content_length_range = Keyword.get(opts, :content_length_range, nil)
    acl = Keyword.get(opts, :acl, nil)
    custom_conditions = Keyword.get(opts, :custom_conditions, [])

    %{
      "expiration" => DateTime.to_iso8601(expiration_date),
      "conditions" =>
        [
          %{"X-Amz-Algorithm" => "AWS4-HMAC-SHA256"},
          %{"X-Amz-Credential" => credential},
          %{"X-Amz-Date" => ExAws.Auth.Utils.amz_date(datetime)},
          %{"bucket" => bucket}
        ] ++
          key_condition(key) ++
          content_length_condition(content_length_range) ++
          acl_condition(acl) ++ custom_conditions
    }
  end

  def datetime_to_erlang_time(datetime) do
    {{datetime.year, datetime.month, datetime.day},
     {datetime.hour, datetime.minute, datetime.second}}
  end
end
