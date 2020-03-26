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
  @amz_headers [:storage_class, :website_redirect_location, :tagging]
  def put_object_headers(opts) do
    opts = opts |> Map.new()

    regular_headers =
      opts
      |> format_and_take(@headers)

    amz_headers =
      opts
      |> format_and_take(@amz_headers)
      |> namespace("x-amz")

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
        {:email, email} -> "emailAddress=\"#{email}\""
        {key, value} -> "#{key}=\"#{value}\""
      end)
      |> Enum.join(", ")

    {permission, grants}
  end

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
        |> Enum.map(&"<#{property}>#{&1}</#{property}>")
      end)

    properties =
      case Map.fetch(rule, :max_age_seconds) do
        :error -> properties
        {:ok, nil} -> properties
        {:ok, value} -> ["<MaxAgeSeconds>#{value}</MaxAgeSeconds>" | properties]
      end

    ["<CORSRule>", properties, "</CORSRule>"]
    |> IO.iodata_to_binary()
  end

  def build_lifecycle_rule(rule) do
    # ID
    properties = ["<ID>", rule.id, "</ID>"]

    # Status
    status_text = if rule.enabled, do: "Enabled", else: "Disabled"
    properties = [["<Status>", status_text, "</Status>"] | properties]

    # Filter
    filter_prefix =
      case Map.get(rule.filter, :prefix, nil) do
        prefix when is_binary(prefix) and prefix != "" ->
          [["<Prefix>", prefix, "</Prefix>"]]

        _ ->
          []
      end

    filter_tags =
      Enum.map(Map.get(rule.filter, :tags, []), fn {key, value} ->
        ["<Tag>", ["<Key>", key, "</Key>", "<Value>", value, "</Value>"], "</Tag>"]
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
          [["<StorageClass>", storage, "</StorageClass>"]]
        end
      },
      expiration: %{
        tag: "Expiration",
        action_tags: fn %{expired_object_delete_marker: marker} ->
          marker = if marker, do: "true", else: "false"
          [["<ExpiredObjectDeleteMarker>", marker, "</ExpiredObjectDeleteMarker>"]]
        end
      },
      noncurrent_version_transition: %{
        tag: "NoncurrentVersionTransition",
        action_tags: fn %{storage: storage} ->
          [["<StorageClass>", storage, "</StorageClass>"]]
        end
      },
      noncurrent_version_expiration: %{
        tag: "NoncurrentVersionExpiration",
        action_tags: fn _data -> [] end
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
       when action in [:transition, :expiration] and is_integer(days) and days > 0 do
    ["<Days>", Integer.to_string(days), "</Days>"]
  end

  defp livecycle_trigger(action, {:days, days})
       when action in [:abort_incomplete_multipart_upload] and is_integer(days) and days > 0 do
    ["<DaysAfterInitiation>", Integer.to_string(days), "</DaysAfterInitiation>"]
  end

  defp livecycle_trigger(action, {:days, days})
       when action in [:noncurrent_version_transition, :noncurrent_version_expiration] and
              is_integer(days) and days > 0 do
    ["<NoncurrentDays>", Integer.to_string(days), "</NoncurrentDays>"]
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
end
