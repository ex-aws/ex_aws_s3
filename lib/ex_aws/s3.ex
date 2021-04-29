defmodule ExAws.S3 do
  @external_resource "README.md"
  @moduledoc "README.md"
             |> File.read!()
             |> String.split("<!-- MDOC !-->")
             |> Enum.fetch!(1)

  import ExAws.S3.Utils
  alias ExAws.S3.Parsers

  @type acl_opts :: {:acl, canned_acl} | grant
  @type grant ::
          {:grant_read, grantee}
          | {:grant_read_acp, grantee}
          | {:grant_write_acp, grantee}
          | {:grant_full_control, grantee}
  @type canned_acl ::
          :private
          | :public_read
          | :public_read_write
          | :authenticated_read
          | :bucket_owner_read
          | :bucket_owner_full_control
  @type grantee :: [
          {:email, binary}
          | {:id, binary}
          | {:uri, binary}
        ]

  @type customer_encryption_opts :: [
          customer_algorithm: binary,
          customer_key: binary,
          customer_key_md5: binary
        ]
  @type encryption_opts ::
          binary
          | [aws_kms_key_id: binary]
          | customer_encryption_opts

  @type presigned_url_opts :: [
          {:expires_in, integer}
          | {:virtual_host, boolean}
          | {:s3_accelerate, boolean}
          | {:query_params, [{binary, binary}]}
          | {:headers, [{binary, binary}]}
        ]

  @type amz_meta_opts :: [{atom, binary} | {binary, binary}, ...]

  ## Buckets
  #############
  @doc "List buckets"
  @spec list_buckets() :: ExAws.Operation.S3.t()
  @spec list_buckets(opts :: Keyword.t()) :: ExAws.Operation.S3.t()
  def list_buckets(opts \\ []) do
    request(:get, "", "/", [params: opts], parser: &ExAws.S3.Parsers.parse_all_my_buckets_result/1)
  end

  @doc "Delete a bucket"
  @spec delete_bucket(bucket :: binary) :: ExAws.Operation.S3.t()
  def delete_bucket(bucket) do
    request(:delete, bucket, "/")
  end

  @doc "Delete a bucket cors"
  @spec delete_bucket_cors(bucket :: binary) :: ExAws.Operation.S3.t()
  def delete_bucket_cors(bucket) do
    request(:delete, bucket, "/", resource: "cors")
  end

  @doc "Delete a bucket lifecycle"
  @spec delete_bucket_lifecycle(bucket :: binary) :: ExAws.Operation.S3.t()
  def delete_bucket_lifecycle(bucket) do
    request(:delete, bucket, "/", resource: "lifecycle")
  end

  @doc "Delete a bucket policy"
  @spec delete_bucket_policy(bucket :: binary) :: ExAws.Operation.S3.t()
  def delete_bucket_policy(bucket) do
    request(:delete, bucket, "/", resource: "policy")
  end

  @doc "Delete a bucket replication"
  @spec delete_bucket_replication(bucket :: binary) :: ExAws.Operation.S3.t()
  def delete_bucket_replication(bucket) do
    request(:delete, bucket, "/", resource: "replication")
  end

  @doc "Delete a bucket tagging"
  @spec delete_bucket_tagging(bucket :: binary) :: ExAws.Operation.S3.t()
  def delete_bucket_tagging(bucket) do
    request(:delete, bucket, "/", resource: "tagging")
  end

  @doc "Delete a bucket website"
  @spec delete_bucket_website(bucket :: binary) :: ExAws.Operation.S3.t()
  def delete_bucket_website(bucket) do
    request(:delete, bucket, "/", resource: "website")
  end

  @type list_objects_opts :: [
          {:delimiter, binary}
          | {:marker, binary}
          | {:prefix, binary}
          | {:encoding_type, binary}
          | {:max_keys, 0..1000}
          | {:stream_prefixes, boolean}
        ]

  @doc """
  List objects in bucket

  Can be streamed.

  ## Examples
  ```
  S3.list_objects("my-bucket") |> ExAws.request

  S3.list_objects("my-bucket") |> ExAws.stream!
  S3.list_objects("my-bucket", delimiter: "/", prefix: "backup") |> ExAws.stream!
  S3.list_objects("my-bucket", prefix: "some/inner/location/path") |> ExAws.stream!
  S3.list_objects("my-bucket", max_keys: 5, encoding_type: "url") |> ExAws.stream!
  ```
  """
  @spec list_objects(bucket :: binary) :: ExAws.Operation.S3.t()
  @spec list_objects(bucket :: binary, opts :: list_objects_opts) :: ExAws.Operation.S3.t()
  @params [:delimiter, :marker, :prefix, :encoding_type, :max_keys]
  def list_objects(bucket, opts \\ []) do
    params =
      opts
      |> format_and_take(@params)

    request(:get, bucket, "/", [params: params, headers: opts[:headers]],
      stream_builder: &ExAws.S3.Lazy.stream_objects!(bucket, opts, &1),
      parser: &ExAws.S3.Parsers.parse_list_objects/1
    )
  end

  @type list_objects_v2_opts :: [
          {:delimiter, binary}
          | {:prefix, binary}
          | {:encoding_type, binary}
          | {:max_keys, 0..1000}
          | {:stream_prefixes, boolean}
          | {:continuation_token, binary}
          | {:fetch_owner, boolean}
          | {:start_after, binary}
        ]

  @doc """
  List objects in bucket

  Can be streamed.

  ## Examples
  ```
  S3.list_objects_v2("my-bucket") |> ExAws.request

  S3.list_objects_v2("my-bucket") |> ExAws.stream!
  S3.list_objects_v2("my-bucket", delimiter: "/", prefix: "backup") |> ExAws.stream!
  S3.list_objects_v2("my-bucket", prefix: "some/inner/location/path") |> ExAws.stream!
  S3.list_objects_v2("my-bucket", max_keys: 5, encoding_type: "url") |> ExAws.stream!
  ```
  """
  @spec list_objects_v2(bucket :: binary) :: ExAws.Operation.S3.t()
  @spec list_objects_v2(bucket :: binary, opts :: list_objects_v2_opts) :: ExAws.Operation.S3.t()
  @params [
    :delimiter,
    :prefix,
    :encoding_type,
    :max_keys,
    :continuation_token,
    :fetch_owner,
    :start_after
  ]
  def list_objects_v2(bucket, opts \\ []) do
    params =
      opts
      |> format_and_take(@params)
      |> Map.put("list-type", 2)

    request(:get, bucket, "/", [params: params, headers: opts[:headers]],
      stream_builder: &ExAws.S3.Lazy.stream_objects!(bucket, opts, &1),
      parser: &ExAws.S3.Parsers.parse_list_objects/1
    )
  end

  @doc "Get bucket acl"
  @spec get_bucket_acl(bucket :: binary) :: ExAws.Operation.S3.t()
  def get_bucket_acl(bucket) do
    request(:get, bucket, "/", resource: "acl")
  end

  @doc "Get bucket cors"
  @spec get_bucket_cors(bucket :: binary) :: ExAws.Operation.S3.t()
  def get_bucket_cors(bucket) do
    request(:get, bucket, "/", resource: "cors")
  end

  @doc "Get bucket lifecycle"
  @spec get_bucket_lifecycle(bucket :: binary) :: ExAws.Operation.S3.t()
  def get_bucket_lifecycle(bucket) do
    request(:get, bucket, "/", resource: "lifecycle")
  end

  @doc "Get bucket policy"
  @spec get_bucket_policy(bucket :: binary) :: ExAws.Operation.S3.t()
  def get_bucket_policy(bucket) do
    request(:get, bucket, "/", resource: "policy")
  end

  @doc "Get bucket location"
  @spec get_bucket_location(bucket :: binary) :: ExAws.Operation.S3.t()
  def get_bucket_location(bucket) do
    request(:get, bucket, "/", resource: "location")
  end

  @doc "Get bucket logging"
  @spec get_bucket_logging(bucket :: binary) :: ExAws.Operation.S3.t()
  def get_bucket_logging(bucket) do
    request(:get, bucket, "/", resource: "logging")
  end

  @doc "Get bucket notification"
  @spec get_bucket_notification(bucket :: binary) :: ExAws.Operation.S3.t()
  def get_bucket_notification(bucket) do
    request(:get, bucket, "/", resource: "notification")
  end

  @doc "Get bucket replication"
  @spec get_bucket_replication(bucket :: binary) :: ExAws.Operation.S3.t()
  def get_bucket_replication(bucket) do
    request(:get, bucket, "/", resource: "replication")
  end

  @doc "Get bucket tagging"
  @spec get_bucket_tagging(bucket :: binary) :: ExAws.Operation.S3.t()
  def get_bucket_tagging(bucket) do
    request(:get, bucket, "/", resource: "tagging")
  end

  @doc "Get bucket object versions"
  @spec get_bucket_object_versions(bucket :: binary) :: ExAws.Operation.S3.t()
  @spec get_bucket_object_versions(bucket :: binary, opts :: Keyword.t()) ::
          ExAws.Operation.S3.t()
  def get_bucket_object_versions(bucket, opts \\ []) do
    request(:get, bucket, "/", [resource: "versions", params: opts],
      parser: &ExAws.S3.Parsers.parse_bucket_object_versions/1
    )
  end

  @doc "Get bucket payment configuration"
  @spec get_bucket_request_payment(bucket :: binary) :: ExAws.Operation.S3.t()
  def get_bucket_request_payment(bucket) do
    request(:get, bucket, "/", resource: "requestPayment")
  end

  @doc "Get bucket versioning"
  @spec get_bucket_versioning(bucket :: binary) :: ExAws.Operation.S3.t()
  def get_bucket_versioning(bucket) do
    request(:get, bucket, "/", resource: "versioning")
  end

  @doc "Get bucket website"
  @spec get_bucket_website(bucket :: binary) :: ExAws.Operation.S3.t()
  def get_bucket_website(bucket) do
    request(:get, bucket, "/", resource: "website")
  end

  @doc "Determine if a bucket exists"
  @spec head_bucket(bucket :: binary) :: ExAws.Operation.S3.t()
  def head_bucket(bucket) do
    request(:head, bucket, "/")
  end

  @doc "List multipart uploads for a bucket"
  @spec list_multipart_uploads(bucket :: binary) :: ExAws.Operation.S3.t()
  @spec list_multipart_uploads(bucket :: binary, opts :: Keyword.t()) :: ExAws.Operation.S3.t()
  @params [:delimiter, :encoding_type, :max_uploads, :key_marker, :prefix, :upload_id_marker]
  def list_multipart_uploads(bucket, opts \\ []) do
    params = opts |> format_and_take(@params)

    request(:get, bucket, "/", [resource: "uploads", params: params], %{
      parser: &Parsers.parse_list_multipart_uploads/1
    })
  end

  @doc "Creates a bucket in the specified region"
  @spec put_bucket(bucket :: binary, region :: binary) :: ExAws.Operation.S3.t()
  def put_bucket(bucket, region, opts \\ [])
  def put_bucket(bucket, "", opts), do: put_bucket(bucket, "us-east-1", opts)

  def put_bucket(bucket, region, opts) do
    headers =
      opts
      |> Map.new()
      |> format_acl_headers

    body = region |> put_bucket_body

    request(:put, bucket, "/", body: body, headers: headers)
  end

  @doc "Update or create a bucket access control policy"
  @spec put_bucket_acl(bucket :: binary, opts :: [acl_opts]) :: ExAws.Operation.S3.t()
  def put_bucket_acl(bucket, grants) do
    request(:put, bucket, "/", headers: format_acl_headers(grants))
  end

  @doc "Update or create a bucket CORS policy"
  @spec put_bucket_cors(bucket :: binary, cors_config :: list(map())) :: ExAws.Operation.S3.t()
  def put_bucket_cors(bucket, cors_rules) do
    rules =
      cors_rules
      |> Enum.map(&build_cors_rule/1)
      |> IO.iodata_to_binary()

    body = "<CORSConfiguration>#{rules}</CORSConfiguration>"
    content_md5 = :crypto.hash(:md5, body) |> Base.encode64()
    headers = %{"content-md5" => content_md5}

    request(:put, bucket, "/", resource: "cors", body: body, headers: headers)
  end

  @doc """
  Update or create a bucket lifecycle configuration

  ## Live-Cycle Rule Format

      %{
        # Unique id for the rule (max. 255 chars, max. 1000 rules allowed)
        id: "123",

        # Disabled rules are not executed
        enabled: true,

        # Filters
        # Can be based on prefix, object tag(s), both or none
        filter: %{
          prefix: "prefix/",
          tags: %{
            "key" => "value"
          }
        },

        # Actions
        # https://docs.aws.amazon.com/AmazonS3/latest/dev/intro-lifecycle-rules.html#intro-lifecycle-rules-actions
        actions: %{
          transition: %{
            trigger: {:date, ~D[2020-03-26]}, # Date or days based
            storage: ""
          },
          expiration: %{
            trigger: {:days, 2}, # Date or days based
            expired_object_delete_marker: true
          },
          noncurrent_version_transition: %{
            trigger: {:days, 2}, # Only days based
            storage: ""
          },
          noncurrent_version_expiration: %{
            trigger: {:days, 2} # Only days based
          },
          abort_incomplete_multipart_upload: %{
            trigger: {:days, 2} # Only days based
          }
        }
      }

  """
  @spec put_bucket_lifecycle(bucket :: binary, lifecycle_rules :: list(map())) ::
          ExAws.Operation.S3.t()
  def put_bucket_lifecycle(bucket, lifecycle_rules) do
    rules =
      lifecycle_rules
      |> Enum.map(&build_lifecycle_rule/1)
      |> IO.iodata_to_binary()

    body = "<LifecycleConfiguration>#{rules}</LifecycleConfiguration>"

    content_md5 = :crypto.hash(:md5, body) |> Base.encode64()
    headers = %{"content-md5" => content_md5}

    request(:put, bucket, "/", resource: "lifecycle", body: body, headers: headers)
  end

  @doc "Update or create a bucket policy configuration"
  @spec put_bucket_policy(bucket :: binary, policy :: String.t()) :: ExAws.Operation.S3.t()
  def put_bucket_policy(bucket, policy) do
    request(:put, bucket, "/", resource: "policy", body: policy)
  end

  @doc "Update or create a bucket logging configuration"
  @spec put_bucket_logging(bucket :: binary, logging_config :: map()) :: no_return
  def put_bucket_logging(bucket, _logging_config) do
    raise "not yet implemented"
    request(:put, bucket, "/")
  end

  @doc "Update or create a bucket notification configuration"
  @spec put_bucket_notification(bucket :: binary, notification_config :: map()) :: no_return
  def put_bucket_notification(bucket, _notification_config) do
    raise "not yet implemented"
    request(:put, bucket, "/")
  end

  @doc "Update or create a bucket replication configuration"
  @spec put_bucket_replication(bucket :: binary, replication_config :: map()) :: no_return
  def put_bucket_replication(bucket, _replication_config) do
    raise "not yet implemented"
    request(:put, bucket, "/")
  end

  @doc "Update or create a bucket tagging configuration"
  @spec put_bucket_tagging(bucket :: binary, tags :: map()) :: no_return
  def put_bucket_tagging(bucket, _tags) do
    raise "not yet implemented"
    request(:put, bucket, "/")
  end

  @doc "Update or create a bucket requestPayment configuration"
  @spec put_bucket_request_payment(bucket :: binary, payer :: :requester | :bucket_owner) ::
          no_return
  def put_bucket_request_payment(bucket, _payer) do
    raise "not yet implemented"
    request(:put, bucket, "/")
  end

  @doc "Update or create a bucket versioning configuration"
  @spec put_bucket_versioning(bucket :: binary, version_config :: binary) :: no_return
  def put_bucket_versioning(bucket, _version_config) do
    raise "not yet implemented"
    request(:put, bucket, "/")
  end

  @doc "Update or create a bucket website configuration"
  @spec put_bucket_website(bucket :: binary, website_config :: binary) :: no_return
  def put_bucket_website(bucket, _website_config) do
    raise "not yet implemented"
    request(:put, bucket, "/")
  end

  ## Objects
  ###########

  @doc "Delete an object within a bucket"
  @spec delete_object(bucket :: binary, object :: binary) :: ExAws.Operation.S3.t()
  def delete_object(bucket, object, opts \\ []) do
    request(:delete, bucket, object, headers: opts |> Map.new())
  end

  @doc "Remove the entire tag set from the specified object"
  @spec delete_object_tagging(bucket :: binary, object :: binary, opts :: Keyword.t()) ::
          ExAws.Operation.S3.t()
  def delete_object_tagging(bucket, object, opts \\ []) do
    request(:delete, bucket, object, resource: "tagging", headers: opts |> Map.new())
  end

  @doc """
  Delete multiple objects within a bucket

  Limited to 1000 objects.
  """
  @spec delete_multiple_objects(
          bucket :: binary,
          objects :: [binary | {binary, binary}, ...]
        ) :: ExAws.Operation.S3.t()
  @spec delete_multiple_objects(
          bucket :: binary,
          objects :: [binary | {binary, binary}, ...],
          opts :: [quiet: true]
        ) :: ExAws.Operation.S3.t()
  def delete_multiple_objects(bucket, objects, opts \\ []) do
    objects_xml =
      Enum.map(objects, fn
        {key, version} ->
          [
            "<Object><Key>",
            escape_xml_string(key),
            "</Key><VersionId>",
            version,
            "</VersionId></Object>"
          ]

        key ->
          ["<Object><Key>", escape_xml_string(key), "</Key></Object>"]
      end)

    quiet =
      case opts do
        [quiet: true] -> "<Quiet>true</Quiet>"
        _ -> ""
      end

    body = [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      "<Delete>",
      quiet,
      objects_xml,
      "</Delete>"
    ]

    content_md5 = :crypto.hash(:md5, body) |> Base.encode64()
    body_binary = body |> IO.iodata_to_binary()

    request(:post, bucket, "/?delete", body: body_binary, headers: %{"content-md5" => content_md5})
  end

  @doc """
  Delete all listed objects.

  When performed, this function will continue making `delete_multiple_objects`
  requests deleting 1000 objects at a time until all are deleted.

  Can be streamed.

  ## Example
  ```
  stream = ExAws.S3.list_objects(bucket(), prefix: "some/prefix") |> ExAws.stream!() |> Stream.map(& &1.key)
  ExAws.S3.delete_all_objects(bucket(), stream) |> ExAws.request()
  ```
  """
  @spec delete_all_objects(
          bucket :: binary,
          objects :: [binary | {binary, binary}, ...] | Enumerable.t()
        ) :: ExAws.Operation.S3DeleteAllObjects.t()
  @spec delete_all_objects(
          bucket :: binary,
          objects :: [binary | {binary, binary}, ...] | Enumerable.t(),
          opts :: [quiet: true]
        ) :: ExAws.Operation.S3DeleteAllObjects.t()
  def delete_all_objects(bucket, objects, opts \\ []) do
    %ExAws.Operation.S3DeleteAllObjects{bucket: bucket, objects: objects, opts: opts}
  end

  @type get_object_response_opts :: [
          {:content_language, binary}
          | {:expires, binary}
          | {:cache_control, binary}
          | {:content_disposition, binary}
          | {:content_encoding, binary}
        ]
  @type get_object_opts :: [
          {:response, get_object_response_opts}
          | {:version_id, binary}
          | head_object_opt
        ]
  @doc """
  Get an object from a bucket

  ## Examples
  ```
  S3.get_object("my-bucket", "image.png")
  S3.get_object("my-bucket", "image.png", version_id: "ae57ekgXPpdiVZLkYVWoTAGRhGJ5swt9")
  ```
  """
  @spec get_object(bucket :: binary, object :: binary) :: ExAws.Operation.S3.t()
  @spec get_object(bucket :: binary, object :: binary, opts :: get_object_opts) ::
          ExAws.Operation.S3.t()
  @response_params [
    :content_type,
    :content_language,
    :expires,
    :cache_control,
    :content_disposition,
    :content_encoding
  ]
  @request_headers [:range, :if_modified_since, :if_unmodified_since, :if_match, :if_none_match]
  def get_object(bucket, object, opts \\ []) do
    opts = opts |> Map.new()

    response_opts =
      opts
      |> Map.get(:response, %{})
      |> format_and_take(@response_params)
      |> namespace("response")

    params =
      opts
      |> format_and_take([:version_id])
      |> Map.merge(response_opts)

    headers =
      opts
      |> format_and_take(@request_headers)

    headers =
      opts
      |> Map.get(:encryption, %{})
      |> build_encryption_headers
      |> Map.merge(headers)

    request(:get, bucket, object, headers: headers, params: params)
  end

  @type download_file_opts :: [
          max_concurrency: pos_integer,
          chunk_size: pos_integer,
          timeout: pos_integer
        ]

  @doc """
  Download an S3 object to a file.

  This operation downloads multiple parts of an S3 object concurrently, allowing
  you to maximize throughput.

  Defaults to a concurrency of 8, chunk size of 1MB, and a timeout of 1 minute.

  ### Streaming to memory

  In order to use `ExAws.stream!/2`, the third `dest` parameter must be set to `:memory`.
  An example would be like the following:

      ExAws.S3.download_file("example-bucket", "path/to/file.txt", :memory)
      |> ExAws.stream!()

  Note that **this won't start fetching anything immediately** since it returns an Elixir `Stream`.

  #### Streaming by line

  Streaming by line can be done with `Stream.chunk_while/4`. Here is an example:

      # Returns a stream which grabs chunks of data from S3 as specified in `opts`
      # but processes the stream line by line. For example, the default chunk
      # size of 1MB means requests for bytes from S3 will ask for 1MB sizes (to be downloaded)
      # however each element of the stream will be a single line.
      def generate_stream(bucket, file, opts \\\\ []) do
        bucket
        |> ExAws.S3.download_file(file, :memory, opts)
        |> ExAws.stream!()
        # Uncomment if you need to gunzip (and add dependency :stream_gzip)
        # |> StreamGzip.gunzip()
        |> Stream.chunk_while("", &chunk_fun/2, &after_fun/1)
      end

      def chunk_fun(chunk, acc) do
        split_chunk(acc, chunk) || split_chunk(chunk, "")
      end

      defp split_chunk("", _append_remaining), do: nil
      defp split_chunk(string, append_remaining) do
        case String.split(string, "\\n", parts: 2) do
          [l] ->
            {:cont, l}
          [l, remaining] ->
            {:cont, l, remaining <> append_remaining}
        end
      end

      def after_fun(""), do: {:cont, ""}
      def after_fun(acc), do: {:cont, acc, ""}
  """
  @spec download_file(bucket :: binary, path :: binary, dest :: :memory | binary) ::
          __MODULE__.Download.t()
  @spec download_file(
          bucket :: binary,
          path :: binary,
          dest :: :memory | binary,
          opts :: download_file_opts
        ) :: __MODULE__.Download.t()
  def download_file(bucket, path, dest, opts \\ []) do
    %__MODULE__.Download{
      bucket: bucket,
      path: path,
      dest: dest,
      opts: opts
    }
  end

  @type upload_opts :: [{:max_concurrency, pos_integer} | initiate_multipart_upload_opts]

  @doc """
  Multipart upload to S3.

  Handles initialization, uploading parts concurrently, and multipart upload completion.

  ## Uploading a stream

  Streams that emit binaries may be uploaded directly to S3. Each binary will be uploaded
  as a chunk, so it must be at least 5 megabytes in size. The `S3.Upload.stream_file`
  helper takes care of reading the file in 5 megabyte chunks.
  ```
  "path/to/big/file"
  |> S3.Upload.stream_file
  |> S3.upload("my-bucket", "path/on/s3")
  |> ExAws.request! #=> :done
  ```

  ## Options

  These options are specific to this function
  * See `Task.async_stream/5`'s `:max_concurrency` and `:timeout` options.
    * `:max_concurrency` - only applies when uploading a stream. Sets the maximum number of tasks to run at the same time. Defaults to `4`
    * `:timeout` - the maximum amount of time (in milliseconds) each task is allowed to execute for. Defaults to `30_000`.

  All other options (ex. `:content_type`) are passed through to
  `ExAws.S3.initiate_multipart_upload/3`.

  """
  @spec upload(
          source :: Enumerable.t(),
          bucket :: String.t(),
          path :: String.t(),
          opts :: upload_opts
        ) :: __MODULE__.Upload.t()
  def upload(source, bucket, path, opts \\ []) do
    %__MODULE__.Upload{
      src: source,
      bucket: bucket,
      path: path,
      opts: opts
    }
  end

  @doc "Get an object's access control policy"
  @spec get_object_acl(bucket :: binary, object :: binary) :: ExAws.Operation.S3.t()
  @spec get_object_acl(bucket :: binary, object :: binary, opts :: Keyword.t()) ::
          ExAws.Operation.S3.t()
  def get_object_acl(bucket, object, opts \\ []) do
    request(:get, bucket, object, resource: "acl", headers: opts |> Map.new())
  end

  @doc "Get a torrent for a bucket"
  @spec get_object_torrent(bucket :: binary, object :: binary) :: ExAws.Operation.S3.t()
  def get_object_torrent(bucket, object) do
    request(:get, bucket, object, resource: "torrent")
  end

  @doc "Get object tagging"
  @spec get_object_tagging(bucket :: binary, object :: binary, opts :: Keyword.t()) ::
          ExAws.Operation.S3.t()
  def get_object_tagging(bucket, object, opts \\ []) do
    request(:get, bucket, object, [resource: "tagging", headers: opts |> Map.new()],
      parser: &ExAws.S3.Parsers.parse_object_tagging/1
    )
  end

  @type head_object_opt ::
          {:encryption, customer_encryption_opts}
          | {:range, binary}
          | {:version_id, binary}
          | {:if_modified_since, binary}
          | {:if_unmodified_since, binary}
          | {:if_match, binary}
          | {:if_none_match, binary}
  @type head_object_opts :: [head_object_opt]

  @doc "Determine if an object exists"
  @spec head_object(bucket :: binary, object :: binary) :: ExAws.Operation.S3.t()
  @spec head_object(bucket :: binary, object :: binary, opts :: head_object_opts) ::
          ExAws.Operation.S3.t()
  @request_headers [:range, :if_modified_since, :if_unmodified_since, :if_match, :if_none_match]
  def head_object(bucket, object, opts \\ []) do
    opts = opts |> Map.new()

    headers =
      opts
      |> format_and_take(@request_headers)

    headers =
      opts
      |> Map.get(:encryption, %{})
      |> build_encryption_headers
      |> Map.merge(headers)

    params = format_and_take(opts, [:version_id])
    request(:head, bucket, object, headers: headers, params: params)
  end

  @doc "Determine the CORS configuration for an object"
  @spec options_object(
          bucket :: binary,
          object :: binary,
          origin :: binary,
          request_method :: atom
        ) :: ExAws.Operation.S3.t()
  @spec options_object(
          bucket :: binary,
          object :: binary,
          origin :: binary,
          request_method :: atom,
          request_headers :: [binary]
        ) :: ExAws.Operation.S3.t()
  def options_object(bucket, object, origin, request_method, request_headers \\ []) do
    headers = [
      {"Origin", origin},
      {"Access-Control-Request-Method", request_method},
      {"Access-Control-Request-Headers", request_headers |> Enum.join(",")}
    ]

    request(:options, bucket, object, headers: headers)
  end

  @doc "Restore an object to a particular version"
  @spec post_object_restore(
          bucket :: binary,
          object :: binary,
          number_of_days :: pos_integer
        ) :: ExAws.Operation.S3.t()
  @spec post_object_restore(
          bucket :: binary,
          object :: binary,
          number_of_days :: pos_integer,
          opts :: [version_id: binary]
        ) :: ExAws.Operation.S3.t()
  def post_object_restore(bucket, object, number_of_days, opts \\ []) do
    params = format_and_take(opts, [:version_id])

    body = """
    <RestoreRequest xmlns="http://s3.amazonaws.com/doc/2006-3-01">
      <Days>#{number_of_days}</Days>
    </RestoreRequest>
    """

    request(:post, bucket, object, resource: "restore", params: params, body: body)
  end

  @type put_object_opts :: [
          {:cache_control, binary}
          | {:content_disposition, binary}
          | {:content_encoding, binary}
          | {:content_length, binary}
          | {:content_type, binary}
          | {:expect, binary}
          | {:expires, binary}
          | {:storage_class, :standard | :reduced_redundancy}
          | {:website_redirect_location, binary}
          | {:encryption, encryption_opts}
          | {:meta, amz_meta_opts}
          | acl_opts
        ]
  @doc "Create an object within a bucket"
  @spec put_object(bucket :: binary, object :: binary, body :: binary) :: ExAws.Operation.S3.t()
  @spec put_object(bucket :: binary, object :: binary, body :: binary, opts :: put_object_opts) ::
          ExAws.Operation.S3.t()
  def put_object(bucket, object, body, opts \\ []) do
    request(:put, bucket, object, body: body, headers: put_object_headers(opts))
  end

  @doc "Create or update an object's access control policy"
  @spec put_object_acl(bucket :: binary, object :: binary, acl :: [acl_opts]) ::
          ExAws.Operation.S3.t()
  def put_object_acl(bucket, object, acl) do
    headers = acl |> Map.new() |> format_acl_headers
    request(:put, bucket, object, headers: headers, resource: "acl")
  end

  @doc """
  Add a set of tags to an existing object

  ## Options

  - `:version_id` - The versionId of the object that the tag-set will be added to.

  """
  @spec put_object_tagging(
          bucket :: binary,
          object :: binary,
          tags :: Access.t(),
          opts :: Keyword.t()
        ) :: ExAws.Operation.S3.t()
  def put_object_tagging(bucket, object, tags, opts \\ []) do
    {version_id, opts} = Keyword.pop(opts, :version_id)

    params =
      if version_id do
        %{"versionId" => version_id}
      else
        %{}
      end

    tags_xml =
      Enum.map(tags, fn
        {key, value} ->
          ["<Tag><Key>", to_string(key), "</Key><Value>", to_string(value), "</Value></Tag>"]
      end)

    body = [
      ~s(<?xml version="1.0" encoding="UTF-8"?>),
      "<Tagging>",
      "<TagSet>",
      tags_xml,
      "</TagSet>",
      "</Tagging>"
    ]

    content_md5 = :crypto.hash(:md5, body) |> Base.encode64()

    headers =
      opts
      |> Map.new()
      |> Map.merge(%{"content-md5" => content_md5})

    body_binary = body |> IO.iodata_to_binary()

    request(:put, bucket, object,
      resource: "tagging",
      body: body_binary,
      headers: headers,
      params: params
    )
  end

  @type put_object_copy_opts :: [
          {:metadata_directive, :COPY | :REPLACE}
          | {:copy_source_if_modified_since, binary}
          | {:copy_source_if_unmodified_since, binary}
          | {:copy_source_if_match, binary}
          | {:copy_source_if_none_match, binary}
          | {:website_redirect_location, binary}
          | {:destination_encryption, encryption_opts}
          | {:source_encryption, customer_encryption_opts}
          | {:cache_control, binary}
          | {:content_disposition, binary}
          | {:content_encoding, binary}
          | {:content_length, binary}
          | {:content_type, binary}
          | {:expect, binary}
          | {:expires, binary}
          | {:storage_class, :standard | :reduced_redundancy}
          | {:website_redirect_location, binary}
          | {:meta, amz_meta_opts}
          | acl_opts
        ]

  @doc "Copy an object"
  @spec put_object_copy(
          dest_bucket :: binary,
          dest_object :: binary,
          src_bucket :: binary,
          src_object :: binary
        ) :: ExAws.Operation.S3.t()
  @spec put_object_copy(
          dest_bucket :: binary,
          dest_object :: binary,
          src_bucket :: binary,
          src_object :: binary,
          opts :: put_object_copy_opts
        ) :: ExAws.Operation.S3.t()
  @amz_headers ~w(
    metadata_directive
    copy_source_if_modified_since
    copy_source_if_unmodified_since
    copy_source_if_match
    copy_source_if_none_match
    storage_class
    website_redirect_location)a
  def put_object_copy(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    opts = opts |> Map.new()

    amz_headers =
      opts
      |> format_and_take(@amz_headers)
      |> namespace("x-amz")

    source_encryption =
      opts
      |> Map.get(:source_encryption, %{})
      |> build_encryption_headers
      |> Enum.into(%{}, fn {<<"x-amz", k::binary>>, v} ->
        {"x-amz-copy-source" <> k, v}
      end)

    destination_encryption =
      opts
      |> Map.get(:destination_encryption, %{})
      |> build_encryption_headers

    regular_headers =
      opts
      |> Map.delete(:encryption)
      |> put_object_headers

    encoded_src_object =
      src_object
      |> String.split("/")
      |> Enum.reject(&(&1 == ""))
      |> Enum.map(&URI.encode_www_form(&1))
      |> Enum.join("/")

    headers =
      regular_headers
      |> Map.merge(amz_headers)
      |> Map.merge(source_encryption)
      |> Map.merge(destination_encryption)
      |> Map.put("x-amz-copy-source", "/#{URI.encode_www_form(src_bucket)}/#{encoded_src_object}")

    request(:put, dest_bucket, dest_object, headers: headers)
  end

  @type initiate_multipart_upload_opts :: [
          {:cache_control, binary}
          | {:content_disposition, binary}
          | {:content_encoding, binary}
          | {:content_type, binary}
          | {:expires, binary}
          | {:storage_class, :standard | :reduced_redundancy}
          | {:website_redirect_location, binary}
          | {:encryption, encryption_opts}
          | acl_opts
        ]

  @doc "Initiate a multipart upload"
  @spec initiate_multipart_upload(bucket :: binary, object :: binary) :: ExAws.Operation.S3.t()
  @spec initiate_multipart_upload(
          bucket :: binary,
          object :: binary,
          opts :: initiate_multipart_upload_opts
        ) :: ExAws.Operation.S3.t()
  def initiate_multipart_upload(bucket, object, opts \\ []) do
    request(:post, bucket, object, [resource: "uploads", headers: put_object_headers(opts)], %{
      parser: &Parsers.parse_initiate_multipart_upload/1
    })
  end

  @doc "Upload a part for a multipart upload"
  @spec upload_part(
          bucket :: binary,
          object :: binary,
          upload_id :: binary,
          part_number :: pos_integer,
          body :: binary
        ) :: ExAws.Operation.S3.t()
  @spec upload_part(
          bucket :: binary,
          object :: binary,
          upload_id :: binary,
          part_number :: pos_integer,
          body :: binary,
          opts :: [encryption_opts | {:expect, binary}]
        ) :: ExAws.Operation.S3.t()
  def upload_part(bucket, object, upload_id, part_number, body, _opts \\ []) do
    params = %{"uploadId" => upload_id, "partNumber" => part_number}
    request(:put, bucket, object, params: params, body: body)
  end

  @type upload_part_copy_opts :: [
          {:copy_source_range, Range.t()}
          | {:copy_source_if_modified_since, binary}
          | {:copy_source_if_unmodified_since, binary}
          | {:copy_source_if_match, binary}
          | {:copy_source_if_none_match, binary}
          | {:destination_encryption, encryption_opts}
          | {:source_encryption, customer_encryption_opts}
        ]

  @doc "Upload a part for a multipart copy"
  @spec upload_part_copy(
          dest_bucket :: binary,
          dest_object :: binary,
          src_bucket :: binary,
          src_object :: binary
        ) :: ExAws.Operation.S3.t()
  @spec upload_part_copy(
          dest_bucket :: binary,
          dest_object :: binary,
          src_bucket :: binary,
          src_object :: binary,
          opts :: upload_part_copy_opts
        ) :: ExAws.Operation.S3.t()
  @amz_headers ~w(
    copy_source_if_modified_since
    copy_source_if_unmodified_since
    copy_source_if_match
    copy_source_if_none_match)a
  def upload_part_copy(dest_bucket, dest_object, src_bucket, src_object, opts \\ []) do
    opts = opts |> Map.new()

    source_encryption =
      opts
      |> Map.get(:source_encryption, %{})
      |> build_encryption_headers
      |> Enum.into(%{}, fn {<<"x-amz", k::binary>>, v} ->
        {"x-amz-copy-source" <> k, v}
      end)

    destination_encryption =
      opts
      |> Map.get(:destination_encryption, %{})
      |> build_encryption_headers

    headers =
      opts
      |> format_and_take(@amz_headers)
      |> namespace("x-amz")
      |> Map.merge(source_encryption)
      |> Map.merge(destination_encryption)

    headers =
      case opts do
        %{copy_source_range: first..last} ->
          Map.put(headers, "x-amz-copy-source-range", "bytes=#{first}-#{last}")

        _ ->
          headers
      end
      |> Map.put("x-amz-copy-source", "/#{src_bucket}/#{src_object}")

    request(:put, dest_bucket, dest_object, [headers: headers], %{
      parser: &Parsers.parse_upload_part_copy/1
    })
  end

  @doc "Complete a multipart upload"
  @spec complete_multipart_upload(
          bucket :: binary,
          object :: binary,
          upload_id :: binary,
          parts :: [{binary | pos_integer, binary}, ...]
        ) :: ExAws.Operation.S3.t()
  def complete_multipart_upload(bucket, object, upload_id, parts) do
    parts_xml =
      parts
      |> Enum.map(fn {part_number, etag} ->
        [
          "<Part>",
          "<PartNumber>",
          Integer.to_string(part_number),
          "</PartNumber>",
          "<ETag>",
          etag,
          "</ETag>",
          "</Part>"
        ]
      end)

    body =
      ["<CompleteMultipartUpload>", parts_xml, "</CompleteMultipartUpload>"]
      |> IO.iodata_to_binary()

    request(:post, bucket, object, [params: %{"uploadId" => upload_id}, body: body], %{
      parser: &Parsers.parse_complete_multipart_upload/1
    })
  end

  @doc "Abort a multipart upload"
  @spec abort_multipart_upload(bucket :: binary, object :: binary, upload_id :: binary) ::
          ExAws.Operation.S3.t()
  def abort_multipart_upload(bucket, object, upload_id) do
    request(:delete, bucket, object, params: %{"uploadId" => upload_id})
  end

  @doc "List the parts of a multipart upload"
  @spec list_parts(bucket :: binary, object :: binary, upload_id :: binary) ::
          ExAws.Operation.S3.t()
  @spec list_parts(bucket :: binary, object :: binary, upload_id :: binary, opts :: Keyword.t()) ::
          ExAws.Operation.S3.t()
  def list_parts(bucket, object, upload_id, opts \\ []) do
    params =
      opts
      |> Map.new()
      |> Map.merge(%{"uploadId" => upload_id})

    request(:get, bucket, object, [params: params], %{parser: &Parsers.parse_list_parts/1})
  end

  @doc """
  Generate a pre-signed URL for an object.

  When option param `:virtual_host` is `true`, the bucket name will be used as
  the hostname. This will cause the returned URL to be 'http' and not 'https'.

  When option param `:s3_accelerate` is `true`, the bucket name will be used as
  the hostname, along with the `s3-accelerate.amazonaws.com` host.

  Additional (signed) query parameters can be added to the url by setting option param
  `:query_params` to a list of `{"key", "value"}` pairs. Useful if you are uploading parts of
  a multipart upload directly from the browser.

  Signed headers can be added to the url by setting option param `:headers` to
  a list of `{"key", "value"}` pairs.
  """
  @spec presigned_url(
          config :: map,
          http_method :: atom,
          bucket :: binary,
          object :: binary,
          opts :: presigned_url_opts
        ) :: {:ok, binary} | {:error, binary}
  @one_week 60 * 60 * 24 * 7
  def presigned_url(config, http_method, bucket, object, opts \\ []) do
    expires_in = Keyword.get(opts, :expires_in, 3600)
    query_params = Keyword.get(opts, :query_params, [])
    virtual_host = Keyword.get(opts, :virtual_host, false)
    s3_accelerate = Keyword.get(opts, :s3_accelerate, false)
    headers = Keyword.get(opts, :headers, [])

    {config, virtual_host} =
      if s3_accelerate,
        do: {put_accelerate_host(config), true},
        else: {config, virtual_host}

    case expires_in > @one_week do
      true ->
        {:error, "expires_in_exceeds_one_week"}

      false ->
        url = url_to_sign(bucket, object, config, virtual_host)
        datetime = :calendar.universal_time()

        ExAws.Auth.presigned_url(
          http_method,
          url,
          :s3,
          datetime,
          config,
          expires_in,
          query_params,
          nil,
          headers
        )
    end
  end

  defp put_bucket_body("us-east-1"), do: ""

  defp put_bucket_body(region) do
    """
    <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <LocationConstraint>#{region}</LocationConstraint>
    </CreateBucketConfiguration>
    """
  end

  defp url_to_sign(bucket, object, config, virtual_host) do
    port = sanitized_port_component(config)
    object = ensure_slash(object)

    case virtual_host do
      true -> "#{config[:scheme]}#{bucket}.#{config[:host]}#{port}#{object}"
      false -> "#{config[:scheme]}#{config[:host]}#{port}/#{bucket}#{object}"
    end
  end

  defp request(http_method, bucket, path, data \\ [], opts \\ %{}) do
    %ExAws.Operation.S3{
      http_method: http_method,
      bucket: bucket,
      path: path,
      body: data[:body] || "",
      headers: data[:headers] || %{},
      resource: data[:resource] || "",
      params: data[:params] || %{}
    }
    |> struct(opts)
  end

  defp put_accelerate_host(config) do
    Map.put(config, :host, "s3-accelerate.amazonaws.com")
  end

  defp escape_xml_string(value) do
    String.replace(value, ["'", "\"", "&", "<", ">", "\r", "\n"], fn
      "'" -> "&apos;"
      "\"" -> "&quot;"
      "&" -> "&amp;"
      "<" -> "&lt;"
      ">" -> "&gt;"
      "\r" -> "&#13;"
      "\n" -> "&#10;"
    end)
  end
end
