defmodule ExAws.S3.Upload do
  @moduledoc """
  Represents an AWS S3 Multipart Upload operation.

  Implements `ExAws.Operation.perform/2`

  ## Examples
  ```
  "path/to/big/file"
  |> S3.Upload.stream_file
  |> S3.upload("my-bucket", "path/on/s3")
  |> ExAws.request! #=> :done
  ```

  See `ExAws.S3.upload/4` for options
  """

  @enforce_keys ~w(bucket path src)a
  defstruct [
    :src,
    :bucket,
    :path,
    :upload_id,
    opts: [],
    service: :s3
  ]

  @type t :: %__MODULE__{
          src: Enumerable.t(),
          bucket: String.t(),
          path: String.t(),
          upload_id: String.t() | nil,
          opts: Keyword.t(),
          service: :s3
        }

  def complete([], op, config) do
    # We must upload at least one "part", otherwise the
    # CompleteMultipartUpload request will fail.  So if there were no
    # parts (because we're uploading an empty file), upload an empty part.
    part = upload_chunk!({"", 1}, op, config)
    complete([part], op, config)
  end

  def complete(parts, op, config) do
    ExAws.S3.complete_multipart_upload(
      op.bucket,
      op.path,
      op.upload_id,
      Enum.sort_by(parts, &elem(&1, 0))
    )
    |> ExAws.request(config)
  end

  def initialize(op, config) do
    init_op = ExAws.S3.initiate_multipart_upload(op.bucket, op.path, op.opts)

    with {:ok, %{body: %{upload_id: upload_id}}} <- ExAws.request(init_op, config) do
      {:ok, %{op | upload_id: upload_id}}
    end
  end

  @doc """
  Open a file stream for use in an upload.

  Chunk size must be at least 5 MiB. Defaults to 5 MiB
  """
  @spec stream_file(path :: binary) :: File.Stream.t()
  @spec stream_file(path :: binary, opts :: [chunk_size: pos_integer]) :: File.Stream.t()
  def stream_file(path, opts \\ []) do
    File.stream!(path, [], opts[:chunk_size] || 5 * 1024 * 1024)
  end

  @doc """
  Upload a chunk for an operation.

  The first argument is a tuple with the binary contents of the chunk, and a
  positive integer index indicating which chunk it is. It will return this index
  along with the `etag` response from AWS necessary to complete the multipart upload.
  """
  @spec upload_chunk!({binary, pos_integer}, t, ExAws.Config.t()) :: {pos_integer, binary}
  def upload_chunk!({chunk, i}, op, config) do
    %{headers: headers} =
      ExAws.S3.upload_part(op.bucket, op.path, op.upload_id, i, chunk, op.opts)
      |> ExAws.request!(config)

    {_, etag} =
      Enum.find(headers, fn {k, _v} ->
        String.downcase(k) == "etag"
      end)

    {i, etag}
  end

  @spec upload_chunk({binary, pos_integer}, t, ExAws.Config.t()) :: {pos_integer, binary}
  def upload_chunk({chunk, i}, op, config) do
    case ExAws.S3.upload_part(op.bucket, op.path, op.upload_id, i, chunk, op.opts)
         |> ExAws.request(config) do
      {:ok, %{headers: headers}} ->
        {_, etag} =
          Enum.find(headers, fn {k, _v} ->
            String.downcase(k) == "etag"
          end)

        {i, etag}

      {:error, reason} ->
        {:error, reason}
    end
  end
end

defimpl ExAws.Operation, for: ExAws.S3.Upload do
  alias ExAws.S3.Upload

  def perform(op, config) do
    with {:ok, op} <- Upload.initialize(op, config) do
      vals =
        op.src
        |> Stream.with_index(1)
        |> Task.async_stream(Upload, :upload_chunk, [Map.delete(op, :src), config],
          max_concurrency: Keyword.get(op.opts, :max_concurrency, 4),
          timeout: Keyword.get(op.opts, :timeout, 30_000)
        )
        |> Enum.map(fn {:ok, val} -> val end)

      case Enum.find(vals, fn
             {:error, _} -> true
             _ -> false
           end) do
        nil ->
          Upload.complete(vals, op, config)

        error ->
          error
      end
    end
  end

  def stream!(_, _), do: raise("not implemented")
end
