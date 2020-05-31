defmodule ExAws.S3.Download do
  @moduledoc """
  Represents an AWS S3 file download operation
  """

  @enforce_keys ~w(bucket path dest)a
  defstruct [
    :bucket,
    :path,
    :dest,
    opts: [],
    service: :s3,
  ]

  @type t :: %__MODULE__{}

  def get_chunk(op, %{start_byte: start_byte, end_byte: end_byte}, config) do
    %{body: body} =
      op.bucket
      |> ExAws.S3.get_object(op.path, [range: "bytes=#{start_byte}-#{end_byte}"])
      |> ExAws.request!(config)

    {start_byte, body}
  end

  def build_chunk_stream(op, config) do
    op.bucket
    |> get_file_size(op.path, config)
    |> chunk_stream(op.opts[:chunk_size] || 1024 * 1024)
  end

  def chunk_stream(file_size, chunk_size) do
    Stream.unfold(0, fn counter ->
      start_byte = counter * chunk_size

      if start_byte >= file_size do
        nil
      else
        end_byte = (counter + 1) * chunk_size

        # byte ranges are inclusive, so we want to remove one. IE, first 500 bytes
        # is range 0-499. Also, we need it bounded by the max size of the file
        end_byte = min(end_byte, file_size) - 1

        {%{start_byte: start_byte, end_byte: end_byte}, counter + 1}
      end
    end)
  end

  defp get_file_size(bucket, path, config) do
    %{headers: headers} = ExAws.S3.head_object(bucket, path) |> ExAws.request!(config)

    headers
    |> Enum.find(fn {k, _} -> String.downcase(k) == "content-length" end)
    |> elem(1)
    |> String.to_integer
  end
end

defimpl ExAws.Operation, for: ExAws.S3.Download do

  alias ExAws.S3.Download

  def perform(op, config) do
    op.dest
    |> File.open([:write, :delayed_write, :binary])
    |> download_to(op, config)
  end

  defp download_to({:error, e}, _op, _config), do: {:error, e}
  defp download_to({:ok, file}, op, config) do
    try do
      op
      |> Download.build_chunk_stream(config)
      |> Task.async_stream(fn boundaries ->
        chunk = Download.get_chunk(op, boundaries, config)
        :ok = :file.pwrite(file, [chunk])
      end,
        max_concurrency: Keyword.get(op.opts, :max_concurrency, 8),
        timeout: Keyword.get(op.opts, :timeout, 60_000)
      )
      |> Stream.run

      File.close(file)

      {:ok, :done}
    rescue
      _ ->
        File.close(file)
        File.rm(op.dest)
        {:error, "error downloading file"}
    end
  end

  def stream!(op, config) do
    op
    |> Download.build_chunk_stream(config)
    |> Task.async_stream(fn boundaries ->
      Download.get_chunk(op, boundaries, config)
    end,
      max_concurrency: Keyword.get(op.opts, :max_concurrency, 8),
      timeout: Keyword.get(op.opts, :timeout, 60_000)
    )
    |> Stream.map(fn
      # Download.get_chunk/3 uses ExAws.request! so if we get here it is
      # successful otherwise it has already risen an error
      {:ok, {_start_byte, chunk}} ->
        chunk
    end)
  end
end
