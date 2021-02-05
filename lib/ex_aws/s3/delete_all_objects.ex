defmodule ExAws.Operation.S3DeleteAllObjects do
  defstruct bucket: nil,
            objects: [],
            opts: [],
            service: :s3

  @type t :: %__MODULE__{}

  defimpl ExAws.Operation do
    def perform(%{bucket: bucket, objects: objects, opts: opts}, config) do
      request_fun = fn objects_in_batch ->
        bucket
        |> ExAws.S3.delete_multiple_objects(objects_in_batch, opts)
        |> ExAws.request(config)
      end

      delete_all_objects(request_fun, objects, opts, [])
    end

    defp delete_all_objects(_request_fun, [], _opts, acc) do
      {:ok, Enum.reverse(acc)}
    end

    defp delete_all_objects(request_fun, objects, opts, acc) do
      {objects, rest} = Enum.split(objects, 1000)

      with {:ok, result} <- request_fun.(objects) do
        delete_all_objects(request_fun, rest, opts, [result | acc])
      end
    end

    def stream!(%{bucket: bucket, objects: objects, opts: opts}, config) do
      objects
      |> Stream.chunk_every(1000)
      |> Stream.flat_map(fn objects_in_batch ->
        bucket
        |> ExAws.S3.delete_multiple_objects(objects_in_batch, opts)
        |> ExAws.request!(config)
      end)
    end
  end
end
