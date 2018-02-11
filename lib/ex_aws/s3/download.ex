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

end

defimpl ExAws.Operation, for: ExAws.S3.Download do

  def perform(op, config) do
    file = File.open!(op.dest, [:write, :delayed_write, :binary])

    opts = Keyword.merge([chunk_size: 1024*1024], op.opts)
    ExAws.S3.get_object(op.bucket, op.path, opts)
    |> ExAws.stream!(config)
    |> Task.async_stream(fn chunk ->
      :ok = :file.pwrite(file, [chunk])
    end,
      max_concurrency: Keyword.get(op.opts, :max_concurrency, 8),
      timeout: Keyword.get(op.opts, :timeout, 60_000)
    )
    |> Stream.run

    File.close(file)

    {:ok, :done}
  end

  def stream!(_op, _config) do
    raise "not supported yet"
  end
end
