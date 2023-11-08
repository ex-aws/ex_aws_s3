defmodule S3Lister do
  @moduledoc """
  Module for listing contents of an S3 bucket.
  """

  def init do
    config =
      ExAws.Config.new(:s3,
        access_key_id: "minio",
        secret_access_key: "minio123",
        scheme: "http://",
        host: "localhost",
        port: 9000,
        debug_requests: true
      )

    Application.put_env(:ex_aws, :s3, config)
  end

  def write_stream_to_files(stream, file_name \\ "output.txt") do
    stream
    |> Stream.map(fn payload ->
      IO.puts(payload)
      payload
    end)
    |> Stream.into(File.stream!(file_name))
    |> Stream.run()
  end

  def select_dhhd_chunk(bucket, s3_key) do
    ExAws.S3.select_object_content(
      bucket,
      s3_key,
      "select ID,Name from S3Object",
      input_serialization: %{csv: %{}},
      output_serialization: %{csv: %{}},
      scan_range: %{start: 0, end: 1000}
    )
    |> ExAws.stream!()
    |> write_stream_to_files()
  end

  def stream_file(bucket, s3_key) do
    ExAws.S3.download_file(bucket, s3_key, :memory)
    |> ExAws.stream!()
    |> Stream.into(File.stream!("local_path.csv"))
    |> Stream.run()
  end
end

S3Lister.init()
S3Lister.select_dhhd_chunk("hiive-local-preqin-data", "flowers_data.csv")
# S3Lister.stream_file("hiive-local-preqin-data", "flowers_data.csv")
