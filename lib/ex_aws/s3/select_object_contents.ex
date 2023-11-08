defmodule ExAws.S3.SelectObjectContents do
  alias ExAws.S3.Utils

  @enforce_keys ~w(bucket path query)a
  defstruct bucket: nil,
            path: nil,
            query: nil,
            input_serialization: nil,
            output_serialization: nil,
            scan_range: nil,
            opts: [],
            service: :s3

  # https://docs.aws.amazon.com/AmazonS3/latest/API/API_SelectObjectContent.html
  def input_params(%{
        csv: csv_input
      }) do
    csv_params =
      %{
        file_header_info: "USE",
        record_delimiter: "\n",
        field_delimiter: ","
      }
      |> Map.merge(csv_input)
      |> Utils.to_xml()

    "<CSV>#{csv_params}</CSV>"
  end

  def input_params(%{
        json: json_input
      }) do
    json_params =
      %{
        type: "DOCUMENT"
      }
      |> Map.merge(json_input)
      |> Utils.to_xml()

    "<JSON>#{json_params}</JSON>"
  end

  def input_params(%{
        parquet: _
      }) do
    "<Parquet></Parquet>"
  end

  def output_params(%{
        csv: csv_output
      }) do
    csv_params =
      %{
        record_delimiter: "\n",
        field_delimiter: ","
      }
      |> Map.merge(csv_output)
      |> Utils.to_xml()

    "<CSV>#{csv_params}</CSV>"
  end

  def output_params(%{
        json: json_output
      }) do
    json_params =
      %{
        record_delimiter: "\n"
      }
      |> Map.merge(json_output)
      |> Utils.to_xml()

    "<JSON>#{json_params}</JSON>"
  end

  def scan_range_params(
        %{
          start: _range_start,
          end: _range_end
        } = scan_range
      ) do
    ExAws.S3.Utils.to_xml(%{
      scan_range: scan_range
    })
  end

  def scan_range_params(_) do
    ""
  end

  def build_payload(
        query,
        input_serialization,
        output_serialization,
        scan_range
      ) do
    """
    <?xml version="1.0" encoding="UTF-8"?>
    <SelectObjectContentRequest xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Expression>#{query}</Expression>
      <ExpressionType>SQL</ExpressionType>
      <InputSerialization>
      #{input_params(input_serialization)}
      </InputSerialization>
      <OutputSerialization>
      #{output_params(output_serialization)}
      </OutputSerialization>
      #{scan_range_params(scan_range)}
    </SelectObjectContentRequest>
    """
  end

  defimpl ExAws.Operation do
    alias ExAws.S3.SelectObjectContents
    alias ExAws.S3.Parsers.EventStream

    def perform(
          _,
          _
        ) do
      raise "Not implemented. Use stream! instead."
    end

    def stream!(
          %{
            bucket: bucket,
            path: path,
            query: query,
            opts: opts
          },
          config
        ) do
      input_serialization = opts[:input_serialization] || %{csv: %{}}
      output_serialization = opts[:output_serialization] || %{csv: %{}}
      scan_range = opts[:scan_range] || nil

      payload =
        SelectObjectContents.build_payload(
          query,
          input_serialization,
          output_serialization,
          scan_range
        )

      params = %{"select" => "", "select-type" => "2"}

      ExAws.stream!(
        %ExAws.Operation.S3{
          http_method: :post,
          bucket: bucket,
          path: path,
          body: payload,
          headers: %{},
          resource: "",
          params: params,
          stream_builder: :octet_stream,
          parser: &EventStream.parse_raw_stream/1
        },
        config
      )
    end
  end
end
