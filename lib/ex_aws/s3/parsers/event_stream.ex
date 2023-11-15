defmodule ExAws.S3.Parsers.EventStream do
  @moduledoc false

  # Parses EventStream messages.

  # AWS encodes EventStream messages in binary as follows:
  # [      prelude     ][     headers   ][    payload    ][   message-crc  ]
  # |<--  12 bytes  -->|<-- variable -->|<-- variable -->|<--  4 bytes  -->|

  # This module parses this information and returns a struct with the prelude, headers and payload.
  # The prelude contains the total length of the message, the length of the headers,
  # the length of the prelude, the CRC of the message, and the length of the payload.

  # Additionally, this module buffers the stream and parses the messages as they come in.
  # Also, stream is transformed such that each item is seperated by line breaks

  # The headers are a map of header names to values.
  # The payload is the actual message data.
  # The message-crc is a CRC32 checksum of the message (excluding the message-crc itself).
  # Refer to https://docs.aws.amazon.com/AmazonS3/latest/API/RESTSelectObjectAppendix.html for more information.

  alias ExAws.S3.Parsers.EventStream.Message
  alias ExAws.S3.Parsers.EventStream.Prelude
  alias ExAws.S3.Parsers.EventStream.Header
  require Logger

  defp buffer_stream(stream) do
    Stream.transform(stream, {nil, <<>>}, &buffer_stream/2)
  end

  defp buffer_stream(chunk, {nil, buffer}) do
    new_buffer = buffer <> chunk
    {:ok, %Prelude{total_length: total_length} = prelude} = Prelude.parse(new_buffer)

    if total_length == byte_size(new_buffer) do
      {:ok, parsed_message} = parse_message(prelude, new_buffer)
      {[parsed_message], {nil, <<>>}}
    else
      buffer_stream(chunk, {prelude, buffer})
    end
  end

  defp buffer_stream(chunk, {%Prelude{total_length: total_length} = prelude, buffer}) do
    new_buffer = buffer <> chunk
    new_buffer_length = byte_size(new_buffer)

    cond do
      new_buffer_length < total_length ->
        # needs more data. put it in the buffer and wait for more
        {[], {prelude, new_buffer}}

      new_buffer_length > total_length ->
        # we have more than one message in the buffer. parse the first one and keep the rest in the buffer
        <<payload::binary-size(total_length), remaining_buffer::binary>> = new_buffer
        {:ok, parsed_message} = parse_message(prelude, payload)
        {[parsed_message], {nil, remaining_buffer}}

      new_buffer_length == total_length ->
        # we have exactly one message in the buffer. parse it and clear the buffer
        <<payload::binary-size(total_length), <<>>::binary>> =
          new_buffer

        {:ok, parsed_message} = parse_message(prelude, payload)
        {[parsed_message], {nil, <<>>}}
    end
  end

  defp chunk_stream_by_linebreaks(stream) do
    Stream.transform(stream, "", fn
      chunk, buffer ->
        case String.split(buffer <> chunk, "\n") do
          lines when length(lines) > 1 ->
            last = Enum.at(lines, -1)
            rest = Enum.slice(lines, 0..-2)
            {rest, last}

          [line] ->
            {[], line}
        end
    end)
  end

  def parse_message(prelude, payload_bytes) do
    with :ok <- Message.verify_message_crc(prelude, payload_bytes),
         {:ok, headers} <- Header.parse(prelude, payload_bytes),
         {:ok, payload} <- Message.parse_payload(prelude, payload_bytes) do
      {:ok, %Message{prelude: prelude, payload: payload, headers: headers}}
    end
  end

  def parse_raw_stream(
        {:ok,
         %{
           stream: stream
         }}
      ) do
    stream
    |> buffer_stream()
    |> Stream.each(&Message.raise_errors!/1)
    |> Stream.filter(&Message.is_record?/1)
    |> Stream.map(&Message.get_payload/1)
    |> chunk_stream_by_linebreaks()
  end

  def parse_raw_stream({:error, {:http_error, _, %{headers: _, status_code: _, stream: stream}}}) do
    stream_error = Enum.into(stream, "")
    raise "Error parsing stream: #{inspect(stream_error)}"
  end

  def parse_raw_stream({:error, error}) do
    raise "Error parsing stream: #{inspect(error)}"
  end
end
