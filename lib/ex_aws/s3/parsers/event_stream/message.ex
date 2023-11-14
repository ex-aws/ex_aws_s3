defmodule ExAws.S3.Parsers.EventStream.Message do
  @moduledoc false

  # Parses EventStream messages. This module parses this information and returns
  # a struct with the prelude, headers and payload. Also verifies the message CRC.

  alias ExAws.S3.Parsers.EventStream.Prelude
  import Bitwise
  require Logger

  defstruct prelude: nil,
            headers: nil,
            payload: nil

  def verify_message_crc(
        %Prelude{
          crc: prelude_crc,
          payload_length: payload_length,
          headers_length: headers_length
        },
        payload_bytes
      ) do
    <<_::binary-size(8), message_bytes::binary-size(payload_length + headers_length + 4),
      message_crc_bytes::binary-size(4)>> = payload_bytes

    message_crc = :binary.decode_unsigned(message_crc_bytes, :big)
    computed_crc = prelude_crc |> :erlang.crc32(message_bytes) |> band(0xFFFFFFFF)

    if computed_crc == message_crc do
      :ok
    else
      {:error, :message_checksum_mismatch}
    end
  end

  def parse_payload(
        %Prelude{
          prelude_length: prelude_length,
          headers_length: headers_length,
          payload_length: payload_length
        },
        payload_bytes
      ) do
    <<_prelude_headers::binary-size(prelude_length + headers_length),
      message_bytes::binary-size(payload_length), _::binary>> = payload_bytes

    {:ok, message_bytes}
  end

  def is_record?(%__MODULE__{headers: headers}) do
    Map.get(headers, ":event-type") == "Records"
  end

  def get_payload(%__MODULE__{payload: payload}) do
    payload
  end

  def raise_errors!(%__MODULE__{headers: headers}) do
    if Map.get(headers, ":message-type") == "error" do
      raise "Error message received: #{inspect(headers)}"
    end
  end
end
