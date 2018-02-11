defmodule ExAws.S3IntegrationTest do
  use ExUnit.Case, async: true

  import Support.BypassHelpers
  setup [:start_bypass]

  test "#list_buckets" do
    assert {:ok, %{body: body}} = ExAws.S3.list_buckets |> ExAws.request
    assert %{buckets: _} = body
  end

  test "get_object can generate a stream with one chunk", %{bypass: bypass} do
    file = :crypto.strong_rand_bytes(10 * 1024 * 1024)
    given_file_accessible_with_bypass("bucket", "object", file, bypass)

    stream = ExAws.S3.get_object("bucket", "object")
           |> ExAws.stream!(exaws_config_for_bypass(bypass))

    assert [{0, ^file}] = Enum.to_list(stream)
  end

  test "get_object can generate a stream with multiple chunks",
    %{bypass: bypass} do
    file = :crypto.strong_rand_bytes(10 * 1024 * 1024 + 15)
    given_file_accessible_with_bypass("bucket", "object", file, bypass)

    chunks = ExAws.S3.get_object("bucket", "object", chunk_size: 1024*1024)
           |> ExAws.stream!(exaws_config_for_bypass(bypass))
           |> Enum.to_list()

    assert Enum.count(chunks) == 11
    assert Enum.map(chunks, fn {_, c} -> c end) |> Enum.into(<<>>) == file
  end

  defp given_file_accessible_with_bypass(bucket, object, content, bypass) do
    content_size = byte_size(content) |> to_string
    request_path = "/#{bucket}/#{object}"
    Bypass.expect bypass, fn conn ->
      case conn do
        %{method: "HEAD", request_path: ^request_path} ->
          conn
          |> Plug.Conn.put_resp_header("Content-Length", content_size)
          |> Plug.Conn.send_resp(200, "")
        %{method: "GET", req_headers: hdrs, request_path: ^request_path} ->
          {"range", "bytes=" <> range} = List.keyfind(hdrs, "range", 0)
          [from, to] = String.split(range, "-")
          part = :binary.part(content,
            String.to_integer(from),
            String.to_integer(to) - String.to_integer(from) + 1)
          conn
          |> Plug.Conn.send_resp(200, part)
      end
    end
  end

end
