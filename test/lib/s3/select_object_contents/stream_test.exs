defmodule ExAws.S3.SelectObjectContent.StreamTest do
  use ExUnit.Case, async: true

  import Support.BypassHelpers
  alias ExAws.S3

  describe "integration test" do
    setup [:start_bypass]

    test "stream SelectObjectContent results", %{bypass: bypass} do
      file = "test.csv"
      bucket = "my-bucket"
      setup_select_object_contents_backend(bypass, self(), bucket, file)

      bucket
      |> S3.select_object_content(file, "select * from s3object")
      |> ExAws.stream!(exaws_config_for_bypass(bypass))

      assert_received :fetched_stream
    end
  end

  defp setup_select_object_contents_backend(bypass, test_pid, bucket_name, path) do
    request_path = "/#{bucket_name}/#{path}"

    Bypass.expect(bypass, fn conn ->
      case conn do
        %{method: "POST", request_path: ^request_path, query_string: "select=&select-type=2"} ->
          send(test_pid, :fetched_stream)

          Plug.Conn.send_resp(conn, 200, [])
      end
    end)
  end
end
