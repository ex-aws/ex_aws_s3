defmodule ExAws.S3.UploadTest do
  use ExUnit.Case, async: true

  import Support.BypassHelpers
  alias ExAws.S3

  describe "integration test" do
    setup [:start_bypass]

    test "uploading a file with a stream", %{bypass: bypass} do
      file_path = __ENV__.file

      setup_multipart_upload_backend(bypass, self(), "my-bucket", "test.txt")

      {:ok, _} =
        file_path
        |> S3.Upload.stream_file()
        |> S3.upload("my-bucket", "test.txt")
        |> ExAws.request(exaws_config_for_bypass(bypass))

      assert_received {:initiated_upload, _headers}
      assert_received {:chunk_uploaded, _part_number, _headers}
      assert_received :completed_upload
    end

    test "uploading an SSE-C encrypted file with a stream", %{bypass: bypass} do
      encryption = [
        customer_algorithm: "AES256",
        customer_key: Base.encode64(:binary.copy("k", 32)),
        customer_key_md5: Base.encode64(:crypto.hash(:md5, :binary.copy("k", 32)))
      ]

      file_path =
        Path.join(System.tmp_dir!(), "sse-c-upload-#{System.unique_integer([:positive])}")

      on_exit(fn -> File.rm(file_path) end)
      File.write!(file_path, [:binary.copy("a", 5 * 1024 * 1024), "last part"])

      setup_multipart_upload_backend(bypass, self(), "my-bucket", "test.txt")

      assert {:ok, _} =
               file_path
               |> S3.Upload.stream_file()
               |> S3.upload("my-bucket", "test.txt", encryption: encryption)
               |> ExAws.request(exaws_config_for_bypass(bypass))

      assert_received {:initiated_upload, init_headers}
      assert_received {:chunk_uploaded, "1", first_part_headers}
      assert_received {:chunk_uploaded, "2", last_part_headers}
      assert_received :completed_upload

      for headers <- [init_headers, first_part_headers, last_part_headers] do
        assert {"x-amz-server-side-encryption-customer-algorithm", "AES256"} in headers
        assert {"x-amz-server-side-encryption-customer-key", encryption[:customer_key]} in headers

        assert {"x-amz-server-side-encryption-customer-key-md5", encryption[:customer_key_md5]} in headers
      end
    end
  end

  defp setup_multipart_upload_backend(bypass, test_pid, bucket_name, path) do
    request_path = "/#{bucket_name}/#{path}"
    upload_id = "a-very-secret-upload"

    Bypass.expect(bypass, fn conn ->
      conn = Plug.Conn.fetch_query_params(conn)

      case conn do
        %{method: "POST", request_path: ^request_path, query_params: %{"uploadId" => ^upload_id}} ->
          body = """
          <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Location>https://s3-eu-west-1.amazonaws.com/my-bucket/#{path}</Location>
            <Bucket>#{bucket_name}</Bucket>
            <Key>#{path}</Key>
            <ETag>&quot;17fbc0a106abbb6f381aac6e331f2a19-1&quot;</ETag>
          </CompleteMultipartUploadResult>
          """

          send(test_pid, :completed_upload)

          conn
          |> Plug.Conn.send_resp(200, body)

        %{method: "POST", request_path: ^request_path} ->
          body = """
          <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Bucket>#{bucket_name}</Bucket>
            <Key>#{path}</Key>
            <UploadId>#{upload_id}</UploadId>
          </InitiateMultipartUploadResult>
          """

          send(test_pid, {:initiated_upload, conn.req_headers})

          conn
          |> Plug.Conn.send_resp(200, body)

        %{method: "PUT", request_path: ^request_path} ->
          send(test_pid, {:chunk_uploaded, conn.query_params["partNumber"], conn.req_headers})

          conn
          |> Plug.Conn.put_resp_header("ETag", "abc123")
          |> Plug.Conn.send_resp(200, "")
      end
    end)
  end
end
