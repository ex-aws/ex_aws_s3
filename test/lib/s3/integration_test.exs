defmodule ExAws.S3IntegrationTest do
  use ExUnit.Case, async: true

  import Support.BypassHelpers

  setup [:start_bypass]

  test "#list_buckets with ExAws.request()", %{bypass: bypass} do
    body = ~S"""
    <?xml version="1.0" encoding="UTF-8"?>
    <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Owner>
        <ID>fcde9916b80a61ee31a082a6a58a7017df4e4e7a9124e3748dbe9ecc414b9cb8</ID>
      </Owner>
      <Buckets>
        <Bucket>
          <Name>ex-aws-s3-test-bucket</Name>
          <CreationDate>2020-06-21T01:32:47.000Z</CreationDate>
        </Bucket>
      </Buckets>
    </ListAllMyBucketsResult>
    """

    Bypass.expect(bypass, fn conn ->
      conn = Plug.Conn.fetch_query_params(conn)

      case conn do
        %{method: "GET", request_path: "/"} ->
          conn
          |> Plug.Conn.put_resp_header("Content-Type", "application/xml")
          |> Plug.Conn.send_resp(200, body)
      end
    end)

    assert {:ok, %{body: body}} =
             ExAws.S3.list_buckets() |> ExAws.request(exaws_config_for_bypass(bypass))

    assert %{buckets: [%{name: "ex-aws-s3-test-bucket"}]} = body
  end

  test "#delete_multiple_objects sends the delete sub-resource in the query string", %{
    bypass: bypass
  } do
    test_pid = self()

    Bypass.expect_once(bypass, fn conn ->
      conn = Plug.Conn.fetch_query_params(conn)
      send(test_pid, {:request, conn.method, conn.request_path, conn.query_params})

      conn
      |> Plug.Conn.put_resp_header("Content-Type", "application/xml")
      |> Plug.Conn.send_resp(
        200,
        ~S(<DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>)
      )
    end)

    ExAws.S3.delete_multiple_objects("bucket", ["foo", "bar"])
    |> ExAws.request!(exaws_config_for_bypass(bypass))

    assert_received {:request, "POST", "/bucket/", %{"delete" => _}}
  end
end
