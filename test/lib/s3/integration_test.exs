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
end
