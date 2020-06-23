defmodule ExAws.S3IntegrationTest do
  use ExUnit.Case, async: true

  import Mox

  setup :verify_on_exit!

  test "#list_buckets with ExAws.request()" do
    headers = [
      {"x-amz-id-2",
       "bcjkrRggpJ2cOi3XidPRAyaCNPe4q3rQsFf2wzrT10feAj9wgbws9AGymVzXEVguTR37PPCLL2s="},
      {"x-amz-request-id", "CFF957E0FA89C0B3"},
      {"Date", "Tue, 23 Jun 2020 09:40:37 GMT"},
      {"Content-Type", "application/xml"},
      {"Transfer-Encoding", "chunked"},
      {"Server", "AmazonS3"}
    ]

    body = ~S"""
    <?xml version="1.0" encoding="UTF-8"?>
    <ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Owner>
        <ID>fcde9916b80a61ee31a082a6a58a7017df4e4e7a9124e3748dbe9ecc414b9cb8</ID>
        <DisplayName>ex_aws_s3</DisplayName>
      </Owner>
      <Buckets>
        <Bucket>
          <Name>ex-aws-s3-test-bucket</Name>
          <CreationDate>2020-06-21T01:32:47.000Z</CreationDate>
        </Bucket>
      </Buckets>
    </ListAllMyBucketsResult>
    """

    ExAws.S3.HttpClientMock
    |> expect(:request, fn :get, "https://s3.amazonaws.com/", _body, _headers, _opts ->
      {:ok, %{status_code: 200, headers: headers, body: body}}
    end)

    assert {:ok, %{body: body}} = ExAws.S3.list_buckets() |> ExAws.request()
    assert %{buckets: [%{name: "ex-aws-s3-test-bucket"}]} = body
  end
end
