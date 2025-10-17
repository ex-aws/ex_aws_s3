defmodule ExAws.S3Test do
  # Setting async: false since we're modifying Application env
  use ExUnit.Case, async: false
  alias ExAws.{S3, Operation}

  test "#list_objects" do
    res =
      S3.list_objects(
        "bucket",
        headers: %{"x-amz-request-payer" => "requester"},
        prefix: "/path/to/objs"
      )

    %Operation.S3{
      headers: headers,
      params: params,
      bucket: bucket,
      http_method: http_method
    } = res

    assert headers == %{"x-amz-request-payer" => "requester"}
    assert params == %{"prefix" => "/path/to/objs"}
    assert bucket == "bucket"
    assert http_method == :get
  end

  test "#list_objects_v2" do
    res =
      S3.list_objects_v2(
        "bucket",
        headers: %{"x-amz-request-payer" => "requester"},
        prefix: "/path/to/objs",
        start_after: "/path/to/objs/sub"
      )

    %Operation.S3{
      headers: headers,
      params: params,
      bucket: bucket,
      http_method: http_method
    } = res

    assert headers == %{"x-amz-request-payer" => "requester"}

    assert params == %{
             "prefix" => "/path/to/objs",
             "start-after" => "/path/to/objs/sub",
             "list-type" => 2
           }

    assert bucket == "bucket"
    assert http_method == :get
  end

  test "#get_object" do
    expected = %Operation.S3{
      bucket: "bucket",
      headers: %{"x-amz-server-side-encryption-customer-algorithm" => "md5"},
      params: %{
        "response-content-type" => "application/json",
        "versionId" => "g6qNRfGox7jrBvrs9x28soa0JdEaRwAN"
      },
      path: "object.json",
      http_method: :get
    }

    assert expected ==
             S3.get_object(
               "bucket",
               "object.json",
               response: [content_type: "application/json"],
               encryption: [customer_algorithm: "md5"],
               version_id: "g6qNRfGox7jrBvrs9x28soa0JdEaRwAN"
             )
  end

  test "#put_object" do
    expected = %Operation.S3{
      body: "data",
      bucket: "bucket",
      headers: %{
        "content-encoding" => "application/json",
        "x-amz-acl" => "public-read",
        "x-amz-server-side-encryption" => "AES256",
        "x-amz-storage-class" => "GLACIER_IR",
        "content-md5" => "asdf",
        "x-amz-meta-foo" => "sqiggles"
      },
      path: "object.json",
      http_method: :put
    }

    assert expected ==
             S3.put_object(
               "bucket",
               "object.json",
               "data",
               content_encoding: "application/json",
               storage_class: :glacier_ir,
               content_md5: "asdf",
               acl: :public_read,
               encryption: "AES256",
               meta: [foo: "sqiggles"]
             )
  end

  test "#put_bucket with non-us-east-1 region" do
    region = "not-us-east-1"
    bucket = "new.bucket"

    expected = %Operation.S3{
      body: """
      <CreateBucketConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <LocationConstraint>#{region}</LocationConstraint>
      </CreateBucketConfiguration>
      """,
      bucket: bucket,
      path: "/",
      http_method: :put
    }

    assert expected == S3.put_bucket(bucket, region)
  end

  test "#put_bucket with us-east-1 region" do
    bucket = "new.bucket"

    expected = %Operation.S3{
      body: "",
      bucket: bucket,
      path: "/",
      http_method: :put
    }

    assert expected == S3.put_bucket(bucket, "us-east-1")
  end

  test "#put_bucket with empty region" do
    bucket = "new.bucket"

    expected = %Operation.S3{
      body: "",
      bucket: bucket,
      path: "/",
      http_method: :put
    }

    assert expected == S3.put_bucket(bucket, "")
  end

  test "#put_object_copy" do
    expected = %Operation.S3{
      bucket: "dest-bucket",
      headers: %{
        "x-amz-acl" => "public-read",
        "x-amz-copy-source" => "/src-bucket/src-object",
        "x-amz-server-side-encryption-customer-algorithm" => "md5",
        "x-amz-copy-source-server-side-encryption-customer-algorithm" => "md5",
        "x-amz-meta-foo" => "sqiggles",
        "x-amz-tagging" => "foo=foo&bar=bar",
        "x-amz-tagging-directive" => :REPLACE
      },
      path: "dest-object",
      http_method: :put
    }

    assert expected ==
             S3.put_object_copy(
               "dest-bucket",
               "dest-object",
               "src-bucket",
               "src-object",
               source_encryption: [customer_algorithm: "md5"],
               acl: :public_read,
               destination_encryption: [customer_algorithm: "md5"],
               meta: [foo: "sqiggles"],
               tagging: "foo=foo&bar=bar",
               tagging_directive: :REPLACE
             )
  end

  test "#put_object_copy basic" do
    expected = %Operation.S3{
      bucket: "dest-bucket",
      headers: %{"x-amz-copy-source" => "/src-bucket/src-object"},
      path: "dest-object",
      http_method: :put
    }

    assert expected ==
             S3.put_object_copy("dest-bucket", "dest-object", "src-bucket", "src-object")
  end

  test "#put_object_copy utf8" do
    expected = %Operation.S3{
      bucket: "dest-bucket",
      headers: %{"x-amz-copy-source" => "/src-bucket/foo/%C3%BC.txt"},
      path: "dest-object",
      http_method: :put
    }

    assert expected ==
             S3.put_object_copy("dest-bucket", "dest-object", "src-bucket", "/foo/ü.txt")
  end

  test "#put_object_copy encoding" do
    expected = %Operation.S3{
      bucket: "dest-bucket",
      headers: %{"x-amz-copy-source" => "/src-bucket/foo/hello%2Bfriend.txt"},
      path: "dest-object",
      http_method: :put
    }

    assert expected ==
             S3.put_object_copy(
               "dest-bucket",
               "dest-object",
               "src-bucket",
               "/foo/hello+friend.txt"
             )
  end

  test "#put_object_copy percent encodes spaces" do
    expected = %Operation.S3{
      bucket: "dest-bucket",
      headers: %{"x-amz-copy-source" => "/src-bucket/foo/hello%20friend.txt"},
      path: "dest-object",
      http_method: :put
    }

    assert expected ==
             S3.put_object_copy(
               "dest-bucket",
               "dest-object",
               "src-bucket",
               "/foo/hello friend.txt"
             )
  end

  test "#complete_multipart_upload" do
    expected = %Operation.S3{
      body:
        "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>foo</ETag></Part><Part><PartNumber>2</PartNumber><ETag>bar</ETag></Part></CompleteMultipartUpload>",
      bucket: "bucket",
      params: %{"uploadId" => "upload-id"},
      path: "object",
      http_method: :post,
      parser: &ExAws.S3.Parsers.parse_complete_multipart_upload/1
    }

    assert expected ==
             S3.complete_multipart_upload("bucket", "object", "upload-id", %{
               1 => "foo",
               2 => "bar"
             })
  end

  test "#upload_part_copy" do
    expected = %Operation.S3{
      bucket: "dest-bucket",
      headers: %{
        "x-amz-copy-source" => "/src-bucket/src-object",
        "x-amz-copy-source-range" => "bytes=1-9",
        "x-amz-copy-source-server-side-encryption-customer-algorithm" => "md5"
      },
      params: %{"uploadId" => "upload-id", "partNumber" => 1},
      path: "dest-object",
      http_method: :put,
      parser: &ExAws.S3.Parsers.parse_upload_part_copy/1
    }

    assert expected ==
             S3.upload_part_copy(
               "dest-bucket",
               "dest-object",
               "src-bucket",
               "src-object",
               "upload-id",
               1,
               1..9,
               source_encryption: [customer_algorithm: "md5"]
             )
  end

  test "#delete_object no options" do
    expected = %Operation.S3{
      body: "",
      bucket: "bucket",
      path: "object",
      http_method: :delete,
      resource: ""
    }

    assert expected == S3.delete_object("bucket", "object")
  end

  test "#delete_object object must not be nil" do
    assert_raise RuntimeError, "object must not be nil", fn ->
      S3.delete_object("bucket", nil)
    end
  end

  test "#delete_object object must not be empty string" do
    assert_raise RuntimeError, "object must not be empty string", fn ->
      S3.delete_object("bucket", "")
    end
  end

  test "#delete_object version_id option" do
    expected = %Operation.S3{
      body: "",
      bucket: "bucket",
      http_method: :delete,
      params: %{"versionId" => "1234"},
      headers: %{
        "x-amz-mfa" => "MFA",
        "x-amz-request-payer" => "RequestPayer",
        "x-amz-bypass-governance-retention" => "BypassGovernanceRetention",
        "x-amz-expected-bucket-owner" => "ExpectedBucketOwner"
      },
      path: "object",
      resource: ""
    }

    assert expected ==
             S3.delete_object("bucket", "object",
               version_id: "1234",
               x_amz_mfa: "MFA",
               x_amz_request_payer: "RequestPayer",
               x_amz_bypass_governance_retention: "BypassGovernanceRetention",
               x_amz_expected_bucket_owner: "ExpectedBucketOwner"
             )
  end

  test "#delete_multiple_objects" do
    expected = %Operation.S3{
      body:
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete><Object><Key>foo</Key></Object><Object><Key>bar</Key><VersionId>v1</VersionId></Object><Object><Key>special characters: &apos;&quot;&amp;&lt;&gt;&#13;&#10;</Key></Object></Delete>",
      bucket: "bucket",
      path: "/?delete",
      headers: %{"content-md5" => "G9Pq8w8AQUesREJndxKbKw=="},
      http_method: :post
    }

    assert expected ==
             S3.delete_multiple_objects("bucket", [
               "foo",
               {"bar", "v1"},
               "special characters: '\"&<>\r\n"
             ])
  end

  test "#delete_multiple_objects sha1" do
    set_content_hash_algorithm(:sha)

    expected = %Operation.S3{
      body:
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete><Object><Key>foo</Key></Object><Object><Key>bar</Key><VersionId>v1</VersionId></Object><Object><Key>special characters: &apos;&quot;&amp;&lt;&gt;&#13;&#10;</Key></Object></Delete>",
      bucket: "bucket",
      path: "/?delete",
      headers: %{"x-amz-checksum-sha1" => "1uLbLBmwtufR1/csrQPCAONFeKU="},
      http_method: :post
    }

    assert expected ==
             S3.delete_multiple_objects("bucket", [
               "foo",
               {"bar", "v1"},
               "special characters: '\"&<>\r\n"
             ])

    on_exit(&unset_content_hash_algorithm/0)
  end

  test "#delete_multiple_objects sha256" do
    set_content_hash_algorithm(:sha256)

    expected = %Operation.S3{
      body:
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete><Object><Key>foo</Key></Object><Object><Key>bar</Key><VersionId>v1</VersionId></Object><Object><Key>special characters: &apos;&quot;&amp;&lt;&gt;&#13;&#10;</Key></Object></Delete>",
      bucket: "bucket",
      path: "/?delete",
      headers: %{"x-amz-checksum-sha256" => "Hsce+MZp64Uy8CwyResJ3y13YGw6jDAAdVGFmFPKPSs="},
      http_method: :post
    }

    assert expected ==
             S3.delete_multiple_objects("bucket", [
               "foo",
               {"bar", "v1"},
               "special characters: '\"&<>\r\n"
             ])

    on_exit(&unset_content_hash_algorithm/0)
  end

  test "#post_object_restore" do
    expected = %Operation.S3{
      body:
        "<RestoreRequest xmlns=\"http://s3.amazonaws.com/doc/2006-3-01\">\n  <Days>5</Days>\n</RestoreRequest>\n",
      bucket: "bucket",
      params: %{"versionId" => 123},
      path: "object",
      resource: "restore",
      http_method: :post
    }

    assert expected == S3.post_object_restore("bucket", "object", 5, version_id: 123)
  end

  test "#head_object" do
    expected = %Operation.S3{
      bucket: "bucket",
      headers: %{"x-amz-server-side-encryption-customer-algorithm" => "md5"},
      params: %{"versionId" => 123},
      path: "object",
      http_method: :head
    }

    assert expected ==
             S3.head_object(
               "bucket",
               "object",
               encryption: [customer_algorithm: "md5"],
               version_id: 123
             )
  end

  test "#presigned_url no opts" do
    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "foo.txt")
    assert_pre_signed_url(url, "https://s3.amazonaws.com/bucket/foo.txt", "3600")
  end

  test "#presigned_url respects port in config" do
    {:ok, url} = S3.presigned_url(config(port: 9090), :get, "bucket", "foo.txt")
    assert_pre_signed_url(url, "https://s3.amazonaws.com:9090/bucket/foo.txt", "3600")
  end

  test "#presigned_url passing expires_in option" do
    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "foo.txt", expires_in: 100)
    assert_pre_signed_url(url, "https://s3.amazonaws.com/bucket/foo.txt", "100")
  end

  test "#presigned_url passing virtual_host=false option" do
    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "foo.txt", virtual_host: false)
    assert_pre_signed_url(url, "https://s3.amazonaws.com/bucket/foo.txt", "3600")
  end

  test "#presigned_url passing virtual_host=true option" do
    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "foo.txt", virtual_host: true)
    assert_pre_signed_url(url, "https://bucket.s3.amazonaws.com/foo.txt", "3600")
  end

  test "#presigned_url passing both expires_in and virtual_host options" do
    opts = [expires_in: 100, virtual_host: true]
    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "foo.txt", opts)
    assert_pre_signed_url(url, "https://bucket.s3.amazonaws.com/foo.txt", "100")
  end

  test "#presigned_url passing s3_accelerate=false option" do
    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "foo.txt", s3_accelerate: false)
    assert_pre_signed_url(url, "https://s3.amazonaws.com/bucket/foo.txt", "3600")
  end

  test "#presigned_url passing s3_accelerate=true option" do
    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "foo.txt", s3_accelerate: true)
    assert_pre_signed_url(url, "https://bucket.s3-accelerate.amazonaws.com/foo.txt", "3600")
  end

  test "#presigned_url passing both virtual_host and s3_accelerate options" do
    opts = [virtual_host: false, s3_accelerate: true]
    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "foo.txt", opts)
    assert_pre_signed_url(url, "https://bucket.s3-accelerate.amazonaws.com/foo.txt", "3600")

    opts = [virtual_host: true, s3_accelerate: false]
    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "foo.txt", opts)
    assert_pre_signed_url(url, "https://bucket.s3.amazonaws.com/foo.txt", "3600")
  end

  test "#presigned_url passing both virtual_host and bucket_as_host options" do
    opts = [virtual_host: false, bucket_as_host: true]
    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "foo.txt", opts)
    assert_pre_signed_url(url, "https://s3.amazonaws.com/bucket/foo.txt", "3600")

    opts = [virtual_host: true, bucket_as_host: false]
    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "foo.txt", opts)
    assert_pre_signed_url(url, "https://bucket.s3.amazonaws.com/foo.txt", "3600")

    opts = [virtual_host: true, bucket_as_host: true]
    {:ok, url} = S3.presigned_url(config(), :get, "bucket.custom-domain.com", "foo.txt", opts)
    assert_pre_signed_url(url, "https://bucket.custom-domain.com/foo.txt", "3600")
  end

  test "#presigned_url passing query_params option" do
    query_params = [
      key_one: "value_one",
      key_two: "value_two"
    ]

    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "foo.txt", query_params: query_params)
    uri = URI.parse(url)
    actual_query = URI.query_decoder(uri.query) |> Enum.map(& &1)

    assert [
             {"key_one", "value_one"},
             {"key_two", "value_two"},
             {"X-Amz-Algorithm", "AWS4-HMAC-SHA256"},
             {"X-Amz-Credential", _},
             {"X-Amz-Date", _},
             {"X-Amz-Expires", _},
             {"X-Amz-SignedHeaders", "host"},
             {"X-Amz-Signature", _}
           ] = actual_query
  end

  test "#presigned_url file is path with slash" do
    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "/foo/bar.txt")
    assert_pre_signed_url(url, "https://s3.amazonaws.com/bucket/foo/bar.txt", "3600")
  end

  test "#presigned_url file is key with query params" do
    query_params = %{"d" => "400"}

    {:ok, url} =
      S3.presigned_url(config(), :get, "bucket", "/foo/bar.txt", query_params: query_params)

    assert_pre_signed_url(
      url,
      "https://s3.amazonaws.com/bucket/foo/bar.txt",
      "3600",
      query_params
    )
  end

  test "#presigned_url file is key with embedded query params" do
    {:ok, url} = S3.presigned_url(config(), :get, "bucket", "/foo/bar.txt?d=400")

    assert_pre_signed_url(url, "https://s3.amazonaws.com/bucket/foo/bar.txt", "3600", %{
      "d" => "400"
    })
  end

  test "#presigned_url raises exception on bad expires_in option" do
    opts = [expires_in: 60 * 60 * 24 * 8]
    {:error, reason} = S3.presigned_url(config(), :get, "bucket", "foo.txt", opts)
    assert "expires_in_exceeds_one_week" == reason
  end

  test "#presigned_url respects port configuration" do
    config = ExAws.Config.new(:s3, port: 1234)
    {:ok, url} = S3.presigned_url(config, :get, "bucket", "foo.txt")
    uri = URI.parse(url)
    assert uri.port == 1234
  end

  test "#get_object_tagging" do
    bucket = "my-bucket"
    object = "test.txt"

    expected = %Operation.S3{
      body: "",
      bucket: bucket,
      http_method: :get,
      path: object,
      resource: "tagging",
      parser: &S3.Parsers.parse_object_tagging/1
    }

    assert expected == S3.get_object_tagging(bucket, object)
  end

  test "#put_object_tagging with empty tags" do
    bucket = "my-bucket"
    object = "test.txt"

    expected = %Operation.S3{
      body: ~S|<?xml version="1.0" encoding="UTF-8"?><Tagging><TagSet></TagSet></Tagging>|,
      bucket: bucket,
      http_method: :put,
      path: object,
      resource: "tagging",
      headers: %{"content-md5" => "3z614bAllL7hKml2qps9rg=="}
    }

    assert expected == S3.put_object_tagging(bucket, object, [])
  end

  test "#put_object_tagging" do
    bucket = "my-bucket"
    object = "test.txt"

    expected = %Operation.S3{
      body:
        ~S|<?xml version="1.0" encoding="UTF-8"?><Tagging><TagSet><Tag><Key>test</Key><Value>hello</Value></Tag></TagSet></Tagging>|,
      bucket: bucket,
      http_method: :put,
      path: object,
      resource: "tagging",
      headers: %{"content-md5" => "1TCz8KGUQRyYv1eCE4bRFQ=="}
    }

    assert expected == S3.put_object_tagging(bucket, object, test: "hello")
  end

  test "#put_object_tagging w/ version_id" do
    bucket = "my-bucket"
    object = "test.txt"
    version_id = "GOh7ob90QUq53H4Vd4aacioB6Nt.NoaU"

    assert %Operation.S3{params: %{"versionId" => ^version_id}} =
             S3.put_object_tagging(bucket, object, [test: "hello"], version_id: version_id)
  end

  test "#delete_object_tagging" do
    bucket = "my-bucket"
    object = "test.txt"

    expected = %Operation.S3{
      bucket: bucket,
      http_method: :delete,
      path: object,
      resource: "tagging"
    }

    assert expected == S3.delete_object_tagging(bucket, object)
  end

  test "#presigned_post simple policy" do
    bucket = "my-bucket"
    key = "test.txt"

    post_data = S3.presigned_post(ExAws.Config.new(:s3), bucket, key)

    assert URI.parse(post_data.url).scheme != nil
    assert policy = post_data.fields["Policy"]

    assert {:ok, json} = Base.decode64(policy)
    assert {:ok, policy} = config().json_codec.decode(json)

    conditions = policy["conditions"]

    assert Enum.find(conditions, & &1["key"]) == %{"key" => key}
    assert Enum.find(conditions, & &1["bucket"]) == %{"bucket" => bucket}
  end

  test "#presigned_post custom key" do
    bucket = "my-bucket"
    key = {:starts_with, "prefix/"}

    post_data = S3.presigned_post(ExAws.Config.new(:s3), bucket, nil, key: key)

    assert policy = post_data.fields["Policy"]
    assert {:ok, json} = Base.decode64(policy)
    assert {:ok, policy} = config().json_codec.decode(json)

    conditions = policy["conditions"]

    assert Enum.find(conditions, &is_list(&1)) == ["starts-with", "$key", "prefix/"]
  end

  test "#presigned_post custom policy" do
    bucket = "my-bucket"
    key = "text.jpg"

    post_data =
      S3.presigned_post(ExAws.Config.new(:s3), bucket, key,
        acl: "public-read",
        custom_conditions: [["starts-with", "$Content-Type", "image/"]],
        content_length_range: [10, 20]
      )

    assert policy = post_data.fields["Policy"]
    assert {:ok, json} = Base.decode64(policy)
    assert {:ok, policy} = config().json_codec.decode(json)

    conditions = policy["conditions"]

    assert Enum.find(conditions, &(is_list(&1) && Enum.at(&1, 0) == "starts-with")) == [
             "starts-with",
             "$Content-Type",
             "image/"
           ]

    assert Enum.find(conditions, &(is_list(&1) && Enum.at(&1, 0) == "content-length-range")) == [
             "content-length-range",
             10,
             20
           ]

    assert Enum.find(conditions, &(is_map(&1) && Map.has_key?(&1, "acl"))) == %{
             "acl" => "public-read"
           }
  end

  test "#presigned_post passing virtual_host=false option" do
    %{url: url} = S3.presigned_post(config(), "bucket", "foo.txt", virtual_host: false)
    assert url == "https://s3.amazonaws.com/bucket"
  end

  test "#presigned_post respects port in config" do
    %{url: url} = S3.presigned_post(config(port: 9090), "bucket", "foo.txt")
    assert url == "https://s3.amazonaws.com:9090/bucket"
  end

  test "#presigned_post passing virtual_host=true option" do
    %{url: url} = S3.presigned_post(config(), "bucket", "foo.txt", virtual_host: true)
    assert url == "https://bucket.s3.amazonaws.com"
  end

  test "#presigned_post passing both expires_in and virtual_host options" do
    opts = [expires_in: 100, virtual_host: true]
    %{url: url} = S3.presigned_post(config(), "bucket", "foo.txt", opts)
    assert url == "https://bucket.s3.amazonaws.com"
  end

  test "#presigned_post passing s3_accelerate=false option" do
    %{url: url} = S3.presigned_post(config(), "bucket", "foo.txt", s3_accelerate: false)
    assert url == "https://s3.amazonaws.com/bucket"
  end

  test "#presigned_post passing s3_accelerate=true option" do
    %{url: url} = S3.presigned_post(config(), "bucket", "foo.txt", s3_accelerate: true)
    assert url == "https://bucket.s3-accelerate.amazonaws.com"
  end

  test "#presigned_post passing both virtual_host and s3_accelerate options" do
    opts = [virtual_host: false, s3_accelerate: true]
    %{url: url} = S3.presigned_post(config(), "bucket", "foo.txt", opts)
    assert url == "https://bucket.s3-accelerate.amazonaws.com"

    opts = [virtual_host: true, s3_accelerate: false]
    %{url: url} = S3.presigned_post(config(), "bucket", "foo.txt", opts)
    assert url == "https://bucket.s3.amazonaws.com"
  end

  test "#presigned_post passing both virtual_host and bucket_as_host options" do
    opts = [virtual_host: false, bucket_as_host: true]
    %{url: url} = S3.presigned_post(config(), "bucket", "foo.txt", opts)
    assert url == "https://s3.amazonaws.com/bucket"

    opts = [virtual_host: true, bucket_as_host: false]
    %{url: url} = S3.presigned_post(config(), "bucket", "foo.txt", opts)
    assert url == "https://bucket.s3.amazonaws.com"

    opts = [virtual_host: true, bucket_as_host: true]
    %{url: url} = S3.presigned_post(config(), "bucket.custom-domain.com", "foo.txt", opts)
    assert url == "https://bucket.custom-domain.com"
  end

  describe "Content hash header" do
    test "MD5" do
      body = "hello world"
      content_hash = :crypto.hash(:md5, body) |> Base.encode64()
      expected = %{"content-md5" => content_hash}
      assert ExAws.S3.calculate_content_header("hello world") === expected
    end

    test "SHA1" do
      set_content_hash_algorithm(:sha)

      body = "hello world"
      content_hash = :crypto.hash(:sha, body) |> Base.encode64()
      expected = %{"x-amz-checksum-sha1" => content_hash}
      assert ExAws.S3.calculate_content_header("hello world") === expected

      on_exit(&unset_content_hash_algorithm/0)
    end

    test "SHA256" do
      set_content_hash_algorithm(:sha256)

      body = "hello world"
      content_hash = :crypto.hash(:sha256, body) |> Base.encode64()
      expected = %{"x-amz-checksum-sha256" => content_hash}
      assert ExAws.S3.calculate_content_header("hello world") === expected

      on_exit(&unset_content_hash_algorithm/0)
    end
  end

  defp set_content_hash_algorithm(alg) do
    Application.put_env(:ex_aws_s3, :content_hash_algorithm, alg)
  end

  defp unset_content_hash_algorithm() do
    Application.delete_env(:ex_aws_s3, :content_hash_algorithm)
  end

  @spec assert_pre_signed_url(
          url,
          expected_scheme_host_path,
          expected_expire,
          expected_query_params
        ) :: none()
        when url: binary,
             expected_scheme_host_path: binary,
             expected_expire: pos_integer(),
             expected_query_params: Access.t()
  defp assert_pre_signed_url(
         url,
         expected_scheme_host_path,
         expected_expire,
         expected_query_params \\ []
       ) do
    uri = URI.parse(url)

    assert expected_scheme_host_path ==
             "#{uri.scheme}://#{uri.host}#{if uri.port != 443 do
               ":#{uri.port}"
             else
               ""
             end}#{uri.path}"

    headers = URI.decode_query(uri.query)

    assert %{
             "X-Amz-Algorithm" => "AWS4-HMAC-SHA256",
             "X-Amz-Credential" => _,
             "X-Amz-Date" => _,
             "X-Amz-Expires" => expires,
             "X-Amz-SignedHeaders" => "host",
             "X-Amz-Signature" => _
           } = headers

    assert expires == expected_expire

    for {key, value} <- expected_query_params do
      assert headers[key] == value
    end
  end

  test "#put_bucket_logging basic" do
    expected = %Operation.S3{
      body: """
      <BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <LoggingEnabled>
          <TargetBucket>logs-bucket</TargetBucket>
          <TargetPrefix></TargetPrefix>
        </LoggingEnabled>
      </BucketLoggingStatus>
      """,
      bucket: "my-bucket",
      path: "/",
      http_method: :put,
      resource: "logging",
      headers: %{"content-md5" => "o2HZlksR1Z3YbY6mb7zRyw=="}
    }

    assert expected ==
             S3.put_bucket_logging("my-bucket", target_bucket: "logs-bucket")
  end

  test "#put_bucket_logging with prefix" do
    expected = %Operation.S3{
      body: """
      <BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <LoggingEnabled>
          <TargetBucket>logs-bucket</TargetBucket>
          <TargetPrefix>access-logs/</TargetPrefix>
        </LoggingEnabled>
      </BucketLoggingStatus>
      """,
      bucket: "my-bucket",
      path: "/",
      http_method: :put,
      resource: "logging",
      headers: %{"content-md5" => "WbIGYlFOQ4nv4Zz2e+FOBg=="}
    }

    assert expected ==
             S3.put_bucket_logging("my-bucket",
               target_bucket: "logs-bucket",
               target_prefix: "access-logs/"
             )
  end

  test "#put_bucket_notification simple SNS" do
    expected = %Operation.S3{
      body: """
      <NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <TopicConfiguration>

      <Topic>arn:aws:sns:us-east-1:123456789012:my-topic</Topic>
      <Event>s3:ObjectCreated:*</Event>

      </TopicConfiguration>

      </NotificationConfiguration>
      """,
      bucket: "my-bucket",
      path: "/",
      http_method: :put,
      resource: "notification",
      headers: %{"content-md5" => "Wd3+E8fYhbDjgjyJdikKdg=="}
    }

    assert expected ==
             S3.put_bucket_notification("my-bucket",
               topic_arn: "arn:aws:sns:us-east-1:123456789012:my-topic",
               events: ["s3:ObjectCreated:*"]
             )
  end

  test "#put_bucket_notification simple Lambda with filters" do
    expected = %Operation.S3{
      body:
        "<NotificationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n<LambdaConfiguration>\n\n<LambdaFunctionArn>arn:aws:lambda:us-east-1:123456789012:function:my-func</LambdaFunctionArn>\n<Event>s3:ObjectCreated:Put</Event>\n<Filter>\n  <S3Key>\n    <FilterRule><Name>suffix</Name><Value>.jpg</Value></FilterRule><FilterRule><Name>prefix</Name><Value>uploads/</Value></FilterRule>\n  </S3Key>\n</Filter>\n\n</LambdaConfiguration>\n\n</NotificationConfiguration>\n",
      bucket: "my-bucket",
      path: "/",
      http_method: :put,
      resource: "notification",
      headers: %{"content-md5" => "HzL/gx+q6KdSU0nSXtlGdA=="}
    }

    assert expected ==
             S3.put_bucket_notification("my-bucket",
               lambda_function_arn: "arn:aws:lambda:us-east-1:123456789012:function:my-func",
               events: ["s3:ObjectCreated:Put"],
               prefix: "uploads/",
               suffix: ".jpg"
             )
  end

  test "#put_bucket_replication simple" do
    expected = %Operation.S3{
      body:
        "<ReplicationConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Role>arn:aws:iam::123456789012:role/replication-role</Role>\n  <Rule>\n  <ID>ReplicationRule</ID>\n  <Status>Enabled</Status>\n  \n  <Destination>\n  <Bucket>arn:aws:s3:::backup-bucket</Bucket>\n  <StorageClass>STANDARD_IA</StorageClass>\n  \n  \n</Destination>\n\n</Rule>\n\n</ReplicationConfiguration>\n",
      bucket: "my-bucket",
      path: "/",
      http_method: :put,
      resource: "replication",
      headers: %{"content-md5" => "vzRqSudwA9DkuVqBeXDOJw=="}
    }

    assert expected ==
             S3.put_bucket_replication("my-bucket",
               role: "arn:aws:iam::123456789012:role/replication-role",
               destination_bucket: "arn:aws:s3:::backup-bucket",
               storage_class: "STANDARD_IA"
             )
  end

  test "#put_bucket_tagging basic" do
    expected = %Operation.S3{
      body:
        ~s(<?xml version="1.0" encoding="UTF-8"?><Tagging><TagSet><Tag><Key>Environment</Key><Value>prod</Value></Tag><Tag><Key>Team</Key><Value>data</Value></Tag></TagSet></Tagging>),
      bucket: "my-bucket",
      path: "/",
      http_method: :put,
      resource: "tagging",
      headers: %{"content-md5" => "thygjpJ3gHhMxMDMLh7t2A=="}
    }

    assert expected ==
             S3.put_bucket_tagging("my-bucket", Environment: "prod", Team: "data")
  end

  test "#put_bucket_tagging with map" do
    expected = %Operation.S3{
      body:
        ~s(<?xml version="1.0" encoding="UTF-8"?><Tagging><TagSet><Tag><Key>Environment</Key><Value>test</Value></Tag></TagSet></Tagging>),
      bucket: "my-bucket",
      path: "/",
      http_method: :put,
      resource: "tagging",
      headers: %{"content-md5" => "mlJj1uyX+gM7aKxzlF/qZA=="}
    }

    assert expected == S3.put_bucket_tagging("my-bucket", %{"Environment" => "test"})
  end

  test "#put_bucket_request_payment requester" do
    expected = %Operation.S3{
      body: """
      <RequestPaymentConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Payer>Requester</Payer>
      </RequestPaymentConfiguration>
      """,
      bucket: "my-bucket",
      path: "/",
      http_method: :put,
      resource: "requestPayment",
      headers: %{"content-md5" => "nukorIYg1xaUbcH03PttFQ=="}
    }

    assert expected == S3.put_bucket_request_payment("my-bucket", :requester)
  end

  test "#put_bucket_request_payment bucket_owner" do
    expected = %Operation.S3{
      body: """
      <RequestPaymentConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Payer>BucketOwner</Payer>
      </RequestPaymentConfiguration>
      """,
      bucket: "my-bucket",
      path: "/",
      http_method: :put,
      resource: "requestPayment",
      headers: %{"content-md5" => "neyNxbaCeZYR6lAHS1geAw=="}
    }

    assert expected == S3.put_bucket_request_payment("my-bucket", :bucket_owner)
  end

  test "#put_bucket_website simple" do
    expected = %Operation.S3{
      body:
        "<WebsiteConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n<IndexDocument><Suffix>index.html</Suffix></IndexDocument>\n</WebsiteConfiguration>\n",
      bucket: "my-bucket",
      path: "/",
      http_method: :put,
      resource: "website",
      headers: %{"content-md5" => "Hkz3aiw9OZDoySr1wb2M5Q=="}
    }

    assert expected == S3.put_bucket_website("my-bucket", index_document: "index.html")
  end

  test "#put_bucket_website with error document" do
    expected = %Operation.S3{
      body: """
      <WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <IndexDocument><Suffix>index.html</Suffix></IndexDocument><ErrorDocument><Key>error.html</Key></ErrorDocument>
      </WebsiteConfiguration>
      """,
      bucket: "my-bucket",
      path: "/",
      http_method: :put,
      resource: "website",
      headers: %{"content-md5" => "YeJ6RDZpMLov4hg49HCwkw=="}
    }

    assert expected ==
             S3.put_bucket_website("my-bucket",
               index_document: "index.html",
               error_document: "error.html"
             )
  end

  test "#put_bucket_website redirect all requests" do
    expected = %Operation.S3{
      body: """
      <WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <RedirectAllRequestsTo>
        <HostName>example.com</HostName>
        <Protocol>https</Protocol>
      </RedirectAllRequestsTo>

      </WebsiteConfiguration>
      """,
      bucket: "my-bucket",
      path: "/",
      http_method: :put,
      resource: "website",
      headers: %{"content-md5" => "puljC8kEX1NQssevvG4gmQ=="}
    }

    assert expected ==
             S3.put_bucket_website("my-bucket",
               redirect_all_requests_to: %{host_name: "example.com", protocol: "https"}
             )
  end

  defp config(opts \\ []), do: ExAws.Config.new(:s3, opts)
end
