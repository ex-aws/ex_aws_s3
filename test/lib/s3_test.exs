defmodule ExAws.S3Test do
  # Setting async: false since we're modifying Application env
  use ExUnit.Case, async: false
  alias ExAws.{S3, Operation}
  import SweetXml, only: [sigil_x: 2, xpath: 2]

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

  test "#upload_part with SSE-C encryption" do
    encryption = [
      customer_algorithm: "AES256",
      customer_key: "customer-key",
      customer_key_md5: "customer-key-md5"
    ]

    op = S3.upload_part("bucket", "object", "upload-id", 1, "data", encryption: encryption)

    assert op.headers == %{
             "x-amz-server-side-encryption-customer-algorithm" => "AES256",
             "x-amz-server-side-encryption-customer-key" => "customer-key",
             "x-amz-server-side-encryption-customer-key-md5" => "customer-key-md5"
           }

    assert op.params == %{"uploadId" => "upload-id", "partNumber" => 1}
    assert op.body == "data"
  end

  test "#upload_part does not forward initiation-only options" do
    for encryption <- ["AES256", [aws_kms_key_id: "kms-key"]] do
      op =
        S3.upload_part("bucket", "object", "upload-id", 1, "data",
          encryption: encryption,
          content_type: "text/plain",
          acl: :private,
          meta: [foo: "bar"]
        )

      assert op.headers == %{}
    end

    assert S3.upload_part("bucket", "object", "upload-id", 1, "data").headers == %{}
  end

  test "#upload_part with expect header" do
    op = S3.upload_part("bucket", "object", "upload-id", 1, "data", expect: "100-continue")

    assert op.headers == %{"expect" => "100-continue"}
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

  test "#put_object_copy percent encodes reserved characters" do
    expected = %Operation.S3{
      bucket: "dest-bucket",
      headers: %{"x-amz-copy-source" => "/src-bucket/evil%20key%3Fx%3D1%26y%3D2"},
      path: "dest-object",
      http_method: :put
    }

    assert expected ==
             S3.put_object_copy("dest-bucket", "dest-object", "src-bucket", "evil key?x=1&y=2")
  end

  test "#put_object_copy percent encodes CR/LF (no header injection)" do
    expected = %Operation.S3{
      bucket: "dest-bucket",
      headers: %{"x-amz-copy-source" => "/src-bucket/a%0D%0Ax-injected%3A%20pwned"},
      path: "dest-object",
      http_method: :put
    }

    assert expected ==
             S3.put_object_copy(
               "dest-bucket",
               "dest-object",
               "src-bucket",
               "a\r\nx-injected: pwned"
             )
  end

  describe "x-amz-copy-source key handling" do
    defp put_copy_source(src_bucket, src_object) do
      S3.put_object_copy("dest-bucket", "dest-object", src_bucket, src_object).headers[
        "x-amz-copy-source"
      ]
    end

    defp part_copy_source(src_bucket, src_object) do
      S3.upload_part_copy(
        "dest-bucket",
        "dest-object",
        src_bucket,
        src_object,
        "upload-id",
        1,
        1..9
      ).headers["x-amz-copy-source"]
    end

    test "empty key segments are kept" do
      for {key, expected} <- [
            {"a//b", "/src-bucket/a//b"},
            {"dir/", "/src-bucket/dir/"},
            {"a/b//", "/src-bucket/a/b//"}
          ] do
        assert put_copy_source("src-bucket", key) == expected
        assert part_copy_source("src-bucket", key) == expected
      end
    end

    test "only one leading slash is dropped" do
      for {key, expected} <- [
            {"/admin/secret", "/src-bucket/admin/secret"},
            {"//admin/secret", "/src-bucket//admin/secret"}
          ] do
        assert put_copy_source("src-bucket", key) == expected
        assert part_copy_source("src-bucket", key) == expected
      end
    end

    test "dot segments are percent encoded" do
      for {key, expected} <- [
            {"a/../b", "/src-bucket/a/%2E%2E/b"},
            {"../other-bucket/key", "/src-bucket/%2E%2E/other-bucket/key"},
            {"./a/.", "/src-bucket/%2E/a/%2E"},
            {"a/.hidden/v1.2..txt", "/src-bucket/a/.hidden/v1.2..txt"}
          ] do
        assert put_copy_source("src-bucket", key) == expected
        assert part_copy_source("src-bucket", key) == expected
      end

      assert part_copy_source("..", "key") == "/%2E%2E/key"
    end

    test "the bucket cannot add path segments" do
      assert put_copy_source("src-bucket/other", "key") == "/src-bucket%2Fother/key"
      assert part_copy_source("src-bucket/other", "key") == "/src-bucket%2Fother/key"
    end

    test "charlist and atom arguments are still accepted" do
      assert part_copy_source(~c"src-bucket", ~c"foo/hello friend.txt") ==
               "/src-bucket/foo/hello%20friend.txt"

      assert part_copy_source(:src_bucket, "key") == "/src_bucket/key"
      assert put_copy_source(~c"src-bucket", ~c"key") == "/src-bucket/key"
    end
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

  test "#upload_part_copy utf8" do
    expected = %Operation.S3{
      bucket: "dest-bucket",
      headers: %{
        "x-amz-copy-source" => "/src-bucket/foo/%C3%BC.txt",
        "x-amz-copy-source-range" => "bytes=1-9"
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
               "/foo/ü.txt",
               "upload-id",
               1,
               1..9
             )
  end

  test "#upload_part_copy encoding" do
    expected = %Operation.S3{
      bucket: "dest-bucket",
      headers: %{
        "x-amz-copy-source" => "/src-bucket/foo/hello%2Bfriend.txt",
        "x-amz-copy-source-range" => "bytes=1-9"
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
               "/foo/hello+friend.txt",
               "upload-id",
               1,
               1..9
             )
  end

  test "#upload_part_copy percent encodes spaces" do
    expected = %Operation.S3{
      bucket: "dest-bucket",
      headers: %{
        "x-amz-copy-source" => "/src-bucket/foo/hello%20friend.txt",
        "x-amz-copy-source-range" => "bytes=1-9"
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
               "/foo/hello friend.txt",
               "upload-id",
               1,
               1..9
             )
  end

  test "#upload_part_copy percent encodes reserved characters" do
    expected = %Operation.S3{
      bucket: "dest-bucket",
      headers: %{
        "x-amz-copy-source" => "/src-bucket/evil%20key%3Fx%3D1%26y%3D2",
        "x-amz-copy-source-range" => "bytes=1-9"
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
               "evil key?x=1&y=2",
               "upload-id",
               1,
               1..9
             )
  end

  test "#upload_part_copy percent encodes CR/LF (no header injection)" do
    expected = %Operation.S3{
      bucket: "dest-bucket",
      headers: %{
        "x-amz-copy-source" => "/src-bucket/a%0D%0Ax-injected%3A%20pwned",
        "x-amz-copy-source-range" => "bytes=1-9"
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
               "a\r\nx-injected: pwned",
               "upload-id",
               1,
               1..9
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

  test "#put_bucket_lifecycle escapes caller-supplied values (GHSA-pcgv-phrm-r85v)" do
    payload =
      "frontend</Value></Tag></Filter>" <>
        "<Expiration><Days>1</Days></Expiration>" <>
        "<Filter><Tag><Key>team</Key><Value>frontend"

    rule = %{id: "tenant-rule", enabled: true, filter: %{tags: [{"team", payload}]}, actions: %{}}

    %Operation.S3{body: body, bucket: "bucket", http_method: :put, resource: "lifecycle"} =
      S3.put_bucket_lifecycle("bucket", [rule])

    refute body =~ "<Expiration>"

    assert body ==
             "<LifecycleConfiguration><Rule><Filter><Tag><Key>team</Key><Value>frontend&lt;/Value&gt;&lt;/Tag&gt;&lt;/Filter&gt;&lt;Expiration&gt;&lt;Days&gt;1&lt;/Days&gt;&lt;/Expiration&gt;&lt;Filter&gt;&lt;Tag&gt;&lt;Key&gt;team&lt;/Key&gt;&lt;Value&gt;frontend</Value></Tag></Filter><Status>Enabled</Status><ID>tenant-rule</ID></Rule></LifecycleConfiguration>"
  end

  describe "XML escaping of caller-supplied values" do
    # A value containing every character the escaper handles. After escaping it must
    # appear in the body as inert text, and an XML parser must read it back verbatim.
    @raw ~s(a<b>&"c'd)
    @escaped "a&lt;b&gt;&amp;&quot;c&apos;d"

    defp texts(body, path), do: xpath(body, ~x"#{path}/text()"ls)

    test "#put_bucket escapes the region" do
      %Operation.S3{body: body} = S3.put_bucket("bucket", @raw)

      refute body =~ "<b>"
      assert body =~ "<LocationConstraint>#{@escaped}</LocationConstraint>"
      assert texts(body, "//LocationConstraint") == [@raw]
    end

    test "#put_bucket_cors escapes rule values" do
      rule = %{
        allowed_origins: [@raw],
        allowed_methods: [@raw],
        allowed_headers: [@raw],
        exposed_headers: [@raw],
        max_age_seconds: @raw
      }

      %Operation.S3{body: body} = S3.put_bucket_cors("bucket", [rule])

      refute body =~ "<b>"

      assert body ==
               "<CORSConfiguration><CORSRule>" <>
                 "<MaxAgeSeconds>#{@escaped}</MaxAgeSeconds>" <>
                 "<AllowedOrigin>#{@escaped}</AllowedOrigin>" <>
                 "<AllowedMethod>#{@escaped}</AllowedMethod>" <>
                 "<AllowedHeader>#{@escaped}</AllowedHeader>" <>
                 "<ExposeHeader>#{@escaped}</ExposeHeader>" <>
                 "</CORSRule></CORSConfiguration>"
    end

    test "#put_bucket_logging escapes target bucket and prefix" do
      %Operation.S3{body: body} =
        S3.put_bucket_logging("bucket", target_bucket: @raw, target_prefix: @raw)

      refute body =~ "<b>"
      assert texts(body, "//LoggingEnabled/TargetBucket") == [@raw]
      assert texts(body, "//LoggingEnabled/TargetPrefix") == [@raw]
    end

    test "#put_bucket_notification escapes ids, ARNs, events and filter rules" do
      config = %{
        topic_configurations: [
          %{
            id: @raw,
            topic_arn: @raw,
            events: [@raw],
            filter: %{key: %{filter_rules: [%{name: @raw, value: @raw}]}}
          }
        ],
        lambda_configurations: [%{id: @raw, lambda_function_arn: @raw, events: [@raw]}],
        queue_configurations: [%{id: @raw, queue_arn: @raw, events: [@raw]}]
      }

      %Operation.S3{body: body} = S3.put_bucket_notification("bucket", config)

      refute body =~ "<b>"
      assert texts(body, "//TopicConfiguration/Id") == [@raw]
      assert texts(body, "//TopicConfiguration/Topic") == [@raw]
      assert texts(body, "//TopicConfiguration/Event") == [@raw]
      assert texts(body, "//TopicConfiguration/Filter/S3Key/FilterRule/Name") == [@raw]
      assert texts(body, "//TopicConfiguration/Filter/S3Key/FilterRule/Value") == [@raw]
      assert texts(body, "//LambdaConfiguration/Id") == [@raw]
      assert texts(body, "//LambdaConfiguration/LambdaFunctionArn") == [@raw]
      assert texts(body, "//LambdaConfiguration/Event") == [@raw]
      assert texts(body, "//QueueConfiguration/Id") == [@raw]
      assert texts(body, "//QueueConfiguration/Queue") == [@raw]
      assert texts(body, "//QueueConfiguration/Event") == [@raw]
    end

    test "#put_bucket_notification escapes simple-format prefix and suffix" do
      %Operation.S3{body: body} =
        S3.put_bucket_notification("bucket",
          topic_arn: @raw,
          id: @raw,
          events: [@raw],
          prefix: @raw,
          suffix: @raw
        )

      refute body =~ "<b>"
      assert texts(body, "//TopicConfiguration/Topic") == [@raw]
      assert texts(body, "//TopicConfiguration/Id") == [@raw]
      assert texts(body, "//TopicConfiguration/Event") == [@raw]
      assert texts(body, "//FilterRule/Value") == [@raw, @raw]
    end

    test "#put_bucket_replication escapes role, rule and destination values" do
      config = %{
        role: @raw,
        rules: [
          %{
            id: @raw,
            status: @raw,
            filter: %{and: %{prefix: @raw, tags: [%{key: @raw, value: @raw}]}},
            destination: %{
              bucket: @raw,
              storage_class: @raw,
              account: @raw,
              access_control_translation: %{owner: @raw}
            }
          },
          %{filter: %{tag: %{key: @raw, value: @raw}}, destination: %{bucket: @raw}},
          %{filter: %{prefix: @raw}, destination: %{bucket: @raw}}
        ]
      }

      %Operation.S3{body: body} = S3.put_bucket_replication("bucket", config)

      refute body =~ "<b>"
      assert texts(body, "//ReplicationConfiguration/Role") == [@raw]
      assert texts(body, "//Rule/ID") == [@raw]
      assert texts(body, "//Rule/Status") == [@raw, "Enabled", "Enabled"]
      assert texts(body, "//Rule/Filter/And/Prefix") == [@raw]
      assert texts(body, "//Rule/Filter/And/Tag/Key") == [@raw]
      assert texts(body, "//Rule/Filter/And/Tag/Value") == [@raw]
      assert texts(body, "//Rule/Filter/Tag/Key") == [@raw]
      assert texts(body, "//Rule/Filter/Tag/Value") == [@raw]
      assert texts(body, "//Rule/Filter/Prefix") == [@raw]
      assert texts(body, "//Destination/Bucket") == [@raw, @raw, @raw]
      assert texts(body, "//Destination/StorageClass") == [@raw]
      assert texts(body, "//Destination/Account") == [@raw]
      assert texts(body, "//Destination/AccessControlTranslation/Owner") == [@raw]
    end

    test "#put_bucket_replication escapes simple-format values" do
      %Operation.S3{body: body} =
        S3.put_bucket_replication("bucket",
          role: @raw,
          destination_bucket: @raw,
          storage_class: @raw,
          prefix: @raw
        )

      refute body =~ "<b>"
      assert texts(body, "//ReplicationConfiguration/Role") == [@raw]
      assert texts(body, "//Rule/Filter/Prefix") == [@raw]
      assert texts(body, "//Destination/Bucket") == [@raw]
      assert texts(body, "//Destination/StorageClass") == [@raw]
    end

    test "#put_bucket_tagging escapes keys and values" do
      %Operation.S3{body: body} = S3.put_bucket_tagging("bucket", %{@raw => @raw})

      assert body ==
               ~s(<?xml version="1.0" encoding="UTF-8"?><Tagging><TagSet>) <>
                 "<Tag><Key>#{@escaped}</Key><Value>#{@escaped}</Value></Tag>" <>
                 "</TagSet></Tagging>"
    end

    test "#put_object_tagging escapes keys and values" do
      %Operation.S3{body: body} = S3.put_object_tagging("bucket", "object", %{@raw => @raw})

      assert body ==
               ~s(<?xml version="1.0" encoding="UTF-8"?><Tagging><TagSet>) <>
                 "<Tag><Key>#{@escaped}</Key><Value>#{@escaped}</Value></Tag>" <>
                 "</TagSet></Tagging>"
    end

    test "#put_bucket_website escapes redirect-all values" do
      %Operation.S3{body: body} =
        S3.put_bucket_website("bucket",
          redirect_all_requests_to: %{host_name: @raw, protocol: @raw}
        )

      refute body =~ "<b>"
      assert texts(body, "//RedirectAllRequestsTo/HostName") == [@raw]
      assert texts(body, "//RedirectAllRequestsTo/Protocol") == [@raw]
    end

    test "#put_bucket_website escapes documents and routing rules" do
      config = %{
        index_document: @raw,
        error_document: @raw,
        routing_rules: [
          %{
            condition: %{key_prefix_equals: @raw},
            redirect: %{
              protocol: @raw,
              host_name: @raw,
              replace_key_prefix_with: @raw,
              replace_key_with: @raw,
              http_redirect_code: @raw
            }
          },
          %{condition: %{http_error_code_returned_equals: @raw}, redirect: %{}}
        ]
      }

      %Operation.S3{body: body} = S3.put_bucket_website("bucket", config)

      refute body =~ "<b>"
      assert texts(body, "//IndexDocument/Suffix") == [@raw]
      assert texts(body, "//ErrorDocument/Key") == [@raw]
      assert texts(body, "//RoutingRule/Condition/KeyPrefixEquals") == [@raw]
      assert texts(body, "//RoutingRule/Condition/HttpErrorCodeReturnedEquals") == [@raw]
      assert texts(body, "//RoutingRule/Redirect/Protocol") == [@raw]
      assert texts(body, "//RoutingRule/Redirect/HostName") == [@raw]
      assert texts(body, "//RoutingRule/Redirect/ReplaceKeyPrefixWith") == [@raw]
      assert texts(body, "//RoutingRule/Redirect/ReplaceKeyWith") == [@raw]
      assert texts(body, "//RoutingRule/Redirect/HttpRedirectCode") == [@raw]
    end

    test "#put_bucket_logging does not let a target bucket inject a node (GHSA-cf93-7gwh-6w98)" do
      payload = "tenant-logs</TargetBucket><TargetBucket>attacker-exfil-bucket"

      %Operation.S3{body: body} = S3.put_bucket_logging("bucket", target_bucket: payload)

      assert texts(body, "//TargetBucket") == [payload]
      refute "attacker-exfil-bucket" in texts(body, "//TargetBucket")
    end

    test "tagging does not let a tag value inject a tag (GHSA-979q-5hj9-vvvv)" do
      payload = "blue</Value></Tag><Tag><Key>owner</Key><Value>admin"

      for %Operation.S3{body: body} <- [
            S3.put_object_tagging("bucket", "object", [{"customer", payload}]),
            S3.put_bucket_tagging("bucket", [{"customer", payload}])
          ] do
        assert texts(body, "//TagSet/Tag/Key") == ["customer"]
        assert texts(body, "//TagSet/Tag/Value") == [payload]
      end
    end

    test "#delete_multiple_objects escapes version ids" do
      %Operation.S3{body: body} = S3.delete_multiple_objects("bucket", [{"key", @raw}])

      refute body =~ "<b>"
      assert body =~ "<VersionId>#{@escaped}</VersionId>"
      assert texts(body, "//Object/VersionId") == [@raw]
    end

    test "#delete_multiple_objects does not let a version id inject objects (GHSA-mwxg-gfj8-x32v)" do
      payload =
        "v1</VersionId></Object>" <>
          "<Object><Key>important-file</Key></Object>" <>
          "<Object><Key>x</Key><VersionId>v2"

      %Operation.S3{body: body} =
        S3.delete_multiple_objects("bucket", [{"my-own-file", payload}])

      assert texts(body, "//Delete/Object/Key") == ["my-own-file"]
      assert texts(body, "//Delete/Object/VersionId") == [payload]
    end

    test "#complete_multipart_upload escapes etags" do
      %Operation.S3{body: body} =
        S3.complete_multipart_upload("bucket", "object", "upload-id", [{1, @raw}])

      refute body =~ "<b>"
      assert body =~ "<ETag>#{@escaped}</ETag>"
      assert texts(body, "//Part/ETag") == [@raw]
    end
  end

  test "#delete_multiple_objects" do
    expected = %Operation.S3{
      body:
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Delete><Object><Key>foo</Key></Object><Object><Key>bar</Key><VersionId>v1</VersionId></Object><Object><Key>special characters: &apos;&quot;&amp;&lt;&gt;&#13;&#10;</Key></Object></Delete>",
      bucket: "bucket",
      path: "/",
      resource: "delete",
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
      path: "/",
      resource: "delete",
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
      path: "/",
      resource: "delete",
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

  describe "#presigned_url with ? in the object key (GHSA-96q9-mcc7-hqj8)" do
    # Keeping the `?` in the key needs an ex_aws release whose
    # `ExAws.Auth.presigned_url/9` accepts a `%URI{}`. With an older ex_aws such
    # a key is refused instead of being presigned for a different object.
    defp presigned_path(http_method, bucket, object, opts \\ []) do
      {:ok, url} = S3.presigned_url(config(), http_method, bucket, object, opts)
      [base, query] = String.split(url, "?", parts: 2)
      {base, URI.decode_query(query)}
    end

    defp ex_aws_keeps_question_mark? do
      match?({:ok, _url}, S3.presigned_url(config(), :get, "bucket", "a?b"))
    rescue
      ArgumentError -> false
    end

    test "a ? in the key is not turned into query params" do
      if ex_aws_keeps_question_mark?() do
        {base, query} = presigned_path(:get, "bucket", "/foo/bar.txt?d=400")
        assert base == "https://s3.amazonaws.com/bucket/foo/bar.txt%3Fd%3D400"
        refute Map.has_key?(query, "d")

        {base, query} = presigned_path(:put, "victim-bucket", "report.pdf?acl")
        assert base == "https://s3.amazonaws.com/victim-bucket/report.pdf%3Facl"
        refute Map.has_key?(query, "acl")

        {base, query} = presigned_path(:get, "victim-bucket", "export.dump?.txt")
        assert base == "https://s3.amazonaws.com/victim-bucket/export.dump%3F.txt"
        refute Map.has_key?(query, ".txt")
      else
        for key <- ["/foo/bar.txt?d=400", "report.pdf?acl", "export.dump?.txt"] do
          assert_raise ArgumentError, ~r/GHSA-96q9-mcc7-hqj8/, fn ->
            S3.presigned_url(config(), :get, "bucket", key)
          end
        end
      end
    end

    test "a ? in the key is kept with virtual host addressing" do
      for opts <- [[virtual_host: true], [s3_accelerate: true]] do
        if ex_aws_keeps_question_mark?() do
          {base, query} = presigned_path(:put, "victim-bucket", "report.pdf?acl", opts)
          assert String.ends_with?(base, "amazonaws.com/report.pdf%3Facl")
          assert URI.parse(base).host =~ ~r/^victim-bucket\.s3(-accelerate)?\.amazonaws\.com$/
          refute Map.has_key?(query, "acl")
        else
          assert_raise ArgumentError, ~r/GHSA-96q9-mcc7-hqj8/, fn ->
            S3.presigned_url(config(), :put, "victim-bucket", "report.pdf?acl", opts)
          end
        end
      end
    end

    test "a ? in a path style bucket name does not start the query" do
      if ex_aws_keeps_question_mark?() do
        {base, query} = presigned_path(:get, "photos?policy", "obj.txt")
        assert base == "https://s3.amazonaws.com/photos%3Fpolicy/obj.txt"
        refute Map.has_key?(query, "policy/obj.txt")
      else
        assert_raise ArgumentError, ~r/GHSA-96q9-mcc7-hqj8/, fn ->
          S3.presigned_url(config(), :get, "photos?policy", "obj.txt")
        end
      end
    end

    test "query params are passed with the :query_params option" do
      opts = [query_params: [{"versionId", "1"}]]

      if ex_aws_keeps_question_mark?() do
        {base, query} = presigned_path(:get, "bucket", "a?b.txt", opts)
        assert base == "https://s3.amazonaws.com/bucket/a%3Fb.txt"
        assert query["versionId"] == "1"
      end

      {base, query} = presigned_path(:get, "bucket", "a.txt", opts)
      assert base == "https://s3.amazonaws.com/bucket/a.txt"
      assert query["versionId"] == "1"
    end

    test "keys without ? are presigned as before" do
      {base, _query} = presigned_path(:get, "bucket", "dir/a b+c#d%.txt")
      assert base == "https://s3.amazonaws.com/bucket/dir/a%20b%2Bc%23d%25.txt"

      config = ExAws.Config.new(:s3, port: 9000, scheme: "http://", host: "localhost")
      {:ok, url} = S3.presigned_url(config, :get, "bucket", "a b.txt")
      assert String.starts_with?(url, "http://localhost:9000/bucket/a%20b.txt?")
    end
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

  test "#put_object_acl with explicit grants" do
    res = S3.put_object_acl("bucket", "object", grant_read: [email: "foo@bar.com", id: "fake_id"])

    assert res.headers == %{"x-amz-grant-read" => ~s(emailAddress="foo@bar.com", id="fake_id")}
    assert res.resource == "acl"
    assert res.http_method == :put
  end

  test "grantee injection is rejected by every operation that accepts grants" do
    injected = [
      email: ~s(victim@example.com", uri="http://acs.amazonaws.com/groups/global/AllUsers)
    ]

    operations = [
      fn -> S3.put_object_acl("bucket", "object", grant_read: injected) end,
      fn -> S3.put_bucket_acl("bucket", grant_full_control: injected) end,
      fn -> S3.put_bucket("bucket", "us-east-1", grant_write: injected) end,
      fn -> S3.put_object("bucket", "object", "body", grant_read_acp: injected) end,
      fn -> S3.put_object_copy("dest", "object", "src", "object", grant_write_acp: injected) end,
      fn -> S3.initiate_multipart_upload("bucket", "object", grant_full_control: injected) end
    ]

    for operation <- operations do
      assert_raise ArgumentError, ~r/invalid S3 grantee/, operation
    end
  end

  describe "bucket names used as the request host" do
    @malicious_bucket "attacker.example/x?"

    test "#presigned_url with virtual_host rejects a bucket name that would change the host" do
      assert_raise ArgumentError, ~r/invalid bucket name/, fn ->
        S3.presigned_url(config(), :get, @malicious_bucket, "foo.txt", virtual_host: true)
      end
    end

    test "#presigned_url with s3_accelerate rejects a bucket name that would change the host" do
      assert_raise ArgumentError, ~r/invalid bucket name/, fn ->
        S3.presigned_url(config(), :get, @malicious_bucket, "foo.txt", s3_accelerate: true)
      end
    end

    test "#presigned_url with bucket_as_host rejects a bucket that is not a plain hostname" do
      assert_raise ArgumentError, ~r/invalid bucket name/, fn ->
        S3.presigned_url(config(), :put, @malicious_bucket, "foo.txt",
          virtual_host: true,
          bucket_as_host: true
        )
      end
    end

    test "#presigned_post with virtual_host rejects a bucket name that would change the host" do
      assert_raise ArgumentError, ~r/invalid bucket name/, fn ->
        S3.presigned_post(config(), @malicious_bucket, "foo.txt", virtual_host: true)
      end
    end

    test "#presigned_url with bucket_as_host and no allowlist accepts any hostname" do
      {:ok, url} =
        S3.presigned_url(config(), :get, "mybucket.example.com", "foo.txt",
          virtual_host: true,
          bucket_as_host: true
        )

      assert_pre_signed_url(url, "https://mybucket.example.com/foo.txt", "3600")
    end

    test "#presigned_url with bucket_as_host only accepts hosts in the allowlist option" do
      opts = [
        virtual_host: true,
        bucket_as_host: true,
        bucket_as_host_allowlist: ["assets.example.com", "media.example.com"]
      ]

      for bucket <- ["assets.example.com", "MEDIA.example.com"] do
        {:ok, url} = S3.presigned_url(config(), :get, bucket, "foo.txt", opts)
        assert_pre_signed_url(url, "https://#{bucket}/foo.txt", "3600")
      end

      for bucket <- [
            "attacker.example",
            "169.254.169.254",
            "localhost",
            "assets.example.com.",
            "assets.example.com.attacker.example",
            "xassets.example.com"
          ] do
        assert_raise ArgumentError, ~r/not in the configured `bucket_as_host_allowlist`/, fn ->
          S3.presigned_url(config(), :put, bucket, "foo.txt", opts)
        end
      end
    end

    test "#presigned_post with bucket_as_host only accepts hosts in the allowlist option" do
      opts = [
        virtual_host: true,
        bucket_as_host: true,
        bucket_as_host_allowlist: ["assets.example.com"]
      ]

      assert %{url: "https://assets.example.com"} =
               S3.presigned_post(config(), "assets.example.com", "foo.txt", opts)

      assert_raise ArgumentError, ~r/not in the configured `bucket_as_host_allowlist`/, fn ->
        S3.presigned_post(config(), "attacker.example", "foo.txt", opts)
      end
    end

    test "bucket_as_host falls back to the allowlist in the S3 config" do
      config = Map.put(config(), :bucket_as_host_allowlist, ["assets.example.com"])
      opts = [virtual_host: true, bucket_as_host: true]

      assert {:ok, _url} = S3.presigned_url(config, :get, "assets.example.com", "foo.txt", opts)

      assert_raise ArgumentError, ~r/not in the configured `bucket_as_host_allowlist`/, fn ->
        S3.presigned_url(config, :get, "attacker.example", "foo.txt", opts)
      end

      assert_raise ArgumentError, ~r/not in the configured `bucket_as_host_allowlist`/, fn ->
        S3.presigned_post(config, "attacker.example", "foo.txt", opts)
      end
    end

    test "the allowlist option takes precedence over the S3 config" do
      config = Map.put(config(), :bucket_as_host_allowlist, ["assets.example.com"])

      opts = [
        virtual_host: true,
        bucket_as_host: true,
        bucket_as_host_allowlist: ["media.example.com"]
      ]

      assert {:ok, _url} = S3.presigned_url(config, :get, "media.example.com", "foo.txt", opts)

      assert_raise ArgumentError, ~r/not in the configured `bucket_as_host_allowlist`/, fn ->
        S3.presigned_url(config, :get, "assets.example.com", "foo.txt", opts)
      end
    end

    test "a nil allowlist option does not switch off the allowlist in the S3 config" do
      config = Map.put(config(), :bucket_as_host_allowlist, ["assets.example.com"])
      opts = [virtual_host: true, bucket_as_host: true, bucket_as_host_allowlist: nil]

      assert {:ok, _url} = S3.presigned_url(config, :get, "assets.example.com", "foo.txt", opts)

      assert_raise ArgumentError, ~r/not in the configured `bucket_as_host_allowlist`/, fn ->
        S3.presigned_url(config, :get, "attacker.example", "foo.txt", opts)
      end
    end

    test "the allowlist in the S3 config survives ExAws.Config.new/2" do
      # Needs an ex_aws release with the GHSA-rcv9-2jx2-63xh fix. Older releases
      # collapse the list to its first element, which fails closed (see below).
      config =
        config(bucket_as_host_allowlist: ["assets.example.com", "media.example.com"])

      opts = [virtual_host: true, bucket_as_host: true]

      if is_list(config[:bucket_as_host_allowlist]) do
        assert {:ok, _url} = S3.presigned_url(config, :get, "media.example.com", "foo.txt", opts)

        assert_raise ArgumentError, ~r/not in the configured `bucket_as_host_allowlist`/, fn ->
          S3.presigned_url(config, :get, "attacker.example", "foo.txt", opts)
        end
      else
        assert_raise ArgumentError, ~r/expected a list of hostnames/, fn ->
          S3.presigned_url(config, :get, "media.example.com", "foo.txt", opts)
        end
      end
    end

    test "atom bucket names are checked as strings" do
      {:ok, url} = S3.presigned_url(config(), :get, :my_bucket, "foo.txt", virtual_host: true)
      assert_pre_signed_url(url, "https://my_bucket.s3.amazonaws.com/foo.txt", "3600")

      assert_raise ArgumentError, ~r/invalid bucket name/, fn ->
        S3.presigned_url(config(), :get, :"attacker.example/x?", "foo.txt", virtual_host: true)
      end

      assert_raise ArgumentError, ~r/invalid bucket name/, fn ->
        S3.presigned_url(config(), :get, nil, "foo.txt", virtual_host: true)
      end

      assert_raise ArgumentError, ~r/not in the configured `bucket_as_host_allowlist`/, fn ->
        S3.presigned_post(config(), :"attacker.example", "foo.txt",
          virtual_host: true,
          bucket_as_host: true,
          bucket_as_host_allowlist: ["assets.example.com"]
        )
      end
    end

    test "an empty allowlist rejects every bucket_as_host bucket" do
      assert_raise ArgumentError, ~r/not in the configured `bucket_as_host_allowlist`/, fn ->
        S3.presigned_url(config(), :get, "assets.example.com", "foo.txt",
          virtual_host: true,
          bucket_as_host: true,
          bucket_as_host_allowlist: []
        )
      end
    end

    test "an allowlist that is not a list fails closed" do
      # Older ex_aws versions collapse a list in the S3 config to its first element.
      config = Map.put(config(), :bucket_as_host_allowlist, "assets.example.com")

      assert_raise ArgumentError, ~r/expected a list of hostnames/, fn ->
        S3.presigned_url(config, :get, "assets.example.com", "foo.txt",
          virtual_host: true,
          bucket_as_host: true
        )
      end
    end

    test "the allowlist does not apply without bucket_as_host" do
      opts = [virtual_host: true, bucket_as_host_allowlist: ["assets.example.com"]]

      {:ok, url} = S3.presigned_url(config(), :get, "my-bucket", "foo.txt", opts)
      assert_pre_signed_url(url, "https://my-bucket.s3.amazonaws.com/foo.txt", "3600")
    end

    test "#presigned_url with virtual_host still accepts ordinary bucket names" do
      for bucket <- ["my-bucket", "my.bucket.name", "Legacy_Bucket_1"] do
        {:ok, url} = S3.presigned_url(config(), :get, bucket, "foo.txt", virtual_host: true)
        assert_pre_signed_url(url, "https://#{bucket}.s3.amazonaws.com/foo.txt", "3600")
      end
    end
  end

  defp config(opts \\ []), do: ExAws.Config.new(:s3, opts)

  describe "HTTP header injection protection" do
    @crlf_value "legit\r\nX-Injected-By-Attacker: pwned\r\nX-Smuggled: yes"

    test "#put_object rejects CR/LF in metadata values" do
      assert_raise ArgumentError, ~r/x-amz-meta-comment/, fn ->
        S3.put_object("bucket", "object", "data", meta: [{"comment", @crlf_value}])
      end
    end

    test "#put_object rejects CR/LF in metadata keys" do
      assert_raise ArgumentError, ~r/invalid HTTP header name/, fn ->
        S3.put_object("bucket", "object", "data", meta: [{"comment\r\nX-Evil: 1", "value"}])
      end
    end

    test "#put_object rejects CR/LF in other header options" do
      assert_raise ArgumentError, ~r/content-disposition/, fn ->
        S3.put_object("bucket", "object", "data",
          content_disposition: "attachment; filename=\"a.txt\"\r\nX-Evil: 1"
        )
      end
    end

    test "#put_object_copy rejects CR/LF in metadata" do
      assert_raise ArgumentError, fn ->
        S3.put_object_copy("dest", "object", "src", "object", meta: [comment: @crlf_value])
      end
    end

    test "#initiate_multipart_upload rejects CR/LF in metadata" do
      assert_raise ArgumentError, fn ->
        S3.initiate_multipart_upload("bucket", "object", meta: [comment: @crlf_value])
      end
    end

    test "caller-supplied headers are validated too" do
      assert_raise ArgumentError, fn ->
        S3.list_objects("bucket", headers: %{"x-amz-request-payer" => @crlf_value})
      end

      assert_raise ArgumentError, fn ->
        S3.get_object_acl("bucket", "object", [{"x-amz-request-payer", @crlf_value}])
      end
    end

    test "ordinary metadata is still accepted" do
      %Operation.S3{headers: headers} =
        S3.put_object("bucket", "object", "data",
          meta: [{"comment", "tabs\tand spaces and ünïcödé"}, {:owner_id, 42}]
        )

      assert headers["x-amz-meta-comment"] == "tabs\tand spaces and ünïcödé"
      assert headers["x-amz-meta-owner_id"] == 42
    end
  end
end
