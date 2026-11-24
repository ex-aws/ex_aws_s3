defmodule ExAws.S3.ImplTest do
  use ExUnit.Case, async: true
  alias ExAws.S3.Utils

  @all_users "http://acs.amazonaws.com/groups/global/AllUsers"

  test "#build_cors_rule" do
    rule = %{allowed_origins: ["*"], allowed_headers: ["foo", "bar"], max_age_seconds: 45}

    assert rule |> Utils.build_cors_rule() ==
             "<CORSRule><MaxAgeSeconds>45</MaxAgeSeconds><AllowedOrigin>*</AllowedOrigin><AllowedHeader>foo</AllowedHeader><AllowedHeader>bar</AllowedHeader></CORSRule>"
  end

  test "format_and_take/2" do
    params = [:foo, :bar_baz]
    opts = %{foo: "foo", bar_baz: "bar_baz", yo: "yo"}

    assert %{"foo" => "foo", "bar-baz" => "bar_baz"} ==
             opts
             |> Utils.format_and_take(params)
  end

  test "format_grant_headers/2" do
    grants = [grant_read: [email: "foo@bar.com", id: "fake_id"]]

    assert grants |> Utils.format_acl_headers() ==
             %{"x-amz-grant-read" => "emailAddress=\"foo@bar.com\", id=\"fake_id\""}
  end

  describe "format_grant_headers/1 grantee validation" do
    test "accepts every documented grantee type" do
      grants = [grant_full_control: [id: "fake_id", uri: @all_users, email: "foo@bar.com"]]

      assert Utils.format_acl_headers(grants) == %{
               "x-amz-grant-full-control" =>
                 ~s(id="fake_id", uri="#{@all_users}", emailAddress="foo@bar.com")
             }
    end

    test "rejects a grantee identifier containing a double quote" do
      injected = ~s(victim@example.com", uri="#{@all_users})

      assert_raise ArgumentError, ~r/invalid S3 grantee/, fn ->
        Utils.format_acl_headers(grant_read: [email: injected])
      end
    end

    test "rejects a grantee identifier containing a comma" do
      assert_raise ArgumentError, ~r/invalid S3 grantee/, fn ->
        Utils.format_acl_headers(grant_read: [id: "abc, id=def"])
      end
    end

    test "rejects control characters and non-ASCII in a grantee identifier" do
      for bad <- [
            "a@b.com\r\nx-amz-acl: public-read",
            "abc\n",
            "abc\0",
            "ünicode@example.com",
            <<0xFF>>
          ] do
        assert_raise ArgumentError, ~r/invalid S3 grantee/, fn ->
          Utils.format_acl_headers(grant_write: [email: bad])
        end
      end
    end

    test "rejects an unsafe grantee type" do
      assert_raise ArgumentError, ~r/invalid S3 grantee/, fn ->
        Utils.format_acl_headers(grant_read: [{~s(id="x", uri), @all_users}])
      end
    end
  end

  test "build_encryption_headers/1" do
    assert Utils.build_encryption_headers("AES256") == %{
             "x-amz-server-side-encryption" => "AES256"
           }

    assert Utils.build_encryption_headers(aws_kms_key_id: "key_id") ==
             %{
               "x-amz-server-side-encryption" => "aws:kms",
               "x-amz-server-side-encryption-aws-kms-key-id" => "key_id"
             }
  end

  describe "build_lifecycle_rule" do
    test "applying rule to unversioned buckets" do
      rule = %{
        id: "123",
        enabled: true,
        filter: %{
          prefix: "prefix/",
          tags: %{}
        },
        actions: %{
          transition: %{
            trigger: {:days, 2},
            storage: ""
          },
          expiration: %{
            trigger: {:days, 2}
          },
          abort_incomplete_multipart_upload: %{
            trigger: {:days, 2}
          }
        }
      }

      assert rule |> Utils.build_lifecycle_rule() ==
               "<Rule><AbortIncompleteMultipartUpload><DaysAfterInitiation>2</DaysAfterInitiation></AbortIncompleteMultipartUpload><Expiration><Days>2</Days></Expiration><Transition><Days>2</Days><StorageClass></StorageClass></Transition><Filter><Prefix>prefix/</Prefix></Filter><Status>Enabled</Status><ID>123</ID></Rule>"
    end

    test "applying rule to versioned buckets" do
      rule = %{
        id: "123",
        enabled: true,
        filter: %{
          prefix: "prefix/",
          tags: %{}
        },
        actions: %{
          transition: %{
            trigger: {:days, 2},
            storage: ""
          },
          expiration: %{
            trigger: {:days, 2},
            expired_object_delete_marker: true
          },
          noncurrent_version_transition: %{
            trigger: {:days, 2},
            storage: ""
          },
          noncurrent_version_expiration: %{
            trigger: {:days, 2},
            newer_noncurrent_versions: 10
          },
          abort_incomplete_multipart_upload: %{
            trigger: {:days, 2}
          }
        }
      }

      assert Utils.build_lifecycle_rule(rule) ==
               Enum.join([
                 "<Rule><AbortIncompleteMultipartUpload><DaysAfterInitiation>2</DaysAfterInitiation></AbortIncompleteMultipartUpload>",
                 "<NoncurrentVersionExpiration><NoncurrentDays>2</NoncurrentDays><NewerNoncurrentVersions>10</NewerNoncurrentVersions></NoncurrentVersionExpiration>",
                 "<NoncurrentVersionTransition><NoncurrentDays>2</NoncurrentDays><StorageClass></StorageClass></NoncurrentVersionTransition>",
                 "<Expiration><Days>2</Days><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration>",
                 "<Transition><Days>2</Days><StorageClass></StorageClass></Transition><Filter><Prefix>prefix/</Prefix></Filter><Status>Enabled</Status><ID>123</ID></Rule>"
               ])
    end

    test "lifecycle rule with 0 day trigger" do
      rule = %{
        id: "123",
        enabled: true,
        filter: %{
          prefix: "prefix/",
          tags: %{}
        },
        actions: %{
          transition: %{
            trigger: {:days, 0},
            storage: ""
          },
          expiration: %{
            trigger: {:days, 0},
            expired_object_delete_marker: true
          },
          noncurrent_version_transition: %{
            trigger: {:days, 0},
            storage: ""
          },
          noncurrent_version_expiration: %{
            trigger: {:days, 0}
          },
          abort_incomplete_multipart_upload: %{
            trigger: {:days, 0}
          }
        }
      }

      assert rule |> Utils.build_lifecycle_rule() ==
               "<Rule><AbortIncompleteMultipartUpload><DaysAfterInitiation>0</DaysAfterInitiation></AbortIncompleteMultipartUpload><NoncurrentVersionExpiration><NoncurrentDays>0</NoncurrentDays></NoncurrentVersionExpiration><NoncurrentVersionTransition><NoncurrentDays>0</NoncurrentDays><StorageClass></StorageClass></NoncurrentVersionTransition><Expiration><Days>0</Days><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration><Transition><Days>0</Days><StorageClass></StorageClass></Transition><Filter><Prefix>prefix/</Prefix></Filter><Status>Enabled</Status><ID>123</ID></Rule>"
    end

    test "escapes XML metacharacters in caller-supplied values" do
      rule = %{
        id: "id <&> \"quoted\" 'single'",
        enabled: true,
        filter: %{
          prefix: "a&b/<c>/",
          tags: %{"k<ey" => "v&alue"}
        },
        actions: %{
          transition: %{
            trigger: {:days, 2},
            storage: "GLACIER</StorageClass><Days>1</Days>"
          },
          noncurrent_version_transition: %{
            trigger: {:days, 2},
            storage: "<x>"
          },
          noncurrent_version_expiration: %{
            trigger: {:days, 2},
            newer_noncurrent_versions: "1</NewerNoncurrentVersions><Foo>"
          }
        }
      }

      assert Utils.build_lifecycle_rule(rule) ==
               Enum.join([
                 "<Rule>",
                 "<NoncurrentVersionExpiration><NoncurrentDays>2</NoncurrentDays><NewerNoncurrentVersions>1&lt;/NewerNoncurrentVersions&gt;&lt;Foo&gt;</NewerNoncurrentVersions></NoncurrentVersionExpiration>",
                 "<NoncurrentVersionTransition><NoncurrentDays>2</NoncurrentDays><StorageClass>&lt;x&gt;</StorageClass></NoncurrentVersionTransition>",
                 "<Transition><Days>2</Days><StorageClass>GLACIER&lt;/StorageClass&gt;&lt;Days&gt;1&lt;/Days&gt;</StorageClass></Transition>",
                 "<Filter><And><Prefix>a&amp;b/&lt;c&gt;/</Prefix><Tag><Key>k&lt;ey</Key><Value>v&amp;alue</Value></Tag></And></Filter>",
                 "<Status>Enabled</Status>",
                 "<ID>id &lt;&amp;&gt; &quot;quoted&quot; &apos;single&apos;</ID>",
                 "</Rule>"
               ])
    end

    test "does not let a tag value inject a lifecycle action (GHSA-pcgv-phrm-r85v)" do
      payload =
        "frontend</Value></Tag></Filter>" <>
          "<Expiration><Days>1</Days></Expiration>" <>
          "<Filter><Tag><Key>team</Key><Value>frontend"

      rule = %{
        id: "tenant-rule",
        enabled: true,
        filter: %{tags: [{"team", payload}]},
        actions: %{}
      }

      xml = Utils.build_lifecycle_rule(rule)

      refute xml =~ "<Expiration>"

      assert xml ==
               "<Rule><Filter><Tag><Key>team</Key><Value>frontend&lt;/Value&gt;&lt;/Tag&gt;&lt;/Filter&gt;&lt;Expiration&gt;&lt;Days&gt;1&lt;/Days&gt;&lt;/Expiration&gt;&lt;Filter&gt;&lt;Tag&gt;&lt;Key&gt;team&lt;/Key&gt;&lt;Value&gt;frontend</Value></Tag></Filter><Status>Enabled</Status><ID>tenant-rule</ID></Rule>"
    end
  end

  test "escape_xml_string/1" do
    assert Utils.escape_xml_string("a'b\"c&d<e>f\rg\nh") ==
             "a&apos;b&quot;c&amp;d&lt;e&gt;f&#13;g&#10;h"

    assert Utils.escape_xml_string("plain") == "plain"
    assert Utils.escape_xml_string(10) == "10"
  end

  describe "validate_headers!/1" do
    test "returns the headers unchanged when they are valid" do
      headers = %{
        "x-amz-meta-comment" => "hello world\t!",
        "content-length" => 5,
        "x-amz-meta-city" => "Zürich"
      }

      assert Utils.validate_headers!(headers) == headers
      assert Utils.validate_headers!([{"x-amz-acl", :private}]) == [{"x-amz-acl", :private}]
      assert Utils.validate_headers!(%{}) == %{}
    end

    test "rejects control characters in values" do
      for bad <- ["a\r\nb", "a\nb", "a\rb", "a\0b", "a\x7fb", "a\x01b"] do
        assert_raise ArgumentError, ~r/invalid value for HTTP header "x-amz-meta-a"/, fn ->
          Utils.validate_headers!(%{"x-amz-meta-a" => bad})
        end
      end
    end

    test "rejects invalid header names" do
      for bad <- ["", "x-amz-meta-a\r\nX-Evil: y", "x amz", "x:amz", "x-amz-meta-ü"] do
        assert_raise ArgumentError, ~r/invalid HTTP header name/, fn ->
          Utils.validate_headers!(%{bad => "value"})
        end
      end
    end

    test "validates non-binary names and values the way they will be serialised" do
      assert_raise ArgumentError, fn -> Utils.validate_headers!([{:"x\r\ny", "v"}]) end

      assert_raise ArgumentError, fn ->
        Utils.validate_headers!([{"x-amz-meta-a", [?a, ?\r, ?\n, ?b]}])
      end
    end
  end

  describe "validate_host_bucket!/1" do
    test "accepts bucket names made of letters, digits, dots, dashes and underscores" do
      for bucket <- ["my-bucket", "my.bucket.name", "Legacy_Bucket_1", "mybucket.example.com"] do
        assert Utils.validate_host_bucket!(bucket) == bucket
      end
    end

    test "rejects characters that would change the request host" do
      for bucket <- [
            "attacker.example/x?",
            "bucket?x",
            "bucket#x",
            "user@bucket",
            "bucket:443",
            "buck et",
            "bucket\r\n",
            "b\u00fccket",
            ""
          ] do
        assert_raise ArgumentError, ~r/invalid bucket name/, fn ->
          Utils.validate_host_bucket!(bucket)
        end
      end
    end
  end
end
