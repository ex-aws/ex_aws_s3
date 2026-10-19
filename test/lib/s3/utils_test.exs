defmodule ExAws.S3.ImplTest do
  use ExUnit.Case, async: true
  alias ExAws.S3.Utils

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
end
