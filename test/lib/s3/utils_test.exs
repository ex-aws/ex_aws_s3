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
  end
end
