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

  test "build_lifecycle_rule" do
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
          trigger: {:days, 2}
        },
        abort_incomplete_multipart_upload: %{
          trigger: {:days, 2}
        }
      }
    }

    assert rule |> Utils.build_lifecycle_rule() ==
             "<Rule><AbortIncompleteMultipartUpload><DaysAfterInitiation>2</DaysAfterInitiation></AbortIncompleteMultipartUpload><NoncurrentVersionExpiration><NoncurrentDays>2</NoncurrentDays></NoncurrentVersionExpiration><NoncurrentVersionTransition><NoncurrentDays>2</NoncurrentDays><StorageClass></StorageClass></NoncurrentVersionTransition><Expiration><Days>2</Days><ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker></Expiration><Transition><Days>2</Days><StorageClass></StorageClass></Transition><Filter><Prefix>prefix/</Prefix></Filter><Status>Enabled</Status><ID>123</ID></Rule>"
  end
end
