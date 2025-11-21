defmodule ExAws.S3MinioTest do
  use ExUnit.Case
  alias ExAws.S3

  @moduletag :minio

  @test_object "test-object.txt"
  @test_content "Hello MinIO from ExAws.S3"
  @test_webhook_target "testhook"

  setup do
    # Generate unique bucket name for this test
    bucket = "ex-aws-s3-test-#{System.unique_integer([:positive])}"

    on_exit(fn ->
      # Cleanup: delete all object versions
      # Note: list_object_versions works for both versioned and non-versioned buckets.
      # For non-versioned buckets, objects have a version ID of "null"
      case S3.list_object_versions(bucket) |> ExAws.request() do
        {:ok, %{body: body}} ->
          # Delete all versions
          for version <- body[:versions] || [] do
            S3.delete_object(bucket, version.key, version_id: version.version_id)
            |> ExAws.request!()
          end

          # Delete all delete markers
          for marker <- body[:delete_markers] || [] do
            S3.delete_object(bucket, marker.key, version_id: marker.version_id)
            |> ExAws.request!()
          end

        _ ->
          :ok
      end

      # Delete the bucket itself
      S3.delete_bucket(bucket) |> ExAws.request!()
    end)

    {:ok, bucket: bucket}
  end

  describe "Bucket operations" do
    test "put_bucket creates bucket in MinIO", %{bucket: bucket} do
      result = S3.put_bucket(bucket, "us-east-1") |> ExAws.request()
      assert {:ok, _} = result
    end

    test "list_objects returns empty list for new bucket", %{bucket: bucket} do
      S3.put_bucket(bucket, "us-east-1") |> ExAws.request()

      {:ok, result} = S3.list_objects(bucket) |> ExAws.request()
      assert result.body.contents == []
    end

    test "list_objects_v2 returns empty list for new bucket", %{bucket: bucket} do
      S3.put_bucket(bucket, "us-east-1") |> ExAws.request()

      {:ok, result} = S3.list_objects_v2(bucket) |> ExAws.request()
      assert result.body.contents == []
    end
  end

  describe "Object operations" do
    setup %{bucket: bucket} do
      S3.put_bucket(bucket, "us-east-1") |> ExAws.request!()

      on_exit(fn ->
        # Clean up test object from this describe block
        S3.delete_object(bucket, @test_object) |> ExAws.request!()
      end)

      :ok
    end

    test "put_object uploads content to MinIO", %{bucket: bucket} do
      result = S3.put_object(bucket, @test_object, @test_content) |> ExAws.request()
      assert {:ok, %{status_code: 200}} = result
    end

    test "get_object retrieves content from MinIO", %{bucket: bucket} do
      S3.put_object(bucket, @test_object, @test_content) |> ExAws.request()

      {:ok, result} = S3.get_object(bucket, @test_object) |> ExAws.request()
      assert result.body == @test_content
    end

    test "head_object checks object existence in MinIO", %{bucket: bucket} do
      S3.put_object(bucket, @test_object, @test_content) |> ExAws.request()

      result = S3.head_object(bucket, @test_object) |> ExAws.request()
      assert {:ok, %{status_code: 200}} = result
    end

    test "delete_object removes object from MinIO", %{bucket: bucket} do
      S3.put_object(bucket, @test_object, @test_content) |> ExAws.request()

      result = S3.delete_object(bucket, @test_object) |> ExAws.request()
      assert {:ok, %{status_code: 204}} = result

      # Verify object is gone
      result = S3.get_object(bucket, @test_object) |> ExAws.request()
      assert {:error, {:http_error, 404, _}} = result
    end

    test "list_objects shows uploaded objects", %{bucket: bucket} do
      S3.put_object(bucket, @test_object, @test_content) |> ExAws.request()

      {:ok, result} = S3.list_objects(bucket) |> ExAws.request()
      assert length(result.body.contents) == 1
      assert List.first(result.body.contents).key == @test_object
    end

    test "list_objects_v2 shows uploaded objects", %{bucket: bucket} do
      S3.put_object(bucket, @test_object, @test_content) |> ExAws.request()

      {:ok, result} = S3.list_objects_v2(bucket) |> ExAws.request()
      assert length(result.body.contents) == 1
      assert List.first(result.body.contents).key == @test_object
    end

    test "put_object with metadata and headers", %{bucket: bucket} do
      result =
        S3.put_object(bucket, @test_object, @test_content,
          content_type: "text/plain",
          meta: [foo: "bar", baz: "qux"]
        )
        |> ExAws.request()

      assert {:ok, %{status_code: 200}} = result

      # Verify metadata was set
      {:ok, head_result} = S3.head_object(bucket, @test_object) |> ExAws.request()
      headers = head_result.headers

      assert Enum.any?(headers, fn {k, v} ->
               String.downcase(k) == "content-type" and v == "text/plain"
             end)

      assert Enum.any?(headers, fn {k, v} ->
               String.downcase(k) == "x-amz-meta-foo" and v == "bar"
             end)

      assert Enum.any?(headers, fn {k, v} ->
               String.downcase(k) == "x-amz-meta-baz" and v == "qux"
             end)
    end
  end

  describe "Object copy operations" do
    setup %{bucket: bucket} do
      S3.put_bucket(bucket, "us-east-1") |> ExAws.request!()
      S3.put_object(bucket, @test_object, @test_content) |> ExAws.request!()

      on_exit(fn ->
        # Clean up any copied objects that might have been created
        S3.delete_object(bucket, "copied-#{@test_object}") |> ExAws.request!()
      end)

      :ok
    end

    test "put_object_copy duplicates object in MinIO", %{bucket: bucket} do
      dest_object = "copied-#{@test_object}"

      result =
        S3.put_object_copy(bucket, dest_object, bucket, @test_object)
        |> ExAws.request()

      assert {:ok, %{status_code: 200}} = result

      # Verify copy exists and has same content
      {:ok, get_result} = S3.get_object(bucket, dest_object) |> ExAws.request()
      assert get_result.body == @test_content
    end
  end

  describe "Multiple object operations" do
    setup %{bucket: bucket} do
      S3.put_bucket(bucket, "us-east-1") |> ExAws.request!()

      on_exit(fn ->
        # Clean up any objects that might have been created
        case S3.list_objects(bucket) |> ExAws.request() do
          {:ok, %{body: %{contents: objects}}} ->
            for object <- objects do
              S3.delete_object(bucket, object.key) |> ExAws.request!()
            end

          _ ->
            :ok
        end
      end)

      :ok
    end

    test "delete_multiple_objects removes multiple objects", %{bucket: bucket} do
      objects = ["obj1.txt", "obj2.txt", "obj3.txt"]

      # Upload multiple objects
      for obj <- objects do
        S3.put_object(bucket, obj, "content for #{obj}") |> ExAws.request()
      end

      # Verify they exist
      {:ok, list_result} = S3.list_objects(bucket) |> ExAws.request()
      assert length(list_result.body.contents) == 3

      # Delete multiple objects
      result = S3.delete_multiple_objects(bucket, objects) |> ExAws.request()
      assert {:ok, %{status_code: 200}} = result

      # Verify they're gone
      {:ok, list_result} = S3.list_objects(bucket) |> ExAws.request()
      assert list_result.body.contents == []
    end
  end

  describe "Object tagging operations" do
    setup %{bucket: bucket} do
      S3.put_bucket(bucket, "us-east-1") |> ExAws.request!()
      S3.put_object(bucket, @test_object, @test_content) |> ExAws.request!()

      on_exit(fn ->
        # Clean up test object and any tags from this describe block
        S3.delete_object(bucket, @test_object) |> ExAws.request!()
      end)

      :ok
    end

    test "put_object_tagging and get_object_tagging work with MinIO", %{bucket: bucket} do
      tags = [environment: "test", team: "engineering"]

      # Set tags
      result = S3.put_object_tagging(bucket, @test_object, tags) |> ExAws.request()
      assert {:ok, %{status_code: 200}} = result

      # Get tags
      {:ok, get_result} = S3.get_object_tagging(bucket, @test_object) |> ExAws.request()
      returned_tags = get_result.body

      assert %{tags: tags} = returned_tags
      tag_map = Enum.into(tags, %{}, fn %{key: key, value: value} -> {key, value} end)
      assert %{"environment" => "test", "team" => "engineering"} = tag_map
    end

    test "delete_object_tagging removes tags from object", %{bucket: bucket} do
      tags = [environment: "test"]

      # Set tags first
      S3.put_object_tagging(bucket, @test_object, tags) |> ExAws.request()

      # Delete tags
      result = S3.delete_object_tagging(bucket, @test_object) |> ExAws.request()
      assert {:ok, %{status_code: 204}} = result

      # Verify tags are gone
      {:ok, get_result} = S3.get_object_tagging(bucket, @test_object) |> ExAws.request()
      assert get_result.body == %{tags: []}
    end
  end

  describe "Bucket versioning operations" do
    setup %{bucket: bucket} do
      S3.put_bucket(bucket, "us-east-1") |> ExAws.request!()

      on_exit(fn ->
        # Clean up all versions of objects when versioning is enabled
        case S3.list_object_versions(bucket) |> ExAws.request() do
          {:ok, %{body: %{versions: versions}}} ->
            for version <- versions do
              S3.delete_object(bucket, version.key, version_id: version.version_id)
              |> ExAws.request!()
            end

          _ ->
            :ok
        end
      end)

      :ok
    end

    test "put_bucket_versioning enables versioning in MinIO", %{bucket: bucket} do
      # Enable versioning with proper XML
      version_config =
        "<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>"

      result = S3.put_bucket_versioning(bucket, version_config) |> ExAws.request()
      assert {:ok, %{status_code: 200}} = result

      # Get versioning status
      {:ok, get_result} = S3.get_bucket_versioning(bucket) |> ExAws.request()
      versioning_config = get_result.body

      # MinIO returns raw XML, so check the string content
      assert versioning_config =~ "Enabled"

      # Upload same object twice to test versioning
      S3.put_object(bucket, @test_object, "version 1") |> ExAws.request()
      S3.put_object(bucket, @test_object, "version 2") |> ExAws.request()

      # List object versions
      {:ok, versions_result} = S3.list_object_versions(bucket) |> ExAws.request()
      versions = versions_result.body.versions

      # Should have 2 versions of the same object
      assert length(versions) == 2
      assert Enum.all?(versions, fn version -> version.key == @test_object end)
    end
  end

  describe "Multipart upload operations" do
    setup %{bucket: bucket} do
      S3.put_bucket(bucket, "us-east-1") |> ExAws.request!()

      on_exit(fn ->
        # Clean up any completed multipart uploads
        S3.delete_object(bucket, @test_object) |> ExAws.request!()
      end)

      :ok
    end

    test "initiate and abort multipart upload", %{bucket: bucket} do
      # Initiate multipart upload
      {:ok, init_result} =
        S3.initiate_multipart_upload(bucket, @test_object) |> ExAws.request()

      upload_id = init_result.body.upload_id
      assert is_binary(upload_id)

      # Abort multipart upload
      result = S3.abort_multipart_upload(bucket, @test_object, upload_id) |> ExAws.request()
      assert {:ok, %{status_code: 204}} = result
    end

    test "complete multipart upload workflow", %{bucket: bucket} do
      # >5MB to ensure multipart
      large_content = String.duplicate("A", 5 * 1024 * 1024 + 1000)

      # Initiate multipart upload
      {:ok, init_result} =
        S3.initiate_multipart_upload(bucket, @test_object) |> ExAws.request()

      upload_id = init_result.body.upload_id

      # Upload part
      part_content = String.slice(large_content, 0, 5 * 1024 * 1024)

      {:ok, part_result} =
        S3.upload_part(bucket, @test_object, upload_id, 1, part_content) |> ExAws.request()

      etag =
        part_result.headers |> Enum.find(fn {k, _} -> String.downcase(k) == "etag" end) |> elem(1)

      # Complete multipart upload
      parts = %{1 => etag}

      result =
        S3.complete_multipart_upload(bucket, @test_object, upload_id, parts)
        |> ExAws.request()

      assert {:ok, %{status_code: 200}} = result

      # Verify object exists
      {:ok, _get_result} = S3.head_object(bucket, @test_object) |> ExAws.request()
    end
  end

  describe "Bucket notification operations" do
    setup %{bucket: bucket} do
      S3.put_bucket(bucket, "us-east-1") |> ExAws.request!()

      on_exit(fn ->
        # Clean up notification configuration
        empty_config = %{}
        S3.put_bucket_notification(bucket, empty_config) |> ExAws.request!()
      end)

      :ok
    end

    test "get_bucket_notification returns empty configuration initially", %{bucket: bucket} do
      {:ok, result} = S3.get_bucket_notification(bucket) |> ExAws.request()

      # MinIO returns empty notification configuration as empty XML
      assert result.body =~ "NotificationConfiguration"
    end

    test "put_bucket_notification configures webhook notifications", %{bucket: bucket} do
      # Note: This test verifies the API works, but actual webhook delivery
      # requires MinIO to have webhook endpoints configured via mc admin config
      webhook_config = %{
        queue_configurations: [
          %{
            id: "test-webhook",
            queue_arn: "arn:minio:sqs::#{@test_webhook_target}:webhook",
            events: ["s3:ObjectCreated:*"],
            filter: %{
              key: %{
                filter_rules: [
                  %{name: "prefix", value: "uploads/"},
                  %{name: "suffix", value: ".jpg"}
                ]
              }
            }
          }
        ]
      }

      # This should succeed even if webhook endpoint isn't configured
      # (MinIO accepts the configuration but won't deliver events)
      result = S3.put_bucket_notification(bucket, webhook_config) |> ExAws.request()
      assert {:ok, %{status_code: 200}} = result

      # Verify configuration was set
      {:ok, get_result} = S3.get_bucket_notification(bucket) |> ExAws.request()
      config_body = get_result.body

      # Should contain the webhook configuration
      assert config_body =~ "test-webhook"
      assert config_body =~ ~r/arn:minio:sqs:[\w-]*:#{@test_webhook_target}:webhook/
      assert config_body =~ "s3:ObjectCreated"
    end

    test "delete_bucket_notification clears configuration", %{bucket: bucket} do
      # First set a configuration
      config = %{
        topic_arn: "arn:aws:sns:us-east-1:123456789012:test-topic",
        events: ["s3:ObjectCreated:Put"]
      }

      S3.put_bucket_notification(bucket, config) |> ExAws.request()

      # Clear the configuration
      empty_config = %{}
      result = S3.put_bucket_notification(bucket, empty_config) |> ExAws.request()
      assert {:ok, %{status_code: 200}} = result

      # Verify configuration is empty
      {:ok, get_result} = S3.get_bucket_notification(bucket) |> ExAws.request()
      config_body = get_result.body

      # Should be empty notification configuration
      assert config_body =~ "NotificationConfiguration"
      refute config_body =~ "test-topic"
    end
  end
end
