defmodule ExAws.S3HetznerTest do
  use ExUnit.Case
  alias ExAws.S3

  @moduletag :hetzner
  @content "Hello Hetzner from ExAws.S3"

  setup do
    bucket = Application.get_env(:ex_aws, :bucket)
    # The SSE customer key is stored as a raw 32-byte string (not base64).
    # S3 expects the key base64-encoded in the request headers, so we encode
    # it here — matching the manual call:
    #   customer_key: Base.encode64(sse_c_key)
    #   customer_key_md5: Base.encode64(:crypto.hash(:md5, sse_c_key))
    raw_key = Application.get_env(:ex_aws, :sse_customer_key)

    assert is_binary(bucket) and bucket != "",
           "S3_BUCKET not set — run via `just in-docker-hetzner-test`"

    assert is_binary(raw_key) and byte_size(raw_key) == 32,
           "SSE_CUSTOMER_KEY not set or not 32 bytes — run via `just in-docker-hetzner-test`"

    key_b64 = Base.encode64(raw_key)
    key_md5_b64 = Base.encode64(:crypto.hash(:md5, raw_key))

    encryption_opts = [
      customer_algorithm: "AES256",
      customer_key: key_b64,
      customer_key_md5: key_md5_b64
    ]

    prefix = "hetzner-test-#{System.unique_integer([:positive])}/"

    on_exit(fn ->
      case S3.list_objects(bucket, prefix: prefix) |> ExAws.request() do
        {:ok, %{body: %{contents: objects}}} ->
          for o <- objects, do: S3.delete_object(bucket, o.key) |> ExAws.request()

        _ ->
          :ok
      end
    end)

    {:ok, bucket: bucket, prefix: prefix, encryption_opts: encryption_opts}
  end

  describe "Connectivity" do
    test "head_bucket succeeds", %{bucket: bucket} do
      assert {:ok, %{status_code: 200}} = S3.head_bucket(bucket) |> ExAws.request()
    end
  end

  describe "Basic object operations" do
    test "put_object + get_object roundtrip", %{bucket: b, prefix: p} do
      key = p <> "basic.txt"
      assert {:ok, %{status_code: 200}} = S3.put_object(b, key, @content) |> ExAws.request()
      assert {:ok, %{body: @content}} = S3.get_object(b, key) |> ExAws.request()
    end

    test "head_object reports existence", %{bucket: b, prefix: p} do
      key = p <> "head.txt"
      S3.put_object(b, key, @content) |> ExAws.request()
      assert {:ok, %{status_code: 200}} = S3.head_object(b, key) |> ExAws.request()
    end

    test "delete_object removes object", %{bucket: b, prefix: p} do
      key = p <> "del.txt"
      S3.put_object(b, key, @content) |> ExAws.request()
      assert {:ok, %{status_code: 204}} = S3.delete_object(b, key) |> ExAws.request()
      assert {:error, {:http_error, 404, _}} = S3.get_object(b, key) |> ExAws.request()
    end
  end

  describe "SSE-C" do
    test "put + get roundtrip with customer key", %{bucket: b, prefix: p, encryption_opts: enc} do
      key = p <> "ssec.txt"

      assert {:ok, %{status_code: 200}} =
               S3.put_object(b, key, "secret", encryption: enc) |> ExAws.request()

      assert {:ok, %{body: "secret"}} =
               S3.get_object(b, key, encryption: enc) |> ExAws.request()
    end

    test "head_object with customer key", %{bucket: b, prefix: p, encryption_opts: enc} do
      key = p <> "ssec-head.txt"
      S3.put_object(b, key, "secret", encryption: enc) |> ExAws.request()

      assert {:ok, %{status_code: 200}} =
               S3.head_object(b, key, encryption: enc) |> ExAws.request()
    end

    test "get_object without key fails", %{bucket: b, prefix: p, encryption_opts: enc} do
      key = p <> "ssec-nokey.txt"
      S3.put_object(b, key, "secret", encryption: enc) |> ExAws.request!()
      assert {:error, _} = S3.get_object(b, key) |> ExAws.request()
    end

    test "get_object with wrong key fails", %{bucket: b, prefix: p, encryption_opts: enc} do
      key = p <> "ssec-wrong.txt"
      S3.put_object(b, key, "secret", encryption: enc) |> ExAws.request!()

      wrong = :crypto.strong_rand_bytes(32)

      wrong_enc = [
        customer_algorithm: "AES256",
        customer_key: Base.encode64(wrong),
        customer_key_md5: Base.encode64(:crypto.hash(:md5, wrong))
      ]

      assert {:error, _} = S3.get_object(b, key, encryption: wrong_enc) |> ExAws.request()
    end

    test "multipart roundtrip with customer key",
         %{bucket: b, prefix: p, encryption_opts: enc} do
      key = p <> "ssec-multipart.txt"

      part_size = 5 * 1024 * 1024
      part1 = :crypto.strong_rand_bytes(part_size)
      part2 = :crypto.strong_rand_bytes(part_size)
      expected = part1 <> part2

      {:ok, init_result} =
        S3.initiate_multipart_upload(b, key, encryption: enc) |> ExAws.request()

      upload_id = init_result.body.upload_id

      etags =
        for {part, n} <- Enum.with_index([part1, part2], 1) do
          {:ok, part_result} =
            S3.upload_part(b, key, upload_id, n, part, encryption: enc)
            |> ExAws.request()

          {n,
           part_result.headers
           |> Enum.find(fn {k, _} -> String.downcase(k) == "etag" end)
           |> elem(1)}
        end
        |> Map.new()

      assert {:ok, %{status_code: 200}} =
               S3.complete_multipart_upload(b, key, upload_id, etags, encryption: enc)
               |> ExAws.request()

      assert {:ok, %{body: ^expected}} =
               S3.get_object(b, key, encryption: enc) |> ExAws.request()
    end
  end
end
