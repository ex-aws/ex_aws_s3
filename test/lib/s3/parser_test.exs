defmodule ExAws.S3.ParserTest do
  use ExUnit.Case, async: true

  test "#parse_list_objects parses CommonPrefixes" do
    list_objects_response = """
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Name>example-bucket</Name>
      <Prefix></Prefix>
      <Marker></Marker>
      <MaxKeys>1000</MaxKeys>
      <Delimiter>/</Delimiter>
      <IsTruncated>false</IsTruncated>
      <Contents>
        <Key>sample.jpg</Key>
        <LastModified>2011-02-26T01:56:20.000Z</LastModified>
        <ETag>&quot;bf1d737a4d46a19f3bced6905cc8b902&quot;</ETag>
        <Size>142863</Size>
        <Owner>
        <ID>canonical-user-id</ID>
        <DisplayName>display-name</DisplayName>
        </Owner>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <CommonPrefixes>
        <Prefix>photos/</Prefix>
      </CommonPrefixes>
    </ListBucketResult>
    """

    result = ExAws.S3.Parsers.parse_list_objects({:ok, %{body: list_objects_response}})
    {:ok, %{body: %{common_prefixes: prefixes}}} = result
    prefix_list = Enum.map(prefixes, &Map.get(&1, :prefix))

    assert ["photos/"] == prefix_list
  end

  test "#parse_list_objects allows unowned objects" do
    list_objects_response = """
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Name>example-bucket</Name>
      <Prefix></Prefix>
      <Marker></Marker>
      <MaxKeys>1000</MaxKeys>
      <Delimiter>/</Delimiter>
      <IsTruncated>false</IsTruncated>
      <Contents>
        <Key>sample.jpg</Key>
        <LastModified>2011-02-26T01:56:20.000Z</LastModified>
        <ETag>&quot;bf1d737a4d46a19f3bced6905cc8b902&quot;</ETag>
        <Size>142863</Size>
        <StorageClass>STANDARD</StorageClass>
      </Contents>
      <CommonPrefixes>
        <Prefix>photos/</Prefix>
      </CommonPrefixes>
    </ListBucketResult>
    """

    result = ExAws.S3.Parsers.parse_list_objects({:ok, %{body: list_objects_response}})
    {:ok, _} = result
  end

  test "#initiate_multipart_upload parses response" do
    initiate_multipart_upload_response = """
    <InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Bucket>somebucket</Bucket>
      <Key>abcd</Key>
      <UploadId>bUCMhxUCGCA0GiTAhTj6cq2rChItfIMYBgO7To9yiuUyDk4CWqhtHPx8cGkgjzyavE2aW6HvhQgu9pvDB3.oX73RC7N3zM9dSU3mecTndVRHQLJCAsySsT6lXRd2Id2a</UploadId>
    </InitiateMultipartUploadResult>
    """

    result =
      ExAws.S3.Parsers.parse_initiate_multipart_upload(
        {:ok, %{body: initiate_multipart_upload_response}}
      )

    {:ok, %{body: %{bucket: bucket, key: key, upload_id: upload_id}}} = result

    assert "somebucket" == bucket
    assert "abcd" == key

    assert "bUCMhxUCGCA0GiTAhTj6cq2rChItfIMYBgO7To9yiuUyDk4CWqhtHPx8cGkgjzyavE2aW6HvhQgu9pvDB3.oX73RC7N3zM9dSU3mecTndVRHQLJCAsySsT6lXRd2Id2a" ==
             upload_id
  end

  test "#parse_list_parts parses empty parts list" do
    response = ~S"""
    <?xml version="1.0" encoding="UTF-8"?>
    <ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Bucket>name_of_my_bucket</Bucket>
      <Key>name_of_my_key.ext</Key>
      <UploadId>e3gloTamzXlqzgRfKIXrFBhnxCfM35jhktoh.wduDUJHy61R_hjglrx_rLguDGxmOvPeDfzJEK7mxgx7eRwPs9XbYXVmDywrRjbJSmqr.McfkCRDjuI4cdB72IYzfFJl</UploadId>
      <Initiator>
        <ID>arn:aws:iam::123456789012:user/username</ID>
        <DisplayName>username</DisplayName>
      </Initiator>
      <Owner>
        <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
        <DisplayName>noone@example.com</DisplayName>
      </Owner>
      <StorageClass>STANDARD</StorageClass>
      <PartNumberMarker>0</PartNumberMarker>
      <NextPartNumberMarker>0</NextPartNumberMarker>
      <MaxParts>1000</MaxParts>
      <IsTruncated>false</IsTruncated>
    </ListPartsResult>
    """

    assert {:ok, %{body: body}} = ExAws.S3.Parsers.parse_list_parts({:ok, %{body: response}})
    assert body == %{parts: []}
  end

  test "#parse_list_parts parses parts of the multipart upload" do
    response = ~S"""
    <?xml version="1.0" encoding="UTF-8"?>
    <ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Bucket>name_of_my_bucket</Bucket>
      <Key>name_of_my_key.ext</Key>
      <UploadId>e3gloTamzXlqzgRfKIXrFBhnxCfM35jhktoh.wduDUJHy61R_hjglrx_rLguDGxmOvPeDfzJEK7mxgx7eRwPs9XbYXVmDywrRjbJSmqr.McfkCRDjuI4cdB72IYzfFJl</UploadId>
      <Initiator>
        <ID>arn:aws:iam::123456789012:user/username</ID>
        <DisplayName>username</DisplayName>
      </Initiator>
      <Owner>
        <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
        <DisplayName>noone@example.com</DisplayName>
      </Owner>
      <StorageClass>STANDARD</StorageClass>
      <PartNumberMarker>0</PartNumberMarker>
      <NextPartNumberMarker>2</NextPartNumberMarker>
      <MaxParts>1000</MaxParts>
      <IsTruncated>false</IsTruncated>
      <Part>
        <PartNumber>1</PartNumber>
        <LastModified>2021-12-10T18:43:58.000Z</LastModified>
        <ETag>&quot;d53f6b1e2a3b54515f8dbcbcbe3aef9e&quot;</ETag>
        <Size>10000000</Size>
      </Part>
      <Part>
        <PartNumber>2</PartNumber>
        <LastModified>2021-12-10T18:43:47.000Z</LastModified>
        <ETag>&quot;d1cae2efbf9bfdec76ef78e5c2dd41e5&quot;</ETag>
        <Size>3811508</Size>
      </Part>
    </ListPartsResult>
    """

    assert {:ok, %{body: body}} = ExAws.S3.Parsers.parse_list_parts({:ok, %{body: response}})

    assert body == %{
             parts: [
               %{
                 part_number: "1",
                 etag: ~s("d53f6b1e2a3b54515f8dbcbcbe3aef9e"),
                 size: "10000000"
               },
               %{
                 part_number: "2",
                 etag: ~s("d1cae2efbf9bfdec76ef78e5c2dd41e5"),
                 size: "3811508"
               }
             ]
           }
  end

  test "#parse_object_tagging parses empty tagset" do
    response = ~S"""
    <?xml version="1.0" encoding="UTF-8"?>
    <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <TagSet/>
    </Tagging>
    """

    assert {:ok, %{body: body}} = ExAws.S3.Parsers.parse_object_tagging({:ok, %{body: response}})
    assert body == %{tags: []}
  end

  test "#parse_object_tagging parses tags" do
    response = ~S"""
    <?xml version="1.0" encoding="UTF-8"?>
    <Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <TagSet>
        <Tag>
          <Key>tag1</Key>
          <Value>val1</Value>
        </Tag>
        <Tag>
          <Key>tag2</Key>
          <Value>val2</Value>
        </Tag>
      </TagSet>
    </Tagging>
    """

    assert {:ok, %{body: body}} = ExAws.S3.Parsers.parse_object_tagging({:ok, %{body: response}})
    assert body == %{tags: [%{key: "tag1", value: "val1"}, %{key: "tag2", value: "val2"}]}
  end

  test "#parse_bucket_object_versions parses ListVersionsResult" do
    response = ~S"""
    <ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01">
    <Name>bucket</Name>
    <Prefix>my</Prefix>
    <KeyMarker/>
    <VersionIdMarker/>
    <MaxKeys>5</MaxKeys>
    <IsTruncated>false</IsTruncated>
    <Version>
        <Key>my-image.jpg</Key>
        <VersionId>3/L4kqtJl40Nr8X8gdRQBpUMLUo</VersionId>
        <IsLatest>true</IsLatest>
         <LastModified>2009-10-12T17:50:30.000Z</LastModified>
        <ETag>&quot;fba9dede5f27731c9771645a39863328&quot;</ETag>
        <Size>434234</Size>
        <StorageClass>STANDARD</StorageClass>
        <Owner>
            <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
            <DisplayName>noone@example.com</DisplayName>
        </Owner>
    </Version>
    <DeleteMarker>
        <Key>my-second-image.jpg</Key>
        <VersionId>03jpff543dhffds434rfdsFDN943fdsFkdmqnh892</VersionId>
        <IsLatest>true</IsLatest>
        <LastModified>2009-11-12T17:50:30.000Z</LastModified>
        <Owner>
            <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
            <DisplayName>noone@example.com</DisplayName>
        </Owner>
    </DeleteMarker>
    <Version>
        <Key>my-second-image.jpg</Key>
        <VersionId>QUpfdndhfd8438MNFDN93jdnJFkdmqnh893</VersionId>
        <IsLatest>false</IsLatest>
        <LastModified>2009-10-10T17:50:30.000Z</LastModified>
        <ETag>&quot;9b2cf535f27731c974343645a3985328&quot;</ETag>
        <Size>166434</Size>
        <StorageClass>STANDARD</StorageClass>
        <Owner>
            <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
            <DisplayName>noone@example.com</DisplayName>
        </Owner>
    </Version>
    <DeleteMarker>
        <Key>my-third-image.jpg</Key>
        <VersionId>03jpff543dhffds434rfdsFDN943fdsFkdmqnh892</VersionId>
        <IsLatest>true</IsLatest>
        <LastModified>2009-10-15T17:50:30.000Z</LastModified>
        <Owner>
            <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
            <DisplayName>noone@example.com</DisplayName>
        </Owner>
    </DeleteMarker>
    <Version>
        <Key>my-third-image.jpg</Key>
        <VersionId>UIORUnfndfhnw89493jJFJ</VersionId>
        <IsLatest>false</IsLatest>
        <LastModified>2009-10-11T12:50:30.000Z</LastModified>
        <ETag>&quot;772cf535f27731c974343645a3985328&quot;</ETag>
        <Size>64</Size>
        <StorageClass>STANDARD</StorageClass>
        <Owner>
            <ID>75aa57f09aa0c8caeab4f8c24e99d10f8e7faeebf76c078efc7c6caea54ba06a</ID>
            <DisplayName>noone@example.com</DisplayName>
        </Owner>
     </Version>
    </ListVersionsResult>
    """

    assert {:ok, %{body: body}} =
             ExAws.S3.Parsers.parse_bucket_object_versions({:ok, %{body: response}})

    %{
      name: name,
      prefix: prefix,
      max_keys: max_keys,
      versions: versions,
      delete_markers: delete_markers
    } = body

    assert name == "bucket"
    assert prefix == "my"
    assert max_keys == "5"

    assert is_list(versions)
    assert is_list(delete_markers)
    assert Enum.count(versions) == 3
    assert Enum.count(delete_markers) == 2

    version1 = Enum.at(versions, 0)
    assert version1[:key] == "my-image.jpg"
    assert version1[:version_id] == "3/L4kqtJl40Nr8X8gdRQBpUMLUo"
    assert version1[:is_latest] == "true"
    assert version1[:size] == "434234"
    assert version1[:last_modified] == "2009-10-12T17:50:30.000Z"
    assert is_map(version1[:owner])
    assert version1[:owner][:display_name] == "noone@example.com"

    delete_marker1 = Enum.at(delete_markers, 0)
    assert delete_marker1[:key] == "my-second-image.jpg"
    assert delete_marker1[:version_id] == "03jpff543dhffds434rfdsFDN943fdsFkdmqnh892"
    assert delete_marker1[:is_latest] == "true"
    assert delete_marker1[:last_modified] == "2009-11-12T17:50:30.000Z"
    assert is_map(delete_marker1[:owner])
    assert delete_marker1[:owner][:display_name] == "noone@example.com"
  end

  describe "#parse_upload_part_copy" do
    test "parses a good response" do
      parse_upload_part_copy_response = """
      <CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <LastModified>2019-02-09T06:27:26.000Z</LastModified>
      <ETag>&quot;7cbef1ad67ecd0d9ba35af98d3de5a94&quot;</ETag>
      </CopyPartResult>
      """

      result =
        ExAws.S3.Parsers.parse_upload_part_copy({:ok, %{body: parse_upload_part_copy_response}})

      assert {:ok, %{body: %{last_modified: last_modified, etag: etag}}} = result
      assert "2019-02-09T06:27:26.000Z" == last_modified
      assert "\"7cbef1ad67ecd0d9ba35af98d3de5a94\"" == etag
    end

    test "handles nil" do
      result = ExAws.S3.Parsers.parse_upload_part_copy({:ok, %{body: nil}})
      assert {:error, %{body: nil}} == result
    end

    test "handles errors by passing them through" do
      error = {:error, "error"}
      result = ExAws.S3.Parsers.parse_upload_part_copy(error)
      assert result == error
    end
  end

  describe "#parse_complete_multipart_upload" do
    test "parses CompleteMultipartUploadResult" do
      complete_multipart_upload_response = """
      <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
        <Location>https://s3-eu-west-1.amazonaws.com/my-bucket/tmp-copy3.mp4</Location>
        <Bucket>my-bucket</Bucket>
        <Key>tmp-copy3.mp4</Key>
        <ETag>&quot;17fbc0a106abbb6f381aac6e331f2a19-1&quot;</ETag>
      </CompleteMultipartUploadResult>
      """

      result =
        ExAws.S3.Parsers.parse_complete_multipart_upload(
          {:ok, %{body: complete_multipart_upload_response}}
        )

      {:ok, %{body: body}} = result

      assert body == %{
               location: "https://s3-eu-west-1.amazonaws.com/my-bucket/tmp-copy3.mp4",
               bucket: "my-bucket",
               key: "tmp-copy3.mp4",
               etag: "\"17fbc0a106abbb6f381aac6e331f2a19-1\""
             }
    end

    test "handles errors by passing them through" do
      error = {:error, "error"}
      result = ExAws.S3.Parsers.parse_complete_multipart_upload(error)
      assert result == error
    end
  end
end
