defmodule ExAws.S3.ParserTest do
  use ExUnit.Case, async: true

  test "#parse_upload parses CompleteMultipartUploadResult" do
    upload_response = """
    <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/\">
      <Location>https://google.com</Location>
      <Bucket>name_of_my_bucket</Bucket>
      <Key>name_of_my_key.ext</Key>
      <ETag>&quot;89asdfasdf0asdfasdfasd&quot;</ETag>
    </CompleteMultipartUploadResult>
    """

    result = ExAws.S3.Parsers.parse_upload({:ok, %{body: upload_response}})
    {:ok, %{body: parsed_body}} = result
    assert parsed_body == %{
      location: "https://google.com",
      bucket: "name_of_my_bucket",
      key: "name_of_my_key.ext",
      eTag: "\"89asdfasdf0asdfasdfasd\""
    }
  end

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
    prefix_list = Enum.map(prefixes, &(Map.get(&1, :prefix)))

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

    result = ExAws.S3.Parsers.parse_initiate_multipart_upload({:ok, %{body: initiate_multipart_upload_response}})
    {:ok, %{body: %{bucket: bucket, key: key, upload_id: upload_id}}} = result

    assert "somebucket" == bucket
    assert "abcd" == key
    assert "bUCMhxUCGCA0GiTAhTj6cq2rChItfIMYBgO7To9yiuUyDk4CWqhtHPx8cGkgjzyavE2aW6HvhQgu9pvDB3.oX73RC7N3zM9dSU3mecTndVRHQLJCAsySsT6lXRd2Id2a" == upload_id
  end

  test "#parse_upload_part_copy parses response" do
    parse_upload_part_copy_response = """
    <CopyPartResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <LastModified>2019-02-09T06:27:26.000Z</LastModified>
      <ETag>&quot;7cbef1ad67ecd0d9ba35af98d3de5a94&quot;</ETag>
    </CopyPartResult>
    """

    result = ExAws.S3.Parsers.parse_upload_part_copy({:ok, %{body: parse_upload_part_copy_response}})
    {:ok, %{body: %{last_modified: last_modified, etag: etag}}} = result

    assert "2019-02-09T06:27:26.000Z" == last_modified
    assert "\"7cbef1ad67ecd0d9ba35af98d3de5a94\"" == etag
  end

  test "#parse_complete_multipart_upload parses response" do
    complete_multipart_upload_response = """
    <CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Location>https://s3-eu-west-1.amazonaws.com/my-bucket/tmp-copy3.mp4</Location>
      <Bucket>my-bucket</Bucket>
      <Key>tmp-copy3.mp4</Key>
      <ETag>&quot;17fbc0a106abbb6f381aac6e331f2a19-1&quot;</ETag>
    </CompleteMultipartUploadResult>
    """

    result = ExAws.S3.Parsers.parse_complete_multipart_upload({:ok, %{body: complete_multipart_upload_response}})
    {:ok, %{body: body}} = result

    assert body == %{
      location: "https://s3-eu-west-1.amazonaws.com/my-bucket/tmp-copy3.mp4",
      bucket: "my-bucket",
      key: "tmp-copy3.mp4",
      etag: "\"17fbc0a106abbb6f381aac6e331f2a19-1\""
    }
  end
end
