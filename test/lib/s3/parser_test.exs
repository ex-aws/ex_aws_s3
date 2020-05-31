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
end
