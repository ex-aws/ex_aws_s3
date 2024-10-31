# Changelog

v2.5.5 (2024-10-31)

- Fix stream! handling when the client adapter models response headers as lists (eg Req)

v2.5.4 (2024-09-10)

- Add empty value checks for `delete_object`
- Handle errors in `parse_complete_multipart_upload`
- Add `newer_concurrent_versions` option to put lifecycle
- Show changelog link on hex info page
- Add missing `meta` opts from the upload type spec
- Add `newer_noncurrent_versions` to `put_bucket_lifecycle`
- Clarify in docs that presigned URLs are generated locally
- Document the :expires_in type
- Add checksum header when hash is not MD5

v2.5.3 (2024-01-11)

- Add option to get auth from AuthCache on every request when uploading to S3 in a stream
- Fix dialyzer warnings with Elixir 1.16
- Allow hash types other than MD5 when generating body hash
- Fix error handling for `parse_upload_part_copy`

v2.5.2 (2023-10-16)

- Properly fix typespec for presigned_url/5, and include support for Elixir datetime types.

v2.5.1 (2023-10-12)

- Fix typespec for presigned_url/5

v2.5.0 (2023-10-04)

- Increase minimum Elixir version to 1.11
- Add tests for Elixir 1.15
- Merge fix for multipart copy (sort of BREAKING: this changes the signature of
 `upload_part_copy`, however that function could never have worked properly previously so I have
 to assume that nobody was actually using it).
- Add support for optional `start_datetime` opt on `presigned_url/5`
- Allow 0 day triggers for lifecycle rules
- Return the full error on download failure rather than the generic "error downloading file" string

v2.4.0 (2023-01-10)

- Update `presigned_post` to support the same URL options as `presigned_url`.
- Add `bucket_as_host` to `presigned_url_opts`
- Bring minimum Elixir and Erlang versions into line with base ExAws
- Use percent encoding instead of www form for header

v2.3.3 (2022-06-30)

- Update docs
- Support updating lifecycles for non-versioned buckets
- Function spec fixes

v2.3.2 (2021-12-03)
- Support tagging_directive in ExAws.S3.Utils.put_object_headers/1
- Add x-amz-security-token field in presigned_post when required

v2.3.1 (2021-10-18)

- Add `presigned_post` function
- Add `version_id` options to `delete_object`
- Add `timeout` to `upload_opts` typespec

v2.3.0 (2021-07-26)

- Documentation fixes
- Fix `upload_opts` typespec
- Implement `put_bucket_versioning`

v2.2.0 (2021-05-08)

- Increase minimum Elixir version to 1.9
- Correctly escape XML keys in `delete_multiple_objects/3`
- Implement `put_bucket_lifecycle`
- `Upload.perform/2`: Remove redundant `Enum.to_list/1` call
- Use common usage doc between module and readme
- Badges and more badges!
- Various documentation fixes/tweaks
- Add multipart upload error handling
- Add dialyzer check

v2.1.0 (2020-11-18)

- Make optional `:sweet_xml` dependency more obvious in docs
- Update docs to make `:timeout` option more explicit
- Add `versionId` for `get_object`
- Include `:version_id` option in typespec for head_object/3
- Add documentation for `ExAws.S3.download_file/4` for streaming to memory
- Pass through optional headers to presigned_url

v2.0.1 (2019-05-11)

- Improved header signing
- Bug fixes for Elixir 1.6.6 and OTP 21

v2.0 (2018-06-22)

- Major Project Split. Please see the main `ExAws` repository for previous changelogs
