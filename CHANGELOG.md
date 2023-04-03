# Changelog

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
