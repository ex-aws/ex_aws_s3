# Changelog

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
