# Changelog

v2.6.1 (2026-11-25)
- Fix: `delete_multiple_objects/3` (and `delete_all_objects/3`, which uses it) deleted nothing with `ex_aws` 2.8.0 and later. Those releases percent-encode the S3 request path, so the `/?delete` path was sent as the object key `?delete`. `delete` is now sent as a sub-resource, which works with every `ex_aws` 2.x release.
- Require Elixir 1.17 or later (and so OTP 27 or later), matching the minimum versions required by `ex_aws` 2.8.2.

v2.6.0 (2026-11-24)
- Security: XML-escape all caller-supplied values that are placed inside XML request bodies. Previously these were interpolated verbatim, so a value containing XML metacharacters could inject additional elements into the request. Fixes GHSA-pcgv-phrm-r85v (`put_bucket_lifecycle/2`), GHSA-cf93-7gwh-6w98 (bucket logging, notification, replication and website configuration), GHSA-979q-5hj9-vvvv (bucket and object tagging) and GHSA-mwxg-gfj8-x32v (version ids in `delete_multiple_objects/3`). The same fix is applied to `put_bucket/3` (region), `put_bucket_cors/2`, `put_bucket_logging/2`, `put_bucket_notification/2`, `put_bucket_replication/2`, `put_bucket_tagging/2`, `put_object_tagging/4`, `put_bucket_website/2`, `post_object_restore/4`, `delete_multiple_objects/3` (version ids) and `complete_multipart_upload/4` (ETags).
- Security: reject grantee identifiers containing `"`, `,`, control or non-ASCII characters in the `grant_read`, `grant_write`, `grant_read_acp`, `grant_write_acp` and `grant_full_control` options. An untrusted grantee value could previously inject additional grantees (including the `AllUsers` group) into the `x-amz-grant-*` headers (GHSA-6fg2-v8g4-vfg2).
- Security: reject header names and values containing control characters (such as CR/LF) in every request builder. Previously `:meta` keys/values and other header options were interpolated into `x-amz-meta-*` and other request headers verbatim, allowing HTTP header injection when they were derived from untrusted input (GHSA-5887-vcfr-5ff8).
- Security: `presigned_url/5` and `presigned_post/4` now reject bucket names containing anything other than letters, digits, `.`, `-` and `_` when the bucket is used as part of the hostname (`virtual_host: true`, `s3_accelerate: true` or `bucket_as_host: true`). Previously a bucket name such as `attacker.example/x?` produced a URL pointing at `attacker.example` (GHSA-p62r-cgcg-9fm7).
- Security: add the opt-in `bucket_as_host_allowlist` option to `presigned_url/5` and `presigned_post/4`. With `bucket_as_host: true` the bucket name is the whole hostname, so a syntactically valid name such as `attacker.example` still becomes the host of the presigned URL or POST form action; the character check above cannot prevent that. Never pass an untrusted bucket name with `bucket_as_host: true`, or set `bucket_as_host_allowlist` to the list of trusted hostnames so any other bucket raises `ArgumentError`. The option falls back to the `bucket_as_host_allowlist` key of the S3 config (the setting `ex_aws` checks for requests, see GHSA-rcv9-2jx2-63xh); reading it from the config needs an `ex_aws` release that supports that key.
- Behaviour change: `presigned_url/5` and `presigned_post/4` raise `ArgumentError` for a bucket name rejected by the checks above. `presigned_url/5` does not return `{:error, reason}` for it, so callers that derive the bucket from untrusted input should validate it first or rescue the exception.
- Security: `presigned_url/5` no longer lets a `?` in the object key (or in a path style bucket name) start the query string. A key such as `report.pdf?acl` used to be presigned as the object `report.pdf` with the `acl` sub-resource, and `export.dump?.txt` as the object `export.dump`. The key is now handed to `ex_aws` as a path of its own and percent-encoded whole (GHSA-96q9-mcc7-hqj8). This needs an `ex_aws` release that includes the fix for that advisory; with an older `ex_aws`, a bucket or key containing `?` raises `ArgumentError` instead of being presigned for a different object.
- Behaviour change: query parameters appended to the object key (`presigned_url(config, :get, bucket, "key?versionId=1")`) are no longer signed as query parameters. Pass them with the `:query_params` option.
- Security: percent-encode the bucket and object key in the `x-amz-copy-source` header built by `upload_part_copy/8`, matching `put_object_copy/5`. Previously reserved characters (including CR/LF) in the source key were sent verbatim, allowing a corrupted copy source or HTTP header injection when the key is attacker-controlled (GHSA-27g7-88jv-xqhq).
- Fix: `put_object_copy/5` and `upload_part_copy/8` no longer drop empty segments from the source key. `a//b` and `dir/` used to be sent as `a/b` and `dir`, which name different objects. A single leading `/` is still dropped, so `/key` keeps meaning `key`.
- `.` and `..` segments of the source key are sent as `%2E` and `%2E%2E` in `x-amz-copy-source`. S3 decodes them back to the same literal key.
- Behaviour change: because the source key passed to `upload_part_copy/8` is now percent-encoded, a `?versionId=...` suffix appended to it is treated as part of the key name rather than as a version selector, as was already the case for `put_object_copy/5`.

v2.5.9 (2025-12-09)
- Implement bucket configuration operations `put_bucket_logging/2`, `put_bucket_notification/2`, `put_bucket_request_payment/2`, and `put_bucket_website/2`.
- Fix `put_object` typespec
- Remove deprecated Owner DisplayName field. This was removed by AWS on November 21, 2025.
- Add integration tests for MinIO
- Add streaming support for `list_object_versions/3`

v2.5.8 (2025-08-12)
- Rename `get_bucket_object_versions` to `list_object_versions`

v2.5.7 (2025-04-15)

- Add support for `If-Match` and `If-None-Match` headers to `PutObject` call
- Handle errors in `parse_bucket_object_versions/1`

v2.5.6 (2024-12-13)

- Fix compilation warning on Elixir 1.17

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
