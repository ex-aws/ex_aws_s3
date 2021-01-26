# ExAws.S3

<!-- MDOC !-->

[![CI](https://github.com/ex-aws/ex_aws_s3/workflows/on-push/badge.svg)](https://github.com/ex-aws/ex_aws_s3/actions?query=workflow%3Aon-push)
[![Module Version](https://img.shields.io/hexpm/v/ex_aws_s3.svg)](https://hex.pm/packages/ex_aws_s3)
[![Hex Docs](https://img.shields.io/badge/hex-docs-lightgreen.svg)](https://hexdocs.pm/ex_aws_s3/)
[![Total Download](https://img.shields.io/hexpm/dt/ex_aws_s3.svg)](https://hex.pm/packages/ex_aws_s3)
[![License](https://img.shields.io/hexpm/l/ex_aws_s3.svg)](https://github.com/ex-aws/ex_aws_s3/blob/master/LICENSE)
[![Last Updated](https://img.shields.io/github/last-commit/ex-aws/ex_aws_s3.svg)](https://github.com/ex-aws/ex_aws_s3/commits/master)

Service module for https://github.com/ex-aws/ex_aws

## Installation

The package can be installed by adding `:ex_aws_s3` to your list of dependencies in `mix.exs`
along with `:ex_aws`, your preferred JSON codec / HTTP client, and optionally `:sweet_xml`
to support operations like `list_objects` that require XML parsing.

```elixir
def deps do
  [
    {:ex_aws, "~> 2.0"},
    {:ex_aws_s3, "~> 2.0"},
    {:poison, "~> 3.0"},
    {:hackney, "~> 1.9"},
    {:sweet_xml, "~> 0.6.6"}, # optional dependency
  ]
end
```

## Operations on AWS S3

### Basic Operations

The vast majority of operations here represent a single operation on S3.

#### Examples
```
S3.list_objects |> ExAws.request! #=> %{body: [list, of, objects]}
S3.list_objects |> ExAws.stream! |> Enum.to_list #=> [list, of, objects]

S3.put_object("my-bucket", "path/to/bucket", contents) |> ExAws.request!
```

### Higher Level Operations

There are also some operations which operate at a higher level to make it easier
to download and upload very large files.

Multipart uploads
```
"path/to/big/file"
|> S3.Upload.stream_file
|> S3.upload("my-bucket", "path/on/s3")
|> ExAws.request #=> {:ok, :done}
```
See `ExAws.S3.upload/4` for options

Download large file to disk
```
S3.download_file("my-bucket", "path/on/s3", "path/to/dest/file")
|> ExAws.request #=> {:ok, :done}
```

### More high level functionality

Task.async_stream makes some high level flows so easy you don't need explicit ExAws support.

For example, here is how to concurrently upload many files.

```
upload_file = fn {src_path, dest_path} ->
  S3.put_object("my_bucket", dest_path, File.read!(src_path))
  |> ExAws.request!
end

paths = %{"path/to/src0" => "path/to/dest0", "path/to/src1" => "path/to/dest1"}

paths
|> Task.async_stream(upload_file, max_concurrency: 10)
|> Stream.run
```

### Configuration

The `scheme`, `host`, and `port` can be configured to hit alternate endpoints.

For example, this is how to use a local minio instance:

```elixir
# config.exs
config :ex_aws, :s3,
  scheme: "http://",
  host: "localhost",
  port: 9000
```

<!-- MDOC !-->

## License

The MIT License (MIT)

Copyright (c) 2014 CargoSense, Inc.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
