# ExAws.S3

Service module for https://github.com/ex-aws/ex_aws

## Installation

The package can be installed by adding `ex_aws_s3` to your list of dependencies in `mix.exs`
along with `:ex_aws` and your preferred JSON codec / http client

```elixir
def deps do
  [
    {:ex_aws, "~> 2.0"},
    {:ex_aws_s3, "~> 2.0"},
    {:poison, "~> 3.0"},
    {:hackney, "~> 1.9"},
  ]
end
```

Documentation can be found at [https://hexdocs.pm/ex_aws_s3](https://hexdocs.pm/ex_aws_s3).
