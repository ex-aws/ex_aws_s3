ExUnit.configure(exclude: [:minio, :hetzner])

IO.puts("Loading test_helpers")

if System.get_env("MIX_TEST_EX_AWS_MINIO") do
  Application.put_env(:ex_aws, :hackney_opts,
    insecure: true,
    recv_timeout: 30_000
  )
end

ExUnit.start()
