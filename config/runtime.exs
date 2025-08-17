import Config

if config_env() == :test and System.get_env("MIX_TEST_EX_AWS_MINIO") do
  config :ex_aws,
    access_key_id: System.get_env("MINIO_ROOT_USER") || "minio",
    secret_access_key: System.get_env("MINIO_ROOT_PASSWORD") || "miniosecret",
    s3: [
      scheme: "http://",
      host: System.get_env("MINIO_HOSTNAME") || "localhost",
      port: System.get_env("MINIO_PORT") || 9000,
      region: "us-east-1"
    ]
end
