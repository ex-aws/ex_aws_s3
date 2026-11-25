import Config

if config_env() == :test and System.get_env("MIX_TEST_EX_AWS_MINIO") do
  config :ex_aws,
    access_key_id: System.get_env("MINIO_ROOT_USER") || "minio",
    secret_access_key: System.get_env("MINIO_ROOT_PASSWORD") || "miniosecret",
    s3: [
      scheme: System.get_env("MINIO_SCHEME") || "https://",
      host: System.get_env("MINIO_HOSTNAME") || "localhost",
      port: System.get_env("MINIO_PORT") || 9000,
      region: System.get_env("MINIO_REGION") || "us-east-1"
    ]
end

if config_env() == :test and System.get_env("MIX_TEST_EX_AWS_HETZNER") do
  config :ex_aws,
    access_key_id: System.get_env("S3_ROOT_USER"),
    secret_access_key: System.get_env("S3_ROOT_PASSWORD"),
    sse_customer_key: System.get_env("SSE_CUSTOMER_KEY"),
    bucket: System.get_env("S3_BUCKET"),
    s3: [
      scheme: System.get_env("S3_SCHEME") || "https://",
      host: System.get_env("S3_HOSTNAME"),
      port: System.get_env("S3_PORT") || 443,
      region: System.get_env("S3_REGION") || "fsn1"
    ]
end
