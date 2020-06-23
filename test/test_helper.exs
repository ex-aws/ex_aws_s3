# Configure access key and secret key with dummy value to make AuthCache work.
System.put_env("AWS_ACCESS_KEY_ID", "dummy")
System.put_env("AWS_SECRET_ACCESS_KEY", "dummy")

# Mox - define the HTTP client mock and configure ex_aws to use it.
Application.ensure_all_started(:mox)
Mox.defmock(ExAws.S3.HttpClientMock, for: ExAws.Request.HttpClient)
Application.put_env(:ex_aws, :http_client, ExAws.S3.HttpClientMock)

ExUnit.start()
