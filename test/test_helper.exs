# Configure access key and secret key with dummy value to make AuthCache work.
System.put_env("AWS_ACCESS_KEY_ID", "dummy")
System.put_env("AWS_SECRET_ACCESS_KEY", "dummy")

ExUnit.start()
