# Elixir/Docker image used for in-container tasks.

elixir_image := env("elixir_image", "elixir:1.18.4-otp-26-alpine")

# MinIO image and container name for the S3 integration tests.
# Override to match CI's pinned tag, e.g.
#   minio_image=quay.io/minio/minio:RELEASE.2025-07-23T15-54-02Z just in-docker-minio-test

minio_image := env("minio_image", "minio/minio:latest")
minio_container := "ex-aws-s3-minio"

# show all recipes
@recipes:
    just --list --justfile {{justfile()}}

# Run the test suite inside a Docker container.
in-docker-test:
    docker run --rm \
        -v "{{ justfile_directory() }}:/app" \
        -w /app \
        "{{ elixir_image }}" \
        sh -c "mix local.hex --force && mix local.rebar --force && mix deps.get && mix test"

# Run the MinIO integration tests against a real S3 endpoint in Docker.
# Starts a MinIO container with TLS (self-signed cert), waits for it to be
# ready, then runs the `:minio`-tagged tests (via the `test.minio` alias)
# in the Elixir container.

# Run the tests against a docker minio container
in-docker-minio-test:
    #!/usr/bin/env bash
    set -euo pipefail

    # Generate a self-signed TLS certificate for MinIO
    certs_dir="$(mktemp -d)"
    echo "Certs dir: ${certs_dir}"
    trap 'docker rm -f {{ minio_container }} >/dev/null 2>&1 || true; rm -rf "$certs_dir"' EXIT

    openssl req -x509 -newkey rsa:2048 -nodes \
        -keyout "$certs_dir/private.key" \
        -out "$certs_dir/public.crt" \
        -days 1 \
        -subj "/CN=localhost" \
        -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
        2>/dev/null

    docker rm -f {{ minio_container }} >/dev/null 2>&1 || true
    docker run -d \
        --name {{ minio_container }} \
        -p 9000:9000 \
        -v "$certs_dir:/root/.minio/certs:ro" \
        -e MINIO_ROOT_USER=minio \
        -e MINIO_ROOT_PASSWORD=miniosecret \
        -e MINIO_NOTIFY_WEBHOOK_ENABLE_testhook=on \
        -e MINIO_NOTIFY_WEBHOOK_ENDPOINT_testhook=http://localhost:9999 \
        {{ minio_image }} \
        server /data --address ":9000"

    echo "Waiting for MinIO (TLS) to be ready..."
    timeout 60s bash -c 'until curl -fsk https://localhost:9000/minio/health/live >/dev/null; do echo "  not ready yet..."; sleep 2; done'

    docker run --rm \
        --network host \
        -v "{{ justfile_directory() }}:/app" \
        -v "$certs_dir:/minio/certs:ro" \
        -w /app \
        -e MIX_TEST_EX_AWS_MINIO=true \
        -e MINIO_SCHEME=https:// \
        -e MINIO_HOSTNAME=localhost \
        -e MINIO_PORT=9000 \
        -e MINIO_CA_CERT=/minio/certs/public.crt \
        "{{ elixir_image }}" \
        sh -c "mix local.hex --force && mix local.rebar --force && mix deps.get && mix test.minio"

# create .sops.yaml
#creation_rules:
#  - path_regex: .*.json
#    key_groups:
#      - age:
#          - the-public-key
# credentials file: .hetzner-credentials.json
# {
#         "HETZNER_HOSTNAME": "fsn1.your-objectstorage.com",
#         "HETZNER_BUCKET": "my-bucket-name",
#         "HETZNER_REGION": "fsn1",
#         "HETZNER_USER": "",
#         "HETZNER_PASSWORD": "",
#         "HETZNER_SSE_CUSTOMER_ALGORITHM": "AES256",
#         "HETZNER_SSE_CUSTOMER_KEY": "" # 32 char random key
# }
# encrypt it: sops encrypt --in-place .hetzner-credentials.json
# Run the tests against a real hetzner object storage
in-docker-hetzner-test CREDENTIALS=".hetzner-credentials.json":
    #!/usr/bin/env bash
    set -euo pipefail

    docker run --rm \
        --network host \
        -v "{{ justfile_directory() }}:/app" \
        -w /app \
        -e MIX_TEST_EX_AWS_HETZNER=true \
        -e S3_ROOT_USER=$(sops decrypt {{ CREDENTIALS }} | jq -r .HETZNER_USER) \
        -e S3_ROOT_PASSWORD=$(sops decrypt {{ CREDENTIALS }} | jq -r .HETZNER_PASSWORD) \
        -e S3_BUCKET=$(sops decrypt {{ CREDENTIALS }} | jq -r .HETZNER_BUCKET) \
        -e S3_SCHEME="https://" \
        -e S3_HOSTNAME=$(sops decrypt {{ CREDENTIALS }} | jq -r .HETZNER_HOSTNAME) \
        -e S3_PORT=443 \
        -e S3_REGION=$(sops decrypt {{ CREDENTIALS }} | jq -r .HETZNER_REGION) \
        -e SSE_CUSTOMER_KEY=$(sops decrypt {{ CREDENTIALS }} | jq -r .HETZNER_SSE_CUSTOMER_KEY) \
        "{{ elixir_image }}" \
        sh -c "mix local.hex --force && mix local.rebar --force && mix deps.get && mix test.hetzner"

# run hex format
format:
    #!/usr/bin/env bash
    set -euo pipefail

    docker run --rm \
        --network host \
        -v "{{ justfile_directory() }}:/app" \
        -w /app \
        "{{ elixir_image }}" \
        sh -c "mix local.hex --force && mix local.rebar --force && mix deps.get && mix format"

# run hex dialyzer
dialyzer:
    #!/usr/bin/env bash
    set -euo pipefail

    docker run --rm \
        --network host \
        -v "{{ justfile_directory() }}:/app" \
        -w /app \
        "{{ elixir_image }}" \
        sh -c "mix local.hex --force && mix local.rebar --force && mix deps.get && mix dialyzer"