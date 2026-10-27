#!/usr/bin/env bash
# Generate the self-signed local-demo certificate; never commit the private key.
set -euo pipefail
project_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cert_dir="$project_dir/config/certs"
mkdir -p "$cert_dir"
if [ -s "$cert_dir/lab.pem" ]; then
  echo "Using existing lab certificate: $cert_dir/lab.pem"
  exit 0
fi
command -v openssl >/dev/null || { echo "Install openssl before generating the lab certificate" >&2; exit 1; }
umask 077
openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
  -subj "/CN=HA Trading Lab" \
  -addext "subjectAltName=IP:127.0.0.1,DNS:localhost,DNS:haproxy-1,DNS:haproxy-2" \
  -keyout "$cert_dir/lab.key" -out "$cert_dir/lab.crt"
cat "$cert_dir/lab.crt" "$cert_dir/lab.key" > "$cert_dir/lab.pem"
# The unprivileged HAProxy container must read this local-demo PEM bind mount.
chmod 644 "$cert_dir/lab.pem"
echo "Created self-signed local-demo certificate: $cert_dir/lab.pem"
