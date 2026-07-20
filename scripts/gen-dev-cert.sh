#!/usr/bin/env bash
# gen-dev-cert.sh — generate a self-signed TLS cert/key for localhost dev/test.
#
# The pj-cloud-server serves TLS (wss:// + https dashboard) when
# server.tls.cert + server.tls.key are set (spec §8.6). This produces a
# throwaway self-signed pair valid for localhost / 127.0.0.1 / ::1 so you can
# exercise the TLS path locally. Clients verify against it with skip-verify:
#   - mcap-cloud-cli: allow_insecure (ixwebsocket TLS option)
#   - devprobe:         -insecure
#
# Usage:
#   scripts/gen-dev-cert.sh [OUT_DIR]
# Default OUT_DIR: /tmp/pj-cloud-dev-tls
# Outputs: $OUT_DIR/server.crt, $OUT_DIR/server.key
set -euo pipefail

OUT_DIR="${1:-/tmp/pj-cloud-dev-tls}"
mkdir -p "${OUT_DIR}"

CRT="${OUT_DIR}/server.crt"
KEY="${OUT_DIR}/server.key"

command -v openssl >/dev/null || { echo "openssl not found on PATH" >&2; exit 1; }

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "${KEY}" -out "${CRT}" \
  -days 365 \
  -subj "/CN=localhost/O=pj-cloud-dev" \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1" \
  2>/dev/null

chmod 600 "${KEY}"
echo "wrote:"
echo "  cert: ${CRT}"
echo "  key:  ${KEY}"
echo
echo "point the server at them, e.g.:"
echo "  pj-cloud-server -listen :8443 ... (with server.tls in the config), or set"
echo "  PJ_CLOUD_TLS_CERT=${CRT} PJ_CLOUD_TLS_KEY=${KEY} via a config file."
