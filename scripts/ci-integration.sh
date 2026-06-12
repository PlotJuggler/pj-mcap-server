#!/usr/bin/env bash
# ci-integration.sh — local driver for the CI {s3,gcs} integration legs (Plan A
# Task 46 / 46a), the SAME synthetic-fixture in-process Go harness that
# .github/workflows/ci.yml runs in GitHub Actions service containers. It exists so
# the CI legs can be proven GREEN WITHOUT GitHub: it stands up its OWN Minio +
# fake-gcs emulators on FRESH HIGH PORTS (never the dev/smoke/matrix ports), seeds
# both buckets with the deterministic synthetic MCAPs from cmd/gen-ci-fixtures,
# runs the `ci_integration`-tagged Go test per backend, and ALWAYS reaps the
# containers on exit.
#
# It is the AS-BUILT adaptation of the plan's `make integration`: the shell
# smoke/matrix gates can't run in CI (they hard-require the on-disk ground-truth
# corpus + the C++ plugin build), so the CI integration surface is this in-process
# Go harness over synthetic fixtures instead.
#
# Ports (HIGH, collision-free with :8080 user / :8081 smoke / :8082 matrix /
# :9000 dev-minio / :4443 fake-gcs-convention):
#   Minio S3 API   : 19010   (console 19011)
#   fake-gcs JSON  : 14450
# Override with PJ_CI_MINIO_PORT / PJ_CI_FAKEGCS_PORT if those are taken.
#
# Final line is exactly one of:
#   CI-INTEGRATION PASS
#   CI-INTEGRATION FAIL: <leg>
# and the exit code matches (0 / non-zero).
set -euo pipefail

# ── resolve paths ────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SERVER_DIR="${REPO_ROOT}/server"

# Go is at $HOME/.local/go/bin and not on the default PATH.
export PATH="${HOME}/.local/go/bin:${HOME}/go/bin:${PATH}"
export GOTOOLCHAIN=local

# ── tunables ─────────────────────────────────────────────────────────────────
MINIO_PORT="${PJ_CI_MINIO_PORT:-19010}"
MINIO_CONSOLE_PORT="${PJ_CI_MINIO_CONSOLE_PORT:-19011}"
FAKEGCS_PORT="${PJ_CI_FAKEGCS_PORT:-14450}"
CI_BUCKET="${PJ_CLOUD_CI_BUCKET:-ci-fixtures}"
MINIO_USER="ciadmin"
MINIO_PASS="cipassword123"

# Image pins — IDENTICAL tags to infra/minio + infra/fake-gcs (ci_facts grounding).
MINIO_IMAGE="minio/minio:RELEASE.2024-06-13T22-53-53Z"
MC_IMAGE="minio/mc:RELEASE.2024-06-12T14-34-03Z"
FAKEGCS_IMAGE="fsouza/fake-gcs-server:1.49.2"

# Unique container/volume names so parallel runs / leftovers don't collide.
SUFFIX="$$"
MINIO_CT="pjci-minio-${SUFFIX}"
FAKEGCS_CT="pjci-fakegcs-${SUFFIX}"
NET="pjci-net-${SUFFIX}"

FIXTURE_DIR=""

log()  { printf '[ci-integration] %s\n' "$*"; }
die()  { printf '[ci-integration] ERROR: %s\n' "$*" >&2; FAIL_LEG="${FAIL_LEG:-$*}"; exit 1; }

FAIL_LEG=""
cleanup() {
  local rc=$?
  set +e
  log "reaping containers / network / scratch"
  docker rm -f "${MINIO_CT}" >/dev/null 2>&1
  docker rm -f "${FAKEGCS_CT}" >/dev/null 2>&1
  docker network rm "${NET}" >/dev/null 2>&1
  [[ -n "${FIXTURE_DIR}" && -d "${FIXTURE_DIR}" ]] && rm -rf "${FIXTURE_DIR}"
  if [[ ${rc} -eq 0 ]]; then
    echo "CI-INTEGRATION PASS"
  else
    echo "CI-INTEGRATION FAIL: ${FAIL_LEG:-unknown}"
  fi
  exit "${rc}"
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || die "docker not on PATH"

# ── 0: generate the synthetic fixtures ───────────────────────────────────────
FIXTURE_DIR="$(mktemp -d)"
log "generating synthetic fixtures into ${FIXTURE_DIR}"
( cd "${SERVER_DIR}" && go run ./cmd/gen-ci-fixtures -out "${FIXTURE_DIR}" ) \
  || die "fixture generation"
FIXTURES=()
while IFS= read -r f; do FIXTURES+=("$f"); done < <(find "${FIXTURE_DIR}" -name '*.mcap' | sort)
[[ ${#FIXTURES[@]} -ge 3 ]] || die "expected >= 3 fixtures, got ${#FIXTURES[@]}"

# A private network lets the mc one-shot reach the minio container by name.
docker network create "${NET}" >/dev/null 2>&1 || true

# ════════════════════════════════════════════════════════════════════════════
# S3 LEG (Minio)
# ════════════════════════════════════════════════════════════════════════════
run_s3_leg() {
  FAIL_LEG="s3"
  log "S3 leg: starting Minio on :${MINIO_PORT} (image ${MINIO_IMAGE})"
  docker run -d --name "${MINIO_CT}" --network "${NET}" \
    -p "127.0.0.1:${MINIO_PORT}:9000" -p "127.0.0.1:${MINIO_CONSOLE_PORT}:9001" \
    -e "MINIO_ROOT_USER=${MINIO_USER}" -e "MINIO_ROOT_PASSWORD=${MINIO_PASS}" \
    "${MINIO_IMAGE}" server /data --console-address ":9001" >/dev/null \
    || die "minio start"

  # Wait for the S3 API to answer (live/ready endpoint).
  local up=""
  for _ in $(seq 1 30); do
    if curl -fsS -m 2 -o /dev/null "http://127.0.0.1:${MINIO_PORT}/minio/health/ready" 2>/dev/null; then
      up=1; break
    fi
    sleep 1
  done
  [[ -n "${up}" ]] || die "minio not ready on :${MINIO_PORT}"

  # Create the bucket + upload the fixtures with a one-shot mc container on the
  # same network (no host mc needed). Mount the fixture dir read-only.
  log "S3 leg: seeding bucket ${CI_BUCKET} via mc (${#FIXTURES[@]} fixtures)"
  local cp_cmds="" f base
  for f in "${FIXTURES[@]}"; do
    base="$(basename "$f")"
    cp_cmds="${cp_cmds} && mc cp -q /fixtures/${base} local/${CI_BUCKET}/${base}"
  done
  docker run --rm --network "${NET}" \
    -v "${FIXTURE_DIR}:/fixtures:ro" \
    --entrypoint /bin/sh "${MC_IMAGE}" -c "
      mc alias set local http://${MINIO_CT}:9000 ${MINIO_USER} ${MINIO_PASS} &&
      mc mb -p local/${CI_BUCKET} ${cp_cmds}
    " >/dev/null || die "minio seed"

  log "S3 leg: running ci_integration test"
  ( cd "${SERVER_DIR}" && \
    PJ_CLOUD_BACKEND=s3 \
    PJ_CLOUD_CI_BUCKET="${CI_BUCKET}" \
    PJ_CLOUD_S3_ENDPOINT="http://127.0.0.1:${MINIO_PORT}" \
    PJ_CLOUD_S3_ACCESS_KEY="${MINIO_USER}" \
    PJ_CLOUD_S3_SECRET_KEY="${MINIO_PASS}" \
    go test -tags=ci_integration -count=1 -run TestCIIntegration_Backend -v ./internal/ws/ ) \
    || die "s3 test"

  docker rm -f "${MINIO_CT}" >/dev/null 2>&1 || true
  log "S3 leg: PASS"
}

# ════════════════════════════════════════════════════════════════════════════
# GCS LEG (fake-gcs-server)
# ════════════════════════════════════════════════════════════════════════════
run_gcs_leg() {
  FAIL_LEG="gcs"
  log "GCS leg: starting fake-gcs on :${FAKEGCS_PORT} (image ${FAKEGCS_IMAGE})"
  docker run -d --name "${FAKEGCS_CT}" \
    -p "127.0.0.1:${FAKEGCS_PORT}:4443" \
    "${FAKEGCS_IMAGE}" \
    -scheme http -host 0.0.0.0 -port 4443 \
    -public-host "127.0.0.1:${FAKEGCS_PORT}" -backend memory -log-level warn >/dev/null \
    || die "fake-gcs start"

  local host="http://127.0.0.1:${FAKEGCS_PORT}"
  local up=""
  for _ in $(seq 1 30); do
    if curl -fsS -m 2 -o /dev/null "${host}/storage/v1/b" 2>/dev/null; then up=1; break; fi
    sleep 1
  done
  [[ -n "${up}" ]] || die "fake-gcs not ready on :${FAKEGCS_PORT}"

  # Create the bucket (idempotent) + upload via the JSON media API (same pattern
  # as infra/fake-gcs/seed.sh, but with the synthetic fixtures).
  log "GCS leg: seeding bucket ${CI_BUCKET} via JSON API"
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    "${host}/storage/v1/b?project=pj-cloud-ci" \
    -H "Content-Type: application/json" -d "{\"name\":\"${CI_BUCKET}\"}")"
  case "${code}" in 200|201|409) ;; *) die "gcs bucket create http ${code}";; esac
  local f base
  for f in "${FIXTURES[@]}"; do
    base="$(basename "$f")"
    code="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
      "${host}/upload/storage/v1/b/${CI_BUCKET}/o?uploadType=media&name=${base}" \
      -H "Content-Type: application/octet-stream" --data-binary "@${f}")"
    [[ "${code}" == "200" ]] || die "gcs upload ${base} http ${code}"
  done

  log "GCS leg: running ci_integration test"
  ( cd "${SERVER_DIR}" && \
    PJ_CLOUD_BACKEND=gcs \
    PJ_CLOUD_CI_BUCKET="${CI_BUCKET}" \
    STORAGE_EMULATOR_HOST="127.0.0.1:${FAKEGCS_PORT}" \
    go test -tags=ci_integration -count=1 -run TestCIIntegration_Backend -v ./internal/ws/ ) \
    || die "gcs test"

  docker rm -f "${FAKEGCS_CT}" >/dev/null 2>&1 || true
  log "GCS leg: PASS"
}

# ── run requested legs (default both; LEG=s3|gcs to scope) ───────────────────
LEG="${LEG:-both}"
case "${LEG}" in
  s3)   run_s3_leg ;;
  gcs)  run_gcs_leg ;;
  both) run_s3_leg; run_gcs_leg ;;
  *)    die "unknown LEG=${LEG} (want s3|gcs|both)" ;;
esac

FAIL_LEG=""
# cleanup trap prints CI-INTEGRATION PASS on rc=0.
