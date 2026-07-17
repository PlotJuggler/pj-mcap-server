#!/usr/bin/env bash
# ci-integration.sh — local driver for the CI {s3,gcs} integration legs, migrated
# (M6 §5.4, the catalog migration) to the PRODUCTION shape: the Python
# mcap_catalog builder (submodule mcap_catalog/) is the SOLE catalog writer —
# invoked as a real `--once` subprocess against the seeded bucket — and the Go
# side (internal/ws's TestCIIntegration_Backend, -tags=ci_integration) only ever
# opens the resulting SQLite catalog READ-ONLY. This is the SAME test
# .github/workflows/ci.yml runs in GitHub Actions service containers; this
# script exists so the CI legs can be proven GREEN WITHOUT GitHub: it stands up
# its OWN Minio + fake-gcs emulators on FRESH HIGH PORTS (never the dev/smoke/
# matrix ports), seeds both buckets with the deterministic synthetic MCAPs from
# cmd/gen-ci-fixtures under a Hive-partitioned layout (`-hive` — the auryn
# builder only catalogs Hive-partitioned keys; a flat key quarantines instead),
# runs the Python builder + the `ci_integration`-tagged Go test per backend, and
# ALWAYS reaps the containers on exit.
#
# Ports (HIGH, collision-free with :8080 user / :8081 smoke / :8082 matrix /
# :9000 dev-minio / :4443 fake-gcs-convention):
#   Minio S3 API   : 19010   (console 19011)
#   fake-gcs JSON  : 14450
# Override with PJ_CI_MINIO_PORT / PJ_CI_FAKEGCS_PORT if those are taken.
#
# Requires PJ_CI_BUILDER_PYTHON: the path to a python3 interpreter with
# boto3, google-cloud-storage, mcap, and watchdog installed (a bare system
# python3 lacks the cloud SDKs the builder's s3/gcs Source backends import
# lazily) — e.g. bootstrap once with:
#   python3 -m venv ~/.venvs/pj-catalog
#   ~/.venvs/pj-catalog/bin/pip install boto3==1.43.40 google-cloud-storage==3.12.0 mcap==1.4.0 watchdog==6.0.0
# then: PJ_CI_BUILDER_PYTHON=~/.venvs/pj-catalog/bin/python3 make ci-integration
# (S3: pin deliberately — the SAME versions .github/workflows/ci.yml's
# `integration` job installs; bump both places together, on purpose, not by
# letting an unpinned `pip install` silently pick up a new upstream release.)
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
MCAP_CATALOG_DIR="${REPO_ROOT}/mcap_catalog"

# Go is at $HOME/.local/go/bin and not on the default PATH.
export PATH="${HOME}/.local/go/bin:${HOME}/go/bin:${PATH}"
export GOTOOLCHAIN=local

# ── the Python catalog builder interpreter (required — no silent skip here:
# this script is the authoritative "both legs PASS" local gate, so a missing
# venv must fail loudly, not quietly no-op) ──────────────────────────────────
: "${PJ_CI_BUILDER_PYTHON:?PJ_CI_BUILDER_PYTHON must be set to a python3 with boto3, google-cloud-storage, mcap, and watchdog installed (see the header comment above for the one-time venv bootstrap)}"
export PJ_CI_BUILDER_PYTHON
[[ -x "${PJ_CI_BUILDER_PYTHON}" ]] || { echo "[ci-integration] ERROR: PJ_CI_BUILDER_PYTHON=${PJ_CI_BUILDER_PYTHON} is not an executable file" >&2; exit 1; }
[[ -f "${MCAP_CATALOG_DIR}/mcap_catalog_builder/__main__.py" ]] \
  || { echo "[ci-integration] ERROR: vendored builder not found at ${MCAP_CATALOG_DIR}/mcap_catalog_builder (expected in-repo under mcap_catalog/)" >&2; exit 1; }

# ── tunables ─────────────────────────────────────────────────────────────────
MINIO_PORT="${PJ_CI_MINIO_PORT:-19010}"
MINIO_CONSOLE_PORT="${PJ_CI_MINIO_CONSOLE_PORT:-19011}"
FAKEGCS_PORT="${PJ_CI_FAKEGCS_PORT:-14450}"
CI_BUCKET="${PJ_CLOUD_CI_BUCKET:-ci-fixtures}"
MINIO_USER="ciadmin"
MINIO_PASS="cipassword123"

# Image pins — IDENTICAL tags to infra/minio + infra/fake-gcs (ci_facts grounding).
MINIO_IMAGE="minio/minio:RELEASE.2024-06-13T22-53-53Z"
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

# ── 0: generate the synthetic fixtures under a Hive-partitioned layout ───────
# `-hive` (not `-hive-big`, which is smoke.sh-only, for the reconnect-resume
# volume fixture no leg here needs) — the auryn builder ONLY catalogs
# Hive-partitioned keys (mcap_catalog_builder/keyparse.py's HIVE_RE); a flat key
# would quarantine into catalog_failures instead of being served.
FIXTURE_DIR="$(mktemp -d)"
log "generating synthetic Hive-keyed fixtures into ${FIXTURE_DIR}"
( cd "${SERVER_DIR}" && go run ./cmd/gen-ci-fixtures -hive -out "${FIXTURE_DIR}" ) \
  || die "fixture generation"
FIXTURES=()
while IFS= read -r f; do FIXTURES+=("$f"); done < <(find "${FIXTURE_DIR}" -name '*.mcap' | sort)
[[ ${#FIXTURES[@]} -ge 3 ]] || die "expected >= 3 fixtures, got ${#FIXTURES[@]}"

# A private network lets containers reach each other by name (unused by the
# seeding steps below, which all go through host-mapped ports, but kept for any
# future container-to-container need / parity with the CI service-container
# topology).
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

  # Seed the bucket (created idempotently) with the Hive-keyed fixture tree via
  # the Go `seed` tool (server/cmd/seed) — it walks -dir RECURSIVELY and uploads
  # each *.mcap with an S3 key equal to its path relative to -dir, so the
  # Hive-partitioned tree (customer=.../date=.../name.mcap) uploads with its
  # full partitioned key intact. Runs on the HOST against the container's
  # published port (no docker-network mc hop needed).
  log "S3 leg: seeding bucket ${CI_BUCKET} via cmd/seed (${#FIXTURES[@]} fixtures)"
  ( cd "${SERVER_DIR}" && go run ./cmd/seed \
      -dir "${FIXTURE_DIR}" -bucket "${CI_BUCKET}" \
      -endpoint "http://127.0.0.1:${MINIO_PORT}" \
      -access-key "${MINIO_USER}" -secret-key "${MINIO_PASS}" \
      -region us-east-1 -force ) \
    || die "minio seed"

  log "S3 leg: running ci_integration test"
  ( cd "${SERVER_DIR}" && \
    PJ_CLOUD_BACKEND=s3 \
    PJ_CLOUD_CI_BUCKET="${CI_BUCKET}" \
    PJ_CLOUD_S3_ENDPOINT="http://127.0.0.1:${MINIO_PORT}" \
    PJ_CLOUD_S3_ACCESS_KEY="${MINIO_USER}" \
    PJ_CLOUD_S3_SECRET_KEY="${MINIO_PASS}" \
    PJ_CI_BUILDER_PYTHON="${PJ_CI_BUILDER_PYTHON}" \
    go test -tags=ci_integration -count=1 -run TestCIIntegration_Backend -v ./internal/ws/ ) \
    || die "s3 test"
  log "S3 leg: PASS"

  # ── sabotage check: prove the harness is NON-VACUOUS. Upload a byte-garbage
  # "fixture" under an otherwise-valid Hive key into the SAME (still-running)
  # bucket, run the Python builder --once against it in isolation (a throwaway
  # DB — never the DB the test above just proved clean), and assert the auryn
  # builder's quarantine contract: the corrupt file lands in catalog_failures
  # and is NEVER visible in `files` (not served). A harness that failed to
  # catch this would be silently vacuous. ──
  run_sabotage_check

  docker rm -f "${MINIO_CT}" >/dev/null 2>&1 || true
}

# run_sabotage_check — S3-only (backend-agnostic proof; no need to repeat on
# GCS): reuses the S3 leg's still-running Minio + already-seeded bucket.
# Delegates to scripts/sabotage-check.sh (S2 — the shared implementation also
# used by .github/workflows/ci.yml's `integration` job), so the two never
# drift apart. That script owns its own scratch-dir cleanup via an EXIT trap
# (N2), independent of this script's outer cleanup trap.
run_sabotage_check() {
  FAIL_LEG="sabotage"
  log "sabotage check: delegating to scripts/sabotage-check.sh (non-vacuous quarantine proof)"
  SABOTAGE_SERVER_DIR="${SERVER_DIR}" \
  SABOTAGE_MCAP_CATALOG_DIR="${MCAP_CATALOG_DIR}" \
  SABOTAGE_PYTHON="${PJ_CI_BUILDER_PYTHON}" \
  SABOTAGE_S3_ENDPOINT="http://127.0.0.1:${MINIO_PORT}" \
  SABOTAGE_S3_BUCKET="${CI_BUCKET}" \
  SABOTAGE_S3_ACCESS_KEY="${MINIO_USER}" \
  SABOTAGE_S3_SECRET_KEY="${MINIO_PASS}" \
  bash "${SCRIPT_DIR}/sabotage-check.sh" \
    || die "sabotage check failed (see output above)"
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

  # Create the bucket (idempotent) + upload via the JSON media API, preserving
  # each fixture's Hive-partitioned RELATIVE path (not just its basename) as the
  # object name — same pattern as infra/fake-gcs/seed.sh, but recursive over the
  # -hive fixture tree instead of a flat explicit key list.
  log "GCS leg: seeding bucket ${CI_BUCKET} via JSON API (${#FIXTURES[@]} fixtures, Hive-keyed)"
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    "${host}/storage/v1/b?project=pj-cloud-ci" \
    -H "Content-Type: application/json" -d "{\"name\":\"${CI_BUCKET}\"}")"
  case "${code}" in 200|201|409) ;; *) die "gcs bucket create http ${code}";; esac
  local f rel
  for f in "${FIXTURES[@]}"; do
    rel="${f#"${FIXTURE_DIR}"/}"
    code="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
      "${host}/upload/storage/v1/b/${CI_BUCKET}/o?uploadType=media&name=${rel}" \
      -H "Content-Type: application/octet-stream" --data-binary "@${f}")"
    [[ "${code}" == "200" ]] || die "gcs upload ${rel} http ${code}"
  done

  log "GCS leg: running ci_integration test"
  # STORAGE_EMULATOR_HOST MUST include the scheme here (unlike the Go-only
  # conventions elsewhere, e.g. matrix.sh's bare "host:port" / scripts/matrix.sh's
  # FAKE_GCS_HOST): this same env var now ALSO reaches the Python builder
  # subprocess (ci_integration_test.go's runBuilderOnce inherits os.Environ()),
  # and google-cloud-storage's STORAGE_EMULATOR_HOST handling requires a full URL
  # ("despite name, includes scheme" — google/cloud/storage/_helpers.py), while
  # cloud.google.com/go/storage auto-adds a scheme when one isn't supplied — so
  # "http://host:port" satisfies BOTH SDKs.
  ( cd "${SERVER_DIR}" && \
    PJ_CLOUD_BACKEND=gcs \
    PJ_CLOUD_CI_BUCKET="${CI_BUCKET}" \
    STORAGE_EMULATOR_HOST="http://127.0.0.1:${FAKEGCS_PORT}" \
    PJ_CI_BUILDER_PYTHON="${PJ_CI_BUILDER_PYTHON}" \
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
