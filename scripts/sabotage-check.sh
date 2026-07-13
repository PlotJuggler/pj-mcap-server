#!/usr/bin/env bash
# scripts/sabotage-check.sh — shared "prove the harness is non-vacuous" red-team
# check (S2): uploads a byte-garbage "fixture" under an otherwise-valid Hive key
# into an S3-compatible bucket, runs the Python mcap_catalog builder `--once`
# against it in ISOLATION (a throwaway DB, never a DB any caller's own
# assertions already ran against), and asserts the auryn builder's quarantine
# contract: the corrupt file lands in catalog_failures and is NEVER visible in
# `files` (not served). A harness that failed to catch this would be silently
# vacuous.
#
# Used by BOTH:
#   - scripts/ci-integration.sh (S3 leg): reuses its already-running Minio +
#     already-seeded bucket, same key prefix as its real fixtures (harmless —
#     it runs AFTER that leg's own assertions have already completed).
#   - .github/workflows/ci.yml's `integration` job (s3 leg only — this is a
#     backend-agnostic proof of the BUILDER's quarantine contract, not of any
#     storage backend, so it need not repeat on gcs): runs as a distinct
#     blocking step against the job's own Minio service container, under a
#     KEY PREFIX distinct from the real seeded fixtures so it can never
#     perturb the main leg's own catalog / assertions.
#
# Required environment:
#   SABOTAGE_SERVER_DIR       - path to server/ (for `go run ./cmd/seed`)
#   SABOTAGE_MCAP_CATALOG_DIR - path to the mcap_catalog/ submodule
#   SABOTAGE_PYTHON           - python3 interpreter with boto3, mcap, watchdog,
#                               google-cloud-storage installed
#   SABOTAGE_S3_ENDPOINT      - e.g. http://127.0.0.1:19010
#   SABOTAGE_S3_BUCKET        - bucket to upload the corrupt fixture into
#   SABOTAGE_S3_ACCESS_KEY / SABOTAGE_S3_SECRET_KEY
# Optional:
#   SABOTAGE_S3_REGION        - default us-east-1
#   SABOTAGE_KEY_PREFIX       - Hive path prefix for the corrupt object
#                               (default below). Callers that upload their own
#                               real fixtures under the SAME bucket at a
#                               different, already-cataloged prefix should pass
#                               a DISTINCT prefix here so this red-team upload
#                               can never collide with / perturb their own
#                               catalog rows or assertions.
#
# Prints one diagnostic line and exits non-zero on any failure, including the
# two quarantine-contract assertions. Cleans up its own scratch dirs on both
# success and failure (N2: cleanup lives in a trap, not only on the happy path).
set -euo pipefail

: "${SABOTAGE_SERVER_DIR:?SABOTAGE_SERVER_DIR must be set}"
: "${SABOTAGE_MCAP_CATALOG_DIR:?SABOTAGE_MCAP_CATALOG_DIR must be set}"
: "${SABOTAGE_PYTHON:?SABOTAGE_PYTHON must be set}"
: "${SABOTAGE_S3_ENDPOINT:?SABOTAGE_S3_ENDPOINT must be set}"
: "${SABOTAGE_S3_BUCKET:?SABOTAGE_S3_BUCKET must be set}"
: "${SABOTAGE_S3_ACCESS_KEY:?SABOTAGE_S3_ACCESS_KEY must be set}"
: "${SABOTAGE_S3_SECRET_KEY:?SABOTAGE_S3_SECRET_KEY must be set}"
SABOTAGE_S3_REGION="${SABOTAGE_S3_REGION:-us-east-1}"
SABOTAGE_KEY_PREFIX="${SABOTAGE_KEY_PREFIX:-customer=test/customer_site=lab/robot=r1/source=synthetic/date=2026-06-22}"

log() { printf '[sabotage-check] %s\n' "$*"; }
die() { printf '[sabotage-check] ERROR: %s\n' "$*" >&2; exit 1; }

# N2: all scratch dirs are reaped in a trap (success OR failure), not only
# after the last assertion — a `die` partway through must not leak them.
SCRATCH_DIRS=()
cleanup() {
  local d
  for d in "${SCRATCH_DIRS[@]:-}"; do
    [[ -n "${d}" && -e "${d}" ]] && rm -rf "${d}"
  done
}
trap cleanup EXIT

log "uploading a corrupt Hive-keyed 'fixture' and confirming the Python builder quarantines it (proves the harness would catch a real corruption bug, not pass vacuously)"

corrupt_name="ci_synth_sabotage.mcap"
corrupt_rel="${SABOTAGE_KEY_PREFIX%/}/${corrupt_name}"
corrupt_dir="$(mktemp -d)"; SCRATCH_DIRS+=("${corrupt_dir}")
mkdir -p "${corrupt_dir}/$(dirname "${corrupt_rel}")"
corrupt_file="${corrupt_dir}/${corrupt_rel}"
printf 'THIS IS NOT A VALID MCAP FILE -- scripts/sabotage-check.sh\n' > "${corrupt_file}"

# Upload just the one corrupt file via the Go `seed` tool (server/cmd/seed) —
# additive PUT, so any real fixtures already in the bucket are untouched.
( cd "${SABOTAGE_SERVER_DIR}" && go run ./cmd/seed \
    -dir "${corrupt_dir}" -bucket "${SABOTAGE_S3_BUCKET}" \
    -endpoint "${SABOTAGE_S3_ENDPOINT}" \
    -access-key "${SABOTAGE_S3_ACCESS_KEY}" -secret-key "${SABOTAGE_S3_SECRET_KEY}" \
    -region "${SABOTAGE_S3_REGION}" -force ) \
  || die "upload of the corrupt fixture failed"

sabotage_db_dir="$(mktemp -d)"; SCRATCH_DIRS+=("${sabotage_db_dir}")
sabotage_db="${sabotage_db_dir}/sabotage.db"
sabotage_log="$(mktemp)"; SCRATCH_DIRS+=("${sabotage_log}")
( cd "${SABOTAGE_MCAP_CATALOG_DIR}" && env \
    AWS_ACCESS_KEY_ID="${SABOTAGE_S3_ACCESS_KEY}" AWS_SECRET_ACCESS_KEY="${SABOTAGE_S3_SECRET_KEY}" \
    AWS_ENDPOINT_URL="${SABOTAGE_S3_ENDPOINT}" \
    AWS_REGION="${SABOTAGE_S3_REGION}" AWS_DEFAULT_REGION="${SABOTAGE_S3_REGION}" \
    "${SABOTAGE_PYTHON}" -m mcap_catalog_builder --source s3 --s3-bucket "${SABOTAGE_S3_BUCKET}" \
    --once --db "${sabotage_db}" --log-level INFO ) >"${sabotage_log}" 2>&1 \
  || { cat "${sabotage_log}"; die "builder --once failed unexpectedly (want a clean quarantine, not a crash)"; }

failcount="$(python3 - "${sabotage_db}" "${corrupt_rel}" <<'PY'
import sqlite3, sys
db, key = sys.argv[1], sys.argv[2]
conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
row = conn.execute("SELECT COUNT(*) FROM catalog_failures WHERE s3_key=?", (key,)).fetchone()
print(row[0])
PY
)"
[[ "${failcount}" == "1" ]] \
  || { cat "${sabotage_log}"; die "corrupt fixture NOT quarantined into catalog_failures (found ${failcount} rows for ${corrupt_rel}) -- the harness would silently miss a real corruption bug"; }

servedcount="$(python3 - "${sabotage_db}" "${corrupt_name}" <<'PY'
import sqlite3, sys
db, name = sys.argv[1], sys.argv[2]
conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
row = conn.execute("SELECT COUNT(*) FROM files WHERE filename=?", (name,)).fetchone()
print(row[0])
PY
)"
[[ "${servedcount}" == "0" ]] \
  || { cat "${sabotage_log}"; die "corrupt fixture WAS served as a healthy files row (count=${servedcount}) -- the quarantine contract broke"; }

log "OK (corrupt fixture quarantined into catalog_failures, not served -- the harness is non-vacuous)"
