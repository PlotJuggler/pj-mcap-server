#!/usr/bin/env bash
# seed.sh — idempotently seed the fake-gcs `recordings` bucket with EXACTLY the 8
# ground-truth MCAPs (the same corpus the Minio dev bucket holds). Uploads via the
# fake-gcs JSON upload API (uploadType=media) so re-running overwrites in place.
#
# Why an explicit list, not a glob over the dataset dir: /home/gn/ws/jkk_dataset02
# contains EXTRAS — `nissan_zala_50_zeg_2_0 (Copy).mcap` (a duplicate of zeg_2 that
# would overlap its time range and break stitch/matrix invariants) plus non-mcap
# notes. Seeding exactly these 8 keeps the GCS leg's pinned counts identical to the
# S3 corpus (zeg_1=33670, imu=14904, Σ8=337861).
#
# Usage: seed.sh [HOST] [GROUND_TRUTH_DIR]
#   HOST default http://localhost:4443 ; GROUND_TRUTH_DIR default /home/gn/ws/jkk_dataset02
# Exit 0 on success; non-zero (and a message) on any failure.
set -euo pipefail

HOST="${1:-http://localhost:4443}"
GTD="${2:-/home/gn/ws/jkk_dataset02}"
BUCKET="recordings"

# The 8 ground-truth keys (lockstep with scripts/matrix.sh M5_KEYS + smoke.sh).
KEYS=(
  nissan_zala_50_sagod_0.mcap
  nissan_zala_50_zeg_1_0.mcap
  nissan_zala_50_zeg_2_0.mcap
  nissan_zala_50_zeg_3_0.mcap
  nissan_zala_50_zeg_4_0.mcap
  nissan_zala_90_country_road_1_0.mcap
  nissan_zala_90_country_road_2_0.mcap
  nissan_zala_90_mixed_0.mcap
)

log() { printf '[fake-gcs-seed] %s\n' "$*"; }
die() { printf '[fake-gcs-seed] ERROR: %s\n' "$*" >&2; exit 1; }

command -v curl >/dev/null 2>&1 || die "curl not on PATH"
[[ -d "${GTD}" ]] || die "ground-truth dir ${GTD} not present"

# Wait until the JSON API answers (compose --wait already gates on the healthcheck,
# but be defensive when run standalone).
for _ in $(seq 1 30); do
  curl -fsS -m 3 -o /dev/null "${HOST}/storage/v1/b" 2>/dev/null && break
  sleep 1
done
curl -fsS -m 3 -o /dev/null "${HOST}/storage/v1/b" 2>/dev/null || die "fake-gcs JSON API not reachable at ${HOST}"

# Create the bucket (idempotent: a 409 'already exists' is fine).
code="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
  "${HOST}/storage/v1/b?project=pj-cloud-dev" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"${BUCKET}\"}")"
case "${code}" in
  200|201|409) log "bucket ${BUCKET} ready (http ${code})" ;;
  *) die "bucket create returned http ${code}" ;;
esac

# Upload exactly the 8 (overwrite-in-place via uploadType=media).
for k in "${KEYS[@]}"; do
  f="${GTD}/${k}"
  [[ -f "${f}" ]] || die "missing ground-truth file ${f}"
  code="$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    "${HOST}/upload/storage/v1/b/${BUCKET}/o?uploadType=media&name=${k}" \
    -H "Content-Type: application/octet-stream" \
    --data-binary "@${f}")"
  [[ "${code}" == "200" ]] || die "upload ${k} returned http ${code}"
  log "uploaded ${k}"
done

# Verify exactly 8 .mcap objects landed.
n="$(curl -fsS "${HOST}/storage/v1/b/${BUCKET}/o" \
  | python3 -c 'import json,sys; d=json.load(sys.stdin); print(sum(1 for i in d.get("items",[]) if i["name"].endswith(".mcap")))')"
[[ "${n}" == "8" ]] || die "expected 8 .mcap objects after seed, saw ${n}"
log "OK: ${n} .mcap objects in ${BUCKET}"
