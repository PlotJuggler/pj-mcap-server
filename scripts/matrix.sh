#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# PENDING MIGRATION (2026-07-06): this script still exercises the LEGACY path —
# the Go server's in-process indexer (`catalog.Open`, read-write) scanning a
# Minio bucket of 8 real "nissan_zala_*" MCAPs from ${GROUND_TRUTH_DIR} below,
# which do NOT exist on this machine (they live on the corpus machine this
# harness was authored on). Worse: that in-process indexer (`catalog.Open`,
# `internal/indexer`) plus its `-db`/`-poll-interval` server flags were DELETED
# outright in the M6 §2.6 cutover (the server only opens the catalog read-only
# now, written by the external Python `mcap_catalog` builder) — so this script
# can no longer even start a working server, let alone reach its legs. Rather
# than let it fail confusingly deep inside `setup()` (wrong file count, a
# missing GROUND_TRUTH_DIR, or a flag the binary silently ignores),
# it FAILS FAST below with a clear message and exit code 2, before touching
# docker or building/starting anything. `scripts/smoke.sh` was migrated in the
# same change that added this note to the NEW production shape (Python
# `mcap_catalog` builder + Go server in `-external-builder` read-only mode,
# over a synthetic Hive-keyed corpus) — see its header comment for the full
# architecture. This script's migration to the same shape (and to a corpus
# this machine actually has) is TRACKED SEPARATELY and intentionally NOT done
# here: it needs its own design pass for the deeper legs (8-file stitch,
# 4-parallel sessions, spans-boundary window) against a synthetic/self-
# contained corpus of comparable scale, which `make smoke`'s 8-file synthetic
# Hive corpus does not attempt to be (it is deliberately small — 6
# DefaultSpecs files + the high-volume bigSpec fixture + the 3D fixture;
# scale/depth is this script's job, not smoke's). The body below (setup +
# legs m1-m8) is kept, UNREACHED, as the starting point for that migration —
# it is not currently run by anything.
# ─────────────────────────────────────────────────────────────────────────────
cat >&2 <<'EOF'
matrix.sh is pending migration to the Python-builder pipeline; it depended
on the retired in-process Go indexer (§2.6) and cannot run against this
server build; see scripts/smoke.sh for the migrated gate.
EOF
exit 2
# ─────────────────────────────────────────────────────────────────────────────
#
# matrix.sh — the DEEPER, SLOWER end-to-end correctness gate for the PJ Cloud
# Connector. Where `make smoke` is the fast (~seconds) per-change regression gate,
# `make matrix` exercises the spec §11 Layer-3/4 round-trip MATRIX: the session
# shapes that only ever existed as Go unit tests get a real, cross-stack,
# mcapdiff-verified leg here (Slice 10 TASK 1.3).
#
# Like smoke it is IDEMPOTENT and SELF-CONTAINED: it ensures Minio is up, builds
# and starts its OWN server instance on :8082 against a temp DB, and ALWAYS reaps
# its server + scratch on exit (success or failure). It NEVER touches :8080 (the
# user's interactive instance) nor :8081 (the smoke harness server).
#
# Final line is exactly one of:
#   MATRIX PASS
#   MATRIX FAIL: <leg>
# and the exit code matches (0 / non-zero).
#
# ─────────────────────────────────────────────────────────────────────────────
# LEGS (each mcapdiff-verified where applicable, against the local on-disk
# originals at ${GROUND_TRUTH_DIR}):
#   m1  half-topics round-trip on zeg_1 (3 of 6 topics) — clean + NO over-delivery
#   m2  none-matching topics  -> zero-result session, exit 0, EMPTY VALID mcap
#   m3  outside-range window  -> zero-result session, exit 0, EMPTY VALID mcap
#   m4  spans-boundary stitched window (zeg_2/zeg_3 boundary) — clean (29461 msgs)
#   m5  8-FILE FULL stitch round-trip vs all 8 originals — clean + monotonic
#       (337861 msgs). NOTE: the corpus has no 10 DISTINCT files; 8 is the max
#       valid non-overlapping stitch, so this is the CORPUS-BOUND version of the
#       spec §11 "10-file" matrix cell (documented deviation).
#   m6  4 PARALLEL sessions (4 concurrent CLI downloads of different files) — all
#       COMPLETE clean, then each mcapdiff'd vs its original
#   m7  duplicate/overlap rejection probes — duplicate seq is a clean usage error
#       (exit 2); an overlapping (same-file-twice) server selection is rejected.
#   m8  GCS DUAL-LEG anti-drift gate (Plan A Task 46a, Asensus M1b): brings up the
#       fake-gcs-server emulator (infra/fake-gcs), seeds it with the SAME 8
#       ground-truth MCAPs, reaps the S3 matrix server and re-boots a SECOND server
#       on :8082 pointed at GCS (fresh temp DB, STORAGE_EMULATOR_HOST). Asserts
#       THROUGH BOTH CLIENT STACKS like the s3 legs: devprobe list == 8, full zeg_1
#       download == 33670 + mcapdiff logically-equal vs the original, AND the C++
#       dexory-cloud-cli list/download round-trip. Then asserts CHANGE-DETECT
#       PARITY: restart on the SAME GCS DB -> indexer logs 0 new / 0 reindexed
#       (warm-start works because ETag == GCS Generation). Reaps the GCS server +
#       its DB; tears fake-gcs down IFF this leg started it.
# ─────────────────────────────────────────────────────────────────────────────
# PINNED GROUND TRUTH — self-contained to THIS script and THIS legacy real
# corpus (${GROUND_TRUTH_DIR}, the 8 "nissan_zala_*" MCAPs). These do NOT
# track scripts/smoke.sh or the C++ live gtests (plugin/toolbox_dexory_cloud/
# tests/*_live_test.cpp) — since the catalog-migration cutover (2026-07-06)
# those run against an entirely different, synthetic Hive-keyed corpus
# (server/internal/genmcap's deterministic generator) that has no relationship
# to jkk_dataset02's file names/counts. Any historical claim of the two
# tracking in lockstep is STALE and no longer true; treat matrix.sh's pins
# below as independent of smoke/C++ ground truth.
# All counts independently re-derived from the on-disk corpus on 2026-06-05:
#   per-file: sagod 44416, zeg_1 33670, zeg_2 43301, zeg_3 21731, zeg_4 22311,
#             country_road_1 46906, country_road_2 73704, mixed 51822  (Σ 337861)
#   boundary window [1696578000000000000,1696578150000000000): zeg_2 18772 +
#             zeg_3 10689 = 29461.
# If the bucket is reseeded, update these HERE only — nothing else needs to
# move in lockstep anymore.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── pinned expectations ──────────────────────────────────────────────────────
readonly GROUND_TRUTH_DIR="/home/gn/ws/jkk_dataset02"

# m1: zeg_1, half of its 6 topics. 14904 + 4513 + 4513 = 23930.
readonly M1_KEY="nissan_zala_50_zeg_1_0.mcap"
readonly M1_TOPICS="/nissan/gps/duro/imu,/nissan/vehicle_speed,/nissan/vehicle_steering"
readonly M1_EXPECT_MSGS=23930

# m2: a topic that matches nothing -> empty plan.
readonly M2_KEY="${M1_KEY}"
readonly M2_TOPICS="/does/not/exist"

# m3: a window far before the corpus -> empty plan.
readonly M3_KEY="${M1_KEY}"
readonly M3_WINDOW="1000000000000000000,1000000001000000000"

# m4: boundary-spanning window across the zeg_2 -> zeg_3 stitch seam.
readonly M4_KEY_A="nissan_zala_50_zeg_2_0.mcap"
readonly M4_KEY_B="nissan_zala_50_zeg_3_0.mcap"
readonly M4_WINDOW="1696578000000000000,1696578150000000000"
readonly M4_EXPECT_MSGS=29461

# m5: ALL 8 files, time-ordered, full stitch. Σ of the per-file counts.
readonly M5_KEYS=(
  nissan_zala_50_sagod_0.mcap
  nissan_zala_50_zeg_1_0.mcap
  nissan_zala_50_zeg_2_0.mcap
  nissan_zala_50_zeg_3_0.mcap
  nissan_zala_50_zeg_4_0.mcap
  nissan_zala_90_country_road_1_0.mcap
  nissan_zala_90_country_road_2_0.mcap
  nissan_zala_90_mixed_0.mcap
)
readonly M5_EXPECT_MSGS=337861

# m6: four distinct files for the parallel leg + their per-file expected counts.
readonly M6_KEYS=(
  nissan_zala_50_sagod_0.mcap
  nissan_zala_50_zeg_1_0.mcap
  nissan_zala_50_zeg_4_0.mcap
  nissan_zala_90_mixed_0.mcap
)
readonly M6_EXPECT_MSGS=(44416 33670 22311 51822)

# m7: duplicate-seq probe key.
readonly M7_KEY="${M1_KEY}"

# m8: GCS dual-leg. Reuses the smoke/matrix ground-truth: full zeg_1 round-trip.
readonly M8_KEY="${M1_KEY}"
readonly M8_EXPECT_MSGS=33670
readonly M8_EXPECT_FILES=8

# ── paths (all absolute; cwd is reset between agent steps elsewhere) ──────────
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly SERVER_DIR="${REPO_ROOT}/server"
readonly PLUGIN_DIR="${REPO_ROOT}/plugin/toolbox_dexory_cloud"
readonly CTEST_DIR="${PLUGIN_DIR}/build"
readonly SDK_CLI="${CTEST_DIR}/bin/dexory-cloud-cli"

# ── harness server config (NEVER :8080 / :8081 / their PID files) ─────────────
readonly MATRIX_PORT=8082
readonly MATRIX_WS="ws://localhost:${MATRIX_PORT}/api/ws"
readonly MATRIX_URL="ws://localhost:${MATRIX_PORT}"
readonly MATRIX_LOG="/tmp/pj-cloud-matrix-server.log"
readonly MATRIX_DB="/tmp/pj-cloud-matrix-catalog.db"
readonly MATRIX_POLL_INTERVAL="30s"
readonly MINIO_HEALTH="http://localhost:9000/minio/health/live"

# ── m8 GCS leg config (fake-gcs emulator; never the dev Minio) ────────────────
readonly FAKE_GCS_COMPOSE="${REPO_ROOT}/infra/fake-gcs/docker-compose.yml"
readonly FAKE_GCS_SEED="${REPO_ROOT}/infra/fake-gcs/seed.sh"
readonly FAKE_GCS_HOST="localhost:4443"
readonly FAKE_GCS_API="http://${FAKE_GCS_HOST}/storage/v1/b"
readonly GCS_DB="/tmp/pj-cloud-matrix-gcs-catalog.db"
readonly GCS_LOG="/tmp/pj-cloud-matrix-gcs-server.log"

# ── go toolchain on PATH (per project context) ───────────────────────────────
export PATH="$HOME/.local/go/bin:$HOME/go/bin:$PATH"
export GOTOOLCHAIN=local

# ── state for cleanup ─────────────────────────────────────────────────────────
MATRIX_SERVER_PID=""
MATRIX_WORKDIR=""
GCS_SERVER_PID=""
FAKE_GCS_STARTED_BY_US=0

log()  { printf '[matrix] %s\n' "$*"; }
fail() { printf 'MATRIX FAIL: %s\n' "$*"; exit 1; }

# stop_server PID NAME — TERM then KILL a server pid, bounded.
stop_server() {
  local pid="$1" name="$2"
  if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
    log "stopping ${name} (pid ${pid})"
    kill "${pid}" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "${pid}" 2>/dev/null || break
      sleep 0.3
    done
    kill -9 "${pid}" 2>/dev/null || true
  fi
}

cleanup() {
  local rc=$?
  if [[ -n "${MATRIX_WORKDIR}" && -d "${MATRIX_WORKDIR}" ]]; then
    rm -rf "${MATRIX_WORKDIR}"
  fi
  stop_server "${MATRIX_SERVER_PID}" "matrix server"
  stop_server "${GCS_SERVER_PID}" "matrix GCS server"
  rm -f "${MATRIX_DB}" "${MATRIX_DB}-wal" "${MATRIX_DB}-shm" 2>/dev/null || true
  rm -f "${GCS_DB}" "${GCS_DB}-wal" "${GCS_DB}-shm" 2>/dev/null || true
  # Tear fake-gcs down IFF the m8 leg is the one that started it (leave a
  # pre-existing emulator exactly as found).
  if [[ "${FAKE_GCS_STARTED_BY_US}" == "1" ]]; then
    log "tearing down fake-gcs (started by this leg)"
    docker compose -f "${FAKE_GCS_COMPOSE}" down >/dev/null 2>&1 || true
  fi
  return $rc
}
trap cleanup EXIT

wait_http() {
  local url="$1" timeout="$2" waited=0
  while ! curl -fsS -m 3 -o /dev/null "${url}" 2>/dev/null; do
    sleep 1
    waited=$((waited + 1))
    if (( waited >= timeout )); then return 1; fi
  done
  return 0
}

# wait_port_free PORT TIMEOUT — block until nothing answers /health on PORT (the
# previous server has fully released the listener), so the next bind succeeds.
wait_port_free() {
  local port="$1" timeout="$2" waited=0
  while curl -fsS -m 2 -o /dev/null "http://localhost:${port}/health" 2>/dev/null; do
    sleep 0.5
    waited=$((waited + 1))
    if (( waited >= timeout * 2 )); then return 1; fi
  done
  return 0
}

# mcap_msg_count FILE — print the message_count of a (possibly empty) on-disk MCAP
# via the mcaptopics tool. A non-zero exit means the file did not parse as MCAP.
mcap_msg_count() {
  "${MCAPTOPICS}" "$1" 2>/dev/null \
    | python3 -c 'import json,sys; print(json.load(sys.stdin)["message_count"])'
}

# cli_download_count JSON — extract messages_received from a `download --json`.
cli_download_count() {
  CLI_JSON="$1" python3 -c \
    'import json,os; print(json.loads(os.environ["CLI_JSON"]).get("messages_received",-1))'
}

# ─────────────────────────────────────────────────────────────────────────────
# Setup: Minio up, build Go tools, build CLI if missing, start server on :8082.
# ─────────────────────────────────────────────────────────────────────────────
MCAPDIFF=""
MCAPTOPICS=""
DEVPROBE=""

setup() {
  log "setup: ensuring Minio is up"
  if ! wait_http "${MINIO_HEALTH}" 30; then
    if [[ -f "${REPO_ROOT}/infra/minio/docker-compose.yml" ]]; then
      log "setup: Minio down — bringing it up via docker compose"
      ( cd "${REPO_ROOT}/infra/minio" && docker compose up -d ) >/dev/null 2>&1 || true
    fi
    wait_http "${MINIO_HEALTH}" 60 || fail "setup: Minio :9000 not healthy"
  fi

  log "setup: building Go tools (server, devprobe, mcapdiff, mcaptopics)"
  command -v go >/dev/null 2>&1 || fail "setup: go not on PATH ($HOME/.local/go/bin expected)"
  ( cd "${SERVER_DIR}" && go build -o ./bin/pj-cloud-server ./cmd/pj-cloud-server ) || fail "setup: server build"
  ( cd "${SERVER_DIR}" && go build -o ./bin/devprobe ./cmd/devprobe )             || fail "setup: devprobe build"
  ( cd "${SERVER_DIR}" && go build -o ./bin/mcapdiff ./cmd/mcapdiff )             || fail "setup: mcapdiff build"
  ( cd "${SERVER_DIR}" && go build -o ./bin/mcaptopics ./cmd/mcaptopics )         || fail "setup: mcaptopics build"
  MCAPDIFF="${SERVER_DIR}/bin/mcapdiff"
  MCAPTOPICS="${SERVER_DIR}/bin/mcaptopics"
  DEVPROBE="${SERVER_DIR}/bin/devprobe"

  [[ -x "${SDK_CLI}" ]] \
    || fail "setup: dexory-cloud-cli missing at ${SDK_CLI} (build it: ./build.sh (from the repo root))"
  [[ -d "${GROUND_TRUTH_DIR}" ]] || fail "setup: ground-truth originals dir ${GROUND_TRUTH_DIR} not present"

  rm -f "${MATRIX_DB}" "${MATRIX_DB}-wal" "${MATRIX_DB}-shm" 2>/dev/null || true
  log "setup: starting matrix server on :${MATRIX_PORT} (db ${MATRIX_DB}, log ${MATRIX_LOG})"
  : > "${MATRIX_LOG}"
  ( cd "${SERVER_DIR}" && exec env -u PJ_CLOUD_TOKEN ./bin/pj-cloud-server \
      -listen ":${MATRIX_PORT}" -db "${MATRIX_DB}" -poll-interval "${MATRIX_POLL_INTERVAL}" \
      >>"${MATRIX_LOG}" 2>&1 ) &
  MATRIX_SERVER_PID=$!
  if ! wait_http "http://localhost:${MATRIX_PORT}/health" 60; then
    tail -n 40 "${MATRIX_LOG}" || true
    fail "setup: :${MATRIX_PORT}/health did not come up within 60s"
  fi
  # Wait until the catalog has indexed all 8 files (cold extract).
  local i fc=0
  for i in $(seq 1 60); do
    fc="$("${DEVPROBE}" -url "${MATRIX_WS}" 2>/dev/null \
        | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("files",[])))' 2>/dev/null || echo 0)"
    [[ "${fc}" -ge 8 ]] && break
    sleep 1
  done
  [[ "${fc}" -ge 8 ]] || fail "setup: catalog did not index >=8 files (saw ${fc})"

  MATRIX_WORKDIR="$(mktemp -d)" || fail "setup: mktemp -d failed"
  log "setup: OK (server pid ${MATRIX_SERVER_PID}, ${fc} files indexed, work ${MATRIX_WORKDIR})"
}

# ─────────────────────────────────────────────────────────────────────────────
# m1: half-topics round-trip — clean + no over-delivery.
# ─────────────────────────────────────────────────────────────────────────────
leg_m1() {
  log "m1: half-topics round-trip on ${M1_KEY} (${M1_TOPICS})"
  local out="${MATRIX_WORKDIR}/m1.mcap" j
  j="$("${SDK_CLI}" --url "${MATRIX_URL}" download "${M1_KEY}" \
      --topics "${M1_TOPICS}" --output "${out}" --json 2>/dev/null)" \
    || fail "m1: download failed"
  [[ -s "${out}" ]] || fail "m1: produced no output"
  local got; got="$(cli_download_count "${j}")"
  [[ "${got}" == "${M1_EXPECT_MSGS}" ]] || fail "m1: messages_received=${got} != ${M1_EXPECT_MSGS}"
  # mcapdiff filters the original to --topics and flags ANY over-delivery.
  "${MCAPDIFF}" --topics "${M1_TOPICS}" "${GROUND_TRUTH_DIR}/${M1_KEY}" "${out}" \
    || fail "m1: NOT clean (under/over-delivery or drift)"
  log "m1: OK (${got} msgs, clean, no over-delivery)"
}

# ─────────────────────────────────────────────────────────────────────────────
# m2: none-matching topics -> zero-result session, exit 0, empty valid MCAP.
# ─────────────────────────────────────────────────────────────────────────────
leg_m2() {
  log "m2: none-matching topics -> zero-result session (${M2_TOPICS})"
  local out="${MATRIX_WORKDIR}/m2.mcap" j
  # Must exit 0 (Eos COMPLETE, 0 messages) — a zero-result selection is success.
  j="$("${SDK_CLI}" --url "${MATRIX_URL}" download "${M2_KEY}" \
      --topics "${M2_TOPICS}" --output "${out}" --json 2>/dev/null)" \
    || fail "m2: zero-result download did not exit 0"
  local got; got="$(cli_download_count "${j}")"
  [[ "${got}" == "0" ]] || fail "m2: messages_received=${got} != 0"
  [[ -s "${out}" ]] || fail "m2: zero-result produced no file (must be an empty VALID mcap)"
  # The file must still parse as MCAP with exactly 0 messages.
  local mc; mc="$(mcap_msg_count "${out}")" || fail "m2: output is not a valid MCAP"
  [[ "${mc}" == "0" ]] || fail "m2: empty file reports ${mc} messages (want 0)"
  log "m2: OK (exit 0, empty valid mcap, 0 messages)"
}

# ─────────────────────────────────────────────────────────────────────────────
# m3: outside-range window -> zero-result session, exit 0, empty valid MCAP.
# ─────────────────────────────────────────────────────────────────────────────
leg_m3() {
  log "m3: outside-range window -> zero-result session (${M3_WINDOW})"
  local out="${MATRIX_WORKDIR}/m3.mcap" j
  j="$("${SDK_CLI}" --url "${MATRIX_URL}" download "${M3_KEY}" \
      --time-range "${M3_WINDOW}" --output "${out}" --json 2>/dev/null)" \
    || fail "m3: outside-range download did not exit 0"
  local got; got="$(cli_download_count "${j}")"
  [[ "${got}" == "0" ]] || fail "m3: messages_received=${got} != 0"
  [[ -s "${out}" ]] || fail "m3: zero-result produced no file (must be an empty VALID mcap)"
  local mc; mc="$(mcap_msg_count "${out}")" || fail "m3: output is not a valid MCAP"
  [[ "${mc}" == "0" ]] || fail "m3: empty file reports ${mc} messages (want 0)"
  log "m3: OK (exit 0, empty valid mcap, 0 messages)"
}

# ─────────────────────────────────────────────────────────────────────────────
# m4: spans-boundary stitched window across the zeg_2 -> zeg_3 seam — clean.
# ─────────────────────────────────────────────────────────────────────────────
leg_m4() {
  log "m4: boundary-spanning stitched window ${M4_KEY_A}+${M4_KEY_B} [${M4_WINDOW})"
  local out="${MATRIX_WORKDIR}/m4.mcap" j
  j="$("${SDK_CLI}" --url "${MATRIX_URL}" download "${M4_KEY_A}" "${M4_KEY_B}" \
      --time-range "${M4_WINDOW}" --output "${out}" --json 2>/dev/null)" \
    || fail "m4: stitched-window download failed"
  [[ -s "${out}" ]] || fail "m4: produced no output"
  local got; got="$(cli_download_count "${j}")"
  [[ "${got}" == "${M4_EXPECT_MSGS}" ]] || fail "m4: messages_received=${got} != ${M4_EXPECT_MSGS}"
  # mcapdiff merges BOTH originals + applies the SAME window, then checks logical
  # equality AND over-delivery (records outside the window must be absent).
  "${MCAPDIFF}" --time-range "${M4_WINDOW}" \
    "${GROUND_TRUTH_DIR}/${M4_KEY_A}" "${GROUND_TRUTH_DIR}/${M4_KEY_B}" "${out}" \
    || fail "m4: NOT clean (under/over-delivery or drift)"
  log "m4: OK (${got} msgs across the seam, clean, no over-delivery)"
}

# ─────────────────────────────────────────────────────────────────────────────
# m5: 8-file FULL stitch round-trip vs all 8 originals — clean + monotonic.
# (Corpus-bound version of the spec's 10-file cell: the corpus has no 10 distinct
#  non-overlapping files; 8 is the maximal valid stitch.)
# ─────────────────────────────────────────────────────────────────────────────
leg_m5() {
  log "m5: 8-FILE FULL stitch round-trip (corpus-bound 10-file cell)"
  local out="${MATRIX_WORKDIR}/m5.mcap" j
  j="$("${SDK_CLI}" --url "${MATRIX_URL}" download "${M5_KEYS[@]}" \
      --output "${out}" --json 2>/dev/null)" \
    || fail "m5: 8-file stitched download failed"
  [[ -s "${out}" ]] || fail "m5: produced no output"
  local got; got="$(cli_download_count "${j}")"
  [[ "${got}" == "${M5_EXPECT_MSGS}" ]] || fail "m5: messages_received=${got} != ${M5_EXPECT_MSGS}"
  # Stitched logical-equality vs ALL 8 originals merged (mcapdiff concatenates +
  # re-sorts the originals by (topic,log_time) before comparing).
  local orig=(); local k
  for k in "${M5_KEYS[@]}"; do orig+=("${GROUND_TRUTH_DIR}/${k}"); done
  "${MCAPDIFF}" "${orig[@]}" "${out}" \
    || fail "m5: NOT logically equal to the 8 merged originals"
  # Monotonic log_time across the stitched stream (no out-of-order across seams):
  # mcaptopics --monotonic reads in FILE (as-written) order and exits 1 on any
  # decreasing step, so this checks the reconstruction's NATIVE write ordering.
  "${MCAPTOPICS}" --monotonic "${out}" >/dev/null \
    || fail "m5: stitched stream log_time NOT monotonic across seams"
  log "m5: OK (${got} msgs, 8-file stitch clean + monotonic)"
}

# ─────────────────────────────────────────────────────────────────────────────
# m6: 4 PARALLEL sessions — concurrent downloads of 4 different files, all
# COMPLETE clean, then each mcapdiff'd vs its original.
# ─────────────────────────────────────────────────────────────────────────────
leg_m6() {
  log "m6: 4 PARALLEL sessions (concurrent downloads of 4 distinct files)"
  local pids=() outs=() rcs=() i
  for i in "${!M6_KEYS[@]}"; do
    local key="${M6_KEYS[$i]}" out="${MATRIX_WORKDIR}/m6_${i}.mcap"
    outs[$i]="${out}"
    ( "${SDK_CLI}" --url "${MATRIX_URL}" download "${key}" --output "${out}" \
        >"${MATRIX_WORKDIR}/m6_${i}.log" 2>&1 ) &
    pids[$i]=$!
  done
  # Wait for all and capture per-download exit codes.
  local fail_any=0
  for i in "${!pids[@]}"; do
    if wait "${pids[$i]}"; then rcs[$i]=0; else rcs[$i]=$?; fail_any=1; fi
  done
  [[ "${fail_any}" == "0" ]] \
    || fail "m6: at least one parallel download did not exit 0 (rcs: ${rcs[*]})"
  # Each reconstructed file must be logically equal to its original + correct count.
  for i in "${!M6_KEYS[@]}"; do
    local key="${M6_KEYS[$i]}" out="${outs[$i]}" want="${M6_EXPECT_MSGS[$i]}"
    [[ -s "${out}" ]] || fail "m6: parallel download of ${key} produced no output"
    local mc; mc="$(mcap_msg_count "${out}")" || fail "m6: ${key} output not a valid MCAP"
    [[ "${mc}" == "${want}" ]] || fail "m6: ${key} reconstructed ${mc} msgs != ${want}"
    "${MCAPDIFF}" "${GROUND_TRUTH_DIR}/${key}" "${out}" \
      || fail "m6: ${key} NOT logically equal to original"
  done
  log "m6: OK (4 concurrent sessions all COMPLETE clean, each mcapdiff-verified)"
}

# ─────────────────────────────────────────────────────────────────────────────
# m7: duplicate/overlap rejection probes.
#   - duplicate sequence in one download is a clean usage error (exit 2);
#   - an overlapping (same file selected twice via distinct ids) selection is
#     rejected server-side as INVALID_REQUEST (devprobe surfaces a download error,
#     non-zero exit). The CLI catches the trivial duplicate before the wire, so the
#     server-overlap path is probed through devprobe with the same id twice.
# ─────────────────────────────────────────────────────────────────────────────
leg_m7() {
  log "m7: duplicate / overlap rejection probes"
  local out="${MATRIX_WORKDIR}/m7_dup.mcap"
  # (a) Duplicate sequence name in the CLI -> usage error, exit 2 (NOT a crash).
  if "${SDK_CLI}" --url "${MATRIX_URL}" download "${M7_KEY}" "${M7_KEY}" \
       --output "${out}" >/dev/null 2>&1; then
    fail "m7: duplicate-sequence download should have failed (exit 2)"
  else
    local rc=$?
    [[ "${rc}" == "2" ]] || fail "m7: duplicate-sequence exit ${rc} != 2 (usage)"
  fi
  # (b) Overlapping selection on the wire: devprobe -download with the SAME key
  # twice (comma form) -> server rejects (INVALID_REQUEST) -> non-zero exit and no
  # valid completed file. The CLI's client-side guard mirrors this; here we drive
  # the wire directly to prove the SERVER is authoritative.
  local out2="${MATRIX_WORKDIR}/m7_overlap.mcap"
  if "${DEVPROBE}" -url "${MATRIX_WS}" -download "${M7_KEY},${M7_KEY}" -out "${out2}" \
       >/dev/null 2>&1; then
    fail "m7: server accepted an overlapping (same-file-twice) selection"
  fi
  log "m7: OK (duplicate seq exits 2; server rejects overlapping selection)"
}

# ─────────────────────────────────────────────────────────────────────────────
# m8: GCS DUAL-LEG anti-drift gate (Plan A Task 46a, Asensus M1b).
#
# Brings up fake-gcs, seeds the SAME 8 ground-truth MCAPs, reaps the S3 matrix
# server, and re-boots a SECOND server on :8082 pointed at GCS (fresh temp DB,
# STORAGE_EMULATOR_HOST). Asserts through BOTH client stacks (devprobe + the C++
# dexory-cloud-cli) that the GCS arm round-trips IDENTICALLY to the S3 arm, then
# asserts change-detect parity (warm-start = 0 re-extracts, ETag == Generation).
# ─────────────────────────────────────────────────────────────────────────────
leg_m8() {
  log "m8: GCS dual-leg (fake-gcs emulator) — 46a anti-drift gate"
  command -v docker >/dev/null 2>&1 || fail "m8: docker not on PATH (needed for fake-gcs)"
  [[ -f "${FAKE_GCS_COMPOSE}" ]] || fail "m8: ${FAKE_GCS_COMPOSE} missing"
  [[ -x "${FAKE_GCS_SEED}" || -f "${FAKE_GCS_SEED}" ]] || fail "m8: ${FAKE_GCS_SEED} missing"

  # Bring fake-gcs up only if it isn't already serving (leave a pre-existing one
  # exactly as found; tear down only what we started).
  if curl -fsS -m 3 -o /dev/null "${FAKE_GCS_API}" 2>/dev/null; then
    log "m8: fake-gcs already up at ${FAKE_GCS_HOST} (will leave it as found)"
  else
    log "m8: starting fake-gcs via ${FAKE_GCS_COMPOSE}"
    docker compose -f "${FAKE_GCS_COMPOSE}" up -d --wait >/dev/null 2>&1 \
      || fail "m8: fake-gcs compose up failed"
    FAKE_GCS_STARTED_BY_US=1
    wait_http "${FAKE_GCS_API}" 30 || fail "m8: fake-gcs JSON API not reachable at ${FAKE_GCS_HOST}"
  fi

  # Seed exactly the 8 ground-truth MCAPs (idempotent).
  log "m8: seeding fake-gcs bucket recordings (8 ground-truth MCAPs)"
  bash "${FAKE_GCS_SEED}" "http://${FAKE_GCS_HOST}" "${GROUND_TRUTH_DIR}" >/dev/null \
    || fail "m8: seed.sh failed"

  # Reap the S3 matrix server and reuse :8082 for the GCS server (sequential).
  stop_server "${MATRIX_SERVER_PID}" "matrix server (S3)"
  MATRIX_SERVER_PID=""
  if ! wait_port_free "${MATRIX_PORT}" 15; then
    fail "m8: :${MATRIX_PORT} did not free up after reaping the S3 server"
  fi

  # Temp GCS config (tagged union: null S3, set GCS).
  local gcfg="${MATRIX_WORKDIR}/gcs-config.yaml"
  cat > "${gcfg}" <<YAML
server:
  listen: ":${MATRIX_PORT}"
storage:
  s3: null
  gcs:
    bucket: recordings
catalog:
  db_path: ${GCS_DB}
indexer:
  poll_interval: ${MATRIX_POLL_INTERVAL}
  startup_scan: true
YAML

  rm -f "${GCS_DB}" "${GCS_DB}-wal" "${GCS_DB}-shm" 2>/dev/null || true
  log "m8: starting GCS server on :${MATRIX_PORT} (db ${GCS_DB}, STORAGE_EMULATOR_HOST=${FAKE_GCS_HOST})"
  : > "${GCS_LOG}"
  ( cd "${SERVER_DIR}" \
      && exec env -u PJ_CLOUD_TOKEN STORAGE_EMULATOR_HOST="${FAKE_GCS_HOST}" \
         ./bin/pj-cloud-server -config "${gcfg}" \
         >>"${GCS_LOG}" 2>&1 ) &
  GCS_SERVER_PID=$!
  if ! wait_http "http://localhost:${MATRIX_PORT}/health" 60; then
    tail -n 40 "${GCS_LOG}" || true
    fail "m8: GCS server :${MATRIX_PORT}/health did not come up within 60s"
  fi

  # Wait until the catalog has indexed all 8 (cold extract from GCS).
  local i fc=0
  for i in $(seq 1 60); do
    fc="$("${DEVPROBE}" -url "${MATRIX_WS}" 2>/dev/null \
        | python3 -c 'import json,sys; print(len(json.load(sys.stdin).get("files",[])))' 2>/dev/null || echo 0)"
    [[ "${fc}" -ge "${M8_EXPECT_FILES}" ]] && break
    sleep 1
  done
  [[ "${fc}" == "${M8_EXPECT_FILES}" ]] \
    || fail "m8: GCS catalog indexed ${fc} files != ${M8_EXPECT_FILES}"
  log "m8: GCS catalog indexed ${fc} files (devprobe list)"

  # (a) devprobe round-trip: full zeg_1 download == 33670 + mcapdiff equal.
  local out="${MATRIX_WORKDIR}/m8_devprobe.mcap"
  "${DEVPROBE}" -url "${MATRIX_WS}" -download "${M8_KEY}" -out "${out}" >/dev/null 2>&1 \
    || fail "m8: devprobe GCS download failed"
  [[ -s "${out}" ]] || fail "m8: devprobe GCS download produced no output"
  local mc; mc="$(mcap_msg_count "${out}")" || fail "m8: devprobe GCS output not a valid MCAP"
  [[ "${mc}" == "${M8_EXPECT_MSGS}" ]] || fail "m8: devprobe GCS ${M8_KEY} ${mc} msgs != ${M8_EXPECT_MSGS}"
  "${MCAPDIFF}" "${GROUND_TRUTH_DIR}/${M8_KEY}" "${out}" \
    || fail "m8: devprobe GCS reconstruction NOT logically equal to the original"
  log "m8: devprobe GCS round-trip clean (${mc} msgs, mcapdiff equal)"

  # (b) C++ dexory-cloud-cli round-trip: list == 8 + download == 33670.
  local clist; clist="$("${SDK_CLI}" --url "${MATRIX_URL}" list --json 2>/dev/null)" \
    || fail "m8: C++ CLI list failed against GCS server"
  local cn
  cn="$(CLI_JSON="${clist}" python3 -c \
    'import json,os; d=json.loads(os.environ["CLI_JSON"]); print(len(d) if isinstance(d,list) else len(d.get("files", d.get("sequences", []))))' 2>/dev/null || echo -1)"
  [[ "${cn}" == "${M8_EXPECT_FILES}" ]] || fail "m8: C++ CLI list returned ${cn} files != ${M8_EXPECT_FILES}"
  local cout="${MATRIX_WORKDIR}/m8_cli.mcap" cj
  cj="$("${SDK_CLI}" --url "${MATRIX_URL}" download "${M8_KEY}" --output "${cout}" --json 2>/dev/null)" \
    || fail "m8: C++ CLI GCS download failed"
  [[ -s "${cout}" ]] || fail "m8: C++ CLI GCS download produced no output"
  local cgot; cgot="$(cli_download_count "${cj}")"
  [[ "${cgot}" == "${M8_EXPECT_MSGS}" ]] || fail "m8: C++ CLI GCS messages_received=${cgot} != ${M8_EXPECT_MSGS}"
  "${MCAPDIFF}" "${GROUND_TRUTH_DIR}/${M8_KEY}" "${cout}" \
    || fail "m8: C++ CLI GCS reconstruction NOT logically equal to the original"
  log "m8: C++ CLI GCS round-trip clean (list ${cn}, ${cgot} msgs, mcapdiff equal)"

  # (c) change-detect parity: restart on the SAME GCS DB -> 0 new / 0 reindexed.
  stop_server "${GCS_SERVER_PID}" "matrix GCS server"
  GCS_SERVER_PID=""
  wait_port_free "${MATRIX_PORT}" 15 || fail "m8: :${MATRIX_PORT} did not free before warm-start"
  : > "${GCS_LOG}"
  log "m8: warm-start restart on SAME GCS DB (change-detect via Generation)"
  ( cd "${SERVER_DIR}" \
      && exec env -u PJ_CLOUD_TOKEN STORAGE_EMULATOR_HOST="${FAKE_GCS_HOST}" \
         ./bin/pj-cloud-server -config "${gcfg}" \
         >>"${GCS_LOG}" 2>&1 ) &
  GCS_SERVER_PID=$!
  if ! wait_http "http://localhost:${MATRIX_PORT}/health" 60; then
    tail -n 40 "${GCS_LOG}" || true
    fail "m8: GCS warm-start server did not come up within 60s"
  fi
  # The warm-start scan runs synchronously before the listener serves /health, so
  # the run-complete line is already present. Assert new=0 reindexed=0.
  local runline
  runline="$(grep 'indexer: run complete' "${GCS_LOG}" | head -1 || true)"
  [[ -n "${runline}" ]] || fail "m8: no 'indexer: run complete' line in warm-start log"
  echo "${runline}" | grep -q 'new=0' || fail "m8: warm-start re-extracted NEW files (expected new=0): ${runline}"
  echo "${runline}" | grep -q 'reindexed=0' || fail "m8: warm-start REINDEXED files (expected reindexed=0): ${runline}"
  # Belt-and-suspenders: no 'indexer: extracted' lines on the warm restart.
  local nx; nx="$(grep -c 'indexer: extracted' "${GCS_LOG}" || true)"
  [[ "${nx}" == "0" ]] || fail "m8: warm-start emitted ${nx} extract lines (expected 0)"
  log "m8: change-detect parity OK (warm-start ${runline#*msg=})"

  # Reap the warm GCS server + its DB now (don't hold it through the rest).
  stop_server "${GCS_SERVER_PID}" "matrix GCS server"
  GCS_SERVER_PID=""
  rm -f "${GCS_DB}" "${GCS_DB}-wal" "${GCS_DB}-shm" 2>/dev/null || true
  log "m8: OK (GCS round-trips via BOTH stacks + warm-start 0 re-extracts)"
}

# ─────────────────────────────────────────────────────────────────────────────
main() {
  log "PJ Cloud Connector MATRIX gate starting (repo: ${REPO_ROOT})"
  setup
  leg_m1
  leg_m2
  leg_m3
  leg_m4
  leg_m5
  leg_m6
  leg_m7
  leg_m8
  echo "MATRIX PASS"
}

main "$@"
