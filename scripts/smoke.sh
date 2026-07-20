#!/usr/bin/env bash
# smoke.sh — the single, self-contained regression gate for the PJ Cloud
# Connector, rewritten for the catalog-migration cutover (auryn-catalog-
# migration-plan.md §5.3, docs/CATALOG_CONTRACT.md). It proves the NEW
# production shape, fully self-contained from a fresh checkout:
#
#   Python builder daemon (mcap_catalog/, the SOLE catalog writer + tag-edit
#   IPC server) + Go server (read-only "external-builder" mode + tag-edit
#   forwarding + unchanged streamer), serving a SYNTHETIC Hive-keyed corpus in
#   Minio — no real dataset, no /home/gn paths, nothing this machine lacks.
#
# It proves:
#   - Minio is up and a fresh synthetic MCAP corpus is seeded under Hive keys
#     (customer=/customer_site=/robot=/source=/date=/name.mcap) into a
#     DEDICATED bucket (never the interactive `recordings` bucket),
#   - the Python catalog builder (--source s3 --no-watch, a rescan-only daemon)
#     builds the SQLite catalog and serves a tag-edit IPC UNIX socket,
#   - the Go server opens that catalog READ-ONLY (its only mode — the Go
#     catalog writer + in-process indexer were deleted, M6 §2.6) and forwards
#     UpdateTags over the tag-edit IPC socket (-tag-ipc-socket) instead of
#     writing the catalog itself,
#   - the Go-side discovery RPCs (Hello/ListFiles/GetFile) return numbers that
#     CROSS-CHECK against an INDEPENDENT ORACLE — `mcaptopics` run directly on
#     the local, pre-upload fixture files — never a hand-hardcoded constant,
#   - the C++ client SDK (BackendConnection) talks to that server live,
#   - both the hermetic and live C++ test suites are green,
#   - a streaming session ROUND-TRIPS (full / topic-subset / time-range /
#     stitched multi-file / half-topics / none-matching) through BOTH client
#     stacks, mcapdiff-verified against the local fixture originals,
#   - the Go server can be RESTARTED with the catalog DB untouched (no
#     re-extraction concept applies anymore — the builder owns extraction; the
#     invariant here is "the served DB's identity is unchanged"), and
#   - the tag-edit flow works END-TO-END through the IPC forwarder, INCLUDING
#     surviving a full catalog REBUILD (`--once --rebuild`, the atomic-publish
#     path, CATALOG_CONTRACT.md §9's tags_override carry-forward) — the Go
#     server must detect the served DB's inode swap (ReopenIfSwapped, its
#     30s freshness ticker) and keep serving the override under the
#     (possibly renumbered) new file id.
#
# It is IDEMPOTENT and SELF-CONTAINED: it ensures Minio is running, generates a
# byte-deterministic synthetic corpus fresh every run, builds and starts its
# OWN server instance on :8081 and its OWN Python builder daemon, runs every
# assertion, and ALWAYS tears everything down on exit (success or failure).
#
# It NEVER touches the user-facing server on :8080 nor its PID file
# (/tmp/pj-cloud-server.pid), and NEVER touches the interactive `recordings`
# Minio bucket — it owns a dedicated bucket (SMOKE_BUCKET below) that it wipes
# and reseeds at the start of every run.
#
# Final line is exactly one of:
#   SMOKE PASS
#   SMOKE FAIL: <step>
# and the exit code matches (0 / non-zero).
#
# ─────────────────────────────────────────────────────────────────────────────
# SELF-DERIVED GROUND TRUTH (do not hand-add hardcoded counts here)
#
# Every MESSAGE COUNT this harness asserts is derived at RUNTIME, never
# hand-hardcoded:
#   - the fixture set is generated fresh every run (`gen-ci-fixtures -hive` +
#     `gen-3d-fixture`, both deterministic — byte-identical across runs); the
#     file COUNT is whatever landed on disk (`find … | wc -l`), not a literal;
#   - per-file / per-topic message counts come from `mcaptopics` run directly
#     on the LOCAL pre-upload fixture (the INDEPENDENT ORACLE) — the Go
#     server's ListFiles/GetFile numbers are cross-checked against that
#     oracle, never assumed correct and never re-derived FROM the server
#     itself (that would be circular);
#   - the time-range window is computed as a fraction of the target file's
#     OWN LOCALLY-derived [start_ns,end_ns) (`mcaptopics` run on the LOCAL
#     original, step a) — NEVER from the server's reported range, which would
#     be circular; step c separately ASSERTS the server's reported start_ns/
#     end_ns equal the local oracle (an actual cross-check, not the window's
#     data source). The window's expected message count then comes from
#     filtering the LOCAL original with `~/Apps/mcap-linux-amd64 filter` and
#     re-deriving the count with `mcaptopics` — again the independent oracle,
#     never a literal.
#
# The only things "pinned" below are STRUCTURAL / NAME identifiers inherent to
# the deterministic generators (a file's leaf name, its Hive dimensions, which
# topic to use as "the subset topic") — exactly as stable and low-risk as the
# old harness's EXPECT_S3_KEY/EXPECT_IMU_TOPIC name constants were, just never
# a numeric count. See compute_local_ground_truth() / derive_server_ground_
# truth() below for where the real numbers come from.
#
# The C++ live gtests (plugin/toolbox_mcap_cloud/tests/*_live_test.cpp)
# CANNOT self-derive at build time (they are compiled long before this script
# runs) — their ground-truth constants are hand-pinned from ONE empirical run
# against this exact deterministic generator (see each file's comment for the
# regeneration command). If the generator in server/internal/genmcap ever
# changes, those constants — and the structural identifiers below — must be
# re-derived together.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── structural identifiers (generator-derived NAMES, never counts) ──────────
# Hive dimension literals gen-ci-fixtures.go hardcodes (hiveKeyFor).
readonly HIVE_CUSTOMER="test"
readonly HIVE_SITE="lab"
readonly HIVE_SOURCE="synthetic"
# The TARGET is gen-ci-fixtures' bigSpec() (`-hive-big`, server/cmd/gen-ci-
# fixtures/main.go) — NOT one of genmcap.DefaultSpecs() — because the C++
# live reconnect-resume tests (testForceDropAfterBatches) need enough VOLUME
# to force multiple WS session batches; DefaultSpecs' files are all well
# under the 512KiB max_batch_bytes default. bigSpec is a fixed, dedicated
# fixture (~6MiB, 3000 msgs / 3 topics: /clock 500, /odom 500, /imu 2000) at
# a FIXED Hive key (robot=r1/date=2026-06-24), never renumbered by
# DefaultSpecs' ordering. STITCH_B is DefaultSpecs()[1] "ci_synth_b.mcap" (3
# topics: /clock,/odom,/scan; robot=r2/date=2026-06-23 per hiveKeyFor(i=1)) —
# a small, ordinary fixture is fine for the stitch leg (no batch-volume need).
# Verified empirically (regenerate: `gen-ci-fixtures -hive -hive-big -out DIR
# -manifest` and inspect the printed paths) — if bigSpec/DefaultSpecs ever
# change, update these lines (and the C++ live-test pins, which target the
# SAME two files).
readonly TARGET_ROBOT="r1"
readonly TARGET_DATE="2026-06-24"
readonly TARGET_FILE="ci_synth_big.mcap"
readonly STITCH_B_ROBOT="r2"
readonly STITCH_B_DATE="2026-06-23"
readonly STITCH_B_FILE="ci_synth_b.mcap"
readonly TARGET_KEY="customer=${HIVE_CUSTOMER}/customer_site=${HIVE_SITE}/robot=${TARGET_ROBOT}/source=${HIVE_SOURCE}/date=${TARGET_DATE}/${TARGET_FILE}"
readonly STITCH_B_KEY="customer=${HIVE_CUSTOMER}/customer_site=${HIVE_SITE}/robot=${STITCH_B_ROBOT}/source=${HIVE_SOURCE}/date=${STITCH_B_DATE}/${STITCH_B_FILE}"
# The 3D fixture (gen-3d-fixture, no -hive mode of its own) is placed under the
# stitch-B partition — it exists purely to bump corpus diversity/count; no leg
# targets it by name.
readonly THREED_ROBOT="${STITCH_B_ROBOT}"
readonly THREED_DATE="${STITCH_B_DATE}"

# SUBSET_TOPIC must be one of target's topics; HALF_TOPICS a 2-of-3 subset;
# NONE_TOPIC must exist in no file. All are NAMES, not counts.
readonly SUBSET_TOPIC="/imu"
readonly HALF_TOPICS="/clock,/imu"
readonly NONE_TOPIC="/does/not/exist"
# Time-range leg (f4): a window covering the middle of the target file's OWN
# reported span (computed at runtime from its start_ns/end_ns — see step f).
readonly WINDOW_START_PCT=20
readonly WINDOW_END_PCT=70

# ── paths (all absolute; cwd is reset between agent steps elsewhere) ──────────
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly SERVER_DIR="${REPO_ROOT}/server"
readonly MCAP_CATALOG_DIR="${REPO_ROOT}/mcap_catalog"
readonly COMPOSE_FILE="${REPO_ROOT}/infra/minio/docker-compose.yml"
readonly PLUGIN_DIR="${REPO_ROOT}/plugin/toolbox_mcap_cloud"
readonly CTEST_DIR="${PLUGIN_DIR}/build"
readonly MCAP_CLI="${HOME}/Apps/mcap-linux-amd64"
readonly VENV_PY="${HOME}/.venvs/pj-catalog/bin/python3"

# ── harness server config (NEVER :8080 / the user's PID file) ─────────────────
readonly SMOKE_PORT=8081
readonly SMOKE_WS="ws://localhost:${SMOKE_PORT}/api/ws"
readonly SMOKE_LOG="/tmp/pj-cloud-smoke-server.log"
# Step g restarts the harness server on a SECOND log to prove it comes back
# clean off the SAME (builder-owned) catalog DB.
readonly SMOKE_LOG2="/tmp/pj-cloud-smoke-server-2.log"
readonly SMOKE_DB="/tmp/pj-cloud-smoke-catalog.db"
readonly SMOKE_CONFIG="/tmp/pj-cloud-smoke-config.yaml"
readonly SMOKE_FIXTURES_DIR="/tmp/pj-cloud-smoke-fixtures"
# Dedicated bucket: NEVER the interactive `recordings` bucket. Wiped + reseeded
# fresh at the start of every run (step a) so file counts are always exact.
readonly SMOKE_BUCKET="smoke-hive"
readonly MINIO_HEALTH="http://localhost:9000/minio/health/live"
readonly MINIO_NETWORK="minio_default"
readonly MC_IMAGE="minio/mc:RELEASE.2024-06-12T14-34-03Z"
# Minio dev credentials (infra/minio/docker-compose.yml) — shared by `seed`,
# the Python builder's boto3 client (via env), and the `mc` throwaway containers.
readonly SMOKE_S3_ACCESS_KEY="admin"
readonly SMOKE_S3_SECRET_KEY="password123"
readonly SMOKE_S3_ENDPOINT="http://localhost:9000"
readonly SMOKE_S3_REGION="us-east-1"

# Python builder daemon: a UNIX socket under /tmp with a SHORT path (AF_UNIX's
# ~108-char limit) keyed by this script's PID so concurrent runs never collide.
readonly TAG_SOCKET="/tmp/pj-smoke-tag.$$.sock"
readonly BUILDER_LOG="/tmp/pj-cloud-smoke-builder.log"
readonly BUILDER_LOG2="/tmp/pj-cloud-smoke-builder-2.log"
readonly REBUILD_LOG="/tmp/pj-cloud-smoke-builder-rebuild.log"
# Rescan-only daemon (--no-watch): discovery is purely periodic. Short so step
# h's forced-rebuild-detection leg (which does NOT depend on this — it uses
# --once --rebuild — but the daemon resumes rescanning after restart) stays
# fast in general use.
readonly RESCAN_INTERVAL=5

# ── go toolchain on PATH (per project context) ───────────────────────────────
export PATH="$HOME/.local/go/bin:$HOME/go/bin:$PATH"

# ── state for cleanup ─────────────────────────────────────────────────────────
SMOKE_SERVER_PID=""
SMOKE_BUILDER_PID=""
# Per-run scratch dir for step f's round-trip artifacts. Removed on EVERY exit
# path (success, fail, signal).
SMOKE_WORKDIR=""
# Set by compute_local_ground_truth (step a) / derive_server_ground_truth
# (step c); consumed by steps e/f/g/h. Not `readonly` — computed at runtime.
ACTUAL_FILE_COUNT=""
GROUND_TRUTH_TARGET=""
GROUND_TRUTH_STITCH_B=""
LOCAL_TARGET_TOTAL=""
LOCAL_TARGET_TOPIC_COUNT=""
LOCAL_TARGET_TOPIC_NAMES=""
LOCAL_TARGET_SUBSET_MSGS=""
LOCAL_TARGET_HALF_MSGS=""
LOCAL_STITCH_B_TOTAL=""
LOCAL_STITCH_MSGS=""
# LOCAL_TARGET_START_NS/END_NS: the INDEPENDENT ORACLE for the target's
# recorded time range, derived by compute_local_ground_truth (step a) from
# mcaptopics run on the LOCAL pre-upload original. step c asserts the
# server's own reported start_ns/end_ns EQUAL these (S3: closes the previous
# circularity where the window leg derived its window from the very
# server-reported values it was meant to be checking). step f4's time-range
# window is computed from these LOCAL values, never from the server.
LOCAL_TARGET_START_NS=""
LOCAL_TARGET_END_NS=""
TARGET_ID=""
TARGET_START_NS=""
TARGET_END_NS=""
STITCH_B_ID=""

log()  { printf '[smoke] %s\n' "$*"; }
# fail — record the verdict and exit non-zero. Does NOT print the verdict
# itself: the EXIT trap (cleanup) is guaranteed to run after this (even on a
# plain `exit`), and it is the trap's job to emit the verdict as the FINAL
# line of output, strictly after every cleanup/teardown log line (B1: the
# final-line contract must hold even though cleanup logs "stopping ..." for
# any still-running harness server/builder daemon).
SMOKE_VERDICT=""
fail() { SMOKE_VERDICT="SMOKE FAIL: $*"; exit 1; }

# Always reap the harness server AND the builder daemon (and only them),
# remove all scratch state, and — LAST, after all of that logging — print the
# verdict, on any exit path (normal completion, fail(), or an unguarded `set
# -e` failure that skipped fail() entirely).
cleanup() {
  local rc=$?
  if [[ -n "${SMOKE_WORKDIR}" && -d "${SMOKE_WORKDIR}" ]]; then
    rm -rf "${SMOKE_WORKDIR}"
  fi
  if [[ -n "${SMOKE_SERVER_PID}" ]] && kill -0 "${SMOKE_SERVER_PID}" 2>/dev/null; then
    log "stopping harness server (pid ${SMOKE_SERVER_PID})"
    kill "${SMOKE_SERVER_PID}" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "${SMOKE_SERVER_PID}" 2>/dev/null || break
      sleep 0.3
    done
    kill -9 "${SMOKE_SERVER_PID}" 2>/dev/null || true
  fi
  if [[ -n "${SMOKE_BUILDER_PID}" ]] && kill -0 "${SMOKE_BUILDER_PID}" 2>/dev/null; then
    log "stopping builder daemon (pid ${SMOKE_BUILDER_PID})"
    kill -TERM "${SMOKE_BUILDER_PID}" 2>/dev/null || true
    for _ in $(seq 1 20); do
      kill -0 "${SMOKE_BUILDER_PID}" 2>/dev/null || break
      sleep 0.3
    done
    kill -9 "${SMOKE_BUILDER_PID}" 2>/dev/null || true
  fi
  rm -f "${SMOKE_DB}" "${SMOKE_DB}-wal" "${SMOKE_DB}-shm" \
        "${SMOKE_LOG2}" "${BUILDER_LOG2}" "${REBUILD_LOG}" \
        "${SMOKE_CONFIG}" "${TAG_SOCKET}" 2>/dev/null || true
  rm -rf "${SMOKE_FIXTURES_DIR}" 2>/dev/null || true

  # Fallback for exit paths that bypassed fail() (e.g. a bare `set -e`
  # failure) — never leave SMOKE_VERDICT unset.
  if [[ -z "${SMOKE_VERDICT}" ]]; then
    if (( rc == 0 )); then
      SMOKE_VERDICT="SMOKE PASS"
    else
      SMOKE_VERDICT="SMOKE FAIL: unexpected error (exit code ${rc})"
    fi
  fi
  printf '%s\n' "${SMOKE_VERDICT}"
  return $rc
}
trap cleanup EXIT

# stop_harness_server — stop the current harness server (only it) and wait for
# it to exit, so step g can restart on the same DB without a port clash.
stop_harness_server() {
  [[ -n "${SMOKE_SERVER_PID}" ]] || return 0
  kill "${SMOKE_SERVER_PID}" 2>/dev/null || true
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    kill -0 "${SMOKE_SERVER_PID}" 2>/dev/null || break
    sleep 0.3
  done
  kill -9 "${SMOKE_SERVER_PID}" 2>/dev/null || true
  SMOKE_SERVER_PID=""
}

# stop_builder_daemon — graceful SIGTERM (the Python daemon's signal handler
# drains the work queue, unlinks the tag socket, and closes the DB cleanly);
# waits, then hard-kills if it doesn't exit.
stop_builder_daemon() {
  [[ -n "${SMOKE_BUILDER_PID}" ]] || return 0
  kill -TERM "${SMOKE_BUILDER_PID}" 2>/dev/null || true
  for _ in $(seq 1 20); do
    kill -0 "${SMOKE_BUILDER_PID}" 2>/dev/null || break
    sleep 0.3
  done
  kill -9 "${SMOKE_BUILDER_PID}" 2>/dev/null || true
  SMOKE_BUILDER_PID=""
}

# wait_http URL TIMEOUT_SECS — poll a URL until it answers 2xx/3xx or times out.
wait_http() {
  local url="$1" timeout="$2" waited=0
  while ! curl -fsS -m 3 -o /dev/null "${url}" 2>/dev/null; do
    sleep 1
    waited=$((waited + 1))
    if (( waited >= timeout )); then
      return 1
    fi
  done
  return 0
}

# wait_unix_socket PATH TIMEOUT_SECS — poll for a UNIX socket special file.
wait_unix_socket() {
  local path="$1" timeout="$2" waited=0
  while [[ ! -S "${path}" ]]; do
    sleep 1
    waited=$((waited + 1))
    if (( waited >= timeout )); then
      return 1
    fi
  done
  return 0
}

# db_query DB SQL — run one query against a SQLite DB read-only via Python's
# stdlib sqlite3 (no `sqlite3` CLI dependency; WAL mode allows this to run
# concurrently with the builder's own connection). Prints the first row's
# columns tab-separated; empty output (and failure) if the DB/row is absent —
# callers treat that as "not ready yet", not a hard error.
db_query() {
  local db="$1" sql="$2"
  python3 - "${db}" "${sql}" <<'PY' 2>/dev/null
import sqlite3, sys
path, sql = sys.argv[1], sys.argv[2]
try:
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
except Exception:
    sys.exit(1)
try:
    row = conn.execute(sql).fetchone()
    if row is None:
        sys.exit(1)
    print("\t".join("" if v is None else str(v) for v in row))
finally:
    conn.close()
PY
}

# wait_catalog_built EXPECTED_COUNT TIMEOUT_SECS — poll build_metadata until a
# completed, failure-free build covering exactly EXPECTED_COUNT files lands.
# S1: every poll also `kill -0`s the builder daemon PID (SMOKE_BUILDER_PID) —
# if it has died, fail IMMEDIATELY (with the builder log tail) instead of
# blindly waiting out the rest of the timeout for a build that will never
# land.
wait_catalog_built() {
  local expected="$1" timeout="$2" waited=0
  while true; do
    if [[ -n "${SMOKE_BUILDER_PID}" ]] && ! kill -0 "${SMOKE_BUILDER_PID}" 2>/dev/null; then
      log "----- builder log (tail) -----"; tail -n 40 "${BUILDER_LOG}" || true
      fail "builder: catalog builder daemon (pid ${SMOKE_BUILDER_PID}) died while waiting for the initial catalog build"
    fi
    local row
    if row="$(db_query "${SMOKE_DB}" "SELECT files_scanned, files_failed, build_outcome FROM build_metadata WHERE id=1")" && [[ -n "${row}" ]]; then
      local scanned failed outcome
      IFS=$'\t' read -r scanned failed outcome <<<"${row}"
      if [[ "${scanned}" == "${expected}" && "${failed}" == "0" && "${outcome}" == "ok" ]]; then
        return 0
      fi
    fi
    sleep 1
    waited=$((waited + 1))
    if (( waited >= timeout )); then
      return 1
    fi
  done
}

# write_smoke_config — a minimal YAML pointing the Go server at SMOKE_BUCKET.
# Only the storage.s3 block is needed: server.listen is overridden by -listen,
# catalog.db_path by -db, external-builder/tag-ipc-socket by their own flags
# (main.go: flags always win over config).
write_smoke_config() {
  cat > "${SMOKE_CONFIG}" <<EOF
storage:
  s3:
    bucket: ${SMOKE_BUCKET}
    region: ${SMOKE_S3_REGION}
    endpoint: ${SMOKE_S3_ENDPOINT}
    access_key: ${SMOKE_S3_ACCESS_KEY}
    secret_key: ${SMOKE_S3_SECRET_KEY}
EOF
}

# start_builder_daemon LOGFILE — launch the Python catalog builder as a
# rescan-only daemon (--no-watch: no SQS/inotify producer, purely periodic
# rescans) against SMOKE_BUCKET, with the tag-edit IPC socket enabled. Sets
# SMOKE_BUILDER_PID. Runs from mcap_catalog/ (the package root) with S3
# endpoint/creds passed as plain AWS env vars — boto3 (>=1.26 / botocore
# >=1.29) honors AWS_ENDPOINT_URL for a Minio-compatible endpoint override with
# NO code change to the builder needed.
start_builder_daemon() {
  local logfile="$1"
  : > "${logfile}"
  ( cd "${MCAP_CATALOG_DIR}" && exec env \
      AWS_ACCESS_KEY_ID="${SMOKE_S3_ACCESS_KEY}" \
      AWS_SECRET_ACCESS_KEY="${SMOKE_S3_SECRET_KEY}" \
      AWS_ENDPOINT_URL="${SMOKE_S3_ENDPOINT}" \
      AWS_REGION="${SMOKE_S3_REGION}" AWS_DEFAULT_REGION="${SMOKE_S3_REGION}" \
      "${VENV_PY}" -m mcap_catalog_builder --source s3 --s3-bucket "${SMOKE_BUCKET}" --no-watch \
      --tag-socket "${TAG_SOCKET}" --db "${SMOKE_DB}" --rescan-interval "${RESCAN_INTERVAL}" \
      --log-level INFO >>"${logfile}" 2>&1 ) &
  SMOKE_BUILDER_PID=$!
}

# run_builder_once_rebuild LOGFILE — synchronous (foreground) `--once
# --rebuild`: builds a fresh temp DB, carries tags_override forward by
# composite identity, and atomically publishes over SMOKE_DB (a NEW inode).
# Must be called with the daemon STOPPED (a live daemon already holds the
# served path open; this is a distinct one-shot process, not a second
# concurrent writer). S2: wrapped in `timeout 120s` so a wedged/hanging
# rebuild fails fast with clear diagnostics instead of hanging the whole
# harness indefinitely; the caller's existing `|| { cat logfile; fail ...}`
# picks up the extra TIMED OUT line appended below.
run_builder_once_rebuild() {
  local logfile="$1" rc=0
  ( cd "${MCAP_CATALOG_DIR}" && env \
      AWS_ACCESS_KEY_ID="${SMOKE_S3_ACCESS_KEY}" \
      AWS_SECRET_ACCESS_KEY="${SMOKE_S3_SECRET_KEY}" \
      AWS_ENDPOINT_URL="${SMOKE_S3_ENDPOINT}" \
      AWS_REGION="${SMOKE_S3_REGION}" AWS_DEFAULT_REGION="${SMOKE_S3_REGION}" \
      timeout 120s "${VENV_PY}" -m mcap_catalog_builder --source s3 --s3-bucket "${SMOKE_BUCKET}" \
      --once --rebuild --db "${SMOKE_DB}" --log-level INFO ) >>"${logfile}" 2>&1 || rc=$?
  if [[ "${rc}" == 124 ]]; then
    printf '[smoke] run_builder_once_rebuild: TIMED OUT after 120s (killed by `timeout`)\n' >>"${logfile}"
  fi
  return "${rc}"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step a: Minio up, dedicated bucket wiped + reseeded with a FRESH synthetic
# Hive-keyed corpus, LOCAL ground-truth oracle derived via mcaptopics.
# ─────────────────────────────────────────────────────────────────────────────
step_minio_and_fixtures() {
  log "step a: building Go tools (server + devprobe + mcapdiff + mcaptopics + seed + fixture generators)"
  command -v go >/dev/null 2>&1 || fail "tools: go not on PATH (\$HOME/.local/go/bin expected)"
  ( cd "${SERVER_DIR}" && go build -o ./bin/pj-cloud-server ./cmd/pj-cloud-server ) \
    || fail "tools: go build pj-cloud-server failed"
  ( cd "${SERVER_DIR}" && go build -o ./bin/devprobe ./cmd/devprobe ) \
    || fail "tools: go build devprobe failed"
  ( cd "${SERVER_DIR}" && go build -o ./bin/mcapdiff ./cmd/mcapdiff ) \
    || fail "tools: go build mcapdiff failed"
  ( cd "${SERVER_DIR}" && go build -o ./bin/mcaptopics ./cmd/mcaptopics ) \
    || fail "tools: go build mcaptopics failed"
  ( cd "${SERVER_DIR}" && go build -o ./bin/seed ./cmd/seed ) \
    || fail "tools: go build seed failed"
  ( cd "${SERVER_DIR}" && go build -o ./bin/gen-ci-fixtures ./cmd/gen-ci-fixtures ) \
    || fail "tools: go build gen-ci-fixtures failed"
  ( cd "${SERVER_DIR}" && go build -o ./bin/gen-3d-fixture ./cmd/gen-3d-fixture ) \
    || fail "tools: go build gen-3d-fixture failed"

  log "step a: ensuring Minio is up"
  [[ -f "${COMPOSE_FILE}" ]] || fail "minio: compose file not found at ${COMPOSE_FILE}"
  docker compose -f "${COMPOSE_FILE}" up -d >/dev/null 2>&1 \
    || fail "minio: docker compose up failed"
  wait_http "${MINIO_HEALTH}" 60 \
    || fail "minio: ${MINIO_HEALTH} did not become healthy within 60s"

  log "step a: wiping s3://${SMOKE_BUCKET} for an exact-count fresh corpus"
  docker run --rm --network "${MINIO_NETWORK}" --entrypoint sh "${MC_IMAGE}" \
      -c "mc alias set local http://minio:9000 admin password123 >/dev/null 2>&1 && mc rb --force local/${SMOKE_BUCKET} >/dev/null 2>&1; exit 0" \
      >/dev/null 2>&1 || true

  log "step a: generating a fresh deterministic synthetic Hive-keyed corpus"
  rm -rf "${SMOKE_FIXTURES_DIR}"
  mkdir -p "${SMOKE_FIXTURES_DIR}"
  "${SERVER_DIR}/bin/gen-ci-fixtures" -hive -hive-big -out "${SMOKE_FIXTURES_DIR}" >/dev/null \
    || fail "fixtures: gen-ci-fixtures failed"

  # gen-3d-fixture has no -hive mode of its own: generate flat, then place the
  # single output file under a Hive path ourselves (the stitch-B partition).
  local threed_tmp="${SMOKE_FIXTURES_DIR}.3d-tmp"
  rm -rf "${threed_tmp}"
  mkdir -p "${threed_tmp}"
  "${SERVER_DIR}/bin/gen-3d-fixture" -out "${threed_tmp}" >/dev/null \
    || fail "fixtures: gen-3d-fixture failed"
  local threed_hive_dir="${SMOKE_FIXTURES_DIR}/customer=${HIVE_CUSTOMER}/customer_site=${HIVE_SITE}/robot=${THREED_ROBOT}/source=${HIVE_SOURCE}/date=${THREED_DATE}"
  mkdir -p "${threed_hive_dir}"
  local threed_file
  threed_file="$(find "${threed_tmp}" -maxdepth 1 -name '*.mcap' | head -n1)"
  [[ -n "${threed_file}" ]] || fail "fixtures: gen-3d-fixture produced no *.mcap output"
  mv "${threed_file}" "${threed_hive_dir}/" || fail "fixtures: could not place the 3D fixture under a Hive path"
  rm -rf "${threed_tmp}"

  ACTUAL_FILE_COUNT="$(find "${SMOKE_FIXTURES_DIR}" -name '*.mcap' | wc -l | tr -d ' ')"
  (( ACTUAL_FILE_COUNT > 0 )) || fail "fixtures: no *.mcap fixtures were generated"
  log "step a: generated ${ACTUAL_FILE_COUNT} synthetic fixture(s) under ${SMOKE_FIXTURES_DIR}"

  GROUND_TRUTH_TARGET="${SMOKE_FIXTURES_DIR}/${TARGET_KEY}"
  GROUND_TRUTH_STITCH_B="${SMOKE_FIXTURES_DIR}/${STITCH_B_KEY}"
  [[ -f "${GROUND_TRUTH_TARGET}" ]] || fail "fixtures: target fixture not found at ${GROUND_TRUTH_TARGET} (DefaultSpecs ordering changed?)"
  [[ -f "${GROUND_TRUTH_STITCH_B}" ]] || fail "fixtures: stitch-B fixture not found at ${GROUND_TRUTH_STITCH_B} (DefaultSpecs ordering changed?)"

  log "step a: seeding s3://${SMOKE_BUCKET} (bucket auto-created if missing; -force since the wipe above may be a no-op on a first-ever run)"
  "${SERVER_DIR}/bin/seed" -dir "${SMOKE_FIXTURES_DIR}" -bucket "${SMOKE_BUCKET}" \
      -endpoint "${SMOKE_S3_ENDPOINT}" -access-key "${SMOKE_S3_ACCESS_KEY}" -secret-key "${SMOKE_S3_SECRET_KEY}" \
      -region "${SMOKE_S3_REGION}" -force \
    || fail "fixtures: seed upload to s3://${SMOKE_BUCKET} failed"

  compute_local_ground_truth
  log "step a: OK (Minio healthy, ${ACTUAL_FILE_COUNT} fixtures seeded into ${SMOKE_BUCKET}, local oracle derived)"
}

# compute_local_ground_truth — the INDEPENDENT ORACLE: run mcaptopics directly
# on the LOCAL pre-upload fixture files (never through the server/catalog) to
# derive every message-count expectation used later. Sets LOCAL_* globals.
compute_local_ground_truth() {
  log "step a: deriving LOCAL ground truth (independent oracle) via mcaptopics"
  local mcaptopics_bin="${SERVER_DIR}/bin/mcaptopics"
  [[ -x "${mcaptopics_bin}" ]] || fail "fixtures: mcaptopics binary missing at ${mcaptopics_bin}"

  local target_json
  target_json="$("${mcaptopics_bin}" "${GROUND_TRUTH_TARGET}")" \
    || fail "fixtures: mcaptopics failed on ${GROUND_TRUTH_TARGET}"
  # N2: LOCAL_TARGET_HALF_MSGS is derived from HALF_TOPICS itself (never a
  # hardcoded topic pair) so the two can never silently drift apart. S3:
  # LOCAL_TARGET_START_NS/END_NS are the independent oracle for the target's
  # recorded time range (mcaptopics' own min/max observed log_time — see
  # cmd/mcaptopics — not the server's ListFiles response).
  eval "$(SMOKE_JSON="${target_json}" python3 - "${SUBSET_TOPIC}" "${HALF_TOPICS}" <<'PY'
import json, os, sys
d = json.loads(os.environ["SMOKE_JSON"])
subset_topic = sys.argv[1]
half_topics = sys.argv[2].split(",")
by = {t["name"]: t["message_count"] for t in d["topics"]}
print(f'LOCAL_TARGET_TOTAL={d["message_count"]}')
print(f'LOCAL_TARGET_TOPIC_COUNT={d["topic_count"]}')
print(f'LOCAL_TARGET_TOPIC_NAMES="{",".join(sorted(by.keys()))}"')
print(f'LOCAL_TARGET_SUBSET_MSGS={by.get(subset_topic, 0)}')
print(f'LOCAL_TARGET_HALF_MSGS={sum(by.get(t, 0) for t in half_topics)}')
print(f'LOCAL_TARGET_START_NS={d["start_ns"]}')
print(f'LOCAL_TARGET_END_NS={d["end_ns"]}')
PY
)" || fail "fixtures: could not parse local mcaptopics output for target"

  local stitch_b_json
  stitch_b_json="$("${mcaptopics_bin}" "${GROUND_TRUTH_STITCH_B}")" \
    || fail "fixtures: mcaptopics failed on ${GROUND_TRUTH_STITCH_B}"
  eval "$(SMOKE_JSON="${stitch_b_json}" python3 - <<'PY'
import json, os
d = json.loads(os.environ["SMOKE_JSON"])
print(f'LOCAL_STITCH_B_TOTAL={d["message_count"]}')
PY
)" || fail "fixtures: could not parse local mcaptopics output for stitch-B"

  LOCAL_STITCH_MSGS=$(( LOCAL_TARGET_TOTAL + LOCAL_STITCH_B_TOTAL ))
  log "step a: local oracle — target(${TARGET_KEY})=${LOCAL_TARGET_TOTAL}msgs/${LOCAL_TARGET_TOPIC_COUNT}topics[${LOCAL_TARGET_TOPIC_NAMES}] [${LOCAL_TARGET_START_NS},${LOCAL_TARGET_END_NS}], stitch_b=${LOCAL_STITCH_B_TOTAL}msgs, subset(${SUBSET_TOPIC})=${LOCAL_TARGET_SUBSET_MSGS}, half(${HALF_TOPICS})=${LOCAL_TARGET_HALF_MSGS}, stitched=${LOCAL_STITCH_MSGS}"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step b1: start the Python catalog builder daemon (--no-watch, rescan-only)
# against the freshly-seeded bucket, with the tag-edit IPC socket enabled.
# ─────────────────────────────────────────────────────────────────────────────
step_builder_daemon() {
  log "step b1: starting the Python catalog builder daemon"
  [[ -x "${VENV_PY}" ]] || fail "builder: venv interpreter not found at ${VENV_PY} — bootstrap it: python3 -m venv ~/.venvs/pj-catalog && ~/.venvs/pj-catalog/bin/pip install boto3==1.43.40 google-cloud-storage==3.12.0 mcap==1.4.0 watchdog==6.0.0"

  rm -f "${SMOKE_DB}" "${SMOKE_DB}-wal" "${SMOKE_DB}-shm" "${TAG_SOCKET}"
  start_builder_daemon "${BUILDER_LOG}"

  wait_catalog_built "${ACTUAL_FILE_COUNT}" 60 || {
    log "----- builder log (tail) -----"; tail -n 40 "${BUILDER_LOG}" || true
    fail "builder: catalog not built (files_scanned==${ACTUAL_FILE_COUNT}, failed=0, outcome=ok) within 60s"
  }
  local failures
  failures="$(db_query "${SMOKE_DB}" "SELECT COUNT(*) FROM catalog_failures")"
  [[ "${failures}" == "0" ]] || {
    log "----- builder log (tail) -----"; tail -n 40 "${BUILDER_LOG}" || true
    fail "builder: catalog_failures is non-empty (${failures}) after the initial build"
  }
  wait_unix_socket "${TAG_SOCKET}" 20 \
    || fail "builder: tag-edit IPC socket did not appear at ${TAG_SOCKET} within 20s"
  log "step b1: OK (builder daemon pid ${SMOKE_BUILDER_PID}; catalog built cold: ${ACTUAL_FILE_COUNT} files, 0 failures; tag-IPC listening)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step b2: build (already done in step a) and start the Go server in
# EXTERNAL-BUILDER (read-only) mode against the Python-built catalog, with
# tag-edit IPC forwarding wired.
# ─────────────────────────────────────────────────────────────────────────────
step_server() {
  log "step b2: starting harness server on :${SMOKE_PORT} (external-builder, db ${SMOKE_DB})"
  write_smoke_config
  : > "${SMOKE_LOG}"
  # Run WITHOUT PJ_CLOUD_TOKEN -> dev anonymous mode (the C++ live test connects
  # with an empty api_key). `exec` so the server process REPLACES the subshell.
  ( cd "${SERVER_DIR}" && exec env -u PJ_CLOUD_TOKEN ./bin/pj-cloud-server \
      -config "${SMOKE_CONFIG}" -listen ":${SMOKE_PORT}" -db "${SMOKE_DB}" \
      -tag-ipc-socket "${TAG_SOCKET}" -allow-anonymous \
      >>"${SMOKE_LOG}" 2>&1 ) &
  SMOKE_SERVER_PID=$!

  if ! wait_http "http://localhost:${SMOKE_PORT}/health" 60; then
    log "----- harness server log (tail) -----"
    tail -n 40 "${SMOKE_LOG}" || true
    fail "server: :${SMOKE_PORT}/health did not come up within 60s"
  fi

  grep -q 'catalog: opened SQLite store READ-ONLY (external builder)' "${SMOKE_LOG}" \
    || { tail -n 40 "${SMOKE_LOG}"; fail "server: did not log opening the catalog read-only (external-builder mode not engaged?)"; }
  grep -q 'indexer: DISABLED (external-builder read-only mode)' "${SMOKE_LOG}" \
    || { tail -n 40 "${SMOKE_LOG}"; fail "server: did not log the indexer being disabled"; }

  log "step b2: OK (harness server pid ${SMOKE_SERVER_PID}, /health up, external-builder mode confirmed)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step c: Go-side discovery assertions via devprobe, cross-checked against the
# LOCAL mcaptopics oracle (never re-derived from the server itself).
# ─────────────────────────────────────────────────────────────────────────────
step_go_assertions() {
  log "step c: Go discovery assertions (ListFiles/GetFile/Hello vs the local oracle)"
  local probe="${SERVER_DIR}/bin/devprobe"
  [[ -x "${probe}" ]] || fail "go-assert: devprobe binary missing at ${probe}"

  local list_json
  list_json="$("${probe}" -url "${SMOKE_WS}" 2>/dev/null)" \
    || fail "go-assert: devprobe (ListFiles) failed against ${SMOKE_WS}"

  eval "$(SMOKE_JSON="${list_json}" python3 - "${TARGET_KEY}" "${STITCH_B_KEY}" <<'PY'
import json, os, sys
d = json.loads(os.environ["SMOKE_JSON"])
target_key, stitch_b_key = sys.argv[1], sys.argv[2]
print(f'SERVER_FILE_COUNT={d.get("file_count", -1)}')
print(f'SERVER_TAG_EDIT_SUPPORTED={str(d.get("hello_response", {}).get("tag_edit_supported")).lower()}')
print(f'SERVER_SUPPORTS_HIERARCHY={str(d.get("hello_response", {}).get("supports_file_hierarchy")).lower()}')
tgt = next((f for f in d.get("files", []) if f.get("s3_key") == target_key), None)
if tgt is None:
    sys.stderr.write(f"target key {target_key!r} not found in ListFiles\n"); sys.exit(2)
sb = next((f for f in d.get("files", []) if f.get("s3_key") == stitch_b_key), None)
if sb is None:
    sys.stderr.write(f"stitch-B key {stitch_b_key!r} not found in ListFiles\n"); sys.exit(3)
print(f'TARGET_ID={tgt["id"]}')
print(f'TARGET_MSGS={tgt["message_count"]}')
print(f'TARGET_START_NS={tgt["start_ns"]}')
print(f'TARGET_END_NS={tgt["end_ns"]}')
print(f'STITCH_B_ID={sb["id"]}')
print(f'STITCH_B_MSGS={sb["message_count"]}')
PY
)" || fail "go-assert: could not resolve target/stitch-B entries from ListFiles (see python error above)"

  [[ "${SERVER_FILE_COUNT}" == "${ACTUAL_FILE_COUNT}" ]] \
    || fail "go-assert: file_count=${SERVER_FILE_COUNT} != generated fixture count ${ACTUAL_FILE_COUNT}"
  [[ "${SERVER_TAG_EDIT_SUPPORTED}" == "true" ]] \
    || fail "go-assert: hello tag_edit_supported=${SERVER_TAG_EDIT_SUPPORTED} (want true — external-builder + tag-ipc-socket configured)"
  [[ "${SERVER_SUPPORTS_HIERARCHY}" == "true" ]] \
    || fail "go-assert: hello supports_file_hierarchy=${SERVER_SUPPORTS_HIERARCHY} (want true — every key is Hive-partitioned with '/')"
  [[ "${TARGET_MSGS}" == "${LOCAL_TARGET_TOTAL}" ]] \
    || fail "go-assert: server message_count=${TARGET_MSGS} != local oracle ${LOCAL_TARGET_TOTAL} for ${TARGET_KEY}"
  [[ "${STITCH_B_MSGS}" == "${LOCAL_STITCH_B_TOTAL}" ]] \
    || fail "go-assert: server message_count=${STITCH_B_MSGS} != local oracle ${LOCAL_STITCH_B_TOTAL} for ${STITCH_B_KEY}"
  # S3: the server-reported recorded range must equal the LOCAL, independently
  # (mcaptopics)-derived range — this is a genuine cross-check now (step f4
  # computes its window from LOCAL_TARGET_START_NS/END_NS, never from these
  # server-reported values, so a server-side start/end bug can no longer hide
  # behind circular data).
  [[ "${TARGET_START_NS}" == "${LOCAL_TARGET_START_NS}" ]] \
    || fail "go-assert: server start_ns=${TARGET_START_NS} != local oracle ${LOCAL_TARGET_START_NS} for ${TARGET_KEY}"
  [[ "${TARGET_END_NS}" == "${LOCAL_TARGET_END_NS}" ]] \
    || fail "go-assert: server end_ns=${TARGET_END_NS} != local oracle ${LOCAL_TARGET_END_NS} for ${TARGET_KEY}"
  log "step c: file_count / hello capabilities / per-file totals / recorded-range OK; ${TARGET_KEY} -> file_id ${TARGET_ID}"

  local get_json
  get_json="$("${probe}" -url "${SMOKE_WS}" -get-file-id "${TARGET_ID}" 2>/dev/null)" \
    || fail "go-assert: devprobe (GetFile ${TARGET_ID}) failed"
  eval "$(SMOKE_JSON="${get_json}" python3 - "${TARGET_KEY}" <<'PY'
import json, os, sys
d = json.loads(os.environ["SMOKE_JSON"])
gf = d.get("get_file")
if gf is None:
    sys.stderr.write("get_file missing from devprobe output\n"); sys.exit(2)
if gf.get("s3_key") != sys.argv[1]:
    sys.stderr.write(f"GetFile s3_key mismatch: {gf.get('s3_key')!r} != {sys.argv[1]!r}\n"); sys.exit(3)
print(f'SERVER_TARGET_TOPIC_COUNT={gf["topic_count"]}')
print(f'SERVER_TARGET_TOPIC_NAMES="{",".join(gf["topic_names"])}"')
PY
)" || fail "go-assert: GetFile assertion failed (see python error above)"

  [[ "${SERVER_TARGET_TOPIC_COUNT}" == "${LOCAL_TARGET_TOPIC_COUNT}" ]] \
    || fail "go-assert: GetFile topic_count=${SERVER_TARGET_TOPIC_COUNT} != local oracle ${LOCAL_TARGET_TOPIC_COUNT}"
  [[ "${SERVER_TARGET_TOPIC_NAMES}" == "${LOCAL_TARGET_TOPIC_NAMES}" ]] \
    || fail "go-assert: GetFile topics=[${SERVER_TARGET_TOPIC_NAMES}] != local oracle [${LOCAL_TARGET_TOPIC_NAMES}]"

  log "step c: OK (GetFile topics cross-check the local oracle exactly: ${SERVER_TARGET_TOPIC_COUNT} topics [${SERVER_TARGET_TOPIC_NAMES}])"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step d: C++ SDK tests — hermetic (no env) then live (MCAP_CLOUD_LIVE_URL).
# Mechanically unchanged from the legacy harness; the gtest ground-truth
# constants themselves were re-pinned (plugin/toolbox_mcap_cloud/tests/) to
# this corpus in the same change that produced this script.
# ─────────────────────────────────────────────────────────────────────────────
step_cpp_tests() {
  log "step d: ensuring C++ plugin test build is current"
  [[ -f "${CTEST_DIR}/CTestTestfile.cmake" ]] \
    || fail "cpp: test build dir not configured at ${CTEST_DIR} (run: ./build.sh (from the repo root) to build the connector plugin)"
  cmake --build "${CTEST_DIR}" -j >/dev/null 2>&1 \
    || fail "cpp: incremental cmake --build failed in ${CTEST_DIR}"

  log "step d: ctest HERMETIC (live test expected to SKIP at gtest level)"
  ( cd "${CTEST_DIR}" && env -u MCAP_CLOUD_LIVE_URL ctest --output-on-failure ) \
    || fail "cpp: hermetic ctest failed"

  local live_bin="${CTEST_DIR}/bin/toolbox_mcap_cloud_backend_live_test"
  [[ -x "${live_bin}" ]] || fail "cpp: live test binary missing at ${live_bin}"
  local hermetic_out
  hermetic_out="$(env -u MCAP_CLOUD_LIVE_URL "${live_bin}" 2>&1)" || true
  printf '%s\n' "${hermetic_out}" | grep -q '\[  SKIPPED \]' \
    || { printf '%s\n' "${hermetic_out}"; fail "cpp: live test did NOT skip when MCAP_CLOUD_LIVE_URL is unset"; }
  log "step d: hermetic OK (suite green; live test correctly SKIPPED at gtest level)"

  log "step d: ctest LIVE against ws://localhost:${SMOKE_PORT} (live tests must RUN, not skip)"
  ( cd "${CTEST_DIR}" && MCAP_CLOUD_LIVE_URL="ws://localhost:${SMOKE_PORT}" \
      ctest --output-on-failure -R McapCloudBackendLive ) \
    || fail "cpp: live ctest (McapCloudBackendLive) failed"

  log "step d: ctest LIVE reconnect-resume + cache (McapCloudSessionResumeLive)"
  ( cd "${CTEST_DIR}" && MCAP_CLOUD_LIVE_URL="ws://localhost:${SMOKE_PORT}" \
      ctest --output-on-failure -R McapCloudSessionResumeLive ) \
    || fail "cpp: live ctest (McapCloudSessionResumeLive) failed"

  log "step d: ctest LIVE session download (McapCloudSessionDownloadLive)"
  ( cd "${CTEST_DIR}" && MCAP_CLOUD_LIVE_URL="ws://localhost:${SMOKE_PORT}" \
      ctest --output-on-failure -R McapCloudSessionDownloadLive ) \
    || fail "cpp: live ctest (McapCloudSessionDownloadLive) failed"

  log "step d: ctest LIVE worker parser-ingest ground-truth (McapCloudParserIngestLive)"
  ( cd "${CTEST_DIR}" && MCAP_CLOUD_LIVE_URL="ws://localhost:${SMOKE_PORT}" \
      ctest --output-on-failure -R McapCloudParserIngestLive ) \
    || fail "cpp: live ctest (McapCloudParserIngestLive) failed"

  local live_out
  if ! live_out="$(MCAP_CLOUD_LIVE_URL="ws://localhost:${SMOKE_PORT}" "${live_bin}" 2>&1)"; then
    printf '%s\n' "${live_out}"
    fail "cpp: live test binary failed in live mode"
  fi
  if printf '%s\n' "${live_out}" | grep -q '\[  SKIPPED \]'; then
    printf '%s\n' "${live_out}"
    fail "cpp: live test SKIPPED in live mode (MCAP_CLOUD_LIVE_URL was not honored)"
  fi
  if ! printf '%s\n' "${live_out}" | grep -qE '\[  PASSED  \] [1-9][0-9]* test'; then
    printf '%s\n' "${live_out}"
    fail "cpp: live test did not report any PASSED tests in live mode"
  fi
  log "step d: OK (hermetic green w/ real skip; live ran and PASSED)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step e: CLI spot check — hello + list + topics for the target sequence,
# through BOTH client stacks.
# ─────────────────────────────────────────────────────────────────────────────
step_cli_spotcheck() {
  log "step e1: C++ SDK CLI spot check (hello + list + topics for ${TARGET_KEY})"
  local sdk_cli="${CTEST_DIR}/bin/mcap-cloud-cli"
  [[ -x "${sdk_cli}" ]] || fail "cli: mcap-cloud-cli missing at ${sdk_cli} (built by step d's cmake --build)"

  local hello_out
  hello_out="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" hello)" \
    || fail "cli: mcap-cloud-cli hello failed"
  printf '%s' "${hello_out}" | grep -q "server_version" \
    || fail "cli: mcap-cloud-cli hello output missing server_version"

  local list_out
  list_out="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" list)" \
    || fail "cli: mcap-cloud-cli list failed"
  printf '%s' "${list_out}" | grep -qF "${TARGET_KEY}" \
    || fail "cli: mcap-cloud-cli list missing ${TARGET_KEY}"

  local cpp_topics_out
  cpp_topics_out="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" topics "${TARGET_KEY}")" \
    || fail "cli: mcap-cloud-cli topics failed"
  local ct
  IFS=',' read -r -a target_topics <<<"${LOCAL_TARGET_TOPIC_NAMES}"
  for ct in "${target_topics[@]}"; do
    printf '%s' "${cpp_topics_out}" | grep -qF "${ct}" \
      || fail "cli: mcap-cloud-cli topics missing ${ct}"
  done
  log "step e1: OK (C++ SDK CLI: hello + list + all ${#target_topics[@]} topics present)"

  log "step e2: Go devprobe spot check (hello + list + topics for ${TARGET_KEY})"
  local probe="${SERVER_DIR}/bin/devprobe"
  [[ -x "${probe}" ]] || fail "cli: devprobe binary missing at ${probe}"

  local out
  out="$("${probe}" -url "${SMOKE_WS}" -get-file-id "${TARGET_ID}" 2>/dev/null)" \
    || fail "cli: devprobe topics for ${TARGET_KEY} failed"

  printf '%s' "${out}" | grep -q '"server_version"' \
    || fail "cli: hello output missing server_version"
  printf '%s' "${out}" | grep -qF "${TARGET_KEY}" \
    || fail "cli: GetFile output missing ${TARGET_KEY}"
  local t
  for t in "${target_topics[@]}"; do
    printf '%s' "${out}" | grep -qF "${t}" \
      || fail "cli: topics output missing ${t}"
  done
  log "step e2: OK (devprobe: hello + GetFile + all ${#target_topics[@]} topics present)"
}

# resolve_file_id KEY — print the file id whose s3_key == KEY (via devprobe list).
resolve_file_id() {
  local key="$1" probe="${SERVER_DIR}/bin/devprobe" list_json
  list_json="$("${probe}" -url "${SMOKE_WS}" 2>/dev/null)" || return 1
  SMOKE_JSON="${list_json}" python3 - "${key}" <<'PY'
import json, os, sys
d = json.loads(os.environ["SMOKE_JSON"])
for f in d.get("files", []):
    if f.get("s3_key") == sys.argv[1]:
        print(f["id"]); break
else:
    sys.exit(1)
PY
}

# ─────────────────────────────────────────────────────────────────────────────
# Step f: streaming ROUND-TRIP — original MCAP -> server -> client ->
# reconstructed MCAP must be LOGICALLY EQUAL. Every expected count below comes
# from the LOCAL oracle (step a) or, for the time-range leg, a fresh `mcap
# filter` + `mcaptopics` pass over the local original — never a literal.
# ─────────────────────────────────────────────────────────────────────────────
step_roundtrip() {
  log "step f: streaming round-trip (mcapdiff logical equality, both client stacks)"

  local mcapdiff="${SERVER_DIR}/bin/mcapdiff"
  local mcaptopics="${SERVER_DIR}/bin/mcaptopics"
  local probe="${SERVER_DIR}/bin/devprobe"
  local sdk_cli="${CTEST_DIR}/bin/mcap-cloud-cli"
  [[ -x "${mcapdiff}" ]]   || fail "roundtrip: mcapdiff binary missing at ${mcapdiff}"
  [[ -x "${mcaptopics}" ]] || fail "roundtrip: mcaptopics binary missing at ${mcaptopics}"
  [[ -x "${probe}" ]]      || fail "roundtrip: devprobe binary missing at ${probe}"
  [[ -x "${sdk_cli}" ]]    || fail "roundtrip: mcap-cloud-cli missing at ${sdk_cli}"
  [[ -x "${MCAP_CLI}" ]]   || fail "roundtrip: official mcap CLI missing at ${MCAP_CLI}"
  [[ -f "${GROUND_TRUTH_TARGET}" ]] \
    || fail "roundtrip: ground-truth original not found at ${GROUND_TRUTH_TARGET}"

  SMOKE_WORKDIR="$(mktemp -d)" || fail "roundtrip: mktemp -d failed"
  local work="${SMOKE_WORKDIR}"

  local cpp_full="${work}/cpp_full.mcap"
  local cpp_subset="${work}/cpp_subset.mcap"
  local go_full="${work}/go_full.mcap"

  # ── f1: C++ SDK CLI full download -> mcapdiff vs original ──
  log "step f1: C++ SDK CLI FULL download of ${TARGET_KEY}"
  "${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download "${TARGET_KEY}" \
      --output "${cpp_full}" >/dev/null 2>&1 \
    || fail "roundtrip: C++ CLI full download failed"
  [[ -s "${cpp_full}" ]] || fail "roundtrip: C++ CLI full download produced no output"
  if ! "${mcapdiff}" "${GROUND_TRUTH_TARGET}" "${cpp_full}"; then
    fail "roundtrip: C++ CLI full download NOT logically equal to original (mcapdiff above)"
  fi
  log "step f1: OK (C++ CLI full round-trip clean; ${LOCAL_TARGET_TOTAL} msgs)"

  # ── f2: C++ SDK CLI subset download -> mcapdiff --topics ──
  log "step f2: C++ SDK CLI SUBSET download (--topics ${SUBSET_TOPIC})"
  local subset_json subset_count
  subset_json="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download "${TARGET_KEY}" \
      --topics "${SUBSET_TOPIC}" --output "${cpp_subset}" --json 2>/dev/null)" \
    || fail "roundtrip: C++ CLI subset download failed"
  [[ -s "${cpp_subset}" ]] || fail "roundtrip: C++ CLI subset download produced no output"
  subset_count="$(SUBSET_JSON="${subset_json}" python3 -c \
    'import json,os; print(json.loads(os.environ["SUBSET_JSON"]).get("messages_received",-1))')" \
    || fail "roundtrip: could not parse subset download --json"
  [[ "${subset_count}" == "${LOCAL_TARGET_SUBSET_MSGS}" ]] \
    || fail "roundtrip: subset messages_received=${subset_count} != local oracle ${LOCAL_TARGET_SUBSET_MSGS}"
  if ! "${mcapdiff}" --topics "${SUBSET_TOPIC}" "${GROUND_TRUTH_TARGET}" "${cpp_subset}"; then
    fail "roundtrip: C++ CLI subset NOT clean (under/over-delivery or drift; mcapdiff above)"
  fi
  log "step f2: OK (C++ CLI subset round-trip clean, no over-delivery; ${LOCAL_TARGET_SUBSET_MSGS} msgs)"

  # ── f3: Go devprobe full download -> mcapdiff vs original ──
  log "step f3: Go devprobe FULL download of ${TARGET_KEY}"
  "${probe}" -url "${SMOKE_WS}" -download "${TARGET_KEY}" -out "${go_full}" >/dev/null 2>&1 \
    || fail "roundtrip: Go devprobe full download failed"
  [[ -s "${go_full}" ]] || fail "roundtrip: Go devprobe full download produced no output"
  if ! "${mcapdiff}" "${GROUND_TRUTH_TARGET}" "${go_full}"; then
    fail "roundtrip: Go devprobe full download NOT logically equal to original (mcapdiff above)"
  fi
  log "step f3: OK (Go devprobe full round-trip clean)"

  # ── f4: C++ SDK CLI TIME-RANGE download. The window is a fraction of the
  # target's OWN LOCALLY-derived [start_ns,end_ns) (S3: never the server's
  # reported range — step c already cross-checked that the two agree, so
  # deriving the window from the server's own numbers here would be
  # circular); the expected count comes from filtering the LOCAL original
  # with the official mcap CLI, then re-deriving the count with mcaptopics —
  # the independent oracle, never a literal. ──
  local duration_ns=$(( LOCAL_TARGET_END_NS - LOCAL_TARGET_START_NS ))
  (( duration_ns > 0 )) || fail "roundtrip: target duration_ns=${duration_ns} is not positive"
  local window_start_ns=$(( LOCAL_TARGET_START_NS + duration_ns * WINDOW_START_PCT / 100 ))
  local window_end_ns=$(( LOCAL_TARGET_START_NS + duration_ns * WINDOW_END_PCT / 100 ))
  local windowed_ref="${work}/windowed_ref.mcap"
  "${MCAP_CLI}" filter "${GROUND_TRUTH_TARGET}" -o "${windowed_ref}" \
      --start "${window_start_ns}" --end "${window_end_ns}" >/dev/null 2>&1 \
    || fail "roundtrip: mcap filter (windowed reference) failed"
  local expect_window_msgs
  expect_window_msgs="$("${mcaptopics}" "${windowed_ref}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["message_count"])')" \
    || fail "roundtrip: could not derive the expected windowed count"
  (( expect_window_msgs > 0 && expect_window_msgs < LOCAL_TARGET_TOTAL )) \
    || fail "roundtrip: windowed reference has ${expect_window_msgs} msgs (want a genuine partial window, 0 < n < ${LOCAL_TARGET_TOTAL})"

  local cpp_window="${work}/cpp_window.mcap"
  log "step f4: C++ SDK CLI TIME-RANGE download (${window_start_ns},${window_end_ns}) -> expect ${expect_window_msgs} msgs"
  local window_json window_count
  window_json="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download "${TARGET_KEY}" \
      --time-range "${window_start_ns},${window_end_ns}" --output "${cpp_window}" --json 2>/dev/null)" \
    || fail "roundtrip: C++ CLI time-range download failed"
  [[ -s "${cpp_window}" ]] || fail "roundtrip: C++ CLI time-range download produced no output"
  window_count="$(WINDOW_JSON="${window_json}" python3 -c \
    'import json,os; print(json.loads(os.environ["WINDOW_JSON"]).get("messages_received",-1))')" \
    || fail "roundtrip: could not parse time-range download --json"
  [[ "${window_count}" == "${expect_window_msgs}" ]] \
    || fail "roundtrip: windowed messages_received=${window_count} != expected ${expect_window_msgs}"
  if ! "${mcapdiff}" --time-range "${window_start_ns},${window_end_ns}" \
       "${GROUND_TRUTH_TARGET}" "${cpp_window}"; then
    fail "roundtrip: C++ CLI time-range NOT clean (under/over-delivery or drift; mcapdiff above)"
  fi
  log "step f4: OK (C++ CLI time-range round-trip clean, no over-delivery; ${expect_window_msgs} msgs)"

  # ── f5: C++ SDK CLI STITCHED download (target + stitch-B) -> mcapdiff over
  # BOTH originals merged -> clean + exactly the summed count. ──
  local cpp_stitch="${work}/cpp_stitch.mcap"
  local stitch_json stitch_count
  log "step f5: C++ SDK CLI STITCHED download of ${TARGET_KEY}+${STITCH_B_KEY}"
  stitch_json="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download \
      "${TARGET_KEY}" "${STITCH_B_KEY}" --output "${cpp_stitch}" --json 2>/dev/null)" \
    || fail "roundtrip: C++ CLI stitched download failed"
  [[ -s "${cpp_stitch}" ]] || fail "roundtrip: C++ CLI stitched download produced no output"
  stitch_count="$(STITCH_JSON="${stitch_json}" python3 -c \
    'import json,os; print(json.loads(os.environ["STITCH_JSON"]).get("messages_received",-1))')" \
    || fail "roundtrip: could not parse stitched download --json"
  [[ "${stitch_count}" == "${LOCAL_STITCH_MSGS}" ]] \
    || fail "roundtrip: stitched messages_received=${stitch_count} != local oracle ${LOCAL_STITCH_MSGS}"
  if ! "${mcapdiff}" "${GROUND_TRUTH_TARGET}" "${GROUND_TRUTH_STITCH_B}" "${cpp_stitch}"; then
    fail "roundtrip: C++ CLI stitched download NOT logically equal to merged originals (mcapdiff above)"
  fi
  # Same-sequence-twice is a clean usage error (exit 2), not a crash / silent pass.
  set +e
  "${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download \
      "${TARGET_KEY}" "${TARGET_KEY}" --output "${work}/dup.mcap" >/dev/null 2>&1
  local dup_rc=$?
  set -e
  [[ "${dup_rc}" == "2" ]] || fail "roundtrip: duplicate-sequence exit ${dup_rc} != 2 (usage)"
  log "step f5: OK (C++ CLI stitched round-trip clean; ${LOCAL_STITCH_MSGS} messages; duplicate-seq exits 2)"

  # ── f6: C++ SDK CLI HALF-TOPICS download (2-of-3 topic subset) ──
  local cpp_half="${work}/cpp_half.mcap"
  log "step f6: C++ SDK CLI HALF-TOPICS download (--topics ${HALF_TOPICS})"
  local half_json half_count
  half_json="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download "${TARGET_KEY}" \
      --topics "${HALF_TOPICS}" --output "${cpp_half}" --json 2>/dev/null)" \
    || fail "roundtrip: C++ CLI half-topics download failed"
  [[ -s "${cpp_half}" ]] || fail "roundtrip: C++ CLI half-topics download produced no output"
  half_count="$(HALF_JSON="${half_json}" python3 -c \
    'import json,os; print(json.loads(os.environ["HALF_JSON"]).get("messages_received",-1))')" \
    || fail "roundtrip: could not parse half-topics download --json"
  [[ "${half_count}" == "${LOCAL_TARGET_HALF_MSGS}" ]] \
    || fail "roundtrip: half-topics messages_received=${half_count} != local oracle ${LOCAL_TARGET_HALF_MSGS}"
  if ! "${mcapdiff}" --topics "${HALF_TOPICS}" "${GROUND_TRUTH_TARGET}" "${cpp_half}"; then
    fail "roundtrip: C++ CLI half-topics NOT clean (under/over-delivery or drift; mcapdiff above)"
  fi
  log "step f6: OK (C++ CLI half-topics round-trip clean, no over-delivery; ${LOCAL_TARGET_HALF_MSGS} msgs)"

  # ── f7: C++ SDK CLI NONE-MATCHING topics -> zero-result -> exit 0 + empty
  # VALID mcap (0 messages). ──
  local cpp_none="${work}/cpp_none.mcap"
  log "step f7: C++ SDK CLI NONE-MATCHING download (--topics ${NONE_TOPIC}) -> zero-result"
  local none_json none_count
  none_json="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download "${TARGET_KEY}" \
      --topics "${NONE_TOPIC}" --output "${cpp_none}" --json 2>/dev/null)" \
    || fail "roundtrip: C++ CLI none-matching download did NOT exit 0 (empty plan must be success)"
  none_count="$(NONE_JSON="${none_json}" python3 -c \
    'import json,os; print(json.loads(os.environ["NONE_JSON"]).get("messages_received",-1))')" \
    || fail "roundtrip: could not parse none-matching download --json"
  [[ "${none_count}" == "0" ]] \
    || fail "roundtrip: none-matching messages_received=${none_count} != 0"
  [[ -s "${cpp_none}" ]] || fail "roundtrip: none-matching produced no file (must be empty VALID mcap)"
  local none_mc
  none_mc="$("${mcaptopics}" "${cpp_none}" 2>/dev/null | python3 -c \
    'import json,sys; print(json.load(sys.stdin)["message_count"])')" \
    || fail "roundtrip: none-matching output is not a valid MCAP"
  [[ "${none_mc}" == "0" ]] || fail "roundtrip: none-matching empty file reports ${none_mc} msgs (want 0)"
  log "step f7: OK (C++ CLI none-matching -> exit 0, empty valid mcap, 0 messages)"

  log "step f: OK (both client stacks round-trip clean on (topic, log_time, payload, publish_time, schema))"
  rm -rf "${work}"
  SMOKE_WORKDIR=""
}

# ─────────────────────────────────────────────────────────────────────────────
# Step g: RESTART PERSISTENCE. Restart ONLY the Go server (the builder daemon
# keeps running — it is the catalog's owner, not the server's concern). Since
# the server never writes the catalog, "0 re-extracts" has no meaning here;
# the invariant is that the SERVED DB FILE's IDENTITY (dev,inode) is
# unchanged (a Go-server-only restart must never trigger — or require — a
# catalog rebuild) and the catalog is served identically before and after.
# NOTE: we deliberately do NOT assert build_metadata.build_id here — it
# increments on EVERY ordinary rescan (RESCAN_INTERVAL fires independently of
# this restart), so it is not a swap-only signal; (dev,inode) IS the swap
# trigger per CATALOG_CONTRACT.md §9 and is what we check.
# ─────────────────────────────────────────────────────────────────────────────
step_restart_persistence() {
  log "step g: restart persistence (Go server only; builder + catalog DB untouched)"
  local probe="${SERVER_DIR}/bin/devprobe"
  [[ -x "${probe}" ]] || fail "restart: devprobe binary missing at ${probe}"

  local ident_before ident_after
  ident_before="$(stat -c '%d:%i' "${SMOKE_DB}")" || fail "restart: could not stat ${SMOKE_DB} before restart"

  stop_harness_server
  log "step g: restarting harness server on the SAME catalog DB (fresh log ${SMOKE_LOG2})"
  : > "${SMOKE_LOG2}"
  ( cd "${SERVER_DIR}" && exec env -u PJ_CLOUD_TOKEN ./bin/pj-cloud-server \
      -config "${SMOKE_CONFIG}" -listen ":${SMOKE_PORT}" -db "${SMOKE_DB}" \
      -tag-ipc-socket "${TAG_SOCKET}" -allow-anonymous \
      >>"${SMOKE_LOG2}" 2>&1 ) &
  SMOKE_SERVER_PID=$!
  if ! wait_http "http://localhost:${SMOKE_PORT}/health" 60; then
    log "----- restart server log (tail) -----"; tail -n 40 "${SMOKE_LOG2}" || true
    fail "restart: :${SMOKE_PORT}/health did not come up within 60s after restart"
  fi

  ident_after="$(stat -c '%d:%i' "${SMOKE_DB}")" || fail "restart: could not stat ${SMOKE_DB} after restart"
  [[ "${ident_before}" == "${ident_after}" ]] \
    || fail "restart: catalog DB identity changed (${ident_before} -> ${ident_after}) — a bare server restart must never rebuild/replace the catalog"

  local list_json fc target_msgs
  list_json="$("${probe}" -url "${SMOKE_WS}" 2>/dev/null)" || fail "restart: devprobe list failed after restart"
  eval "$(SMOKE_JSON="${list_json}" python3 - "${TARGET_KEY}" <<'PY'
import json, os, sys
d = json.loads(os.environ["SMOKE_JSON"])
print(f'fc={d.get("file_count", -1)}')
tgt = next((f for f in d.get("files", []) if f.get("s3_key") == sys.argv[1]), None)
print(f'target_msgs={tgt["message_count"] if tgt else -1}')
PY
)"
  [[ "${fc}" == "${ACTUAL_FILE_COUNT}" ]] || fail "restart: file_count after restart=${fc} != ${ACTUAL_FILE_COUNT}"
  [[ "${target_msgs}" == "${LOCAL_TARGET_TOTAL}" ]] \
    || fail "restart: ${TARGET_KEY} message_count after restart=${target_msgs} != local oracle ${LOCAL_TARGET_TOTAL}"

  log "step g: OK (DB identity unchanged across restart: ${ident_after}; catalog served identically: ${fc} files)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step h: TAG FLOW through the IPC forwarder, INCLUDING surviving a full
# catalog REBUILD (the atomic-publish path + tags_override carry-forward,
# CATALOG_CONTRACT.md §9). Sequence: set -> visible -> stop the DAEMON -> a
# synchronous `--once --rebuild` (a NEW DB inode) -> restart the daemon ->
# wait for the Go server's ReopenIfSwapped (its 30s freshness ticker) to pick
# up the new generation -> assert the override SURVIVED (carried forward by
# composite identity, possibly under a RENUMBERED file id — re-resolved by
# key, never assumed stable) -> unset -> assert gone.
# ─────────────────────────────────────────────────────────────────────────────
step_tag_flow() {
  log "step h: tag flow (set -> visible -> survives rebuild -> unset -> gone)"
  local probe="${SERVER_DIR}/bin/devprobe"
  local sdk_cli="${CTEST_DIR}/bin/mcap-cloud-cli"
  [[ -x "${probe}" ]] || fail "tag: devprobe binary missing at ${probe}"

  local target_id
  target_id="$(resolve_file_id "${TARGET_KEY}")" \
    || fail "tag: could not resolve file id for ${TARGET_KEY}"
  log "step h: target ${TARGET_KEY} -> file_id ${target_id}"

  # ── h1: set verified=yes + smoke_marker=h, through the tag-edit IPC. Prefer
  # the C++ CLI tag verb; fall back to the Go twin. ──
  local used_cpp=0
  if [[ -x "${sdk_cli}" ]] && "${sdk_cli}" --help 2>&1 | grep -qiw 'tag'; then
    if "${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" tag "${TARGET_KEY}" \
         --set verified=yes --set smoke_marker=h >/dev/null 2>&1; then
      used_cpp=1
      log "step h1: set via C++ mcap-cloud-cli tag"
    fi
  fi
  if [[ "${used_cpp}" != "1" ]]; then
    "${probe}" -url "${SMOKE_WS}" -file-id "${target_id}" \
        -set-tag verified=yes -set-tag smoke_marker=h >/dev/null \
      || fail "tag: devprobe set-tag failed"
    log "step h1: set via Go devprobe (-set-tag)"
  fi

  # ── h2: assert visible in list --json (flat metadata + is_override view) ──
  local list_json
  list_json="$("${probe}" -url "${SMOKE_WS}" 2>/dev/null)" || fail "tag: devprobe list failed"
  SMOKE_JSON="${list_json}" python3 - "${target_id}" <<'PY' \
    || fail "tag: set tags NOT visible in list --json (see python error above)"
import json, os, sys
tid = int(sys.argv[1])
d = json.loads(os.environ["SMOKE_JSON"])
f = next((x for x in d["files"] if x["id"] == tid), None)
if f is None:
    sys.stderr.write("target file id not in listing\n"); sys.exit(2)
md = f.get("metadata", {})
if md.get("verified") != "yes" or md.get("smoke_marker") != "h":
    sys.stderr.write(f"flat metadata missing override tags: {md}\n"); sys.exit(3)
ov = {t["key"]: t["is_override"] for t in f.get("tags", [])}
if not ov.get("verified") or not ov.get("smoke_marker"):
    sys.stderr.write(f"FileSummary.tags is_override not set: {f.get('tags')}\n"); sys.exit(4)
print("tags visible + is_override=true")
PY
  log "step h2: OK (verified=yes, smoke_marker=h visible; is_override=true; forwarded over the tag-edit IPC)"

  # ── h3: FULL REBUILD via the atomic-publish path. Stop the daemon (releases
  # the tag socket + its DB connection cleanly), run `--once --rebuild`
  # synchronously (a NEW served-DB inode, tags_override carried forward by
  # composite identity), restart the daemon, then wait for the Go server's
  # own 30s freshness ticker to notice the swap (ReopenIfSwapped) and serve
  # the new generation. The override must SURVIVE — under whatever file id
  # the rebuild assigns (ids are NOT stable across a full rebuild; only
  # re-resolved by KEY is safe, per CATALOG_CONTRACT.md §7). ──
  log "step h3: full catalog REBUILD (stop daemon -> --once --rebuild -> restart daemon)"
  local ident_before_rebuild ident_after_rebuild
  ident_before_rebuild="$(stat -c '%d:%i' "${SMOKE_DB}")" || fail "tag: could not stat ${SMOKE_DB} before rebuild"

  stop_builder_daemon
  : > "${REBUILD_LOG}"
  run_builder_once_rebuild "${REBUILD_LOG}" || {
    log "----- rebuild log -----"; cat "${REBUILD_LOG}" || true
    fail "tag: --once --rebuild failed"
  }
  ident_after_rebuild="$(stat -c '%d:%i' "${SMOKE_DB}")" || fail "tag: could not stat ${SMOKE_DB} after rebuild"
  [[ "${ident_before_rebuild}" != "${ident_after_rebuild}" ]] \
    || fail "tag: rebuild did not swap the served DB's identity (still ${ident_after_rebuild}) — the atomic-publish path did not run"
  grep -q 'tags_override carry-forward' "${REBUILD_LOG}" \
    || { cat "${REBUILD_LOG}"; fail "tag: rebuild log missing the tags_override carry-forward line"; }

  local rebuilt_scanned rebuilt_failed
  local row
  row="$(db_query "${SMOKE_DB}" "SELECT files_scanned, files_failed FROM build_metadata WHERE id=1")" \
    || fail "tag: could not read build_metadata after rebuild"
  IFS=$'\t' read -r rebuilt_scanned rebuilt_failed <<<"${row}"
  [[ "${rebuilt_scanned}" == "${ACTUAL_FILE_COUNT}" && "${rebuilt_failed}" == "0" ]] \
    || fail "tag: post-rebuild build_metadata scanned=${rebuilt_scanned} failed=${rebuilt_failed} (want ${ACTUAL_FILE_COUNT}/0)"

  rm -f "${TAG_SOCKET}"
  start_builder_daemon "${BUILDER_LOG2}"
  wait_unix_socket "${TAG_SOCKET}" 20 \
    || { tail -n 40 "${BUILDER_LOG2}"; fail "tag: restarted daemon's tag-edit IPC socket did not reappear within 20s"; }
  log "step h3: rebuild published (inode ${ident_before_rebuild} -> ${ident_after_rebuild}); daemon restarted, tag-IPC back up"

  # Poll the Go server (its freshness updater ticks every 30s) for the reopen
  # to land — up to 40s. Re-resolve the target by KEY every poll (its file id
  # may have been renumbered by the rebuild).
  log "step h3: waiting (up to 40s) for the Go server to detect the DB swap and serve the surviving override"
  local reopened=0 new_target_id="" i
  for i in $(seq 1 20); do
    local poll_json
    if poll_json="$("${probe}" -url "${SMOKE_WS}" 2>/dev/null)"; then
      local result
      result="$(SMOKE_JSON="${poll_json}" python3 - "${TARGET_KEY}" <<'PY'
import json, os, sys
d = json.loads(os.environ["SMOKE_JSON"])
f = next((x for x in d.get("files", []) if x.get("s3_key") == sys.argv[1]), None)
if f is None:
    print("MISS")
elif f.get("metadata", {}).get("verified") == "yes":
    print(f'FOUND {f["id"]}')
else:
    print(f'PENDING {f["id"]}')
PY
)"
      if [[ "${result}" == FOUND* ]]; then
        new_target_id="${result#FOUND }"
        reopened=1
        break
      fi
    fi
    sleep 2
  done
  [[ "${reopened}" == "1" ]] \
    || fail "tag: override did not reappear via the server within 40s of the rebuild (reopen-on-swap not landing?)"
  log "step h3: OK (override SURVIVED the rebuild; served under file_id ${new_target_id}, live within the poll window)"

  # ── h4: unset both keys (against the possibly-renumbered id) -> gone ──
  if [[ "${used_cpp}" == "1" ]] && "${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" tag "${TARGET_KEY}" \
       --unset verified --unset smoke_marker >/dev/null 2>&1; then
    log "step h4: unset via C++ mcap-cloud-cli tag"
  else
    "${probe}" -url "${SMOKE_WS}" -file-id "${new_target_id}" \
        -unset-tag verified -unset-tag smoke_marker >/dev/null \
      || fail "tag: devprobe unset-tag failed"
    log "step h4: unset via Go devprobe (-unset-tag)"
  fi

  list_json="$("${probe}" -url "${SMOKE_WS}" 2>/dev/null)" || fail "tag: devprobe list (post-unset) failed"
  SMOKE_JSON="${list_json}" python3 - "${TARGET_KEY}" <<'PY' \
    || fail "tag: tags NOT gone after unset (see python error above)"
import json, os, sys
d = json.loads(os.environ["SMOKE_JSON"])
f = next((x for x in d["files"] if x.get("s3_key") == sys.argv[1]), None)
md = f.get("metadata", {}) if f else {}
if "verified" in md or "smoke_marker" in md:
    sys.stderr.write(f"tags still present after unset: {md}\n"); sys.exit(2)
print("tags gone after unset")
PY
  log "step h4: OK (both keys gone after unset)"
  log "step h: OK (tag flow set -> visible -> survives rebuild -> unset -> gone, via the tag-edit IPC forwarder)"
}

# ── step i: key-addressed OpenFresh (wire v2) survives a rowid-SHIFTING
# rebuild. Seed ONE extra fixture whose Hive key sorts lexically FIRST, run a
# full `--once --rebuild` (renumbers rowids, CATALOG_CONTRACT.md §7 — the extra
# file takes the low rowid and SHIFTS every other file's id), wait for the Go
# server's reopen, then download the ORIGINAL target BY KEY and mcapdiff it
# against the local original. Under the old id-addressed wire this exact race
# could stream the WRONG file (the new occupant of a stale rowid); the
# key-addressed open must resolve the key in the server's CURRENT generation. ──
step_key_addressed_open() {
  log "step i: key-addressed open survives a rowid-shifting rebuild (wire v2)"
  local probe="${SERVER_DIR}/bin/devprobe"
  local mcapdiff="${SERVER_DIR}/bin/mcapdiff"
  local sdk_cli="${CTEST_DIR}/bin/mcap-cloud-cli"
  local extra_key="customer=aaa/customer_site=lab/robot=r1/source=synthetic/date=2026-06-22/aaa_shift.mcap"

  local id_before
  id_before="$(db_query "${SMOKE_DB}" "SELECT id FROM files WHERE filename='${TARGET_FILE}'")" \
    || fail "keyaddr: could not read the target's pre-rebuild rowid"

  # Seed the lexically-first extra fixture (a copy of the target original —
  # content is irrelevant, only its KEY position in the scan order matters).
  local extra_dir
  extra_dir="$(mktemp -d)" || fail "keyaddr: mktemp -d failed"
  mkdir -p "${extra_dir}/$(dirname "${extra_key}")"
  cp "${GROUND_TRUTH_TARGET}" "${extra_dir}/${extra_key}" || fail "keyaddr: could not stage the extra fixture"
  "${SERVER_DIR}/bin/seed" -dir "${extra_dir}" -bucket "${SMOKE_BUCKET}" \
      -endpoint "${SMOKE_S3_ENDPOINT}" -access-key "${SMOKE_S3_ACCESS_KEY}" -secret-key "${SMOKE_S3_SECRET_KEY}" \
      -region "${SMOKE_S3_REGION}" -force >/dev/null \
    || fail "keyaddr: seeding the extra fixture failed"
  rm -rf "${extra_dir}"

  # Full rebuild (atomic publish, new inode) + daemon restart, mirroring h3.
  local ident_before ident_after
  ident_before="$(stat -c '%d:%i' "${SMOKE_DB}")" || fail "keyaddr: stat before rebuild failed"
  stop_builder_daemon
  : > "${REBUILD_LOG}"
  run_builder_once_rebuild "${REBUILD_LOG}" || {
    log "----- rebuild log -----"; cat "${REBUILD_LOG}" || true
    fail "keyaddr: --once --rebuild failed"
  }
  ident_after="$(stat -c '%d:%i' "${SMOKE_DB}")" || fail "keyaddr: stat after rebuild failed"
  [[ "${ident_before}" != "${ident_after}" ]] \
    || fail "keyaddr: rebuild did not swap the served DB identity"
  rm -f "${TAG_SOCKET}"
  start_builder_daemon "${BUILDER_LOG2}"

  # The rebuild must have SHIFTED the target's rowid — that renumbering is the
  # very hazard key-addressing removes; without it this step proves nothing.
  local id_after
  id_after="$(db_query "${SMOKE_DB}" "SELECT id FROM files WHERE filename='${TARGET_FILE}'")" \
    || fail "keyaddr: could not read the target's post-rebuild rowid"
  [[ "${id_before}" != "${id_after}" ]] \
    || fail "keyaddr: target rowid did NOT shift (${id_before} -> ${id_after}); the lexically-first extra fixture did not renumber the scan"
  log "step i: target rowid shifted ${id_before} -> ${id_after} (extra key ${extra_key})"

  # Wait (<=40s, the 30s freshness tick + slack) for the server to serve the
  # new generation: the extra key appearing in the listing proves the reopen.
  local reopened=0 i
  for i in $(seq 1 20); do
    local poll_json
    if poll_json="$("${probe}" -url "${SMOKE_WS}" 2>/dev/null)" \
       && grep -q "aaa_shift.mcap" <<<"${poll_json}"; then
      reopened=1
      break
    fi
    sleep 2
  done
  [[ "${reopened}" == "1" ]] || fail "keyaddr: server did not serve the new generation within 40s"

  # THE assertion: open the ORIGINAL target BY KEY on the new generation and
  # prove logical equality with the local original — the key resolved to the
  # right object despite its rowid having moved underneath it.
  [[ -n "${SMOKE_WORKDIR}" && -d "${SMOKE_WORKDIR}" ]] || SMOKE_WORKDIR="$(mktemp -d)"
  local out="${SMOKE_WORKDIR}/keyaddr_after_rebuild.mcap"
  "${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download "${TARGET_KEY}" --output "${out}" >/dev/null 2>&1 \
    || fail "keyaddr: C++ CLI download by key failed after the rowid-shifting rebuild"
  "${mcapdiff}" "${GROUND_TRUTH_TARGET}" "${out}" \
    || fail "keyaddr: post-rebuild download NOT logically equal to the original (wrong object for the key?)"
  log "step i: OK (open-by-key streamed the correct object across a rowid-shifting rebuild; mcapdiff clean)"
}

# ─────────────────────────────────────────────────────────────────────────────
main() {
  log "PJ Cloud Connector SDK smoke harness starting (repo: ${REPO_ROOT}) — catalog-migration cutover: Python builder + external-builder Go server"
  step_minio_and_fixtures
  step_builder_daemon
  step_server
  step_go_assertions
  step_cpp_tests
  step_cli_spotcheck
  step_roundtrip
  step_restart_persistence
  step_tag_flow
  step_key_addressed_open
  # Do NOT print the verdict here — the EXIT trap (cleanup) prints it, LAST,
  # after every teardown log line (B1: the final-line contract).
  SMOKE_VERDICT="SMOKE PASS"
}

main "$@"
