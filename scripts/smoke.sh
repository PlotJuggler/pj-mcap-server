#!/usr/bin/env bash
# smoke.sh — the single, self-contained regression gate for the PJ Cloud
# Connector "SDK harness". It proves, without the GUI, that:
#   - Minio is up and the `recordings` bucket holds the expected MCAP corpus,
#   - the Go catalog server builds and serves the catalog over WS+Protobuf,
#   - the Go-side discovery RPCs (Hello/ListFiles/GetFile) return the pinned
#     ground-truth numbers,
#   - the C++ client SDK (BackendConnection) talks to that server live,
#   - both the hermetic and live C++ test suites are green, and
#   - a streaming session ROUND-TRIPS: original MCAP -> server -> client ->
#     reconstructed MCAP is logically equal on (topic, log_time, payload,
#     publish_time, schema) — proven through BOTH client stacks (the C++ SDK
#     `dexory-cloud-cli` and the Go `devprobe`), for a full download and a
#     topic-subset download (subset additionally asserting zero over-delivery).
#
# It is IDEMPOTENT and SELF-CONTAINED: it ensures Minio is running, builds and
# starts its OWN server instance on :8081, runs every assertion, and ALWAYS
# tears its server down on exit (success or failure).
#
# It NEVER touches the user-facing server on :8080 nor its PID file
# (/tmp/pj-cloud-server.pid). The harness server uses :8081 and a separate log
# at /tmp/pj-cloud-smoke-server.log.
#
# Final line is exactly one of:
#   SMOKE PASS
#   SMOKE FAIL: <step>
# and the exit code matches (0 / non-zero).
#
# ─────────────────────────────────────────────────────────────────────────────
# PINNED GROUND TRUTH
# These numbers describe the current Minio corpus (8 ROS2 MCAPs, nissan_zala_*).
# If the bucket is ever reseeded with different data, update these constants
# HERE *and*, in lockstep:
#   - the expectations baked into the C++ live test
#     PJ4/pj-official-plugins/toolbox_dexory_cloud/tests/backend_connection_live_test.cpp
#     (its GroundTruthCatalog test pins: exactly 8 sequences, the 6 known topics
#     of nissan_zala_50_zeg_1_0.mcap, imu==14904), and
#   - the byte-level round-trip ORIGINAL used by step f, which is the SAME object
#     as ${EXPECT_S3_KEY} but read from the local on-disk corpus at
#     ${GROUND_TRUTH_DIR} (the un-uploaded source the Minio bucket was seeded
#     from — the canonical bytes mcapdiff compares the reconstruction against).
#     ${SUBSET_TOPIC} is the single topic step f's subset leg downloads; it must
#     remain one of the 6 known topics of ${EXPECT_S3_KEY}.
# Keep all three in sync — they are facets of the same gate.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── pinned expectations ──────────────────────────────────────────────────────
readonly EXPECT_FILE_COUNT=8
readonly EXPECT_S3_KEY="nissan_zala_50_zeg_1_0.mcap"
readonly EXPECT_TOPIC_COUNT=6
readonly EXPECT_IMU_TOPIC="/nissan/gps/duro/imu"
readonly EXPECT_IMU_MSGS=14904
readonly EXPECT_TOTAL_MSGS=33670
# Round-trip (step f) ground truth: the local on-disk original + the subset topic.
readonly GROUND_TRUTH_DIR="/home/gn/ws/jkk_dataset02"
readonly GROUND_TRUTH_MCAP="${GROUND_TRUTH_DIR}/${EXPECT_S3_KEY}"
readonly SUBSET_TOPIC="/nissan/vehicle_speed"
readonly EXPECT_SUBSET_MSGS=4513
# Time-range leg (f4): the middle ~30% of zeg_1. 10098 in-window messages —
# pinned in lockstep with internal/format's TestIterate_TimeRange_NoChunkEndDrop
# (the regression test for the chunk-MessageEndTime boundary bug).
readonly TIMERANGE_START_NS=1696577469299761084
readonly TIMERANGE_END_NS=1696577514415840735
readonly EXPECT_WINDOW_MSGS=10098
# Stitched multi-file leg (f5, Slice 7): two consecutive, time-disjoint nissan
# files stitched into ONE continuous logical stream via a single OpenFresh. The
# counts are pinned in lockstep with the C++ live tests
# (backend_connection_live_test.cpp + session_download_live_test.cpp).
readonly STITCH_KEY_A="nissan_zala_50_zeg_2_0.mcap"
readonly STITCH_KEY_B="nissan_zala_50_zeg_3_0.mcap"
readonly EXPECT_STITCH_MSGS=65032
readonly GROUND_TRUTH_MCAP_A="${GROUND_TRUTH_DIR}/${STITCH_KEY_A}"
readonly GROUND_TRUTH_MCAP_B="${GROUND_TRUTH_DIR}/${STITCH_KEY_B}"
# Half-topics leg (f6, Slice 10): 3 of zeg_1's 6 topics. imu 14904 + speed 4513 +
# steering 4513 = 23930. Pinned in lockstep with matrix m1 (scripts/matrix.sh).
readonly HALF_TOPICS="/nissan/gps/duro/imu,/nissan/vehicle_speed,/nissan/vehicle_steering"
readonly EXPECT_HALF_MSGS=23930
# None-matching leg (f7, Slice 10): a topic in no file -> empty plan -> a normal
# OpenSessionResponse + immediate Eos{COMPLETE,0} (success, NOT an error). The CLI
# must exit 0 and write an empty BUT VALID MCAP. Pinned with matrix m2.
readonly NONE_TOPIC="/does/not/exist"

# ── paths (all absolute; cwd is reset between agent steps elsewhere) ──────────
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly SERVER_DIR="${REPO_ROOT}/server"
readonly COMPOSE_FILE="${REPO_ROOT}/infra/minio/docker-compose.yml"
readonly PLUGIN_DIR="${REPO_ROOT}/PJ4/pj-official-plugins"
readonly CTEST_DIR="${PLUGIN_DIR}/build/toolbox_dexory_cloud/Release"

# ── harness server config (NEVER :8080 / the user's PID file) ─────────────────
readonly SMOKE_PORT=8081
readonly SMOKE_WS="ws://localhost:${SMOKE_PORT}/api/ws"
readonly SMOKE_LOG="/tmp/pj-cloud-smoke-server.log"
# Step g restarts the harness server on a SECOND log to prove warm-start.
readonly SMOKE_LOG2="/tmp/pj-cloud-smoke-server-2.log"
# Stable SQLite catalog DB for the harness server. cleanup() wipes it on EVERY
# exit so each `make smoke` starts COLD (a stale DB would mask the step-g
# re-extract assertion) — but step g deliberately keeps it across a restart
# WITHIN one run. A short poll interval makes step h's forced-reindex prompt.
readonly SMOKE_DB="/tmp/pj-cloud-smoke-catalog.db"
readonly SMOKE_POLL_INTERVAL="2s"
readonly MINIO_HEALTH="http://localhost:9000/minio/health/live"
readonly MINIO_NETWORK="minio_default"
readonly MC_IMAGE="minio/mc:RELEASE.2024-06-12T14-34-03Z"

# ── go toolchain on PATH (per project context) ───────────────────────────────
export PATH="$HOME/.local/go/bin:$HOME/go/bin:$PATH"

# ── state for cleanup ─────────────────────────────────────────────────────────
SMOKE_SERVER_PID=""
# Per-run scratch dir for step f's round-trip artifacts. Removed by cleanup() on
# EVERY exit path (success, fail, signal) so reconstructed MCAPs never leak.
SMOKE_WORKDIR=""

log()  { printf '[smoke] %s\n' "$*"; }
fail() { printf 'SMOKE FAIL: %s\n' "$*"; exit 1; }

# Always reap the harness server (and only it) and remove our scratch dir on any
# exit path. The trap registration itself is unchanged.
cleanup() {
  local rc=$?
  if [[ -n "${SMOKE_WORKDIR}" && -d "${SMOKE_WORKDIR}" ]]; then
    rm -rf "${SMOKE_WORKDIR}"
  fi
  if [[ -n "${SMOKE_SERVER_PID}" ]] && kill -0 "${SMOKE_SERVER_PID}" 2>/dev/null; then
    log "stopping harness server (pid ${SMOKE_SERVER_PID})"
    kill "${SMOKE_SERVER_PID}" 2>/dev/null || true
    # give it a moment to drain, then hard-kill if still alive
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "${SMOKE_SERVER_PID}" 2>/dev/null || break
      sleep 0.3
    done
    kill -9 "${SMOKE_SERVER_PID}" 2>/dev/null || true
  fi
  # Wipe the stable catalog DB (+ WAL/SHM sidecars) so the NEXT `make smoke`
  # starts cold — otherwise a leftover DB would make step g's cold-extract
  # assertion lie. Also drop the step-g second log.
  rm -f "${SMOKE_DB}" "${SMOKE_DB}-wal" "${SMOKE_DB}-shm" "${SMOKE_LOG2}" 2>/dev/null || true
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

# ─────────────────────────────────────────────────────────────────────────────
# Step a: Minio up + bucket non-empty
# ─────────────────────────────────────────────────────────────────────────────
step_minio() {
  log "step a: ensuring Minio is up"
  [[ -f "${COMPOSE_FILE}" ]] || fail "minio: compose file not found at ${COMPOSE_FILE}"
  docker compose -f "${COMPOSE_FILE}" up -d >/dev/null 2>&1 \
    || fail "minio: docker compose up failed"
  wait_http "${MINIO_HEALTH}" 60 \
    || fail "minio: ${MINIO_HEALTH} did not become healthy within 60s"
  log "step a: Minio healthy; counting .mcap objects in recordings bucket"

  # One-shot `mc ls` via a throwaway container on the compose network. The
  # minio/mc image ships no grep, so we count .mcap lines on the host side.
  local listing mcap_count
  if ! listing="$(docker run --rm --network "${MINIO_NETWORK}" --entrypoint sh "${MC_IMAGE}" \
        -c "mc alias set local http://minio:9000 admin password123 >/dev/null 2>&1 && mc ls local/recordings" 2>/dev/null)"; then
    fail "minio: 'mc ls local/recordings' failed (is the bucket reachable on network ${MINIO_NETWORK}?)"
  fi
  mcap_count="$(printf '%s\n' "${listing}" | grep -c '\.mcap' || true)"
  if (( mcap_count < 1 )); then
    fail "minio: bucket 'recordings' is EMPTY — seed it before running smoke (this script does NOT auto-seed; upload the nissan_zala_*.mcap corpus to s3://recordings)"
  fi
  log "step a: OK (${mcap_count} .mcap object(s) in recordings)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step b: build the Go server + start our OWN instance on :8081
# ─────────────────────────────────────────────────────────────────────────────
step_server() {
  log "step b: building Go server"
  command -v go >/dev/null 2>&1 || fail "server: go not on PATH ($HOME/.local/go/bin expected)"
  ( cd "${SERVER_DIR}" && go build -o ./bin/pj-cloud-server ./cmd/pj-cloud-server ) \
    || fail "server: go build failed"
  # devprobe is the Go-side discovery + reference-downloader client (steps c, f).
  ( cd "${SERVER_DIR}" && go build -o ./bin/devprobe ./cmd/devprobe ) \
    || fail "server: devprobe build failed"
  # mcapdiff is the logical-equality round-trip diff used in step f.
  ( cd "${SERVER_DIR}" && go build -o ./bin/mcapdiff ./cmd/mcapdiff ) \
    || fail "server: mcapdiff build failed"
  # mcaptopics validates the empty-but-valid MCAP produced by f7's zero-result leg.
  ( cd "${SERVER_DIR}" && go build -o ./bin/mcaptopics ./cmd/mcaptopics ) \
    || fail "server: mcaptopics build failed"

  # COLD start: remove any stale catalog DB so this run's startup is the very
  # first (cold) extract of the whole corpus — step g asserts new==8 from this
  # SMOKE_LOG. The EXIT cleanup also wipes it; doing it here too guards against a
  # crashed previous run leaving a DB behind.
  rm -f "${SMOKE_DB}" "${SMOKE_DB}-wal" "${SMOKE_DB}-shm" 2>/dev/null || true

  log "step b: starting harness server on :${SMOKE_PORT} (db ${SMOKE_DB}, poll ${SMOKE_POLL_INTERVAL}, log ${SMOKE_LOG})"
  : > "${SMOKE_LOG}"
  # Run WITHOUT PJ_CLOUD_TOKEN -> dev anonymous mode (the C++ live test connects
  # with an empty api_key). Bind only :8081, on the stable SMOKE_DB, with a short
  # poll interval (harmless to steps a-f; needed for step h's forced reindex).
  #
  # NOTE: `exec` so the server process REPLACES the subshell — otherwise `$!`
  # is the subshell's PID and the trap would kill the subshell while leaving
  # the real server orphaned (and still listening on :8081) after we exit.
  ( cd "${SERVER_DIR}" && exec env -u PJ_CLOUD_TOKEN ./bin/pj-cloud-server \
      -listen ":${SMOKE_PORT}" -db "${SMOKE_DB}" -poll-interval "${SMOKE_POLL_INTERVAL}" \
      >>"${SMOKE_LOG}" 2>&1 ) &
  SMOKE_SERVER_PID=$!

  if ! wait_http "http://localhost:${SMOKE_PORT}/health" 60; then
    log "----- harness server log (tail) -----"
    tail -n 40 "${SMOKE_LOG}" || true
    fail "server: :${SMOKE_PORT}/health did not come up within 60s"
  fi

  # Cold-start sanity: the very first run over a fresh DB must have extracted the
  # whole corpus (new==EXPECT_FILE_COUNT). This anchors step g's warm-start
  # comparison (a stale DB would have made this 0).
  local cold_new
  cold_new="$(grep -oE 'msg="indexer: run complete".*new=[0-9]+' "${SMOKE_LOG}" | grep -oE 'new=[0-9]+' | head -n1 | cut -d= -f2)"
  if [[ "${cold_new}" != "${EXPECT_FILE_COUNT}" ]]; then
    log "----- harness server log (tail) -----"; tail -n 20 "${SMOKE_LOG}" || true
    fail "server: cold startup new=${cold_new:-<none>} != ${EXPECT_FILE_COUNT} (DB was not cold?)"
  fi
  log "step b: OK (harness server pid ${SMOKE_SERVER_PID}, /health up, cold extract new=${cold_new})"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step c: Go-side discovery assertions via devprobe (JSON parsed by python3)
# ─────────────────────────────────────────────────────────────────────────────
step_go_assertions() {
  log "step c: Go discovery assertions (ListFiles -> file_count, GetFile -> topics)"
  local probe="${SERVER_DIR}/bin/devprobe"
  [[ -x "${probe}" ]] || fail "go-assert: devprobe binary missing at ${probe}"

  local list_json
  list_json="$("${probe}" -url "${SMOKE_WS}" 2>/dev/null)" \
    || fail "go-assert: devprobe (ListFiles) failed against ${SMOKE_WS}"

  # Assert file_count and resolve the file_id whose s3_key is the pinned target.
  # The devprobe JSON is passed via SMOKE_JSON (an env var) so the python program
  # itself can be fed on stdin via the heredoc — you cannot pipe data into
  # `python3 -` AND supply the program on stdin at the same time.
  local target_id
  target_id="$(SMOKE_JSON="${list_json}" python3 - "${EXPECT_FILE_COUNT}" "${EXPECT_S3_KEY}" <<'PY'
import json, os, sys
expect_count = int(sys.argv[1])
expect_key   = sys.argv[2]
d = json.loads(os.environ["SMOKE_JSON"])
fc = d.get("file_count", -1)
if fc != expect_count:
    sys.stderr.write(f"file_count mismatch: got {fc} want {expect_count}\n")
    sys.exit(2)
tid = None
for f in d.get("files", []):
    if f.get("s3_key") == expect_key:
        tid = f.get("id")
        break
if tid is None:
    sys.stderr.write(f"s3_key {expect_key!r} not found among listed files\n")
    sys.exit(3)
print(tid)
PY
)" || fail "go-assert: ListFiles assertion failed (see python error above)"
  log "step c: file_count == ${EXPECT_FILE_COUNT} OK; ${EXPECT_S3_KEY} -> file_id ${target_id}"

  # GetFile that id -> assert 6 topics + the imu/total message counts.
  local get_json
  get_json="$("${probe}" -url "${SMOKE_WS}" -get-file-id "${target_id}" 2>/dev/null)" \
    || fail "go-assert: devprobe (GetFile ${target_id}) failed"

  SMOKE_JSON="${get_json}" python3 - \
      "${EXPECT_S3_KEY}" "${EXPECT_TOPIC_COUNT}" "${EXPECT_IMU_TOPIC}" \
      "${EXPECT_IMU_MSGS}" "${EXPECT_TOTAL_MSGS}" <<'PY' \
    || fail "go-assert: GetFile assertion failed (see python error above)"
import json, os, sys
expect_key, expect_tc, imu_topic, imu_msgs, total_msgs = (
    sys.argv[1], int(sys.argv[2]), sys.argv[3], int(sys.argv[4]), int(sys.argv[5])
)
d = json.loads(os.environ["SMOKE_JSON"])
gf = d.get("get_file")
if gf is None:
    sys.stderr.write("get_file missing from devprobe output\n"); sys.exit(2)
if gf.get("s3_key") != expect_key:
    sys.stderr.write(f"GetFile s3_key mismatch: {gf.get('s3_key')!r} != {expect_key!r}\n"); sys.exit(3)
tc = gf.get("topic_count", -1)
if tc != expect_tc:
    sys.stderr.write(f"topic_count mismatch: got {tc} want {expect_tc}\n"); sys.exit(4)
by = {t["name"]: t.get("message_count") for t in gf.get("topics", [])}
if by.get(imu_topic) != imu_msgs:
    sys.stderr.write(f"{imu_topic} msgs mismatch: got {by.get(imu_topic)} want {imu_msgs}\n"); sys.exit(5)
tot = sum(v for v in by.values() if isinstance(v, int))
if tot != total_msgs:
    sys.stderr.write(f"total msgs mismatch: got {tot} want {total_msgs}\n"); sys.exit(6)
print(f"GetFile OK: {tc} topics, {imu_topic}={imu_msgs}, total={tot}")
PY
  log "step c: GetFile assertions OK (${EXPECT_TOPIC_COUNT} topics, imu=${EXPECT_IMU_MSGS}, total=${EXPECT_TOTAL_MSGS})"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step d: C++ SDK tests — hermetic (no env) then live (DEXORY_CLOUD_LIVE_URL)
# ─────────────────────────────────────────────────────────────────────────────
step_cpp_tests() {
  log "step d: ensuring C++ plugin test build is current"
  [[ -f "${CTEST_DIR}/CTestTestfile.cmake" ]] \
    || fail "cpp: test build dir not configured at ${CTEST_DIR} (run: cd ${PLUGIN_DIR} && ./build.sh toolbox_dexory_cloud with -DBUILD_TESTING=ON)"
  cmake --build "${CTEST_DIR}" -j >/dev/null 2>&1 \
    || fail "cpp: incremental cmake --build failed in ${CTEST_DIR}"

  # Hermetic run: no DEXORY_CLOUD_LIVE_URL -> the live test(s) SKIP by design,
  # everything else must pass. NOTE: this build does not set ctest's
  # SKIP_RETURN_CODE, so a GTEST_SKIP surfaces to ctest as "Passed" — the
  # hermetic suite is therefore simply "all green". We verify the SKIP is real
  # at the gtest level below (it must be a SKIP, not an accidental pass).
  log "step d: ctest HERMETIC (live test expected to SKIP at gtest level)"
  ( cd "${CTEST_DIR}" && env -u DEXORY_CLOUD_LIVE_URL ctest --output-on-failure ) \
    || fail "cpp: hermetic ctest failed"

  # Confirm the gating actually fires when the env is absent: run the live test
  # binary directly with no env and require gtest to report SKIPPED (and zero
  # PASSED). This guards against the gate silently becoming a no-op.
  local live_bin="${CTEST_DIR}/toolbox_dexory_cloud/toolbox_dexory_cloud_backend_live_test"
  [[ -x "${live_bin}" ]] || fail "cpp: live test binary missing at ${live_bin}"
  local hermetic_out
  hermetic_out="$(env -u DEXORY_CLOUD_LIVE_URL "${live_bin}" 2>&1)" || true
  printf '%s\n' "${hermetic_out}" | grep -q '\[  SKIPPED \]' \
    || { printf '%s\n' "${hermetic_out}"; fail "cpp: live test did NOT skip when DEXORY_CLOUD_LIVE_URL is unset"; }
  log "step d: hermetic OK (suite green; live test correctly SKIPPED at gtest level)"

  # Live run: point the SDK at our :8081 harness server. The live test must
  # actually RUN (not skip) and pass. ctest reports SKIP as "Passed" (no
  # SKIP_RETURN_CODE), so we assert at the gtest level: the binary must report
  # PASSED tests and emit NO [  SKIPPED ] marker.
  log "step d: ctest LIVE against ws://localhost:${SMOKE_PORT} (live tests must RUN, not skip)"
  ( cd "${CTEST_DIR}" && DEXORY_CLOUD_LIVE_URL="ws://localhost:${SMOKE_PORT}" \
      ctest --output-on-failure -R DexoryCloudBackendLive ) \
    || fail "cpp: live ctest (DexoryCloudBackendLive) failed"

  # Slice 8 (reconnect-resume + cache): the resume live test forces a mid-pull
  # socket drop and asserts the resume loop drives the stream to COMPLETE with the
  # exact zeg_1 count + no dupes, plus the cache HIT (zero transport). It relies on
  # the server's RetainAfterDisconnect window (default 60s) being longer than the
  # reconnect backoff — true for the harness server, which runs the spec defaults.
  # ADDITIVE: this does not weaken the BackendLive gate above.
  log "step d: ctest LIVE reconnect-resume + cache (DexoryCloudSessionResumeLive)"
  ( cd "${CTEST_DIR}" && DEXORY_CLOUD_LIVE_URL="ws://localhost:${SMOKE_PORT}" \
      ctest --output-on-failure -R DexoryCloudSessionResumeLive ) \
    || fail "cpp: live ctest (DexoryCloudSessionResumeLive) failed"

  # Slice 16 (parser delegation): the worker-level delegated-ingest ground-truth
  # test — 33670 pushes / imu 14904 via the FakeIngestHost recorder; cancel
  # releases the context. ADDITIVE: does not weaken the gates above.
  log "step d: ctest LIVE worker parser-ingest ground-truth (DexoryCloudParserIngestLive)"
  ( cd "${CTEST_DIR}" && DEXORY_CLOUD_LIVE_URL="ws://localhost:${SMOKE_PORT}" \
      ctest --output-on-failure -R DexoryCloudParserIngestLive ) \
    || fail "cpp: live ctest (DexoryCloudParserIngestLive) failed"

  # gtest-level guard: run the live binary directly in live mode and require a
  # real PASS with no skips.
  local live_out
  if ! live_out="$(DEXORY_CLOUD_LIVE_URL="ws://localhost:${SMOKE_PORT}" "${live_bin}" 2>&1)"; then
    printf '%s\n' "${live_out}"
    fail "cpp: live test binary failed in live mode"
  fi
  if printf '%s\n' "${live_out}" | grep -q '\[  SKIPPED \]'; then
    printf '%s\n' "${live_out}"
    fail "cpp: live test SKIPPED in live mode (DEXORY_CLOUD_LIVE_URL was not honored)"
  fi
  if ! printf '%s\n' "${live_out}" | grep -qE '\[  PASSED  \] [1-9][0-9]* test'; then
    printf '%s\n' "${live_out}"
    fail "cpp: live test did not report any PASSED tests in live mode"
  fi
  log "step d: OK (hermetic green w/ real skip; live ran and PASSED)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step e: CLI spot check — hello + list + topics for the known sequence,
# exercised through BOTH client stacks:
#   e1. the C++ `dexory-cloud-cli` (the SDK harness driver — the EXACT
#       BackendConnection class the GUI plugin uses), and
#   e2. the Go `devprobe` (the server-side reference client).
# Asserting both keeps the smoke honest about the path the GUI actually takes.
# ─────────────────────────────────────────────────────────────────────────────
step_cli_spotcheck() {
  log "step e1: C++ SDK CLI spot check (hello + list + topics for ${EXPECT_S3_KEY})"
  local sdk_cli="${CTEST_DIR}/toolbox_dexory_cloud/dexory-cloud-cli"
  [[ -x "${sdk_cli}" ]] || fail "cli: dexory-cloud-cli missing at ${sdk_cli} (built by step d's cmake --build)"

  local hello_out
  hello_out="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" hello)" \
    || fail "cli: dexory-cloud-cli hello failed"
  printf '%s' "${hello_out}" | grep -q "server_version" \
    || fail "cli: dexory-cloud-cli hello output missing server_version"

  local list_out
  list_out="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" list)" \
    || fail "cli: dexory-cloud-cli list failed"
  printf '%s' "${list_out}" | grep -q "${EXPECT_S3_KEY}" \
    || fail "cli: dexory-cloud-cli list missing ${EXPECT_S3_KEY}"

  local cpp_topics_out
  cpp_topics_out="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" topics "${EXPECT_S3_KEY}")" \
    || fail "cli: dexory-cloud-cli topics failed"
  local ct
  for ct in "/nissan/gps/duro/current_pose" "/nissan/gps/duro/imu" "/nissan/gps/duro/mag" \
            "/nissan/gps/duro/status_string" "/nissan/vehicle_speed" "/nissan/vehicle_steering"; do
    printf '%s' "${cpp_topics_out}" | grep -q "${ct}" \
      || fail "cli: dexory-cloud-cli topics missing ${ct}"
  done
  log "step e1: OK (C++ SDK CLI: hello + list + all 6 topics present)"

  log "step e2: Go devprobe spot check (hello + list + topics for ${EXPECT_S3_KEY})"
  local probe="${SERVER_DIR}/bin/devprobe"
  [[ -x "${probe}" ]] || fail "cli: devprobe binary missing at ${probe}"

  # First resolve the file id for the known sequence, then GetFile it.
  local list_json target_id
  list_json="$("${probe}" -url "${SMOKE_WS}" 2>/dev/null)" \
    || fail "cli: devprobe list failed"
  target_id="$(SMOKE_JSON="${list_json}" python3 - "${EXPECT_S3_KEY}" <<'PY'
import json, os, sys
d = json.loads(os.environ["SMOKE_JSON"])
for f in d.get("files", []):
    if f.get("s3_key") == sys.argv[1]:
        print(f["id"]); break
else:
    sys.exit(1)
PY
)" || fail "cli: could not resolve file id for ${EXPECT_S3_KEY}"

  local out
  out="$("${probe}" -url "${SMOKE_WS}" -get-file-id "${target_id}" 2>/dev/null)" \
    || fail "cli: devprobe topics for ${EXPECT_S3_KEY} failed"

  # hello: server_version present.
  printf '%s' "${out}" | grep -q '"server_version"' \
    || fail "cli: hello output missing server_version"
  # list: the known sequence key is present.
  printf '%s' "${out}" | grep -q "${EXPECT_S3_KEY}" \
    || fail "cli: list output missing ${EXPECT_S3_KEY}"
  # topics: each expected topic name is present.
  local topics=(
    "/nissan/gps/duro/current_pose"
    "/nissan/gps/duro/imu"
    "/nissan/gps/duro/mag"
    "/nissan/gps/duro/status_string"
    "/nissan/vehicle_speed"
    "/nissan/vehicle_steering"
  )
  local t
  for t in "${topics[@]}"; do
    printf '%s' "${out}" | grep -q "${t}" \
      || fail "cli: topics output missing ${t}"
  done
  log "step e2: OK (devprobe: hello + list + all 6 topics present)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step f: streaming ROUND-TRIP — original MCAP -> server -> client ->
# reconstructed MCAP must be LOGICALLY EQUAL on (topic, log_time, payload,
# publish_time, schema). This is the v1 byte-level correctness gate (design-spec
# §11 / unified-plan §6 L3). It is exercised end-to-end through BOTH client
# stacks so a wire/protocol/decode bug in either one fails the smoke:
#   f1. C++ SDK `dexory-cloud-cli` FULL download   -> mcapdiff clean
#   f2. C++ SDK `dexory-cloud-cli` SUBSET download  -> mcapdiff clean (filtered
#       original) AND zero over-delivery
#   f3. Go `devprobe -download` FULL download       -> mcapdiff clean
#   f4. C++ SDK CLI TIME-RANGE download             -> mcapdiff --time-range clean
#   f5. C++ SDK CLI STITCHED (2-file) download      -> mcapdiff vs merged clean
#   f6. C++ SDK CLI HALF-TOPICS (3 of 6) download   -> mcapdiff --topics clean
#   f7. C++ SDK CLI NONE-MATCHING download          -> zero-result, exit 0, empty
#       VALID mcap (the cheap matrix preview; the full §11 matrix is `make matrix`)
#
# The reconstructions are compared against the local on-disk ORIGINAL at
# ${GROUND_TRUTH_MCAP} (the canonical bytes the Minio object was seeded from).
# All artifacts live under a per-run mktemp dir, removed on this step's exit.
# ─────────────────────────────────────────────────────────────────────────────
step_roundtrip() {
  log "step f: streaming round-trip (mcapdiff logical equality, both client stacks)"

  local mcapdiff="${SERVER_DIR}/bin/mcapdiff"
  local mcaptopics="${SERVER_DIR}/bin/mcaptopics"
  local probe="${SERVER_DIR}/bin/devprobe"
  local sdk_cli="${CTEST_DIR}/toolbox_dexory_cloud/dexory-cloud-cli"
  [[ -x "${mcapdiff}" ]]   || fail "roundtrip: mcapdiff binary missing at ${mcapdiff}"
  [[ -x "${mcaptopics}" ]] || fail "roundtrip: mcaptopics binary missing at ${mcaptopics}"
  [[ -x "${probe}" ]]      || fail "roundtrip: devprobe binary missing at ${probe}"
  [[ -x "${sdk_cli}" ]]    || fail "roundtrip: dexory-cloud-cli missing at ${sdk_cli}"
  [[ -f "${GROUND_TRUTH_MCAP}" ]] \
    || fail "roundtrip: ground-truth original not found at ${GROUND_TRUTH_MCAP}"

  # Per-run scratch. Recorded in the module-level SMOKE_WORKDIR so the EXIT
  # cleanup removes it on EVERY path (success, a `fail` hard-exit, or a signal) —
  # mktemp output never leaks even when an assertion below aborts the script.
  SMOKE_WORKDIR="$(mktemp -d)" || fail "roundtrip: mktemp -d failed"
  local work="${SMOKE_WORKDIR}"

  local cpp_full="${work}/cpp_full.mcap"
  local cpp_subset="${work}/cpp_subset.mcap"
  local go_full="${work}/go_full.mcap"

  # ── f1: C++ SDK CLI full download -> mcapdiff vs original (must be clean) ──
  log "step f1: C++ SDK CLI FULL download of ${EXPECT_S3_KEY}"
  "${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download "${EXPECT_S3_KEY}" \
      --output "${cpp_full}" >/dev/null 2>&1 \
    || fail "roundtrip: C++ CLI full download failed"
  [[ -s "${cpp_full}" ]] || fail "roundtrip: C++ CLI full download produced no output"
  log "step f1: mcapdiff ${EXPECT_S3_KEY} (full, C++ CLI) vs original"
  if ! "${mcapdiff}" "${GROUND_TRUTH_MCAP}" "${cpp_full}"; then
    fail "roundtrip: C++ CLI full download NOT logically equal to original (mcapdiff above)"
  fi
  log "step f1: OK (C++ CLI full round-trip clean)"

  # ── f2: C++ SDK CLI subset download -> mcapdiff --topics (clean + no over-delivery) ──
  log "step f2: C++ SDK CLI SUBSET download (--topics ${SUBSET_TOPIC})"
  "${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download "${EXPECT_S3_KEY}" \
      --topics "${SUBSET_TOPIC}" --output "${cpp_subset}" >/dev/null 2>&1 \
    || fail "roundtrip: C++ CLI subset download failed"
  [[ -s "${cpp_subset}" ]] || fail "roundtrip: C++ CLI subset download produced no output"
  log "step f2: mcapdiff --topics ${SUBSET_TOPIC} (subset, C++ CLI) vs filtered original"
  # mcapdiff filters the ORIGINAL to --topics before comparing AND reports any
  # reconstructed record outside the selection as over-delivery; a non-zero exit
  # means under-delivery, content drift, OR over-delivery.
  if ! "${mcapdiff}" --topics "${SUBSET_TOPIC}" "${GROUND_TRUTH_MCAP}" "${cpp_subset}"; then
    fail "roundtrip: C++ CLI subset NOT clean (under/over-delivery or drift; mcapdiff above)"
  fi
  log "step f2: OK (C++ CLI subset round-trip clean, no over-delivery; expected ${EXPECT_SUBSET_MSGS} msgs)"

  # ── f3: Go devprobe full download -> mcapdiff vs original (must be clean) ──
  log "step f3: Go devprobe FULL download of ${EXPECT_S3_KEY}"
  "${probe}" -url "${SMOKE_WS}" -download "${EXPECT_S3_KEY}" -out "${go_full}" >/dev/null 2>&1 \
    || fail "roundtrip: Go devprobe full download failed"
  [[ -s "${go_full}" ]] || fail "roundtrip: Go devprobe full download produced no output"
  log "step f3: mcapdiff ${EXPECT_S3_KEY} (full, Go devprobe) vs original"
  if ! "${mcapdiff}" "${GROUND_TRUTH_MCAP}" "${go_full}"; then
    fail "roundtrip: Go devprobe full download NOT logically equal to original (mcapdiff above)"
  fi
  log "step f3: OK (Go devprobe full round-trip clean)"

  # ── f4: C++ SDK CLI TIME-RANGE download -> mcapdiff --time-range (clean + no
  # over-delivery). This leg exists because a real spec-6.3 bug (the chunk
  # MessageEndTime treated as an exclusive bound dropped the last in-window
  # message of interior chunks) shipped past f1-f3 — full and topic-subset
  # downloads never slice time. Window = the middle ~30% of zeg_1; pinned in
  # lockstep with the regression test TestIterate_TimeRange_NoChunkEndDrop.
  local cpp_window="${work}/cpp_window.mcap"
  log "step f4: C++ SDK CLI TIME-RANGE download (${TIMERANGE_START_NS},${TIMERANGE_END_NS})"
  "${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download "${EXPECT_S3_KEY}" \
      --time-range "${TIMERANGE_START_NS},${TIMERANGE_END_NS}" --output "${cpp_window}" >/dev/null 2>&1 \
    || fail "roundtrip: C++ CLI time-range download failed"
  [[ -s "${cpp_window}" ]] || fail "roundtrip: C++ CLI time-range download produced no output"
  log "step f4: mcapdiff --time-range (windowed, C++ CLI) vs filtered original"
  if ! "${mcapdiff}" --time-range "${TIMERANGE_START_NS},${TIMERANGE_END_NS}" \
       "${GROUND_TRUTH_MCAP}" "${cpp_window}"; then
    fail "roundtrip: C++ CLI time-range NOT clean (under/over-delivery or drift; mcapdiff above)"
  fi
  log "step f4: OK (C++ CLI time-range round-trip clean, no over-delivery; expected ${EXPECT_WINDOW_MSGS} msgs)"

  # ── f5: C++ SDK CLI STITCHED download (Slice 7) -> mcapdiff over BOTH
  # originals (merged) -> clean + exactly 65032 messages. Two consecutive,
  # time-disjoint nissan files become ONE continuous logical stream via a single
  # OpenFresh(file_ids[]). mcapdiff merges the two originals (concatenate +
  # re-sort by topic,log_time) before comparing — the stitched-reconstruction
  # gate. The duplicate-sequence usage error is also probed below.
  local cpp_stitch="${work}/cpp_stitch.mcap"
  local stitch_json
  log "step f5: C++ SDK CLI STITCHED download of ${STITCH_KEY_A}+${STITCH_KEY_B}"
  stitch_json="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download \
      "${STITCH_KEY_A}" "${STITCH_KEY_B}" --output "${cpp_stitch}" --json 2>/dev/null)" \
    || fail "roundtrip: C++ CLI stitched download failed"
  [[ -s "${cpp_stitch}" ]] || fail "roundtrip: C++ CLI stitched download produced no output"
  # Assert the received count equals the stitched total (65032).
  local stitch_count
  stitch_count="$(STITCH_JSON="${stitch_json}" python3 -c \
    'import json,os; print(json.loads(os.environ["STITCH_JSON"]).get("messages_received",-1))')" \
    || fail "roundtrip: could not parse stitched download --json"
  [[ "${stitch_count}" == "${EXPECT_STITCH_MSGS}" ]] \
    || fail "roundtrip: stitched messages_received=${stitch_count} != ${EXPECT_STITCH_MSGS}"
  log "step f5: mcapdiff ${STITCH_KEY_A}+${STITCH_KEY_B} (stitched, C++ CLI) vs both originals merged"
  if ! "${mcapdiff}" "${GROUND_TRUTH_MCAP_A}" "${GROUND_TRUTH_MCAP_B}" "${cpp_stitch}"; then
    fail "roundtrip: C++ CLI stitched download NOT logically equal to merged originals (mcapdiff above)"
  fi
  # Same-sequence-twice is a clean usage error (exit 2), not a crash / silent pass.
  if "${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download \
      "${STITCH_KEY_A}" "${STITCH_KEY_A}" --output "${work}/dup.mcap" >/dev/null 2>&1; then
    fail "roundtrip: duplicate-sequence stitched download should have failed (exit 2)"
  else
    local dup_rc=$?
    [[ "${dup_rc}" == "2" ]] || fail "roundtrip: duplicate-sequence exit ${dup_rc} != 2 (usage)"
  fi
  log "step f5: OK (C++ CLI stitched round-trip clean; ${EXPECT_STITCH_MSGS} messages; duplicate-seq exits 2)"

  # ── f6: C++ SDK CLI HALF-TOPICS download (Slice 10, cheap matrix preview) ->
  # mcapdiff --topics over the 3-of-6 selection -> clean + no over-delivery. The
  # deeper topic/window/parallel/8-file matrix lives in `make matrix`; this is the
  # one cheap whole-file topic-subset leg that belongs in the fast gate.
  local cpp_half="${work}/cpp_half.mcap"
  log "step f6: C++ SDK CLI HALF-TOPICS download (--topics ${HALF_TOPICS})"
  local half_json half_count
  half_json="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download "${EXPECT_S3_KEY}" \
      --topics "${HALF_TOPICS}" --output "${cpp_half}" --json 2>/dev/null)" \
    || fail "roundtrip: C++ CLI half-topics download failed"
  [[ -s "${cpp_half}" ]] || fail "roundtrip: C++ CLI half-topics download produced no output"
  half_count="$(HALF_JSON="${half_json}" python3 -c \
    'import json,os; print(json.loads(os.environ["HALF_JSON"]).get("messages_received",-1))')" \
    || fail "roundtrip: could not parse half-topics download --json"
  [[ "${half_count}" == "${EXPECT_HALF_MSGS}" ]] \
    || fail "roundtrip: half-topics messages_received=${half_count} != ${EXPECT_HALF_MSGS}"
  if ! "${mcapdiff}" --topics "${HALF_TOPICS}" "${GROUND_TRUTH_MCAP}" "${cpp_half}"; then
    fail "roundtrip: C++ CLI half-topics NOT clean (under/over-delivery or drift; mcapdiff above)"
  fi
  log "step f6: OK (C++ CLI half-topics round-trip clean, no over-delivery; ${EXPECT_HALF_MSGS} msgs)"

  # ── f7: C++ SDK CLI NONE-MATCHING topics -> zero-result session -> exit 0 +
  # empty VALID mcap (0 messages). Proves the empty-plan contract end-to-end
  # through the real client stack (not just the Go unit test).
  local cpp_none="${work}/cpp_none.mcap"
  log "step f7: C++ SDK CLI NONE-MATCHING download (--topics ${NONE_TOPIC}) -> zero-result"
  local none_json none_count
  none_json="$("${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" download "${EXPECT_S3_KEY}" \
      --topics "${NONE_TOPIC}" --output "${cpp_none}" --json 2>/dev/null)" \
    || fail "roundtrip: C++ CLI none-matching download did NOT exit 0 (empty plan must be success)"
  none_count="$(NONE_JSON="${none_json}" python3 -c \
    'import json,os; print(json.loads(os.environ["NONE_JSON"]).get("messages_received",-1))')" \
    || fail "roundtrip: could not parse none-matching download --json"
  [[ "${none_count}" == "0" ]] \
    || fail "roundtrip: none-matching messages_received=${none_count} != 0"
  [[ -s "${cpp_none}" ]] || fail "roundtrip: none-matching produced no file (must be empty VALID mcap)"
  # The empty file must still parse as a valid MCAP with exactly 0 messages.
  local none_mc
  none_mc="$("${mcaptopics}" "${cpp_none}" 2>/dev/null | python3 -c \
    'import json,sys; print(json.load(sys.stdin)["message_count"])')" \
    || fail "roundtrip: none-matching output is not a valid MCAP"
  [[ "${none_mc}" == "0" ]] || fail "roundtrip: none-matching empty file reports ${none_mc} msgs (want 0)"
  log "step f7: OK (C++ CLI none-matching -> exit 0, empty valid mcap, 0 messages)"

  log "step f: OK (both client stacks round-trip byte-equal on (topic, log_time, payload, publish_time, schema))"
  # Remove the scratch dir now (the EXIT cleanup also handles it; clearing the
  # var afterwards keeps cleanup a no-op rather than rm'ing a stale path).
  rm -rf "${work}"
  SMOKE_WORKDIR=""
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
# Step g: RESTART PERSISTENCE. Stop the harness server and restart it on the
# SAME SMOKE_DB. The SQLite catalog must be served immediately (8 files), and the
# warm-start must NOT re-extract anything: unchanged==8, new==0, reindexed==0
# (8 'skip-unchanged' log lines, 0 'extracted'). Proves file ids/tags persist
# across restart and the indexer warm-starts off the DB.
# ─────────────────────────────────────────────────────────────────────────────
step_restart_persistence() {
  log "step g: restart persistence (warm-start off the existing ${SMOKE_DB})"
  local probe="${SERVER_DIR}/bin/devprobe"
  [[ -x "${probe}" ]] || fail "restart: devprobe binary missing at ${probe}"

  stop_harness_server
  log "step g: restarting harness server on the SAME db (fresh log ${SMOKE_LOG2})"
  : > "${SMOKE_LOG2}"
  ( cd "${SERVER_DIR}" && exec env -u PJ_CLOUD_TOKEN ./bin/pj-cloud-server \
      -listen ":${SMOKE_PORT}" -db "${SMOKE_DB}" -poll-interval "${SMOKE_POLL_INTERVAL}" \
      >>"${SMOKE_LOG2}" 2>&1 ) &
  SMOKE_SERVER_PID=$!
  if ! wait_http "http://localhost:${SMOKE_PORT}/health" 60; then
    log "----- restart server log (tail) -----"; tail -n 40 "${SMOKE_LOG2}" || true
    fail "restart: :${SMOKE_PORT}/health did not come up within 60s after restart"
  fi

  # Catalog served immediately from the DB: 8 files, no re-extraction.
  local n_extract n_skip warm_line
  n_extract="$(grep -c 'msg="indexer: extracted"' "${SMOKE_LOG2}" || true)"
  n_skip="$(grep -c 'msg="indexer: skip-unchanged"' "${SMOKE_LOG2}" || true)"
  warm_line="$(grep 'msg="indexer: run complete"' "${SMOKE_LOG2}" | head -n1 || true)"
  if [[ "${n_extract}" != "0" ]]; then
    log "----- restart server log -----"; cat "${SMOKE_LOG2}" || true
    fail "restart: warm-start RE-EXTRACTED ${n_extract} file(s) — persistence broken (expected 0)"
  fi
  if [[ "${n_skip}" != "${EXPECT_FILE_COUNT}" ]]; then
    fail "restart: warm-start skip-unchanged=${n_skip} != ${EXPECT_FILE_COUNT}"
  fi
  printf '%s' "${warm_line}" | grep -q 'new=0' \
    && printf '%s' "${warm_line}" | grep -q 'reindexed=0' \
    && printf '%s' "${warm_line}" | grep -q "unchanged=${EXPECT_FILE_COUNT}" \
    || fail "restart: warm-start run-complete line not 'new=0 reindexed=0 unchanged=${EXPECT_FILE_COUNT}': ${warm_line}"

  # And the catalog is queryable immediately (served from the DB rows).
  local list_json fc
  list_json="$("${probe}" -url "${SMOKE_WS}" 2>/dev/null)" || fail "restart: devprobe list failed after restart"
  fc="$(SMOKE_JSON="${list_json}" python3 -c 'import json,os; print(json.loads(os.environ["SMOKE_JSON"]).get("file_count",-1))')"
  [[ "${fc}" == "${EXPECT_FILE_COUNT}" ]] || fail "restart: file_count after restart=${fc} != ${EXPECT_FILE_COUNT}"

  log "step g: OK (warm-start: extracted=0, skip-unchanged=${n_skip}, catalog served ${fc} files immediately)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step h: TAG FLOW. devprobe (Go-side twin of the C++ `tag` verb) sets two
# override tags on ${EXPECT_S3_KEY}, asserts they appear in `list --json`
# (flat metadata overlay + FileSummary.tags is_override=true), survive a FORCED
# reindex (re-put the object via mc to bump last_modified), then unset them and
# asserts they are gone. When the C++ `dexory-cloud-cli tag` verb is present it
# is exercised too (task 3 adds it); otherwise the Go twin alone gates the flow.
# ─────────────────────────────────────────────────────────────────────────────
step_tag_flow() {
  log "step h: tag flow (set -> visible -> survives forced reindex -> unset -> gone)"
  local probe="${SERVER_DIR}/bin/devprobe"
  local sdk_cli="${CTEST_DIR}/toolbox_dexory_cloud/dexory-cloud-cli"
  [[ -x "${probe}" ]] || fail "tag: devprobe binary missing at ${probe}"

  local target_id
  target_id="$(resolve_file_id "${EXPECT_S3_KEY}")" \
    || fail "tag: could not resolve file id for ${EXPECT_S3_KEY}"
  log "step h: target ${EXPECT_S3_KEY} -> file_id ${target_id}"

  # ── h1: set verified=yes + smoke_marker=h. Prefer the C++ CLI tag verb when it
  # exists (task 3); fall back to the Go twin. Either way assert via the wire. ──
  local used_cpp=0
  if [[ -x "${sdk_cli}" ]] && "${sdk_cli}" --help 2>&1 | grep -qiw 'tag'; then
    if "${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" tag "${EXPECT_S3_KEY}" \
         --set verified=yes --set smoke_marker=h >/dev/null 2>&1; then
      used_cpp=1
      log "step h1: set via C++ dexory-cloud-cli tag"
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
  log "step h2: OK (verified=yes, smoke_marker=h visible; is_override=true)"

  # ── h3: FORCED reindex — re-upload the identical bytes to bump last_modified.
  # The change-detect triple (etag,size,last_modified) fires on the bumped time.
  # Override must SURVIVE (tags_override is preserved across reindex). ──
  log "step h3: forced reindex via mc re-put of ${EXPECT_S3_KEY}"
  docker run --rm --network "${MINIO_NETWORK}" \
      -v "${GROUND_TRUTH_MCAP}:/data/${EXPECT_S3_KEY}:ro" \
      --entrypoint sh "${MC_IMAGE}" \
      -c "mc alias set local http://minio:9000 admin password123 >/dev/null 2>&1 && mc cp /data/${EXPECT_S3_KEY} local/recordings/${EXPECT_S3_KEY}" \
      >/dev/null 2>&1 \
    || fail "tag: mc re-put of ${EXPECT_S3_KEY} failed"

  # Wait for the poll to notice (a 'reindexed' line for this key in SMOKE_LOG2).
  local reindexed=0 i
  for i in $(seq 1 20); do
    if grep -q "msg=\"indexer: extracted\".*key=${EXPECT_S3_KEY}.*kind=reindexed" "${SMOKE_LOG2}"; then
      reindexed=1; break
    fi
    sleep 1
  done
  [[ "${reindexed}" == "1" ]] || fail "tag: forced reindex did not fire for ${EXPECT_S3_KEY} within 20s"
  log "step h3: reindex fired; asserting override survived"

  list_json="$("${probe}" -url "${SMOKE_WS}" 2>/dev/null)" || fail "tag: devprobe list (post-reindex) failed"
  SMOKE_JSON="${list_json}" python3 - "${target_id}" <<'PY' \
    || fail "tag: override did NOT survive forced reindex (see python error above)"
import json, os, sys
tid = int(sys.argv[1])
d = json.loads(os.environ["SMOKE_JSON"])
f = next((x for x in d["files"] if x["id"] == tid), None)
if f is None:
    sys.stderr.write("target file id missing after reindex (id changed?)\n"); sys.exit(2)
md = f.get("metadata", {})
if md.get("verified") != "yes":
    sys.stderr.write(f"override 'verified' lost across reindex: {md}\n"); sys.exit(3)
print("override survived reindex")
PY
  log "step h3: OK (override survived forced reindex, file id stable)"

  # ── h4: unset both keys -> gone from the flat map (embedded had none) ──
  if [[ "${used_cpp}" == "1" ]] && "${sdk_cli}" --url "ws://localhost:${SMOKE_PORT}" tag "${EXPECT_S3_KEY}" \
       --unset verified --unset smoke_marker >/dev/null 2>&1; then
    log "step h4: unset via C++ dexory-cloud-cli tag"
  else
    "${probe}" -url "${SMOKE_WS}" -file-id "${target_id}" \
        -unset-tag verified -unset-tag smoke_marker >/dev/null \
      || fail "tag: devprobe unset-tag failed"
    log "step h4: unset via Go devprobe (-unset-tag)"
  fi

  list_json="$("${probe}" -url "${SMOKE_WS}" 2>/dev/null)" || fail "tag: devprobe list (post-unset) failed"
  SMOKE_JSON="${list_json}" python3 - "${target_id}" <<'PY' \
    || fail "tag: tags NOT gone after unset (see python error above)"
import json, os, sys
tid = int(sys.argv[1])
d = json.loads(os.environ["SMOKE_JSON"])
f = next((x for x in d["files"] if x["id"] == tid), None)
md = f.get("metadata", {}) if f else {}
if "verified" in md or "smoke_marker" in md:
    sys.stderr.write(f"tags still present after unset: {md}\n"); sys.exit(2)
print("tags gone after unset")
PY
  log "step h4: OK (both keys gone after unset)"
  log "step h: OK (tag flow set -> visible -> survives reindex -> unset -> gone)"
}

# ─────────────────────────────────────────────────────────────────────────────
main() {
  log "PJ Cloud Connector SDK smoke harness starting (repo: ${REPO_ROOT})"
  step_minio
  step_server
  step_go_assertions
  step_cpp_tests
  step_cli_spotcheck
  step_roundtrip
  step_restart_persistence
  step_tag_flow
  echo "SMOKE PASS"
}

main "$@"
