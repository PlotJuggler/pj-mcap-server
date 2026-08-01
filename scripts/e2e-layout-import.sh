#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# e2e-layout-import.sh — the stage-5 cross-repo live E2E gate for the canonical
# layout import (docs/plans/2026-08-01-layout-import-stage5-e2e.md, §1 E3/E4).
#
# SEPARATE from scripts/smoke.sh and deliberately smaller: it proves the SHIPPED
# layout-import stack (real plotjuggler4, real plugin DSOs over the real ABI,
# real network, real MCAP artifact) against its OWN server on :8082, sharing
# ONLY the Minio daemon with the rest of the harnesses:
#   - own bucket `e2e-layout` (idempotent reseed via `seed -check`, run.sh's
#     pattern — the seeded objects are LEFT IN PLACE between runs),
#   - own catalog DB + config + logs + staged-extensions dir in a per-run
#     `mktemp -d` scratch (removed on EVERY exit path),
#   - own Go server on :8082 (shell-owned child PID, NO fixed pidfiles,
#     process-group teardown via setsid),
#   - Minio is started if down but NEVER `docker compose down`ed here.
#
# A COMMON harness flock (/tmp/pj-cloud-harness.lock, shared with smoke.sh)
# serializes this script against `make smoke` — both rebuild server/bin, both
# poke Minio, and neither tolerates a loaded machine well. We WAIT (with a
# message), never fail, when the other harness holds it.
#
# Scenario identities are FROZEN VECTORS (§1 E4a): the e2e-8082-* cases in
# docs/source-descriptor-vectors.json. This script consumes their descriptor
# bytes + identities VERBATIM (no re-serialization) and asserts, live, that
# every s3_key they name is served by the :8082 catalog built from the
# deterministic gen-ci-fixtures corpus — the vectors and the seeded fixtures
# can never silently drift apart.
#
# DSO staging (§1 E1/E7): the cloud plugin .so from THIS repo's build, plus
# mcap-loader (data_load_mcap) and ros-parser (parser_ros) from the sibling
# pj-official-plugins checkout REBUILT against the SDK version this repo pins
# (plugin/SDK_VERSION; the checkout's own SDK_VERSION is pinned older) — the
# rebuild temp-edits that repo's SDK_VERSION and ALWAYS restores it, committing
# nothing there. Every staged .so gets a PROVENANCE record (source repo, git
# rev, SDK version, sha256) — functional, not bureaucratic: PJ4 #491 actively
# rejects incompatible plugins, so an unrecorded stale DSO is a debugging trap.
#
# The gtest leg and the shipped-binary legs are Task 3/4 STUBS for now, logged
# as SKIPPED-pending; everything up to and including bring-up, vector-fixture
# binding, DSO staging + provenance, and the --validate-plugins pre-flight runs
# for real today.
#
# Final line is exactly one of:
#   E2E-LAYOUT-IMPORT PASS
#   E2E-LAYOUT-IMPORT FAIL: <step>
# (exit code matches), except preflight failures, which exit 2 with a one-line
# E2E-LAYOUT-IMPORT PREFLIGHT FAIL reason (the make-matrix fail-fast pattern).
#
# Usage:
#   scripts/e2e-layout-import.sh            # the real run
#   scripts/e2e-layout-import.sh --dry-run  # print the plan, touch nothing
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── paths (all absolute) ─────────────────────────────────────────────────────
readonly REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
readonly SERVER_DIR="${REPO_ROOT}/server"
readonly MCAP_CATALOG_DIR="${REPO_ROOT}/mcap_catalog"
readonly COMPOSE_FILE="${REPO_ROOT}/infra/minio/docker-compose.yml"
readonly VECTORS_JSON="${REPO_ROOT}/docs/source-descriptor-vectors.json"
readonly VENV_PY="${HOME}/.venvs/pj-catalog/bin/python3"

# The connector plugin built in THIS repo.
readonly PLUGIN_BUILD_DIR="${REPO_ROOT}/plugin/toolbox_mcap_cloud/build"
readonly PLUGIN_SO="${PLUGIN_BUILD_DIR}/bin/libtoolbox_mcap_cloud_plugin.so"
readonly PLUGIN_MANIFEST="${PLUGIN_BUILD_DIR}/bin/toolbox_mcap_cloud_plugin.pjmanifest.json"

# The SDK version every staged DSO must be built against — read live from this
# repo's pin (never hardcoded; CLAUDE.md convention).
readonly REQUIRED_SDK_VERSION="$(tr -d '[:space:]' < "${REPO_ROOT}/plugin/SDK_VERSION")"

# Sibling checkouts (overridable for other machines).
readonly PJ4_APP="${E2E_PJ4_APP:-${HOME}/ws_plotjuggler/PJ4/build/pj_app/plotjuggler4}"
readonly OFFICIAL_PLUGINS_ROOT="${E2E_OFFICIAL_PLUGINS:-${HOME}/ws_plotjuggler/pj-official-plugins}"

# ── harness identity (:8082, own bucket; shares ONLY the Minio daemon) ───────
readonly E2E_PORT=8082
readonly E2E_WS="ws://localhost:${E2E_PORT}/api/ws"
readonly E2E_BUCKET="e2e-layout"
readonly MINIO_HEALTH="http://localhost:9000/minio/health/live"
readonly E2E_S3_ACCESS_KEY="admin"
readonly E2E_S3_SECRET_KEY="password123"
readonly E2E_S3_ENDPOINT="http://localhost:9000"
readonly E2E_S3_REGION="us-east-1"

# Common harness lock shared with scripts/smoke.sh (stage-5 §1 E3).
readonly HARNESS_LOCK="/tmp/pj-cloud-harness.lock"

# The three frozen stage-5 scenario identities (§1 E4a) — case NAMES only; the
# bytes live in ${VECTORS_JSON} and are consumed verbatim from there.
readonly VECTOR_CASE_MAIN="e2e-8082-main-cold-warm"
readonly VECTOR_CASE_EAGER="e2e-8082-eager-leg"
readonly VECTOR_CASE_TRUST="e2e-8082-trust-leg"

# Until the PJ4 PR carrying --dump-diagnostics / --exit-after-layout is merged
# AND the Task 3/4 legs below are implemented, their absence downgrades to a
# loud SKIPPED-pending (the legs that need them are stubs anyway). Flip the
# default to 1 (or export E2E_REQUIRE_PJ4_FLAGS=1) when Task 4 lands the
# shipped-binary legs — from then on a PJ4 build without the flags must
# fail-fast, not skip.
readonly REQUIRE_PJ4_FLAGS="${E2E_REQUIRE_PJ4_FLAGS:-0}"

# ── go toolchain on PATH (per project context) ───────────────────────────────
export PATH="$HOME/.local/go/bin:$HOME/go/bin:$PATH"

# ── state for cleanup (per-run; NO fixed pidfiles) ───────────────────────────
E2E_SCRATCH=""
E2E_SERVER_PID=""
# Backup path while the official-plugins SDK_VERSION temp-edit is in flight —
# restored by cleanup() even if the rebuild crashes mid-way.
OFFICIAL_SDK_BAK=""
PJ4_FLAGS_PRESENT=""

log()  { printf '[e2e-layout] %s\n' "$*"; }
E2E_VERDICT=""
fail() { E2E_VERDICT="E2E-LAYOUT-IMPORT FAIL: $*"; exit 1; }

# restore_official_sdk_version — undo the SDK_VERSION temp-edit (idempotent).
restore_official_sdk_version() {
  if [[ -n "${OFFICIAL_SDK_BAK}" && -f "${OFFICIAL_SDK_BAK}" ]]; then
    mv -f "${OFFICIAL_SDK_BAK}" "${OFFICIAL_PLUGINS_ROOT}/SDK_VERSION"
    OFFICIAL_SDK_BAK=""
  fi
}

# cleanup — reap the harness server's PROCESS GROUP, restore the
# official-plugins SDK_VERSION if a temp-edit was in flight, remove the whole
# per-run scratch (db/WAL/SHM, config, logs, staged extensions, sandboxes) and
# print the verdict LAST (smoke's B1 final-line contract). Minio and the
# seeded e2e-layout bucket are deliberately LEFT ALONE.
cleanup() {
  local rc=$?
  restore_official_sdk_version
  if [[ -n "${E2E_SERVER_PID}" ]] && kill -0 "${E2E_SERVER_PID}" 2>/dev/null; then
    log "stopping harness server (pid ${E2E_SERVER_PID}, process group)"
    kill -TERM -- "-${E2E_SERVER_PID}" 2>/dev/null || kill -TERM "${E2E_SERVER_PID}" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
      kill -0 "${E2E_SERVER_PID}" 2>/dev/null || break
      sleep 0.3
    done
    kill -9 -- "-${E2E_SERVER_PID}" 2>/dev/null || kill -9 "${E2E_SERVER_PID}" 2>/dev/null || true
  fi
  if [[ -n "${E2E_SCRATCH}" && -d "${E2E_SCRATCH}" ]]; then
    rm -rf "${E2E_SCRATCH}"
  fi
  if [[ -z "${E2E_VERDICT}" ]]; then
    if (( rc == 0 )); then
      E2E_VERDICT="E2E-LAYOUT-IMPORT PASS"
    else
      E2E_VERDICT="E2E-LAYOUT-IMPORT FAIL: unexpected error (exit code ${rc})"
    fi
  fi
  printf '%s\n' "${E2E_VERDICT}"
  return "${rc}"
}

# preflight_fail — the make-matrix fail-fast pattern: ONE line, exit 2, before
# any resource exists (the cleanup trap is installed only after preflight).
preflight_fail() {
  printf 'E2E-LAYOUT-IMPORT PREFLIGHT FAIL: %s\n' "$*" >&2
  exit 2
}

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

# db_query DB SQL — one read-only query via Python stdlib sqlite3 (no sqlite3
# CLI dependency); first row tab-separated, non-zero/empty if absent.
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

port_in_use() {
  ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE "[:.]${E2E_PORT}\$"
}

# ─────────────────────────────────────────────────────────────────────────────
# --dry-run: print the plan, touch NOTHING (no lock, no docker, no scratch).
# ─────────────────────────────────────────────────────────────────────────────
dry_run() {
  cat <<EOF
[e2e-layout] DRY RUN — plan only, nothing touched.

  lock          ${HARNESS_LOCK} (shared with scripts/smoke.sh; wait, never fail)
  port          :${E2E_PORT} (preflight: must be free)
  bucket        s3://${E2E_BUCKET} @ ${E2E_S3_ENDPOINT} (idempotent seed via seed -check;
                objects left in place; Minio started if down, NEVER composed down)
  scratch       mktemp -d /tmp/pj-e2e-layout.XXXXXX (catalog.db + config + logs +
                extensions/ + sandbox/; removed on every exit path)
  corpus        gen-ci-fixtures -hive -hive-big + gen-3d-fixture (deterministic)
  builder       ${VENV_PY} -m mcap_catalog_builder --once (sole writer, own db)
  server        server/bin/pj-cloud-server -listen :${E2E_PORT} -allow-anonymous (setsid group)
  vectors       ${VECTORS_JSON}
                cases: ${VECTOR_CASE_MAIN}, ${VECTOR_CASE_EAGER}, ${VECTOR_CASE_TRUST}
                (bytes + identities consumed VERBATIM; every s3_key asserted present
                in the live :${E2E_PORT} catalog; identities self-checked = sha256/128
                of the frozen canonical bytes)
  DSO staging   \${scratch}/extensions/:
                - $(basename "${PLUGIN_SO}") (this repo, SDK ${REQUIRED_SDK_VERSION})
                - libmcap_source_plugin.so (data_load_mcap @ ${OFFICIAL_PLUGINS_ROOT},
                  rebuilt against SDK ${REQUIRED_SDK_VERSION} via SDK_VERSION temp-edit+restore)
                - libparser_ros_plugin.so (parser_ros, same rebuild rule)
                each with provenance: source repo, git rev, SDK version, sha256
  validate      ${PJ4_APP}
                --validate-plugins \${scratch}/extensions + --expect-plugin for all three
  PJ4 flags     --dump-diagnostics / --exit-after-layout probed via --help
                (REQUIRE_PJ4_FLAGS=${REQUIRE_PJ4_FLAGS}; absent => $( [[ "${REQUIRE_PJ4_FLAGS}" == "1" ]] && echo "preflight FAIL" || echo "SKIPPED-pending warning" ))
  stubs         gtest leg (Task 3) and shipped-binary legs (Task 4): SKIPPED-pending
  verdict       E2E-LAYOUT-IMPORT PASS / E2E-LAYOUT-IMPORT FAIL: <step>
EOF
  exit 0
}

# ─────────────────────────────────────────────────────────────────────────────
# Preflight (exit 2, one-line reasons) — runs BEFORE the cleanup trap.
# ─────────────────────────────────────────────────────────────────────────────
preflight() {
  command -v go >/dev/null 2>&1 \
    || preflight_fail "go not on PATH (\$HOME/.local/go/bin expected)"
  command -v docker >/dev/null 2>&1 \
    || preflight_fail "docker not found"
  command -v flock >/dev/null 2>&1 \
    || preflight_fail "flock not found (util-linux)"
  [[ -x "${VENV_PY}" ]] \
    || preflight_fail "builder venv missing at ${VENV_PY} (bootstrap: python3 -m venv ~/.venvs/pj-catalog && ~/.venvs/pj-catalog/bin/pip install boto3==1.43.40 google-cloud-storage==3.12.0 mcap==1.4.0 watchdog==6.0.0)"
  [[ -f "${VECTORS_JSON}" ]] \
    || preflight_fail "vectors file missing at ${VECTORS_JSON}"
  [[ -f "${COMPOSE_FILE}" ]] \
    || preflight_fail "Minio compose file missing at ${COMPOSE_FILE}"

  # The connector plugin must be BUILT (this repo's ./build.sh does it) and
  # built against the pinned SDK — its staged provenance depends on both.
  [[ -f "${PLUGIN_SO}" ]] \
    || preflight_fail "cloud plugin not built at ${PLUGIN_SO} (run ./build.sh toolbox_mcap_cloud)"
  [[ -f "${PLUGIN_MANIFEST}" ]] \
    || preflight_fail "cloud plugin manifest sidecar missing at ${PLUGIN_MANIFEST}"
  local plugin_sdk
  plugin_sdk="$(sed -n 's/.*PACKAGE_VERSION "\([^"]*\)".*/\1/p' \
      "${PLUGIN_BUILD_DIR}/plotjuggler_sdk-config-version.cmake" 2>/dev/null | head -n1)"
  [[ "${plugin_sdk}" == "${REQUIRED_SDK_VERSION}" ]] \
    || preflight_fail "cloud plugin build is against SDK '${plugin_sdk:-unknown}', need ${REQUIRED_SDK_VERSION} — rebuild plugin/toolbox_mcap_cloud"

  # Official-plugins checkout (loader + parser DSO source).
  [[ -x "${OFFICIAL_PLUGINS_ROOT}/build.sh" && -f "${OFFICIAL_PLUGINS_ROOT}/SDK_VERSION" ]] \
    || preflight_fail "pj-official-plugins checkout missing/incomplete at ${OFFICIAL_PLUGINS_ROOT}"

  # PJ4 shipped binary + flag capabilities.
  [[ -x "${PJ4_APP}" ]] \
    || preflight_fail "plotjuggler4 binary missing at ${PJ4_APP} (build ~/ws_plotjuggler/PJ4, or set E2E_PJ4_APP)"
  local help_out
  help_out="$(timeout 15s "${PJ4_APP}" --help 2>&1 || true)"
  grep -q -- '--validate-plugins' <<<"${help_out}" \
    || preflight_fail "plotjuggler4 lacks --validate-plugins (unexpectedly old build?)"
  if grep -q -- '--dump-diagnostics' <<<"${help_out}" \
     && grep -q -- '--exit-after-layout' <<<"${help_out}"; then
    PJ4_FLAGS_PRESENT=1
  else
    PJ4_FLAGS_PRESENT=0
    if [[ "${REQUIRE_PJ4_FLAGS}" == "1" ]]; then
      preflight_fail "PJ4 PR not merged/built yet: plotjuggler4 --help lacks --dump-diagnostics/--exit-after-layout (the stage-5 PJ4 PR half A)"
    fi
    log "WARNING: PJ4 PR not merged/built yet — plotjuggler4 --help lacks --dump-diagnostics/--exit-after-layout; the shipped-binary legs stay SKIPPED-pending"
  fi

  # :8082 must be free (matrix.sh's historical port; it fail-fasts today, but a
  # foreign listener here would make every later wait a confusing lie).
  ! port_in_use \
    || preflight_fail "port :${E2E_PORT} is already in use (foreign listener — refusing to start)"

  # Minio: start if down (shared daemon; NEVER torn down by this script).
  docker compose -f "${COMPOSE_FILE}" up -d >/dev/null 2>&1 \
    || preflight_fail "docker compose up for Minio failed (${COMPOSE_FILE})"
  wait_http "${MINIO_HEALTH}" 60 \
    || preflight_fail "Minio did not become healthy at ${MINIO_HEALTH} within 60s"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step a: build the Go tools (shared server/bin — this is what the common
# harness flock serializes against smoke).
# ─────────────────────────────────────────────────────────────────────────────
step_build_tools() {
  log "step a: building Go tools (server + seed + devprobe + fixture generators)"
  local tool
  for tool in pj-cloud-server seed devprobe gen-ci-fixtures gen-3d-fixture; do
    ( cd "${SERVER_DIR}" && go build -o "./bin/${tool}" "./cmd/${tool}" ) \
      || fail "tools: go build ${tool} failed"
  done
  log "step a: OK"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step b: idempotent seed of s3://e2e-layout (run.sh's seed -check pattern —
# seed only when the bucket is empty; the corpus is deterministic, so a re-seed
# would be byte-identical anyway). The seeded objects are LEFT IN PLACE.
# ─────────────────────────────────────────────────────────────────────────────
step_seed() {
  log "step b: checking s3://${E2E_BUCKET} (idempotent seed)"
  local seed_bin="${SERVER_DIR}/bin/seed" rc=0
  local -a seed_args=(-bucket "${E2E_BUCKET}" -endpoint "${E2E_S3_ENDPOINT}"
                      -access-key "${E2E_S3_ACCESS_KEY}" -secret-key "${E2E_S3_SECRET_KEY}"
                      -region "${E2E_S3_REGION}")
  "${seed_bin}" -check "${seed_args[@]}" >/dev/null 2>&1 || rc=$?
  case "${rc}" in
    0)
      log "step b: bucket empty — generating the deterministic Hive corpus (gen-ci-fixtures -hive -hive-big + 3D fixture)"
      local fixtures="${E2E_SCRATCH}/fixtures"
      mkdir -p "${fixtures}"
      "${SERVER_DIR}/bin/gen-ci-fixtures" -hive -hive-big -out "${fixtures}" >/dev/null \
        || fail "seed: gen-ci-fixtures failed"
      # gen-3d-fixture has no -hive mode: place its single output under a Hive
      # partition ourselves (smoke's convention — the r2/2026-06-23 partition).
      local threed_tmp="${E2E_SCRATCH}/fixtures-3d-tmp"
      mkdir -p "${threed_tmp}"
      "${SERVER_DIR}/bin/gen-3d-fixture" -out "${threed_tmp}" >/dev/null \
        || fail "seed: gen-3d-fixture failed"
      local threed_dir="${fixtures}/customer=test/customer_site=lab/robot=r2/source=synthetic/date=2026-06-23"
      mkdir -p "${threed_dir}"
      local threed_file
      threed_file="$(find "${threed_tmp}" -maxdepth 1 -name '*.mcap' | head -n1)"
      [[ -n "${threed_file}" ]] || fail "seed: gen-3d-fixture produced no *.mcap"
      mv "${threed_file}" "${threed_dir}/"
      rm -rf "${threed_tmp}"
      "${seed_bin}" -dir "${fixtures}" "${seed_args[@]}" \
        || fail "seed: upload to s3://${E2E_BUCKET} failed"
      log "step b: OK (seeded $(find "${fixtures}" -name '*.mcap' | wc -l | tr -d ' ') fixtures)"
      ;;
    3)
      log "step b: OK (bucket already seeded — leaving it as-is)"
      ;;
    *)
      fail "seed: seed -check failed (exit ${rc}) — is Minio healthy?"
      ;;
  esac
}

# ─────────────────────────────────────────────────────────────────────────────
# Step c: one-shot catalog build into the per-run scratch DB (sole writer; the
# builder's own flock is per-db, so smoke's daemon on ITS db never conflicts).
# ─────────────────────────────────────────────────────────────────────────────
step_builder_once() {
  log "step c: building the catalog (--once) into ${E2E_SCRATCH}/catalog.db"
  local db="${E2E_SCRATCH}/catalog.db" blog="${E2E_SCRATCH}/builder.log" rc=0
  ( cd "${MCAP_CATALOG_DIR}" && env \
      AWS_ACCESS_KEY_ID="${E2E_S3_ACCESS_KEY}" \
      AWS_SECRET_ACCESS_KEY="${E2E_S3_SECRET_KEY}" \
      AWS_ENDPOINT_URL="${E2E_S3_ENDPOINT}" \
      AWS_REGION="${E2E_S3_REGION}" AWS_DEFAULT_REGION="${E2E_S3_REGION}" \
      timeout 300s "${VENV_PY}" -m mcap_catalog_builder --source s3 \
      --s3-bucket "${E2E_BUCKET}" --once --db "${db}" --log-level INFO ) \
      >>"${blog}" 2>&1 || rc=$?
  if (( rc != 0 )); then
    log "----- builder log (tail) -----"; tail -n 40 "${blog}" || true
    fail "builder: --once exited ${rc}"
  fi
  local row scanned failed outcome
  row="$(db_query "${db}" "SELECT files_scanned, files_failed, build_outcome FROM build_metadata WHERE id=1")" \
    || { tail -n 40 "${blog}" || true; fail "builder: no build_metadata row after --once"; }
  IFS=$'\t' read -r scanned failed outcome <<<"${row}"
  [[ "${outcome}" == "ok" && "${failed}" == "0" ]] \
    || { tail -n 40 "${blog}" || true; fail "builder: outcome=${outcome} failed=${failed} (want ok/0)"; }
  (( scanned > 0 )) || fail "builder: files_scanned=0 (empty catalog?)"
  log "step c: OK (${scanned} files cataloged, 0 failures)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step d: start the harness server on :8082 (read-only catalog; anonymous —
# the harness is a local gate). setsid => the server leads its own process
# group, so teardown can reap the whole group without touching this script's.
# ─────────────────────────────────────────────────────────────────────────────
step_server() {
  log "step d: starting harness server on :${E2E_PORT}"
  local config="${E2E_SCRATCH}/config.yaml" slog="${E2E_SCRATCH}/server.log"
  cat > "${config}" <<EOF
storage:
  s3:
    bucket: ${E2E_BUCKET}
    region: ${E2E_S3_REGION}
    endpoint: ${E2E_S3_ENDPOINT}
    access_key: ${E2E_S3_ACCESS_KEY}
    secret_key: ${E2E_S3_SECRET_KEY}
EOF
  : > "${slog}"
  ( cd "${SERVER_DIR}" && exec env -u PJ_CLOUD_TOKEN setsid ./bin/pj-cloud-server \
      -config "${config}" -listen ":${E2E_PORT}" -db "${E2E_SCRATCH}/catalog.db" \
      -allow-anonymous >>"${slog}" 2>&1 ) &
  E2E_SERVER_PID=$!
  if ! wait_http "http://localhost:${E2E_PORT}/health" 60; then
    log "----- server log (tail) -----"; tail -n 40 "${slog}" || true
    fail "server: :${E2E_PORT}/health did not come up within 60s"
  fi
  grep -q 'catalog: opened SQLite store READ-ONLY (external builder)' "${slog}" \
    || { tail -n 40 "${slog}" || true; fail "server: read-only catalog open not confirmed in the log"; }
  log "step d: OK (server pid ${E2E_SERVER_PID}, /health up)"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step e: vector-fixture binding (§1 E4a). The frozen e2e-8082-* vector cases
# are read VERBATIM (canonical bytes + identity as stored, no re-serialization);
# each identity is self-checked against sha256/128 of those exact bytes, and
# every s3_key they commit to must be served by the live :8082 catalog (plus
# window-containment for the intra-file trust window). This pins vectors <->
# deterministic corpus so neither can drift silently.
# ─────────────────────────────────────────────────────────────────────────────
step_vector_binding() {
  log "step e: vector-fixture binding (${VECTOR_CASE_MAIN}, ${VECTOR_CASE_EAGER}, ${VECTOR_CASE_TRUST})"
  local probe="${SERVER_DIR}/bin/devprobe" list_json
  list_json="$("${probe}" -url "${E2E_WS}" 2>/dev/null)" \
    || fail "vectors: devprobe ListFiles failed against ${E2E_WS}"
  E2E_LIST_JSON="${list_json}" python3 - "${VECTORS_JSON}" \
      "${VECTOR_CASE_MAIN}" "${VECTOR_CASE_EAGER}" "${VECTOR_CASE_TRUST}" <<'PY' \
    || fail "vectors: vector-fixture binding check failed (see above)"
import hashlib, json, os, sys

vectors_path, expected_names = sys.argv[1], sys.argv[2:]
vectors = json.load(open(vectors_path))
cases = {c["name"]: c for c in vectors["cases"] if c["name"] in expected_names}
missing = [n for n in expected_names if n not in cases]
if missing:
    sys.exit(f"missing vector case(s) in {vectors_path}: {missing}")

served = json.loads(os.environ["E2E_LIST_JSON"])
by_key = {f["s3_key"]: f for f in served.get("files", [])}

for name in expected_names:
    c = cases[name]
    canon = c["canonical"]          # frozen bytes, used VERBATIM
    ident = c["identity"]
    want = "mcap-cloud:v1:sha256/128:" + hashlib.sha256(canon.encode()).hexdigest()[:32]
    if ident != want:
        sys.exit(f"{name}: stored identity {ident} != sha256/128 of the stored canonical bytes ({want})")
    d = json.loads(canon)           # parse-only: keys/window for the liveness assertions
    for key in d["s3_keys"]:
        if key not in by_key:
            sys.exit(f"{name}: s3_key {key!r} not served by the :8082 catalog "
                     f"(seeded corpus and frozen vectors have drifted)")
        f = by_key[key]
        s, e = int(d["start_ns"]), int(d["end_ns"])
        if (s, e) != (0, 0):
            fs, fe = int(f["start_ns"]), int(f["end_ns"])
            if not (fs <= s <= e <= fe):
                sys.exit(f"{name}: window [{s},{e}] outside {key!r}'s recorded [{fs},{fe}]")
    print(f"[e2e-layout]   {name}: identity {ident.rsplit(':',1)[-1]} — "
          f"{len(d['s3_keys'])} key(s) served, window ok")
PY
  log "step e: OK"
}

# rebuild_official_plugin PLUGIN — build one official plugin against
# REQUIRED_SDK_VERSION by temp-editing that repo's SDK_VERSION (the per-plugin
# conanfile reads it live) and ALWAYS restoring it — nothing is committed
# there. On failure this is a hard fail: staging the checkout's older-SDK
# binaries silently is exactly what the provenance discipline forbids.
rebuild_official_plugin() {
  local plugin="$1"
  local sdk_file="${OFFICIAL_PLUGINS_ROOT}/SDK_VERSION"
  local blog="${E2E_SCRATCH}/official-${plugin}-build.log"
  log "step f: rebuilding ${plugin} against SDK ${REQUIRED_SDK_VERSION} (SDK_VERSION temp-edit; log ${blog})"
  cp "${sdk_file}" "${sdk_file}.e2e-bak"
  OFFICIAL_SDK_BAK="${sdk_file}.e2e-bak"
  printf '%s\n' "${REQUIRED_SDK_VERSION}" > "${sdk_file}"
  local rc=0
  ( cd "${OFFICIAL_PLUGINS_ROOT}" && ./build.sh "${plugin}" ) >>"${blog}" 2>&1 || rc=$?
  restore_official_sdk_version
  if (( rc != 0 )); then
    log "----- ${plugin} build log (tail) -----"; tail -n 30 "${blog}" || true
    fail "dso: ${plugin} rebuild against SDK ${REQUIRED_SDK_VERSION} FAILED (exit ${rc}) — likely SDK API drift; NOT staging the checkout's older-SDK binaries. Full log kept at ${blog} until teardown; report BLOCKED with the tail above."
  fi
}

# stage_official_dso PLUGIN SONAME MANIFEST — ensure a REQUIRED_SDK_VERSION
# build of one official plugin exists (rebuild if missing or built against a
# different SDK), stage its .so + manifest sidecar, and record provenance.
stage_official_dso() {
  local plugin="$1" soname="$2" manifest="$3"
  local build_dir="${OFFICIAL_PLUGINS_ROOT}/build/${plugin}"
  local so="${build_dir}/Release/bin/${soname}"
  local sidecar="${build_dir}/Release/bin/${manifest}"
  local verfile="${build_dir}/plotjuggler_sdk-config-version.cmake"
  local built_sdk=""
  [[ -f "${verfile}" ]] \
    && built_sdk="$(sed -n 's/.*PACKAGE_VERSION "\([^"]*\)".*/\1/p' "${verfile}" | head -n1)"
  if [[ ! -f "${so}" || "${built_sdk}" != "${REQUIRED_SDK_VERSION}" ]]; then
    log "step f: ${plugin} needs a rebuild (so present: $([[ -f ${so} ]] && echo yes || echo no), built SDK: ${built_sdk:-none}, need ${REQUIRED_SDK_VERSION})"
    rebuild_official_plugin "${plugin}"
    built_sdk="$(sed -n 's/.*PACKAGE_VERSION "\([^"]*\)".*/\1/p' "${verfile}" | head -n1)"
  fi
  [[ -f "${so}" ]] || fail "dso: ${so} still missing after rebuild"
  [[ -f "${sidecar}" ]] || fail "dso: manifest sidecar ${sidecar} missing"
  [[ "${built_sdk}" == "${REQUIRED_SDK_VERSION}" ]] \
    || fail "dso: ${plugin} build is against SDK '${built_sdk}', need ${REQUIRED_SDK_VERSION} — refusing to stage"
  cp "${so}" "${sidecar}" "${E2E_SCRATCH}/extensions/"
  record_provenance "${so}" "${sidecar}" "${OFFICIAL_PLUGINS_ROOT}" "${built_sdk}"
}

# record_provenance SO MANIFEST REPO SDK — one line per staged DSO: manifest
# id=version, SDK it was built against, source repo, git rev (+ -dirty), sha256.
record_provenance() {
  local so="$1" manifest="$2" repo="$3" sdk="$4"
  local idver rev dirty="" sha
  idver="$(python3 -c 'import json,sys; m=json.load(open(sys.argv[1])); print(m["id"] + "=" + m["version"])' "${manifest}")" \
    || fail "provenance: cannot parse ${manifest}"
  rev="$(git -C "${repo}" rev-parse --short=12 HEAD 2>/dev/null || echo unknown)"
  [[ -z "$(git -C "${repo}" status --porcelain 2>/dev/null)" ]] || dirty="-dirty"
  sha="$(sha256sum "${so}" | awk '{print $1}')"
  printf '%s sdk=%s repo=%s rev=%s%s sha256=%s so=%s\n' \
      "${idver}" "${sdk}" "${repo}" "${rev}" "${dirty}" "${sha}" "$(basename "${so}")" \
    >> "${E2E_SCRATCH}/provenance.txt"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step f: DSO staging with provenance (§1 E1/E7). Both E2E halves (gui-test +
# shipped binary) must consume the SAME staged, provenance-recorded DSOs.
# ─────────────────────────────────────────────────────────────────────────────
step_stage_dsos() {
  log "step f: staging DSOs into ${E2E_SCRATCH}/extensions"
  mkdir -p "${E2E_SCRATCH}/extensions"
  : > "${E2E_SCRATCH}/provenance.txt"

  # 1) the cloud connector plugin from THIS repo (SDK pin preflighted).
  cp "${PLUGIN_SO}" "${PLUGIN_MANIFEST}" "${E2E_SCRATCH}/extensions/"
  record_provenance "${PLUGIN_SO}" "${PLUGIN_MANIFEST}" "${REPO_ROOT}" "${REQUIRED_SDK_VERSION}"

  # 2) + 3) loader + parser from pj-official-plugins, rebuilt on the pinned SDK.
  stage_official_dso data_load_mcap libmcap_source_plugin.so mcap_source_plugin.pjmanifest.json
  stage_official_dso parser_ros libparser_ros_plugin.so parser_ros_plugin.pjmanifest.json

  log "step f: staged DSO provenance:"
  sed 's/^/[e2e-layout]   /' "${E2E_SCRATCH}/provenance.txt"
  log "step f: OK"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step g: shipped-binary plugin validation pre-flight (§1 E5): plotjuggler4
# --validate-plugins over the staged extensions dir with an --expect-plugin
# entry for EVERY staged plugin (the validator rejects loaded-but-unexpected
# plugins). Runs offscreen in a private XDG sandbox.
# ─────────────────────────────────────────────────────────────────────────────
step_validate_plugins() {
  log "step g: plotjuggler4 --validate-plugins over the staged extensions"
  mkdir -p "${E2E_SCRATCH}/sandbox/config" "${E2E_SCRATCH}/sandbox/cache" "${E2E_SCRATCH}/sandbox/data"
  local -a expect_args=()
  local m idver
  for m in "${E2E_SCRATCH}/extensions/"*.pjmanifest.json; do
    idver="$(python3 -c 'import json,sys; m=json.load(open(sys.argv[1])); print(m["id"] + "=" + m["version"])' "${m}")" \
      || fail "validate: cannot parse ${m}"
    expect_args+=(--expect-plugin "${idver}")
  done
  (( ${#expect_args[@]} == 6 )) \
    || fail "validate: expected 3 staged manifests, found $(( ${#expect_args[@]} / 2 ))"
  local vlog="${E2E_SCRATCH}/validate-plugins.log" rc=0
  env QT_QPA_PLATFORM=offscreen \
      XDG_CONFIG_HOME="${E2E_SCRATCH}/sandbox/config" \
      XDG_CACHE_HOME="${E2E_SCRATCH}/sandbox/cache" \
      XDG_DATA_HOME="${E2E_SCRATCH}/sandbox/data" \
      timeout 120s "${PJ4_APP}" --nosplash \
      --validate-plugins "${E2E_SCRATCH}/extensions" "${expect_args[@]}" \
      >"${vlog}" 2>&1 || rc=$?
  if (( rc != 0 )); then
    log "----- validate-plugins log (tail) -----"; tail -n 30 "${vlog}" || true
    fail "validate: plotjuggler4 --validate-plugins exited ${rc} (staged DSOs rejected?)"
  fi
  log "step g: OK (all 3 staged plugins validated: $(printf '%s ' "${expect_args[@]}" | sed 's/--expect-plugin //g'))"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step h [TODO — Task 3]: the live gui-test leg. Runs the PJ4
# main_window_layout_import_e2e_test (real MainWindow offscreen, the staged
# DSOs, this live :8082 server) with the live env set — a SKIPPED gtest FAILS
# the harness once implemented (smoke's pattern).
# ─────────────────────────────────────────────────────────────────────────────
step_gtest_leg() {
  log "step h: SKIPPED-pending — Task 3 (PJ4 live gui-test main_window_layout_import_e2e_test) not implemented yet"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step i [TODO — Task 4]: the shipped-binary legs — plotjuggler4 --layout
# --exit-after-layout --dump-diagnostics for the cold / warm / EAGER / trust
# scenarios (frozen vector identities above), asserting diagnostic IDs, the
# reconstructed artifact, and the zero-network Prometheus counters
# (pj_cloud_sessions_total / pj_cloud_ws_connections_total).
# ─────────────────────────────────────────────────────────────────────────────
step_shipped_legs() {
  if [[ "${PJ4_FLAGS_PRESENT}" == "1" ]]; then
    log "step i: SKIPPED-pending — Task 4 (shipped-binary cold/warm/EAGER/trust legs) not implemented yet"
  else
    log "step i: SKIPPED-pending — Task 4 not implemented AND the PJ4 flags (--dump-diagnostics/--exit-after-layout) are absent (PJ4 PR not merged/built yet)"
  fi
}

# ─────────────────────────────────────────────────────────────────────────────
main() {
  if [[ "${1:-}" == "--dry-run" ]]; then
    dry_run
  elif [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    printf '\nUsage: %s [--dry-run]\n' "$0"
    exit 0
  elif [[ -n "${1:-}" ]]; then
    printf 'unknown argument: %s (only --dry-run is supported)\n' "$1" >&2
    exit 2
  fi

  # Shared harness lock: WAIT for smoke (or another e2e run), never fail.
  exec 200>"${HARNESS_LOCK}"
  if ! flock -n 200; then
    log "waiting for the shared harness lock ${HARNESS_LOCK} (another smoke/e2e run is active)..."
    flock 200
  fi

  preflight
  E2E_SCRATCH="$(mktemp -d /tmp/pj-e2e-layout.XXXXXX)"
  trap cleanup EXIT
  log "stage-5 layout-import E2E harness starting (repo ${REPO_ROOT}, scratch ${E2E_SCRATCH})"

  step_build_tools
  step_seed
  step_builder_once
  step_server
  step_vector_binding
  step_stage_dsos
  step_validate_plugins
  step_gtest_leg
  step_shipped_legs

  E2E_VERDICT="E2E-LAYOUT-IMPORT PASS"
}

main "$@"
