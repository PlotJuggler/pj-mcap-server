#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# e2e-layout-import.sh — the stage-5 cross-repo live E2E gate for the canonical
# layout import. As-built reference: docs/layout-import-architecture.md;
# operations: docs/layout-sharing-runbook.md.
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
# It proves, live and end to end:
#   a-g  bring-up: shared Go tools, deterministic corpus in its own bucket, a
#        one-shot Python catalog build, the read-only server on :8082, the
#        frozen vectors bound to the served corpus, the three REAL DSOs staged
#        with provenance, and plotjuggler4's own --validate-plugins pre-flight;
#   h    the LIVE PJ4 gui-test (main_window_layout_import_e2e_test): a real
#        MainWindow offscreen against this server through the staged DSOs —
#        cold promotion with a live progressive witness, the literal GUI
#        save/load flow warm + zero-network, EAGER_ONLY in-process, the trust
#        gate, and the three-way catalog-equality signature. The harness sets
#        the live env, so a SKIPPED test is a GATING BUG and FAILS the run;
#   i    the SHIPPED binary: three `plotjuggler4 --layout --exit-after-layout
#        --dump-diagnostics` legs in private XDG sandboxes — (1) cold, which
#        must promote (artifact materialized, zero layout-import failure ids,
#        curves resolved); (2) warm, the same layout in the same sandbox, which
#        must touch NO network (pj_cloud_sessions_total and
#        pj_cloud_ws_connections_total unchanged, artifact mtime unchanged);
#        (3) EAGER_ONLY with a REGULAR FILE as the cache root, which must
#        report `layout-import-eager-only` and materialize nothing. Leg 1 runs
#        FIRST on purpose: it is the baseline that proves the parsers decode,
#        without which a decode failure could masquerade as the EAGER result.
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
# Post-mortem artifacts (provenance record, every diagnostic dump, the gtest
# log + JSON, the server/builder logs) are copied OUT of the per-run scratch
# into /tmp/pj-e2e-layout-artifacts/<timestamp>/ (override: E2E_ARTIFACT_DIR)
# on EVERY exit path — the scratch itself is always removed.
#
# The corpus in s3://e2e-layout is seeded ONCE and left in place. It is the
# decode oracle for both legs, so after a gen-ci-fixtures/genmcap change you
# must EMPTY the bucket to force a reseed — a stale corpus surfaces as
# unresolved curves (the harness says so in the failure message).
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
# repo's pin (never hardcoded; CLAUDE.md convention). Declaration split from
# assignment (SC2155) and fail-fast: an unreadable/empty pin would otherwise
# ride silently into every later SDK comparison and garble their messages.
REQUIRED_SDK_VERSION="$(tr -d '[:space:]' < "${REPO_ROOT}/plugin/SDK_VERSION" 2>/dev/null || true)"
if [[ -z "${REQUIRED_SDK_VERSION}" ]]; then
  printf 'E2E-LAYOUT-IMPORT PREFLIGHT FAIL: cannot read the SDK pin from %s\n' \
      "${REPO_ROOT}/plugin/SDK_VERSION" >&2
  exit 2
fi
readonly REQUIRED_SDK_VERSION

# Sibling checkouts (overridable for other machines). BOTH PJ4 halves — the
# shipped binary and the live gui-test — come from ONE build tree, so that the
# staged DSOs meet the same ABI on both sides; E2E_PJ4_BUILD moves the whole
# pair (e.g. to a PJ4 worktree's build/), the two per-binary overrides exist
# for odd layouts only.
readonly PJ4_BUILD="${E2E_PJ4_BUILD:-${HOME}/ws_plotjuggler/PJ4/build}"
readonly PJ4_APP="${E2E_PJ4_APP:-${PJ4_BUILD}/pj_app/plotjuggler4}"
readonly PJ4_GUI_TEST="${E2E_PJ4_GUI_TEST:-${PJ4_BUILD}/pj_app/main_window_layout_import_e2e_test}"
readonly OFFICIAL_PLUGINS_ROOT="${E2E_OFFICIAL_PLUGINS:-${HOME}/ws_plotjuggler/pj-official-plugins}"

# Kept post-mortem artifacts (survive the scratch teardown). Declaration split
# from assignment (SC2155), as with the SDK pin above.
ARTIFACT_DIR="${E2E_ARTIFACT_DIR:-}"
if [[ -z "${ARTIFACT_DIR}" ]]; then
  ARTIFACT_DIR="/tmp/pj-e2e-layout-artifacts/$(date +%Y%m%d-%H%M%S)"
fi
readonly ARTIFACT_DIR

# The gui-test's scenario count — a live run must report exactly this many
# tests, all PASSED (a SKIP means the live gating broke; see step h).
readonly GUI_TEST_EXPECTED_TESTS=5

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

# The stage-5 PJ4 build (--dump-diagnostics + --exit-after-layout + the live
# gui-test binary) is a HARD requirement now that steps h/i are real: without
# it there is no gate left to run, so preflight fail-fasts. Set
# E2E_REQUIRE_PJ4_FLAGS=0 to downgrade to a loud SKIPPED-pending instead —
# only useful while bisecting the bring-up steps on a machine whose PJ4 build
# predates the stage-5 PR.
readonly REQUIRE_PJ4_FLAGS="${E2E_REQUIRE_PJ4_FLAGS:-1}"

# The layout-import FAILURE diagnostic ids (PJ4 MainWindow + ImportRuntime).
# Any of these in a leg's dump is a hard failure. Deliberately excludes the
# non-failure ids: `layout-import-eager-only` (a degradation each leg asserts
# for explicitly) and the `...-unresolved-curves*` pair (asserted separately
# as the decode oracle).
readonly LAYOUT_IMPORT_FAILURE_IDS=(
  layout-import-untrusted
  layout-import-refused
  layout-import-query-failed
  layout-import-query-invalid
  layout-import-descriptor-invalid
  layout-import-job-failed
  layout-import-job-start-failed
  layout-import-load-failed
  layout-import-load-rejected
  layout-import-provider-unavailable
  layout-import-size-limit
  layout-import-materialized-missing
  layout-import-cancelled
)

# ── go toolchain on PATH (per project context) ───────────────────────────────
export PATH="$HOME/.local/go/bin:$HOME/go/bin:$PATH"

# ── state for cleanup (per-run; NO fixed pidfiles) ───────────────────────────
E2E_SCRATCH=""
E2E_SERVER_PID=""
# Backup path while the official-plugins SDK_VERSION temp-edit is in flight —
# restored by cleanup() even if the rebuild crashes mid-way.
OFFICIAL_SDK_BAK=""
PJ4_FLAGS_PRESENT=""
# Set by run_layout_leg for its caller (exit code + diagnostic dump path).
E2E_LEG_RC=0
E2E_LEG_DUMP=""

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

# keep_artifacts — copy the post-mortem set (provenance, diagnostic dumps,
# gtest log/JSON, leg logs, server/builder logs, the authored layouts) out of
# the doomed scratch. Runs from cleanup(), so it must survive ANY exit path
# and never fail the run itself. The server config is deliberately NOT copied
# (it carries the Minio credentials).
keep_artifacts() {
  [[ -n "${E2E_SCRATCH}" && -d "${E2E_SCRATCH}" ]] || return 0
  mkdir -p "${ARTIFACT_DIR}" 2>/dev/null || return 0
  find "${E2E_SCRATCH}" -maxdepth 1 -type f \
       \( -name '*.log' -o -name '*.json' -o -name '*.txt' -o -name '*.xml' \) \
       -exec cp -f {} "${ARTIFACT_DIR}/" \; 2>/dev/null || true
  log "post-mortem artifacts kept in ${ARTIFACT_DIR}"
}

# cleanup — reap the harness server's PROCESS GROUP, restore the
# official-plugins SDK_VERSION if a temp-edit was in flight, preserve the
# post-mortem artifacts, remove the whole per-run scratch (db/WAL/SHM, config,
# logs, staged extensions, sandboxes) and print the verdict LAST (smoke's B1
# final-line contract). Minio and the seeded e2e-layout bucket are
# deliberately LEFT ALONE.
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
  # AFTER the server is reaped, so its log is complete in the kept copy.
  keep_artifacts
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
  PJ4 surface   --dump-diagnostics / --exit-after-layout probed via --help, plus
                ${PJ4_GUI_TEST}
                (REQUIRE_PJ4_FLAGS=${REQUIRE_PJ4_FLAGS}; absent => $( [[ "${REQUIRE_PJ4_FLAGS}" == "1" ]] && echo "preflight FAIL" || echo "steps h/i SKIPPED-pending" ))
  step h        the live gui-test, env MCAP_CLOUD_E2E_{URL,EXTENSIONS,VECTORS} +
                QT_QPA_PLATFORM=offscreen; expects ${GUI_TEST_EXPECTED_TESTS} tests, all PASSED
                (a SKIPPED test FAILS the harness — the env is set, so a skip is a gating bug)
  step i        3 shipped-binary legs, each offscreen in its own XDG sandbox under
                \${scratch}/legs/ (trust ledger pre-seeded, update-check/telemetry off):
                  cold  ${VECTOR_CASE_MAIN}: exit 0 + artifact + clean dump
                  warm  same layout/sandbox: exit 0 + pj_cloud_{sessions,ws_connections}_total
                        unchanged + artifact mtime unchanged
                  eager ${VECTOR_CASE_EAGER} with MCAP_CLOUD_CACHE_DIR=<a REGULAR FILE>:
                        exit 0 + layout-import-eager-only + nothing materialized
  artifacts     ${ARTIFACT_DIR} (dumps, gtest log+JSON, leg logs, provenance, layouts)
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
  # The stage-5 acceptance surface: BOTH flags (step i's only observation
  # channel) AND the live gui-test binary (step h) must exist, or there is no
  # gate left to run.
  local missing=""
  grep -q -- '--dump-diagnostics' <<<"${help_out}" || missing+=" --dump-diagnostics"
  grep -q -- '--exit-after-layout' <<<"${help_out}" || missing+=" --exit-after-layout"
  [[ -x "${PJ4_GUI_TEST}" ]] || missing+=" ${PJ4_GUI_TEST}"
  if [[ -z "${missing}" ]]; then
    PJ4_FLAGS_PRESENT=1
  else
    PJ4_FLAGS_PRESENT=0
    if [[ "${REQUIRE_PJ4_FLAGS}" == "1" ]]; then
      preflight_fail "PJ4 build at ${PJ4_BUILD} lacks the stage-5 acceptance surface (missing:${missing}) — build the layout-import stage-5 PJ4 branch, or point E2E_PJ4_BUILD at a build tree that has it"
    fi
    log "WARNING: PJ4 build at ${PJ4_BUILD} lacks the stage-5 acceptance surface (missing:${missing}); steps h and i stay SKIPPED-pending (E2E_REQUIRE_PJ4_FLAGS=0)"
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
  # 200>&- : do NOT let the server's process group inherit the shared harness
  # flock FD — a SIGKILL'd script would otherwise leave the surviving group
  # holding the lock and the next smoke/e2e run blocked forever.
  ( cd "${SERVER_DIR}" && exec env -u PJ_CLOUD_TOKEN setsid ./bin/pj-cloud-server \
      -config "${config}" -listen ":${E2E_PORT}" -db "${E2E_SCRATCH}/catalog.db" \
      -allow-anonymous >>"${slog}" 2>&1 200>&- ) &
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

# rebuild_official_plugin PLUGIN STALE_SO — build one official plugin against
# REQUIRED_SDK_VERSION by temp-editing that repo's SDK_VERSION (the per-plugin
# conanfile reads it live) and ALWAYS restoring it — nothing is committed
# there. On failure this is a hard fail: staging the checkout's older-SDK
# binaries silently is exactly what the provenance discipline forbids.
rebuild_official_plugin() {
  local plugin="$1" stale_so="$2"
  local sdk_file="${OFFICIAL_PLUGINS_ROOT}/SDK_VERSION"
  local blog="${E2E_SCRATCH}/official-${plugin}-build.log"
  # Drop any prior .so FIRST: a SIGKILL between conan-install (which already
  # writes the new-SDK version file) and the link step must never leave a
  # stale binary that the next run's skip-path would stage with a lying
  # sdk=<new> provenance.
  rm -f "${stale_so}"
  log "step f: rebuilding ${plugin} against SDK ${REQUIRED_SDK_VERSION} (SDK_VERSION temp-edit; log ${blog})"
  cp "${sdk_file}" "${sdk_file}.e2e-bak"
  OFFICIAL_SDK_BAK="${sdk_file}.e2e-bak"
  printf '%s\n' "${REQUIRED_SDK_VERSION}" > "${sdk_file}"
  local rc=0
  ( cd "${OFFICIAL_PLUGINS_ROOT}" && ./build.sh "${plugin}" ) >>"${blog}" 2>&1 || rc=$?
  restore_official_sdk_version
  if (( rc != 0 )); then
    # Preserve the FULL log outside the scratch (teardown deletes the scratch
    # milliseconds after this message; a C++ template error never fits a tail).
    local kept_log="/tmp/pj-e2e-layout-${plugin}-build.log"
    mv -f "${blog}" "${kept_log}" 2>/dev/null || kept_log="${blog}"
    log "----- ${plugin} build log (tail) -----"; tail -n 30 "${kept_log}" || true
    fail "dso: ${plugin} rebuild against SDK ${REQUIRED_SDK_VERSION} FAILED (exit ${rc}) — likely SDK API drift; NOT staging the checkout's older-SDK binaries. Full log kept at ${kept_log}; report BLOCKED with its contents."
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
    rebuild_official_plugin "${plugin}" "${so}"
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
# Step h: the live gui-test leg (§1 E1a). Runs the PJ4
# main_window_layout_import_e2e_test — a real MainWindow offscreen, the staged
# DSOs, this live :8082 server — with the live env set. Because the harness
# SETS that env, a SKIPPED test can only mean the live gating broke, so a skip
# FAILS the harness (smoke's pattern). The binary builds its OWN private XDG
# sandbox internally (a QTemporaryDir installed before QApplication), so this
# leg needs no sandbox of its own.
# ─────────────────────────────────────────────────────────────────────────────
step_gtest_leg() {
  if [[ "${PJ4_FLAGS_PRESENT}" != "1" ]]; then
    log "step h: SKIPPED-pending — the PJ4 stage-5 acceptance surface is absent (E2E_REQUIRE_PJ4_FLAGS=0)"
    return 0
  fi
  log "step h: live gui-test $(basename "${PJ4_GUI_TEST}") (${GUI_TEST_EXPECTED_TESTS} scenarios, live env)"
  local glog="${E2E_SCRATCH}/gtest-e2e.log" gjson="${E2E_SCRATCH}/gtest-e2e.json" rc=0
  env QT_QPA_PLATFORM=offscreen \
      MCAP_CLOUD_E2E_URL="ws://localhost:${E2E_PORT}" \
      MCAP_CLOUD_E2E_EXTENSIONS="${E2E_SCRATCH}/extensions" \
      MCAP_CLOUD_E2E_VECTORS="${VECTORS_JSON}" \
      timeout 1200s "${PJ4_GUI_TEST}" "--gtest_output=json:${gjson}" \
      >"${glog}" 2>&1 || rc=$?
  if [[ ! -f "${gjson}" ]]; then
    log "----- gui-test log (tail) -----"; tail -n 40 "${glog}" || true
    fail "gtest: ${PJ4_GUI_TEST} wrote no JSON report (exit ${rc}; crash?) — log kept in ${ARTIFACT_DIR}"
  fi
  # Distinguish PASSED / FAILED / SKIPPED from the machine-readable report: a
  # skip is invisible in the process exit code but is a GATING BUG here.
  local summary
  # 2>&1: the assertion detail rides sys.exit()'s stderr message.
  summary="$(E2E_EXPECTED_TESTS="${GUI_TEST_EXPECTED_TESTS}" python3 - "${gjson}" 2>&1 <<'PY'
import json, os, sys

report = json.load(open(sys.argv[1]))
expected = int(os.environ["E2E_EXPECTED_TESTS"])
passed, failed, skipped = [], [], []
for suite in report.get("testsuites", []):
    for case in suite.get("testsuite", []):
        name = f"{suite.get('name', '?')}.{case.get('name', '?')}"
        if case.get("result") == "SKIPPED" or case.get("status") == "NOTRUN":
            skipped.append(name)
        elif case.get("failures"):
            failed.append(name)
        else:
            passed.append(name)
problems = []
if skipped:
    problems.append(
        "SKIPPED (the harness set the live env, so a skip means the live gating "
        "broke): " + ", ".join(skipped))
if failed:
    problems.append("FAILED: " + ", ".join(failed))
total = len(passed) + len(failed) + len(skipped)
if total != expected:
    problems.append(f"ran {total} test(s), expected {expected} (scenario added/removed?)")
if problems:
    sys.exit("; ".join(problems))
print(f"{len(passed)}/{expected} scenarios passed")
PY
)" || fail "gtest: ${summary:-live gui-test assertions failed} (exit ${rc}; full log in ${ARTIFACT_DIR})"
  (( rc == 0 )) || fail "gtest: ${PJ4_GUI_TEST} exited ${rc} despite a clean JSON report (log in ${ARTIFACT_DIR})"
  log "step h: OK (${summary})"
}

# ─────────────────────────────────────────────────────────────────────────────
# The shipped-binary legs (step i) and their helpers.
# ─────────────────────────────────────────────────────────────────────────────

# make_sandbox NAME — a private XDG sandbox under the run scratch, pre-seeded
# with (a) the trust ledger for this harness's origin (the legs are not a
# trust-gate test — the gui-test owns that scenario — so they start trusted),
# and (b) a QSettings file that turns the app's startup update-check and
# telemetry ping OFF, so a leg touches no network but its own. Prints the
# sandbox root.
make_sandbox() {
  local sandbox="${E2E_SCRATCH}/legs/$1"
  mkdir -p "${sandbox}"/{config,cache,data,home} "${sandbox}/config/mcap_cloud" \
           "${sandbox}/config/PlotJuggler"
  printf '{"v":1,"origins":["ws://localhost:%s"]}\n' "${E2E_PORT}" \
    > "${sandbox}/config/mcap_cloud/trusted_origins.json"
  # Keys are written in QSettings' own INI escaping (':' -> %3A) — that is the
  # exact form the app round-trips, so it can never mis-read them.
  cat > "${sandbox}/config/PlotJuggler/PlotJuggler4.conf" <<'EOF'
[General]
Preferences%3A%3Acheck_updates_on_startup=false
Preferences%3A%3Asend_anonymous_stats=false
EOF
  printf '%s\n' "${sandbox}"
}

# write_leg_layout CASE TOPIC FIELD ARTIFACT OUT — hand-author the layout a
# production save would have written for one FROZEN vector case: a
# source-bound document with one topic/field curve and one <materialize>
# stanza whose provider/identity are attributes and whose descriptor bytes are
# the vector's `canonical` string VERBATIM in CDATA (§1 E4a — never
# re-serialized here; the vectors are the independent witness of the
# cross-repo canonicalizer). PJ4 never cross-checks the embedded identity, so
# it must come from the vector, not from us.
write_leg_layout() {
  local case_name="$1" topic="$2" field="$3" artifact="$4" out="$5"
  python3 - "${VECTORS_JSON}" "${case_name}" "${topic}" "${field}" "${artifact}" "${out}" <<'PY'
import json, sys
from xml.sax.saxutils import quoteattr

vectors_path, case_name, topic, field, artifact, out = sys.argv[1:7]
cases = {c["name"]: c for c in json.load(open(vectors_path))["cases"]}
if case_name not in cases:
    sys.exit(f"vector case {case_name!r} not in {vectors_path}")
case = cases[case_name]
canonical, identity = case["canonical"], case["identity"]
if "]]>" in canonical:
    sys.exit(f"{case_name}: canonical bytes contain ']]>' — needs the split-CDATA form")
with open(out, "w", encoding="utf-8") as fh:
    fh.write(
        '<root pj4_version="4" binding="source">\n'
        ' <tabbed_widget parent="main_window">\n'
        '  <Tab id="t1" containers="1">\n'
        '   <Container>\n'
        '    <DockArea id="a1" name="View">\n'
        '     <plot id="plot1" mode="TimeSeries">\n'
        f'      <curve topic={quoteattr(topic)} field={quoteattr(field)}/>\n'
        '     </plot>\n'
        '    </DockArea>\n'
        '   </Container>\n'
        '  </Tab>\n'
        ' </tabbed_widget>\n'
        ' <previouslyLoaded_Datafiles>\n'
        f'  <fileInfo filename={quoteattr(artifact)}>\n'
        f'   <materialize provider="mcap-cloud" identity={quoteattr(identity)}>'
        f'<![CDATA[{canonical}]]></materialize>\n'
        '  </fileInfo>\n'
        ' </previouslyLoaded_Datafiles>\n'
        '</root>\n')
print(identity)
PY
}

# identity_hex CASE — the vector identity's trailing hex (the cache artifact's
# basename).
identity_hex() {
  python3 - "${VECTORS_JSON}" "$1" <<'PY'
import json, sys
cases = {c["name"]: c for c in json.load(open(sys.argv[1]))["cases"]}
print(cases[sys.argv[2]]["identity"].rsplit(":", 1)[-1])
PY
}

# scrape_counter NAME — one Prometheus counter off the live server's /metrics.
scrape_counter() {
  local body
  body="$(curl -fsS -m 10 "http://localhost:${E2E_PORT}/metrics")" || return 1
  awk -v n="$1" '$1 == n { print $2; found = 1 } END { exit found ? 0 : 1 }' <<<"${body}"
}

# run_layout_leg LABEL SANDBOX LAYOUT [ENV=VAL ...] — one shipped-binary run:
# the REAL plotjuggler4, offscreen, in its own XDG sandbox, over the staged
# DSOs, quitting at the restore-settlement boundary with a failure-aware exit
# code. Sets E2E_LEG_RC / E2E_LEG_DUMP for the caller (never fails by itself —
# every leg words its own failure).
run_layout_leg() {
  local label="$1" sandbox="$2" layout="$3"; shift 3
  local log="${E2E_SCRATCH}/leg-${label}.log"
  E2E_LEG_DUMP="${E2E_SCRATCH}/dump-${label}.json"
  E2E_LEG_RC=0
  env "$@" \
      QT_QPA_PLATFORM=offscreen \
      HOME="${sandbox}/home" \
      XDG_CONFIG_HOME="${sandbox}/config" \
      XDG_CACHE_HOME="${sandbox}/cache" \
      XDG_DATA_HOME="${sandbox}/data" \
      timeout 900s "${PJ4_APP}" --nosplash \
      --plugin-dir "${E2E_SCRATCH}/extensions" \
      --layout "${layout}" \
      --exit-after-layout --exit-after-layout-timeout 300 \
      --dump-diagnostics "${E2E_LEG_DUMP}" \
      >"${log}" 2>&1 || E2E_LEG_RC=$?
}

# check_dump LABEL DUMP EXPECT_EAGER — assert on diagnostic IDs (never on
# message text, §1 E2). Hard-fails on ANY layout-import failure id and on
# `layout-import-unresolved-curves` (the drain-time id — the decode oracle:
# the curve only resolves if the real ros-parser decoded the corpus);
# EXPECT_EAGER 1/0 requires the presence/absence of `layout-import-eager-only`.
check_dump() {
  local label="$1" dump="$2" expect_eager="$3" out=""
  # 2>&1: the assertion detail rides sys.exit()'s stderr message.
  out="$(E2E_FAILURE_IDS="${LAYOUT_IMPORT_FAILURE_IDS[*]}" \
         python3 - "${dump}" "${expect_eager}" 2>&1 <<'PY'
import json, os, sys

dump_path, expect_eager = sys.argv[1], sys.argv[2] == "1"
try:
    doc = json.load(open(dump_path))
except Exception as exc:                       # noqa: BLE001 - reported verbatim
    sys.exit(f"unreadable diagnostic dump {dump_path}: {exc}")
if doc.get("version") != 1:
    sys.exit(f"unexpected diagnostic dump version {doc.get('version')!r} (want 1)")
records = doc.get("records", [])
failure_ids = set(os.environ["E2E_FAILURE_IDS"].split())
problems = []

hits = [r for r in records if r.get("id") in failure_ids]
if hits:
    problems.append("layout-import failure diagnostic(s): " + "; ".join(
        f"{r.get('id')} [{r.get('level')}] {r.get('message')}" for r in hits))

unresolved = [r for r in records if r.get("id") == "layout-import-unresolved-curves"]
if unresolved:
    problems.append(
        "curves stayed unresolved after the import drained — the layout's curve did "
        "not bind to decoded data (stale corpus in the e2e-layout bucket? empty it "
        "to force a reseed; or a parser DSO that cannot decode it): " + "; ".join(
            r.get("message", "") for r in unresolved))

eager = sum(1 for r in records if r.get("id") == "layout-import-eager-only")
if expect_eager and eager == 0:
    problems.append(
        "no layout-import-eager-only diagnostic — the broken cache root did NOT "
        "degrade the import to EAGER_ONLY (ids seen: " +
        ", ".join(sorted({r.get("id", "") for r in records})) + ")")
if not expect_eager and eager:
    problems.append(f"unexpected layout-import-eager-only x{eager} — this leg must PROMOTE")

if problems:
    sys.exit(" | ".join(problems))
print(f"{len(records)} diagnostic record(s), no failure ids, curves resolved" +
      (f", eager-only x{eager}" if expect_eager else ""))
PY
)" || fail "leg ${label}: ${out} (dump kept in ${ARTIFACT_DIR})"
  log "  leg ${label}: ${out}"
}

# ─────────────────────────────────────────────────────────────────────────────
# Step i: the shipped-binary legs (§1 E1b, E4b, E4c). Three runs of the REAL
# plotjuggler4 with `--layout --exit-after-layout --dump-diagnostics`, each in
# its own private XDG sandbox:
#   1 cold  — must PROMOTE: exit 0, the cache artifact materialized, a clean
#             dump. This is the BASELINE that proves the parsers decode; it
#             runs first on purpose (see leg 3).
#   2 warm  — the SAME layout in the SAME sandbox: exit 0, and ZERO network —
#             pj_cloud_sessions_total and pj_cloud_ws_connections_total
#             unchanged across the run, artifact mtime unchanged.
#   3 EAGER — a fresh sandbox and a REGULAR FILE as the cache root, so
#             <file>/<digest>.lock fails non-contended => the tee is dropped
#             (§9.6) => EAGER_ONLY: exit 0, `layout-import-eager-only` in the
#             dump, and NO artifact anywhere.
# ─────────────────────────────────────────────────────────────────────────────
step_shipped_legs() {
  if [[ "${PJ4_FLAGS_PRESENT}" != "1" ]]; then
    log "step i: SKIPPED-pending — the PJ4 stage-5 acceptance surface is absent (E2E_REQUIRE_PJ4_FLAGS=0)"
    return 0
  fi
  log "step i: shipped-binary legs (cold / warm / EAGER_ONLY) with ${PJ4_APP}"

  # ---- leg 1: cold ---------------------------------------------------------
  local main_sandbox main_layout main_hex main_artifact
  main_sandbox="$(make_sandbox main)"
  main_hex="$(identity_hex "${VECTOR_CASE_MAIN}")" || fail "legs: cannot read the ${VECTOR_CASE_MAIN} identity"
  main_artifact="${main_sandbox}/cache/mcap_cloud/sessions/${main_hex}.mcap"
  main_layout="${E2E_SCRATCH}/layout-cold.pj4.xml"
  write_leg_layout "${VECTOR_CASE_MAIN}" "/imu" "linear_acceleration/x" \
      "${main_artifact}" "${main_layout}" >/dev/null \
    || fail "legs: cannot author the cold layout from vector ${VECTOR_CASE_MAIN}"
  [[ ! -e "${main_artifact}" ]] || fail "legs: cold leg needs a fresh sandbox but ${main_artifact} already exists"

  run_layout_leg cold "${main_sandbox}" "${main_layout}"
  (( E2E_LEG_RC == 0 )) || {
    log "----- cold leg log (tail) -----"; tail -n 40 "${E2E_SCRATCH}/leg-cold.log" || true
    fail "leg cold: plotjuggler4 --exit-after-layout exited ${E2E_LEG_RC} (1 = load failed, 2 = timeout, 64 = usage; artifacts in ${ARTIFACT_DIR})"
  }
  [[ -f "${main_artifact}" ]] \
    || fail "leg cold: the promoted import materialized no artifact at ${main_artifact}"
  check_dump cold "${E2E_LEG_DUMP}" 0
  local artifact_stamp_before
  artifact_stamp_before="$(stat -c '%Y %s' "${main_artifact}")"
  log "  leg cold: OK (exit 0, artifact $(stat -c '%s' "${main_artifact}") bytes)"

  # ---- leg 2: warm (zero network) -----------------------------------------
  local sessions_before conns_before sessions_after conns_after
  sessions_before="$(scrape_counter pj_cloud_sessions_total)" \
    || fail "leg warm: cannot scrape pj_cloud_sessions_total from :${E2E_PORT}/metrics"
  conns_before="$(scrape_counter pj_cloud_ws_connections_total)" \
    || fail "leg warm: cannot scrape pj_cloud_ws_connections_total from :${E2E_PORT}/metrics"

  run_layout_leg warm "${main_sandbox}" "${main_layout}"
  (( E2E_LEG_RC == 0 )) || {
    log "----- warm leg log (tail) -----"; tail -n 40 "${E2E_SCRATCH}/leg-warm.log" || true
    fail "leg warm: plotjuggler4 --exit-after-layout exited ${E2E_LEG_RC} (artifacts in ${ARTIFACT_DIR})"
  }
  sessions_after="$(scrape_counter pj_cloud_sessions_total)" \
    || fail "leg warm: cannot re-scrape pj_cloud_sessions_total"
  conns_after="$(scrape_counter pj_cloud_ws_connections_total)" \
    || fail "leg warm: cannot re-scrape pj_cloud_ws_connections_total"
  [[ "${sessions_before}" == "${sessions_after}" ]] \
    || fail "leg warm: pj_cloud_sessions_total moved ${sessions_before} -> ${sessions_after} — the warm load opened a SESSION instead of reading the cache"
  [[ "${conns_before}" == "${conns_after}" ]] \
    || fail "leg warm: pj_cloud_ws_connections_total moved ${conns_before} -> ${conns_after} — the warm load CONNECTED instead of reading the cache"
  [[ "$(stat -c '%Y %s' "${main_artifact}")" == "${artifact_stamp_before}" ]] \
    || fail "leg warm: the cache artifact was rewritten (mtime/size changed) — not a cache hit"
  check_dump warm "${E2E_LEG_DUMP}" 0
  log "  leg warm: OK (exit 0, zero network: sessions=${sessions_after} ws_connections=${conns_after} unchanged, artifact untouched)"

  # ---- leg 3: EAGER_ONLY (the pinned §10 requirement, shipped binary) ------
  local eager_sandbox eager_layout eager_hex eager_artifact eager_cache_file
  eager_sandbox="$(make_sandbox eager)"
  eager_hex="$(identity_hex "${VECTOR_CASE_EAGER}")" || fail "legs: cannot read the ${VECTOR_CASE_EAGER} identity"
  eager_artifact="${eager_sandbox}/cache/mcap_cloud/sessions/${eager_hex}.mcap"
  eager_layout="${E2E_SCRATCH}/layout-eager.pj4.xml"
  write_leg_layout "${VECTOR_CASE_EAGER}" "/imu" "linear_acceleration/x" \
      "${eager_artifact}" "${eager_layout}" >/dev/null \
    || fail "legs: cannot author the eager layout from vector ${VECTOR_CASE_EAGER}"
  # The lever: a REGULAR FILE as the cache root (a read-only DIRECTORY does
  # NOT work — the cache chmods its root 0700 before locking).
  eager_cache_file="${eager_sandbox}/cache-root-as-file"
  printf 'not a directory\n' > "${eager_cache_file}"

  run_layout_leg eager "${eager_sandbox}" "${eager_layout}" \
      MCAP_CLOUD_CACHE_DIR="${eager_cache_file}"
  (( E2E_LEG_RC == 0 )) || {
    log "----- eager leg log (tail) -----"; tail -n 40 "${E2E_SCRATCH}/leg-eager.log" || true
    fail "leg eager: plotjuggler4 --exit-after-layout exited ${E2E_LEG_RC}; EAGER_ONLY is a USABLE outcome and must still settle 0. The cold leg passed, so the parsers decode — this is a real regression, not a corpus problem (artifacts in ${ARTIFACT_DIR})"
  }
  check_dump eager "${E2E_LEG_DUMP}" 1
  [[ ! -e "${eager_artifact}" ]] \
    || fail "leg eager: an artifact appeared at ${eager_artifact} — EAGER_ONLY must materialize NOTHING"
  [[ -f "${eager_cache_file}" ]] \
    || fail "leg eager: the regular-file cache root at ${eager_cache_file} is gone — the cache must never replace it"
  [[ ! -e "${eager_cache_file}/${eager_hex}.mcap" ]] \
    || fail "leg eager: an artifact appeared under the broken cache root"
  log "  leg eager: OK (exit 0, layout-import-eager-only reported, nothing materialized)"

  log "step i: OK (3/3 shipped-binary legs)"
}

# ─────────────────────────────────────────────────────────────────────────────
main() {
  if [[ "${1:-}" == "--dry-run" ]]; then
    dry_run
  elif [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    # The whole leading comment block, verbatim: from line 2 up to (not
    # including) its closing rule. Derived, never a hardcoded line range — the
    # header grows every time a leg does.
    awk 'NR == 2 { next } NR > 2 && /^# ─/ { exit } NR > 2' "${BASH_SOURCE[0]}" \
      | sed 's/^# \{0,1\}//'
    printf '\nUsage: %s [--dry-run]\n\n' "$0"
    printf 'Environment:\n'
    printf '  E2E_PJ4_BUILD           PJ4 build tree for BOTH the shipped binary and the\n'
    printf '                          live gui-test (default %s)\n' "${HOME}/ws_plotjuggler/PJ4/build"
    printf '  E2E_PJ4_APP             override just the plotjuggler4 binary\n'
    printf '  E2E_PJ4_GUI_TEST        override just the gui-test binary\n'
    printf '  E2E_OFFICIAL_PLUGINS    pj-official-plugins checkout (loader + parser DSOs)\n'
    printf '  E2E_ARTIFACT_DIR        kept post-mortem artifacts (default /tmp/pj-e2e-layout-artifacts/<timestamp>)\n'
    printf '  E2E_REQUIRE_PJ4_FLAGS   1 (default) = a PJ4 build without the stage-5 flags/gui-test\n'
    printf '                          fails preflight; 0 = downgrade steps h/i to SKIPPED-pending\n'
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
  # Trap FIRST: even a mktemp failure must still print the verdict line
  # (cleanup guards an empty E2E_SCRATCH).
  trap cleanup EXIT
  E2E_SCRATCH="$(mktemp -d /tmp/pj-e2e-layout.XXXXXX)"
  mkdir -p "${ARTIFACT_DIR}"
  log "stage-5 layout-import E2E harness starting (repo ${REPO_ROOT}, scratch ${E2E_SCRATCH})"
  log "PJ4 build ${PJ4_BUILD} · post-mortem artifacts -> ${ARTIFACT_DIR}"

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
