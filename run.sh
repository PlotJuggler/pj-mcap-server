#!/usr/bin/env bash
# run.sh — start the connector's TWO-PROCESS backend for a named TARGET:
#   the Python `mcap_catalog` builder (mcap_catalog/ — the SOLE catalog writer
#   + tag-edit IPC server) and the Go server (a pure read-only catalog reader
#   + unchanged streamer, M6 cutover). The Go server refuses to start until the
#   builder has PUBLISHED a catalog DB — build_outcome 'ok' (clean) OR
#   'partial' (some files quarantined into catalog_failures, the rest served
#   normally — BY DESIGN, see wait_catalog_built) both count — so bring-up
#   always starts the builder FIRST and waits for its initial build before
#   starting the server.
#
#   ./run.sh                    same as --dexory_minio
#   ./run.sh --dexory_minio     LOCAL: Minio (S3) + synthetic recordings + builder + server on :8080.
#   ./run.sh --dexory_aws       Dexory staging bucket on AWS S3 (config.dexory-staging.yaml, :8084).
#   ./run.sh --asensus_google   Asensus bucket on Google Cloud Storage (config.asensus-staging.yaml).
#   ./run.sh <path/to.yaml>     power user: any S3/GCS server config file.
#
# Idempotent: if the target's server is ALREADY running it is reused (and its
# catalog builder's liveness is checked too — a dead builder under a live
# server is repaired in place instead of silently staying dead); if the SAME
# target's builder is still doing its (possibly slow, real-bucket) first
# build with the server not up yet, that builder is reused rather than
# restarted from scratch; if a DIFFERENT target's backend is running, it
# (both the server AND its builder) is stopped and replaced.
#
# Stop: make server-stop   (reaps BOTH the Go server and the Python builder daemon;
#                            LOCAL also: cd infra/minio && docker compose down)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="$HOME/.local/go/bin:$HOME/go/bin:$PATH"
command -v go >/dev/null || { echo "ERROR: 'go' not on PATH (expected $HOME/.local/go/bin/go)"; exit 1; }

BIN="$ROOT/server/bin"; DEPLOY="$ROOT/server/deploy"; MCAP_CATALOG_DIR="$ROOT/mcap_catalog"
PIDFILE=/tmp/pj-cloud-server.pid
LOGFILE=/tmp/pj-cloud-server.log
BUILDER_PIDFILE=/tmp/pj-cloud-builder.pid
BUILDER_LOGFILE=/tmp/pj-cloud-builder.log
# BUILDER_STATE records which TARGET the currently-running builder belongs to
# (line 1) and a hash of the exact args it was launched with (line 2 — see
# builder_state_hash/ensure_builder), so a re-run for the SAME target (e.g.
# retrying after a slow real-bucket cold scan) reuses it instead of killing
# and restarting the scan from zero, UNLESS the args have drifted.
BUILDER_STATE=/tmp/pj-cloud-builder.target
TAG_SOCKET=/tmp/pj-cloud-tag.sock
VENV_PY="$HOME/.venvs/pj-catalog/bin/python3"

need() { [ -x "$BIN/$1" ] || ( echo "    (building server/bin/$1)"; cd "$ROOT/server" && go build -o "bin/$1" "./cmd/$1" ); }
need_venv() {
  [ -x "$VENV_PY" ] || {
    echo "ERROR: catalog-builder venv not found at $VENV_PY"
    echo "  Bootstrap it once (pins match CI/scripts/smoke.sh):"
    echo "    python3 -m venv $HOME/.venvs/pj-catalog"
    echo "    $VENV_PY -m pip install boto3==1.43.40 google-cloud-storage==3.12.0 mcap==1.4.0 watchdog==6.0.0"
    exit 1
  }
}
port_up() { ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE ":${1#:}$"; }

# our_server_up PORT — true iff OUR pj-cloud-server owns PORT: the pidfile must
# name a live process. A bare port probe is NOT enough for the reuse branches —
# any foreign listener (live-caught: a stray Tomcat on :8080) would satisfy it
# and run.sh would print "Backend ready" with no backend at all. A dead/absent
# pidfile while the port IS occupied is therefore a hard error, not a reuse.
our_server_up() {
  if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    return 0
  fi
  if port_up "$1"; then
    echo "ERROR: something else is listening on ${1} (not pj-cloud-server — its"
    echo "       pidfile $PIDFILE is absent or names a dead process)."
    echo "       Free the port or change 'listen' in the target config, then re-run."
    exit 1
  fi
  return 1
}
usage() { sed -n '2,27p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

# yaml_val FILE KEY — first "key: value" scalar in FILE, quotes/comments stripped;
# empty (not an error) if KEY is absent — under `set -e`/`pipefail` a plain
# `grep | sed...` pipeline would abort the whole script on a merely-absent key
# (e.g. GCS configs have no top-level `region:`), so the grep's own exit
# status is swallowed with `|| true` before piping onward.
# Good enough for the flat storage.{s3,gcs} blocks in server/deploy/config.*.yaml
# (mirrors the existing `listen:` grep below, just generalized to any key).
yaml_val() {
  { grep -E "^[[:space:]]*${2}:" "$1" 2>/dev/null || true; } | head -1 \
    | sed -E "s/^[[:space:]]*${2}:[[:space:]]*//" \
    | sed -E 's/[[:space:]]+#.*$//' \
    | sed -E 's/^"(.*)"$/\1/; s/^'"'"'(.*)'"'"'$/\1/' \
    | sed -E 's/[[:space:]]+$//'
}

# parse_storage_config CONFIG — sets STORAGE_KIND/BUCKET/PREFIX/REGION/ENDPOINT/
# ACCESSKEY/SECRETKEY/CREDFILE. ACCESSKEY/SECRETKEY are ONLY present for a
# power-user config that inlines static creds (e.g. a self-hosted S3 with a
# fixed key pair, or dev Minio) — the committed dexory-staging/asensus-staging
# configs deliberately leave them empty/absent so both the Go server AND (via
# these) the builder fall back to the AWS default credential chain / ADC.
parse_storage_config() {
  STORAGE_KIND=s3; BUCKET=""; PREFIX=""; REGION=""; ENDPOINT=""; ACCESSKEY=""; SECRETKEY=""; CREDFILE=""
  grep -qE '^[[:space:]]*gcs:' "$1" && STORAGE_KIND=gcs
  BUCKET="$(yaml_val "$1" bucket)"
  PREFIX="$(yaml_val "$1" prefix)"
  REGION="$(yaml_val "$1" region)"
  ENDPOINT="$(yaml_val "$1" endpoint)"
  ACCESSKEY="$(yaml_val "$1" access_key)"
  SECRETKEY="$(yaml_val "$1" secret_key)"
  CREDFILE="$(yaml_val "$1" credentials_file)"
}

# builder_state_target / builder_state_hash — read line 1 (target name) / line
# 2 (a hash of the exact builder args last used for it) of BUILDER_STATE. The
# hash lets a same-target re-run detect "same target, but the args this
# invocation would use have drifted" (bucket/prefix/db path changed under an
# unchanged target name) instead of silently reusing a builder that no longer
# matches what was asked for — see ensure_builder.
builder_state_target() { [ -f "$BUILDER_STATE" ] && sed -n '1p' "$BUILDER_STATE" 2>/dev/null || true; }
builder_state_hash()   { [ -f "$BUILDER_STATE" ] && sed -n '2p' "$BUILDER_STATE" 2>/dev/null || true; }

stop_other_server() {
  if { [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; } \
     || { [ -f "$BUILDER_PIDFILE" ] && kill -0 "$(cat "$BUILDER_PIDFILE")" 2>/dev/null; }; then
    echo "    a backend for another target is running — stopping it (server + catalog builder)..."
    local old_server_pid old_builder_pid
    old_server_pid="$([ -f "$PIDFILE" ] && cat "$PIDFILE" 2>/dev/null || true)"
    old_builder_pid="$([ -f "$BUILDER_PIDFILE" ] && cat "$BUILDER_PIDFILE" 2>/dev/null || true)"
    make -C "$ROOT" server-stop || {
      [ -n "$old_server_pid" ] && { kill "$old_server_pid" 2>/dev/null || true; }
      [ -n "$old_builder_pid" ] && { kill "$old_builder_pid" 2>/dev/null || true; }
    }
    rm -f "$PIDFILE" "$BUILDER_PIDFILE" "$BUILDER_STATE"
    # Wait for the OLD processes to actually die (a signal is not a
    # guarantee) — a new builder binding the tag socket, or a new server
    # binding the port, must never race the old one's own unlink/unbind.
    local waited=0
    while { [ -n "$old_server_pid" ] && kill -0 "$old_server_pid" 2>/dev/null; } \
       || { [ -n "$old_builder_pid" ] && kill -0 "$old_builder_pid" 2>/dev/null; }; do
      waited=$((waited + 1))
      [ "$waited" -lt 50 ] || break   # ~5s at 0.1s polls
      sleep 0.1
    done
  fi
}

start_server() {  # args = pj-cloud-server flags. Detached so it survives this script / the GUI.
  setsid "$BIN/pj-cloud-server" "$@" >"$LOGFILE" 2>&1 < /dev/null &
  echo $! >"$PIDFILE"; sleep 1.5
  kill -0 "$(cat "$PIDFILE")" 2>/dev/null || { echo "ERROR: server exited immediately — see $LOGFILE:"; tail -5 "$LOGFILE"; exit 1; }
}

# DB_QUERY_PY is db_query's SQLite read-only lookup, factored into a variable
# (rather than inlined twice) so db_query can invoke it with or without
# stderr suppressed — see db_query's own comment for why that matters.
DB_QUERY_PY='
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
'

# db_query DB SQL [show-errors] — first row, tab-separated, of SQL against DB
# opened read-only (stdlib sqlite3; safe to run concurrently with the
# builder's own connection). stderr is suppressed by default — routine
# polling before the DB/table exists yet is EXPECTED to "fail" quietly, and
# every call site already treats a non-zero exit as "not ready yet, try
# again" — but a genuine error (bad SQL, permissions, a corrupt DB, ...) would
# look identical without a 3rd arg. Pass any non-empty 3rd arg to let stderr
# through instead; wait_catalog_built does this once, on a timeout, so a real
# failure isn't a total black box.
db_query() {
  if [ -n "${3:-}" ]; then
    python3 -c "$DB_QUERY_PY" "$1" "$2"
  else
    python3 -c "$DB_QUERY_PY" "$1" "$2" 2>/dev/null
  fi
}

# wait_catalog_built DB BUILDER_PID TIMEOUT_SECS — poll build_metadata until a
# PUBLISHED build (build_outcome='ok', i.e. every file parsed cleanly, OR
# 'partial', i.e. some files were quarantined into catalog_failures but the
# rest published normally) lands, or the builder dies, or we time out. A
# partial build is fully servable — quarantining unparseable files rather
# than blocking the whole catalog on them is the DESIGN
# (mcap_catalog/CATALOG_CONTRACT.md), not a failure state, so it is treated
# the same as 'ok' for "is the catalog ready" purposes and only surfaced as a
# printed warning.
wait_catalog_built() {
  local db="$1" builder_pid="$2" timeout="$3" waited=0
  local sql="SELECT files_scanned, files_failed, build_outcome FROM build_metadata WHERE id=1"
  while true; do
    kill -0 "$builder_pid" 2>/dev/null || {
      echo "ERROR: catalog builder (pid $builder_pid) died while waiting for the initial build — see $BUILDER_LOGFILE:"
      tail -n 40 "$BUILDER_LOGFILE" || true
      return 1
    }
    local row
    if row="$(db_query "$db" "$sql")" && [ -n "$row" ]; then
      local scanned failed outcome
      IFS=$'\t' read -r scanned failed outcome <<<"$row"
      if [ "$outcome" = "ok" ] || [ "$outcome" = "partial" ]; then
        echo "    catalog built: ${scanned} file(s) scanned, ${failed} failed"
        if [ "$outcome" = "partial" ]; then
          echo "    WARNING: ${failed} file(s) failed to catalog and were quarantined (see the"
          echo "    catalog_failures table) — the rest of the catalog published normally and is"
          echo "    fully servable; this is expected behavior, not a build failure."
        fi
        return 0
      fi
    fi
    sleep 1; waited=$((waited + 1))
    [ "$waited" -lt "$timeout" ] || {
      echo "    timed out after ${timeout}s waiting for the catalog build — re-running the last"
      echo "    check with errors visible (in case the query itself is the problem):"
      db_query "$db" "$sql" show-errors || true
      return 1
    }
  done
}

# ensure_builder TARGET DB_PATH TIMEOUT_SECS -- BUILDER_ARGS...
# Starts (or, for a same-target retry with the SAME args, REUSES) the catalog
# builder daemon, then blocks until its initial catalog build completes or
# TIMEOUT_SECS elapses.
#
# NEVER deletes DB_PATH (or its -wal/-shm sidecars): a pre-existing DB at this
# path may hold tags_override rows (real user tag edits) from an earlier run,
# and the Python builder's publish path is atomic + etag-skip-aware, so
# reusing it just warm-starts (zero re-extracts for files whose
# (etag,size,last_modified) haven't changed) instead of throwing tag edits
# away for no reason. The only thing this function ever removes is the
# tag-edit IPC socket (stale from an unclean previous exit).
ensure_builder() {
  local target="$1" db_path="$2" timeout="$3"; shift 3
  local args_hash
  args_hash="$(printf '%s\0' "$@" | sha256sum | cut -d' ' -f1)"
  local prev_target prev_hash
  prev_target="$(builder_state_target)"
  prev_hash="$(builder_state_hash)"
  local builder_alive=0
  [ -f "$BUILDER_PIDFILE" ] && kill -0 "$(cat "$BUILDER_PIDFILE")" 2>/dev/null && builder_alive=1
  local reuse=0
  if [ "$builder_alive" = "1" ] && [ "$prev_target" = "$target" ]; then
    if [ "$prev_hash" = "$args_hash" ]; then
      reuse=1
    else
      echo "    WARNING: a catalog builder is already running for target '$target' (pid"
      echo "    $(cat "$BUILDER_PIDFILE")) but with DIFFERENT arguments than this run would use"
      echo "    (bucket/prefix/db path/etc. changed under the same target name) — restarting"
      echo "    it with the current arguments."
    fi
  fi
  if [ "$reuse" = "1" ]; then
    echo "    catalog builder already running for this target (pid $(cat "$BUILDER_PIDFILE")) — reusing it."
  else
    need_venv
    if [ "$builder_alive" = "1" ]; then
      # The args-mismatch case above found one alive for this same target —
      # never let two builder processes hold the same DB open for writing.
      kill "$(cat "$BUILDER_PIDFILE")" 2>/dev/null || true
      local w=0
      while kill -0 "$(cat "$BUILDER_PIDFILE")" 2>/dev/null; do
        w=$((w + 1)); [ "$w" -lt 50 ] || break; sleep 0.1
      done
    fi
    if [ -n "$prev_target" ] && [ "$prev_target" != "$target" ] && [ -s "$db_path" ]; then
      echo "    NOTE: $db_path already holds a catalog built for target '$prev_target'; switching"
      echo "    to '$target' over the SAME db file — the builder's initial reconcile will"
      echo "    resweep against the new bucket (rows for the old target's objects are pruned"
      echo "    as part of that same reconcile, not deleted up front)."
    fi
    rm -f "$TAG_SOCKET"
    : > "$BUILDER_LOGFILE"
    ( cd "$MCAP_CATALOG_DIR" && exec setsid "$VENV_PY" -m mcap_catalog_builder "$@" ) >>"$BUILDER_LOGFILE" 2>&1 < /dev/null &
    echo $! >"$BUILDER_PIDFILE"
    printf '%s\n%s\n' "$target" "$args_hash" >"$BUILDER_STATE"
    sleep 1
    kill -0 "$(cat "$BUILDER_PIDFILE")" 2>/dev/null || { echo "ERROR: catalog builder exited immediately — see $BUILDER_LOGFILE:"; tail -10 "$BUILDER_LOGFILE"; exit 1; }
  fi
  echo "    waiting for the initial catalog build (up to ${timeout}s)..."
  wait_catalog_built "$db_path" "$(cat "$BUILDER_PIDFILE")" "$timeout"
}

# cloud_builder_args CONFIG — sets DB_PATH + BARGS (and exports the storage
# credentials into the environment) for the Python builder against the
# S3/GCS target described by CONFIG. Shared by the fresh-start path and the
# "server up, builder dead" repair path below, so both ever start the SAME
# builder for a given CONFIG.
cloud_builder_args() {
  local config="$1"
  DB_PATH="$(yaml_val "$config" db_path)"; DB_PATH="${DB_PATH:-/tmp/pj-cloud-catalog.db}"
  parse_storage_config "$config"
  [ -n "$BUCKET" ] || { echo "ERROR: could not determine storage.{s3,gcs}.bucket from $config"; exit 1; }
  if [ "$STORAGE_KIND" = gcs ]; then
    [ -n "$CREDFILE" ] && export GOOGLE_APPLICATION_CREDENTIALS="$CREDFILE"
    BARGS=(--source gcs --gcs-bucket "$BUCKET")
    [ -n "$PREFIX" ] && BARGS+=(--gcs-prefix "$PREFIX")
  else
    [ -n "$ENDPOINT" ] && export AWS_ENDPOINT_URL="$ENDPOINT"
    if [ -n "$REGION" ]; then export AWS_REGION="$REGION" AWS_DEFAULT_REGION="$REGION"; fi
    # Static creds ONLY if the config inlines them (power-user / self-hosted
    # S3); absent (the dexory-staging/asensus-staging norm) means fall back
    # to whatever AWS credential chain is already in the environment
    # (AWS_PROFILE, exported above for --dexory_aws; ~/.aws/credentials; IAM role).
    [ -n "$ACCESSKEY" ] && export AWS_ACCESS_KEY_ID="$ACCESSKEY"
    [ -n "$SECRETKEY" ] && export AWS_SECRET_ACCESS_KEY="$SECRETKEY"
    BARGS=(--source s3 --s3-bucket "$BUCKET")
    [ -n "$PREFIX" ] && BARGS+=(--s3-prefix "$PREFIX")
  fi
  BARGS+=(--no-watch --tag-socket "$TAG_SOCKET" --db "$DB_PATH" --rescan-interval 300 --log-level INFO)
}

# start_local_builder — ensures the Minio-backed catalog builder for
# --dexory_minio is running. Called from both the fresh-start path and the
# "server up, builder dead" repair path, so both ever start the SAME builder.
start_local_builder() {
  export AWS_ENDPOINT_URL="http://localhost:9000" AWS_ACCESS_KEY_ID="admin" AWS_SECRET_ACCESS_KEY="password123"
  export AWS_REGION="us-east-1" AWS_DEFAULT_REGION="us-east-1"
  ensure_builder "$TARGET" /tmp/pj-cloud-catalog.db 60 \
    --source s3 --s3-bucket recordings --no-watch \
    --tag-socket "$TAG_SOCKET" --db /tmp/pj-cloud-catalog.db --rescan-interval 300 --log-level INFO
}

launch_app() {  # $1 = port like :8084
  local url="ws://localhost${1}"
  cat <<EOF

  Backend ready:  $url   (server log: $LOGFILE, builder log: $BUILDER_LOGFILE)
  CLI check:      plugin/toolbox_dexory_cloud/build/bin/dexory-cloud-cli --url $url list
  Stop:           make server-stop   (reaps both the server and the catalog builder)
EOF
}

# ---------------- parse args ----------------
TARGET=""
for a in "$@"; do
  case "$a" in
    -h|--help) usage; exit 0 ;;
    *) if [ -z "$TARGET" ]; then TARGET="$a"; else echo "ERROR: unexpected argument '$a'"; usage; exit 2; fi ;;
  esac
done
TARGET="${TARGET:---dexory_minio}"

# Is a builder from a PREVIOUS invocation already working on this SAME target
# (server not up yet — e.g. mid cold-scan on a real bucket)? If so, the
# per-mode blocks below skip stop_other_server and let ensure_builder reuse it.
SAME_TARGET_BUILDING=0
if [ "$(builder_state_target)" = "$TARGET" ] \
   && [ -f "$BUILDER_PIDFILE" ] && kill -0 "$(cat "$BUILDER_PIDFILE")" 2>/dev/null; then
  SAME_TARGET_BUILDING=1
fi

# ---------------- target -> MODE / CONFIG / CRED_HINT ----------------
MODE=local; CONFIG=""; CRED_HINT=""
case "$TARGET" in
  --dexory_minio) MODE=local ;;
  --dexory_aws)
    MODE=cloud; CONFIG="$DEPLOY/config.dexory-staging.yaml"
    export AWS_PROFILE="${AWS_PROFILE:-dexory-staging}"
    CRED_HINT="AWS_PROFILE=$AWS_PROFILE (or AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY)" ;;
  --asensus_google)
    MODE=cloud; CONFIG="$DEPLOY/config.asensus-staging.yaml"
    CRED_HINT="Application Default Credentials (gcloud auth application-default login)" ;;
  --*) echo "ERROR: unknown target '$TARGET'"; echo; usage; exit 2 ;;
  *)   MODE=cloud; CONFIG="$TARGET"; CRED_HINT="your cloud-provider environment credentials" ;;
esac

need pj-cloud-server

# ---------------- CLOUD (real S3 / GCS bucket) ----------------
if [ "$MODE" = cloud ]; then
  [ -f "$CONFIG" ] || { echo "ERROR: config not found: $CONFIG"; exit 1; }
  if grep -q 'REPLACE_ME' "$CONFIG"; then
    echo "==> Target '$TARGET' is not wired up yet — edit $CONFIG (set storage.*.bucket + prefix),"
    echo "    supply credentials ($CRED_HINT), then re-run."
    exit 0
  fi
  LISTEN="$(yaml_val "$CONFIG" listen | grep -oE ':[0-9]+' | head -1 || true)"; LISTEN="${LISTEN:-:8080}"
  if our_server_up "$LISTEN"; then
    echo "==> Target '$TARGET': backend already up on $LISTEN — reusing it."
    if [ -f "$BUILDER_PIDFILE" ] && kill -0 "$(cat "$BUILDER_PIDFILE")" 2>/dev/null; then
      : # catalog builder still alive — nothing to do
    else
      echo "    WARNING: the server is up but its catalog builder is NOT running — tag edits"
      echo "    and catalog freshness have silently stopped working. Restarting just the"
      echo "    builder for '$TARGET'..."
      cloud_builder_args "$CONFIG"
      ensure_builder "$TARGET" "$DB_PATH" 1200 "${BARGS[@]}"
    fi
  else
    [ "$SAME_TARGET_BUILDING" = "1" ] || stop_other_server
    echo "==> Target '$TARGET' -> bucket in $CONFIG (no local Minio).  Credentials: $CRED_HINT"

    cloud_builder_args "$CONFIG"
    echo "==> [1/2] Starting the Python catalog builder against ${STORAGE_KIND}://${BUCKET}${PREFIX:+/$PREFIX}"
    echo "    (a cold real-bucket first scan can take a while — bounded to 20 minutes here)"

    if ensure_builder "$TARGET" "$DB_PATH" 1200 "${BARGS[@]}"; then
      echo "==> [2/2] Starting the Go server on $LISTEN (read-only catalog reader)"
      start_server -config "$CONFIG" -db "$DB_PATH" -tag-ipc-socket "$TAG_SOCKET"
    else
      echo "ERROR: catalog builder did not complete its initial build within 20 minutes."
      echo "  It is still running in the background (pid $(cat "$BUILDER_PIDFILE" 2>/dev/null || echo '?'); log $BUILDER_LOGFILE)."
      echo "  Re-run './run.sh $TARGET' once you see it finish — it will be reused, not restarted."
      exit 1
    fi
  fi
  launch_app "$LISTEN"
  exit 0
fi

# ---------------- LOCAL (--dexory_minio) ----------------
if our_server_up :8080; then
  echo "==> --dexory_minio: backend already up on :8080 — reusing it."
  if [ -f "$BUILDER_PIDFILE" ] && kill -0 "$(cat "$BUILDER_PIDFILE")" 2>/dev/null; then
    : # catalog builder still alive — nothing to do
  else
    echo "    WARNING: the server on :8080 is up but its catalog builder is NOT running — tag"
    echo "    edits and catalog freshness have silently stopped working. Restarting just the"
    echo "    builder now (against the same Minio 'recordings' bucket)..."
    start_local_builder || { echo "ERROR: catalog builder did not complete its initial build within 60s — see $BUILDER_LOGFILE"; exit 1; }
  fi
else
  command -v docker >/dev/null || { echo "ERROR: docker not found — install Docker, or use a cloud target (./run.sh --dexory_aws)"; exit 1; }
  need seed; need gen-ci-fixtures; need gen-3d-fixture
  [ "$SAME_TARGET_BUILDING" = "1" ] || stop_other_server
  echo "==> [1/4] Starting Minio (S3) on :9000  (console :9001, admin/password123)"
  ( cd "$ROOT/infra/minio" && docker compose up -d )
  echo "    waiting for Minio + the bucket to be ready..."
  set +e; mready=0
  for _ in $(seq 1 40); do
    "$BIN/seed" -check >/dev/null 2>&1; rc=$?
    if [ "$rc" = "0" ] || [ "$rc" = "3" ]; then mready=1; break; fi
    sleep 1
  done
  set -e
  [ "$mready" = "1" ] || { echo "ERROR: Minio did not become ready on :9000 within 40s"; exit 1; }
  echo "==> [2/4] Seeding synthetic Hive-keyed recordings (skipped if the bucket already has data)"
  SEED_CHECK_ERR="$(mktemp)"
  seed_rc=0
  "$BIN/seed" -check >/dev/null 2>"$SEED_CHECK_ERR" || seed_rc=$?
  case "$seed_rc" in
    0)
      SEED_DIR="$(mktemp -d)"; trap 'rm -rf "$SEED_DIR"' EXIT
      # -hive: lay fixtures out under customer=/customer_site=/robot=/source=/date=
      # keys (the auryn builder only catalogs Hive-partitioned keys — a flat key
      # is recorded as a catalog_failures row, "unparseable key", and never appears
      # in the catalog at all).
      "$BIN/gen-ci-fixtures" -hive -out "$SEED_DIR" >/dev/null
      THREED_TMP="$(mktemp -d)"
      "$BIN/gen-3d-fixture" -out "$THREED_TMP" >/dev/null
      # date=2026-06-24 matches gen-ci-fixtures' bigHiveKey path (customer=test/
      # customer_site=lab/robot=r1/source=synthetic/date=2026-06-24/ci_synth_big.mcap),
      # which is written only when -hive-big is ALSO passed; this call passes
      # -hive alone, so DefaultSpecs (dated 2026-06-22/23) is everything that
      # actually lands there besides our own 3D fixture below — no collision.
      THREED_HIVE_DIR="$SEED_DIR/customer=test/customer_site=lab/robot=r1/source=synthetic/date=2026-06-24"
      mkdir -p "$THREED_HIVE_DIR"
      THREED_FILE="$(find "$THREED_TMP" -maxdepth 1 -name '*.mcap' | head -n1)"
      [ -n "$THREED_FILE" ] || { echo "ERROR: gen-3d-fixture produced no .mcap file under $THREED_TMP"; exit 1; }
      mv "$THREED_FILE" "$THREED_HIVE_DIR/"
      rm -rf "$THREED_TMP"
      "$BIN/seed" -dir "$SEED_DIR"
      ;;
    3)
      echo "    bucket already has recordings — leaving it as-is"
      ;;
    *)
      echo "ERROR: seed -check failed (exit $seed_rc):"
      sed 's/^/    /' "$SEED_CHECK_ERR" || true
      rm -f "$SEED_CHECK_ERR"
      exit 1
      ;;
  esac
  rm -f "$SEED_CHECK_ERR"
  echo "==> [3/4] Starting the Python catalog builder daemon (sole catalog writer + tag-edit IPC)"
  start_local_builder || { echo "ERROR: catalog builder did not complete its initial build within 60s — see $BUILDER_LOGFILE"; exit 1; }
  echo "==> [4/4] Starting the Go server on :8080  (read-only catalog reader; no credentials needed)"
  start_server -listen :8080 -db /tmp/pj-cloud-catalog.db -tag-ipc-socket "$TAG_SOCKET"
fi
launch_app :8080
