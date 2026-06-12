#!/usr/bin/env bash
# run.sh — start the connector's Go server for a named TARGET and (interactively)
# launch PlotJuggler against it.
#
#   ./run.sh                    same as --dexory_minio
#   ./run.sh --dexory_minio     LOCAL: Minio (S3) + synthetic recordings + server on :8080.
#   ./run.sh --dexory_aws       Dexory staging bucket on AWS S3 (config.dexory-staging.yaml, :8084).
#   ./run.sh --asensus_google   Asensus bucket on Google Cloud Storage (config.asensus-staging.yaml).
#   ./run.sh <path/to.yaml>     power user: any S3/GCS server config file.
#
#   Add --no-gui to bring up the backend only (no PlotJuggler). The GUI is also
#   skipped automatically when run non-interactively (output piped/redirected).
#
# Idempotent: if the target's server is ALREADY running it is reused; if a server
# for a DIFFERENT target is running it is stopped and replaced.
# Stop: make server-stop   (LOCAL also: cd infra/minio && docker compose down)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="$HOME/.local/go/bin:$HOME/go/bin:$PATH"
command -v go >/dev/null || { echo "ERROR: 'go' not on PATH (expected $HOME/.local/go/bin/go)"; exit 1; }

BIN="$ROOT/server/bin"; DEPLOY="$ROOT/server/deploy"
PIDFILE=/tmp/pj-cloud-server.pid
LOGFILE=/tmp/pj-cloud-server.log

need() { [ -x "$BIN/$1" ] || ( echo "    (building server/bin/$1)"; cd "$ROOT/server" && go build -o "bin/$1" "./cmd/$1" ); }
port_up() { ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE ":${1#:}$"; }
usage() { sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; }

stop_other_server() {
  if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
    echo "    a server for another target is running (pid $(cat "$PIDFILE")) — stopping it..."
    make -C "$ROOT" server-stop >/dev/null 2>&1 || { kill "$(cat "$PIDFILE")" 2>/dev/null || true; rm -f "$PIDFILE"; }
    sleep 1
  fi
}
start_server() {  # args = pj-cloud-server flags. Detached so it survives this script / the GUI.
  setsid "$BIN/pj-cloud-server" "$@" >"$LOGFILE" 2>&1 < /dev/null &
  echo $! >"$PIDFILE"; sleep 1.5
  kill -0 "$(cat "$PIDFILE")" 2>/dev/null || { echo "ERROR: server exited immediately — see $LOGFILE:"; tail -5 "$LOGFILE"; exit 1; }
}

launch_app() {  # $1 = port like :8084
  local url="ws://localhost${1}"
  if [ "$NO_GUI" = 1 ] || ! [ -t 1 ]; then
    cat <<EOF

  Backend ready:  $url   (log: $LOGFILE)
  Headless check: plugin/toolbox_dexory_cloud/build/bin/dexory-cloud-cli --url $url list
  GUI:            run './run.sh ${TARGET}' in a desktop terminal (this run was non-interactive/--no-gui)
  Stop:           make server-stop
EOF
    return
  fi
  if [ ! -x "$ROOT/PJ4/build/pj_app/pj_app" ]; then
    echo "  Backend up at $url, but the GUI isn't built — run ./build.sh, then re-run."; return
  fi
  if [ -z "${DISPLAY:-}${WAYLAND_DISPLAY:-}" ]; then
    echo "  Backend up at $url; no display detected — connect a client, or run on a desktop."; return
  fi
  echo
  echo "==> Backend ready at $url.  Launching PlotJuggler —"
  echo "    open the 'Dexory Cloud' panel and connect to  $url"
  exec "$ROOT/PJ4/run.sh"
}

# ---------------- parse args ----------------
TARGET=""; NO_GUI=0
for a in "$@"; do
  case "$a" in
    --no-gui|--headless) NO_GUI=1 ;;
    -h|--help) usage; exit 0 ;;
    *) if [ -z "$TARGET" ]; then TARGET="$a"; else echo "ERROR: unexpected argument '$a'"; usage; exit 2; fi ;;
  esac
done
TARGET="${TARGET:---dexory_minio}"

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
  LISTEN="$(grep -E '^[[:space:]]*listen:' "$CONFIG" | grep -oE ':[0-9]+' | head -1)"; LISTEN="${LISTEN:-:8080}"
  if port_up "$LISTEN"; then
    echo "==> Target '$TARGET': backend already up on $LISTEN — reusing it."
  else
    stop_other_server
    echo "==> Target '$TARGET' -> bucket in $CONFIG (no local Minio).  Credentials: $CRED_HINT"
    start_server -config "$CONFIG"
    echo "    indexing the bucket (a cold real bucket's first scan can take a while)..."
    for _ in $(seq 1 30); do grep -q 'pj-cloud-server listening' "$LOGFILE" 2>/dev/null && break; sleep 2; done
    grep -q 'pj-cloud-server listening' "$LOGFILE" 2>/dev/null && echo "    ready." || echo "    still scanning (watch $LOGFILE for 'listening')."
  fi
  launch_app "$LISTEN"
  exit 0
fi

# ---------------- LOCAL (--dexory_minio) ----------------
if port_up :8080; then
  echo "==> --dexory_minio: backend already up on :8080 — reusing it."
else
  command -v docker >/dev/null || { echo "ERROR: docker not found — install Docker, or use a cloud target (./run.sh --dexory_aws)"; exit 1; }
  need seed; need gen-ci-fixtures; need gen-3d-fixture
  stop_other_server
  echo "==> [1/3] Starting Minio (S3) on :9000  (console :9001, admin/password123)"
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
  echo "==> [2/3] Seeding synthetic recordings (skipped if the bucket already has data)"
  if "$BIN/seed" -check >/dev/null; then
    SEED_DIR="$(mktemp -d)"; trap 'rm -rf "$SEED_DIR"' EXIT
    "$BIN/gen-ci-fixtures" --out "$SEED_DIR" >/dev/null
    "$BIN/gen-3d-fixture"  --out "$SEED_DIR" >/dev/null
    "$BIN/seed" -dir "$SEED_DIR"
  else
    echo "    bucket already has recordings — leaving it as-is"
  fi
  echo "==> [3/3] Starting the Go server on :8080  (default config -> local Minio; no credentials needed)"
  start_server -listen :8080
fi
launch_app :8080
