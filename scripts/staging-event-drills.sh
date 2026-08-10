#!/usr/bin/env bash
# staging-event-drills.sh — Phase-4 burn-in drills (design 2026-07-30 §9).
#
#   staging-event-drills.sh freshness --bucket B --region R \
#       [--db /tmp/pj-cloud-catalog.db] [--timeout 120]
#
# freshness: upload a fixture under a drill Hive key -> assert the catalog row
# appears (event-tier latency), delete the object -> assert the row is
# removed. PASS/FAIL on stdout with measured latencies; nonzero exit on FAIL.
#
# Prereq: a builder running with --sqs-url against the same bucket+db
# (MCAP_CATALOG_SQS_URL=<url> ./run.sh --aws). The kill-9 and
# notification-outage drills are documented procedures in
# server/deploy/README.md ("Burn-in drills") — they need operator timing.
set -euo pipefail

CMD="${1:-}"; shift || true
BUCKET="" REGION="" DB="/tmp/pj-cloud-catalog.db" TIMEOUT=120
while [ $# -gt 0 ]; do
  case "$1" in
    --bucket)  BUCKET="$2"; shift 2 ;;
    --region)  REGION="$2"; shift 2 ;;
    --db)      DB="$2"; shift 2 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
if [ "$CMD" != freshness ] || [ -z "$BUCKET" ] || [ -z "$REGION" ]; then
  echo "usage: $0 freshness --bucket B --region R [--db PATH] [--timeout S]" >&2
  exit 2
fi

# python3 + stdlib sqlite3, read-only URI — the repo convention (run.sh
# db_query, sabotage-check.sh); no sqlite3-CLI dependency.
count() {
  python3 - "$DB" "$1" <<'PY'
import sqlite3, sys
try:
    conn = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
    print(conn.execute("SELECT COUNT(*) FROM files WHERE filename=?",
                       (sys.argv[2],)).fetchone()[0])
except Exception:
    print(0)
PY
}

wait_count() { # $1=filename $2=want -> prints elapsed seconds, or fails
  local t0 now
  t0=$(date +%s)
  while :; do
    if [ "$(count "$1")" = "$2" ]; then
      now=$(date +%s); echo $((now - t0)); return 0
    fi
    now=$(date +%s)
    [ $((now - t0)) -ge "$TIMEOUT" ] && return 1
    sleep 1
  done
}

expect_count() { # $1=filename $2=want $3=pass-label $4=fail-label
  local t
  if t=$(wait_count "$1" "$2"); then
    echo "PASS: $3 in ${t}s"
  else
    echo "FAIL: $4 within ${TIMEOUT}s"; exit 1
  fi
}

[ -f "$DB" ] || { echo "FAIL: catalog DB not found at $DB (is the builder running?)" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
if [ ! -x "$REPO_ROOT/server/bin/gen-ci-fixtures" ]; then
  (cd "$REPO_ROOT/server" && go build -o bin/gen-ci-fixtures ./cmd/gen-ci-fixtures)
fi
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
"$REPO_ROOT/server/bin/gen-ci-fixtures" -out "$TMP"
F=$(find "$TMP" -name '*.mcap' | head -1)
[ -n "$F" ] || { echo "FAIL: fixture generation produced no .mcap" >&2; exit 1; }
FN="drill-$(date +%s).mcap"
K="customer=drill/customer_site=lab/robot=r1/source=ros-bags/date=$(date -u +%F)/$FN"

echo "== upload s3://$BUCKET/$K"
aws s3 cp "$F" "s3://$BUCKET/$K" --region "$REGION" --only-show-errors
expect_count "$FN" 1 "row appeared" "row did not appear"

echo "== delete s3://$BUCKET/$K"
aws s3 rm "s3://$BUCKET/$K" --region "$REGION" --only-show-errors
expect_count "$FN" 0 "row removed" "row not removed"
echo "FRESHNESS DRILL PASS"
