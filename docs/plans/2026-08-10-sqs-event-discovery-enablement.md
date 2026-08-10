# SQS Event-Discovery Enablement (Phases 3–6 + §7.1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the 2026-07-30 event-discovery design's remaining phases: turn the shipped-but-dark S3→SQS event tier ON against the staging bucket (Phases 3–4), implement the tier-2 hot-window scoped audit (Phase 5), implement nightly fixed-hour full audits + `catalog_failures` hygiene (Phase 6), and add the §7.1 server nicety (vanished object → `ERROR_NOT_FOUND`).

**Architecture:** All builder work stays inside `mcap_catalog/` per the design's scope (no schema change, no wire change). The hot audit is a NEW module (`hot_audit.py`) with a *mandatory scope* and per-prefix fail-closed coverage — never a filter on `full_reconcile`. Scheduling extends the existing single-arbiter `AuditCoordinator` (one audit queued-or-running at a time; full supersedes hot). The Go change is 3 files: a `storage.ErrNotFound` sentinel, dual-wrap in both classifiers, and a pure `planBuildErrorCode` helper at the plan-build error site.

**Tech Stack:** Python 3.11+ (builder; venv `~/.venvs/pj-catalog`), Go 1.23 (server), bash + aws CLI (staging infra), pytest / go test.


**EXECUTION AMENDMENTS (2026-08-10, during execution):**
1. **Runbook-only staging** (operator decision): the only AWS identity on the dev
   box is read-only (`dexory-s3-reader`), so Tasks 2/5/17's live-AWS halves are
   NOT executed. The setup/drill scripts land as tested artifacts + runbook;
   translator tests use SYNTHESIZED payloads for all three event families
   (marked as such); capturing real payloads is the documented follow-up.
2. **Codex consult verdict folded** (session 019feb32-8045-7950-88fd-19ea3967dc22):
   setup-script ownership guard made structural (empty-or-exactly-ours, converge
   attributes); `--prefix`/`--whole-bucket` made explicit; hot audit is S3-only
   (no `LocalSource.list_prefix` — local `intended_key` overrides break raw-key
   scoping); failure hygiene compares raw ∪ effective keys; hot tier gets capped
   backoff and zero-coverage-⇒-failed; fixed-hour honored for the non-immediate
   initial delay; `ErrNotFound` excludes bucket-absence (NoSuchBucket /
   ErrBucketNotExist stay plain ErrPermanent); systemd enablement via a drop-in
   example (ExecStart hardcodes --no-watch); sidecar keys `tag_edit_expired`/
   `tag_edit_failed` (design §6 names); registry derivation index-only DISTINCT
   (incremental in-memory registry REJECTED: staleness risk for ~100ms/30min).

**Authoritative design:** `docs/plans/2026-07-30-builder-event-discovery-design.md` (v2, Codex-reviewed, 19 findings folded). Section references (§3.5, §4.2, §5.2…) below point there. Do not re-litigate its decisions.

**Repo conventions that bind this plan:**
- Work in a fresh worktree `.worktrees/sqs-event-enablement` off `origin/main` (Task 0).
- Builder tests: `cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/ -q` (267 passed, 2 skipped at plan time).
- The documentation audit (Task 18) is a MERGE GATE, not a reminder.
- Per the team rule, get a Codex consult on this plan before executing it (`codex-exec` skill), and an adversarial review at the end.

**Honest compression note:** the design's Phases 4→5→6 assume multi-day burn-in gaps. This plan performs a *smoke-level* staging validation of each phase; the multi-day burn-in (DLQ observation, lifecycle-payload capture, prod enablement) remains an ops activity documented in `server/deploy/README.md` and is explicitly NOT claimed done by this plan.

**Operator inputs required before Tasks 2/5/17 (staging):**
- `PJ_STAGING_BUCKET` — the real staging S3 bucket name (the committed `server/deploy/config.aws-staging.yaml` ships `REPLACE_ME`; fill it locally, never commit real values).
- `PJ_STAGING_REGION` — the bucket's region.
- Working AWS credentials (`aws sts get-caller-identity` must succeed) with `sqs:CreateQueue/SetQueueAttributes/GetQueueAttributes/ReceiveMessage/DeleteMessage/PurgeQueue` and `s3:GetBucketNotification/PutBucketNotification/PutObject/DeleteObject/PutLifecycleConfiguration` on the staging resources.

---

## File structure (what gets created/modified)

**Created:**
- `scripts/staging-sqs-setup.sh` — idempotent staging queue/DLQ/notification provisioning (merge-guarded).
- `scripts/staging-event-drills.sh` — the scripted `freshness` burn-in drill.
- `server/deploy/docker-compose.aws.events.yml` — Phase-4 overlay (consumer ON).
- `mcap_catalog/mcap_catalog_builder/hot_audit.py` — tier-2 scoped audit (registry, scoped LIST orchestration, scoped sweep).
- `mcap_catalog/mcap_catalog_builder/tests/test_hot_audit.py`
- `mcap_catalog/mcap_catalog_builder/tests/data/s3_events/*.json` — captured real AWS payloads (sanitized).
- `server/internal/ws/session_error_code_test.go`

**Modified:**
- `mcap_catalog/mcap_catalog_builder/s3_storage.py` — `S3Source.list_prefix`.
- `mcap_catalog/mcap_catalog_builder/storage.py` — `LocalSource.list_prefix`.
- `mcap_catalog/mcap_catalog_builder/reconcile.py` — extract `_run_extract_apply` helper; `catalog_failures` hygiene.
- `mcap_catalog/mcap_catalog_builder/__main__.py` — `AuditItem.audit_kind`, coordinator hot/fixed-hour scheduling, worker dispatch, CLI flags.
- `mcap_catalog/mcap_catalog_builder/status.py` — `hot_audit_finished`, `maintenance_window`, tag-edit counters, `failures_pruned`.
- `mcap_catalog/mcap_catalog_builder/tag_ipc.py` — telemetry hooks in `handle_tag_edit`.
- `mcap_catalog/mcap_catalog_builder/tests/{test_audit_scheduler,test_s3_producer,test_reconcile,test_status,test_cli}.py` — additions.
- `run.sh` — `MCAP_CATALOG_SQS_URL` support for cloud targets.
- `server/internal/storage/{storage.go,s3.go,gcsreader.go}` + tests — `ErrNotFound`.
- `server/internal/ws/handlers_session.go` — `planBuildErrorCode`.
- Docs per Task 18.

---

### Task 0: Worktree + branch + plan-review gate

**Files:** none (setup only)

- [ ] **Step 1: Fetch and create the worktree** (global convention: worktrees live in `.worktrees/`, new feature = new branch off `origin/main`)

```bash
cd /home/davide/ws_plotjuggler/mcap_server
git fetch origin
git worktree add .worktrees/sqs-event-enablement -b sqs-event-enablement origin/main
cd .worktrees/sqs-event-enablement
```

Expected: new worktree on branch `sqs-event-enablement`. (The stale `.worktrees/builder-event-discovery` from PR #17 exists — leave it alone.)

- [ ] **Step 2: Baseline the test suites**

```bash
cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/ -q; cd ..
cd server && go test ./... > /dev/null && echo GO-OK; cd ..
```

Expected: `267 passed, 2 skipped` and `GO-OK`. If not, STOP — the baseline is broken, fix `main` first.

- [ ] **Step 3: Codex consult on this plan** (team rule). Use the `codex-exec` skill: hand it this plan file + the design doc, ask for blocking objections only. Fold accepted findings before continuing.

---

### Task 1: Phase 3 — `scripts/staging-sqs-setup.sh`

Idempotent provisioning per design §3.1: DLQ + queue (retention 4 d, visibility 300 s, redrive maxReceiveCount 5), queue policy for `s3.amazonaws.com`, bucket notifications for the three event families with `suffix=.mcap` — **merge-guarded**: it refuses to touch a bucket whose existing notification configuration it does not own.

**Files:**
- Create: `scripts/staging-sqs-setup.sh`

- [ ] **Step 1: Write the script**

```bash
#!/usr/bin/env bash
# staging-sqs-setup.sh — provision the S3->SQS event-discovery infra (design
# 2026-07-30 §3.1) for ONE bucket, idempotently.
#
#   staging-sqs-setup.sh --bucket <name> --region <region> [--prefix <p>] \
#                        [--name-base pj-cloud-catalog] [--dry-run]
#
# Creates <name-base>-events + <name-base>-dlq (retention 4 days, visibility
# 300 s, redrive after 5 receives), grants s3.amazonaws.com SendMessage from
# the bucket ARN, and installs the bucket notification configuration
# (ObjectCreated:* / ObjectRemoved:* / LifecycleExpiration:* filtered to
# suffix=.mcap [+ prefix]). FAIL-CLOSED: an existing notification
# configuration this script did not write aborts the run — merging into a
# foreign config could silently drop someone else's targets
# (PutBucketNotificationConfiguration REPLACES the whole document).
set -euo pipefail

BUCKET="" REGION="" PREFIX="" NAME_BASE="pj-cloud-catalog" DRY_RUN=0
while [ $# -gt 0 ]; do
  case "$1" in
    --bucket)    BUCKET="$2"; shift 2 ;;
    --region)    REGION="$2"; shift 2 ;;
    --prefix)    PREFIX="$2"; shift 2 ;;
    --name-base) NAME_BASE="$2"; shift 2 ;;
    --dry-run)   DRY_RUN=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
[ -n "$BUCKET" ] && [ -n "$REGION" ] || { echo "usage: $0 --bucket B --region R [--prefix P] [--dry-run]" >&2; exit 2; }

run() { if [ "$DRY_RUN" = 1 ]; then echo "DRY: $*"; else "$@"; fi; }

QUEUE="${NAME_BASE}-events"
DLQ="${NAME_BASE}-dlq"
RETENTION=345600   # 4 days (§3.1: covers an initial build + a prolonged outage)
VISIBILITY=300     # §3.1

echo "== DLQ: $DLQ"
run aws sqs create-queue --region "$REGION" --queue-name "$DLQ" \
  --attributes "{\"MessageRetentionPeriod\":\"$RETENTION\"}" >/dev/null || true
DLQ_URL=$(aws sqs get-queue-url --region "$REGION" --queue-name "$DLQ" --query QueueUrl --output text)
DLQ_ARN=$(aws sqs get-queue-attributes --region "$REGION" --queue-url "$DLQ_URL" \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)

echo "== queue: $QUEUE"
run aws sqs create-queue --region "$REGION" --queue-name "$QUEUE" --attributes "{
  \"MessageRetentionPeriod\":\"$RETENTION\",
  \"VisibilityTimeout\":\"$VISIBILITY\",
  \"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"
}" >/dev/null || true
QUEUE_URL=$(aws sqs get-queue-url --region "$REGION" --queue-name "$QUEUE" --query QueueUrl --output text)
QUEUE_ARN=$(aws sqs get-queue-attributes --region "$REGION" --queue-url "$QUEUE_URL" \
  --attribute-names QueueArn --query Attributes.QueueArn --output text)

echo "== queue policy (allow s3.amazonaws.com from arn:aws:s3:::$BUCKET)"
POLICY=$(cat <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"s3.amazonaws.com"},"Action":"sqs:SendMessage","Resource":"$QUEUE_ARN","Condition":{"ArnLike":{"aws:SourceArn":"arn:aws:s3:::$BUCKET"}}}]}
EOF
)
run aws sqs set-queue-attributes --region "$REGION" --queue-url "$QUEUE_URL" \
  --attributes "{\"Policy\": $(printf '%s' "$POLICY" | python3 -c 'import json,sys; print(json.dumps(sys.stdin.read()))')}"

echo "== bucket notification configuration (merge-guarded)"
EXISTING=$(aws s3api get-bucket-notification-configuration --region "$REGION" --bucket "$BUCKET")
if [ "$EXISTING" != "{}" ] && ! printf '%s' "$EXISTING" | grep -q "$QUEUE_ARN"; then
  echo "REFUSING: $BUCKET already has a notification configuration this script" >&2
  echo "does not own (Put REPLACES the whole document). Merge manually:" >&2
  printf '%s\n' "$EXISTING" >&2
  exit 3
fi
FILTER_RULES="[{\"Name\":\"suffix\",\"Value\":\".mcap\"}"
[ -n "$PREFIX" ] && FILTER_RULES="$FILTER_RULES,{\"Name\":\"prefix\",\"Value\":\"$PREFIX\"}"
FILTER_RULES="$FILTER_RULES]"
NOTIF=$(cat <<EOF
{"QueueConfigurations":[{"Id":"${NAME_BASE}-mcap-events","QueueArn":"$QUEUE_ARN","Events":["s3:ObjectCreated:*","s3:ObjectRemoved:*","s3:LifecycleExpiration:*"],"Filter":{"Key":{"FilterRules":$FILTER_RULES}}}]}
EOF
)
run aws s3api put-bucket-notification-configuration --region "$REGION" \
  --bucket "$BUCKET" --notification-configuration "$NOTIF"

echo
echo "queue URL: $QUEUE_URL"
echo "Pass this as MCAP_CATALOG_SQS_URL / --sqs-url (Phase 4)."
```

- [ ] **Step 2: Lint and dry-run it**

```bash
chmod +x scripts/staging-sqs-setup.sh
shellcheck scripts/staging-sqs-setup.sh
./scripts/staging-sqs-setup.sh --bucket example --region us-east-1 --dry-run || true
```

Expected: shellcheck clean (SC2086-style findings fixed, not suppressed); dry-run prints the `DRY:` command lines for the DLQ/queue steps, then fails at the first real `aws sqs get-queue-url` (dry-run doesn't create) — that's acceptable; note it in the header comment if it bothers you, or export `DLQ_URL`/`QUEUE_URL` placeholders under `DRY_RUN=1`:

```bash
if [ "$DRY_RUN" = 1 ]; then DLQ_URL="DRY-DLQ-URL"; DLQ_ARN="DRY-DLQ-ARN"; QUEUE_URL="DRY-QUEUE-URL"; QUEUE_ARN="DRY-QUEUE-ARN"; fi
```

(guard each `aws … get-queue-*` the same way — the dry run must reach the end and print the notification JSON).

- [ ] **Step 3: Commit**

```bash
git add scripts/staging-sqs-setup.sh
git commit -m "feat(deploy): staging SQS/DLQ/notification provisioning script (Phase 3, merge-guarded)"
```

---

### Task 2: Phase 3 — execute against staging, verify durable accumulation, capture real payloads

The design's Phase 3 is "notifications on, consumer off — queue accumulates durably". That state is also the perfect moment to capture **real AWS payloads** for §9's translator tests (open question 2 of the design) without stealing messages from a consumer.

**Files:**
- Create: `mcap_catalog/mcap_catalog_builder/tests/data/s3_events/create_put.json`
- Create: `mcap_catalog/mcap_catalog_builder/tests/data/s3_events/create_multipart.json` (if the drill upload is multipart; otherwise skip)
- Create: `mcap_catalog/mcap_catalog_builder/tests/data/s3_events/delete_delete.json`
- Create: `mcap_catalog/mcap_catalog_builder/tests/data/s3_events/ack_testevent.json`

- [ ] **Step 1: Preflight**

```bash
: "${PJ_STAGING_BUCKET:?set PJ_STAGING_BUCKET}" "${PJ_STAGING_REGION:?set PJ_STAGING_REGION}"
aws sts get-caller-identity
```

- [ ] **Step 2: Provision**

```bash
./scripts/staging-sqs-setup.sh --bucket "$PJ_STAGING_BUCKET" --region "$PJ_STAGING_REGION"
export MCAP_CATALOG_SQS_URL=<queue URL printed above>
```

Expected: ends with `queue URL: https://sqs...`. Enabling notifications typically enqueues one `s3:TestEvent` immediately.

- [ ] **Step 3: Generate events** — upload then delete one drill object

```bash
(cd server && go build -o bin/gen-ci-fixtures ./cmd/gen-ci-fixtures)
mkdir -p /tmp/sqs-drill && server/bin/gen-ci-fixtures -out /tmp/sqs-drill
F=$(ls /tmp/sqs-drill/**/*.mcap 2>/dev/null | head -1 || find /tmp/sqs-drill -name '*.mcap' | head -1)
K="customer=drill/customer_site=lab/robot=r1/source=ros-bags/date=$(date -u +%F)/capture-$(date +%s).mcap"
aws s3 cp "$F" "s3://$PJ_STAGING_BUCKET/$K" --region "$PJ_STAGING_REGION"
sleep 5
aws s3 rm "s3://$PJ_STAGING_BUCKET/$K" --region "$PJ_STAGING_REGION"
```

- [ ] **Step 4: Verify accumulation + capture bodies**

```bash
mkdir -p mcap_catalog/mcap_catalog_builder/tests/data/s3_events
i=0
while [ $i -lt 10 ]; do
  MSG=$(aws sqs receive-message --region "$PJ_STAGING_REGION" --queue-url "$MCAP_CATALOG_SQS_URL" \
        --max-number-of-messages 1 --wait-time-seconds 5 --query 'Messages[0]')
  [ "$MSG" = "null" ] || [ -z "$MSG" ] && break
  printf '%s' "$MSG" | python3 -c '
import json, sys
m = json.load(sys.stdin)
body = m["Body"]
try:
    doc = json.loads(body)
except ValueError:
    name = "ack_unparseable"
else:
    recs = doc.get("Records") or []
    if not recs:
        name = "ack_testevent"
    else:
        ev = recs[0]["eventName"]
        fam = "create" if ev.startswith("ObjectCreated") else \
              "delete" if ev.startswith("ObjectRemoved") or ev.startswith("LifecycleExpiration") else "ack_unknown"
        name = f"{fam}_{ev.split(\":\")[-1].lower()}"
open(f"mcap_catalog/mcap_catalog_builder/tests/data/s3_events/{name}.json", "w").write(body)
print(name, m["ReceiptHandle"][:20])
'
  i=$((i+1))
done
ls mcap_catalog/mcap_catalog_builder/tests/data/s3_events/
```

Expected: at least `create_put.json` (or `create_completemultipartupload.json`), `delete_delete.json` (or `delete_deletemarkercreated.json` on a versioned bucket), and likely `ack_testevent.json`. **These bodies prove the queue accumulates durably with the consumer off — that IS the Phase 3 verification.**

- [ ] **Step 5: Sanitize** (no account leakage in committed fixtures)

```bash
python3 - <<'EOF'
import json, pathlib
for p in pathlib.Path("mcap_catalog/mcap_catalog_builder/tests/data/s3_events").glob("*.json"):
    doc = json.loads(p.read_text())
    for r in doc.get("Records", []):
        b = r.get("s3", {}).get("bucket", {})
        b["name"] = "staging-bucket"; b["arn"] = "arn:aws:s3:::staging-bucket"
        b.get("ownerIdentity", {}).update({"principalId": "EXAMPLE"})
        r.get("userIdentity", {}).update({"principalId": "EXAMPLE"})
        r.get("requestParameters", {}).update({"sourceIPAddress": "127.0.0.1"})
        r.get("responseElements", {}).pop("x-amz-id-2", None)
        r.get("responseElements", {}).pop("x-amz-request-id", None)
    p.write_text(json.dumps(doc, indent=1))
EOF
git diff --stat  # nothing outside tests/data/s3_events expected
```

- [ ] **Step 6: Purge the drill messages** (so Phase 4's consumer starts clean)

```bash
aws sqs purge-queue --region "$PJ_STAGING_REGION" --queue-url "$MCAP_CATALOG_SQS_URL"
```

- [ ] **Step 7: Commit**

```bash
git add mcap_catalog/mcap_catalog_builder/tests/data/s3_events/
git commit -m "test(builder): captured real S3 event payloads from staging (Phase 3 verification)"
```

---

### Task 3: Translator characterization tests over the captured payloads

Design §3.2: the translator must be "tested against captured real AWS payloads". `translate_body(body, on_unknown=None) -> list[tuple[str, str]]` (`s3_producer.py:163`) is the entry; kinds are `"catalog"`/`"delete"`.

**Files:**
- Modify: `mcap_catalog/mcap_catalog_builder/tests/test_s3_producer.py`

- [ ] **Step 1: Write the test** (append to `test_s3_producer.py`)

```python
# -- §3.2: real captured AWS payloads (tests/data/s3_events, Phase 3) ---------

import json as _json
import pathlib

import pytest

from mcap_catalog_builder.s3_producer import translate_body

_DATA = pathlib.Path(__file__).parent / "data" / "s3_events"
_EXPECT = {"create": "catalog", "delete": "delete", "ack": None}


@pytest.mark.parametrize(
    "path", sorted(_DATA.glob("*.json")), ids=lambda p: p.name
)
def test_translator_handles_captured_real_payloads(path):
    """Family encoded in the filename prefix: create_* -> one catalog record,
    delete_* -> one delete record, ack_* -> zero records (ack immediately).
    The key must round-trip URL-decoded (S3 event keys are URL-encoded)."""
    family = path.name.split("_", 1)[0]
    body = path.read_text()
    records = translate_body(body)
    kinds = [k for k, _key in records]
    if _EXPECT[family] is None:
        assert records == []
    else:
        assert kinds == [_EXPECT[family]]
        key = records[0][1]
        assert key.endswith(".mcap") and "%" not in key  # decoded, not raw


def test_captured_payload_fixtures_exist():
    """Phase 3 committed at least one create and one delete real payload —
    without them the parametrized test above silently collects nothing."""
    names = {p.name.split("_", 1)[0] for p in _DATA.glob("*.json")}
    assert "create" in names and "delete" in names
```

- [ ] **Step 2: Run it**

```bash
cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/test_s3_producer.py -q; cd ..
```

Expected: PASS. If a captured payload translates differently than the filename claims, the TRANSLATOR is wrong (these are real AWS documents) — fix `_classify`/`translate_body`, not the test.

- [ ] **Step 3: Commit**

```bash
git add mcap_catalog/mcap_catalog_builder/tests/test_s3_producer.py
git commit -m "test(builder): translator characterization over captured real AWS payloads (§3.2/§9)"
```

---

### Task 4: Phase 4 wiring — `run.sh` + compose overlay + docs

Design §3.6: deploys gain `--sqs-url` (env `MCAP_CATALOG_SQS_URL`) and drop `--no-watch`; local Minio stays rescan-only.

**Files:**
- Modify: `run.sh` (the `cloud_builder_args` function, currently ending `BARGS+=(--no-watch --tag-socket "$TAG_SOCKET" --db "$DB_PATH" --rescan-interval 300 --log-level INFO)`)
- Create: `server/deploy/docker-compose.aws.events.yml`
- Modify: `server/deploy/README.md` (the "S3 event notifications (SQS)" section)

- [ ] **Step 1: `run.sh`** — replace the final `BARGS+=` line of `cloud_builder_args` with:

```bash
  if [ "$STORAGE_KIND" = s3 ] && [ -n "${MCAP_CATALOG_SQS_URL:-}" ]; then
    # Event-driven discovery (design 2026-07-30 Phase 4): SQS is the freshness
    # path, so the full audit relaxes to the 6 h safety net (Phase 0 value).
    BARGS+=(--sqs-url "$MCAP_CATALOG_SQS_URL" --rescan-interval 21600)
  else
    BARGS+=(--no-watch --rescan-interval 300)
  fi
  BARGS+=(--tag-socket "$TAG_SOCKET" --db "$DB_PATH" --log-level INFO)
```

Also add one line to the usage header comment near the top of `run.sh`:

```bash
#   MCAP_CATALOG_SQS_URL=<queue url> ./run.sh --aws   # event-driven builder discovery (Phase 4)
```

Note: `start_local_builder` (the `--local`/Minio path) is intentionally untouched — MinIO has no SQS (§3.6).

- [ ] **Step 2: The compose overlay** — create `server/deploy/docker-compose.aws.events.yml`:

```yaml
# Phase-4 overlay (design 2026-07-30 §3.6/§8): event-driven discovery ON.
# Compose `command` overrides are WHOLESALE, so the full list is repeated.
# Usage:
#   docker compose -f docker-compose.aws.yml -f docker-compose.aws.events.yml up -d
# Prereq: scripts/staging-sqs-setup.sh ran for this bucket; the instance role
# carries the SQS grants (docs/ec2-deploy.md).
services:
  builder:
    command:
      - "--source=s3"
      - "--s3-bucket=${PJ_CLOUD_S3_BUCKET:?set PJ_CLOUD_S3_BUCKET}"
      - "--s3-prefix=${PJ_CLOUD_S3_PREFIX?set PJ_CLOUD_S3_PREFIX (empty string = whole bucket)}"
      - "--sqs-url=${MCAP_CATALOG_SQS_URL:?set MCAP_CATALOG_SQS_URL (scripts/staging-sqs-setup.sh prints it)}"
      - "--tag-socket=/var/lib/pj-cloud/tag.sock"
      - "--db=/var/lib/pj-cloud/catalog.db"
      # Events carry freshness; the 6 h full audit is the safety net until
      # Phase 6 moves it to a nightly fixed hour.
      - "--rescan-interval=21600"
      - "--log-level=INFO"
```

- [ ] **Step 3: `server/deploy/README.md`** — in the existing "S3 event notifications (SQS)" section (line ~177): replace the raw `aws sqs create-queue …` command block with a pointer to `scripts/staging-sqs-setup.sh` (keep the raw commands as a collapsed reference), and change the enablement paragraph to name the overlay file and `run.sh`'s `MCAP_CATALOG_SQS_URL`. Keep the existing alarm guidance (`ApproximateAgeOfOldestMessage`, DLQ depth) verbatim.

- [ ] **Step 4: Verify + commit**

```bash
bash -n run.sh && shellcheck run.sh || true   # no NEW findings vs. main
docker compose -f server/deploy/docker-compose.aws.yml -f server/deploy/docker-compose.aws.events.yml config >/dev/null && echo COMPOSE-OK
git add run.sh server/deploy/docker-compose.aws.events.yml server/deploy/README.md
git commit -m "feat(deploy): Phase-4 wiring — MCAP_CATALOG_SQS_URL in run.sh + compose events overlay"
```

Expected: `COMPOSE-OK` (compose validates the merged config; the `:?` guards fire only at `up`).

---

### Task 5: Phase 4 — staging burn-in (consumer ON, drills)

**Files:**
- Create: `scripts/staging-event-drills.sh`
- Modify: `server/deploy/README.md` (burn-in drill procedures)

- [ ] **Step 1: The scripted freshness drill** — create `scripts/staging-event-drills.sh`:

```bash
#!/usr/bin/env bash
# staging-event-drills.sh — Phase-4 burn-in drills (design 2026-07-30 §9).
#
#   staging-event-drills.sh freshness --bucket B --region R [--db /tmp/pj-cloud-catalog.db]
#
# freshness: upload a fixture under a drill Hive key -> assert the catalog row
# appears (event tier latency), delete the object -> assert the row disappears.
# PASS/FAIL on stdout with measured latencies. Requires: a builder running with
# --sqs-url against the same bucket+db (MCAP_CATALOG_SQS_URL=... ./run.sh --aws).
set -euo pipefail

CMD="${1:-}"; shift || true
BUCKET="" REGION="" DB="/tmp/pj-cloud-catalog.db" TIMEOUT=120
while [ $# -gt 0 ]; do
  case "$1" in
    --bucket) BUCKET="$2"; shift 2 ;;
    --region) REGION="$2"; shift 2 ;;
    --db)     DB="$2"; shift 2 ;;
    --timeout) TIMEOUT="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
[ "$CMD" = freshness ] && [ -n "$BUCKET" ] && [ -n "$REGION" ] || {
  echo "usage: $0 freshness --bucket B --region R [--db PATH] [--timeout S]" >&2; exit 2; }

count() { sqlite3 "file:$DB?mode=ro" "SELECT COUNT(*) FROM files WHERE filename='$1'" 2>/dev/null || echo 0; }

wait_count() { # $1=filename $2=want -> prints elapsed seconds or fails
  local t0; t0=$(date +%s)
  while :; do
    [ "$(count "$1")" = "$2" ] && { echo $(( $(date +%s) - t0 )); return 0; }
    [ $(( $(date +%s) - t0 )) -ge "$TIMEOUT" ] && return 1
    sleep 1
  done
}

command -v sqlite3 >/dev/null || { echo "FAIL: sqlite3 not installed" >&2; exit 1; }
[ -f "$DB" ] || { echo "FAIL: catalog DB not found at $DB (is the builder running?)" >&2; exit 1; }

(cd server && go build -o bin/gen-ci-fixtures ./cmd/gen-ci-fixtures) 2>/dev/null || true
TMP=$(mktemp -d) && trap 'rm -rf "$TMP"' EXIT
server/bin/gen-ci-fixtures -out "$TMP"
F=$(find "$TMP" -name '*.mcap' | head -1)
FN="drill-$(date +%s).mcap"
K="customer=drill/customer_site=lab/robot=r1/source=ros-bags/date=$(date -u +%F)/$FN"

echo "== upload s3://$BUCKET/$K"
aws s3 cp "$F" "s3://$BUCKET/$K" --region "$REGION" --only-show-errors
if T=$(wait_count "$FN" 1); then echo "PASS: row appeared in ${T}s"; else
  echo "FAIL: row did not appear within ${TIMEOUT}s"; exit 1; fi

echo "== delete s3://$BUCKET/$K"
aws s3 rm "s3://$BUCKET/$K" --region "$REGION" --only-show-errors
if T=$(wait_count "$FN" 0); then echo "PASS: row removed in ${T}s"; else
  echo "FAIL: row not removed within ${TIMEOUT}s"; exit 1; fi
echo "FRESHNESS DRILL PASS"
```

```bash
chmod +x scripts/staging-event-drills.sh && shellcheck scripts/staging-event-drills.sh
```

- [ ] **Step 2: Enable the consumer on staging and run the drill**

```bash
# terminal 1 (leaves the stack up):
MCAP_CATALOG_SQS_URL="$MCAP_CATALOG_SQS_URL" ./run.sh --aws
# terminal 2:
./scripts/staging-event-drills.sh freshness --bucket "$PJ_STAGING_BUCKET" --region "$PJ_STAGING_REGION" \
  --db "$(grep -E '^\s*db_path:' server/deploy/config.aws-staging.yaml | awk '{print $2}')"
```

Expected: `FRESHNESS DRILL PASS` with both latencies well under 60 s (design target: minutes). Also check the sidecar:

```bash
DB=$(grep -E '^\s*db_path:' server/deploy/config.aws-staging.yaml | awk '{print $2}')
python3 -c "import json;d=json.load(open('$DB.status.json'));print({k:d[k] for k in d if k.startswith(('producer_','events_'))})"
```

Expected: `producer_last_poll_ok_at` fresh, `events_applied >= 2`, `events_acked >= 2`.

- [ ] **Step 3: kill −9 mid-batch drill** (redelivery proof, §9) — perform manually, record the result in the PR body:

```bash
for i in 1 2 3 4 5; do aws s3 cp "$F" \
  "s3://$PJ_STAGING_BUCKET/customer=drill/customer_site=lab/robot=r1/source=ros-bags/date=$(date -u +%F)/kill9-$i.mcap" \
  --region "$PJ_STAGING_REGION" --only-show-errors; done
kill -9 "$(cat /tmp/pj-cloud-builder.pid)"          # while events are in flight
MCAP_CATALOG_SQS_URL="$MCAP_CATALOG_SQS_URL" ./run.sh --aws   # restart
# within ~VisibilityTimeout (300 s) + processing, all 5 rows must exist exactly once:
sqlite3 "file:$DB?mode=ro" "SELECT COUNT(*) FROM files WHERE filename LIKE 'kill9-%'"
```

Expected: `5` (redelivery absorbed by idempotency; no duplicates possible — UNIQUE composite). Clean up: `aws s3 rm --recursive s3://$PJ_STAGING_BUCKET/customer=drill/ --region "$PJ_STAGING_REGION"` and wait for the delete events to prune the rows.

- [ ] **Step 4: Lifecycle rule for deferred payload capture** (design open question 2 — LifecycleExpiration payloads take ≥1 day to materialize):

```bash
aws s3api put-bucket-lifecycle-configuration --region "$PJ_STAGING_REGION" --bucket "$PJ_STAGING_BUCKET" \
  --lifecycle-configuration '{"Rules":[{"ID":"drill-lifecycle-capture","Status":"Enabled",
    "Filter":{"Prefix":"customer=drill/customer_site=lab/robot=r1/source=lifecycle/"},
    "Expiration":{"Days":1}}]}'
aws s3 cp "$F" "s3://$PJ_STAGING_BUCKET/customer=drill/customer_site=lab/robot=r1/source=lifecycle/date=$(date -u +%F)/expire-me.mcap" --region "$PJ_STAGING_REGION"
```

Record in the PR body: *"LifecycleExpiration payload capture pending — check the queue/DLQ tomorrow, add the body to `tests/data/s3_events/` in a follow-up."* This is a known deferred item, not a plan failure.

- [ ] **Step 5: README drill procedures** — add a "Burn-in drills" subsection to `server/deploy/README.md`'s SQS section documenting the three drills (freshness = the script; kill −9 = the Step-3 commands; notification outage = disable the bucket notification config, upload, verify the hot audit (Phase 5) repairs within one cadence, re-enable).

- [ ] **Step 6: Commit**

```bash
git add scripts/staging-event-drills.sh server/deploy/README.md
git commit -m "feat(deploy): Phase-4 burn-in — freshness drill script + drill runbook"
```

---

### Task 6: Phase 5a — `list_prefix` on `S3Source` and `LocalSource` (TDD)

The hot audit's fail-closed contract needs a LIST primitive whose return means *complete*: raise on any error, raise `ReconcileCancelled` on stop, never return a partial list.

**Files:**
- Modify: `mcap_catalog/mcap_catalog_builder/s3_storage.py`
- Modify: `mcap_catalog/mcap_catalog_builder/storage.py`
- Test: `mcap_catalog/mcap_catalog_builder/tests/test_hot_audit.py` (new file, first tests)

- [ ] **Step 1: Write the failing tests** — create `mcap_catalog/mcap_catalog_builder/tests/test_hot_audit.py`:

```python
"""Tier-2 hot-window scoped audit (design 2026-07-30 §4): scoped LIST,
registry derivation, fail-closed coverage, scoped sweep."""

import datetime as dt
import threading

import pytest

from mcap_catalog_builder.reconcile import ReconcileCancelled, full_reconcile
from mcap_catalog_builder.s3_storage import S3Source
from mcap_catalog_builder.storage import LocalSource
from mcap_catalog_builder.tests.fixtures import InMemoryS3Client, write_minimal_mcap

CH = [("/a", "S", "ros2msg", 2)]
TODAY = dt.date(2026, 6, 2)
PA = "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-06-02/"
PB = "customer=beta/customer_site=hq/robot=r2/source=ros-bags/date=2026-06-02/"
KA = PA + "a.mcap"
KB = PB + "b.mcap"


def _raw(tmp_path):
    local = str(tmp_path / "src.mcap")
    write_minimal_mcap(local, channels=CH)
    with open(local, "rb") as f:
        return f.read()


class PagedClient(InMemoryS3Client):
    """Two-page pagination for list_objects_v2 (InMemoryS3Client is single-page)."""

    def list_objects_v2(self, Bucket, Prefix="", Delimiter=None, ContinuationToken=None):
        keys = sorted(k for k in self._objects if k.startswith(Prefix))
        page = keys[:1] if ContinuationToken is None else keys[1:]
        return {
            "Contents": [{"Key": k, "Size": len(self._objects[k]),
                          "ETag": f'"etag-{k}"'} for k in page],
            "IsTruncated": ContinuationToken is None and len(keys) > 1,
            "NextContinuationToken": "page2",
        }


class Page2FailsClient(PagedClient):
    """Page 1 OK, page 2 raises for keys under fail_prefix — a PARTIAL listing."""

    def __init__(self, objects, fail_prefix):
        super().__init__(objects)
        self._fail_prefix = fail_prefix

    def list_objects_v2(self, Bucket, Prefix="", Delimiter=None, ContinuationToken=None):
        if Prefix.startswith(self._fail_prefix) and ContinuationToken is not None:
            raise RuntimeError("page 2 boom")
        return super().list_objects_v2(Bucket, Prefix, Delimiter, ContinuationToken)


# -- list_prefix --------------------------------------------------------------

def test_s3_list_prefix_paginates_to_completion(tmp_path):
    raw = _raw(tmp_path)
    src = S3Source(PagedClient({KA: raw, PA + "a2.mcap": raw, KB: raw}), "bucket")
    got = src.list_prefix(PA)
    assert sorted(l.key for l in got) == [KA, PA + "a2.mcap"]  # complete, scoped
    for l in got:
        assert l.stat.etag  # fingerprints come from the LIST itself


def test_s3_list_prefix_raises_on_page_failure_never_partial(tmp_path):
    raw = _raw(tmp_path)
    src = S3Source(Page2FailsClient({KA: raw, PA + "a2.mcap": raw}, PA), "bucket")
    with pytest.raises(RuntimeError, match="page 2 boom"):
        src.list_prefix(PA)


def test_s3_list_prefix_stop_raises_cancelled(tmp_path):
    raw = _raw(tmp_path)
    src = S3Source(PagedClient({KA: raw}), "bucket")
    stop = threading.Event()
    stop.set()
    with pytest.raises(ReconcileCancelled):
        src.list_prefix(PA, stop_event=stop)


def test_local_list_prefix_scopes_and_completes(tmp_path):
    root = tmp_path / "root"
    for key in (KA, KB):
        dest = root / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        write_minimal_mcap(str(dest), channels=CH)
    src = LocalSource(str(root))
    got = src.list_prefix(PA)
    assert [l.key for l in got] == [KA]
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/test_hot_audit.py -q; cd ..
```

Expected: `AttributeError: 'S3Source' object has no attribute 'list_prefix'`.

- [ ] **Step 3: Implement `S3Source.list_prefix`** — add to `s3_storage.py` after `list_all`:

```python
    def list_prefix(
        self, prefix: str, stop_event: threading.Event | None = None
    ) -> list[Listing]:
        """LIST one exact prefix to COMPLETION (serial pagination) and return
        every ``.mcap`` listing under it. Raises on any error, and raises
        ``ReconcileCancelled`` on stop — never returns a partial result. The
        hot audit's fail-closed coverage (design 2026-07-30 §4.2) equates
        "returned" with "complete", so a partial return here would turn an
        enumeration failure into deletions.
        """
        out: list[Listing] = []
        kw: dict = {"Bucket": self._bucket, "Prefix": prefix}
        while True:
            if stop_event is not None and stop_event.is_set():
                # Deferred import: reconcile imports storage.py (not this
                # module) at top level, so this cannot cycle — but keep it
                # lazy so the dependency stays invisible at import time.
                from .reconcile import ReconcileCancelled
                raise ReconcileCancelled("hot-audit LIST cancelled")
            resp = self._c.list_objects_v2(**kw)
            for o in resp.get("Contents", []):
                if o["Key"].endswith(".mcap"):
                    out.append(self._listing(o))
            if not resp.get("IsTruncated"):
                return out
            kw["ContinuationToken"] = resp["NextContinuationToken"]
```

- [ ] **Step 4: Implement `LocalSource.list_prefix`** — add to `storage.py` after `list_all` (imports `Path` is already used in `list_all`; reuse):

```python
    def list_prefix(
        self, prefix: str, stop_event: threading.Event | None = None
    ) -> list[Listing]:
        """Local mirror of ``S3Source.list_prefix`` (tests/dev): the complete
        listing of one key prefix, or an exception — never a partial result."""
        out: list[Listing] = []
        root = Path(self._root)
        for p in sorted(root.rglob("*.mcap")):
            if stop_event is not None and stop_event.is_set():
                from .reconcile import ReconcileCancelled
                raise ReconcileCancelled("hot-audit LIST cancelled")
            rel_parts = p.relative_to(root).parts
            if any(part.startswith(".") for part in rel_parts):
                continue
            if not _is_catalogable_name(p.name):
                continue
            rel = "/".join(rel_parts)
            if not rel.startswith(prefix):
                continue
            st = self.stat(rel)
            if st is not None:
                out.append(Listing(key=rel, stat=st))
        return out
```

- [ ] **Step 5: Run tests — all four pass; full suite still green**

```bash
cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/ -q; cd ..
```

- [ ] **Step 6: Commit**

```bash
git add mcap_catalog/mcap_catalog_builder/s3_storage.py mcap_catalog/mcap_catalog_builder/storage.py mcap_catalog/mcap_catalog_builder/tests/test_hot_audit.py
git commit -m "feat(builder): list_prefix — complete-or-raise scoped LIST on S3Source/LocalSource (Phase 5)"
```

---

### Task 7: Phase 5b — extract the shared read→apply engine from `full_reconcile`

Behavior-preserving refactor so `hot_audit` can reuse the bounded-window extract/apply machinery without duplicating it (DRY) and without touching the pinned memory invariant.

**Files:**
- Modify: `mcap_catalog/mcap_catalog_builder/reconcile.py`

- [ ] **Step 1: Move the block** — in `reconcile.py`, cut `full_reconcile`'s `_apply` closure and the whole `if workers > 1 … else …` read-phase block (currently lines 236–294) into a module-level function placed right before `full_reconcile`:

```python
def _run_extract_apply(
    conn, caches, source, to_extract, *, workers, source_spec,
    tally, progress=None, stop_event=None,
) -> None:
    """The shared read-phase (parallel, network-bound, NO DB) -> apply-phase
    (serial, single-writer) engine, used by ``full_reconcile`` and the tier-2
    ``hot_audit``. Moved verbatim from full_reconcile — the bounded-window
    invariant (never submit-all + as_completed; see _bounded_completions)
    lives HERE now and is still pinned by
    test_reconcile_extraction_results_release_incrementally."""

    def _apply(ex) -> None:
        _raise_if_stopped(stop_event)
        st = apply_extract(conn, caches, ex).status
        tally[st] += 1
        _raise_if_stopped(stop_event)
        if progress is not None:
            progress.file_done(st, getattr(ex, "summary_via", ""))

    if workers > 1 and len(to_extract) > 1:
        # (the existing pool-selection comment block moves verbatim)
        n = min(workers, len(to_extract))
        if source_spec is not None:
            pool = ProcessPoolExecutor(
                max_workers=n, initializer=_init_worker, initargs=(source_spec,),
                mp_context=multiprocessing.get_context("forkserver"),
            )
            submit = lambda item: pool.submit(_extract_task, item)  # noqa: E731
        else:
            pool = ThreadPoolExecutor(max_workers=n)
            submit = lambda item: pool.submit(extract_summary, source, *item)  # noqa: E731
        try:
            for ex in _bounded_completions(
                submit, to_extract, window=2 * n, stop_event=stop_event
            ):
                _apply(ex)
        finally:
            cancelled = stop_event is not None and stop_event.is_set()
            pool.shutdown(wait=not cancelled, cancel_futures=cancelled)
    else:
        for item in to_extract:
            _raise_if_stopped(stop_event)
            ex = extract_summary(source, *item)
            _raise_if_stopped(stop_event)
            _apply(ex)
```

(Keep the long forkserver comment with the moved code — it explains a production hazard.) In `full_reconcile`, the removed block becomes:

```python
    _run_extract_apply(
        conn, caches, source, to_extract, workers=workers,
        source_spec=source_spec, tally=tally, progress=progress,
        stop_event=stop_event,
    )
```

- [ ] **Step 2: Full suite green** (this is the whole verification — pure refactor, no new tests)

```bash
cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/ -q; cd ..
```

Expected: same pass count as baseline. Pay attention to `test_reconcile_extraction_results_release_incrementally` — it pins the moved invariant.

- [ ] **Step 3: Commit**

```bash
git add mcap_catalog/mcap_catalog_builder/reconcile.py
git commit -m "refactor(builder): extract _run_extract_apply from full_reconcile (behavior-preserving)"
```

---### Task 8: Phase 5c — `hot_audit.py`: registry derivation (TDD)

**Files:**
- Create: `mcap_catalog/mcap_catalog_builder/hot_audit.py`
- Test: `mcap_catalog/mcap_catalog_builder/tests/test_hot_audit.py` (append)

- [ ] **Step 1: Write the failing tests** (append to `test_hot_audit.py`):

```python
# -- registry derivation (§4.1) ----------------------------------------------

from mcap_catalog_builder.db import record_failure
from mcap_catalog_builder.hot_audit import hot_prefixes, registry_combos


def test_registry_combos_from_files_and_quarantine(tmp_db, tmp_path):
    conn, caches = tmp_db
    raw = _raw(tmp_path)
    full_reconcile(conn, caches, S3Source(InMemoryS3Client({KA: raw}), "bucket"))
    # A quarantined key's combo must NOT be exiled from the registry (§4.1:
    # quarantine deletes the files row, so files alone would forget it).
    record_failure(conn, KB, "boom")
    conn.commit()
    combos = registry_combos(conn)
    assert ("acme", "hq", "r1", "ros-bags") in combos
    assert ("beta", "hq", "r2", "ros-bags") in combos
    # An unparseable failure key contributes nothing (and does not raise).
    record_failure(conn, "not-a-hive-key.mcap", "boom")
    conn.commit()
    assert len(registry_combos(conn)) == 2


def test_hot_prefixes_window_and_shape():
    combos = {("acme", "hq", "r1", "ros-bags")}
    targets = hot_prefixes(combos, TODAY, window_days=2)
    assert [t[0] for t in targets] == [
        "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-05-31/",
        "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-06-01/",
        "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-06-02/",
    ]
    prefix, combo, date = targets[-1]
    assert combo == ("acme", "hq", "r1", "ros-bags") and date == "2026-06-02"
```

Run: `cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/test_hot_audit.py -q; cd ..` — expected: `ModuleNotFoundError: mcap_catalog_builder.hot_audit`.

- [ ] **Step 2: Create `mcap_catalog/mcap_catalog_builder/hot_audit.py`** with the registry half:

```python
"""Tier-2 hot-window scoped audit (event-discovery design 2026-07-30 §4).

A frequent, cheap, SCOPED audit of the prefixes where change is expected: the
registry of dimension combos known to the catalog (files ∪ parseable
``catalog_failures`` keys) × the last ``window_days`` of ``date=`` partitions.
Fail-closed by construction (§4.2):

- a prefix counts as *covered* only if its pagination COMPLETED (list_prefix
  raises rather than returning a partial result);
- the deletion sweep runs per covered prefix ONLY — global deletion is
  structurally impossible from a scoped feed;
- a hot audit NEVER stamps ``build_metadata`` (that is whole-catalog freshness,
  owned by the tier-3 full audit). Its telemetry goes to the sidecar only.

Runs on the single writer thread like everything else (an ``AuditItem`` with
``audit_kind="hot"`` dispatched by ``worker_loop``).
"""

import datetime as dt
import logging
from concurrent.futures import ThreadPoolExecutor

from .builder import resolve_key_dims
from .db import record_failure
from .keyparse import parse_hive_key
from .reconcile import ReconcileCancelled, _raise_if_stopped, _run_extract_apply

logger = logging.getLogger(__name__)

# Hot prefixes are small (one robot-day each); a modest pool keeps the LIST
# phase snappy without the full sharded-LIST machinery.
_LIST_PREFIX_THREADS = 8

Combo = tuple[str, str, str, str]  # (customer, site, robot, source)


def registry_combos(conn) -> set[Combo]:
    """§4.1: dimension combos present in ``files`` ∪ parseable keys in
    ``catalog_failures`` (a quarantined file must not exile its combo)."""
    combos: set[Combo] = set()
    for r in conn.execute(
        "SELECT DISTINCT c.name AS customer, s.name AS site, r.name AS robot, "
        "src.name AS source FROM files f "
        "JOIN customers c ON c.id = f.customer_id "
        "JOIN sites s ON s.id = f.site_id "
        "JOIN robots r ON r.id = f.robot_id "
        "JOIN sources src ON src.id = f.source_id"
    ).fetchall():
        combos.add((r["customer"], r["site"], r["robot"], r["source"]))
    for r in conn.execute("SELECT s3_key FROM catalog_failures").fetchall():
        dims = parse_hive_key(r["s3_key"])
        if dims is not None:
            combos.add((dims["customer"], dims["site"], dims["robot"], dims["source"]))
    return combos


def hot_prefixes(
    combos: set[Combo], today: dt.date, window_days: int
) -> list[tuple[str, Combo, str]]:
    """Hive prefixes for ``date ∈ [today − window_days, today]`` per combo,
    oldest-first, as ``(prefix, combo, date)`` — the combo+date ride along so
    the scoped sweep never has to re-parse a prefix string. Dates are ISO
    (matching the builder's own key convention); a bucket using non-ISO
    ``date=`` values is repaired by tier 3 only (§4.3's honesty list)."""
    dates = [
        (today - dt.timedelta(days=n)).isoformat()
        for n in range(window_days, -1, -1)
    ]
    out: list[tuple[str, Combo, str]] = []
    for combo in sorted(combos):
        customer, site, robot, source = combo
        for d in dates:
            out.append((
                f"customer={customer}/customer_site={site}/robot={robot}/"
                f"source={source}/date={d}/",
                combo, d,
            ))
    return out
```

- [ ] **Step 3: Run — the two new tests pass**

```bash
cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/test_hot_audit.py -q; cd ..
```

- [ ] **Step 4: Commit**

```bash
git add mcap_catalog/mcap_catalog_builder/hot_audit.py mcap_catalog/mcap_catalog_builder/tests/test_hot_audit.py
git commit -m "feat(builder): hot-audit registry derivation — files ∪ quarantined combos × date window (§4.1)"
```

---

### Task 9: Phase 5d — `hot_audit()` scoped reconcile (TDD)

**Files:**
- Modify: `mcap_catalog/mcap_catalog_builder/hot_audit.py`
- Test: `mcap_catalog/mcap_catalog_builder/tests/test_hot_audit.py` (append)

- [ ] **Step 1: Write the failing tests** (append; these are §9's scoped-audit matrix):

```python
# -- hot_audit (§4.2): scoped catalog + fail-closed scoped sweep --------------

from mcap_catalog_builder.hot_audit import hot_audit


def _seed(conn, caches, tmp_path, objects):
    """Full-reconcile the given objects in, returning the raw bytes used."""
    raw = _raw(tmp_path)
    full_reconcile(conn, caches, S3Source(InMemoryS3Client(
        {k: raw for k in objects}), "bucket"))
    return raw


def _count(conn, filename):
    return conn.execute(
        "SELECT COUNT(*) FROM files WHERE filename=?", (filename,)
    ).fetchone()[0]


def test_hot_audit_catalogs_new_skips_unchanged(tmp_db, tmp_path):
    conn, caches = tmp_db
    raw = _seed(conn, caches, tmp_path, [KA])
    src = S3Source(InMemoryS3Client({KA: raw, PA + "new.mcap": raw}), "bucket")
    tally = hot_audit(conn, caches, src, today=TODAY)
    assert tally["cataloged"] == 1 and tally["skipped"] == 1
    assert _count(conn, "new.mcap") == 1
    assert tally["covered_prefixes"] >= 1 and tally["skipped_prefixes"] == 0


def test_hot_audit_deletes_only_inside_covered_prefixes(tmp_db, tmp_path):
    """One failed prefix among many (§9): the failed prefix's stale row
    SURVIVES; the covered prefix's stale row is deleted."""
    conn, caches = tmp_db
    raw = _seed(conn, caches, tmp_path, [KA, KB])
    # Both objects vanish; prefix A's LIST breaks on page 2 -> A uncovered.
    src = S3Source(Page2FailsClient({PA + "x1.mcap": raw, PA + "x2.mcap": raw}, PA),
                   "bucket")
    tally = hot_audit(conn, caches, src, today=TODAY)
    assert tally["skipped_prefixes"] >= 1
    assert _count(conn, "a.mcap") == 1      # uncovered prefix: NO deletion
    assert _count(conn, "b.mcap") == 0      # covered empty prefix: deleted
    assert tally["deleted"] == 1


def test_hot_audit_covered_empty_prefix_is_authoritative(tmp_db, tmp_path):
    conn, caches = tmp_db
    _seed(conn, caches, tmp_path, [KA])
    src = S3Source(InMemoryS3Client({}), "bucket")   # everything gone, LISTs fine
    tally = hot_audit(conn, caches, src, today=TODAY)
    assert tally["deleted"] == 1 and _count(conn, "a.mcap") == 0


def test_hot_audit_ignores_dates_outside_window(tmp_db, tmp_path):
    conn, caches = tmp_db
    raw = _raw(tmp_path)
    old = "customer=acme/customer_site=hq/robot=r1/source=ros-bags/date=2026-05-01/old.mcap"
    full_reconcile(conn, caches, S3Source(InMemoryS3Client({old: raw}), "bucket"))
    # Object vanishes, but its date is outside [TODAY-2, TODAY]: tier 3's job.
    tally = hot_audit(conn, caches, src=S3Source(InMemoryS3Client({}), "bucket"),
                      today=TODAY)
    assert _count(conn, "old.mcap") == 1 and tally["deleted"] == 0


def test_hot_audit_never_stamps_build_metadata(tmp_db, tmp_path):
    conn, caches = tmp_db
    raw = _seed(conn, caches, tmp_path, [KA])
    before = conn.execute("SELECT build_id FROM build_metadata").fetchone()[0]
    hot_audit(conn, caches,
              S3Source(InMemoryS3Client({KA: raw, PA + "n.mcap": raw}), "bucket"),
              today=TODAY)
    after = conn.execute("SELECT build_id FROM build_metadata").fetchone()[0]
    assert after == before  # §4.2: a subset scan stamping freshness would lie


def test_hot_audit_quarantined_combo_is_scanned_and_repaired(tmp_db, tmp_path):
    conn, caches = tmp_db
    record_failure(conn, KB, "boom")   # combo known ONLY via quarantine
    conn.commit()
    raw = _raw(tmp_path)
    tally = hot_audit(conn, caches,
                      S3Source(InMemoryS3Client({KB: raw}), "bucket"), today=TODAY)
    assert tally["cataloged"] == 1 and _count(conn, "b.mcap") == 1


def test_hot_audit_stop_raises_cancelled_and_changes_nothing(tmp_db, tmp_path):
    conn, caches = tmp_db
    raw = _seed(conn, caches, tmp_path, [KA])
    stop = threading.Event()
    stop.set()
    with pytest.raises(ReconcileCancelled):
        hot_audit(conn, caches, S3Source(InMemoryS3Client({}), "bucket"),
                  today=TODAY, stop_event=stop)
    assert _count(conn, "a.mcap") == 1
```

Run to verify failure: `ImportError: cannot import name 'hot_audit'`.

- [ ] **Step 2: Implement** — append to `hot_audit.py`:

```python
def _stored_for(conn, caches, combo: Combo, date: str) -> dict[str, tuple[int, str]]:
    """{filename: (files.id, etag)} for one combo+date, via the UNIQUE
    composite index — O(scope) on the SQLite side (§4.2), never a full-table
    fingerprint load like full_reconcile's."""
    customer, site, robot, source = combo
    cid = caches.customer.get(customer)
    sid = caches.site.get((cid, site)) if cid is not None else None
    rid = caches.robot.get((sid, robot)) if sid is not None else None
    srcid = caches.source.get(source)
    if None in (cid, sid, rid, srcid):
        return {}   # no ids cached => no rows can exist for this combo
    return {
        r["filename"]: (r["id"], r["etag"])
        for r in conn.execute(
            "SELECT id, filename, etag FROM files WHERE customer_id=? AND "
            "site_id=? AND robot_id=? AND source_id=? AND date=?",
            (cid, sid, rid, srcid, date),
        ).fetchall()
    }


def hot_audit(
    conn, caches, source, *, window_days: int = 2, workers: int = 1,
    source_spec=None, stop_event=None, today: dt.date | None = None,
) -> dict[str, int]:
    """One tier-2 pass: scoped LIST -> catalog changes -> scoped sweep.

    Returns the tally ``{cataloged, skipped, failed, deleted,
    covered_prefixes, skipped_prefixes}``. Raises ``ReconcileCancelled`` on
    stop (partial passes change nothing the next pass can't redo)."""
    tally = {"cataloged": 0, "skipped": 0, "failed": 0, "deleted": 0,
             "covered_prefixes": 0, "skipped_prefixes": 0}
    _raise_if_stopped(stop_event)
    targets = hot_prefixes(
        registry_combos(conn),
        today if today is not None else dt.datetime.now(dt.timezone.utc).date(),
        window_days,
    )
    if not targets:
        return tally

    # LIST phase: each prefix independently. Covered = pagination COMPLETED
    # (list_prefix raises otherwise); a failed prefix is excluded from BOTH
    # the catalog and the sweep below — fail-closed per prefix (§4.2).
    covered: list[tuple[str, Combo, str, list]] = []

    def _list_one(target):
        prefix, combo, date = target
        return prefix, combo, date, source.list_prefix(prefix, stop_event=stop_event)

    with ThreadPoolExecutor(
        max_workers=min(_LIST_PREFIX_THREADS, len(targets))
    ) as pool:
        for fut in [pool.submit(_list_one, t) for t in targets]:
            try:
                covered.append(fut.result())
            except ReconcileCancelled:
                raise
            except Exception as e:  # noqa: BLE001 — excluded, never partial
                tally["skipped_prefixes"] += 1
                logger.warning(
                    "hot audit: prefix excluded from coverage (LIST failed): %s", e
                )
    _raise_if_stopped(stop_event)
    tally["covered_prefixes"] = len(covered)

    # Classify: scoped fingerprint lookup per covered prefix; unchanged files
    # skip with zero network (the listing carries the etag), like tier 3.
    to_extract: list = []
    for _prefix, combo, date, listings in covered:
        stored = _stored_for(conn, caches, combo, date)
        for lst in listings:
            _raise_if_stopped(stop_event)
            res = resolve_key_dims(lst.key, source)
            if res is None:
                record_failure(conn, lst.key, "unparseable key")
                conn.commit()
                tally["failed"] += 1
                continue
            dims, eff_key = res
            row = stored.get(dims["filename"])
            if row is not None and row[1] == lst.stat.etag:
                tally["skipped"] += 1
                continue
            to_extract.append((lst.key, lst.stat, dims, eff_key))

    _run_extract_apply(
        conn, caches, source, to_extract, workers=workers,
        source_spec=source_spec, tally=tally, progress=None,
        stop_event=stop_event,
    )

    # Scoped sweep: deletions ONLY for rows inside covered prefixes. The
    # per-prefix row set is re-read here, AFTER apply, so rows just written
    # are present and can never be swept. One transaction, rolled back on
    # cancellation (mirrors full_reconcile's sweep discipline).
    try:
        for _prefix, combo, date, listings in covered:
            _raise_if_stopped(stop_event)
            listed = {lst.key.rsplit("/", 1)[-1] for lst in listings}
            for filename, (row_id, _etag) in _stored_for(conn, caches, combo, date).items():
                if filename not in listed:
                    conn.execute("DELETE FROM files WHERE id=?", (row_id,))
                    tally["deleted"] += 1
        _raise_if_stopped(stop_event)
        conn.commit()
    except ReconcileCancelled:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise

    # NO record_build here, ever (§4.2) — build_metadata is whole-catalog
    # freshness owned by the tier-3 full audit; a subset stamp would lie.
    logger.info(
        "hot audit: cataloged=%d skipped=%d failed=%d deleted=%d "
        "covered=%d excluded=%d",
        tally["cataloged"], tally["skipped"], tally["failed"], tally["deleted"],
        tally["covered_prefixes"], tally["skipped_prefixes"],
    )
    return tally
```

- [ ] **Step 3: Run — all `test_hot_audit.py` tests pass, full suite green**

```bash
cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/ -q; cd ..
```

- [ ] **Step 4: Commit**

```bash
git add mcap_catalog/mcap_catalog_builder/hot_audit.py mcap_catalog/mcap_catalog_builder/tests/test_hot_audit.py
git commit -m "feat(builder): tier-2 hot-window scoped audit — fail-closed per-prefix coverage, no build stamp (§4.2)"
```

---

### Task 10: Phase 5e — sidecar telemetry: hot audit, maintenance window, tag-edit counters

Design §6's additive fields: `hot_audit_*`, `maintenance_window_active`, `tag_edits_{expired,failed}`. (`feed_snapshot_age` is Phase 7 — out of scope.)

**Files:**
- Modify: `mcap_catalog/mcap_catalog_builder/status.py`
- Modify: `mcap_catalog/mcap_catalog_builder/tag_ipc.py`
- Test: `mcap_catalog/mcap_catalog_builder/tests/test_status.py` (append)

- [ ] **Step 1: Failing tests** (append to `test_status.py`, following its existing read-the-sidecar-JSON pattern — open the file first and mirror how existing tests construct `StatusWriter` and read `writer.path`):

```python
def test_hot_audit_and_maintenance_fields(tmp_path):
    import json
    w = StatusWriter(str(tmp_path / "cat.db"))
    w.update(phase="idle", force=True)
    w.hot_audit_finished("ok", 1.5, covered=6, skipped=1)
    w.maintenance_window(True)
    doc = json.load(open(w.path))
    assert doc["hot_audit_outcome"] == "ok"
    assert doc["hot_audit_covered_prefixes"] == 6
    assert doc["hot_audit_skipped_prefixes"] == 1
    assert doc["maintenance_window_active"] is True
    w.maintenance_window(False)
    assert json.load(open(w.path))["maintenance_window_active"] is False


def test_tag_edit_counters(tmp_path):
    import json
    w = StatusWriter(str(tmp_path / "cat.db"))
    w.update(phase="idle", force=True)
    w.tag_edit_expired()
    w.tag_edit_failed()
    w.tag_edit_expired()
    doc = json.load(open(w.path))
    assert doc["tag_edits_expired"] == 2 and doc["tag_edits_failed"] == 1
```

- [ ] **Step 2: Implement in `status.py`** — in `StatusWriter.__init__`, extend the counters block:

```python
        self._tag_edits_expired = 0
        self._tag_edits_failed = 0
```

After `full_audit_finished`, add:

```python
    def hot_audit_finished(self, outcome: str, duration: float,
                           covered: int, skipped: int) -> None:
        """Tier-2 result (§4.2: hot-audit status goes to the sidecar ONLY —
        never build_metadata)."""
        self.update(
            hot_audit_last=_utc_iso(),
            hot_audit_outcome=outcome,
            hot_audit_duration=max(0.0, duration),
            hot_audit_covered_prefixes=covered,
            hot_audit_skipped_prefixes=skipped,
            force=True,
        )

    def maintenance_window(self, active: bool) -> None:
        """§5.2: the declared window while a full audit holds the writer —
        events pause and tag edits can expire; make it sidecar-visible."""
        self.update(maintenance_window_active=bool(active), force=True)

    def tag_edit_expired(self) -> None:
        with self._counters_lock:
            self._tag_edits_expired += 1
            n = self._tag_edits_expired
        self.update(tag_edits_expired=n)

    def tag_edit_failed(self) -> None:
        with self._counters_lock:
            self._tag_edits_failed += 1
            n = self._tag_edits_failed
        self.update(tag_edits_failed=n)
```

And in `ReconcileProgress`, after `event_applied`:

```python
    def hot_audit_finished(self, outcome: str, duration: float,
                           covered: int, skipped: int) -> None:
        if self._status is not None:
            self._status.hot_audit_finished(outcome, duration, covered, skipped)

    def maintenance_window(self, active: bool) -> None:
        if self._status is not None:
            self._status.maintenance_window(active)

    def tag_edit_expired(self) -> None:
        if self._status is not None:
            self._status.tag_edit_expired()

    def tag_edit_failed(self) -> None:
        if self._status is not None:
            self._status.tag_edit_failed()
```

- [ ] **Step 3: Hook `handle_tag_edit`** — in `tag_ipc.py`, change the signature and the two outcome sites:

```python
def handle_tag_edit(conn, caches, item: TagEditItem, telemetry=None) -> None:
```

In the expired branch (after `item.result.status = "expired"`), add:

```python
            if telemetry is not None:
                telemetry.tag_edit_expired()
```

In the `except Exception` branch (after `item.result.status = "error"`), add:

```python
        if telemetry is not None:
            telemetry.tag_edit_failed()
```

And in `__main__.py`'s `worker_loop`, change the call site to:

```python
            if isinstance(ev, TagEditItem):
                handle_tag_edit(conn, caches, ev, telemetry=progress)
                continue
```

- [ ] **Step 4: Run + commit**

```bash
cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/ -q; cd ..
git add mcap_catalog/mcap_catalog_builder/{status.py,tag_ipc.py,__main__.py} mcap_catalog/mcap_catalog_builder/tests/test_status.py
git commit -m "feat(builder): sidecar telemetry — hot_audit_*, maintenance_window_active, tag_edits_{expired,failed} (§6)"
```

---

### Task 11: Phase 5f — coordinator: hot cadence + single arbiter + worker dispatch (TDD)

**Files:**
- Modify: `mcap_catalog/mcap_catalog_builder/__main__.py`
- Test: `mcap_catalog/mcap_catalog_builder/tests/test_audit_scheduler.py` (append)

- [ ] **Step 1: Failing tests** (append):

```python
def test_hot_audits_run_completion_relative_alongside_full():
    """hot_interval small, full interval large: hot items flow with
    audit_kind='hot', completion-relative, through the SAME single arbiter."""
    work_q = queue.Queue()
    stop = threading.Event()
    coordinator = AuditCoordinator(
        work_q, stop, interval=60.0, backoff_initial=0.02, hot_interval=0.05
    )
    coordinator.start(immediate=False)
    try:
        first = work_q.get(timeout=1.0)
        assert first.audit_kind == "hot"
        assert coordinator.queued_or_running
        assert first.start()
        completed = time.monotonic()
        first.finish(AuditResult("ok", 0.01))
        second = work_q.get(timeout=1.0)
        assert second.audit_kind == "hot"
        assert time.monotonic() - completed >= 0.05 * 0.70  # completion-relative
        second.finish(AuditResult("ok", 0.01))
    finally:
        stop.set()
        coordinator.join()


def test_due_full_supersedes_due_hot():
    """Both due when the scheduler wakes: full wins (§5.2)."""
    work_q = queue.Queue()
    stop = threading.Event()
    coordinator = AuditCoordinator(
        work_q, stop, interval=0.05, backoff_initial=0.02, hot_interval=0.05
    )
    coordinator.start(immediate=True)   # full due NOW; hot due at 0.05
    try:
        first = work_q.get(timeout=1.0)
        assert first.audit_kind == "full"
        # Hold the full audit past the hot due-time: the arbiter must not
        # queue a hot item while an audit is queued/running.
        time.sleep(0.12)
        assert work_q.empty()
        first.finish(AuditResult("ok", 0.01))
        second = work_q.get(timeout=1.0)
        assert second.audit_kind in ("hot", "full")  # both are legal next
        second.finish(AuditResult("ok", 0.01))
    finally:
        stop.set()
        coordinator.join()


def test_worker_dispatches_hot_item_to_hot_audit(tmp_db, monkeypatch):
    import mcap_catalog_builder.__main__ as main_mod

    conn, caches = tmp_db
    calls = []
    monkeypatch.setattr(
        main_mod, "hot_audit",
        lambda *a, **kw: calls.append(("hot", kw.get("window_days"))) or
        {"cataloged": 0, "skipped": 0, "failed": 0, "deleted": 0,
         "covered_prefixes": 3, "skipped_prefixes": 1},
    )
    monkeypatch.setattr(
        main_mod, "full_reconcile",
        lambda *a, **kw: calls.append(("full", None)),
    )
    hot = AuditItem(audit_kind="hot")
    full = AuditItem()
    work_q = queue.Queue()
    work_q.put(hot)
    work_q.put(full)
    work_q.put(WatchEvent("stop"))
    worker_loop(conn, caches, object(), work_q, hot_window_days=5)
    assert calls == [("hot", 5), ("full", None)]
    assert hot.wait(threading.Event()).outcome == "ok"
    assert full.wait(threading.Event()).outcome == "ok"
```

Run: expected failures — `AuditCoordinator.__init__() got an unexpected keyword argument 'hot_interval'`, `AuditItem() takes no arguments`… 

- [ ] **Step 2: Implement in `__main__.py`.**

(a) Import `hot_audit` at the top: `from .hot_audit import hot_audit`.

(b) `AuditItem.__init__` gains the kind:

```python
    def __init__(self, audit_kind: str = "full") -> None:
        self.audit_kind = audit_kind
        self._lock = threading.Lock()
        self._done = threading.Event()
        self._state = "pending"  # pending | running | finished
        self._result: AuditResult | None = None
```

(c) `AuditCoordinator.__init__` signature grows (keep existing params/behavior; docstring: "Single arbiter for BOTH audit tiers: at most one audit — hot or full — queued or running; a due full audit supersedes a due hot one."):

```python
    def __init__(
        self,
        work_q,
        stop_event: threading.Event,
        interval: float,
        *,
        backoff_initial: float = _AUDIT_BACKOFF_INITIAL,
        intake_gate=None,
        hot_interval: float = 0.0,          # 0 = tier 2 disabled
        full_audit_hour: int | None = None,  # None = completion-relative (Task 13)
        wall_clock=time.time,
    ) -> None:
```

storing `self._hot_interval = max(0.0, hot_interval)`, `self._full_hour = full_audit_hour`, `self._wall = wall_clock` alongside the existing fields.

(d) `_enqueue_if_idle` takes the kind:

```python
    def _enqueue_if_idle(self, kind: str = "full") -> AuditItem | None:
        with self._lock:
            if self._stop_event.is_set() or self._queued_or_running:
                return None
            item = AuditItem(audit_kind=kind)
            ...
```

(rest unchanged; `request_due()` keeps calling it with the default — an external due request is a full audit.)

(e) Replace `_run` with the two-cadence loop (the §3.5 gate handshake moves into `_fire`, still cleared in `finally` on every path):

```python
    def _fire(self, kind: str) -> AuditResult | None:
        """Run one audit through the arbiter: pause/drain intake (§3.5, both
        tiers), enqueue (or observe the externally-requested active item),
        wait for the worker's result. None = stop arrived."""
        try:
            if self._gate is not None:
                self._gate.pause()
                if not self._gate.wait_drained(self._stop_event):
                    return None
            item = self._enqueue_if_idle(kind)
            if item is None:
                with self._lock:
                    item = self._current
                if item is None:
                    return None   # stop raced the due tick
            try:
                return item.wait(self._stop_event)
            finally:
                self._clear_active(item)
        finally:
            if self._gate is not None:
                self._gate.resume()

    def _run(self, initial_delay: float) -> None:
        mono = time.monotonic
        failures = 0
        next_full = mono() + initial_delay
        next_hot = (mono() + self._hot_interval) if self._hot_interval > 0 else None
        while True:
            due_times = [next_full] if next_hot is None else [next_full, next_hot]
            if self._stop_event.wait(max(0.0, min(due_times) - mono())):
                break
            now = mono()
            full_due = now >= next_full
            if not full_due and (next_hot is None or now < next_hot):
                continue
            kind = "full" if full_due else "hot"   # full supersedes hot (§5.2)
            result = self._fire(kind)
            if result is None or result.outcome == "cancelled":
                break
            if kind == "hot":
                # Completion-relative for success AND failure: the next tick
                # self-heals; hot failures never get a fast retry storm.
                next_hot = mono() + self._hot_interval
                continue
            if result.outcome == "ok":
                failures = 0
                next_full = mono() + self._full_delay()
            else:
                failures += 1
                cap = self._interval if self._full_hour is None else _FIXED_HOUR_BACKOFF_CAP
                backoff = min(cap, self._backoff_initial * (2 ** (failures - 1)))
                if self._full_hour is not None:
                    # Never retry past the next fixed slot (skip-missed, §5.2).
                    backoff = min(backoff, _next_fixed_hour_delay(self._wall(), self._full_hour))
                next_full = mono() + backoff
                logger.warning(
                    "full audit failed; retrying in %.1fs (failure %d): %s",
                    backoff, failures, result.error or "unknown error",
                )

        # Stop may arrive while a timer is waiting. If an external due request
        # queued an item, make it a dropped terminal item. (unchanged block)
        with self._lock:
            current = self._current
        if current is not None:
            try:
                current.cancel_if_pending()
            finally:
                self._clear_active(current)

    def _full_delay(self) -> float:
        """Delay from NOW to the next full audit: completion-relative interval,
        or the next fixed UTC hour (Task 13)."""
        if self._full_hour is None:
            return self._interval
        return _next_fixed_hour_delay(self._wall(), self._full_hour)
```

with the module constant near `_AUDIT_BACKOFF_INITIAL`:

```python
_FIXED_HOUR_BACKOFF_CAP = 3600.0  # fixed-hour mode: retry at most hourly
```

(`_next_fixed_hour_delay` itself is Task 13 — until then, add a stub `def _next_fixed_hour_delay(now_wall, hour): raise NotImplementedError` is NOT needed because `full_audit_hour` stays `None` everywhere until Task 13; reference it only via the `self._full_hour is not None` branches, which are dead until then. If the linter complains about the undefined name, add the Task 13 function now — it is small.)

(f) `worker_loop` — signature gains `hot_window_days: int = 2`; replace the `AuditItem` branch:

```python
        if isinstance(ev, AuditItem):
            if not ev.start():
                continue  # pending audit was dropped during shutdown
            started = time.monotonic()
            outcome = "ok"
            error = None
            is_full = ev.audit_kind != "hot"
            hot_tally: dict | None = None
            try:
                if is_full:
                    if progress is not None:
                        progress.maintenance_window(True)   # §5.2's declared window
                    full_reconcile(
                        conn, caches, source, workers=workers,
                        source_spec=source_spec, progress=progress,
                        stop_event=stop_event,
                    )
                else:
                    hot_tally = hot_audit(
                        conn, caches, source, window_days=hot_window_days,
                        workers=workers, source_spec=source_spec,
                        stop_event=stop_event,
                    )
            except ReconcileCancelled:
                outcome = "cancelled"
                logger.info("%s audit cancelled", ev.audit_kind)
            except Exception as e:  # noqa: BLE001 - result is explicit; worker survives
                outcome = "failed"
                error = f"{type(e).__name__}: {e}"
                logger.exception("%s audit failed", ev.audit_kind)
            finally:
                result = AuditResult(outcome, time.monotonic() - started, error)
                try:
                    if progress is not None:
                        if is_full:
                            progress.maintenance_window(False)
                            progress.audit_finished(result.outcome, result.duration)
                            progress.idle()
                        else:
                            t = hot_tally or {}
                            progress.hot_audit_finished(
                                result.outcome, result.duration,
                                t.get("covered_prefixes", 0),
                                t.get("skipped_prefixes", 0),
                            )
                except Exception:  # noqa: BLE001 - observability cannot kill the worker
                    logger.exception("failed to record audit status")
                finally:
                    ev.finish(result)
            continue
```

- [ ] **Step 3: Run — new tests pass AND every pre-existing scheduler/gate test still passes** (they pin the full-only behavior; `hot_interval=0` must be exactly the old coordinator):

```bash
cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/ -q; cd ..
```

- [ ] **Step 4: Commit**

```bash
git add mcap_catalog/mcap_catalog_builder/__main__.py mcap_catalog/mcap_catalog_builder/tests/test_audit_scheduler.py
git commit -m "feat(builder): single-arbiter dual-cadence scheduling — hot tier alongside full audits (§5.2)"
```

---

### Task 12: Phase 5g — CLI flags + wiring (TDD)

**Files:**
- Modify: `mcap_catalog/mcap_catalog_builder/__main__.py`
- Test: `mcap_catalog/mcap_catalog_builder/tests/test_cli.py` (append)

- [ ] **Step 1: Failing tests** (append to `test_cli.py`, mirroring its existing parser/exit-code patterns — read the file's existing style first):

```python
def test_hot_audit_flags_defaults():
    args = build_parser().parse_args(["."])
    assert args.hot_audit_interval == 0.0     # tier 2 is opt-in (deploy flag)
    assert args.hot_audit_window_days == 2
    assert args.full_audit_hour is None


def test_hot_audit_rejected_for_gcs(caplog):
    rc = main([".", "--source", "gcs", "--gcs-bucket", "b",
               "--hot-audit-interval", "1800", "--once"])
    assert rc == 2


def test_full_audit_hour_range_validated():
    rc = main([".", "--full-audit-hour", "24", "--once"])
    assert rc == 2
```

- [ ] **Step 2: Implement.** In `build_parser()` after `--rescan-interval`:

```python
    p.add_argument("--hot-audit-interval", type=float, default=0.0,
                   help="[s3/local daemon] seconds between tier-2 hot-window scoped "
                        "audits (design 2026-07-30 §4): a cheap LIST of only the "
                        "registry-derived recent date= prefixes, repairing lost "
                        "events within one cadence. 0 = disabled (default). "
                        "Deploys with the SQS event tier typically set 1800.")
    p.add_argument("--hot-audit-window-days", type=int, default=2,
                   help="tier-2 window W: audit date partitions in "
                        "[today-W, today] UTC (default: 2)")
    p.add_argument("--full-audit-hour", type=int, default=None,
                   help="run the FULL audit at this fixed UTC hour (0-23) "
                        "nightly, skip-missed, instead of completion-relative "
                        "--rescan-interval (design §5.2/§5.3 — Phase 6; enable "
                        "only after the event tier has burned in)")
```

In `main()` right after `args = build_parser().parse_args(argv)` and logging setup:

```python
    if args.full_audit_hour is not None and not (0 <= args.full_audit_hour <= 23):
        logging.getLogger(__name__).error("--full-audit-hour must be 0-23 (UTC)")
        return 2
    if args.hot_audit_interval > 0 and args.source == "gcs":
        logging.getLogger(__name__).error(
            "--hot-audit-interval requires --source s3 or local "
            "(GCS has no scoped-LIST wiring yet)"
        )
        return 2
```

In `_locked_main`, the coordinator/worker wiring becomes:

```python
        audit_coordinator = AuditCoordinator(
            work_q, stop_event, args.rescan_interval, intake_gate=intake_gate,
            hot_interval=args.hot_audit_interval,
            full_audit_hour=args.full_audit_hour,
        )
        audit_coordinator.start(immediate=startup_audit)

        worker_loop(conn, caches, source, work_q, workers=args.extract_workers,
                    source_spec=extract_spec, progress=progress,
                    stop_event=stop_event,
                    hot_window_days=args.hot_audit_window_days)
```

- [ ] **Step 3: Run + commit**

```bash
cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/ -q; cd ..
git add mcap_catalog/mcap_catalog_builder/__main__.py mcap_catalog/mcap_catalog_builder/tests/test_cli.py
git commit -m "feat(builder): --hot-audit-interval/--hot-audit-window-days/--full-audit-hour flags"
```

---

### Task 13: Phase 6a — fixed-hour full audits (TDD)

**Files:**
- Modify: `mcap_catalog/mcap_catalog_builder/__main__.py`
- Test: `mcap_catalog/mcap_catalog_builder/tests/test_audit_scheduler.py` (append)

- [ ] **Step 1: Failing tests:**

```python
def test_next_fixed_hour_delay_math():
    import calendar
    from mcap_catalog_builder.__main__ import _next_fixed_hour_delay

    # 2026-08-10 10:30:00 UTC
    now = calendar.timegm((2026, 8, 10, 10, 30, 0, 0, 0, 0))
    assert _next_fixed_hour_delay(now, 11) == 1800.0          # same day, 30 min out
    assert _next_fixed_hour_delay(now, 2) == 15.5 * 3600.0    # tomorrow 02:00
    at_slot = calendar.timegm((2026, 8, 10, 11, 0, 0, 0, 0, 0))
    assert _next_fixed_hour_delay(at_slot, 11) == 86400.0     # STRICTLY after now


def test_fixed_hour_schedules_from_completion_skip_missed(monkeypatch):
    """After a full audit completes, the next one lands one (patched) fixed-hour
    delay later — computed from NOW, never from a nominal missed schedule."""
    import mcap_catalog_builder.__main__ as main_mod

    monkeypatch.setattr(main_mod, "_next_fixed_hour_delay", lambda now, hour: 0.06)
    work_q = queue.Queue()
    stop = threading.Event()
    coordinator = AuditCoordinator(
        work_q, stop, interval=999.0, backoff_initial=0.02, full_audit_hour=2
    )
    coordinator.start(immediate=True)
    try:
        first = work_q.get(timeout=1.0)
        assert first.audit_kind == "full"
        done = time.monotonic()
        first.finish(AuditResult("ok", 0.01))
        second = work_q.get(timeout=1.0)
        assert 0.04 <= time.monotonic() - done < 1.0   # the patched slot, not 999s
        second.finish(AuditResult("ok", 0.01))
    finally:
        stop.set()
        coordinator.join()
```

- [ ] **Step 2: Implement** — add near `_FIXED_HOUR_BACKOFF_CAP` in `__main__.py` (plus `import calendar` at the top):

```python
def _next_fixed_hour_delay(now_wall: float, hour: int) -> float:
    """Seconds from ``now_wall`` (unix) to the NEXT occurrence of ``hour``:00
    UTC STRICTLY after now. Always computed from now — a slot that passed while
    the process was down or an audit overran is simply skipped (§5.2
    skip-missed), never replayed."""
    t = time.gmtime(now_wall)
    today_slot = float(calendar.timegm((t.tm_year, t.tm_mon, t.tm_mday, hour, 0, 0, 0, 0, 0)))
    if now_wall < today_slot:
        return today_slot - now_wall
    return today_slot + 86400.0 - now_wall
```

(The `self._full_hour is not None` branches written in Task 11 now light up; nothing else changes.)

- [ ] **Step 3: Run + commit**

```bash
cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/test_audit_scheduler.py -q; cd ..
git add mcap_catalog/mcap_catalog_builder/__main__.py mcap_catalog/mcap_catalog_builder/tests/test_audit_scheduler.py
git commit -m "feat(builder): fixed-UTC-hour nightly full audits, skip-missed, backoff capped at next slot (§5.2-5.3)"
```

---

### Task 14: Phase 6b — `catalog_failures` hygiene in the full audit (TDD)

Design §5.3 / Codex finding 18: failure rows for vanished objects live forever. The full audit removes failure rows absent from a *complete* enumeration — and only the full audit (the hot audit never touches failure hygiene). Fail-closed: this code is reachable only after `list_all` completed without raising.

**Files:**
- Modify: `mcap_catalog/mcap_catalog_builder/reconcile.py`
- Modify: `mcap_catalog/mcap_catalog_builder/status.py` (`finished` counter)
- Test: `mcap_catalog/mcap_catalog_builder/tests/test_reconcile.py` (append)

- [ ] **Step 1: Failing test** (append to `test_reconcile.py` — check its existing imports; it drives `full_reconcile` over local roots and/or fakes):

```python
def test_full_reconcile_prunes_failure_rows_for_vanished_objects(tmp_db, tmp_path):
    from mcap_catalog_builder.db import record_failure
    from mcap_catalog_builder.s3_storage import S3Source
    from mcap_catalog_builder.tests.fixtures import InMemoryS3Client, write_minimal_mcap

    conn, caches = tmp_db
    local = str(tmp_path / "src.mcap")
    write_minimal_mcap(local, channels=[("/a", "S", "ros2msg", 1)])
    raw = open(local, "rb").read()
    present_bad = "customer=a/customer_site=s/robot=r/source=x/date=2026-06-02/present.mcap"
    record_failure(conn, present_bad, "still failing")     # object still listed
    record_failure(conn, "customer=a/customer_site=s/robot=r/source=x/date=2026-06-02/gone.mcap",
                   "object vanished")                       # object NOT listed
    conn.commit()

    tally = full_reconcile(conn, caches, S3Source(
        InMemoryS3Client({present_bad: b"not an mcap"}), "bucket"))

    keys = {r["s3_key"] for r in conn.execute("SELECT s3_key FROM catalog_failures")}
    assert present_bad in keys          # listed => kept (still quarantined)
    assert not any("gone.mcap" in k for k in keys)   # absent from a COMPLETE enum => pruned
    assert tally["failures_pruned"] == 1
```

- [ ] **Step 2: Implement.** In `full_reconcile`: extend the tally init to

```python
    tally = {"cataloged": 0, "skipped": 0, "failed": 0, "deleted": 0, "failures_pruned": 0}
```

and inside the existing sweep `try:` block, after the `files` deletion loop and before `conn.commit()`:

```python
        # §5.3 catalog_failures hygiene (tier 3 ONLY): prune failure rows whose
        # keys are absent from this COMPLETE enumeration. Reachable only when
        # list_all finished without raising, so absence here is authoritative —
        # the same fail-closed rule as row deletion.
        all_keys = {lst.key for lst in listings}
        for r in conn.execute("SELECT s3_key FROM catalog_failures").fetchall():
            _raise_if_stopped(stop_event)
            if r["s3_key"] not in all_keys:
                conn.execute("DELETE FROM catalog_failures WHERE s3_key=?", (r["s3_key"],))
                tally["failures_pruned"] += 1
```

In `status.py` `ReconcileProgress.finished`, add alongside `deleted=`:

```python
            failures_pruned=tally.get("failures_pruned", 0),
```

- [ ] **Step 3: Run + commit**

```bash
cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/ -q; cd ..
git add mcap_catalog/mcap_catalog_builder/{reconcile.py,status.py} mcap_catalog/mcap_catalog_builder/tests/test_reconcile.py
git commit -m "feat(builder): catalog_failures hygiene in the full audit — fail-closed pruning (§5.3)"
```

---

### Task 15: §7.1 — vanished object → `ERROR_NOT_FOUND` at session plan-build (Go, TDD)

`ERROR_NOT_FOUND` already exists in the proto (code 3). The change: a `storage.ErrNotFound` sentinel that the classifiers attach to the object-absent subset of `ErrPermanent` (Go 1.23 supports multiple `%w`), and a pure `planBuildErrorCode` helper at `handlers_session.go:279`. Mid-stream errors (line 440) intentionally stay `ERROR_S3_UNAVAILABLE`. **403 is deliberately NOT mapped** — S3 returns 403 for missing objects when the caller lacks `s3:ListBucket`, and claiming "deleted" on an auth failure would misdirect the operator.

**Files:**
- Modify: `server/internal/storage/storage.go`, `server/internal/storage/s3.go`, `server/internal/storage/gcsreader.go`
- Modify: `server/internal/ws/handlers_session.go`
- Test: `server/internal/storage/retry_test.go`, `server/internal/storage/gcsreader_test.go`, create `server/internal/ws/session_error_code_test.go`

- [ ] **Step 1: Failing tests.** In `retry_test.go`'s `TestS3_ClassifyErrors`, append at the end of the function:

```go
	// §7.1: the object-absent subset additionally carries ErrNotFound so the
	// session plan-build can report a vanished recording as ERROR_NOT_FOUND.
	for _, code := range []string{"NoSuchKey", "NotFound", "NoSuchBucket"} {
		if !errors.Is(classify(&apiErr{code: code, msg: "x"}), ErrNotFound) {
			t.Errorf("code %q should carry ErrNotFound", code)
		}
	}
	if errors.Is(classify(&apiErr{code: "AccessDenied", msg: "x"}), ErrNotFound) {
		t.Error("AccessDenied must NOT carry ErrNotFound (403 can mask auth problems)")
	}
```

In `gcsreader_test.go`'s `TestGCS_ClassifyErrors`, append:

```go
	if !errors.Is(classifyGCS(gcs.ErrObjectNotExist), ErrNotFound) {
		t.Error("ErrObjectNotExist should carry ErrNotFound")
	}
```

Create `server/internal/ws/session_error_code_test.go`:

```go
package ws

import (
	"errors"
	"fmt"
	"testing"

	"pj-cloud/server/internal/storage"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// §7.1 of the 2026-07-30 event-discovery design: during the deletion-staleness
// window a catalog row can name a vanished object. Plan-build must surface
// that as ERROR_NOT_FOUND ("recording was deleted — refresh the list"), never
// as a generic bucket outage.
func TestPlanBuildErrorCode(t *testing.T) {
	gone := fmt.Errorf("load chunk index for file 7: %w",
		fmt.Errorf("%w: %w: NoSuchKey", storage.ErrPermanent, storage.ErrNotFound))
	if got := planBuildErrorCode(gone); got != pb.ErrorCode_ERROR_NOT_FOUND {
		t.Errorf("vanished object: got %v, want ERROR_NOT_FOUND", got)
	}
	outage := fmt.Errorf("load chunk index for file 7: %w",
		fmt.Errorf("%w: connection refused", storage.ErrTransient))
	if got := planBuildErrorCode(outage); got != pb.ErrorCode_ERROR_S3_UNAVAILABLE {
		t.Errorf("outage: got %v, want ERROR_S3_UNAVAILABLE", got)
	}
	denied := fmt.Errorf("%w: AccessDenied", storage.ErrPermanent)
	if got := planBuildErrorCode(denied); got != pb.ErrorCode_ERROR_S3_UNAVAILABLE {
		t.Errorf("auth failure: got %v, want ERROR_S3_UNAVAILABLE", got)
	}
	if !errors.Is(gone, storage.ErrPermanent) {
		t.Error("dual-wrap must preserve ErrPermanent for existing retry logic")
	}
}
```

Run: `cd server && go test ./internal/storage/ ./internal/ws/ -run 'ClassifyErrors|PlanBuildErrorCode' 2>&1 | tail -5; cd ..` — expected: undefined `ErrNotFound` / `planBuildErrorCode` compile errors.

- [ ] **Step 2: Implement.** `storage.go`, in the `var (...)` block:

```go
	// ErrNotFound marks the object-absent subset of ErrPermanent (404 /
	// NoSuchKey / NoSuchBucket). Retry logic keys on ErrPermanent as before;
	// the session plan-build additionally keys on this to report a vanished
	// recording as ERROR_NOT_FOUND instead of a generic bucket outage
	// (event-discovery design 2026-07-30 §7.1). Auth-shaped permanents
	// (403/AccessDenied) deliberately do NOT carry it.
	ErrNotFound = errors.New("storage: object not found")
```

`s3.go` `classify`: change the typed-error arm and split the code-switch arm:

```go
	var nf *types.NoSuchKey
	var nb *types.NoSuchBucket
	if errors.As(err, &nf) || errors.As(err, &nb) {
		return fmt.Errorf("%w: %w: %v", ErrPermanent, ErrNotFound, err)
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch code := apiErr.ErrorCode(); {
		case code == "NoSuchKey" || code == "NotFound" || code == "NoSuchBucket":
			return fmt.Errorf("%w: %w: %v", ErrPermanent, ErrNotFound, err)
		case code == "AccessDenied" || code == "Forbidden" || strings.HasPrefix(code, "InvalidAccessKeyId") ||
			// PreconditionFailed = a GetRangeVersioned If-Match miss: the object
			// was overwritten (a new version). Retrying can NEVER succeed against
			// this ETag, so it is PERMANENT — retrying just delays the clean
			// "object changed mid-session" failure.
			code == "PreconditionFailed" || code == "412":
			return fmt.Errorf("%w: %v", ErrPermanent, err)
		default:
			return fmt.Errorf("%w: %v", ErrTransient, err)
		}
	}
```

`gcsreader.go` `classifyGCS`: the sentinel arm becomes

```go
	if errors.Is(err, gcs.ErrObjectNotExist) || errors.Is(err, gcs.ErrBucketNotExist) {
		return fmt.Errorf("%w: %w: %v", ErrPermanent, ErrNotFound, err)
	}
```

and in the status-code switch, split 404 out of the permanent arm:

```go
	case code == http.StatusNotFound:
		return fmt.Errorf("%w: %w: %v", ErrPermanent, ErrNotFound, err)
	case code == http.StatusForbidden, code == http.StatusBadRequest:
		return fmt.Errorf("%w: %v", ErrPermanent, err)
```

`handlers_session.go`: add the helper (near the plan-build section; `errors` and `pj-cloud/server/internal/storage` imports — add if absent):

```go
// planBuildErrorCode maps a plan-build failure onto the wire code: a vanished
// object (storage.ErrNotFound — deleted after cataloging; the §7.1 deletion-
// staleness window) is ERROR_NOT_FOUND so the client can say "recording was
// deleted — refresh the list"; everything else stays ERROR_S3_UNAVAILABLE.
func planBuildErrorCode(err error) pb.ErrorCode {
	if errors.Is(err, storage.ErrNotFound) {
		return pb.ErrorCode_ERROR_NOT_FOUND
	}
	return pb.ErrorCode_ERROR_S3_UNAVAILABLE
}
```

and change line 279's send to:

```go
		c.sendError(reqID, 0, planBuildErrorCode(err), msg, err.Error())
```

- [ ] **Step 3: Full server suite + race**

```bash
cd server && go vet ./... && go test ./... && go test -race ./internal/ws/ ./internal/storage/; cd ..
```

Expected: all pass (existing `ErrPermanent` assertions unaffected — the dual wrap preserves `errors.Is(err, ErrPermanent)`).

- [ ] **Step 4: Commit**

```bash
git add server/internal/storage/ server/internal/ws/
git commit -m "feat(server): vanished object at plan-build -> ERROR_NOT_FOUND via storage.ErrNotFound (§7.1)"
```

---

### Task 16: Phase 5/6 deploy wiring

**Files:**
- Modify: `server/deploy/docker-compose.aws.events.yml`
- Modify: `server/deploy/README.md`
- Modify: `server/deploy/pj-cloud-builder.service`

- [ ] **Step 1: Extend the events overlay** — the builder command in `docker-compose.aws.events.yml` gains, after `--sqs-url=…`:

```yaml
      # Tier 2 (Phase 5): scoped hot-window audit every 30 min — repairs lost
      # events within one cadence; fail-closed per prefix.
      - "--hot-audit-interval=1800"
      # Phase 6 (enable LAST, after tiers 1-2 burn in): nightly full audit at a
      # fixed off-peak UTC hour instead of the 6 h completion-relative interval.
      # Uncomment both lines together and pick the hour for your fleet:
      #- "--full-audit-hour=3"
```

(and when `--full-audit-hour` is uncommented the operator removes `--rescan-interval=21600`; say so in a comment — `--rescan-interval` is ignored for scheduling when the fixed hour is set, but keeping both is confusing.)

Note: `--rescan-interval` is NOT ignored in code (it still caps completion-relative mode); with `--full-audit-hour` set it is unused. Verify the comment you write matches the code you wrote in Task 11 (`_full_delay`).

- [ ] **Step 2: `pj-cloud-builder.service`** — update the comment block (lines ~62-68) that currently describes `--no-watch` as "the safe default": add the two-line recipe for event mode (drop `--no-watch`, add `--sqs-url` + `--hot-audit-interval 1800`, later `--full-audit-hour`).

- [ ] **Step 3: README** — extend the SQS section's enablement paragraph with the Phase 5/6 knobs and the design's ordering warning (hot audit with events; fixed-hour only after burn-in).

- [ ] **Step 4: Verify + commit**

```bash
docker compose -f server/deploy/docker-compose.aws.yml -f server/deploy/docker-compose.aws.events.yml config >/dev/null && echo COMPOSE-OK
git add server/deploy/
git commit -m "feat(deploy): Phase 5/6 knobs in the events overlay + service/README guidance"
```

---

### Task 17: Staging validation of Phases 5–6 + notification-outage drill

- [ ] **Step 1: Restart the staging builder with the hot audit on** (short cadence for the drill):

Stop the running stack (`make server-stop`), then relaunch with a drill-tuned builder. `run.sh` doesn't pass hot-audit flags, so run the builder directly for this validation (mirroring what `run.sh` does — pidfiles included — is unnecessary for a one-shot validation; keep the Go server down):

```bash
DB=/tmp/pj-staging-hotaudit-drill.db
~/.venvs/pj-catalog/bin/python -m mcap_catalog_builder \
  --source s3 --s3-bucket "$PJ_STAGING_BUCKET" \
  --sqs-url "$MCAP_CATALOG_SQS_URL" \
  --db "$DB" --rescan-interval 21600 \
  --hot-audit-interval 60 --hot-audit-window-days 2 \
  --log-level INFO &
BUILDER_PID=$!
```

- [ ] **Step 2: Notification-outage drill** (§9 — the drill Phase 4 could not run, now meaningful with tier 2):

```bash
# 1. Break event delivery (temporarily disable notifications):
aws s3api put-bucket-notification-configuration --region "$PJ_STAGING_REGION" \
  --bucket "$PJ_STAGING_BUCKET" --notification-configuration '{}'
# 2. Upload under a registry-known combo (the drill combo from Task 5) with TODAY's date:
aws s3 cp "$F" "s3://$PJ_STAGING_BUCKET/customer=drill/customer_site=lab/robot=r1/source=ros-bags/date=$(date -u +%F)/outage-drill.mcap" --region "$PJ_STAGING_REGION"
# 3. No event will arrive. Within ~2 hot cadences (<=120 s here) the row must appear:
for i in $(seq 1 24); do
  N=$(sqlite3 "file:$DB?mode=ro" "SELECT COUNT(*) FROM files WHERE filename='outage-drill.mcap'" 2>/dev/null || echo 0)
  [ "$N" = 1 ] && { echo "HOT-AUDIT REPAIR PASS (${i}x5s)"; break; }; sleep 5
done
# 4. Restore notifications:
./scripts/staging-sqs-setup.sh --bucket "$PJ_STAGING_BUCKET" --region "$PJ_STAGING_REGION"
```

Expected: `HOT-AUDIT REPAIR PASS`. Also verify the sidecar:

```bash
python3 -c "import json;d=json.load(open('$DB.status.json'));print({k:d[k] for k in d if k.startswith('hot_audit_')})"
```

Expected: `hot_audit_outcome: ok`, `hot_audit_covered_prefixes >= 1`, recent `hot_audit_last`.

- [ ] **Step 3: Fixed-hour sanity** — restart the builder with `--full-audit-hour $(date -u -d '+2 minutes' +%H)` is NOT possible (hour granularity); instead verify the log line arithmetic: start with `--full-audit-hour 3` and confirm the startup log's first scheduled delay matches the wall-clock distance to 03:00 UTC (add a one-line INFO in `_run` if none exists: `logger.info("next full audit in %.0fs", ...)` — if you add it, keep it and cover it by eye, not by test). Then clean up:

```bash
kill $BUILDER_PID
aws s3 rm --recursive "s3://$PJ_STAGING_BUCKET/customer=drill/" --region "$PJ_STAGING_REGION"
rm -f "$DB" "$DB"-wal "$DB"-shm "$DB".status.json "$DB".writer.lock
```

- [ ] **Step 4: Record all drill outcomes** (freshness/kill9/outage latencies) in a short section of the PR body.

---

### Task 18: Documentation audit — THE MERGE GATE

Grep before trusting this list: `grep -rn "no-watch\|rescan-interval\|hot.audit\|sqs\|ERROR_S3_UNAVAILABLE\|catalog_failures" --include=*.md . | grep -v .worktrees | grep -v plans/`

- [ ] **Step 1: `docs/CATALOG_CONTRACT.md` §12** — extend the additive-fields sentence with: `hot_audit_last`/`hot_audit_outcome`/`hot_audit_duration`/`hot_audit_covered_prefixes`/`hot_audit_skipped_prefixes` (tier-2 result; sidecar-only by design), `maintenance_window_active`, `tag_edits_expired`/`tag_edits_failed`, and the reconcile counter `failures_pruned`. Then:

```bash
cp docs/CATALOG_CONTRACT.md mcap_catalog/CATALOG_CONTRACT.md
cmp docs/CATALOG_CONTRACT.md mcap_catalog/CATALOG_CONTRACT.md && echo CONTRACT-IN-SYNC
```

- [ ] **Step 2: `mcap_catalog/CLAUDE.md`** — Architecture section: add `hot_audit.py` to the module layering list; extend the `AuditCoordinator` sentence (dual cadence, single arbiter, fixed-hour option); note tier-3-only `catalog_failures` hygiene.

- [ ] **Step 3: `mcap_catalog/README.md` + `mcap_catalog/mcap_catalog_builder/README.md`** — document the three new flags and the SQS enablement pointer.

- [ ] **Step 4: Root `CLAUDE.md`** — "Current state" bullet: the remaining-follow-ups line still lists SQS phases as pending — update to "event tier code+staging-validated through Phase 6; prod enablement = ops runbook". Add TWO pins to "Decisions & pins":
  - *Hot audits are fail-closed per prefix and NEVER stamp `build_metadata`* — pinned by `test_hot_audit_deletes_only_inside_covered_prefixes` / `test_hot_audit_never_stamps_build_metadata`.
  - *A vanished object at session plan-build is `ERROR_NOT_FOUND`, not `ERROR_S3_UNAVAILABLE`; auth-shaped permanents (403) deliberately stay UNAVAILABLE* — pinned by `TestPlanBuildErrorCode`.

- [ ] **Step 5: Design doc** — `docs/plans/2026-07-30-builder-event-discovery-design.md` header: update **Status** to "Phases 0–6 implemented (2026-08-XX, this plan); Phase 7 gated (§5.4 prerequisite)". Update §7.1's "candidate" paragraph with a dated "implemented" note.

- [ ] **Step 6: Deploy docs** — `docs/ec2-deploy.md` (the SQS-tier note now points at a shipped overlay, verify wording), `server/deploy/README.md` (done in Tasks 4/5/16 — re-read it end-to-end once for coherence).

- [ ] **Step 7: `proto/pj_cloud.proto`** — comment-only: line ~331's `ERROR_NOT_FOUND` comment says "missing key fails the whole open before any storage read"; extend with "also returned when a cataloged object has vanished from the bucket at plan-build (deletion-staleness window)". **Comment change only — regenerating bindings must produce a byte-identical Go diff plus the comment; verify `git diff server/internal/wire/` shows only comment lines, or skip regen entirely (comments don't affect the wire).** Do NOT hand-edit generated Go: if regen is needed use protoc 3.21.12; if unsure, put the note in the design doc instead and leave the proto untouched.

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "docs: event-discovery Phases 3-6 documentation audit (contract §12 x2, CLAUDE.md pins, runbooks)"
```

---

### Task 19: Full gates + PR

- [ ] **Step 1: Everything, from scratch**

```bash
cd mcap_catalog && ~/.venvs/pj-catalog/bin/python -m pytest mcap_catalog_builder/tests/ -q; cd ..
cd server && go vet ./... && go test ./... && go test -race ./...; cd ..
shellcheck scripts/staging-sqs-setup.sh scripts/staging-event-drills.sh
make smoke
PJ_CI_BUILDER_PYTHON=~/.venvs/pj-catalog/bin/python3 scripts/ci-integration.sh
```

Expected: pytest all green (≈290+), Go green incl. race, `SMOKE PASS`, `CI-INTEGRATION PASS`. Smoke exercises the rescan path with the new coordinator — a regression there means Task 11 broke full-only mode.

- [ ] **Step 2: Adversarial review** (team rule for milestone boundaries): run a Codex review of the branch diff (`codex-exec` skill), plus `/code-review`. Fix confirmed findings; re-run Step 1.

- [ ] **Step 3: PR**

```bash
git push -u origin sqs-event-enablement
gh pr create --title "Event-discovery Phases 3-6: SQS tier live on staging, hot-window audit, nightly fixed-hour, ERROR_NOT_FOUND (§7.1)" --body "$(cat <<'EOF'
Implements the remaining non-gated phases of docs/plans/2026-07-30-builder-event-discovery-design.md:

- Phase 3: staging SQS/DLQ/notifications provisioned (scripts/staging-sqs-setup.sh, merge-guarded); real AWS payloads captured into translator tests.
- Phase 4: consumer wiring (run.sh MCAP_CATALOG_SQS_URL, compose events overlay); freshness + kill-9 drills PASSED on staging (latencies in the drills section below).
- Phase 5: tier-2 hot-window scoped audit — fail-closed per-prefix coverage, scoped SQLite lookups, never stamps build_metadata; single-arbiter dual-cadence scheduling; sidecar telemetry.
- Phase 6: --full-audit-hour fixed-UTC nightly (skip-missed, backoff capped at next slot) + catalog_failures hygiene.
- §7.1: vanished object at plan-build -> ERROR_NOT_FOUND via storage.ErrNotFound (403 deliberately NOT mapped).

NOT in scope: Phase 7 (InventoryFeed — gated on streaming reconcile), GCS Pub/Sub, prod enablement (ops runbook in server/deploy/README.md). Deferred: LifecycleExpiration payload capture (staging lifecycle rule armed; check queue after 1 day).

Drill results: <fill from Task 17 notes>

Documentation audit: performed per CLAUDE.md merge gate — CONTRACT §12 updated in BOTH copies (cmp-verified), CLAUDE.md pins added, deploy runbooks + design-doc status updated, proto comment note handled per Task 18 Step 7.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-review record (done at plan-writing time)

- **Spec coverage:** §3.1→Task 1-2, §3.2 captured-payload tests→Tasks 2-3, §3.6→Task 4, §8 Phase 3→Task 2, Phase 4→Tasks 4-5, §4.1→Task 8, §4.2→Task 9 (+Task 6's complete-or-raise primitive), §4.3 honesty→`test_hot_audit_ignores_dates_outside_window` + doc notes, §5.2 arbiter/supersede→Task 11, fixed-hour/skip-missed→Task 13, §5.3 hygiene→Task 14, §6 sidecar→Task 10 (+`feed_snapshot_age` correctly excluded as Phase 7), §7.1→Task 15, §9 unit matrix→Tasks 3/9/11/13/14 tests, §9 live validation→Tasks 5/17. Phase 7 explicitly out (gated).
- **Known simplifications (deliberate):** hot-audit failures reschedule at the plain cadence (no fast backoff — a hot tick self-heals); hot audit passes `progress=None` into the shared engine so tier-2 runs never mutate the `phase` fields (§4.2's sidecar-only rule); `request_due()` remains full-only.
- **Type consistency check:** `AuditItem(audit_kind=…)` matches Tasks 11 tests; `hot_audit` kwargs (`window_days`, `today`, `stop_event`) consistent across Tasks 9/11; `list_prefix(prefix, stop_event=…)` consistent across Tasks 6/9; sidecar field names in Task 10 == Task 18's contract list.
