#!/usr/bin/env bash
# staging-sqs-setup.sh — provision the S3->SQS event-discovery infra (design
# 2026-07-30 §3.1) for ONE bucket, idempotently.
#
#   staging-sqs-setup.sh --bucket <name> --region <region> \
#                        {--prefix <p> | --whole-bucket} \
#                        [--name-base pj-cloud-catalog] [--dry-run]
#
# Creates <name-base>-events + <name-base>-dlq (retention 4 days, visibility
# 300 s, redrive after 5 receives), grants s3.amazonaws.com SendMessage from
# the bucket ARN, and installs the bucket notification configuration
# (ObjectCreated:* / ObjectRemoved:* / LifecycleExpiration:* filtered to
# suffix=.mcap [+ prefix]).
#
# The notification PREFIX must match the builder's --s3-prefix EXACTLY (the
# event worker catalogs whatever key an event carries — an out-of-scope event
# would be cataloged and then swept by the prefix-scoped full audit, churning
# forever). It is therefore a REQUIRED choice: pass --prefix <builder prefix>
# or the explicit --whole-bucket (matching an empty --s3-prefix).
#
# FAIL-CLOSED ownership (Codex consult 2026-08-10): the bucket's existing
# notification configuration must be EMPTY or EXACTLY the single queue
# configuration this script owns (id <name-base>-mcap-events, nothing else —
# no Topic/Lambda/EventBridge entries). Anything else aborts: Put REPLACES the
# whole document, so "merging" into a foreign config would silently drop
# someone else's targets. Queue attributes are CONVERGED on every run
# (set-queue-attributes), so a pre-existing queue with drifted
# retention/visibility/redrive is corrected, not silently kept.
#
# IAM needed by the CALLER of this script: sqs:CreateQueue, sqs:GetQueueUrl,
# sqs:GetQueueAttributes, sqs:SetQueueAttributes on both queue names;
# s3:GetBucketNotification + s3:PutBucketNotification on the bucket.
#
# --dry-run prints every mutating aws command instead of running it (queue
# URLs/ARNs are stand-ins, since nothing was created to look up).
set -euo pipefail

BUCKET="" REGION="" PREFIX="" NAME_BASE="pj-cloud-catalog" DRY_RUN=0 WHOLE_BUCKET=0
while [ $# -gt 0 ]; do
  case "$1" in
    --bucket)       BUCKET="$2"; shift 2 ;;
    --region)       REGION="$2"; shift 2 ;;
    --prefix)       PREFIX="$2"; shift 2 ;;
    --whole-bucket) WHOLE_BUCKET=1; shift ;;
    --name-base)    NAME_BASE="$2"; shift 2 ;;
    --dry-run)      DRY_RUN=1; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done
usage() {
  echo "usage: $0 --bucket B --region R {--prefix P | --whole-bucket} [--name-base N] [--dry-run]" >&2
  exit 2
}
[ -n "$BUCKET" ] && [ -n "$REGION" ] || usage
# The prefix decision is explicit, never a silent default (see header).
if [ "$WHOLE_BUCKET" = 1 ] && [ -n "$PREFIX" ]; then usage; fi
if [ "$WHOLE_BUCKET" = 0 ] && [ -z "$PREFIX" ]; then usage; fi

# Mutating calls go through run(): dry mode prints them; real mode swallows
# stdout (create-queue's JSON blob) — errors still surface on stderr.
run() { if [ "$DRY_RUN" = 1 ]; then echo "DRY: $*"; else "$@" >/dev/null; fi; }

QUEUE="${NAME_BASE}-events"
DLQ="${NAME_BASE}-dlq"
RETENTION=345600   # 4 days (§3.1: covers an initial build + a prolonged outage)
VISIBILITY=300     # §3.1

# ensure_queue NAME: idempotently create NAME and set QURL/QARN (dry-run
# stand-ins included). Callers converge their own attributes afterwards, so a
# pre-existing queue with drifted values is corrected, not silently kept.
ensure_queue() {
  local name="$1"
  run aws sqs create-queue --region "$REGION" --queue-name "$name" || true
  if [ "$DRY_RUN" = 1 ]; then
    QURL="DRY-${name}-URL" QARN="arn:aws:sqs:$REGION:000000000000:$name"
  else
    QURL=$(aws sqs get-queue-url --region "$REGION" --queue-name "$name" \
      --query QueueUrl --output text)
    QARN=$(aws sqs get-queue-attributes --region "$REGION" --queue-url "$QURL" \
      --attribute-names QueueArn --query Attributes.QueueArn --output text)
  fi
}

echo "== DLQ: $DLQ"
ensure_queue "$DLQ"
DLQ_URL="$QURL" DLQ_ARN="$QARN"
run aws sqs set-queue-attributes --region "$REGION" --queue-url "$DLQ_URL" \
  --attributes "{\"MessageRetentionPeriod\":\"$RETENTION\"}"

echo "== queue: $QUEUE (retention 4d, visibility ${VISIBILITY}s, redrive after 5)"
ensure_queue "$QUEUE"
QUEUE_URL="$QURL" QUEUE_ARN="$QARN"
run aws sqs set-queue-attributes --region "$REGION" --queue-url "$QUEUE_URL" --attributes "{
  \"MessageRetentionPeriod\":\"$RETENTION\",
  \"VisibilityTimeout\":\"$VISIBILITY\",
  \"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"
}"

echo "== queue policy (allow s3.amazonaws.com from arn:aws:s3:::$BUCKET)"
POLICY="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"s3.amazonaws.com\"},\"Action\":\"sqs:SendMessage\",\"Resource\":\"$QUEUE_ARN\",\"Condition\":{\"ArnLike\":{\"aws:SourceArn\":\"arn:aws:s3:::$BUCKET\"}}}]}"
POLICY_ATTR=$(printf '%s' "$POLICY" | python3 -c 'import json,sys; print(json.dumps({"Policy": sys.stdin.read()}))')
run aws sqs set-queue-attributes --region "$REGION" --queue-url "$QUEUE_URL" \
  --attributes "$POLICY_ATTR"

echo "== bucket notification configuration (ownership-guarded)"
if [ "$DRY_RUN" = 1 ]; then
  EXISTING="{}"
else
  EXISTING=$(aws s3api get-bucket-notification-configuration --region "$REGION" \
    --bucket "$BUCKET")
fi
# Structural guard: proceed ONLY if the existing configuration is empty, or is
# exactly one QueueConfiguration with our id and NOTHING else (no Topic/
# Lambda/EventBridge entries). A grep-for-our-ARN guard would pass a MIXED
# document and the Put below would then delete the foreign targets.
OWNED=$(printf '%s' "$EXISTING" | python3 -c "
import json, sys
doc = json.loads(sys.stdin.read() or '{}')
qs = doc.get('QueueConfigurations', [])
others = [k for k in doc if k != 'QueueConfigurations']
if not doc:
    print('empty')
elif not others and len(qs) == 1 and qs[0].get('Id') == '${NAME_BASE}-mcap-events':
    print('ours')
else:
    print('foreign')
")
if [ "$OWNED" = foreign ]; then
  {
    echo "REFUSING: $BUCKET already has a notification configuration that is not"
    echo "exactly the single ${NAME_BASE}-mcap-events queue entry (Put REPLACES"
    echo "the whole document, which would drop the other targets). Merge manually:"
    printf '%s\n' "$EXISTING"
  } >&2
  exit 3
fi
FILTER_RULES="[{\"Name\":\"suffix\",\"Value\":\".mcap\"}"
if [ -n "$PREFIX" ]; then
  FILTER_RULES="$FILTER_RULES,{\"Name\":\"prefix\",\"Value\":\"$PREFIX\"}"
fi
FILTER_RULES="$FILTER_RULES]"
NOTIF="{\"QueueConfigurations\":[{\"Id\":\"${NAME_BASE}-mcap-events\",\"QueueArn\":\"$QUEUE_ARN\",\"Events\":[\"s3:ObjectCreated:*\",\"s3:ObjectRemoved:*\",\"s3:LifecycleExpiration:*\"],\"Filter\":{\"Key\":{\"FilterRules\":$FILTER_RULES}}}]}"
run aws s3api put-bucket-notification-configuration --region "$REGION" \
  --bucket "$BUCKET" --notification-configuration "$NOTIF"

echo
echo "queue URL: $QUEUE_URL"
echo "Pass this as MCAP_CATALOG_SQS_URL / --sqs-url (Phase 4)."
