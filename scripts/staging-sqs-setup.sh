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
#
# --dry-run prints every mutating aws command instead of running it (queue
# URLs/ARNs are stand-ins, since nothing was created to look up).
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
[ -n "$BUCKET" ] && [ -n "$REGION" ] || {
  echo "usage: $0 --bucket B --region R [--prefix P] [--name-base N] [--dry-run]" >&2
  exit 2
}

# Mutating calls go through run(): dry mode prints them; real mode swallows
# stdout (create-queue's JSON blob) — errors still surface on stderr.
run() { if [ "$DRY_RUN" = 1 ]; then echo "DRY: $*"; else "$@" >/dev/null; fi; }

QUEUE="${NAME_BASE}-events"
DLQ="${NAME_BASE}-dlq"
RETENTION=345600   # 4 days (§3.1: covers an initial build + a prolonged outage)
VISIBILITY=300     # §3.1

echo "== DLQ: $DLQ"
run aws sqs create-queue --region "$REGION" --queue-name "$DLQ" \
  --attributes "{\"MessageRetentionPeriod\":\"$RETENTION\"}" || true
if [ "$DRY_RUN" = 1 ]; then
  DLQ_URL="DRY-DLQ-URL" DLQ_ARN="arn:aws:sqs:$REGION:000000000000:$DLQ"
else
  DLQ_URL=$(aws sqs get-queue-url --region "$REGION" --queue-name "$DLQ" \
    --query QueueUrl --output text)
  DLQ_ARN=$(aws sqs get-queue-attributes --region "$REGION" --queue-url "$DLQ_URL" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)
fi

echo "== queue: $QUEUE (retention 4d, visibility ${VISIBILITY}s, redrive after 5)"
run aws sqs create-queue --region "$REGION" --queue-name "$QUEUE" --attributes "{
  \"MessageRetentionPeriod\":\"$RETENTION\",
  \"VisibilityTimeout\":\"$VISIBILITY\",
  \"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$DLQ_ARN\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"
}" || true
if [ "$DRY_RUN" = 1 ]; then
  QUEUE_URL="DRY-QUEUE-URL" QUEUE_ARN="arn:aws:sqs:$REGION:000000000000:$QUEUE"
else
  QUEUE_URL=$(aws sqs get-queue-url --region "$REGION" --queue-name "$QUEUE" \
    --query QueueUrl --output text)
  QUEUE_ARN=$(aws sqs get-queue-attributes --region "$REGION" --queue-url "$QUEUE_URL" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)
fi

echo "== queue policy (allow s3.amazonaws.com from arn:aws:s3:::$BUCKET)"
POLICY="{\"Version\":\"2012-10-17\",\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"Service\":\"s3.amazonaws.com\"},\"Action\":\"sqs:SendMessage\",\"Resource\":\"$QUEUE_ARN\",\"Condition\":{\"ArnLike\":{\"aws:SourceArn\":\"arn:aws:s3:::$BUCKET\"}}}]}"
POLICY_ATTR=$(printf '%s' "$POLICY" | python3 -c 'import json,sys; print(json.dumps({"Policy": sys.stdin.read()}))')
run aws sqs set-queue-attributes --region "$REGION" --queue-url "$QUEUE_URL" \
  --attributes "$POLICY_ATTR"

echo "== bucket notification configuration (merge-guarded)"
if [ "$DRY_RUN" = 1 ]; then
  EXISTING="{}"
else
  EXISTING=$(aws s3api get-bucket-notification-configuration --region "$REGION" \
    --bucket "$BUCKET")
fi
if [ -n "$EXISTING" ] && [ "$EXISTING" != "{}" ] && \
   ! printf '%s' "$EXISTING" | grep -q "$QUEUE_ARN"; then
  {
    echo "REFUSING: $BUCKET already has a notification configuration this script"
    echo "does not own (Put REPLACES the whole document). Merge manually:"
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
