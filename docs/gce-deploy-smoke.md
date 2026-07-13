# GCE Deployment Smoke (Asensus) — real ADC + persistent-disk catalog survival

**Status:** operator runbook for a **pending** real-deployment validation (the
real-bucket M1 gate). Extracted 2026-06-22 from the archived Plan C "Task 8a"
before that doc was deleted; adapted to the current repo's tooling.

## Why this exists

The emulator matrix (`make matrix`, the CI `{s3,gcs}` legs) proves wire / format /
protocol correctness against MinIO + fake-gcs, but it **cannot** validate the real
Asensus deployment shape:

- a long-lived server container on a **GCE VM**,
- reading the bucket via **Application Default Credentials from an
  instance-attached service account** — **no key file on disk**,
- with a **persistent disk** holding `catalog.db` that must survive a VM/container
  restart **without a full re-scan**.

Real GCE + ADC + the metadata server can't run in GitHub-hosted CI, so this is a
**manual checklist** (or a scheduled *self-hosted* GCE runner job). It is the
operational expression of the unified plan's M2c-ASEN acceptance gate.

> **Emulator-fidelity caveat (load-bearing):** run **≥1 real-GCS and ≥1 real-S3**
> deploy smoke before declaring the cloud backend a proven drop-in. The emulators
> are high-fidelity but not authoritative for ADC, the metadata server, TLS, and
> persistent-disk survival.

## Preconditions (on the GCE VM)

- Server container running and serving TLS (e.g. via `server/deploy/` —
  `Dockerfile` + `pj-cloud-server.service` + `config.asensus-staging.yaml`).
- The VM's **attached service account** has bucket-read scope
  (`roles/storage.objectViewer`); **no** `GOOGLE_APPLICATION_CREDENTIALS`, **no**
  key file on disk (ADC resolves via the metadata server).
- A **persistent disk** mounted at `/var/lib/pj-cloud` holding `catalog.db`
  (`mkfs.ext4` once, `/etc/fstab` mount so it survives reboots).
- `dexory-cloud-cli` available on the VM (built by `./build.sh
  toolbox_dexory_cloud`).

## The five checks

1. **ADC, no key on disk** — `GOOGLE_APPLICATION_CREDENTIALS` is unset AND the
   metadata server vends a token for the attached SA.
2. **`/health` is 200 over TLS.**
3. **Catalog lists the real bucket via ADC** (≥1 file).
4. **A streaming session round-trips over `wss://`** (download → non-empty MCAP).
5. **`catalog.db` survives a restart without a full re-scan** — after restarting
   the container, the file count is immediately the same (a full re-scan would
   briefly show 0 and rewrite every row); assert via the
   `pj_cloud_indexer_full_scans_total` (current) metric.

## `scripts/gce_smoke.sh` (run ON the GCE VM)

```bash
#!/usr/bin/env bash
# gce_smoke.sh — Asensus GCE deployment smoke. Run ON the GCE VM.
# Preconditions above: server container running, attached SA with bucket-read
# scope, persistent disk at /var/lib/pj-cloud, NO key file on disk.
set -euo pipefail

SERVER=${SERVER:-https://localhost:8443}          # TLS health/metrics endpoint
WSS=${WSS:-wss://localhost:8443}                  # ws endpoint (DEXORY_CLOUD_URL)
TOKEN=${PJ_CLOUD_TOKEN:-}                          # bearer token if auth enabled
CLI=${PJCLOUD_CLI:-dexory-cloud-cli}
DB=${CATALOG_DB:-/var/lib/pj-cloud/catalog.db}
CONTAINER=${CONTAINER:-pj-cloud-server}

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "== 1. ADC: confirm NO service-account key file on disk =="
[[ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]] \
  || fail "GOOGLE_APPLICATION_CREDENTIALS is set — ADC-via-attached-SA requires NO key on disk"
curl -fsS -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token' \
  >/dev/null || fail "metadata server did not vend an ADC token (is an SA attached?)"
echo "   ok: no key on disk; metadata server vends an ADC token"

echo "== 2. /health is 200 over TLS =="
curl -fsS -k "$SERVER/health" >/dev/null || fail "/health not healthy"
echo "   ok: /health 200"

echo "== 3. Catalog lists the real GCS bucket (via ADC) =="
N=$("$CLI" --url "$WSS" --insecure list --json \
     | python3 -c 'import sys,json; print(len(json.load(sys.stdin)))')
[[ "$N" -ge 1 ]] || fail "catalog returned 0 files — ADC bucket read failed"
echo "   ok: catalog lists $N sequence(s) via ADC"

echo "== 4. Streaming session round-trips over wss:// =="
SEQ=$("$CLI" --url "$WSS" --insecure list --json \
       | python3 -c 'import sys,json; print(json.load(sys.stdin)[0]["name"])')
OUT=$(mktemp /tmp/gce-rebuilt.XXXXXX.mcap)
"$CLI" --url "$WSS" --insecure download "$SEQ" --output "$OUT"
[[ -s "$OUT" ]] || fail "session download produced an empty MCAP"
echo "   ok: streamed '$SEQ' to $OUT ($(stat -c%s "$OUT") bytes) over wss"

echo "== 5. Persistent-disk catalog survives a restart WITHOUT a full re-scan =="
[[ -f "$DB" ]] || fail "catalog.db not found on the persistent disk at $DB"
sudo docker restart "$CONTAINER" >/dev/null
for _ in $(seq 1 30); do curl -fsS -k "$SERVER/health" >/dev/null 2>&1 && break; sleep 2; done
METRIC=$(curl -fsS -k "$SERVER/metrics" | grep -E '^pj_cloud_indexer_full_scans_total' || true)
N_AFTER=$("$CLI" --url "$WSS" --insecure list --json \
           | python3 -c 'import sys,json; print(len(json.load(sys.stdin)))')
[[ "$N_AFTER" -eq "$N" ]] || fail "file count changed after restart ($N -> $N_AFTER) — catalog did not survive"
echo "   ok: catalog.db survived restart; $N_AFTER served immediately (scan metric: ${METRIC:-n/a})"

echo "ALL GCE SMOKE CHECKS PASSED"
```

## How to run

- **Manual:** SSH to the VM, ensure the preconditions, run `gce_smoke.sh`. Expect
  `ALL GCE SMOKE CHECKS PASSED`.
- **Scheduled (optional):** a `workflow_dispatch` + weekly-cron GitHub workflow
  (`gce-smoke.yml`) targeting a **self-hosted runner** registered on (or with
  deploy access to) the GCE VM. It does NOT run on GitHub-hosted runners (no real
  metadata server / ADC there). If no self-hosted runner is registered, this
  manual checklist is the fallback.

## Adapting under the auryn migration (two-process deploy)

Once the catalog writer is the separate **Python `mcap_catalog_builder`** (see
[`auryn-catalog-migration-plan.md`](auryn-catalog-migration-plan.md)) and the Go
server is **read-only**:

- Check 5's "no full re-scan after restart" becomes "the **Python builder**
  warm-starts with zero re-extracts AND the Go reader serves immediately from the
  existing `catalog.db`." Prefer the new `pj_cloud_catalog_*` freshness series
  (migration-plan 6.5) over `pj_cloud_indexer_full_scans_total`.
- The restart check should also confirm the **atomic-publish** invariant
  (migration-plan 6.2a): the reader never serves a torn catalog while the builder
  rebuilds.
- ADC for GCS is already SDK-handled (`STORAGE_EMULATOR_HOST` unset in prod); the
  builder uses the same attached-SA ADC path.
