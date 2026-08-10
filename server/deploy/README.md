# pj-cloud-server deploy artifacts

Operational artifacts for shipping the PJ Cloud Connector backend. Since the M6
catalog-migration cutover this is a **TWO-PROCESS system** — say this plainly,
because it wasn't always true:

- **`builder`** — the Python `mcap_catalog` package (vendored directly under
  `mcap_catalog/` as regular source files, NOT a git submodule; `pip install`-able
  deps only, no repo checkout needed at runtime beyond the package itself). It is
  the **SOLE catalog writer**: it scans the bucket, writes the SQLite catalog, and serves the
  tag-edit UNIX-socket IPC (`docs/CATALOG_CONTRACT.md` §10) that is now the
  *only* way a tag edit can be applied.
- **`server`** (`pj-cloud-server`, the Go binary) — a **pure read-only catalog
  reader + unchanged streamer**. It opens the builder's SQLite DB
  `mode=ro` and forwards `UpdateTags` calls over the tag-edit IPC socket. It
  has **no writer path of its own** — `catalog.OpenReadOnly` fails fast
  (process exits 1) if the DB doesn't exist yet, by design.

**The "one static binary" property is GONE.** A from-scratch deploy needs both
processes running, sharing one local volume (`/var/lib/pj-cloud`: the SQLite
DB + WAL/SHM sidecars, and the tag-edit UNIX socket). See "Two-process
deployment" below before adapting any of this to your own infra.

| File | Purpose |
|---|---|
| `Dockerfile` | Multi-stage build of the Go server → `gcr.io/distroless/static-debian12:nonroot`. |
| `Dockerfile.builder` | Build of the Python catalog builder daemon (`python:3.12-slim`). |
| `docker-compose.yml` | Minio + `builder`, each with its own container healthcheck; `server` is gated on `builder`'s health and probed externally via `/health` (no container healthcheck of its own — distroless); config/volumes mounted. |
| `docker-compose.aws.yml` | **S3 use case** deploy (real AWS bucket, no Minio, IAM-role creds) — `builder` + `server` only. See `docs/ec2-deploy.md`. |
| `deploy.config.yaml` | Compose-tuned server config (plaintext :8080, `minio:9000` endpoint, `tag_ipc_socket` pointed at the shared volume). |
| `config.aws-ec2.yaml` | Server config for the S3-use-case Compose deploy (real S3, empty creds = IAM role, shared-volume DB/socket paths). |
| `config.example.yaml` | The FULL server config surface, commented, field-verified against `config.go`. |
| `pj-cloud-server.service` | systemd unit for the Go server, bare-metal deploy. |
| `pj-cloud-builder.service` | systemd unit for the Python builder daemon, bare-metal deploy. |

For a step-by-step **EC2** walkthrough (Docker Compose, IAM instance role, IMDS
hop-limit, security group, TLS) see `docs/ec2-deploy.md`.

## Two-process deployment (read this first)

Both processes need **exactly one thing** from each other: a shared local
directory holding the SQLite catalog DB and a UNIX socket. The rules below are
LOCKED (D2 design review) — deviating from them risks silent catalog
corruption or a tag-edit path that silently stops working:

1. **The shared volume MUST be a real local filesystem — never NFS/EFS or any
   network filesystem.** SQLite's WAL mode requires real mmap'd shared-memory
   byte-range locking that network filesystems do not implement correctly
   (silent corruption risk on concurrent access); a UNIX socket also cannot be
   served correctly off most network filesystems. A Docker named volume on
   the host's local storage driver, or a plain local directory for a
   bare-metal deploy, are both fine. This is why `docker-compose.yml` mounts
   one named volume (`catalog-data`) into **only** the `builder` and `server`
   services — no other service gets it.
2. **Socket + DB permissions: one coherent ownership scheme, picked once per
   deploy shape.** This repo picks:
   - **Compose:** both containers run as the **same numeric UID 65532** (the
     Go image's existing nonroot user; `Dockerfile.builder` now also runs as
     `65532:65532`). Same UID means ordinary owner read/write is enough — no
     shared-group juggling across two different container images/distros.
   - **systemd (bare metal):** both units run as the **same `pjcloud`
     user/group**, sharing `/var/lib/pj-cloud` via each unit's
     `ReadWritePaths=`.
   Either scheme is "one coherent ownership model" — don't mix them (e.g.
   don't make the compose services use different UIDs while relying on a
   shared group; nothing in these artifacts sets that up).
3. **Stale-socket unlink is builder-side, already implemented.** If the
   builder is killed uncleanly, the next startup unlinks any leftover
   `tag.sock` itself before rebinding — nothing extra to configure.
4. **First-boot ordering: NONE NEEDED (degraded start).** The server boots
   without a catalog in EVERY deploy shape: `/health` answers 503 `waiting
   for first catalog build (builder: extracting 123/456)` (the progress
   detail comes from the builder's `<db>.status.json` sidecar —
   CATALOG_CONTRACT.md §12), catalog RPCs fail fast with the retryable
   `ERROR_CATALOG_UNAVAILABLE`, and the 30 s reopen tick opens the catalog
   the moment the builder's first atomic publish lands. Consequences:
   - **Compose:** `server` depends on `builder` only with
     `condition: service_started`; the builder's healthcheck now means
     "alive and progressing, not fatally failed" (status sidecar fresh and
     `phase != "error"`), NEVER "first build complete" — deploy success is
     no longer a function of bucket size. A builder with broken credentials
     goes unhealthy within ~1 minute with the reason in the sidecar's
     `last_error`.
   - **systemd (bare metal):** the old crash-loop-until-published dance is
     gone — the server unit starts cleanly on the first attempt and simply
     reports 503 until the catalog appears. `Restart=on-failure` +
     `StartLimitIntervalSec=0` stay as ordinary crash protection.
   Watch `docker compose logs -f builder` / `journalctl -u
   pj-cloud-builder` for the periodic reconcile-progress lines, or read the
   sidecar directly.
5. **Every SUCCESSFUL tag edit is logged server-side with the client's WS
   remote address.** The WS `UpdateTags` handler logs `remote` before
   forwarding to the builder's IPC socket; the failure paths (bad file id,
   IPC unavailable, ...) log the same field too — nothing to enable here.
6. **Exactly ONE builder per served DB — now kernel-enforced.** The builder
   takes an exclusive `flock` on `<db_path>.writer.lock` at startup (before any
   DB write or socket bind) and holds it for its lifetime; a SECOND builder
   started on the same `--db` fails fast with **exit code 3** (naming the holder
   PID) instead of interleaving writes or stealing the tag socket
   (CATALOG_CONTRACT.md §11). No config needed — but keep restart policies from
   double-starting it: Compose uses `restart: unless-stopped` on the builder,
   and a systemd deploy must run a single builder unit per DB. The kernel drops
   the lock on any process death, so a crash never leaves a stale lock.

## Container images

Prerequisite for the builder image: `mcap_catalog/` is now VENDORED directly in
this repo (regular source files, not a git submodule), so a plain `git clone`
already contains it — no submodule init is needed and the `COPY` step just works.

```bash
# Go server (build context is server/, Dockerfile lives under deploy/)
docker build -t pj-cloud-server:dev -f server/deploy/Dockerfile server
# Python builder (build context is the REPO ROOT — needs mcap_catalog/, a
# sibling of server/, not reachable from a server/-scoped context)
docker build -t pj-cloud-builder:dev -f server/deploy/Dockerfile.builder .
# or just: make docker   (server image only; see docker-compose.yml for both)
```

The server image is distroless/static (no shell, no curl): probe liveness over
HTTP from outside the container —
`curl -fsS http://HOST:8080/health` returns 503 `waiting for first catalog
build (...)` while the builder's first scan runs, then `ok`. Config is supplied with `--config /etc/pj-cloud/config.yaml` (the
default `CMD`); mount your config there. Secrets come from the environment
(`PJ_CLOUD_TOKEN`, `PJ_CLOUD_S3_*`, `PJ_CLOUD_DASHBOARD_PASSWORD`,
`PJ_CLOUD_TLS_*`); the config's `${ENV}` references are expanded at load.

The builder image ships its own storage/tag-socket defaults as `CMD`
(`--no-watch --tag-socket /var/lib/pj-cloud/tag.sock --db
/var/lib/pj-cloud/catalog.db`) but has **no default bucket** — S3 vs GCS,
bucket, and prefix are deployment-specific, so a real deploy always overrides
`command:`/`ExecStart` with the full argument list (see `docker-compose.yml`'s
`builder` service or `pj-cloud-builder.service`'s header for both shapes).
Credentials come from the environment exactly like the Go server's storage
credentials (AWS_* env / the AWS default credential chain, or
`GOOGLE_APPLICATION_CREDENTIALS` / ADC for GCS).

## docker compose (builder + server + Minio)

`docker-compose.yml` stands the whole backend up on one box: Minio (bucket
`recordings`, `admin`/`password123`), a one-shot `minio-init` that creates the
bucket, the `builder` service (writes the catalog + serves tag-edit IPC), and
the `server` service (reads that catalog, forwards tag edits) pointed at
`http://minio:9000` via `deploy.config.yaml`.

```bash
cd server/deploy
# Upload your recordings to s3://recordings first (nothing in this stack writes objects):
#   mc alias set local http://localhost:9000 admin password123
#   mc cp *.mcap local/recordings/
# PJ_CLOUD_TOKEN is REQUIRED — the server is fail-closed and refuses to start without
# it (or set PJ_CLOUD_ALLOW_ANONYMOUS=1 to run with no auth on purpose):
PJ_CLOUD_TOKEN=changeme docker compose up -d --build   # completes in seconds — no build gate
docker compose logs -f builder     # periodic reconcile-progress lines
curl -fsS http://localhost:8080/health   # 503 "waiting for first catalog build (...)" -> ok
```

- Ports: `8080` (ws:// + http dashboard/health/metrics), `9000`/`9001` (Minio API/console).
- The SQLite catalog + tag-edit socket persist in the `catalog-data` volume
  (shared by `builder` and `server` only); bucket data in `minio-data`.
- For a REAL S3/GCS deploy, don't hand-edit this local stack — use the dedicated
  templates and runbooks:
  - **S3:** `config.aws-ec2.yaml` + `docker-compose.aws.yml` (fill in the `REPLACE_ME`
    bucket + region + prefix); full walkthrough in `docs/ec2-deploy.md`.
  - **GCS:** `config.gcs-staging.yaml` (fill in `REPLACE_ME_with_your_gcs_bucket` +
    prefix; ADC for credentials); see `docs/gce-deploy-smoke.md`.

## systemd (bare metal)

Install **both** `pj-cloud-server.service` and `pj-cloud-builder.service`
under a shared `pjcloud` user with `ProtectSystem=strict`. Install steps,
the secrets `EnvironmentFile` layout, and the builder's storage-target
argument convention (`BUILDER_ARGS` in `/etc/pj-cloud/builder.env`) are
documented in each unit file's header comments — read `pj-cloud-builder.service`
first, since the server unit depends on it having run at least once.

## S3 event notifications (SQS) — enabling event-driven discovery

Design: `docs/plans/2026-07-30-builder-event-discovery-design.md` (§3, §8).
The builder ships the full event tier (ack-hardened SQS intake, tier-2
hot-window audit, fixed-hour full audits), but every deploy shape above still
runs `--no-watch` (rescan-only). Enablement is a phased ops change — create
the infra (Phase 3), burn in the consumer (Phase 4), turn on the hot audit
(Phase 5), and only then loosen the full audit to nightly (Phase 6).

### Phase 3 — provision the queue + notifications

```bash
scripts/staging-sqs-setup.sh --bucket <bucket> --region <region> \
    {--prefix <builder --s3-prefix value> | --whole-bucket}
```

Idempotent: DLQ + events queue (retention 4 d, visibility 300 s, redrive after
5 receives), the s3.amazonaws.com queue policy, and the bucket notification
configuration (`ObjectCreated:*`/`ObjectRemoved:*`/`LifecycleExpiration:*`,
`suffix=.mcap`). Two deliberate guards:

- **The notification prefix must equal the builder's `--s3-prefix` exactly**
  (the flag choice is mandatory): the event worker catalogs whatever key an
  event carries, so an out-of-scope event would be cataloged and then swept by
  the prefix-scoped full audit — churn forever.
- **Ownership is structural, fail-closed**: the script proceeds only if the
  bucket's existing notification configuration is empty or exactly its own
  single entry — `PutBucketNotificationConfiguration` REPLACES the whole
  document, so it never "merges" into a foreign config (exit 3 instead).

Caller IAM for the script: `sqs:CreateQueue/GetQueueUrl/GetQueueAttributes/
SetQueueAttributes`, `s3:GetBucketNotification/PutBucketNotification`.
Builder-runtime IAM (on EC2: add to the instance role from
`docs/ec2-deploy.md`): `sqs:ReceiveMessage`, `sqs:DeleteMessage`,
`sqs:ChangeMessageVisibility`, `sqs:GetQueueAttributes` on the queue ARN.

Phase 3 is safe to do **early**, well before enabling the consumer: only
objects uploaded *after* the notification config exists generate events, and
the queue accumulates them durably (4-day retention) until Phase 4 drains it.
**Never purge the events queue** — the accumulated backlog is exactly what
Phase 4's consumer is supposed to drain; purging discards real changes.

**Capturing real payloads for the translator test fixtures** (the committed
fixtures under `mcap_catalog/mcap_catalog_builder/tests/data/s3_events/` are
SYNTHESIZED — replace them with captured ones when access allows): do it
during Phase 3, while the consumer is off. Upload + delete a drill object
under the notification prefix, `aws sqs receive-message` the bodies into
files named by family (`create_*`/`delete_*`/`ack_*` — the test derives the
expectation from the prefix), sanitize account identifiers, and let the
messages redeliver or expire naturally. For `LifecycleExpiration:*` payloads,
add a 1-day expiry lifecycle rule on a dedicated drill prefix — but NOTE:
`put-bucket-lifecycle-configuration` REPLACES all lifecycle rules, so fetch
and re-merge the existing rules, and capture the expiry event before the
consumer is enabled (a running consumer acks it away).

### Phase 4 — enable the consumer + burn-in drills

Compose: use the overlay (it swaps `--no-watch` for `--sqs-url` and turns on
the tier-2 hot audit):

```bash
PJ_CLOUD_S3_BUCKET=<bucket> PJ_CLOUD_S3_PREFIX=<prefix> \
MCAP_CATALOG_SQS_URL=<queue url> \
docker compose -f docker-compose.aws.yml -f docker-compose.aws.events.yml up -d
```

systemd: the unit's `ExecStart` hardcodes `--no-watch`, so `BUILDER_ARGS`
alone cannot enable events — install a drop-in that replaces `ExecStart`
(see the "Event-mode drop-in" comment block in `pj-cloud-builder.service`).

Dev box against staging: `MCAP_CATALOG_SQS_URL=<url> ./run.sh --aws`.

Keep the 6 h `--rescan-interval` until the burn-in drills pass. Alarm on SQS
`ApproximateAgeOfOldestMessage` and DLQ depth > 0; the sidecar's
`producer_last_poll_ok_at` distinguishes a dead intake thread from a quiet
bucket.

**Burn-in drills** (design §9 — record the results before loosening anything):

1. **Freshness** (scripted): with the consumer running,
   `scripts/staging-event-drills.sh freshness --bucket <b> --region <r> --db <db>`
   uploads a fixture under a drill Hive key, asserts the catalog row appears,
   deletes the object, asserts the row disappears — PASS/FAIL with latencies.
2. **kill −9 mid-batch** (redelivery proof): upload ~5 objects, `kill -9` the
   builder while they are in flight, restart it, and verify within
   ~VisibilityTimeout (300 s) that all rows exist exactly once (idempotency
   absorbs the replay; the UNIQUE composite makes duplicates impossible).
3. **Notification outage** (run only once Phase 5 is on): note a sentinel
   object is absent from the catalog, disable the bucket notification config
   (SAVE the original document first and restore it in a shell `trap` — an
   interrupted drill must not leave notifications off), upload the sentinel
   under a registry-known combo with today's date, and verify the hot audit
   catalogs it within ~2 cadences — fail explicitly on timeout, not silently.
   Run the builder from `mcap_catalog/` (`python -m mcap_catalog_builder`
   resolves the package from the cwd), and wait for the sidecar to report
   `phase: idle` before starting the drill so the initial full scan cannot
   produce a false pass.

### Phases 5–6 — hot audit + nightly fixed hour

The overlay already passes `--hot-audit-interval=1800` (tier 2: scoped
hot-window audit; fail-closed per prefix; status in the sidecar's
`hot_audit_*` fields only — it never stamps `build_metadata`). After tiers
1–2 have burned in, uncomment `--full-audit-hour=<utc hour>` in the overlay
(or the systemd drop-in) to move the full audit to a nightly fixed off-peak
hour — the safety net loosens LAST (design §8). While a full audit runs, the
sidecar shows `maintenance_window_active: true` (events pause, tag edits can
expire — the declared ~30 min nightly window, design §5.2).

## TLS

Both the compose and systemd paths run the Go server in plaintext by default.
To serve `wss://` + an HTTPS dashboard, generate a dev cert with
`scripts/gen-dev-cert.sh` (or supply a real pair) and set
`server.tls.{cert,key}` in the config (or `PJ_CLOUD_TLS_CERT`/`PJ_CLOUD_TLS_KEY`).
TLS is all-or-nothing — set both or neither. TLS applies only to the Go
server's WS/HTTP listener; the tag-edit IPC socket is a local UNIX socket and
has no TLS concept.

## Config reference

`config.example.yaml` is the authoritative, commented field list for the Go
server (server incl. TLS + `response_compression`, auth, storage.s3,
catalog.db_path, catalog.tag_ipc_socket, session, dashboard.basic_auth,
metrics). Defaults: dashboard OFF (empty password disables it gracefully),
metrics ON and unauthenticated, plaintext transport, client auth **FAIL-CLOSED**
(the server REFUSES to start unless `bearer_token` / `PJ_CLOUD_TOKEN` is set, or
`-allow-anonymous` / `PJ_CLOUD_ALLOW_ANONYMOUS=1` is passed to run with no auth on
purpose), tag-edit forwarding OFF
(empty `tag_ipc_socket` — UpdateTags is rejected outright until it's set to a
reachable builder socket), and `response_compression` ON (opt-in per client via
Hello — the server only wraps a bulky catalog RPC response when the client
advertised ZSTD support and the body actually shrinks). The builder daemon's own flags are documented via
`python3 -m mcap_catalog_builder --help` and in `mcap_catalog/CATALOG_CONTRACT.md`.
