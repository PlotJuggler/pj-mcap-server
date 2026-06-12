# PJ Cloud Connector (`pj-mcap-server`)

Self-hosted **Go server + C++ client SDK + PlotJuggler 4 toolbox plugin** that
serves MCAP recordings from an S3-compatible bucket to PlotJuggler on demand:
browse a queryable catalog (time/topic/tag filters, Lua predicates), select
files + topics + a time range, and stream exactly that subset — including
**server-side stitching** of consecutive recordings into one continuous
session — with reconnect-resume and a repeat-fetch cache.

## Layout

| Path | What |
|---|---|
| `proto/pj_cloud.proto` | Canonical wire schema (WS + Protobuf envelope) |
| `server/` | Go server: SQLite catalog, indexer, session streaming, tag editing |
| `PJ4/` | Vendored PlotJuggler 4 working tree (source-only) |
| `PJ4/pj-official-plugins/toolbox_dexory_cloud/` | The "Dexory Cloud" toolbox plugin + `dexory-cloud-cli` |
| `infra/minio/` | Local S3 (Minio) — the storage endpoint for development |
| `scripts/smoke.sh` | `make smoke` — the end-to-end regression gate |
| `*.md` (root) | Design spec, plans, M1 report + **demo runbook** |

## Quick start — self-contained, no cloud account or private data

Two scripts bring the whole thing up locally with **synthetic** recordings (Minio
+ generated MCAPs + the server) — no AWS, no credentials, nothing to seed by hand.

**Prerequisites:** Docker, and the Go toolchain at `$HOME/.local/go`. The GUI also
needs Qt 6.8.3 (the build below prints the one-time install command if it's missing);
you can test fully **headless** without it.

```bash
# 1. Build the server + plugin (+ the GUI app if Qt is installed)
./build.sh

# 2. Start the local backend: Minio + synthetic recordings + server on :8080
#    (idempotent; seeding is skipped if the bucket already has data)
./run.sh

# 3a. Headless check — list the catalog through the real client
PJ4/pj-official-plugins/build/toolbox_dexory_cloud/Release/toolbox_dexory_cloud/dexory-cloud-cli \
  --url ws://localhost:8080 list

# 3b. Or the GUI — launch PlotJuggler, open the "Dexory Cloud" panel, connect to
#     ws://localhost:8080, then browse / filter / download.
cd PJ4 && ./run.sh
```

Stop everything: `make server-stop && (cd infra/minio && docker compose down)`.

### Targets — local Minio or a real cloud bucket

`run.sh` takes a **named target**:

| Command | What |
|---|---|
| `./run.sh` &nbsp;or&nbsp; `./run.sh --dexory_minio` | **Local** Minio + synthetic data (the quickstart above). No cloud, no credentials. **`:8080`** |
| `./run.sh --dexory_aws` | **Dexory staging** bucket on AWS S3. Creds: `AWS_PROFILE` (defaults to `dexory-staging`) or `AWS_ACCESS_KEY_ID/…`. **`:8084`** |
| `./run.sh --asensus_google` | **Asensus** bucket on Google Cloud Storage. Creds: Application Default Credentials. *(fill in `server/deploy/config.asensus-staging.yaml` first)* **`:8085`** |
| `./run.sh <path/to.yaml>` | Power-user: any S3/GCS server config file. |

The cloud targets skip Minio + seeding, read the bucket + port from their config
(`server/deploy/config.*-staging.yaml`), and take credentials from your environment.
A cold real bucket's first scan is ~1s/file over WAN, so **scope `prefix:`** to a
sub-folder of a large data-lake bucket and make **`region:`** match (a
`301/PermanentRedirect` in the log names the correct region). One server runs at a
time — `make server-stop` to switch targets. See `scripts/RUNBOOK.md` for the worked Dexory example.

**Your own bucket:** copy `config.example.yaml` (S3) or `config.asensus-staging.yaml`
(GCS), set `bucket`/`prefix` (leave credentials out → default chain / ADC), then
`./run.sh <your.yaml>`.

Run the full regression gate (maintainer; needs the pinned corpus): `make smoke` (final line `SMOKE PASS`).

## Documentation

- **Demo walkthrough:** `2026-06-05-m1-demo-runbook.md` (step-by-step, copy-paste commands)
- **M1 report:** `2026-06-05-dexory-m1-report.md` (what was built / verified / open items)
- **Agent/developer handbook:** `CLAUDE.md` (as-built state, amendments, conventions)
- **Design spec + plans:** `2026-05-28-*.md`, `2026-06-03-*.md`, `2026-06-04-two-endpoints-approach.md`
