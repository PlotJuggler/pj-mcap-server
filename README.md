# PJ Cloud Connector (`pj-mcap-server`)

Self-hosted **Go server + C++ client SDK + PlotJuggler 4 toolbox plugin** that
serves MCAP recordings from an S3-compatible or GCS bucket to PlotJuggler on
demand: browse a queryable catalog (time/topic/tag filters), select files +
topics + a time range, and stream exactly that subset — including
**server-side stitching** of consecutive recordings into one continuous
session — with reconnect-resume and a repeat-fetch cache.

## Layout

| Path | What |
|---|---|
| `proto/pj_cloud.proto` | Canonical wire schema (WS + Protobuf envelope) |
| `server/` | Go server: SQLite catalog, indexer, session streaming, tag editing |
| `plugin/toolbox_dexory_cloud/` | "Dexory Cloud" toolbox plugin + `dexory-cloud-cli` (builds standalone) |
| `infra/minio/` | Local S3 (Minio) — development storage endpoint |
| `scripts/smoke.sh` | `make smoke` — end-to-end regression gate |
| `arch/` | Design spec and implementation plans |

## Quick start — local, no cloud account needed

**Prerequisites:** Docker, Go toolchain at `$HOME/.local/go`, Conan 2, CMake ≥ 3.21.
The `plotjuggler_sdk` Conan package must be in your cache before building the plugin
(see `plugin/SDK_VERSION` for the required version).

```bash
# 1. Build the Go server + CLI plugin
./build.sh

# 2. Start the local backend: Minio + synthetic recordings + server on :8080
./run.sh

# 3. Check the catalog via the CLI
plugin/toolbox_dexory_cloud/build/bin/dexory-cloud-cli \
  --url ws://localhost:8080 list
```

Stop everything: `make server-stop && (cd infra/minio && docker compose down)`.

### Targets

`run.sh` takes a named target:

| Command | What |
|---|---|
| `./run.sh` or `./run.sh --dexory_minio` | Local Minio + synthetic data. No credentials. **`:8080`** |
| `./run.sh --dexory_aws` | Dexory staging bucket on AWS S3. Creds: `AWS_PROFILE` (defaults to `dexory-staging`). **`:8084`** |
| `./run.sh --asensus_google` | Asensus bucket on GCS. Creds: Application Default Credentials. *(fill in `server/deploy/config.asensus-staging.yaml` first)* **`:8085`** |
| `./run.sh <path/to.yaml>` | Any S3/GCS server config file. |

One server runs at a time — `make server-stop` to switch targets.

Run the full regression gate (needs the pinned corpus): `make smoke`.
