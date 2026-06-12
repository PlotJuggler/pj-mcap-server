# pj-cloud-server deploy artifacts

Operational artifacts for shipping the PJ Cloud Connector server (Plan A Task 36).
The server is a single static Go binary (pure-Go SQLite, `CGO_ENABLED=0`) with no
machine-specific embedding.

| File | Purpose |
|---|---|
| `Dockerfile` | Multi-stage build → `gcr.io/distroless/static-debian12:nonroot`. |
| `docker-compose.yml` | Server + Minio stack, healthchecked, config mounted. |
| `deploy.config.yaml` | Compose-tuned config (plaintext :8080, `minio:9000` endpoint). |
| `config.example.yaml` | The FULL config surface, commented, field-verified against `config.go`. |
| `pj-cloud-server.service` | systemd unit for a bare-metal deploy. |

## Container image

```bash
# from the repo root (build context is server/, Dockerfile lives under deploy/)
docker build -t pj-cloud-server:dev -f server/deploy/Dockerfile server
# or: make docker
```

The image is distroless/static (no shell, no curl): probe liveness over HTTP from
outside the container — `curl -fsS http://HOST:8080/health` returns `ok` once the
catalog DB is reachable. Config is supplied with `--config /etc/pj-cloud/config.yaml`
(the default `CMD`); mount your config there. Secrets come from the environment
(`PJ_CLOUD_TOKEN`, `PJ_CLOUD_S3_*`, `PJ_CLOUD_DASHBOARD_PASSWORD`, `PJ_CLOUD_TLS_*`);
the config's `${ENV}` references are expanded at load.

## docker compose (server + Minio)

`docker-compose.yml` stands the whole backend up on one box: Minio (bucket
`recordings`, `admin`/`password123`), a one-shot `minio-init` that creates the
bucket, and the server pointed at `http://minio:9000` via `deploy.config.yaml`.

```bash
cd server/deploy
# Upload your recordings to s3://recordings first (the server only READS objects):
#   mc alias set local http://localhost:9000 admin password123
#   mc cp *.mcap local/recordings/
PJ_CLOUD_TOKEN=changeme docker compose up -d --build
curl -fsS http://localhost:8080/health        # -> ok
```

- Ports: `8080` (ws:// + http dashboard/health/metrics), `9000`/`9001` (Minio API/console).
- The SQLite catalog persists in the `catalog-data` volume; bucket data in `minio-data`.
- For a REAL S3 deploy: drop the `minio`/`minio-init` services and edit
  `deploy.config.yaml`'s `storage.s3` (endpoint + region + creds), or run the
  `server` service alone against AWS.

## systemd (bare metal)

`pj-cloud-server.service` runs the binary directly under a dedicated `pjcloud`
user with `ProtectSystem=strict`. Install steps and the secrets `EnvironmentFile`
layout are documented in the unit file's header comments.

## TLS

Both the compose and systemd paths run plaintext by default. To serve `wss://` +
an HTTPS dashboard, generate a dev cert with `scripts/gen-dev-cert.sh` (or supply
a real pair) and set `server.tls.{cert,key}` in the config (or `PJ_CLOUD_TLS_CERT`
/ `PJ_CLOUD_TLS_KEY`). TLS is all-or-nothing — set both or neither.

## Config reference

`config.example.yaml` is the authoritative, commented field list (server incl.
TLS, auth, storage.s3, catalog.db_path, indexer, session, dashboard.basic_auth,
metrics). Defaults: dashboard OFF (empty password disables it gracefully), metrics
ON and unauthenticated, plaintext transport.
