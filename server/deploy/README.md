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
4. **First-boot ordering: two different mechanisms for two different deploy
   shapes.**
   - **Compose** expresses this properly: `builder` has its own container
     healthcheck (a python3 one-liner — open the catalog DB read-only,
     check `build_metadata.last_build_ns > 0`), and `server` is gated on
     `depends_on: builder: condition: service_healthy` — so the `server`
     container is not even started until the builder has published its
     first catalog. `server` still keeps `restart: unless-stopped` too, as
     a belt-and-suspenders fallback for a later transient hiccup, but it is
     no longer the first-boot ordering mechanism.
   - **systemd (bare metal)** has no equivalent health-gate primitive for
     "and it has also completed its first build", so it still relies on
     `catalog.OpenReadOnly` failing fast (server process exits 1) plus
     `Restart=on-failure` — the server unit crash-loops at `RestartSec`
     intervals until the builder's first scan publishes a DB. This only
     works because both units also set `StartLimitIntervalSec=0` (systemd's
     default 5-starts/10s rate limit would otherwise give up and leave the
     unit permanently `failed` long before a cold-bucket scan finishes) —
     see each unit's `[Unit]` section. Watch `journalctl -u
     pj-cloud-builder` for build progress; the crash-loop is normal, not a
     bug.
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
`curl -fsS http://HOST:8080/health` returns `ok` once the catalog DB is
reachable. Config is supplied with `--config /etc/pj-cloud/config.yaml` (the
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
PJ_CLOUD_TOKEN=changeme docker compose up -d --build
docker compose logs -f builder     # watch the initial catalog build
curl -fsS http://localhost:8080/health        # -> ok (once the builder's first scan lands)
```

- Ports: `8080` (ws:// + http dashboard/health/metrics), `9000`/`9001` (Minio API/console).
- The SQLite catalog + tag-edit socket persist in the `catalog-data` volume
  (shared by `builder` and `server` only); bucket data in `minio-data`.
- For a REAL S3/GCS deploy: drop the `minio`/`minio-init` services, edit
  `deploy.config.yaml`'s `storage.{s3,gcs}` block (endpoint/region/creds), and
  edit the `builder` service's `command:` to match (`--source`,
  `--s3-bucket`/`--gcs-bucket`, prefix) plus its `environment:` for
  credentials.

## systemd (bare metal)

Install **both** `pj-cloud-server.service` and `pj-cloud-builder.service`
under a shared `pjcloud` user with `ProtectSystem=strict`. Install steps,
the secrets `EnvironmentFile` layout, and the builder's storage-target
argument convention (`BUILDER_ARGS` in `/etc/pj-cloud/builder.env`) are
documented in each unit file's header comments — read `pj-cloud-builder.service`
first, since the server unit depends on it having run at least once.

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
