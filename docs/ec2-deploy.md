# EC2 deployment runbook — PJ Cloud Connector for the **S3 use case** (AWS S3, Docker Compose)

Step-by-step for standing up the **two-process backend** on a single EC2
instance, serving MCAP recordings from the S3 bucket to PlotJuggler
clients over WebSocket, using the container/compose deploy shape.

> **This runbook is SPECIFIC TO THE S3 USE CASE.** The bucket, region, the Hive
> prefix, and the S3 backend are all specific to your deployment. **The GCS use
> case is a separate GCS-on-GCE deploy** — see `docs/gce-deploy-smoke.md`, not
> this file. The S3 settings are supplied via two committed template artifacts you
> fill in below: `server/deploy/config.aws-ec2.yaml` and
> `server/deploy/docker-compose.aws.yml` (both ship with `REPLACE_ME` placeholders
> for the bucket + prefix). The main per-environment knobs are the bucket, region,
> and the S3 **prefix** (how much data to serve) — called out below.

The systemd (bare-metal) shape is an alternative for a box without Docker — see
`server/deploy/pj-cloud-{server,builder}.service`. This runbook uses Compose
because it health-gates the builder→server first-boot ordering properly.

---

## 0. What you are deploying (read this first)

Since the M6 catalog-migration cutover the backend is **two processes**, not one
static binary. In this deploy they are two containers on one box:

| Container | Image | Role |
|---|---|---|
| **builder** | `pj-cloud-builder` (Python `mcap_catalog` on `python:3.12-slim`) | **SOLE catalog writer**: scans the S3 bucket, extracts MCAP footer/summary metadata, writes the SQLite catalog, serves the tag-edit UNIX-socket IPC. |
| **server** | `pj-cloud-server` (Go, `distroless/static`) | **Read-only catalog reader + streamer**: opens the SQLite DB `mode=ro`, serves WS (catalog RPCs + session streaming), forwards tag edits over the builder's socket. |

They share **one Docker named volume** (`catalog-data`, mounted at
`/var/lib/pj-cloud` in both) holding the SQLite DB and the tag-edit socket.
Compose gates `server` on the builder's healthcheck
(`build_metadata.last_build_ns > 0`), so the server never starts before the
first catalog is published.

> **HARD RULE:** `catalog-data` MUST be a **local** Docker named volume (the
> default — on the instance's EBS storage). **NEVER back it with EFS / NFS /
> FSx** — SQLite WAL needs real mmap'd shared-memory locking that network
> filesystems don't implement correctly (silent corruption), and a UNIX socket
> can't be served correctly off one either.

---

## 1. Provision the EC2 instance

| Setting | Recommendation | Why |
|---|---|---|
| AMI | Amazon Linux 2023 or Ubuntu 22.04 LTS | Docker-ready; systemd for the daemon. |
| Instance type | `t3.medium` (2 vCPU / 4 GB) to start | The server is light. The builder's first scan parallelizes across CPU cores, so more vCPUs speed up a large cold catalog. |
| Storage | **EBS gp3**, ≥ 20 GB | The Docker named volume lives on the local EBS root — correct for `catalog-data`. |
| Region | **Same region as your S3 bucket** | Deploy in-region: cross-region range-GETs are the extraction bottleneck. |
| Security group | Inbound `22` (SSH, your IP) + `8080` (WS/health/dashboard, or restrict to your client CIDR). Outbound `443` to S3. | Don't expose the dashboard publicly without a password. |

**IAM instance role (no secrets on the box).** Attach a role with a read-only S3
policy — both containers pick it up via the AWS default credential chain, so no
keys are ever stored. Minimal policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "List", "Effect": "Allow", "Action": ["s3:ListBucket"],
      "Resource": "arn:aws:s3:::YOUR_S3_BUCKET" },
    { "Sid": "Read", "Effect": "Allow", "Action": ["s3:GetObject"],
      "Resource": "arn:aws:s3:::YOUR_S3_BUCKET/*" }
  ]
}
```

> **CONTAINER + IMDS GOTCHA (do not skip):** a container reaching the instance
> role is **one extra network hop**, so IMDSv2's default hop limit of 1 blocks
> it and the containers get *no* credentials. Raise it to 2:
> ```bash
> TOKEN=$(curl -s -X PUT http://169.254.169.254/latest/api/token \
>   -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
> IID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" \
>   http://169.254.169.254/latest/meta-data/instance-id)
> aws ec2 modify-instance-metadata-options --instance-id "$IID" \
>   --http-put-response-hop-limit 2 --http-tokens required
> ```
> (Or set it at launch: **Advanced details → Metadata response hop limit = 2**.)

The connector only **reads** the bucket — never grant write.

---

## 2. Install Docker + Compose

```bash
# Amazon Linux 2023
sudo dnf install -y docker git && sudo systemctl enable --now docker
sudo usermod -aG docker "$USER" && newgrp docker      # log out/in to take effect
# the compose v2 plugin:
sudo dnf install -y docker-compose-plugin || \
  (sudo mkdir -p /usr/libexec/docker/cli-plugins && \
   sudo curl -SL https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64 \
     -o /usr/libexec/docker/cli-plugins/docker-compose && \
   sudo chmod +x /usr/libexec/docker/cli-plugins/docker-compose)

# Ubuntu 22.04
sudo apt-get update && sudo apt-get install -y docker.io docker-compose-v2 git
sudo usermod -aG docker "$USER" && newgrp docker

docker compose version    # verify v2
```

---

## 3. Get the code

```bash
git clone <your-repo-url> pj-mcap-server
cd pj-mcap-server
```

No Go/Python toolchain needed on the box — the images build everything. The
builder source (`mcap_catalog/`) is VENDORED directly in the repo (not a
submodule), so a plain clone already contains it and the builder image's `COPY`
step just works — its build context is the repo root.

---

## 4. Fill in your S3 settings

The two committed artifacts ship as **templates** with placeholders. Set the same
three values in **both** files (they must match):

| Setting | In `config.aws-ec2.yaml` | In `docker-compose.aws.yml` |
|---|---|---|
| **bucket** | `storage.s3.bucket` | the builder's `--s3-bucket=` arg |
| **region** | `storage.s3.region` | `AWS_REGION` / `AWS_DEFAULT_REGION` (both services) |
| **prefix** | `storage.s3.prefix` | the builder's `--s3-prefix=` arg |

`bucket` ships as `REPLACE_ME_...` — the server will not serve real data until you
set it. `region` must match the bucket's region. `prefix` decides **how much data
to serve**.

**Choosing a prefix.** `""` scans the whole bucket. If the bucket is a large
Hive-partitioned lake, an unscoped scan can exceed the scan budget — scope to one
partition first and widen later. List the bucket to see its layout:

```bash
aws s3 ls s3://YOUR_BUCKET/ --region YOUR_REGION
```

Example prefix — all ROS bags for one robot at one site:
`customer=<x>/customer_site=<y>/robot=<z>/source=ros-bags/`

---

## 5. Bring it up

```bash
cd server/deploy
PJ_CLOUD_TOKEN='<a-long-random-shared-bearer-token>' \
  docker compose -f docker-compose.aws.yml up -d --build
```

- `PJ_CLOUD_TOKEN` is the single shared bearer token clients must present. **Auth
  is fail-closed:** with it unset the server *refuses to start*. To run with no
  authentication on purpose (only behind a private security group, never a public
  port) also pass `PJ_CLOUD_ALLOW_ANONYMOUS=1`.
- To enable the dashboard, also pass `PJ_CLOUD_DASHBOARD_PASSWORD=...` (empty ⇒
  dashboard stays disabled).
- Put these in a `.env` file next to the compose file if you prefer (`docker
  compose` auto-loads it) — keep it `chmod 600`, don't commit it.
- **One builder per catalog DB.** The builder takes an exclusive lock on the
  catalog DB at startup (CATALOG_CONTRACT.md §11); a second builder on the same
  DB exits code 3. `docker-compose.aws.yml` uses `restart: unless-stopped`
  on the builder (not `always`) so a transient exit never double-starts it — keep
  it that way, and don't run a manual `--rebuild` builder against the live DB.

Compose builds both images, starts `builder`, waits for its first catalog to
publish (its healthcheck), then starts `server`.

---

## 6. Verify

```bash
cd server/deploy

# 1. Builder scanning / publishing (a large first scan takes a few minutes):
docker compose -f docker-compose.aws.yml logs -f builder

# 2. Server (only starts once the builder is healthy):
docker compose -f docker-compose.aws.yml logs -f server

# 3. Health endpoint (external probe — the distroless image has no curl inside):
curl -fsS http://localhost:8080/health          # -> ok

# 4. End-to-end through the real client stack (from a box with the CLI built):
mcap-cloud-cli --url ws://<ec2-host>:8080 list
```

**AWS gotchas:**
- **Health never becomes `ok` / builder stuck** with `NoCredentialProviders` or
  timeouts to `169.254.169.254` → the **IMDS hop limit** is still 1 (§1). Fix it,
  then `docker compose ... restart`.
- **301 / PermanentRedirect on the first List** → wrong region. The error names
  the real one; fix `region:` in `config.aws-ec2.yaml` and `AWS_REGION` in the
  compose file, then restart.
- **Scan never finishes** → the `prefix` is too broad (§4). Narrow it.

---

## 7. TLS (serve `wss://` publicly)

The server runs **plaintext** by default. Two options:
- **In front (recommended for EC2):** an ALB or nginx terminating TLS and
  proxying WebSocket upgrades to the container's `8080`. Keep the security group
  tight and the server private.
- **At the server:** mount a cert/key pair into the container, set
  `PJ_CLOUD_TLS_CERT`/`PJ_CLOUD_TLS_KEY` (or `server.tls.{cert,key}` in the
  config), publish the TLS port. All-or-nothing — set both or neither. Clients
  then use `wss://` (the plugin/CLI `--insecure` flag accepts a self-signed cert).

The tag-edit IPC is a local UNIX socket with no TLS concept.

---

## 8. Day-2 operations

```bash
cd server/deploy
C="docker compose -f docker-compose.aws.yml"

$C logs -f builder            # / server
$C ps                         # status + health
$C restart                    # after editing config/prefix
$C down                       # stop (keeps the catalog-data volume)
$C down -v                    # stop AND wipe the catalog (forces a full re-scan next up)
```

**Updating:** `git pull`, then `$C up -d --build` rebuilds the changed image(s)
and restarts (`mcap_catalog/` is vendored — `git pull` brings it along).

**Widening the served data:** edit the `prefix` in **both** artifacts (§4),
`$C restart`. The builder's next reconcile picks up the newly-in-scope objects;
out-of-scope rows are pruned in the same pass. Catalog freshness otherwise
tracks the `--rescan-interval` (300s); the server hot-swaps onto each
atomically-published rebuild without a restart.

---

## Reference

- `server/deploy/docker-compose.aws.yml` — the S3-use-case compose (this deploy).
- `server/deploy/config.aws-ec2.yaml` — the S3-use-case server config (this deploy).
- `server/deploy/README.md` — deploy-kit overview + the base local-dev compose + the systemd shape.
- `server/deploy/pj-cloud-{server,builder}.service` — the systemd alternative.
- `docs/CATALOG_CONTRACT.md` — the cross-process contract (schema, publish/reopen, tag IPC).
- `docs/gce-deploy-smoke.md` — the **GCS use case** GCS/GCE deploy (the other deployment).
