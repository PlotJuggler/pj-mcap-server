# mcap_catalog_builder

A daemon that watches a folder of `.mcap` recordings (or, with `--source s3`, an S3
bucket) and keeps the SQLite **catalog** (`schema.sql`) in sync: insert/update when a
file is added or modified, hard-delete when a file is removed. It is the **single
writer** to the DB. Both backends drive the same single-writer worker through a
storage `Source` seam (see [S3 backend](#s3-backend-experimental)).

## Usage

Run from `mcap_catalog/` (the directory that contains the `mcap_catalog_builder/`
package):

```bash
python3 -m mcap_catalog_builder <watch_root> [options]                 # local (default)
python3 -m mcap_catalog_builder --source s3 --s3-bucket B --sqs-url U  # S3
```

| Option | Default | Meaning |
|---|---|---|
| `watch_root` (positional) | `.` | [local] folder of `.mcap` files to watch (recursive) |
| `--source` | `local` | backend: `local`, `s3`, or `gcs` |
| `--s3-bucket` | — | [s3] bucket name (required for `--source s3`) |
| `--s3-prefix` | `""` | [s3] key prefix to scope the listing |
| `--gcs-bucket` | — | [gcs] bucket name (required for `--source gcs`) |
| `--gcs-prefix` | `""` | [gcs] key prefix to scope the listing |
| `--sqs-url` | — | [s3] SQS queue URL for S3 event notifications (required for `--source s3` **unless** `--no-watch` or `--once`) |
| `--once` | off | run one full reconcile, publish the catalog, and exit (no daemon/watch). The one-shot build mode CI and the read-only server bring-up use |
| `--rebuild` | off | build into a temp DB and publish it atomically (implied when `--db` doesn't exist yet); valid with `--once` or in daemon mode |
| `--db` | `/tmp/pj-cloud-catalog.db` | catalog SQLite file |
| `--tag-socket` | off | path for the tag-edit IPC unix socket (daemon mode only; see [Tag-edit IPC](#tag-edit-ipc)) |
| `--rescan-interval` | `300.0` | seconds between safety re-scans |
| `--no-watch` | off | daemon mode: start **no** live event producer (no local watchdog/inotify observer, no S3 SQS long-poll thread) — discovery is then rescan-only, driven purely by `--rescan-interval`. With `--source s3`, also drops the `--sqs-url` requirement. No-op for `--source gcs` (already rescan-only) and for `--once` |
| `--extract-workers` | `2×CPU, max 32` | concurrency for the full-reconcile read phase (fetch+parse summaries). For a remote bucket (`--source s3`) these are worker **processes** — each with its own client and its own GIL, so the GIL-bound pure-Python MCAP parse scales across cores; for a local watch root (or `--source gcs`) they are threads. The DB apply stays serial either way. Rarely needs tuning |
| `--debounce` | `2.0` | [local] seconds to debounce file events |
| `--stability-checks` | `3` | [local] size-stability polls before cataloging |
| `--stability-interval` | `0.5` | [local] seconds between stability polls |
| `--log-level` | `INFO` | `DEBUG`/`INFO`/`WARNING`/`ERROR` |

On startup it runs a full **reconcile** (catalog missing objects, hard-delete vanished
rows), then watches for changes — via `watchdog` (inotify) for `local`, or by draining
**S3→SQS** notifications for `s3` — plus a periodic safety re-scan.

**`--no-watch` (rescan-only daemon).** Pass `--no-watch` to skip starting any live
event producer at all: no `watchdog`/inotify observer for `local`, no SQS long-poll
thread for `s3`. The startup reconcile, the periodic `--rescan-interval` re-scan
thread, the worker loop, and the tag-edit IPC server (`--tag-socket`) all still run
exactly as without the flag — only file *discovery* changes, to purely
rescan-driven. This is for hosts where inotify is unavailable (e.g. exhausted
`fs.inotify.max_user_instances`) or where SQS isn't wired up yet; `--source gcs` is
already rescan-only, so `--no-watch` is a harmless no-op there, and it's likewise a
no-op with `--once` (which never starts a producer regardless).

## How dimensions are resolved

Each file's `customer/site/robot/source/date` come from, in order:

1. an **`s3_key` MCAP metadata** record (`{"key": "customer=…/customer_site=…/…/<f>.mcap"}`);
2. else the file's **path relative to `watch_root`**, if it is Hive-structured;
3. else → `catalog_failures` (the raw key is kept; the file is skipped).

The parse is trusted only if `rebuild_hive_key(dims) == key` (round-trip), so a
near-miss key is never guessed into a wrong row.

> **Caveat:** the real sample files in `../DATA/samples` are **flat** and carry **no
> `s3_key`**, so they route to `catalog_failures` as-is. The tests therefore copy
> them into a Hive tree (`make_hive_fixture`) or synthesize MCAPs with an injected
> `s3_key`. Per-file stats come only from the MCAP **summary** — never the embedded
> `rosbag2` metadata, which describes the whole multi-day bag.

## Change detection & removal

- **Fingerprint = the `etag`** (R4). Cataloging skips with no body read when the stored
  `etag` is unchanged. Local files have no real ETag, so `etag` is a synthetic
  `local:{size}:{mtime}` token (so a size *or* mtime change re-catalogs); S3 uses the
  object's real ETag, taken straight from the listing.
- **Removal** hard-deletes the `files` row (tags cascade); the append-only lookups,
  dictionaries, and `topic_sets` are left as harmless orphans (no GC).

## Architecture (single writer)

The `watchdog` observer, the debounce `Timer`s, and the periodic-rescan thread are
**producers** — they only enqueue events. One `worker_loop` (main thread) drains the
queue and performs **every** DB write. SQLite runs in **WAL** mode, so external
readers can query the catalog concurrently while the daemon writes.

The `topic_counts` blob is built from the file's sorted topic-set members with
`channel_message_counts.get(channel_id, 0)` (zero-message channels are absent from
that dict), guarded by an in-transaction `sum(counts) == message_count` check that
routes any mismatch to `catalog_failures` — making a wrong count impossible to commit.

## Tag-edit IPC

`update_tags()` (`db.py`) is the sole writer of `tags_override` — this daemon
never accepts writes from anywhere but its own single-writer queue. A
future/external caller (the Go read-only server's `UpdateTags` RPC handler)
reaches it over `--tag-socket <path>`: a UNIX-socket JSON/HTTP endpoint,
started only in daemon mode after the startup reconcile/publish completes.
See `CATALOG_CONTRACT.md`'s "Tag-edit IPC" section (catalog-migration §1.1
DECISION D2(a)) for the wire shape, deadline semantics, and the socket's
trust-boundary caveat (no built-in auth — it's a local, same-host endpoint).

## S3 backend (experimental)

The single-writer worker, reconcile, and catalog transaction are **backend-agnostic**:
they talk to a storage **`Source`** seam (`stat` / `open_summary` / `list_all` +
event-translation helpers), never to `os`/`open` directly. Two backends implement it:

- `storage.py` — the `Source` protocol plus `LocalSource` (the local filesystem).
- `s3_storage.py` — `S3Source` + `S3RangeReader`: reads an MCAP summary with **1–2
  HTTP range GETs** (footer → summary offset → summary), uses the **S3 ETag** as the
  R4 fingerprint, and lists via paginated `list_objects_v2`. The message body is
  never downloaded.
- `s3_producer.py` — `s3_event_producer`: drains **S3→SQS** notifications into the
  same `WatchEvent` queue the inotify handler feeds (the cloud-native inotify).

`builder.catalog_object` / `delete_by_key` are the unified core; `catalog_file` /
`delete_by_path` remain as thin local wrappers. These S3 modules **never import
`boto3`** — the client is injected — so the library and its tests run with no AWS
dependency; only the daemon's `--source s3` mode (and the example) import boto3.

> **Scope (experimental):** the S3 path is wired end-to-end (`--source s3`) and fully
> unit-tested against a fake in-memory S3/SQS, but it has not yet been run against a
> live bucket at scale, and human-edited tags / parallel cataloging are still future
> work. The S3 modules need `boto3` + AWS credentials only at deploy time.

### How to try it

**1. Unit tests — no AWS, no boto3.** A fake in-memory S3 client serves real MCAP
bytes and records every range requested, so the cheap-read property is asserted
directly:

```bash
cd /home/davide/ws_plotjuggler/mcap_server/mcap_catalog
python3 -m pytest mcap_catalog_builder/tests/test_s3_storage.py \
                  mcap_catalog_builder/tests/test_s3_producer.py \
                  mcap_catalog_builder/tests/test_storage.py -v
```

**2. Against a real bucket** — needs `boto3` and AWS credentials (env vars,
`~/.aws`, or an instance role) with `s3:GetObject` (and `s3:ListBucket` for `--list`):

```bash
pip install boto3   # only needed to actually hit S3

# Read one recording's signals/counts/time span — prints bytes fetched vs object
# size, showing the body was skipped (e.g. "fetched 7,914 of 512,338,001 bytes"):
python3 mcap_catalog_builder/examples/s3_read_summary.py s3://my-bucket/customer=acme/.../x.mcap

# List the .mcap objects under a prefix (key + ETag, from the listing, no body read):
python3 mcap_catalog_builder/examples/s3_read_summary.py --list s3://my-bucket/customer=acme/
```

**3. Run the daemon against S3.** First wire the bucket: add a bucket notification
(`ObjectCreated:*`, `ObjectRemoved:*`) targeting an SQS queue, and give the host
`s3:GetObject` + `s3:ListBucket` + `sqs:ReceiveMessage` + `sqs:DeleteMessage`. Then:

```bash
pip install boto3
python3 -m mcap_catalog_builder --source s3 \
    --s3-bucket my-bucket --s3-prefix customer=acme/ \
    --sqs-url https://sqs.eu-west-1.amazonaws.com/123456789012/mcap-events \
    --db /var/lib/pj/catalog.db
```

On startup it lists the bucket and reconciles, then catalogs each upload as its SQS
event arrives — reading only the footer+summary of each object (no body download).
Run it on always-on compute **in the bucket's region**, with the SQLite catalog on a
**local/EBS disk** (never on S3). `test_s3_producer.py` shows the producer contract
with a fake SQS client.

## Tests

```bash
cd /home/davide/ws_plotjuggler/mcap_server/mcap_catalog
python3 -m compileall mcap_catalog_builder
python3 -m pytest mcap_catalog_builder/tests/ -v
```

The real-data end-to-end case is skipped (never failed) when `../DATA/samples` is absent.
