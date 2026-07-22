# PJ Cloud Connector — harness runbook (operator notes)

This is the operator-facing guide to the repo's test/verification gates: what each
one proves, what it needs, the ports it owns, the ground-truth pins, and how to
recover from common failures.

It is the **as-built** runbook (Plan C Task 9, adapted). The plan's original
runbook assumed a Qt CLI + `docker-compose` stack + `pjcloud-cli` over
`wss://:8443`; NONE of that is as-built. The real harness is:

- an **in-process Go server** (no separate process for the unit/CI legs),
- a **C++ `mcap-cloud-cli`** over **`ws://`** (ixwebsocket, zero Qt) for the
  end-to-end shell gates,
- the wire bindings are **checked in** (`server/internal/wire/pj_cloud`) — the
  gates never run `protoc` / `make proto`.

---

## The gates at a glance

| Gate                  | Command               | What it proves                                                                 | Port(s) it owns | Needs Docker? | Needs the real corpus? |
|-----------------------|-----------------------|--------------------------------------------------------------------------------|-----------------|---------------|------------------------|
| Unit + race           | `make test` / `make race` | Every package's logic, including `-race`. Hermetic.                          | none            | no            | no                     |
| Smoke                 | `make smoke`          | The whole NEW pipeline end-to-end: Python `mcap_catalog` builder (sole catalog writer + tag-edit IPC) + Go server (read-only reader) + C++ CLI, against a FRESH SYNTHETIC Hive-keyed corpus it generates itself. | **:8081**       | yes (Minio)   | no (synthetic, self-seeded) |
| Matrix                | `make matrix`         | The deeper spec §11 L3/L4 round-trip MATRIX (half-topics, none-matching, out-of-range, spans-boundary, 8-file stitch, 4-parallel, overlap-rejection), each `mcapdiff`-verified, on both s3 + gcs legs. | **:8082** | yes (Minio + fake-gcs) | **yes** |
| CI integration        | `make ci-integration` | The `{s3,gcs}` CI legs (Plan A 46/46a) over **synthetic** fixtures — the same in-process Go harness GitHub runs in service containers, proven locally. | none for the server (in-process); emulators on **:19010 / :14450** | yes (Minio + fake-gcs) | no (synthetic) |
| Bench (throughput)    | `make bench`          | Streaming throughput (MB/s) + the in-process CPU/backpressure microbenches.    | **:8082** for its throwaway server | yes (Minio) for throughput; the microbenches are in-process | yes for throughput; no for the microbenches |
| Bench (storage)       | `make bench-storage`  | `GetRange` MB/s parity, S3/Minio vs GCS/fake-gcs, on one corpus object.        | none            | yes (fake-gcs) | **yes**                |

> Bench and matrix both use **:8082** but NEVER run concurrently — run one at a
> time.

---

## Ports — the fixed allocation

Do not collide with these:

| Port    | Owner                                                              |
|---------|-------------------------------------------------------------------|
| `:8080` | the **interactive / user** server (`make server-start`).          |
| `:8081` | **smoke**'s throwaway server (`scripts/smoke.sh`).                 |
| `:8082` | **matrix** and **bench**'s throwaway server.                      |
| `:9000` | the **dev Minio** S3 API (`infra/minio`, console `:9001`).        |
| `:4443` | the **fake-gcs** convention port (`infra/fake-gcs`, matrix/bench-storage). |
| `:19010`/`:19011` | **ci-integration**'s OWN throwaway Minio (API / console).|
| `:14450`| **ci-integration**'s OWN throwaway fake-gcs.                       |

The interactive server records its PID/log at `/tmp/pj-cloud-server.{pid,log}`.
`make server-start` / `make server-stop` manage it; `smoke`/`matrix`/`ci-integration`
never touch it.

---

## Prerequisites

- **Go 1.23+** at `$HOME/.local/go/bin` (NOT on the default PATH). Always:
  ```bash
  export PATH=$HOME/.local/go/bin:$HOME/go/bin:$PATH
  export GOTOOLCHAIN=local
  ```
- **Docker** for any gate that touches storage (smoke, matrix, ci-integration,
  bench, bench-storage).
- **`make smoke` is fully self-contained** — it generates its own synthetic,
  Hive-keyed MCAP corpus every run (`gen-ci-fixtures -hive` + `gen-3d-fixture`)
  and seeds it into a DEDICATED bucket (`smoke-hive`, wiped + reseeded every
  run); it never touches `recordings` and needs no real dataset on disk.
- **The real ground-truth corpus** on disk at `/home/gn/ws/jkk_dataset02` AND
  seeded into the Minio `recordings` bucket — required by **matrix** and the
  **bench** corpus legs only (NOT smoke, since the catalog-migration cutover —
  see below). The shell gates do **not** auto-seed; bring up `infra/minio`
  (`docker compose up -d`) and upload the `nissan_zala_*.mcap` corpus before
  running them.
- **A Python venv at `~/.venvs/pj-catalog`** with `boto3`, `google-cloud-storage`,
  `mcap`, and `watchdog` installed — smoke's Python catalog-builder daemon runs
  under this interpreter (`python3 -m venv ~/.venvs/pj-catalog && ~/.venvs/pj-catalog/bin/pip
  install boto3 google-cloud-storage mcap watchdog`). smoke fails fast with this
  exact bootstrap command if the venv is missing.
- **The C++ CLI** built (`./build.sh` from the repo root, or a standalone
  `conan install` + `cmake` in `plugin/toolbox_mcap_cloud`) for smoke/matrix —
  they shell out to `mcap-cloud-cli`. (smoke/matrix incrementally rebuild it.)

`make ci-integration` and the in-process bench microbenches need **only** Docker
+ Go (no corpus, no CLI) — they are the parts that run in GitHub CI.

---

## Ground truth — smoke (synthetic, self-derived) vs matrix (real corpus, hand-pinned)

**`make smoke` (2026-07-06 rewrite): almost nothing is hand-hardcoded.** Its
corpus is generated fresh every run, and every message-count assertion is
derived at runtime from an INDEPENDENT ORACLE — `mcaptopics` run directly on
the local, pre-upload fixture files — never re-derived from the server itself
and never a literal in the script. See `scripts/smoke.sh`'s header comment
("SELF-DERIVED GROUND TRUTH") for the full explanation. The only things it
hardcodes are STRUCTURAL identifiers inherent to the deterministic generator
(a fixture's filename, its Hive dimensions, which topic is "the subset topic")
— e.g. `TARGET_KEY` (the target fixture's full Hive object key) and
`SUBSET_TOPIC="/imu"`.

The **C++ live gtests** (`plugin/toolbox_mcap_cloud/tests/*_live_test.cpp`)
CANNOT self-derive — they're compiled long before smoke.sh runs — so their
ground-truth constants ARE hand-pinned, from one empirical run against the same
deterministic generator smoke.sh uses (`gen-ci-fixtures -hive` +
`gen-3d-fixture`). If `server/internal/genmcap.DefaultSpecs()` ever changes
(different topics/counts/ordering), **both** `scripts/smoke.sh`'s structural
identifiers **and** the four live-test files' constants need updating together
— each live-test file's header comment names the regeneration command.

**`make matrix` still targets the LEGACY path** (the Go in-process indexer over
the real `nissan_zala_*` corpus at `/home/gn/ws/jkk_dataset02`) — its migration
to the new external-builder shape is tracked separately (see
`scripts/matrix.sh`'s header note). Its real-corpus ground truth is pinned in
`scripts/matrix.sh`'s shell constants, unchanged by the smoke rewrite.

The CI/synthetic ground truth (used by `make ci-integration`, distinct from
smoke's fixtures despite sharing the same generator) lives in exactly **one**
place and cannot drift: `server/internal/genmcap` (`DefaultSpecs()`). Both the
generator (`cmd/gen-ci-fixtures`, which writes the bytes) and the harness
(`internal/ws/ci_integration_test.go`, which reads the same `FileSpec` list to
know what to assert) read it — change a spec and both move at once.

---

## Reseeding the real corpus (matrix / bench only — smoke needs none of this)

1. Bring Minio up:
   ```bash
   cd infra/minio && docker compose up -d        # bucket `recordings`, :9000
   ```
2. Upload the corpus objects (the `nissan_zala_*.mcap` set) into
   `s3://recordings/` (e.g. via `mc cp`). The bytes on disk at
   `/home/gn/ws/jkk_dataset02/` are the round-trip ground truth `mcapdiff`
   compares against — keep them identical to what you upload.
3. If the corpus changed (different files / counts), update
   `scripts/matrix.sh`'s constants, then run `make matrix` to confirm green.
   (`make smoke` is unaffected — it never reads `recordings`.)

For the GCS leg (matrix / bench-storage), seed the fake-gcs `recordings` bucket
with the same objects via `infra/fake-gcs/seed.sh`.

---

## CI integration fixtures (synthetic)

`cmd/gen-ci-fixtures` writes the deterministic synthetic MCAPs from
`internal/genmcap.DefaultSpecs()`. The set covers: three time-disjoint
ZSTD files (A/B/C, the stitch core), an **uncompressed** chunk-container file
(D — exercises the codec's no-compression decode), a **tiny single-chunk**
file (E — the small-file edge), and an **LZ4-frame** chunk-container file
(F — exercises the codec's LZ4-frame decode, `chunks.go` `lz4.NewReader`). So
all three chunk-container codecs the streaming half decodes (ZSTD / None / LZ4)
are wired as CI dimensions and asserted end-to-end on both `{s3,gcs}` legs.
`genmcap` also exposes a `Compression` / `PayloadBytes` / `ChunkSize` /
`Metadata` knob set and a `CorruptChunkBody` helper used by the hermetic
integrity test (which now rejects corrupted ZSTD *and* LZ4 frames).

Regenerate to a directory:
```bash
cd server && go run ./cmd/gen-ci-fixtures -out /tmp/fix -manifest
```
The bytes are byte-identical across runs (no `time.Now` / no randomness), which
is what the warm-start (0 re-extracts) assertion relies on.

---

## Bench notes

- `make bench` runs the `bench`-tagged tests: the corpus **throughput** gate
  (a throwaway server on :8082 + Minio :9000 — SKIPS if Minio/corpus absent) and
  the in-process **microbenches** (`BenchmarkCompressionCPU`,
  `BenchmarkBackpressureLatency`) which need no infra and emit grep-able lines
  (`BENCH_COMPRESSION_MBPS=…`, `BENCH_BACKPRESSURE_P50_MS=… P99_MS=…`).
- `make bench-storage` brings fake-gcs up, seeds it, runs the `GetRange` parity
  test (`STORAGE_PARITY_S3_MBPS=` / `STORAGE_PARITY_GCS_MBPS=`), and tears it
  down.
- The nightly **`.github/workflows/bench.yml`** runs the in-process microbenches
  in service containers, uploads the machine-readable lines as an artifact, and
  is a **SOFT** gate — it fails only on a build/run error, never on a speed
  number (the baseline figures are reference-machine; a cgroup-limited runner is
  not comparable).

---

## Troubleshooting

**Orphaned throwaway servers / stale ports.**
The shell gates reap their server on exit, but a hard kill can leave one behind.
- Interactive server: `make server-stop` (clears `/tmp/pj-cloud-server.pid`).
- A stuck `:8081`/`:8082` listener: find and kill it —
  `ss -ltnp | grep -E ':8081|:8082'` then `kill <pid>`.
- ci-integration containers: named `pjci-minio-<pid>` / `pjci-fakegcs-<pid>` —
  `docker ps | grep pjci` then `docker rm -f <name>` (the script normally reaps
  them in its EXIT trap).

**Stale catalog DBs.**
The Python builder holds an exclusive single-writer lock (`<db>.writer.lock`,
CATALOG_CONTRACT.md §11) for its lifetime, so **only one builder may run per
served DB** — a second builder on the same `--db` exits code 3 (naming the
holder PID). The gates below each use a DISTINCT DB path precisely so their
builders never contend; if you start a builder by hand, don't point a second
one at a DB a gate (or the interactive instance) is already building.

Each gate uses its OWN SQLite DB so they never share state:
- smoke: `/tmp/pj-cloud-smoke-catalog.db` (owned by the Python builder daemon
  now — smoke wipes it at the start of every run; the Go server only reads it).
- matrix: `/tmp/pj-cloud-matrix-catalog.db` (+ `…-gcs-catalog.db`)
- interactive / default: `/tmp/pj-cloud-catalog.db` (override with `-db` or
  `PJ_CLOUD_DB`).
If a gate behaves as though it has stale data, delete its DB plus the WAL/SHM
siblings (`<db> <db>-wal <db>-shm`) and rerun.

**`make smoke` fails with a "venv interpreter not found" error.**
Bootstrap the catalog-builder venv the error names:
`python3 -m venv ~/.venvs/pj-catalog && ~/.venvs/pj-catalog/bin/pip install boto3 google-cloud-storage mcap watchdog`.

**`make smoke` fails at step b1 (catalog not built within 60s) or with
`catalog_failures` non-empty.**
Check `/tmp/pj-cloud-smoke-builder.log` — a real bug in the Python builder or a
Minio connectivity problem (confirm `docker compose -f infra/minio/docker-
compose.yml ps` is healthy) surfaces there.

**`make smoke` fails at the C++ live leg with a count mismatch.**
Either a real product regression, OR the four live-test files' hand-pinned
constants drifted from `server/internal/genmcap.DefaultSpecs()` (see "Ground
truth" above) — reconcile them together, they cannot self-derive.

**`make matrix` fails at step a (bucket empty).**
The `recordings` bucket isn't seeded. matrix does NOT auto-seed — bring up
`infra/minio` and upload the corpus (see "Reseeding the real corpus").

**ci-integration: bucket under-seeded / 0 fixtures.**
The seed step failed; check Docker is up and the `mc` / fake-gcs upload step in
`scripts/ci-integration.sh` succeeded. The test fails LOUDLY (not skips) on an
empty bucket — a CI leg with no data is a seed bug.

**ci-integration: port already in use (:19010 / :14450).**
Override with `PJ_CI_MINIO_PORT` / `PJ_CI_FAKEGCS_PORT`.

**`gcs` leg can't reach the emulator.**
`STORAGE_EMULATOR_HOST` is unset/wrong. In-container it is the container host;
from the host it must be the published `127.0.0.1:<port>`. The matrix /
bench-storage gates set this for you; if you run a leg by hand, export it.

**`gcs` leg re-scans everything every poll.**
The change-detect triple is keyed wrong — the gcsreader must map GCS
`Generation` into the `etag` slot and `Updated` into `last_modified`
(unified-plan §3.2). This is a code bug, not an operator issue.

## SDK package

The plugin builds against the Conan package `plotjuggler_sdk/<SDK_VERSION>` (pin
file `plugin/SDK_VERSION`, currently 0.11.0 — carries the toolbox parser-ingest
tail slots). On a fresh machine, publish it once from the sibling SDK checkout
before any plugin build (the repo-root `./build.sh` checks the cache and prints
this exact command if it's missing):

    cd ~/ws_plotjuggler/plotjuggler_sdk-cloud
    conan create . -s build_type=Release -s compiler.cppstd=20 --build=missing

## Real AWS staging bucket (S3-use-case M1 real-bucket run)

Config: `server/deploy/config.aws-staging.yaml` (set `storage.s3.bucket` +
`region` — the file ships with a `REPLACE_ME` placeholder; real AWS — no endpoint
override, no inline secrets; the connector only READS the bucket).

One-time credential drop (NOT in the repo, never commit):

    mkdir -p ~/.aws && chmod 700 ~/.aws
    cat > ~/.aws/credentials << 'CREDS'
    [aws-staging]
    aws_access_key_id     = <PASTE>
    aws_secret_access_key = <PASTE>
    CREDS
    chmod 600 ~/.aws/credentials

Run + verify (read-only). The backend is TWO processes — the Python builder (the
sole catalog writer) then the read-only Go server. `run.sh --aws` orchestrates
both (builder first, waits for its first published catalog, then the server on
:8084), and defaults `AWS_PROFILE=aws-staging` to match the profile above:

    ./run.sh --aws                             # needs the builder venv (see `make smoke`)
    # watch the builder scan the real bucket:
    tail -f /tmp/pj-cloud-builder.log          # expect "catalog built: N file(s) scanned"
    # then list through the real client stack:
    plugin/toolbox_mcap_cloud/build/bin/mcap-cloud-cli --url ws://localhost:8084 list

If the first List fails with 301/PermanentRedirect, the error names the
bucket's actual region — fix `region:` in the staging config and restart.
