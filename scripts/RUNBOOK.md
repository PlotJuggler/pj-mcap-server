# PJ Cloud Connector — harness runbook (operator notes)

This is the operator-facing guide to the repo's test/verification gates: what each
one proves, what it needs, the ports it owns, the ground-truth pins, and how to
recover from common failures.

It is the **as-built** runbook (Plan C Task 9, adapted). The plan's original
runbook assumed a Qt CLI + `docker-compose` stack + `pjcloud-cli` over
`wss://:8443`; NONE of that is as-built. The real harness is:

- an **in-process Go server** (no separate process for the unit/CI legs),
- a **C++ `dexory-cloud-cli`** over **`ws://`** (ixwebsocket, zero Qt) for the
  end-to-end shell gates,
- the wire bindings are **checked in** (`server/internal/wire/pj_cloud`) — the
  gates never run `protoc` / `make proto`.

---

## The gates at a glance

| Gate                  | Command               | What it proves                                                                 | Port(s) it owns | Needs Docker? | Needs the real corpus? |
|-----------------------|-----------------------|--------------------------------------------------------------------------------|-----------------|---------------|------------------------|
| Unit + race           | `make test` / `make race` | Every package's logic, including `-race`. Hermetic.                          | none            | no            | no                     |
| Smoke                 | `make smoke`          | The whole pipeline end-to-end (Go server + C++ CLI) against the real corpus.   | **:8081**       | yes (Minio)   | **yes**                |
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
- **The real ground-truth corpus** on disk at `/home/gn/ws/jkk_dataset02` AND
  seeded into the Minio `recordings` bucket — required by smoke, matrix, and the
  bench corpus legs. The shell gates do **not** auto-seed; bring up
  `infra/minio` (`docker compose up -d`) and upload the `nissan_zala_*.mcap`
  corpus before running them.
- **The C++ CLI** built (`cd PJ4/pj-official-plugins && ./build.sh toolbox_dexory_cloud`)
  for smoke/matrix — they shell out to `dexory-cloud-cli`. (smoke/matrix build it
  on demand if missing.)

`make ci-integration` and the in-process bench microbenches need **only** Docker
+ Go (no corpus, no CLI) — they are the parts that run in GitHub CI.

---

## Ground truth — the TWO lockstep locations

The real-corpus expectations are pinned in **two** files that MUST move in
lockstep whenever the `recordings` bucket is reseeded with a different corpus:

1. `scripts/smoke.sh` — shell constants:
   - `EXPECT_FILE_COUNT=8`
   - `EXPECT_S3_KEY="nissan_zala_50_zeg_1_0.mcap"`
   - `EXPECT_IMU_MSGS=14904` (the `…/imu` topic count in that file)
2. `PJ4/pj-official-plugins/toolbox_dexory_cloud/tests/backend_connection_live_test.cpp`
   — the C++ live-test constants the GUI's exact `BackendConnection` asserts:
   - `kExpectedSequenceCount = 8`
   - `kKnownSequence = "nissan_zala_50_zeg_1_0.mcap"`
   - `kImuMessageCount = 14904`
   - (also `kExpectFileHierarchy = false` and
     `kExpectMetadataVocab = {robot_id, procedure_date, operator}` — the
     server-reported `BackendCapabilities`, pinned identically in both lanes).

If you reseed and these drift apart, smoke will fail at the C++ live leg with a
mismatch. **Change both, together.**

The CI/synthetic ground truth (NOT real-corpus) lives in exactly **one** place
and cannot drift: `server/internal/genmcap` (`DefaultSpecs()`). Both the
generator (`cmd/gen-ci-fixtures`, which writes the bytes) and the harness
(`internal/ws/ci_integration_test.go`, which reads the same `FileSpec` list to
know what to assert) read it — change a spec and both move at once.

---

## Reseeding the real corpus

1. Bring Minio up:
   ```bash
   cd infra/minio && docker compose up -d        # bucket `recordings`, :9000
   ```
2. Upload the corpus objects (the `nissan_zala_*.mcap` set) into
   `s3://recordings/` (e.g. via `mc cp`). The bytes on disk at
   `/home/gn/ws/jkk_dataset02/` are the round-trip ground truth `mcapdiff`
   compares against — keep them identical to what you upload.
3. If the corpus changed (different files / counts), update **both** lockstep
   locations above, then run `make smoke` to confirm green.

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
Each gate uses its OWN SQLite DB so they never share state:
- smoke: `/tmp/pj-cloud-smoke-catalog.db`
- matrix: `/tmp/pj-cloud-matrix-catalog.db` (+ `…-gcs-catalog.db`)
- interactive / default: `/tmp/pj-cloud-catalog.db` (override with `-db` or
  `PJ_CLOUD_DB`).
If a gate behaves as though it has stale data, delete its DB plus the WAL/SHM
siblings (`<db> <db>-wal <db>-shm`) and rerun — the indexer cold-extracts from
scratch.

**`make smoke` fails at step a (bucket empty).**
The `recordings` bucket isn't seeded. smoke does NOT auto-seed — bring up
`infra/minio` and upload the corpus (see "Reseeding the real corpus").

**`make smoke` fails at the C++ live leg with a count mismatch.**
The two lockstep ground-truth locations drifted (or the bucket was reseeded with
a different corpus). Reconcile `scripts/smoke.sh` and
`tests/backend_connection_live_test.cpp` (see "Ground truth").

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

## SDK package (Slice 16+)

Plugins build against the Conan package `plotjuggler_sdk/<SDK_VERSION>` (pin file
`PJ4/pj-official-plugins/SDK_VERSION`, currently 0.6.1 — a local-fork bump
carrying the toolbox parser-ingest tail slots). On a fresh machine, publish it
once from the EDITED in-tree SDK before any plugin build:

    cd PJ4/plotjuggler_sdk
    conan create . -s build_type=Release -s compiler.cppstd=20 --build=missing

(`scripts/bump_core_version.py` is deliberately NOT used for this version — it
would move the extern/plotjuggler_core submodule to a nonexistent upstream tag.)

## Real AWS staging bucket (Dexory M1 real-bucket run)

Config: `server/deploy/config.dexory-staging.yaml` (bucket
`dexory-data-offload-staging-bucket`, real AWS — no endpoint override, no
inline secrets; the connector only READS the bucket).

One-time credential drop (NOT in the repo, never commit):

    mkdir -p ~/.aws && chmod 700 ~/.aws
    cat > ~/.aws/credentials << 'CREDS'
    [dexory-staging]
    aws_access_key_id     = <PASTE>
    aws_secret_access_key = <PASTE>
    CREDS
    chmod 600 ~/.aws/credentials

Run + verify (read-only):

    cd server && go build -o ./bin/pj-cloud-server ./cmd/pj-cloud-server
    AWS_PROFILE=dexory-staging ./bin/pj-cloud-server \
      -config deploy/config.dexory-staging.yaml -listen :8084 \
      -db /tmp/pj-cloud-dexory-staging.db > /tmp/pj-cloud-staging.log 2>&1 &
    # watch the indexer scan, then list through the real client stack:
    tail -f /tmp/pj-cloud-staging.log          # expect "indexer: ... run complete scanned=N"
    <plugin build dir>/dexory-cloud-cli --url ws://localhost:8084 list

If the first List fails with 301/PermanentRedirect, the error names the
bucket's actual region — fix `region:` in the staging config and restart.
