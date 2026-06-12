# infra/fake-gcs — the Asensus/M1b GCS emulator endpoint

This is the GCS-leg analog of `infra/minio`: a local
[`fsouza/fake-gcs-server`](https://github.com/fsouza/fake-gcs-server) emulator that
the storage seam's `gcs` arm (`server/internal/storage/gcsreader.go`, Plan A Task 14b)
talks to over `STORAGE_EMULATOR_HOST`. It exists so the dual-leg correctness gate
(Plan A Task 46a) can prove the `storage.gcs` arm round-trips **identically** to the
S3/Dexory arm against the same 8-MCAP corpus.

**It is NOT auto-started by `make smoke`.** `make smoke` stays entirely on Minio
(`:9000`) and must succeed with this emulator down. Only the deeper `make matrix`
gate (leg **m8**) brings this up — and tears it down again if it was the one that
started it. Manual use is fine too (see below).

## What it serves

- HTTP (not HTTPS) on `:4443`, `-public-host localhost:4443`. The Go storage client
  over `STORAGE_EMULATOR_HOST=localhost:4443` uses plain HTTP, which sidesteps
  self-signed-cert trust friction. (`cloud.google.com/go/storage` auto-points at the
  emulator and skips auth whenever `STORAGE_EMULATOR_HOST` is set — see
  `gcsreader.go`'s `NewGCS` doc comment.)
- `memory` backend: the bucket + objects live only for the container's lifetime, so
  every leg starts clean.
- Bucket `recordings`, seeded with **exactly the 8 ground-truth MCAPs** (the same
  corpus Minio holds): `nissan_zala_50_{sagod,zeg_1..4}_0.mcap`,
  `nissan_zala_90_{country_road_1,country_road_2,mixed}_0.mcap`.

## Why seed-via-API and not a bulk mount

`/home/gn/ws/jkk_dataset02` holds **extras** beyond the 8: a
`nissan_zala_50_zeg_2_0 (Copy).mcap` (a duplicate of zeg_2 whose time range would
overlap and break the stitch / overlap-rejection invariants) plus non-`.mcap` notes.
A bulk read-only mount as bucket `recordings` would index a 9th file and drift the
pinned counts. `seed.sh` therefore uploads **exactly** the 8 named keys (an explicit
list, not a glob) via the JSON upload API. The pinned counts stay identical to the S3
corpus: `zeg_1 = 33670`, `imu = 14904`, `Σ8 = 337861`.

## Manual use

```bash
# bring it up (compose --wait blocks on the healthcheck)
cd infra/fake-gcs && docker compose up -d --wait

# seed exactly the 8 ground-truth MCAPs (idempotent — re-running overwrites)
./seed.sh                                    # defaults to http://localhost:4443 + the dataset dir
# or: ./seed.sh http://localhost:4443 /home/gn/ws/jkk_dataset02

# point a server at it
cd ../../server
STORAGE_EMULATOR_HOST=localhost:4443 \
  go run ./cmd/pj-cloud-server -listen :8083 \
  -config <(printf 'storage:\n  s3: null\n  gcs:\n    bucket: recordings\n')

# tear down
cd ../infra/fake-gcs && docker compose down
```

## Storage-parity note (the ~10% target)

`make bench-storage` (`server/bench/storage_parity_test.go`) measures raw `GetRange`
throughput Minio vs fake-gcs on one ground-truth object and reports both numbers. On
this **shared dev box** (emulator + server + client co-resident, in-memory backend) it
asserts only a generous non-flapping floor — `gcs >= 25% of s3` — because both are
loopback-and-CPU-bound, not network-bound. The plan's "~10% parity" figure is a
**reference-machine** criterion (real GCS vs real S3 over a real link), not what this
co-resident microbench measures; treat the reported numbers as a sanity check, not a
SLA.
