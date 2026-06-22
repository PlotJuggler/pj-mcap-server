# Auryn Catalog Migration — Deferred Implementation Plan

**Status:** PREPARED FOR EXECUTION (planned 2026-06-22; decisions locked). The auryn
Python builder is vendored in this repo as the **`mcap_catalog/`** submodule.
**Authors:** Davide + Claude + Codex (three-way review; see investigation 2026-06-22).
**Supersedes for the catalog subsystem:** the Go `internal/indexer` write path.

> Required execution sub-skill when this plan is picked up:
> `superpowers:executing-plans`. Tasks are `- [ ]` checkboxes; follow the spec's
> §13 build order where it still applies. Keep `make smoke` / `make matrix` green
> at every task boundary.

### Execution entry point (read this first)

- **Python builder (sole writer):** the **`mcap_catalog/`** submodule = the auryn
  `mcap_catalog_builder` (`schema.sql`, `db.py`, `builder.py`, `storage.py`, …),
  pinned at auryn `main`. Fresh clone: `git submodule update --init mcap_catalog`.
- **Go server (read-only reader + UNCHANGED streamer):** `server/` (this repo).
- **The two specs:** this doc (the migration) + `catalog-vocabulary-rpc.md` (the
  `GetVocabulary` filter RPC, locked V1–V7). Both are self-contained — execution
  should not need the planning conversation; if it does, fix the doc.
- **First slice (smallest end-to-end proof):** §1.0 contract doc + §1.2
  `schema_version` + §2.1 read-only open + §5.1 Hive fixtures — a Go server reading
  an auryn-written DB on regenerated fixtures, proving the cross-language contract
  before the heavy query rewrite / override-layer port.
- **Resolve at execution time:** sub-decisions **D1** (s3_key rebuild) and **D2**
  (tag-edit write path) in §7 — they gate §2.4 and §1.1 respectively.

---

## 0. Goal & chosen architecture

Replace the current **Go-writes-and-reads** catalog with a **split**:

```
  ┌────────────────────────┐         ┌──────────────────────────────────┐
  │ Python mcap_catalog_    │  WAL    │ Go pj-cloud-server (READ-ONLY     │
  │ builder  (SOLE WRITER)  │ ─────►  │ catalog) + UNCHANGED streamer     │
  │  new SQLite schema      │ shared  │  internal/ws + internal/session   │
  └────────────────────────┘  DB     └──────────────────────────────────┘
```

- **Python `mcap_catalog/mcap_catalog_builder`** (vendored submodule) becomes the
  *sole* writer of the SQLite catalog (new schema: path dimensions, topic-set dedup,
  partial error indexes, EAV tags, event-driven discovery + authoritative reconcile).
- **Go `pj-cloud-server`** is gutted on the write side (`internal/indexer` deleted,
  `internal/catalog` writer paths deleted) and becomes a **read-only** consumer of
  the auryn schema. **The streaming subsystem (`internal/session`, `internal/ws`
  session path, `internal/format` chunk code) is UNCHANGED** — it keeps the
  ~200 MB/s SOW path in Go.

### Locked decisions (Davide, 2026-06-22)

| # | Decision | Status |
|---|---|---|
| 1 | **Streaming stays in Go.** 200 MB/s gate is GIL-bound in CPython. | LOCKED |
| 2 | **Production bucket is Hive-partitioned** → auryn's key model fits. Only local synthetic fixtures are flat → **regenerate fixtures under Hive keys** (option a). | LOCKED |
| 3 | **Port the tag override layer** (`tags_override` + `tags_effective` + `UpdateTags`) into the Python writer; Go reads only. | LOCKED, deferred |
| 4 | **Add a GCS backend** to the Python builder (S3+local today). | LOCKED, deferred |
| 5 | **Chunk-index pre-warm:** lazy A + **A+ background warmer** (Go, read-only) + LRU eviction now; **persist raw MCAP summary bytes** as the deferred durable upgrade. NOT option B (Go-struct contract), NOT option C (summary locator). | LOCKED |
| 6 | **Add a `schema_version` table + compat check** at both writer and reader. | LOCKED |

### Non-goals (this migration)
- Moving streaming to Python (decision 1).
- Multi-tenancy / realtime pacing / new auth (unchanged M1 non-goals).
- Numeric "signal > X" filtering (`file_metrics`) — auryn forward-declares it; out of scope here.

---

## 1. The cross-language contract (the centerpiece — design FIRST)

The SQLite file is now an **interface between two languages with two release
cadences.** Everything else depends on pinning it. The Go reader currently serves
these wire fields (`proto/pj_cloud.proto`) that the new schema MUST satisfy:

| Wire field | Source today (Go schema) | Must come from (auryn schema) |
|---|---|---|
| `FileSummary.s3_key` + flat key `"s3_key"` | `files.s3_key` | **rebuild from dimensions** (`rebuild_hive_key`) OR a new stored key column — **DECISION D1** |
| `FileSummary.size_bytes` / flat `"size_bytes"` | `files.size_bytes` | `files.size_bytes` ✅ |
| `FileSummary.recorded` + flat `"start_ns"`/`"end_ns"`/`"duration_ns"` | `files.start_time_ns`/`end_time_ns` | `files.start_time_ns`/`end_time_ns` ✅ |
| `FileSummary.topic_count` / flat `"topic_count"` | count of `topics` rows | `count(topic_set_members WHERE set_id=files.topic_set_id)` ✅ |
| `FileSummary.message_count` / flat `"message_count"` | `files.message_count` | `sum(decode(files.topic_counts))` ✅ (verified at write) |
| flat `"chunk_count"` | `files.chunk_count` | **NOT STORED in auryn** → Python must add `files.chunk_count` (MCAP `Statistics.chunk_count`) — **TASK 2.2** |
| `FileSummary.tags` (+ `Tag.is_override`) | `tags_effective` view | **port override layer** (decision 3) — **TASK 1.1** |
| `ListFilesResponse.metadata` (flat tags_effective map) | `tags_effective` | same as above |
| `GetFileResponse.topics[]` (name, schema_name, schema_encoding, message_count) | `topics` rows | `topic_names`+`schemas`+`topic_set_members`+`topic_counts` join ✅ |
| `FileFilter` {recorded_between, topics_any_of, tag_all, tag_any} | Go SQL | rewrite against new tables — **TASK 2.3** |

**The 8 `DerivedMetadataKeys` (`caps.go`) are a frozen client-ingest contract** —
all 8 (`s3_key, size_bytes, message_count, topic_count, chunk_count, duration_ns,
start_ns, end_ns`) must remain derivable. Audit before sign-off.

### Tasks — contract
- [ ] **1.0** Write a `CATALOG_CONTRACT.md` (in both repos) enumerating: every table/column the Go reader reads, the `topic_counts` varint encoding (unsigned LEB128, `topic_id ASC`), `WITHOUT ROWID` ordering assumptions, the `tags_effective` semantics, and the `schema_version` value. This is the change-control document.
- [ ] **1.1 (decision 3)** Port the **tag override layer** into auryn:
  - [ ] Add `tags_override(file_id, key, value NULLABLE, updated_at)` + a `tags_effective` view (override-wins, NULL masks embedded), mirroring `server/internal/catalog/schema.sql`.
  - [ ] Builder writes embedded tags into `tags` (rename to `tags_embedded` for clarity) and **never touches `tags_override`** on re-catalog (the override-survives-reindex invariant; auryn's smoke must cover it like Go's step h).
  - [ ] Implement an `update_tags(file_id, set, unset)` write entrypoint in the Python writer (the only process that may write).
  - [ ] **DECISION D2 — the tag-edit write path.** Go is read-only, but the wire has `UpdateTags` (client→server). Resolve how a client tag edit reaches the Python writer:
    - (a) **Go forwards** `UpdateTags` to the Python builder over a local IPC endpoint (unix socket / localhost HTTP); Python applies to `tags_override`. *Cleanest — preserves "Python sole writer".* **(recommended)**
    - (b) Go writes **only** `tags_override` (narrow documented exception to single-writer; relies on WAL + `busy_timeout` for the rare, tiny write). Simpler, but breaks the clean invariant.
    - (c) Client writes tags via a separate Python endpoint, bypassing the Go WS (client change).
- [ ] **1.2 (decision 6)** Add `schema_version` (single-row table) to auryn's `schema.sql`; the builder writes it; the Go reader and Python `open_db` both **fail fast on mismatch** with a clear message. Pick an integer; bump on any contract-affecting change.

---

## 2. Go read-only adapter (rewrite the read side; keep the streamer)

- [ ] **2.1** New read-only catalog open path: `catalog.OpenReadOnly(dbPath)` that opens with `mode=ro` (or `immutable`/`query_only`) and **does NOT** apply the embedded Go schema (today `catalog.Open` always runs `CREATE TABLE IF NOT EXISTS` its own schema — `store.go` — which would silently diverge since both schemas name a table `files`). Add the `schema_version` compat check here.
- [ ] **2.2 (TASK from 1)** Coordinate with Python to add `files.chunk_count` (from MCAP `Statistics.chunk_count`); Go `flatMetadata` reads it.
- [ ] **2.3** Rewrite the catalog read queries against the new tables:
  - [ ] `FilterFiles` — keyset pagination on `files.id`; topic predicate becomes the `topic_names`→`idx_tsm_topic`→`topic_set_members`→`idx_files_set` join; tag predicates over `tags_effective`; **new dimension predicates** (customer/site/robot/source/date) + `has_error`.
  - [ ] `GetFile` — decode the `topic_counts` varint blob (port `varint.decode_counts_blob` to Go) joined with `topic_set_members`/`topic_names`/`schemas` to rebuild `TopicInfo[]`.
  - [ ] `caps.go` — `HasHierarchicalKey` now true (Hive keys); `DistinctMetadataKeys` recomputed from the new tables (8 derived ∪ distinct `tags_effective` keys).
- [ ] **2.4 (decision D1)** `s3_key` production: implement `rebuild_hive_key` in Go (exact inverse, round-trip-checked) so `handlers_session.go` gets each file's object key from dimensions for range-GETs (`session.FileKeys`). *Recommended over storing a raw key* (no duplication, single source of truth) — but if any non-Hive keys ever exist in prod, prefer a stored `s3_key` column instead. Confirm at execution time.
- [ ] **2.5** New wire filters: extend `FileFilter` (proto) with dimension + `has_error` predicates → regenerate Go bindings (checked-in) → client (`toolbox_dexory_cloud`) gains the facet UI. **DECISION D3:** do this now, or hide dimensions behind existing tag/metadata semantics for M1 and add native filters later.
- [ ] **2.6** Delete the write side: remove `internal/indexer/` entirely; remove `internal/catalog` writer paths (`runWriter`, `UpsertFile`, `ReplaceTopicsForFile`, `Replace*Tags*`, `SetOverride`…). **KEEP** `internal/format/chunks.go` + `ChunkIndex` (streaming path) and `internal/storage` (range-GETs). Flip/route `TagEditSupported` per D2.
- [ ] **2.7** `main.go` wiring: drop the indexer goroutine + warm-start scan; open the catalog read-only; start the A+ warmer (§3).

---

## 3. Chunk-index pre-warm (decision 5)

- [ ] **3.1** Add `singleflight` around `cachedChunkIndex` (`handlers_session.go:~57`) so concurrent cold opens of the same file dedupe the WAN summary read. (~0.5d)
- [ ] **3.2** **A+ background warmer** (`internal/warm` or in `main.go`): after startup, read the Python catalog, select candidate files, and call the existing cache-fill path (`Codec.ChunkIndex`→`ChunkIndexCache.Put`) with **bounded concurrency**, **read-only** (never writes the catalog → Python stays sole writer). M1: warm all eagerly. Lake scale: warm by recency/access; expose a `pj_cloud_chunkindex_warm_*` metric. (1–3d)
- [ ] **3.3** Replace `ChunkIndexCache`'s crude full-reset eviction (`indexcache.go`) with **LRU / size-aware** eviction — required before lake scale regardless of option. (small)
- [ ] **3.4 (deferred upgrade)** Persist **raw MCAP summary bytes** + etag in the DB (Python writes them while cataloging; Go rebuilds `FileChunkIndex` locally → durable 0-WAN cold open). Contract = "MCAP summary bytes at spec version X" (stable, spec-defined — *not* Go-struct serialization). Only when cold-open latency is a *measured* lake-scale problem. (4–7d) — **do NOT** do option B (Go-struct persistence) or C (locator-only).

---

## 4. Python writer completeness

- [ ] **4.1 (decision 4)** **GCS backend** for the builder, behind the existing `Source` protocol (mirror `S3Source`): identity = `Generation` (decimal string) + `Updated`, slotted into the `(etag, size, last_modified_ns)` triple exactly as Go's `gcsreader.go` does. `STORAGE_EMULATOR_HOST` for the fake-gcs leg. (Asensus M1b.)
- [ ] **4.2** Retry/backoff parity: add a `retry_with` (≈50–800ms, permanent short-circuit, cancel-aware) wrapping the S3 + GCS call bodies (Go has `storage.retryWith`; auryn has none).
- [ ] **4.3** `synchronous=NORMAL` PRAGMA under WAL (set consciously in both `db.py` and the Go reader DSN; document the agreed value).
- [ ] **4.4** Populate `files.chunk_count` (TASK 2.2 counterpart) + confirm `has_error` is actually set (today `derive_tags()` returns `[]` and `has_error` is always 0 — wire up real validation-health).
- [ ] **4.5 (quarantine visibility — from Plan A/C)** auryn already records per-file extract failures in `catalog_failures(s3_key, failed_at_ns, error_text)` and continues the build (one poison file must NOT abort it — keep this). Add: the **Go reader surfaces the last-N `catalog_failures` on the dashboard** so operators see *which* files failed and why (distinct from 4.4's per-file `has_error` health flag).
- [ ] **4.6 (unsummarized-file precondition — from Plan A/C)** All counts/time-bounds come from the MCAP `Statistics`/summary section; the FormatCodec **rejects unsummarized files**. The Python builder must treat an unsummarized file as an explicit **quarantine/extract-failure** (→ `catalog_failures`), never silent zero-counts — and the regenerated Hive fixtures (5.1) MUST stay **chunked + summarized + Statistics** (an unsummarized fixture is invalid input).

---

## 5. Dev/test harness (decision 2)

- [ ] **5.1** Regenerate local synthetic fixtures under **Hive keys** (`gen-ci-fixtures`, `gen-3d-fixture`, `seed`, the smoke/matrix/CI corpora): keys like `customer=test/customer_site=lab/robot=r1/source=synthetic/date=2026-06-22/<name>.mcap`. Keeps **dev == prod** and exercises dimension extraction in CI.
- [ ] **5.2** Update the lockstep ground-truth pins (`scripts/smoke.sh` constants + `tests/backend_connection_live_test.cpp`) to the new keys + new catalog assertions (dimensions present, `has_error`, tag override-survives-reindex).
- [ ] **5.3** New cross-language integration leg: **Python builder writes the DB → Go reader serves → round-trip** (replaces the Go-indexer-writes path in smoke/matrix/CI). The auryn unit suite + a joint e2e that starts the Python builder, then the Go server read-only against the same DB.
- [ ] **5.4** CI: add the Python builder job (its pytest suite) + the joint e2e to `.github/workflows/ci.yml`; keep both `{s3, gcs}` legs.

---

## 6. Operations / deployment

- [ ] **6.1** Two-process deployment (`server/deploy/`): Python builder as a daemon/sidecar + Go server (read-only), **shared DB volume**, `PJ_CLOUD_DB` pointing both at the same path. Updates: Dockerfile(s), compose, systemd units, `config.example.yaml`, README. (The "one static binary" property is lost — document it.)
- [ ] **6.2** WAL discipline: Python = sole writer; Go = read-only (enforced via `mode=ro`). `busy_timeout` on both. The only Go→DB write risk is the tag-edit path — resolved by D2 (recommended (a): Go never writes).
- [ ] **6.2a (atomic catalog publish — from Plan C/A; CORRECTNESS)** Cross-process WAL has NO defined handoff for the builder *rebuilding the DB underneath* a live reader. The Python builder must build to a **temp DB then atomically rename** over the served path (never mutate the served file mid-rebuild); the Go reader must **detect the swap** (inode/mtime, or a bumped `schema_version`/build-id row) and **reopen**, so it never serves a torn/half-rebuilt catalog. This is the missing companion to "Go never writes" (6.2) and the "two writers" risk-register line.
- [ ] **6.3** Discovery/infra: decide S3 event-driven (auryn needs `--sqs-url` + bucket notifications → new SQS infra) vs **reconcile-only polling** (simpler, higher latency, no SQS). Independent of schema.
- [ ] **6.4** `run.sh` / bring-up: `--dexory_minio` etc. must now (a) start the Python builder against the bucket and (b) start the Go server read-only. Preserve the one-command local bring-up.
- [ ] **6.5 (catalog-freshness observability — replaces orphaned `indexer_runs_total`)** Moving the indexer out-of-process orphans the in-process freshness signals (last-reindex, `indexer_runs_total`, dashboard staleness). Fix: the Python builder writes a **build-metadata row** (`last_build_ts`, `files_scanned`, `build_outcome`, `builder_version`) into the catalog; the Go reader reads it and **re-exports `pj_cloud_catalog_*`** Prometheus series + a dashboard "catalog freshness / staleness" panel.

---

## 7. Open sub-decisions (resolve at execution time)

| ID | Decision | Recommendation |
|---|---|---|
| **D1** | `s3_key`: rebuild from dimensions vs stored column | Rebuild (`rebuild_hive_key`), unless non-Hive prod keys exist |
| **D2** | Tag-edit write path with read-only Go | (a) Go forwards to Python over local IPC |
| **D3** | New dimension filters now vs deferred to client | **RESOLVED** → server-side dimension filtering + a `GetVocabulary` RPC; see [`catalog-vocabulary-rpc.md`](catalog-vocabulary-rpc.md) |
| **D4** | S3 discovery: SQS event-driven vs reconcile-poll | Reconcile-poll for M1 simplicity; SQS when latency matters |

## 8. Risk register

- **Rowid stability across rebuilds.** Wire `file_id` = catalog `files.id`. Normal browse→open is safe (client resolves by *name* from a fresh `list`), but **in-flight sessions across a reindex** and `source_file_id` in batch bodies assume stability. Mitigate: reindex doesn't renumber existing rows (auryn upserts by composite key, preserving `id`); verify.
- **Silent schema divergence.** Mitigated by `schema_version` (1.2) + the contract doc (1.0).
- **Two writers to one SQLite.** Mitigated by D2(a) — Go never writes.
- **IdxCache pre-warm gap.** Mitigated by §3 (A+ warmer); note restart-cold is *already* today's behavior on a warm-DB restart, so this is not a new regression.
- **Flat fixtures → empty catalog.** Mitigated by 5.1 (Hive fixtures).

## 9. Sequencing & rough sizing

```
1 (contract) ─► 2 (Go reader) ─► 5 (harness) ─► 6 (deploy)
        └─► 4 (Python completeness) ─┘
3 (chunk-index) is independent; land 3.1–3.3 alongside 2.
```

| Block | Rough size |
|---|---|
| 1 contract + schema_version + override-layer port | 3–5 d |
| 2 Go read-only adapter (queries, key rebuild, gut writer) | 5–8 d |
| 3 chunk-index (3.1 ~0.5d, 3.2 1–3d, 3.3 small) | 2–4 d |
| 4 Python completeness (GCS deferred ~3–4d, retry/pragma) | 2–3 d (+GCS) |
| 5 harness (Hive fixtures, joint e2e, CI) | 3–5 d |
| 6 deploy (two-process, run.sh) | 2–3 d |

**Suggested first executable slice when picked up:** Task 1.0 (contract doc) + 1.2
(`schema_version`) + 2.1 (read-only open) + 5.1 (Hive fixtures) — the smallest set
that lets a Go server read an auryn-written DB end-to-end on regenerated fixtures,
proving the contract before the heavier query rewrite and the override-layer port.
