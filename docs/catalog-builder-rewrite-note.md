# If the catalog builder were rewritten: language and design (2026-08-10)

**Status:** design note. No rewrite is proposed or scheduled — this records the position
so that *if* a rewrite is ever forced (a major schema redesign, a second bucket layout,
a maintainability wall), the decision starts from evidence rather than from scratch.

Third of three companion records:
`docs/data-lake-alternatives-review.md` (should we have built it?),
`docs/capability-review-2026-08.md` (what does it do today?), this one (if we did it
again, how?).

---

## 1. Language: Go, in the server repo, sharing its packages

Not "Go is better than Python". The argument is specific to this codebase, and it is
about **deleting a boundary**, not about speed.

### 1.1 What the two-language split actually costs

Every row below is duplicated work that exists *only* because the writer and the reader
are in different languages:

| Surface | Python side | Go side | Nature of the duplication |
|---|---|---|---|
| WAN-aware MCAP footer/summary read | `targeted_summary.py` (277 lines, 3 → 1.58 GETs/file, measured 2026-07-29) | `internal/format/summary_reader.go` (≤3 ranged GETs, built after a measured 3m24s-per-file pathology) | **The same optimization, solved independently twice, measured twice, maintained twice** |
| Object key ↔ dimensions | `keyparse.py` (`HIVE_RE` + `rebuild_hive_key`) | `rebuildHiveKey` (`auryn_read.go:26`, "the exact inverse of the Python builder's") | A hand-maintained inverse pair across a language boundary |
| `topic_counts` codec | `varint.encode_counts_blob` | `decodeCountsBlob` ("must stay byte-identical to the writer") | Byte-level format agreement, enforced by comment |
| Schema knowledge | `schema.sql` | the reader's queries + `crosslang_test.go` | Two encodings of one truth |
| The contract itself | — | — | `CATALOG_CONTRACT.md`, 39,299 bytes, **two byte-identical copies** kept in sync and verified with `cmp` |
| Version interlock | `SCHEMA_VERSION` + fail-fast in `db.py` | fail-fast in the reader | Exists purely to police the boundary |
| Dependency pinning | `boto3` / `google-cloud-storage` / `mcap` / `watchdog`, pinned identically in the CI workflow, `smoke.sh`, `ci-integration.sh` and `Dockerfile.builder` ("keep these three in lockstep") | vendored in `go.mod` | A four-place manual pin |

**The M6 migration was right to separate the writer from the reader as processes. It also
separated them by language, and only the first separation was load-bearing.** Single
writer, independent restart, and process isolation are all preserved by two binaries in
one repo: `cmd/pj-cloud-builder` beside `cmd/pj-cloud-server`, sharing `internal/storage`
(the `BlobStore` seam, S3 + GCS) and `internal/format` (MCAP). Every row of the table
above disappears.

### 1.2 The GIL tax

The second-hardest file in the builder is hard mostly because of the GIL. `reconcile.py`
carries a `ProcessPoolExecutor` with an explicit `forkserver` context (to dodge the
fork-after-threads hazard created by the sidecar heartbeat thread), a picklable
`SourceSpec` because a boto3 client cannot cross a process boundary, an `_init_worker`
per-process singleton, ~57 MiB RSS per worker, and `_bounded_completions` windowing.

In Go that is a bounded worker pool feeding a single writer goroutine over a channel,
sharing one client. The domain logic — classify, skip, extract, apply — is unchanged; the
scaffolding around it mostly evaporates.

### 1.3 Deployment

Today: a second base image (`python:3.12-slim`), four pinned wheels, and the
`~/.venvs/pj-catalog` bootstrap that every developer and both harness scripts must
remember. After: one more `CGO_ENABLED=0` static binary in the image already being built.

### 1.4 The rewrite would be unusually safe here

`scripts/catalog-semantic-diff.py` already exists — id-dictionary-decoded comparison, the
only valid equivalence check between two catalogs (they are not byte-reproducible). Run
old and new builders over the same bucket, assert `SEMANTICALLY IDENTICAL`. Very few
rewrites come with a ready-made oracle; this one does, because the parallel-extraction
work already needed it.

### 1.5 What stays Python

The **content-aware metrics pass** (R11–R13, `file_metrics`). numpy/scipy/ML belong in
Python, and `REQUIREMENTS.md` already declares that pass *distinct* from the metadata
builder. So the language boundary does not vanish — it moves to where it earns something:
Go for footer-only metadata on the hot path, Python for optional enrichment that reads
payloads off the critical path.

### 1.6 Why not Rust or C++ — REVISED 2026-08-10 after an ecosystem check

The original two-sentence dismissal ("good MCAP crate, but the win is reuse") was too
thin, and a deliberate look at the 2026 ecosystem splits the answer by scope.

**For the builder rewrite in isolation: still Go, unchanged.** The claimed win is
*contract deletion* — joining the reader's language so `CATALOG_CONTRACT.md`, the
byte-identical codecs, and the duplicated summary reader all disappear. Rust does not
deliver that: the server stays Go, so a Rust builder is still a two-language system with
the entire §1.1 table intact — it merely swaps Python for a faster, safer second
language. Real gains (the GIL scaffolding of §1.2 evaporates; the official `mcap` crate
is upstreamed into foxglove/mcap with memory-mapped indexed reading and
partial-file recovery; `object_store` is Apache-Arrow-governed and production-proven at
InfluxDB/crates.io scale), but the headline benefit is gone. Reuse still decides it.

**For the lakehouse compute plane (the greenfield note's Tiers 2–3 and the analytical
door): the ecosystem answer is Rust, and it is not close.** What the check found:

- **The columnar stack's canonical implementations are Rust.** `arrow-rs` +
  `parquet` are the reference tooling of the new lakehouse generation; DataFusion is
  the embeddable query engine of the "deconstructed data systems" era — ClickBench-
  leading on Parquet, tens of millions of plans/day inside InfluxDB 3, chosen by teams
  *migrating off DuckDB* partly to remove their last C++ dependency. Go has no
  equivalent: its Parquet/Arrow libraries are second-tier, and there is no embeddable
  Go SQL engine over Parquet — the Go path to an analytical door is shelling out to
  DuckDB via cgo, which this repo's constraints forbid.
- **The training-export target is Rust-native.** Lance (and the LanceDB stack) is
  Rust; a LeRobot/Lance projection tier written in Go would be calling across a
  boundary that a Rust tier gets in-process.
- **The robotics-infrastructure field is moving the same way.** Rerun's entire data
  layer is Rust; dora-rs hit 1.0 in Q1 2026; the official MCAP Rust implementation is
  first-party. A Rust compute plane swims with this current; a Go one swims alone.
- **Two side benefits line up with this repo's actual needs:** `object_store`'s
  unified ranged-read API is precisely the primitive the whole system lives on, and
  Rust's wasm story could serve the plugin's wasm decode core from the same codebase.
  Python analyzers keep their place by *wrapping* a Rust core (the PyO3 pattern Polars
  and Lance use) instead of reimplementing readers.

**The decision rule, then, is about trajectory, not taste.** If the product stays
"catalog + streaming connector," Go-only is the low-entropy choice and this note's
recommendation stands as written. If the lakehouse evolution is *committed* — Parquet
projections, an embedded query engine, training exports — the new compute-plane
components should start in Rust, because that is where their load-bearing libraries
are canonical; and at that point folding a rewritten T0 indexer into the Rust plane
(rather than Go) becomes defensible, with the Go server remaining untouched as the
hardened interactive door. The three-language cost this note warns about is real
either way — the mitigation is to draw the boundary per *plane* (Go: serving; Rust:
compute; Python: exploratory analyzers over a Rust core), never per component.

C++ remains rejected: aws-sdk-cpp plus hand-rolled plumbing, with neither Go's reuse
nor Rust's ecosystem to show for it.

### 1.7 The wider premise: if BOTH writer and server are rewritten (added 2026-08-10)

Everything above answers "rewrite the builder, keep the server." Under the wider
premise — one language for both sides — the analysis inverts, because the argument
that carried Go was *joining the reader's language*, and with the reader itself in
play, **either language deletes the contract**. The tiebreaker moves to what each
side's workload needs, and from the §1.6 ecosystem check every remaining asymmetry
points one way:

- **The lakehouse compute plane** (Parquet projections, embedded query engine,
  Lance/LeRobot export) is only first-class in Rust — in Go it is second-tier
  libraries or forbidden cgo. A full rewrite that picks Go unifies the product as it
  is; one that picks Rust unifies the product this review series says it wants to
  become.
- **The server's own stack has no gap in Rust**: tokio + tungstenite/axum for the WS
  layer, prost for the wire, rusqlite for the read-only catalog, `object_store` for
  ranged reads (a better fit for the range-GET-shaped serving path than the Go SDKs),
  the first-party `mcap` crate for the format layer, no GC in the streaming hot path.
  One property changes shape: the CGO_ENABLED=0 purity becomes "vendored C compiled
  statically" (rusqlite/zstd-sys) — the operational property (one static
  cross-compiled binary, no runtime deps) is preserved via musl/zig builds, the
  no-C-toolchain purity is not. (Watch item, not a plan: Turso's pure-Rust SQLite.)
- **The C++ plugin and the wasm decode core become an argument instead of a
  bystander**: Rust↔C++ FFI (cxx) is far saner than cgo, and Rust's wasm toolchain is
  best-in-class — one decode core could serve the native server, the C++ plugin, and
  the browser build from a single crate.

**So under the both-sides premise: Rust.** Go remains the answer only if the
lakehouse ambition is dropped — then its simplicity, iteration speed, and the
avoidance of Rust's learning curve win on their own.

Two honest caveats, recorded so the recommendation is not mistaken for an urging:

1. **The premise is the expensive part.** The builder has a real rewrite driver
   (§1.1–§1.2); the server does not — it is hardened, pinned by tests encoding years
   of subtle findings (handshake starvation, deterministic marshal, TLS/SNI, resume),
   and rewriting it discards that for zero user-visible gain. A both-sides rewrite is
   justified by the lakehouse trajectory or not at all.
2. **If it happens, it happens strangler-style, server last**: shared core crates
   first (keyparse, varint, summary read, schema), then the builder (gated on
   `catalog-semantic-diff.py`), then the compute plane, and the server only when the
   Rust stack has proven itself — behind the same wire contract, gated on the
   language-agnostic harness (`make smoke` / `e2e-layout` don't care what serves the
   protocol). The proto and the harness make even a server swap a verifiable step
   rather than a leap.

---

## 2. What I would do differently

### 2.1 Make the key → dimension mapping configurable (the big one)

`HIVE_RE` (`keyparse.py:15`) is one hardcoded regex requiring exactly:

```
customer=<c>/customer_site=<s>/robot=<r>/source=<x>/date=<d>/<name>.mcap
```

Anything else — a customer whose bucket is `/{fleet}/{date}/{vin}/`, or flat filenames —
routes **every object** to `catalog_failures`. For a product positioned as "point it at
your lake", the builder currently works on exactly one lake's layout.

Rewrite: a **declarative per-deployment pattern** (named-capture template or regex,
validated at startup), the raw object key stored **verbatim** alongside the parsed
dimensions, and the round-trip guard kept — checked against the *configured* pattern
rather than a compiled-in one. Storing the key costs roughly 1 GB at 8.78M files against
a 2.5 GB catalog; it buys arbitrary customer layouts and ends the need for an exact
parser inverse on the reader side.

Dimension *names* would follow: `customer/site/robot/source` is one deployment's ontology
baked into table names. A generic `dimensions(file_id, dim_id, value_id)` shape with a
configured display order is the portable form — at the cost of losing the strict
hierarchy that currently makes illegal states unrepresentable. That trade needs measuring
before committing; the hierarchy is genuinely valuable in the picker.

### 2.2 Migrations, not fail-fast rebuild

`open_db` refuses a version mismatch and the documented recovery is "delete the DB"
(`db.py:104-120`) — a ~23-hour rebuild tax on every schema change, which will quietly
discourage exactly the schema evolution the product needs (`dimension_counts`, derived
tags, `file_metrics` all want one). A migrations table with additive steps plus targeted
backfill jobs removes it.

### 2.3 …which, together with 2.2, shrinks the generation machinery

Snapshot leases, the opaque generation token, and `ERROR_STALE_CATALOG` all exist because
rowids renumber when a rebuild publishes a *new file*. With migrations, full rebuilds
become rare and renumbering mostly stops — that layer becomes a safety net rather than a
load-bearing protocol on the wire. Keep the mechanism (it is correct and cheap); stop
depending on it for routine operation.

### 2.4 Streaming reconcile from day one

Temp indexed table, presence and diff computed in SQL, the bucket never resident in
memory — and enumeration off the writer thread from the start. Both are already written
down as follow-on work; in a rewrite they are the default, not a retrofit.

### 2.5 Never ship `derive_tags() → []`

Three trivial derived tags on day one (duration bucket, topic count, a topic-set layout
id) would have kept the facet path honest instead of building an 804 ms scan over a table
that has been empty in every production catalog since the beginning. A schema surface
with no producer is a liability, not a placeholder.

### 2.6 Publish-as-artifact as a first-class mode

The publish step already produces a checkpointed, self-contained, sidecar-free file.
Make "publish → upload to the bucket → readers download and open read-only" a supported
deployment mode. It removes the builder/server same-host constraint without introducing
Postgres, and it reuses the `ReopenIfSwapped` path unchanged.

### 2.7 Instrument both sides from the start

The builder's status sidecar is genuinely good and should be kept. The server has 23
counters and gauges and zero histograms; per-RPC and per-storage-operation latency belongs
in the first version, not the fifth.

### 2.8 One thing to reconsider, not to change

The packed `topic_counts` varint blob forecloses per-topic count predicates ("files where
`/imu` has more than N messages") — the alternative, a `file_topic_counts` row per
(file, topic), is ~175M rows at current scale and is correctly rejected. In a single
language the codec-duplication cost disappears, so the blob stays. If per-topic
thresholds ever matter, add rows for the top-K topics only rather than unpacking the blob.

---

## 3. What I would keep unchanged

The parts that took real thought, none of which are language-specific:

- **Quarantine semantics** — a file is never simultaneously cataloged and failed.
- **Round-trip-or-quarantine** — never guess a row from a near-miss key.
- **The count check** — `sum(counts) == message_count` inside the transaction.
- **Cache reload on rollback** — ids from a rolled-back transaction can never poison the
  in-memory caches.
- **etag-skip before any body read** — a restart over a cataloged lake reads zero files.
- **Targeted read with the streamed path as the sole semantics authority** for quarantine
  verdicts.
- **Atomic publish** (temp → checkpoint-gate → rename → dirfsync).
- **The status sidecar** and **`catalog-semantic-diff.py`**.

---

## 4. Sequencing, if it ever happens

1. Port `keyparse` + `varint` + the summary extractor into Go **as shared packages the
   current reader also uses**, deleting the duplicate implementations first. This is
   valuable on its own and does not commit to a rewrite.
2. Build `cmd/pj-cloud-builder` against those packages, single-threaded, no daemon.
3. Gate on `catalog-semantic-diff.py` over the staging corpus: `SEMANTICALLY IDENTICAL`.
4. Add the worker pool, the event/audit producers, the tag endpoint (now an in-process
   call for a co-located server, or an HTTP endpoint — the unix socket exists only
   because of the language split).
5. Cut over behind the same publish protocol; keep the Python builder runnable for one
   release as the differential oracle.

## 5. What would change this recommendation

- **The metrics pass lands first and grows large.** If payload-reading enrichment becomes
  the centre of gravity rather than a side pass, Python's ecosystem outweighs the
  boundary cost and the split should stay.
- **A second consumer of the catalog appears in another language.** Then the contract is
  earning its keep and the boundary is real rather than accidental.
- **The team is Python-first in practice.** A rewrite that nobody is comfortable
  maintaining is worse than a boundary that is merely tedious. This note argues from the
  code, not from who works on it.
