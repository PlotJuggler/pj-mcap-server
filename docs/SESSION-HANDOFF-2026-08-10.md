# Session handoff — data-lake architecture review (2026-08-10)

**Purpose:** everything needed to continue this discussion on another machine. Transient
document — delete it once the discussion concludes or its conclusions land in the
permanent records.

**Where things are:**

| | |
|---|---|
| Branch | `claude/data-lake-architecture-review-ht8xhj` (8 commits, docs only) |
| PR | [#26 — docs: data-lake architecture review — four companion records](https://github.com/PlotJuggler/pj-mcap-server/pull/26) |
| Artifact | [The Recordings Never Move](https://claude.ai/code/artifact/ab1d5790-1c03-4c8f-9b62-91eabe759f1b) — plain-language explainer of the greenfield architecture (private; shareable from its menu) |
| Related | PR #25 (event-discovery Phases 3–6) — open, all checks green, read and reconciled into the capability review |

**To resume elsewhere:** `git fetch && git checkout claude/data-lake-architecture-review-ht8xhj`,
then read this file plus the four records below.

---

## 1. What this session was

A review of the catalog/data-lake side of `pj-mcap-server`, driven by a sequence of
questions from Davide. No code changed — the output is four design/review records, one
educational artifact, and a set of decisions recorded below.

The four permanent records, in the order they were written:

1. **`docs/data-lake-alternatives-review.md`** — *should we have built the catalog or
   bought it?*
2. **`docs/capability-review-2026-08.md`** — *what does the stack actually do today, and
   what would I improve?*
3. **`docs/catalog-builder-rewrite-note.md`** — *if the builder were rewritten: which
   language, and what would change?*
4. **`docs/mcap-lakehouse-architecture-note.md`** — *greenfield: how would you architect
   an MCAP-native system that evolves into a lakehouse?*

---

## 2. Established facts (don't re-derive these)

Production scale and measurements confirmed from code and plan docs during the review:

- **8.78M files**, 74 customers / 162 sites / 275 robots; catalog ~2.5 GB.
- Ingest: **1.58 range-GETs and 214 KiB per file** after the targeted-summary work;
  etag-skip means a restart over a cataloged lake re-reads **zero** files.
- Cold-build rate: **6,353 files/min** measured on the 25,559-file staging corpus →
  **~23 h for a full 8.78M rebuild** (WAN threads; in-region unmeasured).
- `GetVocabulary`: **2,953 ms** steady state, **40,727 ms** under builder load. Tag-facet
  scan alone = **804 ms**, the costliest leg.
- Code: builder 4,591 lines Python (+5,386 tests); Go catalog reader 2,372; tagipc 163.
- Wire: deterministic marshal takes a 32k-file listing from 1,111,112 B → 512,385 B.
- Session cap `MaxConcurrent` = **16, global**, no per-client cap. Metrics: **23 counters
  and gauges, zero histograms**.
- `derive_tags()` returns `[]` and always has → `tags_embedded` is **empty in every
  production catalog**.
- `HIVE_RE` is one hardcoded regex — any other bucket layout quarantines **every** file.
- `full_reconcile` holds `listings` + `stored` + `dims_by_key` + `present` — Θ(objects)
  parent memory, ~2 GB known baseline.
- Schema bump = full rebuild; `open_db` fails fast, documented recovery is "delete the DB".
  **No migration path exists.**

---

## 3. Decisions reached, with the reasoning compressed

### 3.1 Was building the catalog right? — Yes, narrowly

Nothing on the market reads an MCAP footer. Every alternative would still have left the
expensive half (the summary extractor with its correctness guards) to build. What was
replaceable is the storage/serving half.

- **DuckLake** *is* your architecture (metadata in SQL, data in object storage), arrived
  at independently ~a year earlier; it only reached v1.0 in April 2026.
- **Iceberg/Delta/Hudi:** no — they prune scans over few big files; you do point lookups
  and row-grain upserts over 8.78M rows, and every re-upload becomes a MERGE plus
  compaction.
- **Foxglove/Roboto/Rerun** sell the capability but not the product (PlotJuggler-native,
  don't-touch-my-bucket, self-hosted).
- **We reinvented, under local names:** snapshot isolation (atomic publish + generation
  token ≈ metadata-pointer swap + snapshot id) and columnar compression (`topic_sets`
  dedup + varint blob ≈ dictionary encoding + RLE).

### 3.2 Migrate to DuckDB or Postgres? — No to both, with trip-wires

**DuckDB: no, categorically.** The decisive reason isn't cgo (though `CGO_ENABLED=0`
cross-compiles do forbid it) — it's that DuckDB takes an *exclusive* file lock, so a
builder mutating while the server reads live is unsupported. Your catalog grows **in
place via WAL** (`reopens_total` stays 0 while `files_scanned` climbs); DuckDB would force
every incremental batch into a full rebuild-and-publish of a 2.5 GB file.

**Postgres: right escape hatch, wrong time.** It would delete ~1.5k lines (tag IPC,
flock, publish protocol, swap detection, leases, generation tokens, catalog-free `Hello`)
and remove the same-host constraint — but that machinery is *already written, tested, and
green*, so the benefit is sunk-cost recovery. It also doesn't fix the facet latency
(materialized counts do).

**Trip-wires that flip it to yes:** (1) **Kubernetes** — two containers sharing a local
volume with SQLite WAL doesn't survive it; this is the most likely trigger by far.
(2) >1 server replica or split-host. (3) Multi-user tag editing with real permissions.

**Cheaper middle path if split-host is the actual want:** the publish step already
produces a checkpointed, self-contained, sidecar-free file. Ship it — builder uploads the
`.db` to the bucket, replicas download and open read-only through the existing
`ReopenIfSwapped` path. ~100 lines, no new stateful service.

### 3.3 Adopt Amazon S3 Metadata? — No, not now (costed)

Pricing is **not** the objection: **$2.63 one-time** backfill for 8.78M objects, **$0**
ongoing (free below 1B objects), **$0.30/million** journal updates (~$0.27/month).

The objection: it replaces the **cheap** half of the audit. A full LIST at 8.78M objects
is 8,780 requests — **$0.044**, minutes when sharded. The expensive half is the
writer-thread diff, which an Iceberg result set still has to stream into and be diffed
against. **The streaming reconcile is the real fix and is needed either way** — which is
what the event-discovery design already says (`InventoryFeed` is *gated on* it), and its
own trigger ("nightly audit > ~2 h, tens of millions of objects") puts you below the line.

**Kept for later:** the **journal table** (not the inventory table) as a durable,
ordered, replayable change log — a watermark turns "lost events → enumerate everything"
into "resume from sequence N". Revisit *after* streaming reconcile. Real adoption cost is
deployment surface (table bucket + Athena/Glue or an Iceberg client + IAM in every
customer account, AWS-only), not dollars.

### 3.4 Top improvements to the current system (ranked)

1. **One schema bump carrying derived tags + `dimension_counts`, plus a backfill path.**
   Derived tags are free from the summary already in hand (duration bucket, message rate,
   topic count, has-`/tf`/pointcloud/camera, topic-set layout id, short/gap flags).
   Highest value on the list — it turns the catalog from an inventory into something you
   can ask questions of. Spend the ~23 h rebuild once, and make the *next* change an
   `ALTER`.
2. **Push time filtering to the server.** The client sends only customer/site/robot while
   the server implements `recorded_between`/`topics_any_of`/`tag_all`/`tag_any`. Time is
   the most natural axis in a recording lake and it's currently client-side over ~32k-row
   listings. Add per-robot date bounds to `GetVocabulary` (the deferred V6).
3. **Streaming reconcile** — temp indexed table, presence/diff in SQL. Now also the
   blocker on PR #25's Phase 7, so it's on in-flight critical path.
4. **Instrument** — per-RPC duration histograms, storage latency, errors by code. The
   2.9 s → 40 s regression was found by running the CLI five times by hand.
5. **Multi-user hygiene** — per-connection session quota, author column on
   `tags_override`, document that tenant isolation is by deployment.

Also open: quarantined files invisible to users (dashboard-only); no dimension GC;
`matrix.sh` dead (exit 2) so no real-corpus perf gate; GCS has no event discovery and
PR #25's hot-window audit is S3-only, widening the asymmetry.

### 3.5 Language, if rewritten — evolved across three turns

**Turn 1 — builder only, keep the Go server: Go, in the server repo.** The argument is
*contract deletion*, not language preference. The decisive evidence: the WAN-aware MCAP
footer read **exists twice** (`targeted_summary.py` and `internal/format/summary_reader.go`),
solved independently, measured separately. Plus `keyparse`'s hand-maintained inverse in
Go, the byte-identical varint codec, schema knowledge encoded twice, `CATALOG_CONTRACT.md`
in two `cmp`-verified copies, and a four-place dependency pin. *The M6 split of writer
from reader as processes was right; the split by language was incidental and is what
costs.* Python stays for the content-aware metrics pass.

**Turn 2 — "would you consider Rust?" (ecosystem check performed):** for the builder
*alone*, still Go — a Rust builder keeps the whole two-language duplication table intact.
But for the **lakehouse compute plane**, Rust is the ecosystem answer and it's not close:
`arrow-rs`/`parquet` are canonical, DataFusion is the embeddable engine of this generation
(no Go equivalent short of cgo DuckDB), Lance/LeRobot exports are Rust-native, the official
`mcap` crate is first-party and upstreamed, `object_store` matches the range-GET primitive,
and the field (Rerun, dora-rs 1.0) is moving that way.

**Turn 3 — "I meant both writer AND server": Rust.** With the reader in play, *either*
language deletes the contract, so Go's trump card is gone and the tiebreaker moves to
workload. The server has no Rust gap (tokio + tungstenite/axum, prost, rusqlite,
`object_store`, first-party `mcap`); the static-binary property survives via musl builds
(what changes is `CGO_ENABLED=0` purity, since rusqlite/zstd-sys vendor C); and the C++
plugin + wasm decode core flip from bystander to *argument* (cxx FFI is far saner than
cgo; one decode crate could serve native, plugin, and browser).

**The decision rule: choose by trajectory, and draw the boundary per *plane*** — Go:
serving / Rust: compute / Python: analyzers over a Rust core (PyO3) — **never per
component**. Worst outcome would be a Rust builder *and* a Go compute plane: boundary cost
twice, ecosystem once.

**Two caveats recorded:** the premise is the expensive part (the builder has a rewrite
driver; the hardened server does not — a both-sides rewrite is justified by the lakehouse
trajectory or not at all), and it would go strangler-style with the **server last**, gated
on `catalog-semantic-diff.py` for the builder and on the language-agnostic harness
(`make smoke` / `make e2e-layout` don't care what serves the protocol) for the server.

### 3.6 The greenfield architecture

One sentence: **an immutable MCAP lake, a SQL metadata plane whose spine is a registry of
derived artifacts, a compute plane of idempotent analyzers keyed to content identity, and
two access doors.**

Industry grounding: **Foxglove ran transcode-into-a-lake-bucket for years, then shipped
index-in-place for self-hosted sites** — with a producer contract (zstd, <1 MB chunks,
time-bounded files, high-bitrate topics separated) because index-in-place makes
performance hostage to how robots write. They kept both models. **Rerun** indexes in place
by design, with SQL/dataframe (DataFusion) access over recordings and a PyTorch dataloader
reading them directly; `.rrd` is their moat and their adoption tax. **Roboto** = index +
topic statistics + user "Actions". **LeRobot v3** = the training-side export target.

The design:

- **Rule zero:** MCAPs immutable; everything else is a rebuildable projection. No ingest
  transcode, no proprietary format.
- **Three grains from day one:** recording (file) → **run/mission** (the browse unit,
  spans ~36 files) → **event/moment** (where value concentrates).
- **The artifact registry:** `(content_id=etag, producer, producer_version) → status`.
  Every derived thing is a row. Freshness is a query; analyzer upgrades are a version
  bump; re-upload invalidates automatically; every fact has an author. *This is the
  abstraction that turns a catalog into a lakehouse.*
- **Three compute tiers:** T0 footer indexer (mandatory, cheap) / T1 sandboxed analyzers
  (opt-in payload readers → tags, events, stats) / T2 policy-driven projections
  (topic→Parquet, LeRobot export, preview MCAPs, opt-in re-chunked *cache*).
- **Two doors:** interactive subset streaming (what you have) + the catalog **published as
  open tables** in the bucket so customers/agents query with their own tools, no vendor API.
- **A shipped producer contract:** "lakehouse-ready MCAP" profile + linter, graded per file.
- **Human notes always win** — machine labels are recomputable, human judgment isn't;
  separate layers, overrides never overwritten.

**Your current system is two boxes of this** (interactive door + T0 indexer). Extension
order: grains → registry → analyzers → projections/open tables → producer linter.

### 3.7 DataFusion / DuckDB / DuckLake / Vortex — conversational only, NOT yet in any doc

This is the part most at risk of being lost; capture it if it matters.

- **DataFusion** = "SQLite for analytics" — an embeddable Rust query engine (SQL parser +
  optimizer + vectorized execution over Arrow/Parquet), no storage, no server. Powers
  InfluxDB 3; ClickBench-leading on Parquet. Natively understands Hive-partitioned
  directories (**your lake is already in its favorite shape**), and its `TableProvider`
  trait is how you'd later expose MCAP itself as a SQL table (the Rerun move). **Useful
  exactly when the lakehouse steps land, not before** — and keep it away from the
  interactive path, which is index-seek work SQLite does better.
- **DuckDB** = not a dependency, but **the universal consumer** of your analytical door.
  The acceptance test for published projections should literally be "DuckDB opens it."
- **DuckLake** = the most concretely useful of the four. Parquet in object storage +
  metadata as plain SQL tables (SQLite/Postgres/DuckDB). It solves the problem the
  architecture note hand-waved: *which files constitute the current consistent version of
  a projection* — snapshots, file lists, time travel, transactional updates, as a spec
  instead of bespoke code. Writable from any language (unlike Iceberg's Avro manifest
  tree), metadata can be a SQLite file in `derived/`. Caveat: young, vendor-concentrated
  ecosystem — but low risk as a *publishing* format since the data underneath is plain
  Parquet.
- **Vortex** = watch-list. LF AI & Data incubation, format stable from v0.36; claims ~100×
  faster random access / 10–20× scans vs Parquet; Rust-native, DataFusion pushdown, DuckDB
  extension (Jan 2026). Random access is what training dataloaders need, so it competes
  with Lance for the *training* projections. Mid-2026 verdict: credible prototype, not a
  proven estate — and the analytical door's value is universality, which is Parquet's.
- **Composition:** open interface = Parquet tracked by DuckLake (metadata in SQLite);
  engine inside the product = DataFusion; training-path format = Lance vs Vortex, decided
  when that tier is built. None replaces anything current — they all live in `derived/`,
  the part that's safe to adopt or abandon because the MCAPs never move.

**Addendum 2026-08-21 — DuckDB 2.0 preview (announced 2026-08-17, release fall 2026)**
nuances two of the verdicts above:

- **Quack client/server goes stable.** The "no DuckDB in the serving path" verdict rested
  on cgo + the exclusive single-process file lock. The second leg changes: DuckDB 2.0 can
  run as a network server (`ATTACH 'quack:host'`, token auth). This does NOT reopen the
  browse-path question (still seek-shaped, still SQLite) — but it adds a legitimate
  *third shape* for the analytical door: a stock `duckdb` server process over `derived/`
  + DuckLake, zero custom code, same "one more stateful service" cost class as Postgres.
  Compare against embed-DataFusion when the served door is actually built.
- **The DuckLake bet is reinforced**: partition-aware query planning for lakehouse
  formats, and a DuckDB Foundation advisory board covering DuckDB/DuckLake/Quack —
  softening the vendor-concentration caveat.
- Also relevant: stable async I/O ("dramatically faster queries on network storage",
  Parquet first) improves the consumer side of published Parquet for free; the stable
  single-header C API makes embedding DuckDB in the C++ plugin cheaper (client-side
  analytics — never blocked by cgo, now easier); storage format v2.0 is a breaking
  change, which reinforces choosing SQLite (not a .duckdb file) as the DuckLake
  metadata home; `NEAREST` top-k similarity joins are a future hook for
  "find moments like this one" (lesson 9 territory).

---

## 4. Threads left open

- Whether to fold §3.7 (DataFusion/DuckDB/DuckLake/Vortex) into the architecture note as a
  "technology choices for the derived plane" section. **Currently only in chat.**
- Whether to merge PR #26 as-is, or hold it until the conclusions are acted on.
- The artifact could be re-pitched for a different audience (customer/investor) from the
  same source — offered, not done.
- PR #25 is open and green; its first real SQS enablement will also be its first real
  test (live staging execution was not possible — the dev box's only AWS identity is
  read-only, `dexory-s3-reader`).

## 5. Suggested opening prompt for the next session

> Read `docs/SESSION-HANDOFF-2026-08-10.md` on branch
> `claude/data-lake-architecture-review-ht8xhj`, plus the four records it references.
> We're continuing the data-lake architecture discussion from there. <your question>
