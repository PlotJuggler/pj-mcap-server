# Greenfield: an MCAP-native lakehouse architecture (2026-08-10)

**Status:** design note — a from-scratch architectural answer, deliberately independent
of how this repo implements things. Fourth of the companion records:
alternatives review (*should we have built it?*), capability review (*what does it do?*),
rewrite note (*how would we rebuild the builder?*), this one (*if the product is
"serve MCAP natively, evolve into a lakehouse," what is the right architecture?*).

Target user: the common robotics startup — ROS 2 → MCAP, recordings landing in S3/GCS,
wants browse/find/visualize now and ML/analytics later. Evolution path explicitly in
scope: original MCAPs living alongside strategic Parquet projections, and automatic
tagging by payload analysis.

---

## 1. What the industry actually does (August 2026)

### 1.1 Foxglove — ran both architectures, and the order matters

Foxglove's original Data Platform is the **transcode** model: uploads land in an inbox
bucket, the platform rewrites them into smaller, better-partitioned per-topic MCAP files
in a separate **lake bucket** it controls, and indexes by device / time range / topic.
Query performance is guaranteed because they own the layout; the price is full data
duplication and a platform-shaped copy of your lake.

Then — years in — they shipped **index-in-place for self-hosted sites**: a metadata
index over the customer's *existing* bucket, reading the original recordings at query
time, explicitly for "teams that already treat object storage as their canonical
archive." Two details of that announcement carry most of the lesson:

- Query performance becomes **hostage to how the robots wrote the files**, so
  index-in-place ships with a producer contract: lz4/zstd compression, chunks under
  ~1 MB, time-bounded files, high-bitrate streams stored apart from telemetry.
- They kept both models. Transcode survives as the answer when producer discipline
  can't be assumed.

Foxglove also models **devices** and **events** (instant or time-range annotations) as
first-class entities — recordings hang off devices; value hangs off events.

### 1.2 Rerun — the query-native end of the spectrum

Rerun's unit is the **column chunk**: variable-sized Arrow record batches carrying
multi-rate, multimodal data, stored in `.rrd` files with an embedded manifest enabling
random access without full scans. The open-source catalog server registers recordings
**in place** and lazy-loads chunks on demand; Rerun Hub (commercial) is the same catalog
scaled to object storage, dynamically materializing merged schemas across heterogeneous
chunks. Access is **SQL and dataframes** (DataFusion) directly against recordings —
including MCAP, via unified loaders that produce Arrow chunk streams rather than
converting the source — plus a PyTorch dataloader that streams training batches straight
from the recordings, eliminating the export step. They explicitly reject Parquet
row-groups as a poor fit for multi-rate robotics data.

The bet: the API for the ML era is a query engine over recordings, with the visualizer
as just one consumer. The risk they accept: `.rrd` is theirs — an MCAP-native shop must
either convert or trust their loaders.

### 1.3 The rest, compressed

- **Roboto**: ingest = *indexing + statistical summarization* per topic; RoboQL queries
  over metadata + topic stats; SDK fetches topic slices; **Actions** = user-supplied
  post-processing jobs — the "analyzer" pattern productized.
- **LeRobot v3** (Hugging Face): the de-facto *training-side* format — multi-episode
  Parquet shards for low-dimensional signals + MP4 shards per camera + JSON metadata;
  16k+ public datasets. Whatever a robotics lakehouse becomes, this is a shape it must
  be able to *export*.
- **Heex**: event-triggered capture — only windows around triggers ever leave the robot.
  The reminder that the unit of value is the *moment*, not the file.
- **DuckLake / S3 Metadata / Iceberg-catalog trend**: metadata belongs in SQL; lake data
  stays put in open formats. (Established in the alternatives review.)

### 1.4 The convergence

Three independent vendors arrived at the same three moves:

1. **Leave the data where it is; derive everything.** Foxglove built index-in-place
   *after* transcode; Rerun indexes in place by design; S3 Metadata is AWS shipping the
   same idea as a managed service.
2. **Grains above the file.** Devices/runs/episodes as identity; events as the value
   grain. Nobody serious treats the file as the semantic unit — files are log-rotation
   artifacts.
3. **Two access doors.** An interactive door (stream a selected subset into a
   visualizer) and an analytical door (SQL/dataframes over metadata + columnar
   projections, feeding notebooks, training, and increasingly agents).

---

## 2. The architecture I would build

One sentence: **an immutable MCAP lake, a SQL metadata plane whose core abstraction is a
ledger of derived artifacts, a compute plane of idempotent analyzers and projections
keyed to content identity, and two access doors over both.**

```
                        ┌─────────────────────────────────────────────┐
                        │  STORAGE PLANE  (customer's bucket, S3/GCS) │
                        │   mcap/…  (immutable source of truth)       │
                        │   derived/… (projections: parquet, preview) │
                        └──────────────┬──────────────────────────────┘
                                       │ range-GETs only; never rewritten
        ┌──────────────────────────────┼───────────────────────────────┐
        │ COMPUTE PLANE                │                               │
        │  T0 footer indexer (mandatory, cheap, 1-2 GETs/file)         │
        │  T1 analyzers (opt-in, read payloads → tags, events, stats)  │
        │  T2 projections (policy-driven → parquet, LeRobot, preview)  │
        └──────────────┬───────────────────────────────────────────────┘
                       │ everything written with provenance
        ┌──────────────▼───────────────────────────────────────────────┐
        │ METADATA PLANE (SQL)                                         │
        │  recordings · runs · events · tags(2-layer) · topic stats    │
        │  artifact ledger: (content_id, producer, version) → status   │
        └──────┬────────────────────────────────────┬──────────────────┘
               │                                    │
     ┌─────────▼──────────┐              ┌──────────▼──────────────┐
     │ INTERACTIVE DOOR   │              │ ANALYTICAL DOOR         │
     │ subset streaming:  │              │ open tables (parquet/   │
     │ topics × time ×    │              │ ducklake export) + SQL; │
     │ runs → visualizer  │              │ notebooks, training,    │
     │ (PJ, Foxglove, …)  │              │ agents, BYO engine      │
     └────────────────────┘              └─────────────────────────┘
```

### 2.1 Principle zero: the MCAP lake is immutable; everything else is a rebuildable projection

The single load-bearing decision, and the one the industry converged on from both
directions. Never transcode at ingest (Foxglove's original model, which they themselves
walked back for self-hosted), never adopt a proprietary recording format (Rerun's `.rrd`
is their moat and their adoption tax — an MCAP-native audience already chose its format).
Every derived thing — index, tag, event, Parquet shard, preview — must be deletable and
reconstructible from the lake. This is what makes the lakehouse evolution *safe*: you
can add, version, and discard materializations forever without a migration of truth.

### 2.2 Three grains, from day one

- **Recording** (one MCAP object): content identity = etag; carries the footer facts.
- **Run / episode**: the semantic unit — one deployment/mission/teleop session, spanning
  N recordings via rotation. Identity = (robot, time interval) or an explicit run id
  stamped by the recorder. Browse, stitching, annotation, and export all address runs;
  the file list is an implementation detail behind them.
- **Event**: a time interval (possibly instantaneous) on a run, with tags and
  provenance. *This is where value concentrates* — "the intervention at 14:32", "the
  localization jump" — and it is what analyzers mostly produce, what humans mostly
  annotate, and what training-set curation mostly selects.

Retrofitting grains later is the expensive path (this repo's stitching re-derives the
run per session; annotations attach to files because files are all there is). In a
greenfield schema, `runs` and `events` cost two tables and repay themselves immediately.

### 2.3 The metadata plane: SQL, with a derived-artifact ledger at its core

Metadata lives in a SQL database (single-writer SQLite or Postgres — a deployment
choice behind a seam, per the rewrite note; not an architectural one). The schema keeps
what the current system got right — normalized dimensions, deduped topic-set layouts,
two-layer tags where human overrides always win and survive rebuilds, quarantine as a
first-class outcome — and adds the generalization that turns a catalog into a lakehouse:

**The artifact ledger.** One table:

```
artifacts(content_id,        -- the recording's etag: artifacts bind to CONTENT
          producer,          -- "footer-index" | "tagger:battery" | "proj:parquet" | …
          producer_version,
          status,            -- pending | built | failed(reason) | stale
          output_ref,        -- rows written / derived-object key
          built_at)
```

Every derived thing in the system — the T0 index row, each analyzer's tags/events, each
Parquet projection — is an entry. The ledger *is* the lakehouse: freshness is a query
(`content_id × producer` pairs not `built` at current `producer_version`), re-analysis
after an analyzer upgrade is a version bump, staleness on re-upload is automatic (new
etag ⇒ no entries), and "what produced this tag?" always has an answer. R12/R13 of the
current requirements (metrics stamped by fingerprint, pending-never-scan) are this
ledger specialized to one artifact kind; the greenfield move is making it the spine for
*all* of them.

**Provenance on every assertion.** Tags and events carry their producer. Human edits are
a distinct layer that masks or overrides machine output and is never touched by
recomputation — the override-survives-reindex model, generalized from tags to events.

### 2.4 The compute plane: three tiers, one contract

All tiers read the lake via range-GETs, write metadata with provenance, and are
idempotent per `(content_id, producer, producer_version)` — which makes retries,
backfills, and re-runs boring. The ledger is the work queue: at startup or on schedule,
`pending ∪ stale` is the todo list; discovery (events or listing) only inserts rows.

- **Tier 0 — footer indexer.** Mandatory, cheap (1–2 range GETs/file), the only tier on
  the ingest critical path. Reads summary/footer only; emits recording facts, topic
  stats, run assignment. Correctness bar as per the current builder (round-trip-or-
  quarantine, count checks, streamed-fallback authority) — that discipline is a keeper.
- **Tier 1 — analyzers.** Opt-in payload readers: auto-taggers, event detectors,
  per-signal statistics (min/max/percentiles for threshold queries), embedding
  extractors later. Each declares input topics + output kinds; runs as a sandboxed job
  (container; per-signal stats can be built-in). This is Roboto's Actions and the
  "system that adds tags to MCAPs automatically" — as ledger subscribers, not as
  ingest steps. An analyzer crash quarantines one artifact, never the catalog.
- **Tier 2 — projections.** Policy-driven materializations into `derived/` in the same
  bucket: topic → Parquet for analytics ("strategic timeseries conversion" — for the
  ~10% of topics that are low-rate numeric signals, exactly where Parquet fits and
  Rerun's row-group objection doesn't bite); LeRobot-shaped export for training;
  downsampled preview MCAPs for instant scrubbing of huge recordings; optionally a
  re-chunked copy of a *hot* dataset when producer discipline is poor — Foxglove's lake
  bucket reframed as a cache with an eviction policy instead of an ingest requirement.

### 2.5 Two access doors

- **Interactive door** — the subset-streaming server: runs × topics × time window →
  progressive download into a visualizer, resume, cancel, latched-state replay. This is
  what the current repo does well and what a PlotJuggler-native product uniquely offers;
  unchanged in role, re-pointed at runs instead of files.
- **Analytical door** — the metadata plane itself published as **open tables** (the
  catalog exported to Parquet/DuckLake in `derived/catalog/`, refreshed on publish), so
  a customer aims DuckDB/Polars/Spark — or an agent — at their own bucket with zero
  vendor API in the path. Optionally a thin Arrow Flight / DataFusion service later for
  joins across metadata + Tier 2 projections. The strategic point: for the ML era the
  *query surface* is the product's second half, and making it open formats instead of a
  proprietary RPC is both the cheap and the differentiating choice for a self-hosted,
  bring-your-own-bucket product.

### 2.6 The producer contract

Foxglove's index-in-place preconditions, adopted as a shipped artifact rather than a
docs page: a recorder profile + linter defining **"lakehouse-ready MCAP"** — chunked +
summarized (already enforced by the current FormatCodec), zstd, chunks ≲1 MB,
time-bounded files, high-bitrate topics separated from telemetry, and metadata records
carrying identity (run id, robot id — the existing `s3_key` record generalized). Run the
linter in the upload path or CI on the robot side; grade files in the catalog. The
cheapest place to fix query performance is at write time, and a startup audience will
happily adopt a profile that makes every tool faster.

### 2.7 What I would explicitly not do

- **No transcode at ingest** — walked back by the vendor who tried it; keep re-chunking
  as an opt-in cache (T2).
- **No proprietary recording format** — MCAP-native *is* the wedge against Rerun for
  this audience.
- **No Iceberg as the catalog** — file-grain row mutability and interactive point
  lookups; settled in the alternatives review. Open-table *export* of the catalog, yes.
- **No mandatory SaaS control plane** — self-hosted, bucket-resident derived data;
  everything above works air-gapped.

---

## 3. Mapping the existing system onto this

The point of the exercise: the built system is not displaced by this architecture — it
*is* two of its boxes. The interactive door (subset streaming, resume, stitching,
latched replay) and the Tier 0 indexer (footer-only, quarantine discipline, atomic
publish) exist and are hardened. What the greenfield design adds around them, in
dependency order:

1. **Grains** — `runs` + `events` tables; recorder metadata for run identity; browse
   and open-session address runs. (Schema bump; pairs with the one already planned.)
2. **Ledger + provenance** — the artifacts table; tags/events carry producers; the
   existing tag-override layer generalizes to events unchanged.
3. **Analyzer bus** — `derive_tags()`'s replacement is the first, in-process analyzer
   (the capability review's summary-derived tags); containerized Tier 1 follows.
4. **Projections** — topic→Parquet with ledger-tracked freshness; catalog export as
   open tables (the analytical door's cheapest form); preview MCAPs.
5. **Producer contract** — the linter + profile, and a catalog "grade" per file.

Each step is independently shippable, none rewrites the streaming path, and the first
two are where the leverage is: they change what every later feature can address.
