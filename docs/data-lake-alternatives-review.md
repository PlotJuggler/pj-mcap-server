# Data-lake alternatives review — what we built, and what we could have bought

**Date:** 2026-08-10 · **Status:** review record, no code change · **Scope:** the catalog
half of the backend (the Python `mcap_catalog/` builder + the Go read-only reader),
*not* the streaming/session path.

The question this answers: **the catalog creator is ~7.1k lines of hand-written code
plus ~5.4k lines of tests. Was there an off-the-shelf data-lake product that would have
produced the same result?**

Method: read the builder and reader as they stand today (`mcap_catalog_builder/`,
`server/internal/catalog/`, `schema.sql`, `CATALOG_CONTRACT.md`, the deploy compose
files, the 2026-07-29 / 2026-08-09 performance plans), then survey the current market
(August 2026) for anything that covers the same job. Verdicts below are argued from the
constraints the code actually enforces, not from feature lists.

---

## 1. What we actually built

Precisely, so the comparison is fair:

> A **file-grain, derived metadata index** over a Hive-partitioned MCAP lake. One row per
> object. Built by reading each object's **MCAP footer/summary only** (1–2 range GETs,
> never the payload). Stored in **one SQLite file**. Published **atomically**. Served
> **read-only** by a separate pure-Go process that also streams the message data.

Measured facts (from the plan docs and CI, not estimates):

| Fact | Value |
|---|---|
| Production corpus | **8.78M files**, 74 customers / 162 sites / 275 robots |
| Catalog size | ~2.5 GB (≈0.7 GB per 1M files, thanks to the topic-set dedup) |
| Cost to catalog one file | 1.58 range-GETs, 214 KiB read (after the targeted-summary work) |
| Cold-build throughput | 6,353 files/min measured on the 25,559-file staging corpus |
| Warm restart over a cataloged lake | **zero** file reads (etag skip) |
| Facet query (`GetVocabulary`) at 8.78M rows | 2,953 ms steady state; 40,727 ms under builder load |
| Filtered page (keyset on `files.id`) | single-digit ms, flat with corpus size |
| Code | 4,591 lines Python (builder) + 2,372 Go (reader) + 163 (tag IPC) |

The schema is not naive: dimensions normalized into `customers/sites/robots/sources`,
the *set* of channels deduped into `topic_sets` (most files share a layout) with per-file
counts packed as a LEB128 varint blob, tags as indexed EAV in two layers
(`tags_embedded` rewritten every build, `tags_override` user-owned and carried forward
by composite identity), quarantine in `catalog_failures`, freshness in `build_metadata`.

And a lot of the machinery is not about *data* at all — it is about **serving a
single-writer file safely to a live reader**: a `flock` single-writer lock, atomic
temp+rename publish with a verified WAL checkpoint gate, (dev,inode) swap detection with
live reopen, snapshot leasing, an opaque generation token on the wire so a stale cursor
fails `ERROR_STALE_CATALOG`, a catalog-free `Hello` path, and a unix-socket tag-edit IPC
that exists **only because SQLite admits one writer**.

---

## 2. The constraint set any alternative has to clear

These are enforced by the code and CI today, and they are what most of the market fails:

1. **The reader is a pure-Go, CGO-free, cross-compiled static binary.** `release.yml`
   builds `linux/{amd64,arm64}` with `CGO_ENABLED=0`; the catalog reader is
   `modernc.org/sqlite` precisely for this. Anything requiring cgo (DuckDB, libduckdb,
   RocksDB) or a JVM is out of the serving path by construction.
2. **Self-hosted, customer-deployable, bring-your-own-bucket.** The product is a server
   the customer runs next to *their* lake (`docs/ec2-deploy.md`, `docs/gce-deploy-smoke.md`).
   Data never moves to a vendor.
3. **Two clouds.** S3 *and* GCS, behind one `BlobStore` seam.
4. **We do not own the lake layout.** The bucket is written by the customer's robots;
   the catalog is a *derived, rebuildable cache* of it. We may not rewrite objects,
   re-partition, or convert the corpus to another format.
5. **Interactive latency budget.** The browse UI has a 10 s hard client timeout and wants
   sub-second facets; a filtered page must stay flat at 8.78M rows.
6. **Row-grain mutability.** Per-object upsert on re-upload, hard-delete on vanish, plus
   *user tag edits that must survive a full rebuild*.
7. **Footer-only extraction** — the whole point. Metadata for 8.78M multi-GB recordings
   without reading the recordings.

---

## 3. The landscape, with verdicts

### 3.1 Open table formats — Iceberg / Delta Lake / Hudi

The natural framing: `files` becomes an Iceberg table partitioned by
`customer/site/date`, topics an `array<struct>`, tags a `map`, queried by Trino/Athena/
Spark and written by the same footer-reading job.

**Why it does not fit here.** Iceberg is built to prune *scans over few large files*; our
workload is point lookups, keyset pages, and low-cardinality facets over one wide table
with row-grain updates. Every per-object upsert becomes a MERGE that rewrites data files
or accumulates merge-on-read delete files, so we would inherit a compaction service to
keep 8.78M rows healthy — machinery strictly larger than the publish/swap code it would
replace. The Go read side would need `iceberg-go` + a Parquet reader (young, and the
ergonomics of an interactive point query through a manifest tree are poor), or a query
service (Trino/Athena) that costs a network hop and per-query money for a UI facet
refresh. And it drags in a catalog service — Glue, Polaris, Nessie, or a REST catalog —
which is exactly the "one more thing the customer must run" we were avoiding.

**Verdict: no**, at this scale and grain. Worth reconsidering only if the catalog grows a
genuinely analytical second life (fleet-wide aggregates, joins against other datasets).

**But note what we borrowed without naming it:** the atomic publish + generation token
*is* a table format's snapshot isolation — metadata-pointer swap plus snapshot id. We
reinvented it in ~200 lines instead of adopting a spec. At our scale that was the cheaper
trade, but it should be stated plainly rather than treated as novel.

### 3.2 DuckLake — the closest thing to what we built

[DuckLake](https://ducklake.select/) stores **all table metadata in a SQL database**
(SQLite, Postgres or DuckDB) with data files in object storage, and *inlines* small
writes directly into the catalog DB instead of producing tiny Parquet files. It hit
**v1.0 / production-ready in April 2026** with backward-compatibility guarantees.

That is our architecture, arrived at independently and about a year earlier: metadata in
a SQL database, bulk data left in the bucket. The difference is that our "data files" are
MCAPs we did not write and cannot rewrite, and our metadata is domain-specific (topic
sets, message counts, MCAP time bounds) rather than column statistics. A DuckLake table
whose data is entirely inlined would be… a SQLite database with more ceremony.

**Verdict: vindication, not a missed option.** DuckLake did not exist in usable form when
this was designed, and adopting it now would buy nothing we do not already have.

### 3.3 DuckDB as the query engine

The one place off-the-shelf tech clearly beats us: the **facet problem**. Four whole-table
`GROUP BY`s over 8.78M rows cost ~400 ms each in SQLite (2.44 s total, and 40 s while the
builder writes). A columnar engine answers those in milliseconds — this is the query shape
Parquet/DuckDB exists for.

Two blockers, both hard:

- **cgo.** `go-duckdb` needs it; constraint #1 forbids it in the serving path.
- **Concurrency topology.** DuckDB is single-writer *process* with a file lock — a builder
  holding the file read-write and a server reading it live is not the supported shape.
  Our whole design depends on exactly that (SQLite WAL: one writer, many concurrent
  readers, in different processes).

**Verdict: no for the serving path.** The right answer to the facet cost is the one the
2026-08-09 plan already reaches — scope the query, then materialize `dimension_counts`
(schema v4). Which is, notably, what a table format would have given us for free from
partition summaries.

### 3.4 Postgres — the boring alternative, and the strongest one

The alternative that deserves the most uncomfortable look. Not a "data lake solution", but
it satisfies every constraint above except "no extra service", and it **deletes** a large
share of the code we wrote:

| Machinery we built | Reason it exists | Under Postgres |
|---|---|---|
| Tag-edit unix-socket IPC (`tagipc`, `tag_ipc.py`, ~500 LOC + auth caveat) | SQLite admits one writer, and the writer is the Python process | gone — the Go server just writes the row |
| `<db>.writer.lock` flock, exit-3 second-builder guard | same | gone (advisory lock if wanted) |
| Atomic temp+rename publish, WAL checkpoint gate, sidecar hygiene (423 LOC) | replacing a live SQLite file is a corruption vector | gone — MVCC + a transaction |
| (dev,inode) swap detection + `ReopenIfSwapped` + 30 s tick | consequence of the file swap | gone |
| Snapshot leasing + generation token + `ERROR_STALE_CATALOG` | rowids renumber across rebuilds | mostly gone — no rebuild-by-replacement |
| Catalog-free `Hello` via `CapsSnapshot` | one pinned connection, starved by a slow RPC | gone — a connection pool |
| "catalog volume must be local, never NFS/EFS" | SQLite WAL | gone — builder and server can live on different hosts |

That last row is the one with product consequences: today the builder and the server
**must share a local filesystem** (`docker-compose.aws.yml`, one named volume). You cannot
put ingest on one box and two read replicas on another. The deploy is already
docker-compose, so "one more container" is a smaller price than it looks.

**Verdict: the defensible SQLite counter-argument is deployment shape, not performance** —
a single file you can copy, rsync, inspect with `sqlite3`, and ship inside a static
binary's world with no server to administer. That is a real product property for a
self-hosted connector, and it is why the choice was right for M1. But it is worth writing
down that we paid for it in roughly 1.5k lines of concurrency machinery, and that a
Postgres backend behind the existing store seam is the natural escape hatch if
multi-reader or split-host deployment ever becomes a requirement.

### 3.5 Search engines — Elasticsearch/OpenSearch, and the Quilt pattern

[Quilt](https://github.com/quiltdata/quilt) is the closest *generic* prior art: a
versioned S3 data portal that syncs every object into Elasticsearch and serves faceted,
sub-second search over millions of objects, plus Athena for the SQL side. Its shape —
bucket is truth, index is derived, facets from the index — is exactly ours.

What it does not do: open the files. Quilt indexes object metadata and text/manifest
content; nothing in it knows what an MCAP summary is, so the expensive, domain-specific
half (footer parse, topic sets, message counts, quarantine on a non-round-tripping key)
would still be ours to write. And self-hosting an OpenSearch cluster next to a customer's
lake is far heavier than a 2.5 GB file.

**Verdict: no.** Right pattern, wrong weight class, and it would have replaced only the
cheap half.

### 3.6 Managed object-metadata — S3 Metadata tables, S3 Inventory, Glue crawlers

**[Amazon S3 Metadata](https://aws.amazon.com/s3/features/metadata/)** (GA January 2025)
is the one genuinely missed off-the-shelf component. It maintains, as managed read-only
Iceberg tables, a **journal table** of near-real-time object events and a **live inventory
table** of every object with its current metadata (size, etag, storage class, tags, custom
metadata), queryable from Athena.

That is a drop-in for the *discovery* half of our builder on the AWS leg: the SQS
event-primary path, the `full_reconcile` LIST sweep, and etag change-detection are all
answerable as a SQL query against a table AWS maintains for us. Note in particular that
the authoritative deletion sweep — today a full LIST over 8.78M objects — becomes an
anti-join against the live inventory table.

What it cannot do: anything *inside* the MCAP. Start/end time, topics, schemas, message
counts, chunk counts still require our footer reads.

S3 Inventory (batch, daily, no events) is the weaker predecessor of the same idea. Glue
crawlers infer tabular schemas and are irrelevant here.

**Verdict (revised 2026-08-10, after costing it): no, not now — and cost is not why.**
Priced out: live-inventory backfill of 8.78M objects is **$2.63 one-time**, ongoing
live inventory is **free** below 1 billion objects, and journal updates are **$0.30 per
million** (~$0.27/month at ~30k new files/day). Money does not decide this.

What decides it is that S3 Metadata replaces the **cheap** half of the audit. A full
LIST at 8.78M objects is 8,780 requests — **$0.044 per audit**, minutes of wall time
when sharded. The expensive half is what happens after the listing arrives: the 8.78M-row
`stored` map, the classify loop, and the deletion sweep, all on the single writer thread
with `listings`/`dims_by_key`/`present` resident (§2.3 of the capability review). An
Iceberg result set still has to stream in and be diffed. **The streaming reconcile is the
real fix and is required either way** — which is exactly what the event-discovery design
already says (`InventoryFeed` is *gated on* the streaming rework), and its own trigger
("nightly audit > ~2 h, roughly tens of millions of objects") puts us below the line.

Two things keep it on the list for later:

- **The journal table as a durable, ordered, replayable change log** — not the inventory
  table. SQS is at-least-once with no replay after ack, which is why the full audit must
  stay authoritative for removals. A journal watermark turns "we lost events, enumerate
  everything" into "resume from sequence N": O(changes), not O(objects). Worth a spike
  *after* streaming reconcile.
- **Bulk object user-metadata without a HEAD per object** — a second route to the empty
  `tags_embedded` problem. For a two-cloud product, MCAP metadata records are the better
  channel (cloud-neutral, written by the recorder, survive a bucket copy, and
  `extract_s3_key` already reads one).

The real adoption cost is deployment surface, not dollars: a table bucket, a metadata
configuration, Athena+Glue or an Iceberg client, and IAM for all of it **in every
customer account**, AWS-only, as a permanent second code path. Worth checking whether
`pyiceberg` against the S3 Tables Iceberg REST endpoint removes the Athena/Glue half of
that ask.

### 3.7 Governance catalogs — DataHub, OpenMetadata, Amundsen, Unity, Polaris, Glue

All of these are **dataset-grain** catalogs: they describe *tables and their lineage*, not
8.78M individual files, and they are built for humans browsing a data platform, not for a
UI issuing keyset-paginated queries with a 10 s budget.

**Verdict: no — wrong grain, by a factor of ~10⁶.** They would be the answer to
"where does this dataset come from", never to "which 40 recordings contain `/imu` on
2026-05-19".

### 3.8 The robotics verticals — Foxglove, Roboto, Rerun, ReductStore

This is where "the same result" is actually purchasable, and it deserves an honest look.

- **[Foxglove](https://foxglove.dev/)** indexes recordings by device, time range and topic,
  and its self-managed "primary site" keeps data in your bucket. It is the closest
  commercial match to the *whole* system, streaming half included — its self-hosted API
  even exposes the per-topic file split it creates on import, i.e. it solves topic-subset
  streaming by **rewriting the lake into per-topic files**. That is a design we are
  explicitly not allowed to choose (constraint #4) and did not need: we do it with range
  GETs against the original MCAP.
- **[Roboto](https://www.roboto.ai/)** ingests MCAP/ROS/PX4 as "indexing and statistical
  summarization", exposes topic statistics and metadata through RoboQL and a Python SDK,
  and can fetch slices of topic data. Functionally the nearest thing to our catalog +
  filter + subset-fetch story, with a query language we would not have had to invent.
- **[Rerun](https://rerun.io/)** shipped, in 0.32, an **open-source catalog server that
  indexes recordings in place** (on disk, or in object store for their hosted product),
  with a Lance-backed commercial data platform behind it. "Index in place, query over a
  directory of recordings" is our exact sentence.
- **[ReductStore](https://www.reduct.store/)** is the lightweight self-hosted end: time-
  series blob storage with labels and retention, and no MCAP-semantic indexing.

**Verdict: the capability was buyable; the product was not.** Every one of these brings
its own UI, its own storage opinions, and (for the two mature ones) a per-seat/per-TB SaaS
relationship. What none of them delivers is *this* deliverable: a **PlotJuggler-native
connector** that streams a filtered, stitched, topic-subset session straight into PJ4's
parser pipeline, self-hosted against a customer bucket we do not restructure, with layouts
that re-download their own data. The differentiator is the client integration and the
deployment shape, not the catalog — but the catalog had to exist to make the client
integration possible, and none of these will sell you *just* the catalog.

### 3.9 Lance / LanceDB

Built for multimodal random access at training time — the row is a sample, not a
recording. Rerun uses it underneath for exactly that. Wrong grain for "which files exist";
relevant only if we ever serve ML training reads.

---

## 4. Scorecard

| Option | Footer-grain MCAP facts | Pure-Go CGO-free reader | Self-hosted, BYO bucket | Row-grain upsert + user overrides | Sub-second facets @8.78M | Verdict |
|---|---|---|---|---|---|---|
| **SQLite (as built)** | ✅ ours | ✅ | ✅ | ✅ | ⚠️ 2.9 s → needs materialized counts | **shipped** |
| Iceberg / Delta / Hudi + Trino | ❌ ours anyway | ❌ | ⚠️ + catalog svc | ❌ MERGE + compaction | ⚠️ + network hop | no |
| DuckLake | ❌ ours anyway | ❌ | ✅ | ✅ | ✅ | same idea, later |
| DuckDB engine | ❌ ours anyway | ❌ cgo | ✅ | ⚠️ file-lock topology | ✅ | no (serving path) |
| Postgres | ❌ ours anyway | ✅ | ⚠️ +1 service | ✅ | ⚠️ same fix needed | **credible** |
| OpenSearch / Quilt | ❌ ours anyway | ✅ (HTTP) | ⚠️ heavy | ✅ | ✅ | no |
| S3 Metadata tables | ❌ discovery only | n/a | ⚠️ AWS-only | n/a | n/a | **partial win** |
| DataHub / OpenMetadata | ❌ | n/a | ⚠️ | ❌ grain | ❌ | no |
| Foxglove / Roboto / Rerun | ✅ theirs | n/a | ⚠️ vendor terms | ✅ | ✅ | buyable capability, wrong product |

---

## 5. Verdict and what to do about it

**What was genuinely not purchasable.** The MCAP-summary-grain extractor with its
correctness guards (key round-trip or quarantine, `sum(counts) == message_count`, cache
reload on rollback, targeted read with the streamed path as semantics authority), and the
whole client-facing half. No off-the-shelf data-lake product reads an MCAP footer, and the
two vendors that do sell the capability sell it as their platform, not as a component.
Building the catalog was correct.

**What we reinvented, and should call by its real name.** Snapshot isolation (atomic
publish + generation token ≈ metadata-pointer swap + snapshot id), and columnar
compression (`topic_sets` dedup + varint counts blob ≈ dictionary encoding + RLE over a
repeated column). Both were cheap in our context and both are standard elsewhere; naming
them makes the next reader's job easier and stops us treating them as bespoke.

**What the SQLite choice actually cost.** ~1.5k lines of single-writer concurrency
machinery — IPC socket, flock, publish protocol, swap detection, leases, generation
tokens, catalog-free `Hello` — plus a deployment constraint that pins builder and server
to one host's local filesystem. Right call for M1; worth revisiting the day multi-reader
or split-host deployment is asked for.

**Three concrete follow-ups, ranked by value per unit of work:**

1. **Materialize `dimension_counts` (schema v4).** Already planned. It is the fix for the
   only measured performance problem in the read path, and it is what every table format
   would have handed us from partition summaries. *Cost: small, one cross-repo schema bump.*
2. ~~Cost out S3 Metadata live-inventory tables as the S3 leg's discovery source.~~
   **Done 2026-08-10 — the answer is no, not now** (§3.6 above carries the revised
   verdict). It replaces the $0.044-per-audit LIST and leaves the writer-thread diff
   untouched; the streaming reconcile is the actual fix and is needed regardless. Revisit
   the **journal** table (watermark-resume discovery) *after* streaming reconcile lands,
   or when the nightly audit passes the ~2 h trigger already recorded in the
   event-discovery design.
3. **Keep a Postgres backend on the table behind the store seam** — not to build now, but
   as the documented answer to "can the builder run on a different host than the server"
   and "can we run two read replicas". Today the honest answer is no, and the reason is
   the file, not the schema.

---

## Sources

- [DuckLake — integrated data lake and catalog format](https://ducklake.select/) ·
  [DuckLake v1.0 production-readiness](https://ducklake.select/2026/04/13/ducklake-10/) ·
  [InfoQ: DuckLake 1.0](https://www.infoq.com/news/2026/05/ducklake-sql-catalog/)
- [MotherDuck: the open lakehouse stack — DuckDB and table formats](https://motherduck.com/blog/open-lakehouse-stack-duckdb-table-formats/) ·
  [Definite: DuckLake vs Iceberg, an operator's verdict](https://www.definite.app/blog/duck-lake-vs-iceberg)
- [DuckDB concurrency documentation](https://duckdb.org/docs/current/connect/concurrency)
- [Amazon S3 Metadata](https://aws.amazon.com/s3/features/metadata/) ·
  [S3 Metadata tables overview](https://docs.aws.amazon.com/AmazonS3/latest/userguide/metadata-tables-overview.html) ·
  [S3 Metadata for all objects (AWS News)](https://aws.amazon.com/blogs/aws/amazon-s3-metadata-now-supports-metadata-for-all-your-s3-objects/)
- [Quilt: searchable, versioned S3 data catalog](https://blog.quilt.bio/searchable-versioned-s3-data-catalog-2026) ·
  [quiltdata/quilt](https://github.com/quiltdata/quilt)
- [Foxglove data platform docs](https://docs.foxglove.dev/docs/data) ·
  [Foxglove API (self-hosted site endpoints)](https://docs.foxglove.dev/api)
- [Roboto platform](https://www.roboto.ai/platform/) · [Roboto docs — concepts](https://docs.roboto.ai/learn/concepts.html)
- [Rerun: a new data layer for robot learning](https://rerun.io/blog/data-layer-for-robot-learning) ·
  [LanceDB](https://www.lancedb.com/)
- [ReductStore: comparing data management tools for robotics](https://www.reduct.store/blog/data-management-tools)
- [Iceberg catalogs 2025 landscape (e6data)](https://www.e6data.com/blog/iceberg-catalogs-2025-emerging-catalogs-modern-metadata-management)
