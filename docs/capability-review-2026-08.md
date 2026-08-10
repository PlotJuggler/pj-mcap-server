# Capability review — the system as a data-lake product (2026-08-10)

**Status:** review record, no code change. Companion to
`docs/data-lake-alternatives-review.md` (which asked *should we have built this?*);
this one asks *what does it actually do today, and what would I change?*

Scope: the whole serving stack — Python builder, Go server, wire protocol, and the
shipped C++ client — assessed against the scenario the code is now in: **8.78M files,
74 customers / 162 sites / 275 robots**, multiple users browsing and pulling subsets.

Everything below is cited to code or to a measured number in the plan record. Where I
estimate, I say so.

---

## 1. What is genuinely strong

Not throat-clearing — these set the baseline the gaps are measured against.

**Ingest economics are excellent.** 1.58 range-GETs and 214 KiB per file
(`targeted_summary.py`, measured 2026-07-29), etag-skip before any body read, and a
restart over a cataloged lake that re-reads **zero** files. This is the expensive,
easy-to-get-wrong part of any lake indexer, and it is done well.

**The correctness discipline is unusual and worth keeping.** Dimensions are trusted only
if `rebuild_hive_key(dims) == key` (`builder.py:106`); `sum(counts) != message_count`
rolls the file into quarantine (`builder.py:175`); caches reload from committed state on
rollback (`builder.py:232`); a file is never simultaneously "cataloged" and "failed"
(`_quarantine_existing`); the targeted reader defers to the streamed path as the sole
semantics authority for every quarantine verdict. Most indexers at this scale guess.

**The read path takes generation safety seriously.** Snapshot leasing, an opaque
generation token on `ListFiles`/`GetVocabulary`, and `ERROR_STALE_CATALOG` instead of
silently mis-serving a renumbered rowid. Systems ten times this size get this wrong.

**The wire is well-judged.** Opt-in compressed envelope with deterministic marshaling
(1,111,112 B → 512,385 B on a 32k listing), inner ZSTD batches, resume, cancel,
server-side stitching, and `OpenFresh.include_latched` — replaying latched state that
falls before a time window is a robotics detail almost nobody gets right.

**The client integration is the moat.** Zero decoders shipped, host parser delegation,
and layout-driven re-download. That is the part no vendor sells you.

---

## 2. The gaps, ranked by what they cost in the data-lake scenario

### 2.1 The lake is *inventoried* but not *characterized* — the headline gap

`derive_tags()` returns `[]` (`mcap_summary.py:94-96`). It has always returned `[]`.

Chase the consequences through the schema:

- **`tags_embedded` is empty in every production catalog.** Every tag in the system is
  hand-typed by a human through the tag-edit IPC.
- **`has_error` degenerates.** `is_error_tag` can never fire with no embedded tags, so
  the flag reduces to `message_count == 0` (`builder.py:223-225`). The "list recordings
  that failed validation" use case in `REQUIREMENTS.md` is, in practice, "list empty
  recordings".
- **The tag-facet scan — measured 804 ms, the single costliest leg of `GetVocabulary` —
  scans a table that is empty or near-empty.** We pay the most expensive query in the
  browse path to return almost nothing.
- **`file_metrics` / `file_metric_status` are forward-declared and unwritten** (R11–R13).
  No numeric-threshold queries.

So at 8.78M files the only ways to narrow are customer / site / robot / date / topic —
i.e. *where and when it was recorded*, never *what happened in it*. The questions a data
lake exists to answer — "which runs had a fault", "which have a long teleop
intervention", "which are longer than five minutes", "which have the new sensor
layout" — have no answer here. That is precisely the capability Roboto sells as topic
statistics + RoboQL and Foxglove sells as events.

**The cheap fix nobody has taken:** a meaningful set of tags is derivable from the
summary *we already parsed*, at zero additional I/O — duration bucket, message rate,
topic count, presence of `/tf` / pointcloud / camera topics, a topic-set "layout id",
short/empty/suspicious-gap flags, schema-encoding mix. That populates the facets, gives
`has_error` real meaning, and makes the existing EAV + override machinery earn its
keep — without touching the R2 footer-only invariant.

`file_metrics` (payload-reading) stays a separate, later, opt-in pass. Do not conflate
the two: the free one is worth shipping on its own.

### 2.2 The client uses a quarter of the server's filtering power

The server implements `recorded_between`, `topics_any_of`, `tag_all`, `tag_any` and four
dimension ids (`auryn_read.go:206-281`). The shipped client sends **only** customer, site
and robot (`backend_connection.cpp:528-537`).

Everything else is filtered **client-side over a fully-downloaded listing**:
`date_filter.h`, `name_filter.h`, `query_filter.h`, the Lua engine. At 8.78M files across
275 robots that is a mean of ~32k rows per robot crossing the wire per browse — and
`docs/plans/2026-08-09-…` records a real 43k-row site listing.

The 2026-08-09 change made robot a server-side gate for exactly this reason. The same
argument applies one level further, and time is the *most* natural axis in a recording
lake: "last week's runs from this robot" is the single most common query, and today it
downloads a year of rows and filters them in the GUI.

Two supporting gaps make this worse:

- **No date/time facet in `GetVocabulary`.** `DimRobot` date bounds are explicitly
  deferred (V6, `pj_cloud.proto:193`), so the picker cannot even show "this robot has
  data from March to August" without downloading the listing first.
- **`files.date` is unindexed** (`schema.sql:80-98` indexes time, dimensions, topic set,
  errors — never `date`), even though `date=` is a first-class Hive partition. Time-range
  queries ride `idx_files_time`, so this is latent rather than acute, but a
  `date = ?` predicate is a full scan today.

### 2.3 Audit and ingest scaling

- **`full_reconcile` is Θ(objects) in parent RAM.** It materializes `listings`,
  `stored`, `dims_by_key` and `present` — four whole-bucket structures
  (`reconcile.py:179-307`). The team has measured this ("the known ~2 GB baseline") and
  already specified the fix — feed rows staged into a temp indexed SQLite table with
  presence/diff computed in SQL — but only as a *prerequisite for Phase 7 Inventory*
  (`2026-07-30-builder-event-discovery-design.md:355-365`). I would promote it to work in
  its own right: it is also the most likely contributor to the read path's worst
  observed pathology (`GetVocabulary` at **40,727 ms** while the builder runs), since a
  multi-GB parent plus WAL churn shares one box with the reader.
- **Audit enumeration runs on the writer thread** (acknowledged as a non-goal in the same
  design). At 8.78M objects the classify loop and the `stored` map build occupy the one
  thread that must also service tag IPC and event applies.
- **Cold-build time is ~23 hours at the only rate we have measured** (8.78M ÷ 6,353
  files/min). That number is from a WAN thread prototype and in-region processes should
  beat it — but *nobody has measured a full cold build at production scale*, and it is
  the number that matters most for the next item.
- **A schema bump means a full rebuild, with no migration path.** `open_db` fails fast on
  version mismatch and the documented recovery is "delete the DB"
  (`db.py:104-120`). So shipping `dimension_counts` (the planned v3→v4) currently costs a
  ~23-hour rebuild in production. Atomic publish means the old catalog keeps serving
  throughout, so this is a cost rather than an outage — but it is a cost that will
  discourage exactly the schema evolution the product needs.
- **GCS has no event discovery.** S3 gets SQS-driven near-real-time discovery; GCS is
  rescan-only, so new-file latency equals the rescan interval and every cycle is a full
  LIST. An asymmetry worth stating in the deploy docs even if Pub/Sub parity stays
  deferred.

### 2.4 Multi-user serving

- **One shared bearer token, no identity.** Isolation between the 74 customers in the
  catalog is by *deployment* (one server per customer, `PJ_CLOUD_S3_PREFIX=customer=…`).
  That is a legitimate architecture, but it means the phrase "data lake" cannot yet mean
  one shared deployment, and it should be said plainly in the docs rather than inferred.
- **Tag edits are unattributable.** `tags_override` has `updated_at` but no author
  (`schema.sql:161-167`). In a multi-user lake, shared annotations without provenance
  are the kind of thing people stop trusting after the first disagreement.
- **`MaxConcurrent` is global (16), with no per-connection or per-client cap**
  (`config.go:296`, `registry.go:200`). One client opening sixteen sessions denies
  service to the whole team. There is no rate limiting anywhere.
- **Zero latency instrumentation.** `metrics.go` exports 23 counters and gauges and
  **not one histogram** — no per-RPC duration, no storage-request latency, no error
  counter by `ErrorCode`. The 2.9 s → 40 s `GetVocabulary` regression was found by
  running `mcap-cloud-cli vocab` five times by hand. For a service with a 10 s client
  timeout, that is the single cheapest missing thing.

### 2.5 Smaller, but real

- **Quarantined files are invisible to users.** `catalog_failures` surfaces on the
  dashboard only; nothing in the client says "17 recordings under this robot could not be
  cataloged". At 8.78M files, a 1% failure rate is 88,000 silently missing recordings.
- **No dimension GC** (documented as by-design). A robot renamed repeatedly leaves a row
  in every `GetVocabulary` response forever.
- **`matrix.sh` is dead** (exit 2, pending migration). The real-corpus performance gate
  does not exist, so performance regressions are caught by hand — which is precisely how
  §2.4's last item was found.
- **No server-side chunk-data cache** (only chunk *indexes*, `indexcache.go`). Popular
  files re-fetch per user. Probably the right call in-region; worth a measurement rather
  than an assumption if egress ever shows up on a bill.

---

## 3. What I would do, in order

**1. One schema bump that pays for itself: derived tags + `dimension_counts`.**
Both are schema v3→v4, and a bump costs a full rebuild — so spend it once. Derived tags
come free from the summary already in hand (§2.1); `dimension_counts` fixes the only
measured read-path problem. Ship them together, and add a **backfill/migration path** in
the same PR so the *next* schema change is an `ALTER` plus a targeted pass rather than
another 23-hour rebuild. This is the highest-value item on the list by a wide margin: it
turns the catalog from an inventory into something you can ask questions of.

**2. Push time filtering to the server.** Send `recorded_between` from the client's date
picker (server side already implemented and indexed), and add per-robot date bounds to
`GetVocabulary` (the deferred V6) so the picker can bound itself before the first
listing. Follow with topic filtering in the gate. This is a small client change against
capability that already exists — the cheapest large win in the browse path.

**3. Streaming reconcile.** Stage the feed into a temp indexed table; compute skip and
presence as SQL joins. Already specified, already justified, and it unblocks S3 Inventory
while removing the periodic multi-GB parent.

**4. Instrument before optimizing anything else.** Per-RPC duration histograms, storage
request latency, error counters by `ErrorCode`. A day of work that would have caught the
vocabulary regression before a user did, and that is a precondition for making any honest
claim about §2.3's unmeasured numbers.

**5. Multi-user hygiene, when a second team touches one deployment.** Per-connection
session quota, an author column on `tags_override`, and a documented statement that
tenant isolation is by deployment. None of these are urgent while the shape is one server
per customer — all of them are cheap now and awkward later.

**Deliberately not on this list:** the Postgres/DuckDB question (settled in the
companion doc — no, with named trip-wires), `file_metrics` payload extraction (real value,
but sequence it *after* free derived tags prove the facet path), and GCS event parity
(correct to defer while production is S3, worth documenting as an asymmetry).

---

## 4. The one-sentence version

The hard, unglamorous half — cheap correct ingest, safe concurrent serving, a
well-judged wire, a real client integration — is built to a standard well above average;
what is missing is the half that makes a lake *searchable* rather than merely *listed*,
and the instrumentation to know when either half is degrading.
