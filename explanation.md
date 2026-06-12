# PJ Cloud Connector — Architecture & Code Walkthrough

A discussion-ready map of the system: the major moving parts, the real files /
classes / functions behind each, and the design decisions a lead engineer is
likely to probe. Written to be readable without reading Go — every component is
described by *what it does and why*, with the exact symbol names so you can point
at them in a conversation.

> One-line mental model: **a streaming service (Netflix) for robot recordings.**
> Recordings (`.mcap` files) live in a cloud bucket; a server streams just the
> slice you ask for to PlotJuggler over one WebSocket. "Streaming" = *progressive
> download*, not wall-clock playback.

---

## 1. The three processes and how they talk

```
   Cloud bucket                 Go server (one static binary)            PlotJuggler 4 + plugin
   (S3 or GCS)                  server/cmd/pj-cloud-server/main.go        (the user)
  ┌────────────┐   ranged      ┌──────────────────────────────────┐    ┌────────────────────────┐
  │  *.mcap     │   GET reads   │ indexer → catalog (table of      │    │ "Dexory Cloud" toolbox  │
  │  files      │◀─────────────▶│   contents, SQLite)              │◀══▶│  panel: browse, filter, │
  │             │   (chunks)    │ session (fetch + stream batches) │ WS │  pick, download         │
  └────────────┘               │ storage / format / authn seams   │ +PB│ dexory-cloud-cli (no Qt)│
                               └──────────────────────────────────┘    └────────────────────────┘
```

- **Bucket** — passive storage. The server only ever *reads* it (ranged GETs).
- **Server** — `server/` (Go, one static binary, no cgo). The librarian in the middle.
- **Plugin** — `PJ4/pj-official-plugins/toolbox_dexory_cloud/` (C++). Lives inside
  PlotJuggler; also ships a Qt-free CLI for headless testing.
- **Link** — a single WebSocket per client carries *both* catalog RPCs and the data
  stream, multiplexed; every message is a Protobuf envelope (the "PB" above).

---

## 2. The wire protocol — the contract (`proto/pj_cloud.proto`)

This is the single source of truth both sides compile against. Go bindings are
**checked in** (`server/internal/wire/pj_cloud/`); C++ bindings are generated at
build time by the Conan `protoc`. Everything is wrapped in two envelopes:

- **`ClientMessage`** / **`ServerMessage`** — each carries a `request_id` + a
  `oneof payload`. This `oneof` is how one socket multiplexes many request types.

The messages, grouped by purpose:

| Purpose | Messages |
|---|---|
| Handshake | `Hello` → `HelloResponse` (carries `Capabilities` + `BackendCapabilities` — the server's filterable key vocabulary + `supports_file_hierarchy`) |
| Browse | `ListFilesRequest` (+ `FileFilter`, `TagPredicate`, cursor) → `ListFilesResponse` (`FileSummary[]`, `Tag[]`, next cursor) |
| Inspect | `GetFileRequest` → `GetFileResponse` (`TopicInfo[]`, `FlatMetadata`) |
| Tag | `UpdateTagsRequest` → `UpdateTagsResponse` |
| Stream | `OpenSessionRequest` (`oneof mode { OpenFresh, OpenResume }`) → `OpenSessionResponse` (`TopicBinding[]`, `SchemaBinding[]`, `estimated_chunk_bytes`, `approximate_messages`, `subscription_id`) |
| Data | `MessageBatch` (carries `MessageBatchBody` = `Message[]`, compressed per `body_encoding`), `Progress`, `Eos` (`EosReason`: COMPLETE / CANCELLED / ERROR / RESUME_NOT_POSSIBLE) |
| Control | `CancelSession`, `SessionAck` (flow-control: lets the server prune delivered batches), `Error` (`ErrorCode`) |
| Encodings | `PayloadEncoding` (per-message), `BodyEncoding` (per-batch: RAW / ZSTD) |

**Discussion points:**
- *Why one socket for everything?* Resume + cancel + progress are naturally tied to
  the same connection lifetime; multiplexing avoids a second control channel.
- *`OpenFresh` vs `OpenResume`* is the heart of reconnect-and-resume (§7C).
- *`SessionAck`* is the backpressure/flow-control knob — the client acks delivered
  batches so the server can free its retain buffer.

---

## 3. The Go server, package by package (`server/internal/…`)

Entry point **`server/cmd/pj-cloud-server/main.go`** wires everything together and
is deliberately *backend-neutral* (it never names S3 or GCS — see the seams, §6).

### 3.1 `config` — how it's configured
`Config` and its nested structs (`ServerConfig`, `StorageConfig`, `IndexerConfig`,
`SessionConfig`, `AuthConfig`, `TLSConfig`, `DashboardConfig`, `MetricsConfig`).
The important one is **`StorageConfig` = a tagged union: `s3` XOR `gcs`** (exactly
one, fail-fast). YAML + env + flags; `Default()` seeds the local Minio dev profile.

### 3.2 `storage` — the **BlobStore** seam (the only place a cloud SDK is imported)
- **`BlobStore` (interface)**: `GetRange(key, off, len)`, `Head(key)`, `List(prefix, token)`.
  That's the *entire* contract the rest of the server sees.
- **`s3.go`** (Amazon/Minio) and **`gcsreader.go`** (Google) implement it; `storage.New`
  dispatches based on the config union. `ObjectInfo` is the `(etag, size, last_modified)`
  identity triple.
- **`retryWith`** wraps every S3/GCS call (50–800 ms backoff, ctx-cancel-aware,
  short-circuits permanent errors via sentinel `ErrTransient`/`ErrPermanent`).
- *Talking point:* **GCS reuses the same ETag triple** — its change-detect identity is
  `Generation` + `Updated` mapped into `etag`/`last_modified`, so the indexer and DB
  schema didn't change at all to add a second cloud.

### 3.3 `format` — the MCAP **codec** + the WAN read path (the perf story)
- **`Codec` (interface)** via `NewCodec("mcap")` — parses a recording's summary and
  reads message chunks. `FileSummary` (`TopicInfo[]`, `TimeWindow`, message counts),
  `FileChunkIndex`/`ChunkRef` (where chunks live + what's in them), `RawMessage`.
- **`summary_reader.go` → `newSummarySource`** — **THE headline optimization.** A
  blob is read over a high-latency link where *each read is a round-trip*. The naive
  reader turned the footer/summary walk into hundreds of tiny ranged GETs (3m24s for
  one file on the real bucket). `summarySource` collapses that into **≤3 GETs**: a
  trailing probe (last 64 KB — usually catches the whole index), one exact read of the
  summary section only if larger, and a 4 KB head prefetch; everything else is served
  from memory. **MessageIndex records are never read** — chunk-topic membership comes
  from summary offsets, counts from the `Statistics` record.
- **`chunks.go`** — decompresses chunk records by hand (zstd via klauspost, **LZ4 via
  the *frame* reader `lz4.NewReader`** — a real bug fix: writers emit LZ4 frames, the
  old code used raw-block decode) and resolves each message's channel via the file's
  global channel/schema table.
- **`indexcache.go` → `ChunkIndexCache`** (`.Get(key, etag, fileID)`) — caches the
  expensive chunk index keyed by `(s3_key | etag)`. The **indexer pre-warms it for
  free** while cataloging (it already read the summary), so by the time you open a
  session the plan is already in memory. An overwritten file (new etag) misses naturally.
- *Talking point:* the trailing probe is **deliberately small** — over-reading transfers
  more bytes and is slower on a bandwidth-bound link than a second small GET. Fewer
  trips ≠ bigger trips.

### 3.4 `indexer` — builds the table of contents
- **`Scanner.RunOnce(ctx)`** — lists the bucket, and for each new/changed object calls
  the **`SummaryExtractor`** (`NewCodecExtractor`, which uses the codec + the chunk-index
  cache) to read its summary, then upserts into the catalog. `RunStats` reports
  `scanned/new/reindexed/unchanged/failed`.
- **`Loop`** runs `RunOnce` on an interval; the **first scan is synchronous** (warm-start)
  so the catalog is populated *before* traffic is accepted. A warm DB ⇒ `unchanged==N`
  (zero re-extracts). `warm_start_timeout` bounds the cold scan (real WAN scans exceed 2m).
- *Talking point:* point the first scan at a **scoped** prefix of a giant data-lake
  bucket (the staging config scopes to one robot/day) or you blow the startup budget.

### 3.5 `catalog` — the local database (SQLite, no cgo)
- **`Store`** over `modernc.org/sqlite` in WAL mode. **Reads** use the connection pool;
  **all writes funnel through a single writer goroutine** (one job = one transaction) —
  eliminates writer contention without app-level locks.
- `FileRecord`, `TopicRecord`, `TagKV`/`EffectiveTag`, `FilterArgs`, `FileSummary`.
- **File ids are SQLite rowids and stay stable across reindexes** (in-place UPSERT keeps
  the id) — the pagination + session-resolve contract depends on this.
- Server-side **`FileFilter`** predicates + **cursor pagination**; a **`tags_effective`**
  view (user tags override derived keys); `UpdateTags`; `DistinctMetadataKeys` feeds the
  capability vocabulary.
- *Talking point:* `Write`-after-`Close` returns `ErrStoreClosed` (idempotent Close, no
  send-on-closed-channel) — a real graceful-shutdown fix.

### 3.6 `session` — fetch + stream + resume (the most intricate package)
- **`BuildPlan(files, indexes, args)`** — validates the request (pairwise **non-overlap**
  of stitched files, inverted-range rejection, **empty-plan contract**), orders files by
  start time, intersects the requested time window with chunks, and returns a **`Plan`**
  with pre-flight `estimated_chunk_bytes` + `approximate_messages`.
- **`Producer`** — fetches chunks (with a bounded, order-preserving **prefetch window**
  to hide GET latency), filters by topic + time, batches messages, and appends to the…
- **`RetainBuffer`** (`Append`) — a bounded ring (256 batches / 64 MB) with **producer
  backpressure**; it's what makes resume possible (recently-sent batches are retained
  until acked).
- **`Consumer.Run`** (`sendBatch`, `sendProgress`, `drainAndEos`) — the per-subscription
  pump that writes batches to the socket, emits `Progress`, prunes on `Ack`, and finishes
  with `Eos`.
- **`Registry`** — owns live sessions; `BindConsumer`, eviction, `CancelAll` for shutdown.
- **`SessionState.RecordAppend` / `CountersThrough(seq)`** — a **per-seq cumulative
  ledger** so that after a resume the `Eos` totals are *exact* (a naive carry
  over-counted replayed in-flight batches — caught by a test).
- *Talking point:* the **producer/consumer split** is the whole reason resume works — the
  producer keeps filling the retain buffer independent of which consumer (original or
  reattached) is draining it.

### 3.7 `ws` — the connection layer
- **`Handler.ServeHTTP`** upgrades the WebSocket; **`connState.dispatch`** is the
  per-connection **read loop** that routes each `ClientMessage` to a handler
  (`handlers_catalog.go` for browse/tag, `handlers_session.go` for streaming).
- **`connState.handleOpenSession`** — **the async-dispatch fix.** Plan-building (which
  does WAN reads) used to run *inline* on the read loop, which (a) blocked other RPCs and
  (b) **starved the keepalive ping/pong so the connection killed itself.** It now runs on a
  guarded background task; the read loop stays free to answer pings and catalog RPCs while
  a plan builds. (`async_open_test.go::TestOpenSession_DoesNotBlockCatalogRPCsOnTheSameConnection`
  is the regression.)
- **`SessionDeps`** bundles the store, codec, blob, registry, config, metrics; **`ConnAPI`**
  abstracts the socket so handlers are testable.

### 3.8 cross-cutting (config-gated OFF by default)
- **`authn`**: `ClientAuthenticator` (interface) — bearer token via
  `crypto/subtle.ConstantTimeCompare`, dev-anonymous when unset.
- **`dashboard`** (Basic-auth HTML, registered only when active), **`metrics`** (`Metrics`,
  Prometheus `/metrics` = 11 `pj_cloud_*` series, always-unauth `/health`).
- TLS (`TLSConfig` → wss/https), panic recovery in 6 scopes, graceful shutdown.

### 3.9 tooling (`server/cmd/`)
`devprobe` (ground-truth assertions, Go client), **`mcapdiff`** (the v1 correctness gate —
*logical* equality, §8), `mcaptopics` / `paginate` (scale tools), `gen-3d-fixture` /
`gen-ci-fixtures` (synthetic CDR fixtures), and `genmcap` (the in-process fixture writer).

---

## 4. The C++ plugin, file by file (`toolbox_dexory_cloud/src/`)

The plugin is a PlotJuggler **Toolbox** (a non-modal panel, modeled on `toolbox_mosaico`),
registered in **`dexory_cloud_toolbox.cpp`**:
`class DexoryCloudToolbox : public PJ::ToolboxPluginBase`, exported with
`PJ_TOOLBOX_PLUGIN(...)` + `PJ_DIALOG_PLUGIN(...)`.

### 4.1 Transport
- **`backend_connection.{hpp,cpp}` → `BackendConnection`** — the WebSocket client over
  **ixwebsocket** (not Qt-WebSockets — a deliberate as-built deviation from the spec; the
  CLI links zero Qt). Owns connect/hello/list/topics/open/download/cancel/tag. Returns
  `ServerVersion`, `BackendCaps`. **A fresh `BackendConnection` is created per download**
  (a cancelled session's stale `Eos` must not poison the next pull — a live-test-caught bug).
- **`wire_mapping.{hpp,cpp}`** — translates between the Protobuf messages and the plugin's
  own value types (`SessionInfo`, `SessionTopic`, `MessageBatch`…). Keeps protobuf out of
  the rest of the plugin.

### 4.2 The worker and the UI
- **`fetch_worker.{hpp,cpp}` → `FetchWorker`** — a background **command-queue worker** (the
  Mosaico pattern): the dialog posts commands (connect, list, pull-topics, fetch); the worker
  runs them off the UI thread and drains results back via an `onTick` event queue. It calls
  `pullTopicsAsync` → `openSessionFresh`/`openSessionResume` → `downloadSession`, then pushes
  decoded messages to the host parsers (via the `ParserIngestDriver`).
- **`dexory_cloud_dialog.{hpp,cpp}` → `DexoryCloudDialog`** + `DialogState` — the panel
  itself, built from **`ui/dexory_cloud_panel.ui`** (compiled to a `constexpr char[]` by
  `pj_embed_ui`; **the `.ui` must stay ASCII** for that hex embed). Rendered by the host's
  **PanelEngine** (a *harvest* model — the plugin reads widget state back, it doesn't hold
  live `QWidget*`). `SequenceRecord`/`SequenceInfo`/`TagRow` are its row model.

### 4.3 Decode + resume + stitch
- **`session_decode.{hpp,cpp}`** — turns the streamed `MessageBatch`es back into
  `DecodedMessage`s (decompress body, split messages); the CLI's `McapWriterSink`
  reconstructs a local `.mcap` from these.
- **`session_key.hpp` → `SessionKey`** — the stable identity of "this exact request"
  (files + topics + time range), used to match a reconnect to its session.
- **`session_cache.hpp` → `CachedSession`** — a COMPLETE-only LRU; a repeat Fetch of the
  same `SessionKey` replays from cache with **zero transport**.
- **`stitch_select.h` → `StitchedSelection`** (the Dexory headline) — N selected sequences
  present as **one synthetic continuous record**: sorted by `(min_ts, name)` so a reordered
  selection produces an identical request; client-side pairwise non-overlap pre-validation
  (server is authoritative); the topics panel shows the *union*.
- **`aggregate_sessions.h`** (`AggInput`/`AggSession`), **`slider_window.h`** (`SliderWindow`
  — note the int64→ns overflow fix lives here), **`date_filter.h`/`name_filter.h`/
  `query_filter.h`** — the filtering/aggregation helpers.

### 4.4 The "ships zero decoders" piece
- **`parser_ingest_driver.{hpp,cpp}` → `ParserIngestDriver`** — the Slice-16 core. The
  plugin **does not decode ROS/CDR**. It binds once per topic and **pushes raw CDR bytes to
  the host's `parser_ros`** through the SDK's parser-ingest tail slots (§5). `IngestBindResult`
  is the per-topic binding handle. (The earlier in-plugin `rosx_introspection` decoder was
  deleted.)

### 4.5 Display + config + query
- **`seq_display.h` → `shortenSequenceName`** — display-only: `customer=dexory/…/leaf.mcap`
  → `dexory/…/leaf.mcap` (strips Hive `k=` prefixes, drops the `date=` segment). The full S3
  key stays the identity (display→key map + collision fallback).
- **`s3_key_fields.h` → `parseS3KeyFields`** — splits a Hive key into the basic-filter fields.
- **`hierarchy_prefix.h`** (the `supports_file_hierarchy` prefix combo),
  **`credential_store.{hpp,cpp}` → `FileCredentialStore`** (api_key in an atomic 0600 JSON
  file, not plaintext settings; libsecret is a drop-in behind the seam),
  **`settings_store.{hpp,cpp}`**, **`server_history.{h,cpp}`**, **`tls_utils.h`**.
- **`src/query/`** (`engine.h`, `ast.h`, `token.h`, `complete.h`, `edit.h`) — the Lua-style
  Advanced metadata query engine, ported from Mosaico.

### 4.6 The CLI
- **`tools/dexory_cloud_cli.cpp`** — `dexory-cloud-cli` (`hello`/`list`/`topics`/`download`/
  `tag`), the *exact* `BackendConnection` the GUI uses, **linked with zero Qt** (an `ldd`
  ctest guard enforces it). This is what `make smoke` drives.

### 4.7 WASM
- **`wasm/`** — a build of the pure client decode core (session_key/cache/stitch/decode +
  a vendored zstd decoder) compiled to ~146 KB wasm and run under node, proving the decode
  path is browser-portable (transport/TLS are explicitly out of scope).

---

## 5. The PJ4 host integration (the part that's *not* just the plugin)

This is the most architecturally interesting host-side work — how a Toolbox plugin reaches
PlotJuggler's full parser + 3D pipeline.

### 5.1 Host-delegated parsing (the ABI tail slots)
- The toolbox runtime host vtable (`PJ_toolbox_runtime_host_vtable_t`) gained two
  **ABI-appendable tail slots**: **`create_parser_ingest`** (offset 24) and
  **`release_parser_ingest`** (offset 32), `static_assert`-pinned and struct-size-gated —
  **no protocol-version bump** (the documented "ABI-v5 growth" mechanism: older hosts simply
  don't have the slots and the plugin degrades cleanly).
- **`ToolboxRuntimeHost`** owns one **`DataSourceRuntimeHost`** parser-ingest context per
  toolbox dataset (`ParserIngestDeps{catalog, registrar}`). `MainWindow` injects the deps
  (the extension catalog + a **GUI-marshalled** SessionManager registrar). The net effect:
  the **entire file-load parser pipeline** (schema classify → ObjectStore registration with
  `builtin_object_type` metadata → render-parser registration) runs **unchanged** on
  toolbox/cloud datasets.
- *Talking point:* this *closed by design* the original "GROUNDED FACT 1" gap (parser
  dispatch lived only on the data-source runtime view, never on toolboxes). It also got us
  `parser_ros`'s *specialized* handlers for free, closing the Imu/PoseStamped flatten-parity
  gap — file-open trees == cloud-fetch trees.

### 5.2 The 3D path (TF + pointclouds) — `pj_scene3D`
- **`TransformService`** (`pj_scene3D/widgets`, a `QObject`) — keeps **one `TransformBuffer`
  per dataset**. `ingestFrameTransformsForDataset(id)` walks the dataset's object topics,
  finds the `/tf` (`kFrameTransforms`) topic, and fills that buffer; it emits the
  **`datasetTransformsReady(id)`** signal when done.
- **`Scene3DDockWidget`** — the 3D dock. `setTransformService` wires it; it binds a
  per-dataset buffer once (`prepareTransformBufferForTopic`) and reads its frame list +
  orphan states from it. **Two fixes from this session live here:** (1) MainWindow now calls
  `ingestFrameTransformsForDataset` for cloud/toolbox datasets (file-open did, cloud didn't);
  (2) `Scene3DDockWidget::onDatasetTransformsReady` now *listens* to that signal and calls
  `view_->refreshAvailableFrames()` + `recomputeOrphanStates()` — because a cloud dataset
  fills the TF buffer *after* the view already bound it empty (the file-open path ingests
  before any view binds; cloud ingests post-download).
- *Talking point worth raising:* a pointcloud only renders if its `frame_id` resolves through
  the TF tree at the tracker time — which is why `/tf` **and** `/tf_static` are both needed,
  and why an *empty* debris-detector cloud (0 points) shows nothing even when everything else
  is correct.

---

## 6. The six seams (the abstraction story)

The design is built around swappable boundaries so the same codebase serves two customers
(Dexory/S3, Asensus/GCS) and stays testable:

| Seam | Interface | Implementations | Why |
|---|---|---|---|
| Storage | `storage.BlobStore` | `s3.go`, `gcsreader.go` | one server, two clouds; only this pkg imports a cloud SDK |
| Format | `format.Codec` | MCAP | isolate the recording format |
| Auth | `authn.ClientAuthenticator` | shared bearer | constant-time token check, dev-anonymous |
| Client sink | C++ `SessionSink` (CLI's `McapWriterSink`) | CLI writer / GUI ingest | decode core is Widgets-free, reusable |
| Parser ingest | SDK tail slots | host `parser_ros` | plugin ships zero decoders |
| Wire | `proto/pj_cloud.proto` | Go (checked-in) + C++ (build-gen) | one contract both sides share |

---

## 7. End-to-end flows (trace these in a discussion)

**A. Browse.** Plugin `Hello` → server returns `Capabilities`. Plugin `ListFilesRequest`
(+ filter) → `connState.dispatch` → catalog handler → `catalog.Store` query (served from
SQLite, *no bucket traffic*) → `ListFilesResponse`. Selecting a row → `GetFileRequest` →
`TopicInfo[]`.

**B. Open + stream.** Plugin builds a `StitchedSelection`, sends `OpenSessionRequest{OpenFresh}`
→ `handleOpenSession` dispatches a background task → `BuildPlan` (using the **pre-warmed**
`ChunkIndexCache`, usually zero new GETs) → `OpenSessionResponse` (bindings + estimates). The
`Producer` fetches chunks (prefetch window) → filters → batches → `RetainBuffer`; the
`Consumer` pumps `MessageBatch` + `Progress` to the socket → `Eos{COMPLETE}`. Plugin
`session_decode` → `ParserIngestDriver` pushes raw CDR to `parser_ros` → plots/3D fill in.

**C. Resume after disconnect.** The socket drops mid-download. The plugin reconnects, looks up
the `SessionKey`, and sends `OpenSessionRequest{OpenResume, resume_after_seq}`. If the session
is still live in the `Registry` and the batch is still in the `RetainBuffer`, a *new* `Consumer`
attaches and replays from `resume_after_seq` (gap/dupe-free; `SessionState.CountersThrough`
keeps the final totals exact). If it was evicted, the server returns `Eos{RESUME_NOT_POSSIBLE}`
verbatim (partial kept; never a silent re-open). 3 attempts, 1s/4s/16s, cancel-interruptible.

**D. 3D render.** Download includes `/tf` + a pointcloud. On `on_data_changed`, MainWindow
ingests TF (`ingestFrameTransformsForDataset`) → `datasetTransformsReady` →
`Scene3DDockWidget::onDatasetTransformsReady` refreshes the frame tree + orphan states. Drag
the cloud into a 3D view; `PointCloudLayer` resolves its `frame_id` through the TF buffer at
the tracker time and draws.

---

## 8. Performance — the WAN story (why waits dropped from minutes to instant)

The enemy is **round-trips**, not bytes (each ranged GET ≈ tens of ms regardless of size).
The wins, in order of impact:

1. **`summarySource`** — file summary in **≤3 GETs** instead of hundreds; MessageIndex never read.
2. **`ChunkIndexCache` pre-warmed by the indexer** — the open-session plan is already in memory
   (≈76 s → instant for a windowed plan).
3. **Out-of-window skip** — files/chunks outside the requested time range are never opened.
4. **Async `handleOpenSession`** — plan-build off the read loop, so keepalive survives (the
   inline grind had let ping-keepalive kill its own connection).
5. **Producer prefetch window** — fetch the next chunk while processing the current one.

Net: open-a-recording minutes → instant; 34-file scan ~3.5 min → ~2 s warm / ~39 s cold;
real downloads ~75k msgs / 24 s. **Caveat:** prep is now instant, but the byte transfer itself
is still bandwidth-bound — a 700 MB download just takes as long as the link allows.

---

## 9. Testing & gates

- **`make smoke`** (`scripts/smoke.sh`, server on `:8081`) — the regression gate: discovery
  counts, plugin `ctest` twice (hermetic + live), round-trips f1–f7 (incl. the parser-ingest
  live leg), warm-start persistence, tag flow. Final line `SMOKE PASS`/`FAIL`.
- **`make matrix`** (`:8082`) — deeper legs m1–m8 incl. the 8-file 337861-message full stitch
  and **m8 = the GCS dual-leg** over fake-gcs.
- **`mcapdiff`** — the v1 correctness contract: **logical, not byte, equality** on
  `(topic, log_time, payload, publish_time, schema name/encoding/data)`. MCAP writers
  legitimately differ on chunking/compression, so byte-equality would be wrong.
- **CI** (`.github/workflows/ci.yml`) — 4 jobs (unit, race, seam-confinement grep, and a
  `backend:[s3,gcs]` integration matrix over real Minio + fake-gcs containers with synthetic
  in-process fixtures — **no `/home/gn/ws/jkk_dataset02` and no protoc in CI**).
- **Ground truth is pinned in four lockstep places** (`smoke.sh`, `matrix.sh`,
  `fake-gcs/seed.sh`, `backend_connection_live_test.cpp`): Σ8 = 337861, zeg_1 = 33670, imu = 14904.

---

## 10. Discussion points — where a lead engineer will dig

- **Host-delegated parsing vs in-plugin decode.** We *reversed* an earlier in-plugin-decode
  amendment once the SDK tail slots existed. The plugin now ships zero decoders; correctness
  depends on the host's `parser_ros` and the GUI-marshalled registrar. (Pin:
  `parser_config_json` must be non-empty or `parser_ros` silently degrades to generic
  scalar-only.)
- **Logical vs byte equality** — a commercial wording item (the SOW said "byte-for-byte").
- **Single-writer SQLite** — simple and contention-free; the trade-off is all writes serialize
  through one goroutine (fine at this scale; worth flagging for future multi-tenant load).
- **Stitching identity** — sorting selected files by `(min_ts, name)` makes the request
  order-independent; the server is the authority on non-overlap.
- **ixwebsocket, not Qt-WebSockets** — the as-built transport; there is no standalone
  `client-core` lib. Don't "restore" the spec's Qt shape without a new decision.
- **Known follow-ups:** CI activation on GitHub (workflows committed, not yet triggered);
  real-bucket M1 client-witnessed run; the 200 MB/s reference-machine bench (today `make bench`
  only asserts a localhost regression floor); qtkeychain vs the file-based credential store;
  doc drift (SDK pin is live `0.7.1` vs `0.6.1` in CLAUDE.md; the plugin README still says
  "decoded in the plugin" — stale since Slice 16).

---

*Authoritative companion docs in this repo:* `CLAUDE.md` (the slice-by-slice narrative),
`2026-05-28-pj-cloud-connector-design.md` (the design spec), `2026-06-03-unified-cloud-connector-plan.md`
(the seams + milestones), and Plans A–D. This file is the orientation map; those are the depth.
