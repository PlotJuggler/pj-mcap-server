# Canonical Cloud Layouts — Replay via Materializable Cached Files (design v3.1)

**Status:** DRAFT v3.1 — Codex adversarial pass complete (21 findings), all 13 mandatory
amendments incorporated below. Awaiting user approval before the implementation plan.
**Date:** 2026-07-27
**Repos touched:** this repo (plugin `toolbox_mcap_cloud`, docs) + PJ4 (host + SDK, upstreamed
like prior cloud hooks) + `pj-official-plugins` (`data_load_mcap` version pinned in the matrix).
**Supersedes:** design v1 (virtual-file `FileSourceBase` — reviewed **no-go**, 12 findings) and
design v2 (parallel `<replay_sources>` rail — superseded once PJ4 #470/#464 were factored in).

## 1. Goal

One `.pj4.xml` that, when loaded (GUI **File → Load Layout**, or `plotjuggler --layout` with zero
prompts), automatically reconnects to an MCAP Cloud server and re-downloads an exact session —
specific MCAP files (s3 keys), a specific time window, an optional topic subset — fully automating
what the user does manually in the plugin dialog. Locked requirements:

- **Single file.** Everything embeds in the one `.pj4.xml` (no sidecar descriptor).
- **Three usage modes:** interactive GUI reload (a confirm prompt is acceptable), scripted
  `--layout` startup with zero interaction, team-shared layouts (no secrets embedded,
  credentials resolved per machine).
- **PJ4 host changes are allowed** and get upstreamed.

## 2. Core idea

> A cloud session doesn't need to *pretend* to be a file (v1's fatal mistake) — we **make it
> one**, and we make the authored dataset and the replayed dataset **the same kind of thing**.

Every fetch materializes into a **request-addressed** local cache file:

```
$XDG_CACHE_HOME/mcap_cloud/sessions/<descriptor-digest>.mcap
```

and — the load-bearing amendment from the v3 review — **on successful completion the eager
dataset is transactionally replaced by a stock `FileLoader` load of that cache file**
(#464 `replace_dataset_id` refill: stable `DatasetId`/`TopicId`s, curve keys survive, rollback on
failure). From that moment the dataset *is* an ordinary `data_load_mcap`-loaded file: real loader
record captured by the stock path (`FileLoader` `saveConfig()` capture + source-path install +
`onFileLoaded` `LoadedSource` recording), lazy cold-chunk storage, and byte-identical semantics
between authoring and replay. The layout then records a **normal `<fileInfo>`** plus one new
provider-generic child:

```xml
<fileInfo filename=".../mcap_cloud/sessions/ab12....mcap" prefix="...">
  <dataset source_index="0" display_offset_ns="..." timeline_order="..."/>
  <plugin ID="MCAP Loader"><![CDATA[ { "filepath": "...", "use_log_time": true, ... } ]]></plugin>
  <materialize provider="mcap-cloud"
               identity="mcap-cloud:v1:sha256/128:ab12...">
    <![CDATA[ { canonical descriptor JSON } ]]>
  </materialize>
</fileInfo>
```

(The `<plugin>` ID above is whatever the stock capture emits — today `FileLoader` writes the
loader's display name and matches `expected_plugin_id` against `candidate->name`
(`FileLoader.cpp:1135`, `:2154`); the loader's manifest id is `mcap-loader`. The replay series
moves this to stable manifest-ID semantics with a name-based compat fallback for existing
layouts. The provider id is the toolbox's stable manifest id `mcap-cloud`.)

On layout load:

- **Cache hit** (file present, structurally valid) → the stock reload path runs. Zero new code
  on the load itself, **zero network**. Works even if the provider plugin is absent (§6.4).
- **Cache miss** → identity-first resolution → trust gate → provider
  `materialize(descriptor)` (download-to-file only, no ingest) → stock `FileLoader` load.

The `<materialize>` element is a **provider-generic source record** — "how this dataset came to
be and how to re-obtain it" — not a cloud one-off:

| provider     | descriptor                                   |
|--------------|----------------------------------------------|
| `file`       | implicit — today's `<fileInfo>` unchanged    |
| `mcap-cloud` | the canonical session tuple (§4)             |
| *(future)*   | mosaico, live captures, databases, ...       |

Persistence formats outlive code: the schema is where the unified model lives; host
orchestration can converge later (§11).

## 3. Dependencies and lineage

- **PJ4 #470** `feat/toolbox-ingest-progress` (OPEN, assumed merging; reviewed at branch tip
  `f4bd56df`): host-side progressive toolbox imports — per-dataset lifecycle pairing, throttled
  progressive publish, cooperative stop, title-bar strip; zero SDK/ABI change (lights up the
  progress surface on the `create_parser_ingest` fat pointer,
  `data_source_host_views.hpp:144-167`). Our plugin adopts this surface and is its live
  verification vehicle. **Scope limit found in review:** its `on_ingest_finished(DatasetId)` is a
  lifecycle-pairing signal with *no outcome* — it fires on normal finish, on release-without-
  finish, and on host teardown (`ToolboxRuntimeHost.cpp:46-69, 243-261, 279-331` on the branch).
  It is therefore **not** used as the success boundary; the adopt step (§6.1) is. The
  lifecycle-tracking hoist from `MainWindow` into `SessionManager`/`AppSession` remains a
  follow-up in our series
  ([recorded on #470](https://github.com/PlotJuggler/PJ4/pull/470#issuecomment-5093599773)).
- **PJ4 #464** (MERGED): transactional per-dataset Reload/Replace. An explicit
  `replace_dataset_id` target wins regardless of path (`FileLoader.cpp:1184`), the new source
  path commits only after success (`FileLoader.cpp:2132`), rollback leaves data/name/path
  untouched. This is the engine of both the authoring adopt step and replay-into-loaded-dataset.
  `FileLoader::sameSourceIdentity` stays **string-typed and untouched** — provider identity is
  resolved against the host's source-record registry to a `DatasetId` first, then passed via
  `replace_dataset_id` (review finding: do not overload the two-string comparison).
- **Design lineage:** v1 hid the session behind a virtual `FileSourceBase` (no-go: plugin-family
  exclusivity, non-URI path layer, blocking loads, credential confusion, 32-bit identity). v2
  built a parallel `<replay_sources>` rail + `ReplayManager`; superseded — #470/#464 supply the
  lifecycle/progress/stop/refill machinery, and the cache-file unification ("both are sources of
  data; lazy vs eager storage is the only real difference — and we can save the download and go
  lazy too") collapses the parallel rail into the file rail. v2's descriptor/identity/trust work
  carries forward intact. v3.0 → v3.1: thirteen mandatory amendments from the second Codex pass
  (§15).

## 4. The descriptor — public, allowlisted, versioned

```json
{ "v": 1, "kind": "mcap-cloud-session",
  "server_uri": "wss://mcap.example.com",
  "s3_keys": ["cust/site/robot/2026-07-12/a.mcap", "..."],
  "topics": [],
  "start_ns": "1780012345000000000",
  "end_ns":   "1780012399000000000",
  "include_latched": true,
  "display_name": "Run 42" }
```

- `topics` is **exactly the wire request** sent to `OpenFresh` (Custom mode includes the forced
  `/tf`, `/tf_static` appendage; empty = all). Timestamps are absolute epoch nanoseconds as
  **decimal strings**; `"0"/"0"` = whole range. `include_latched` explicit.
- **Never contains:** bearer token, cert path, `allow_insecure`, query history. Validation
  **rejects** URI userinfo, query strings, and fragments outright.
- **Identity** = `mcap-cloud:v1:sha256/128:<hex>` over a canonical serialization (sorted keys,
  string-encoded integers, UTF-8, no insignificant whitespace). `display_name` is excluded from
  the digest **and from identity comparison** (a rename is not a collision). New module; the
  process-local FNV `SessionKey` (`src/session_key.hpp`) is untouched and never crosses a file
  boundary. Canonicalization **test vectors are shared verbatim between both repos**
  (`CATALOG_CONTRACT.md` discipline). Unknown `v`/`kind` → provider refuses with a diagnostic.
- **Size/complexity limits** are part of validation (§7): maximum descriptor/CDATA size, key and
  topic counts, string lengths.
- **Exact-replay caveat (explicit):** the digest names the *request*, not the content. Exact
  byte replay is guaranteed only under the deployment prerequisite that bucket objects are
  immutable (our stated convention). Embedding per-key server object versions/ETags in the
  descriptor is specified as the v2-of-descriptor upgrade path; until then the spec claims
  request-replay, not content-replay.

Dedup and cache lookups compare digest as a fast path and confirm with a full
canonical-descriptor comparison (excluding `display_name`) before treating two sessions as
identical.

## 5. The cache — request-addressed, leased, durable

- Location: `$XDG_CACHE_HOME/mcap_cloud/sessions/<digest>.mcap` (override
  `MCAP_CLOUD_CACHE_DIR`). Owned by the plugin; the host never enumerates it. Directory and
  files are private (0700/0600), created with symlink-safe exclusive operations.
- **Embedded provenance:** the writer embeds the canonical descriptor as an MCAP Metadata
  record, so a cache file self-describes which request produced it — digest collisions and
  wrong-file substitution are detectable from the file alone.
- **Atomic, validated finalization:** write to a **per-process unique** partial
  (`<digest>.mcap.partial.<pid>`), then: close the writer → reopen and validate (footer, summary,
  Statistics, embedded descriptor, expected message/channel counts) → `fsync` file → atomic
  rename → `fsync` directory (where supported). The vendored `FileWriter` ignores short-write /
  flush / close failures outside debug asserts (`writer.inl:40`) — the validate-reopen step is
  therefore mandatory, not optional. Cancellation or any failure deletes the partial and commits
  nothing.
- **Cross-platform locking (shared/exclusive), not bare `flock`:** materialization, eviction,
  and startup cleanup take the digest's **exclusive** lease (non-blocking for cleanup, plus an
  age threshold so process B never deletes process A's live partial). Every **live cache-backed
  dataset holds a shared lease** for its whole lifetime — mandatory because `data_load_mcap`'s
  cold-chunk store re-opens the file lazily on first cold miss (`message_byte_store.hpp:75,141`;
  fetchers outlive the handle, `mcap_source.cpp:431`), so Linux unlink-while-open does **not**
  protect a loaded dataset. Leases are released on dataset deletion / session teardown.
- **Eviction:** size-capped LRU (configurable; suggested 20 GB), skips leased files, takes the
  exclusive lease per victim, uses its own touch-file timestamps (atime is unreliable under
  `relatime`/`noatime`; hits explicitly touch). Free-space reserve enforced before a
  materialization starts. Users may wipe the cache **only when no cache-backed datasets are
  loaded** — the spec documents this restriction (every file is re-materializable, but not while
  live).
- **Corruption policy (narrow):** provider `query` performs a cheap structural + identity check
  (footer magic, summary present, embedded descriptor matches). Only a *structural/identity*
  failure turns the entry into a miss (delete + one re-materialization, tracked per job); a
  parser/config failure during the subsequent load surfaces as a load error **without** deleting
  the file.

## 6. Flows

### 6.1 Authoring (interactive Fetch — dual-path with completion-time adoption)

```
user: browse -> gate -> select files/window/topics -> Fetch
plugin: FetchWorker downloads
        ├─ eager streaming ingest (existing path, adopting the #470 progress surface)
        └─ tee: raw wire-decoded records -> cache partial (bounded async queue, §6.5)
on complete (plugin-decided success: EOS reached, zero losses, writer finalized+validated):
        adopt_materialized_source(dataset_id, cache_path, descriptor)   [runtime-host tail slot]
host:   queues a STOCK FileLoader load of cache_path with
          replace_dataset_id = dataset_id, expected loader = MCAP loader,
          preset config { filepath: cache_path, use_log_time: true, ... }, skip_dialog
        -> transactional refill swaps eager data for the lazy file-backed dataset
           (stable ids; rollback on failure keeps the eager dataset + no record)
        on fileLoaded: stock capture creates the real LoadedSource (loader id + accepted
           saveConfig); host attaches {provider, descriptor, identity} to it, atomically with
           the catalog notification
user: arranges plots -> File -> Save Layout -> <fileInfo> + <materialize> emitted
```

Why adoption instead of a bare `commit_source_record`: the review showed a two-argument commit
cannot mint what layout save requires — a `LoadedSource` only exists via the stock capture chain
(`FileLoader.cpp:2154` config capture, `:2162` source-path install, `MainWindow.cpp:2665`
recording), and the eager parser semantics differ from the loader's (log-time vs publish-time
default, `"{}"` parser config vs the loader's array-limit/timestamp/schema settings) — adoption
makes the saved dataset *identical* to what replay will produce, closing both gaps in one move.

Plot-while-downloading is preserved during the transfer (#470 surface); adoption happens once at
the end. Partial/cancelled fetches adopt nothing and delete their partial. **In-memory
cache-hit** fetches whose disk file is missing cannot "re-tee from memory" (the in-memory
`SessionCache` stores counts, not bytes — `session_cache.hpp:53`): the in-memory entry is
evicted and a normal network fetch runs. A cache hit whose disk file exists re-adopts it.

### 6.2 Replay (Load Layout / `--layout`) — rewrite first, then classify

A small host coordinator — **`LayoutImportBatch`** — owns the restore transaction. It exists
because a cache miss materializes *before* `FileLoader` is busy, and today's state machine has
nowhere to retain the layout across that gap (`MainWindow.cpp:4292-4376`: classification →
`isBusy()` branch → catalog-empty abort at `:5127`; progressive completion is bound to
`FileLoader::queueDrained` at `:5219`). The batch owns: the working layout document + prior
workspace (rollback), provider jobs and the subsequent load requests, cancellation/shutdown,
per-job outcomes, one final drain signal, and the **interactive vs non-interactive policy**
(persisted through async completion — `startup_auto_reload_` resets when `loadLayoutFromPath`
returns, `MainWindow.cpp:5059`, so it cannot govern late provider jobs; and today's drain calls
`promptMissingCurves()` unconditionally, `MainWindow.cpp:5485`).

Ordering per `<fileInfo>` with a `<materialize>` child (review Blocker 3 — resolution must
rewrite the *document*, not just pick a path):

```
1. parse layout; validate descriptor (schema, limits, no smuggled credentials)
2. provider query (strictly bounded, §6.3): trust class + local cache path | miss
3. resolve the effective local path (hit: cached path; miss: the path materialize will produce)
4. rewrite the working <fileInfo filename>
5. remap every matching *_dataset_path qualifier (existing remapDatasetSourcePaths pass,
   LayoutXml.cpp:546)
6. rewrite the loader preset filepath (FileLoader rewrite_preset_filepath hint)
7. classify: already-loaded (registry match by identity -> #464 replace_dataset_id if a refresh
   is wanted, else skip) / cache hit -> stock load / miss -> trust-gated materialize job
   chained into the stock load
```

Widgets restore immediately; curves bind progressively via the stock busy-restore path once the
batch has loads in flight. Failures are per-job (§10). Materialize jobs are sequential in v1 of
the feature.

### 6.3 Provider query — strict contract

Runs synchronously on the GUI thread (the settings backend owns one unsynchronized `QSettings`,
`QSettingsBackend.h:16`, so this is also the *safe* thread). Hard rules: descriptor
parse/validation, in-memory trust lookup, cheap file stat + structural/identity check — **no
network, no lock waits, no full-file scan, no credential resolution** (a cache-hit answer must
not touch credentials at all). One provider instance is bound once per batch and queried for all
descriptors. The async `materialize` ABI specifies: exactly-once completion, callback thread,
descriptor/callback lifetimes, provider DSO lifetime, cancel-and-join semantics, the
cancellation-vs-successful-rename race, and shutdown ordering (batch jobs are owned by the batch
— #470's shutdown only joins `FileLoader` and panel ingest hosts).

**Provider bind mode:** binding the provider for query/materialize **must not** run the
interactive dialog initialization — today `setSettings()` triggers `initFromSettings()` which
auto-connects to the most recent server (`mcap_cloud_dialog.cpp:553, 567`). The plugin adds a
headless provider entry that performs no auto-connect and touches the network only inside an
authorized `materialize`.

### 6.4 Old-PJ4 degradation and provider-absent fallback

- **Same machine, cache intact:** works on today's unmodified PJ4 — real path, real file, stock
  loader; the unknown `<materialize>` sibling is ignored by the existing reader (verified:
  `extractDataSource` walks `<dataset>` children and selects `<plugin>` by name,
  `LayoutXml.cpp:77`; out-of-tree cache paths stay absolute, `MainWindow.cpp:229, 6378`). This
  is conditional on the adoption step having produced a real loader record (§6.1) — which it
  does by construction.
- **Provider plugin absent on a new host:** if the hinted file exists and validates, the stock
  load proceeds anyway (don't regress the strongest degraded mode); the provider is required
  only for cross-machine resolution and materialization. Full provenance checking is
  unavailable without the provider — inherent to graceful degradation.
- **Other machine / purged cache, old PJ4:** path doesn't exist → existing "data source
  missing" handling (skip + diagnostic; existing abort if nothing at all loads). No crash, no
  schema break; `pj4_version` stays 4.
- **WASM:** native-only. The provider/cache code is compile-guarded out, and the WASM layout
  path **explicitly detects** `<materialize>` records and emits an "unsupported in browser"
  diagnostic (the old reader's silent ignore is not relied on).

## 7. Security model

A layout is untrusted input: XML anyone can send you that names a server and asks PJ4 to connect
with your credentials. Guards, all independent:

1. **Trust gate** (host asks, provider answers — before any network touch). *Trusted* = the
   normalized origin exists in a dedicated **trusted-origin ledger written only after a
   successful interactive Hello** (not the credential store — credentials can be saved before
   any successful connection, `mcap_cloud_dialog.cpp:1808` vs `:2536`). GUI: untrusted origins
   require explicit confirmation in the consolidated reload prompt. `--layout`: untrusted → job
   fails with a diagnostic. Zero prompts means zero prompts *for already-trusted origins*.
2. **Credential origin binding** (plugin). `MCAP_CLOUD_API_KEY` is honored only when
   `MCAP_CLOUD_URL` is set and its **parsed origin** equals the target's — both parsed as URLs
   (scheme + IDNA host + effective port; userinfo/query/fragment rejected), not compared via
   `normalizeServerKey()` string munging (`server_history.cpp:51` is not an origin parser).
   Absent or mismatched → the environment token is ignored and only the target origin's stored
   credential is used. The layout never carries token / cert path / `allow_insecure`.
3. **Resource limits** (trust is not enough — a trusted-origin layout can still be a
   resource-exhaustion vector): per-machine caps on descriptor size, key/topic counts, estimated
   and actual download bytes, duration, plus a free-disk reserve. GUI confirmation displays the
   server's size estimate; non-interactive mode refuses above configured limits with a
   diagnostic.

Residual, documented: secret-free layouts still leak customer/site/robot metadata via keys,
hostnames, topics, time ranges — and the absolute cache path leaks the author's username/cache
root. Inherent to shareable layouts; a sharing-policy note, not a mechanism fix.

## 8. Host + SDK changes (PJ4)

SDK / ABI (all `struct_size`-gated tail additions, no protocol bump):

- Capability flag `PJ_TOOLBOX_CAPABILITY_SOURCE_PROVIDER`.
- Toolbox plugin vtable tail: `source_provider_query(descriptor) -> {trust, cached_path?}`
  (strict §6.3 contract) and `source_provider_materialize(descriptor, callbacks) -> job`
  (async download-to-file; fully specified lifetime/threading ABI).
- Toolbox runtime host vtable tail: `adopt_materialized_source(dataset_id, cache_path,
  descriptor_json)` — GUI-marshalled; host re-checks dataset existence/generation, then drives
  the stock `FileLoader` replace + descriptor attach as one ordered transaction whose result is
  visible before any Save Layout can observe completion. (Replaces v3.0's underspecified
  `commit_source_record`.)

Host:

- **`LayoutImportBatch`** (§6.2) — the one new coordinator; deliberately small (it owns the
  restore transaction, not progress/publish/stop, which are #470's).
- `SessionManager`: source records ({provider, descriptor, identity}) attached to
  `LoadedSource`/dataset; invalidated on dataset delete; updated on merge (WASM-ledger
  discipline); registry queryable by identity → `DatasetId`. Includes the lifecycle-tracking
  hoist promised on #470.
- Layout save: emit `<materialize>` for sources carrying records. Layout load: the §6.2
  rewrite-then-classify ordering.
- `FileLoader`: stable manifest-ID matching for `expected_plugin_id` with a display-name compat
  fallback (existing name-vs-id mismatch, `FileLoader.cpp:1135` vs captured name `:2154`).
  `sameSourceIdentity` **unchanged**.

Explicitly **not** built (v2 components dissolved or #470-supplied): `<replay_sources>`,
`ReplayRecord` as a separate registry concept, the `ReplayManager` batch coordinator (shrunk to
`LayoutImportBatch`), the progressive-restore generalization, and any second plugin family.

## 9. Plugin changes (this repo)

1. **Descriptor module** — schema, validation + limits, canonical serialization, sha256/128
   digest; shared vectors. *(Independently shippable.)*
2. **Cancel hardening** — cancellation joins `sendAndWait`'s wake predicate (cancel during
   OpenSession can block ~120 s today); cooperative stop (`is_stop_requested`) checked in the
   pull loop. *(Independently shippable.)*
3. **Credential origin binding** with a real URL/origin parser (§7.2). *(Independently
   shippable.)*
4. **Trusted-origin ledger** — written on successful interactive Hello only. *(Independently
   shippable.)*
5. **Cache manager** — request-addressed store, embedded-descriptor provenance,
   validate-then-rename finalization, cross-platform shared/exclusive leases, per-process
   partials, LRU + touch-files + free-space reserve, orphan cleanup with lock + age threshold.
6. **Fetch tee** — bounded async queue with owned payload copies off the ingest hot path;
   explicit backpressure when full; writer failure cancels the fetch and prevents adoption.
   (`SessionCache` stores `DatasetId`; missing-disk-file hits evict and refetch — §6.1.)
7. **#470 progress-surface adoption** in `ParserIngestDriver`/`FetchWorker` — also #470's live
   verification.
8. **Provider implementation** — headless bind mode (no auto-connect), `source_provider_query`,
   `source_provider_materialize` (the CLI's `downloadToMcap` writer refactored into a shared
   sink TU — it currently lives in the CLI target only, `CMakeLists.txt:184`, and must avoid a
   duplicate `MCAP_IMPLEMENTATION` definition), `adopt_materialized_source` calls at fetch
   completion.

## 10. Semantics, failure behavior

- **Reuse policy:** identity = "same request" (§4 caveat on content vs request). Cache file
  present + valid → no network. Dataset already loaded with a matching source record → skip (or
  #464 refresh on explicit user request). Forcing re-fetch = delete the dataset / cache entry
  (subject to leases).
- **Per-job failure, never destructive:** a failed query/materialize/load leaves other jobs
  running; its curves stay unbound and are **not removed** in non-interactive mode (diagnostics
  only — the batch suppresses `promptMissingCurves` and the dialog fallback); GUI mode gets one
  end-of-batch summary with retry/remove. Errors carry actionable text ("no credentials for
  wss://… — connect once in the MCAP Cloud toolbox or set MCAP_CLOUD_API_KEY + MCAP_CLOUD_URL").
- **Cancel/exit:** materialize honors cancel with bounded waits; partials never survive; batch
  jobs are owned and joined by the batch at shutdown.
- **Adoption failure** (stock load of the cache file fails after a successful download): the
  transactional refill rolls back — the eager dataset stays, no source record is attached, the
  layout simply won't carry a replay record for it; diagnostic emitted.

## 11. Considered and rejected

- **v1 virtual `FileSourceBase`** — no-go per first adversarial review.
- **v2 parallel replay rail** — superseded; #470/#464 + cache-file unification (§3).
- **Unified ImportJob host refactor** — deferred by rule-of-three; the provider-generic *schema*
  is the part of the unified model that must exist now, because persistence outlives code.
  `LayoutImportBatch` is deliberately the smallest coordinator that closes the async gap, not a
  general import pipeline.
- **File-routed interactive Fetch** (download, then load — single path, no eager ingest) —
  rejected: discards plot-while-downloading, the exact UX #470 exists to provide. The adoption
  step gets the same end state without the UX loss.
- **Piggybacking commit on #470's `on_ingest_finished`** — rejected by review: that callback has
  no success outcome (fires on abandon/teardown too). The plugin decides success; adoption is
  the terminal transaction.
- **Extending `sameSourceIdentity` with descriptor semantics** — rejected by review; identity
  resolves through the source-record registry to a `DatasetId`.
- **Content-addressed cache via server ETags** — not rejected, staged: descriptor v2 upgrade
  path (§4) once exact content replay is required.
- **Sidecar descriptor file / server-side layouts** — out of scope (single-file decision);
  server-hosted layouts remain a possible later milestone over the same descriptor.

## 12. Testing

- **Both repos:** shared canonical-serialization + digest vectors, byte-identical.
- **Plugin:** descriptor validation matrix (incl. rejected userinfo/query/fragment URIs, limit
  enforcement); cache manager (validated finalization incl. injected short-write, lock
  contention across two processes, eviction-vs-lease, orphan cleanup age threshold,
  corrupt-file-as-miss narrowness); tee (bounded-queue backpressure — **measured**, not assumed;
  writer-failure cancels fetch); round-trip: cache file `mcapdiff`-equal to a direct CLI
  download of the same tuple; provider query (no-network assertion) and materialize hermetic
  tests + live gtests against the smoke server; `sendAndWait` cancel regression. `make smoke`
  green at every stage.
- **PJ4:** `layout_xml` round-trip for `<materialize>` (incl. old-reader ignore + WASM
  diagnostic paths); rewrite-then-classify ordering unit tests (steps 1–7, §6.2);
  `LayoutImportBatch` tests (per-job failure isolation, non-interactive policy persistence,
  cancellation, drain); a **fake source-provider test plugin** driving load-time integration
  incl. non-interactive `--layout`; adoption tests (eager→file refill keeps ids; rollback on
  failed adoption; **catalog equality** between an authored-and-adopted dataset and a
  replay-loaded one — semantic equality, not just `mcapdiff`); stable-ID + name-fallback loader
  matching.
- **Matrix pin:** `data_load_mcap` (and parser set) version pinned in the E2E matrix — replay
  has a runtime dependency on a compatible loader.

## 13. Build order (cross-repo hazards resolved)

1. **Plugin foundations** *(no host dependency, each shippable)* — descriptor module + vectors;
   cancel predicate; credential origin binding + trusted-origin ledger; cache manager.
2. **#470 lands** (merge/rebase); plugin adopts the progress surface (its live verification).
3. **SDK ABI + host** — capability + three tail slots + fake-provider host tests;
   `LayoutImportBatch`; source records + registry (incl. lifecycle hoist); layout
   rewrite-then-classify; stable-ID loader matching. **Publish the new SDK package and bump the
   plugin's SDK pin** (currently `plugin/SDK_VERSION` = 0.11.0 vs PJ4's SDK recipe 0.19.0 —
   the provider implementation cannot compile before this step).
4. **Plugin dual-path + provider** — shared writer-sink TU refactor; fetch tee;
   `SessionCache` DatasetId; headless bind mode; query/materialize/adopt implementations.
5. **End-to-end + docs** — GUI flow, `--layout` flow, team-sharing runbook (incl. metadata/path
   leakage note), pinned-loader matrix, live coverage.

## 14. References

- PJ4 #470 (OPEN, reviewed at `f4bd56df`), #464 (MERGED), #453 (async load pipeline, MERGED).
- Plugin anchors: `src/session_key.hpp` (process-local, unchanged), `src/fetch_worker.cpp`,
  `src/backend_connection.{hpp,cpp}` (blocking waits), `tools/session_download.cpp`
  (`downloadToMcap` writer), `src/session_cache.hpp`, `src/credential_store.hpp`,
  `src/server_history.cpp` (`normalizeServerKey` — not an origin parser), `manifest.json`
  (stable id `mcap-cloud`).
- Host anchors: `pj_app/src/MainWindow.cpp` (`:4292-4376` classification, `:5127` abort,
  `:5219` drain binding, `:5485` unconditional prompt, `:5059` startup flag reset, `:2665`
  LoadedSource recording, `:6328` save filter), `pj_app/src/LayoutXml.cpp` (`:77`
  extractDataSource, `:546` remapDatasetSourcePaths), `pj_app/src/FileLoader.cpp` (`:958`
  sameSourceIdentity, `:1135` name-vs-id, `:1184` replace target, `:1318` skip_dialog/rewrite,
  `:2132` path commit, `:2154` config capture), `pj_runtime/ToolboxRuntimeHost.*` (#470 branch),
  `pj_runtime/include/pj_runtime/QSettingsBackend.h:16`.
- `data_load_mcap` anchors: `mcap_source.cpp:163, 329, 431`, `mcap_dialog.hpp:69`
  (`use_log_time`), `contrib/mcap/message_byte_store.hpp:75,141` (lazy reopen),
  `contrib/mcap/writer.inl:40` (silent write failures).

## 15. Adversarial review record (v3 pass, 2026-07-27)

Second Codex pass (same thread as the v1 review), against the real code including the #470
branch tip. **Verdict: v3 sound to proceed after mandatory amendments** — all incorporated in
this v3.1: 3 blockers (async-restore gap → `LayoutImportBatch` §6.2/§8; loader-record handoff →
completion-time adoption §6.1; cross-machine document rewrite → §6.2 ordering), 5 criticals
(cache leases for lazy reopen §5; #470 finished-callback not a success signal §3/§11; headless
bind auto-connect + trust ledger §6.3/§7; request-vs-content addressing made explicit §4/§5;
eager-vs-loader parser divergence → adoption §6.1), 8 high (forced refetch replaces
"re-tee from memory"; validated finalization; narrow corruption policy; cross-platform
locking/cleanup; registry-based identity instead of `sameSourceIdentity` overload;
non-interactive policy persistence; strict query/materialize ABI contract; resource limits), 5
medium (origin parser; conditional old-PJ4 claims + path leakage note; provider-absent stock
fallback; explicit WASM diagnostic; cross-repo SDK/build sequencing). Verified-true claims kept:
old reader ignores `<materialize>`; out-of-tree cache paths stay absolute. Open items tracked
for implementation: tee backpressure measurement, production S3 immutability check, Windows
two-process behavior, #470 pre-merge drift.
