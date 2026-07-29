# Descriptor-Import Rename — Remaining Scopes (PLANNED, NOT YET EXECUTED)

> **Status:** PLANNED ONLY. Scope 0 (SDK PR #160 replay→import) EXECUTED 2026-07-29
> (commit `67cb6ce`, pushed; PR retitled). Everything below is the deliberate
> follow-up scope — do **not** execute until the owner says go.

**Decision being propagated:** the feature vocabulary "descriptor **replay**" becomes
"descriptor **import**" (rationale: "replay" implies already-downloaded data and, in
robotics, connotes rosbag-style time-paced playback — a misreading this project has a
pinned decision against; "import" names the integration and matches the host stack:
PJ4 #470 progressive bulk-import, planned `LayoutImportBatch`, future `ImportJob`).
Separately decided: `adopt()` → `promote_to_file_source` on the SDK service side.

**Codex naming consult verdict (2026-07-29, session `019fadae-c338-7b41-a1af-48a56c647516`):
ACK the full bundle**, with refinements now baked into the scopes below:
- Service id = **`pj.source_promotion.v1`** (not `file_source_promotion` — redundant and
  ambiguous; not keeping `materialized_source` — preserves the state-noun ambiguity).
  C family: `PJ_SOURCE_PROMOTION_HOST_SERVICE_V1`, `PJ_source_promotion_host_t`,
  `PJ_source_promotion_host_vtable_t`.
- Outcome tails = **`SUCCEEDED_EAGER_ONLY` / `SUCCEEDED_PROMOTED`** (PROMOTED over
  FILE_BACKED — the cross-vocabulary reference is a feature: PROMOTED = the whole
  promotion transaction succeeded, not merely "a file exists"). Pin meanings in the
  header: EAGER_ONLY = usable dataset, no promotion transaction completed (an artifact
  may still exist); PROMOTED = `promote_to_file_source` reached its result callback
  with ok=true; a sync rejection or async promotion failure yields EAGER_ONLY (not
  FAILED) if the eager dataset remains usable.
- C++ scope-qualification (generic vocabulary reserved for the future `ImportJob`
  envelope): `DescriptorImportStartRequest`, `DescriptorImportOutcome` (already applied
  in Scope 0), and `SourcePromotionRequest` — never bare `PromotionRequest`.
- Plugin descriptor vocabulary (Scope 1): **NOT mechanical Replay→Import** — the
  descriptor is durable source identity used before/during/after an import, not itself
  an import. Use the **SourceDescriptor** map (details in Scope 1).
- Spec phrase (Scope 2): rename to **"layout import"** / `canonical-layout-import.md`
  (optionally titled "Descriptor-backed Layout Import"); ABI/product divergence
  rejected — the product phrase carries the same playback misreading.
- Explicitly UNCHANGED (semantic audit): enum numeric values and struct layouts; bulk
  unpaced transfer; callback order/threading/cancel/join/lifetime; accepted≠succeeded;
  canonical descriptor JSON bytes + digest + `mcap-cloud:v1:sha256/128:` identity;
  layout `<materialize>` stanza and query `is_materialized` (they describe actual
  artifact materialization — do NOT rename to import/promoted); legitimate replay
  vocabulary (latched, resume, WAL).

## Critical exclusion — the OTHER "replay"

`git grep -i replay` in this repo hits a **different, unrelated concept** that MUST NOT
be renamed: the streaming session's reconnect-resume machinery and SQLite WAL notes.
Verified occurrences to LEAVE UNTOUCHED:

- `proto/pj_cloud.proto` — latched / transient-local replay; `seq>resume_after_seq`
  replay contract (wire protocol, session resume).
- `server/internal/session/*.go` — resume replays retained batches (retain.go,
  producer.go, registry*, resume_test, seed_producer_test).
- `plugin/toolbox_mcap_cloud/src/backend_connection.*`, `backend_types.hpp`,
  `fetch_worker.cpp`, `mcap_cloud_dialog.hpp`, `session_key.hpp`, `vocab_select.hpp`,
  `tools/mcap_cloud_cli.cpp` — resume/latched-replay comments referencing spec §6.6.
- `mcap_catalog/mcap_catalog_builder/publish.py` — WAL "stale frames replayed".
- `docs/history.md`, `arch/2026-05-28-…design.md` — historical record; never rewrite.

Rule of thumb during execution: rename only vocabulary belonging to the
**layout/descriptor feature** (descriptor, layout, provider extension); every
"replay" tied to *resume, retained batches, latched topics, or WAL frames* stays.

## Scope 1 — plugin repo, PR #11 branch `feat/layout-replay-foundations` (OPEN, unmerged)

Per the consult verdict: **SourceDescriptor** vocabulary, NOT mechanical Replay→Import
(the descriptor is durable source identity, not itself an import).

Files (verified on the branch):
- `plugin/toolbox_mcap_cloud/src/replay_descriptor.hpp` → `source_descriptor.hpp`
- `plugin/toolbox_mcap_cloud/src/replay_descriptor.cpp` → `source_descriptor.cpp`
- `plugin/toolbox_mcap_cloud/tests/replay_descriptor_test.cpp` →
  `source_descriptor_test.cpp` (+ CMake test registration line)
- `docs/replay-descriptor-vectors.json` → `docs/source-descriptor-vectors.json`
  (cross-repo contract file — PJ4 must consume the NEW name; nothing consumes it yet,
  which is why this is cheap only while unmerged/pre-host-work)

Symbol map (consult's concrete map):
- `ReplayDescriptor` → `SourceDescriptor`
- `parseReplayDescriptor` → `parseSourceDescriptor`
- `canonicalReplayJson` → `canonicalSourceDescriptorJson`
- `replayIdentity` → `descriptorIdentity`
- `toReplayJson` → `toSourceDescriptorJson`
- test suite names, comments accordingly.
- **Persisted MCAP metadata key** `kProvenanceName = "mcap_cloud/replay_descriptor"`
  (verified: `session_file_cache.cpp:32`, also referenced in
  `session_mcap_writer.hpp:51`) → `"mcap_cloud/source_descriptor"`. Externally
  observable but unpublished; existing dev caches carrying the old key are discarded
  (or treated read-only-miss) — no migration shim.

**No wire/identity impact:** the identity string format
(`mcap-cloud:v1:sha256/128:<hex>`) and the descriptor `kind` (`mcap-cloud-session`)
contain no "replay" — verified. The vectors' bytes are unchanged; only the file name
and code symbols move.

Branch mechanics: PR #11 is open — add the rename as a new commit on its branch (no
history rewrite), update the PR title/body if they say "replay".

## Scope 2 — the spec (this repo, main)

- `docs/canonical-layout-replay.md` → `docs/canonical-layout-import.md` (git mv), plus
  a prose sweep of feature-vocabulary "replay" (§ titles, ABI names now matching the
  renamed SDK, `<materialize>` stanza text unaffected). Add a short §15 entry recording
  the rename decision + rationale + date.
- `docs/plans/2026-07-28-layout-replay-stage1-foundations.md` — executed historical
  plan: LEAVE (historical record), or add a one-line header note pointing at the
  rename. Do not rewrite.
- `CLAUDE.md` current-state prose if it references the replay feature by name.

## Scope 3 — SDK PR #160 follow-up (service half; verdict in, ready to execute on "go")

- Service id `pj.materialized_source.v1` → **`pj.source_promotion.v1`**; method
  `adopt` → `promote_to_file_source`; C names
  `PJ_MATERIALIZED_SOURCE_HOST_SERVICE_V1` → `PJ_SOURCE_PROMOTION_HOST_SERVICE_V1`,
  `PJ_materialized_source_host{,_vtable}_t` → `PJ_source_promotion_host{,_vtable}_t`,
  `PJ_materialized_source_adopt_request_v1_t` → `PJ_source_promotion_request_v1_t`,
  result-fn typedef accordingly.
- C++: `MaterializedSourceHostView/Service` → `SourcePromotionHostView/Service` with
  `promoteToFileSource()`; `AdoptRequest` → **`SourcePromotionRequest`** (scope-
  qualified — never bare `PromotionRequest`).
- Outcome tails: `SUCCEEDED_UNMATERIALIZED/MATERIALIZED` →
  **`SUCCEEDED_EAGER_ONLY/PROMOTED`** (C and C++ `kSucceeded*`), with the verdict's
  pinned meanings written into the header (EAGER_ONLY covers promotion-failed-but-
  dataset-usable; PROMOTED = result callback ok=true). Enum NUMERIC values unchanged.
- File `materialized_source_service_test.cpp` → `source_promotion_service_test.cpp`;
  ABI sentinels for the renamed structs (names in asserts/messages only — offsets
  unchanged); CHANGELOG 0.20.0 entry + toolbox-guide/ARCHITECTURE/REQUIREMENTS swept
  ("adoption" prose → "promotion"; section retitled).
- Note: query-result `is_materialized` and the layout `<materialize>` stanza are
  NOT renamed (verdict: they describe actual artifact materialization).
- One commit on the PR #160 branch; gates: zero
  `materialized_source|adopt`-vocabulary greps outside legit uses, 52/52 both trees.

## Scope 4 — presentation & memory (after Scopes 1–3)

- Artifact `https://claude.ai/code/artifact/63b7f871-71ca-4a48-8a69-02448f85ba0d`
  (PR #160 explainer) — republish with import/promotion vocabulary.
- Auto-memory files (`canonical-layout-replay-design.md` + MEMORY.md hook) — update
  names + record the decision.
- PR #160 title/body — updated in the SDK pass (Scope 0, running now).

## Ordering & gates

1. (running) SDK PR #160 replay→import; gate: `git grep -in replay` = 0 in that repo,
   52/52 both configs.
2. Scope 3 service rename (after consult verdict) — same gates.
3. Scope 1 plugin branch (PR #11) — gate: 46/46 ctest + `make smoke` PASS; the
   exclusion rule applied (resume/latched/WAL "replay" untouched — review the diff for
   accidental hits).
4. Scope 2 spec rename — no build gate; link-check references to the renamed file.
5. Scope 4 presentation/memory.

Est. total: ~1–2 h agent time. Everything is pre-publish/pre-merge except the spec
(docs-only), so no compatibility fallout anywhere; the only cross-repo contract touched
is the vectors file name, consumed by nothing yet.
