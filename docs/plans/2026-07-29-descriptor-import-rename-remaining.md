# Descriptor-Import Rename — Remaining Scopes (PLANNED, NOT YET EXECUTED)

> **Status:** PLANNED ONLY. The SDK half (PlotJuggler/plotjuggler_sdk PR #160) is being
> renamed now, per owner decision 2026-07-29. Everything below is the deliberate
> follow-up scope — do **not** execute until the owner says go.

**Decision being propagated:** the feature vocabulary "descriptor **replay**" becomes
"descriptor **import**" (rationale: "replay" implies already-downloaded data and, in
robotics, connotes rosbag-style time-paced playback — a misreading this project has a
pinned decision against; "import" names the integration and matches the host stack:
PJ4 #470 progressive bulk-import, planned `LayoutImportBatch`, future `ImportJob`).
Separately decided (pending the Codex naming consult verdict for details):
`adopt()` → `promote_to_file_source` on the SDK service side.

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

Files (verified on the branch):
- `plugin/toolbox_mcap_cloud/src/replay_descriptor.hpp` → `import_descriptor.hpp`
- `plugin/toolbox_mcap_cloud/src/replay_descriptor.cpp` → `import_descriptor.cpp`
- `plugin/toolbox_mcap_cloud/tests/replay_descriptor_test.cpp` → `import_descriptor_test.cpp`
  (+ CMake test registration line)
- `docs/replay-descriptor-vectors.json` → `docs/import-descriptor-vectors.json`
  (cross-repo contract file — PJ4 must consume the NEW name; nothing consumes it yet,
  which is why this is cheap only while unmerged/pre-host-work)

Symbol map (open detail: Codex consult question (d) — mechanical `Replay→Import` vs
plain "descriptor" names; default below is mechanical, adjust to the verdict):
- `parseReplayDescriptor` → `parseImportDescriptor`
- `canonicalReplayJson` → `canonicalImportJson` (or `canonicalDescriptorJson`)
- `replayIdentity` → `importIdentity` (or `descriptorIdentity`)
- `toReplayJson` → `toImportJson`
- struct/type names, test suite names, comments accordingly.

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

## Scope 3 — SDK PR #160 follow-up (service half; awaiting Codex consult verdict)

- `pj.materialized_source.v1` → `pj.source_promotion.v1` (or consult's pick),
  `adopt` → `promote_to_file_source`, `PJ_materialized_source_adopt_request_v1_t` →
  `PJ_source_promotion_request_v1_t`, `MaterializedSourceHostView/Service` →
  `SourcePromotionHostView/Service`, `AdoptRequest` → `PromotionRequest`,
  outcome tails `SUCCEEDED_UNMATERIALIZED/MATERIALIZED` →
  `SUCCEEDED_EAGER_ONLY/PROMOTED` (or `SUCCEEDED_FILE_BACKED` per verdict),
  file `materialized_source_service_test.cpp` renamed to match, CHANGELOG + guides
  swept. One commit on the PR #160 branch.

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
