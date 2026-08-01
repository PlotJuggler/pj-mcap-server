# Stage-5 — Cross-Repo Live E2E + Docs Closure (v2, consult-amended)

> **For agentic workers:** REQUIRED SUB-SKILL: superpowers:subagent-driven-development.
> **Status: LOCKED** — Codex consult 019fbc46-5c29-7b42-8564-577608674d85 (2026-08-01)
> returned 8 blocking corrections on v1; ALL folded in below. §5 is the verdict record.

**Goal:** prove the shipped canonical-layout-import stack end-to-end with REAL parts —
real `plotjuggler4` (both the shipped CLI entrypoint AND the real `MainWindow` in a
live gui-test), real `libtoolbox_mcap_cloud_plugin.so` over the real ABI, real network,
real MCAP artifact, real `mcap-loader` promotion — including the pinned §10
requirement: `layout-import-eager-only` shown firing **in the shipped binary too**.
Then close §13 item 5 (GUI flow · `--layout` flow incl. progressive miss · team-sharing
runbook with the §7 leakage note · pinned-loader matrix · live coverage) and the docs.

## 1. LOCKED decisions (do not re-litigate)

- **E1 — hybrid, with the shipped binary carrying the EAGER proof too.** (a) A NEW
  live-gated PJ4 gui-test (`main_window_layout_import_e2e_test`) — real `MainWindow`
  offscreen, REAL plugin `.so`s staged (cloud + `mcap-loader` + `ros-parser`), live
  server — is the semantics centerpiece. (b) Shipped `plotjuggler4 --layout` legs
  (cold/warm/EAGER) with the new `--dump-diagnostics` as the observation channel.
  Both halves use the SAME staged, provenance-recorded DSOs. When the harness sets
  the live env, a SKIPPED gtest FAILS the harness (smoke's pattern).
- **E2 — `--dump-diagnostics <json-path>` lands NOW (PJ4 PR), not as a follow-up.**
  Serializes every diagnostic of the run (collect the whole CLI run — do not
  truncate to the 200-ring) as stable records: `level`, `source`, `id`, `message`,
  `timestamp`. Scripts assert IDs, never message text.
- **E3 — separate `scripts/e2e-layout-import.sh` on :8082, sharing ONLY the Minio
  daemon** (own bucket `e2e-layout`, own catalog DB — the builder flock is per-db;
  never `docker compose down`). A COMMON harness `flock` added to BOTH smoke.sh and
  the e2e script serializes them (shared `server/bin` rebuilds, Minio startup,
  artifact staging, machine load). Per-run `mktemp -d` + shell-owned child PIDs
  (smoke's discipline); NO fixed pidfiles; port-availability preflight;
  teardown removes DB/WAL/SHM, config, logs, sandboxes — Minio stays up. PJ4
  prerequisites absent ⇒ fail-fast with a clear message (local gate, not CI).
- **E4 — scenario corrections:**
  - **(a) Identity source = frozen vectors.** Add explicit `:8082` E2E descriptor
    cases to `docs/source-descriptor-vectors.json` (one per distinct scenario
    identity) and seed fixtures matching those exact keys; consume descriptor
    bytes + identity VERBATIM (no jq/reserialization — saved provenance is
    emitted as verbatim CDATA). Rationale: PJ4 never compares the layout's
    embedded identity against the provider's — a bogus hand-authored identity
    would go unnoticed; vectors keep it honest without linking the canonicalizer
    under test.
  - **(b) Zero-network witnesses**: Prometheus `/metrics` counters
    `pj_cloud_sessions_total` (zero OpenSession) AND
    `pj_cloud_ws_connections_total` (zero connection), before/after the warm and
    trust legs.
  - **(c) The EAGER lever**: `MCAP_CLOUD_CACHE_DIR=<path to a REGULAR FILE>` —
    creating `<file>/<digest>.lock` fails deterministically, non-contended ⇒ tee
    dropped (§9.6) ⇒ EAGER_ONLY. (A read-only DIRECTORY does NOT work: the cache
    chmods its root 0700 before locking.) Fresh identity (different tuple —
    key/topics/range/latched; `display_name` is excluded from identity) so the
    run is a miss. Baseline-first discipline stands: prove the writable-cache
    PROMOTED run before flipping the lever, or a missing parser masquerades as
    the EAGER result via FAILED.
  - **(d) Catalog equality (§12)**: a dataset-ID-normalized signature — scalar
    `(topic, field_path, logical_type, row_count, range)` + object `(topic,
    object_type, normalized_metadata, count, range)` — compared across THREE
    states: the complete eager dataset immediately before promotion, the
    promoted dataset, and a later import/warm stock load after removing the
    prior dataset. Plus DatasetId/TopicId stability and bound-curve survival.
    (Topics+counts alone are near-tautological — both sides would be stock reads
    of one artifact.)
  - **(e) Progressive miss re-proven ONCE live**: scenario 1 must observe ≥1 real
    `ingestProgressed`, a curve bound while the batch is still active, and the
    same curve after promotion (the binder suite proves this with a fake; §13-5
    names progressive miss import — the live event ordering needs one real
    witness).
  - **False-positive guards**: the warm leg must REMOVE the loaded dataset first
    (or fresh process) — `findAlreadyLoaded()` resolves by provenance and would
    bypass cache+loader; the trust leg must use a fresh identity/cache MISS —
    cache hits deliberately bypass the trust gate.
- **E5 — `--exit-after-layout` lands NOW (PJ4 PR, with E2):** quits at the actual
  restore settlement/failure boundary, with a timeout and failure-aware nonzero
  exit status. `--screenshot` may remain a watchdog only — never the success
  oracle (it quits unconditionally and reports 0). Pre-flight: `--validate-plugins
  <dir>` with ALL THREE whitelist entries (`mcap-cloud=0.3.0`, `mcap-loader=1.0.0`,
  `ros-parser=1.0.0`) — the validator rejects loaded-but-unexpected plugins.
- **E6 — the literal GUI flow is AUTOMATED in the gui-test**, not a manual
  checklist: trigger the save/load `QAction`s via the friend peer; zero-delay
  timer selects the path in the modal, verifies the "save data source" binding
  checkbox default, accepts; automate the reload/trust message boxes (PJ4's
  `file_dialog_test` modal technique). The manual checklist survives only as
  runbook guidance.
- **E7 — routing + closure are enumerated (see §3, §4).** All changes via PRs:
  PJ4 PR (gui-test + `--dump-diagnostics` + `--exit-after-layout` + CMake +
  `pj_app/CLAUDE.md` pointer to the architecture doc); this-repo PR (script,
  vector additions, runbook, Makefile/README entries, spec/architecture/CLAUDE
  closure). Official-plugins: the loader/parser DSOs staged for the E2E must be
  REBUILT against SDK 0.20.0 and their provenance recorded (the checked-out tree
  is pinned 0.18.0 — existing binaries are NOT acceptable as the pinned matrix on
  manifest version strings alone); an official-plugins SDK_VERSION bump PR if the
  pin must move for the record.

## 2. Environment rules (consult "misses" — all binding)

- **SDK identity recorded, not assumed**: PJ4 submodule `3a63fe36` (v0.20.0-1) vs
  plugin's Conan 0.20.0 — descriptor/ABI headers byte-identical (verified), but
  record both provenances in the run matrix.
- **XDG isolation**: private `XDG_CONFIG_HOME`/`XDG_CACHE_HOME`/`XDG_DATA_HOME` +
  plugin/cache roots + test org/app names set BEFORE `QApplication`/`MainWindow`
  construction (the plugin reads `XDG_CONFIG_HOME` directly; the extension service
  also scans user-configured folders). Never switch sandboxes mid-process; mutate
  ledger/cache state inside ONE sandbox and only after the prior provider
  instance is retired (trust preloads at `ImportRuntime` construction).
- **Descriptor bytes verbatim** end to end.
- **:8082 hygiene**: availability preflight; process-group traps; a leftover
  builder `.writer.lock` is harmless — never treat it as a pidfile.

## 3. Task breakdown

1. **PJ4 PR half A — the two flags.** `--dump-diagnostics` (full-run JSON) +
  `--exit-after-layout` (settlement-bound quit, timeout, failure-aware exit code),
  red-first unit/gui coverage, wired into `main.cpp` beside the existing
  `--screenshot` block.
2. **This repo — vectors + fixtures + harness skeleton.** New `:8082` vector cases
  (frozen bytes + identities); deterministic fixture seeding matching those keys;
  `scripts/e2e-layout-import.sh` bring-up/teardown + the common smoke flock +
  `E2E-LAYOUT-IMPORT PASS/FAIL` line; `--validate-plugins` pre-flight; DSO
  staging with provenance recording (incl. 0.20.0-rebuilt loader/parser).
3. **PJ4 PR half B — the live gui-test.** Scenarios (amended): 1 cold+progressive
  witness+promotion; 2 automated menu save→(unload)→reload warm with zero-network
  counters; 3 EAGER via regular-file cache root + `layout-import-eager-only`
  asserted in-process; 4 trust fresh-miss refusal then ledger-seeded success;
  5 three-way catalog-equality signature. Live-gated; skip-fails-harness contract.
4. **This repo — shipped-binary legs** in the script: cold/warm/EAGER via
  `--layout --exit-after-layout --dump-diagnostics`, asserting diagnostic IDs +
  artifact + metrics.
5. **Runbook + docs closure + PRs.** `docs/layout-sharing-runbook.md` (per the
  consult's content list: version/artifact matrix, GUI+headless commands,
  diagnostics format + IDs table, trust bootstrap, cache location/purge/recovery,
  metrics, §7 leakage warning verbatim, harness lock/:8082/Minio ownership rules,
  manual GUI checklist as guidance, and the do-not-commit-real-layouts warning).
  Then the enumerated closure list in §4.

## 4. Docs-closure checklist (enumerated; execute in Task 5)

1. `docs/canonical-layout-import.md:5-7` Status — strike "only the stage-5 … remains".
2. `docs/canonical-layout-import.md` §13 item 5 — add the `(DONE …)` annotation.
3. `docs/canonical-layout-import.md` §12 — add the live-E2E leg; mark the catalog
   equality (`:611` region) and matrix-pin asks closed with pointers.
4. `docs/layout-import-architecture.md:16` — drop "Remaining: …".
5. `docs/layout-import-architecture.md` §6 — stage-5 gap bullet → DONE.
6. `docs/layout-import-architecture.md` §5 — add the E2E artifacts (script, gui
   test, runbook) to "Where the proof lives".
7. `CLAUDE.md` layout-import section — CODE-COMPLETE → SHIPPED/VERIFIED; drop the
   remaining-work sentence; add the e2e script to the commands section.
8. `README.md` — layout-import pointer (runbook + architecture doc).
9. `scripts/smoke.sh` header "It proves:" — unchanged (E2E is a separate gate) but
   add the shared-flock note.
10. PJ4 `pj_app/CLAUDE.md` — pointer to the architecture doc (rides the PJ4 PR).
11. Memory + spec Status date stamps.
12. Stage-4/stage-3 plan docs — NO edits (historical provenance).

## 5. Consult record (2026-08-01)

Codex session `019fbc46-5c29-7b42-8564-577608674d85`, verdict on v1: **NO** — 8
blockers, all folded above: E1 amended (shipped binary must carry the EAGER
diagnostic proof; skip-fails-harness), E2 REVERSED (dump flag is acceptance
infrastructure — land now), E3 amended (common flock; no fixed pidfiles), E4
amended (frozen :8082 vectors — PJ4 never cross-checks embedded identity; the
read-only-dir lever INVALID (cache chmods 0700) → regular-file root; metrics
counters named `pj_cloud_sessions_total`/`pj_cloud_ws_connections_total`; warm
leg must unload first (`findAlreadyLoaded` provenance short-circuit); trust needs
a fresh miss (cache hits bypass the gate); catalog equality = 3-way normalized
signature; progressive miss re-proven once live), E5 amended (`--exit-after-layout`
with failure-aware exit; screenshot never the oracle; all three
`--expect-plugin` entries), E6 REVERSED (automate the literal QAction/modal flow
— `file_dialog_test` technique), E7 amended (runbook content list; enumerated
closure; PR routing incl. official-plugins provenance — the checked-out tree is
SDK-pinned 0.18.0, binaries not acceptable on version strings alone).
