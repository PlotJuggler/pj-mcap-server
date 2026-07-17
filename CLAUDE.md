# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this directory is

This is the **design / planning workspace AND implementation repo for the "PJ Cloud
Connector"** — a commercial engagement (clients: Dexory/S3, Asensus/GCS) to build a
self-hosted server + client that serves MCAP recordings from a cloud bucket to
PlotJuggler-class clients on demand.

This repo (`pj-mcap-server`, a git repo on branch `main`) holds the design spec, the
implementation plans, the commercial proposal, the **Go server + connector plugin**
implementation, and (2026-06-22) the **vendored auryn Python catalog builder** (in
`mcap_catalog/` — VENDORED directly as of 2026-07-17; formerly a submodule of
`AurynRobotics/mcap_server`, de-submoduled so this repo is fully self-contained).
**[LOCAL DECISION 2026-06-04]** The
implementation code goes **in this repo**: every `pj-cloud/<path>` reference in the
plans maps to `/home/gn/ws/PJ4_Server_Template/pj-mcap-server/<path>` (the plans were
originally written for a separate `pj-cloud/` repo at
`/home/davide/ws_plotjuggler/pj-cloud`; the absolute paths in the plans have been
rewritten to this repo, and Plan A Task 1 Step 1 was amended — do **not** `git init` a
new repo).

## Current state (2026-07-17): catalog migration COMPLETE + merged to main + wire v2

**The auryn catalog migration (M1–M6 incl. the full M6 tail) is DONE, and the whole
project has been merged to `main` and pushed.** The 2026-07-17 pre-merge review (dual
Claude + Codex) fixes and the follow-on wire/catalog hardening are all landed; **GitHub
CI is fully green** (unit/builder-tests/integration-{s3,gcs}/race/seam + wasm-compile +
bench). The production architecture is a **two-process system**: the Python
**`mcap_catalog/`** builder (VENDORED directly in this repo — no longer a submodule) is
the SOLE catalog writer (schema v3, atomic publish via temp+rename, `--tag-socket`
tag-edit IPC, `--no-watch` rescan-only daemon mode, and a **single-writer flock** on
`<db>.writer.lock` so a second builder on the same `--db` exits 3) and the Go server is a
**pure read-only catalog reader + unchanged streamer** — `catalog.OpenReadOnly` is the
ONLY constructor, `internal/indexer/` and every catalog write API are DELETED (−2k LOC),
`UpdateTags` forwards over a unix-socket IPC to the builder (key-addressed; rowids
renumber across rebuilds), and the server detects an atomically-published rebuild by
(dev,inode) file identity and reopens live (`ReopenIfSwapped`, 30s tick). Cross-request
generation safety is now explicit: reads lease a **snapshot** (`Store.Acquire`,
drain-then-close), and a **generation token** (opaque bytes) rides `ListFiles`/
`GetVocabulary` responses so a stale pagination cursor or dimension-id filter fails
`ERROR_STALE_CATALOG` instead of silently mis-serving a renumbered id. `OpenFresh` is
**key-addressed** (wire v2: `s3_keys`, `file_ids` removed; Hello `protocol_version` = 2).
`-external-builder`/`-poll-interval` are deprecated no-ops.
`CATALOG_CONTRACT.md` (§9 publish/reopen, §10 tag IPC, §11 single-writer lock;
byte-identical copies in `docs/` and `mcap_catalog/`) is the cross-language contract —
schema/IPC changes MUST update it.

- **Remaining follow-ups:** `matrix.sh` migration (fail-fasts with exit 2 today; needs the
  `jkk_dataset02` corpus machine); the C++ facet UI (client-side `GetVocabulary`, now that
  the generation token makes cached dimension ids safe to echo —
  `docs/catalog-vocabulary-rpc.md`); real-bucket GCE smoke (`docs/gce-deploy-smoke.md`);
  builder gaps by design: `derive_tags()` stub, GCS Pub/Sub discovery, `file_metrics`.
- **Team rule:** technical decisions are cross-checked with a standing Codex instance
  before they're locked; milestone boundaries get adversarial review (Codex + Claude —
  this caught ~35 real defects across the M6 tail).

Implementation history (Slices 1–16 + migration narrative): `docs/history.md` —
provenance, not instructions.

## Decisions & pins (do not re-litigate)

Settled, present-tense facts about the system, distilled from the implementation
history (`docs/history.md`) and the migration record. Verify against code if in
doubt, but don't relitigate the decision itself.

- **"Dexory Cloud" is a TOOLBOX, forever** — a user product decision; Slice 16's
  host parser-delegation tail slots made a DataSource shape unnecessary (a Toolbox
  can now reach the full parser pipeline).
- **The connector plugin ships ZERO message decoders.** Decoding happens via host
  parser delegation through the SDK's `create_parser_ingest`/`release_parser_ingest`
  tail slots on `PJ_toolbox_runtime_host_vtable_t` (ABI-appendable, `struct_size`-gated,
  no protocol bump).
- **`parser_config_json` MUST be non-empty (`"{}"` at minimum)** or parser_ros
  silently degrades to generic scalar-only (`loadConfig` is the only path to
  specialized-handler registration); ROS type names must pass VERBATIM (e.g.
  `tf2_msgs/msg/TFMessage`) — parser_ros normalizes internally.
- **Transport is ixwebsocket; the CLI and plugin link zero Qt.** QtWebSockets /
  a standalone `client-core` (the original plan) is a SUPERSEDED design — do not
  "restore" it without a new decision.
- **LZ4 chunks are FRAMES, decoded with `lz4.NewReader`** — not raw blocks. Chunk
  `UncompressedCRC` is read but NEVER verified; the only integrity surface is a
  zstd/lz4 decode failure.
- **Catalog RPC responses use an OPT-IN compressed envelope** (`EncodedServerMessage`,
  field 20 in the `ServerMessage` oneof; client negotiates via
  `Hello.accepted_response_encodings`). Server wraps only via an EXPLICIT allowlist
  (ListFiles/GetVocabulary/GetFile/OpenSession/UpdateTags) — HelloResponse/Error/
  Progress/Eos/MessageBatch are ALWAYS raw (handshake ordering, latency, and batches
  are already ZSTD inside), with a raw fallback when compression doesn't shrink.
  Outer envelope carries ZERO request_id/subscription_id — the inner message is the
  sole routing authority (client unwraps BEFORE routing, hardened decoder: 64 MiB
  cap, exactly-one-frame, exact-size). **Default level 1** (bodies compress well
  even at the fastest level); transport-level config `server.response_compression`,
  distinct from the session `body_zstd_level`. WS permessage-deflate stays OFF at
  both ends (it would double-compress the batch frames).
- **GCS change-detect identity = `Generation` (decimal string) + `Updated`** — never
  MD5/CRC32C — slotted into the existing `(etag,size,last_modified)` triple with
  zero indexer/schema change.
- **Catalog reader swap trigger = (dev,inode) file identity, NEVER `build_id`**
  (freshness-only signal). File rowids renumber across rebuilds, so anything crossing
  a request/process boundary is addressed by DURABLE identity, never rowid: sessions
  by `s3_key` (OpenFresh v2), and any RPC that still carries rowids (ListFiles cursors,
  GetVocabulary dimension ids) is bound to the **generation token** and rejects a
  stale one with `ERROR_STALE_CATALOG`. Multi-query reads lease ONE snapshot
  (`Store.Acquire` — atomic `{db, identity, generation}`, drain-then-close) rather
  than re-fetching `s.DB()` per query.
- **`database/sql` connection pools lazily open new connections BY PATH** — the
  read-only catalog store pins `MaxOpenConns(1)` with verified identity so a
  second pooled connection can't silently read a stale/swapped file.
- **Tags are stored as EAV, not JSONB** (decided 2026-07-06) — indexed filter
  predicates and facet `GROUP BY`s need real columns/rows, and provenance
  (override vs. derived) is cleanest as table separation, not a blob shape.
- **`tags_override` is user data**: the Python builder never touches it on a
  re-catalog, and rebuilds carry it forward by composite identity (not rowid).
- **The tag-edit unix socket bypasses WS bearer auth** — the Go layer is the auth
  boundary. The catalog volume (SQLite WAL) must never sit on NFS/EFS.
- **Client auth = ONE shared bearer token, enforced FAIL-CLOSED** (checked at the
  WS `Hello` via the `ClientAuthenticator` seam, constant-time compare). The
  server binary REFUSES to start when no token is configured; running with no
  auth requires an explicit `-allow-anonymous` flag / `PJ_CLOUD_ALLOW_ANONYMOUS=1`
  (the dev scripts `run.sh`/`smoke.sh` pass it). Per-client identity / permissions
  / multi-tenancy stay an M1 non-goal — the seam is where real auth slots in.
- **The MCAP `FormatCodec` REJECTS unsummarized files** — fixtures must be chunked
  + summarized + carry Statistics, or the codec refuses them outright.
- **A `FileSourceBase` without `file_extensions` is UNREACHABLE in the host**
  (extension-gated, `FileLoader.cpp:141-149`) — don't ship one without extensions
  and expect it to load.
- **protoc for the checked-in Go bindings = 3.21.12.** A clean regen from
  `proto/pj_cloud.proto` must be byte-identical to what's checked in — verify with
  `git diff` before touching the proto; never hand-edit the generated Go.
- **`.ui` files must stay ASCII** (the build hex-embeds them) — PanelEngine's widget
  whitelist has no `QTreeWidget` binding, so any hierarchy UI is a prefix-filter
  combo, not a tree.
- **Ground truth for the C++ live gtests comes from a deterministic synthetic Hive
  corpus** (`gen-ci-fixtures -hive -hive-big` + `gen-3d-fixture`); `make smoke`
  derives everything else (counts, topics) at runtime via `mcaptopics` — never
  hardcode a count that isn't one of the few pinned live-gtest values.
- A cancelled session's stale `Eos` must not poison the next pull — the plugin opens
  a **fresh `BackendConnection` per download** (Slice 8, live-test-caught).
- **`RESUME_NOT_POSSIBLE` fails verbatim** with the partial result kept — never a
  silent fallback to a fresh `OpenSession`.
- Observability (dashboard/metrics registration) **must attach BEFORE `loop.Start`**,
  or warm-start indexer/reader counters are lost — a real wiring-order bug once
  found in `main`.
- Stitched multi-file selection: **client-side pairwise non-overlap pre-validation
  is a UX nicety only — the server remains authoritative** for overlap validation.
- API keys are stored via the plugin's own `credential_store` seam (atomic 0600 JSON
  under `XDG_CONFIG_HOME`), never in plaintext `SettingsView`; libsecret remains a
  documented drop-in behind the same seam if/when it's worth the Qt6 pull.

## Reference codebases (MANDATORY context — always reuse these)

Any agent doing design or implementation work here **must** ground itself in these
local codebases first. Do not guess SDK/plugin APIs — read the real headers.

> **CURRENT REPO LAYOUT (2026-06-22, still true):** PJ4, `plotjuggler_sdk`, and
> `pj-official-plugins` are **NOT submodules of this repo** (removed in `82a8c2f`) —
> they are managed as sibling checkouts under `~/ws_plotjuggler/` (e.g. `PJ4-cloud/`,
> `plotjuggler_sdk-cloud/`). The connector plugin builds **standalone** at
> `plugin/toolbox_dexory_cloud/` against the SDK Conan package (**0.11.0** — built on
> this machine 2026-07-06 from `~/ws_plotjuggler/plotjuggler_sdk-cloud`). The ONLY
> directory `mcap_catalog/` is VENDORED (formerly the only submodule). Prose below that mentions `PJ4/` as a
> submodule or SDK 0.8.1 is a stale reference-reading aid — the fork/vendoring history
> lives in `docs/history.md`. Paths below were verified 2026-06-04 on the original
> machine (`/home/gn/...`).

### 1. `/home/gn/ws/PJ4` — the PlotJuggler 4 application workspace

The product this connector ultimately plugs into. Read first:

- `/home/gn/ws/PJ4/CLAUDE.md` — project handbook (module placement, build, conventions).
- `/home/gn/ws/PJ4/PJ4_PLAN.md` — authoritative architecture doc (three-level
  architecture; plugin discovery/load model; `IDataWidget` contract).
- `/home/gn/ws/PJ4/plotjuggler_sdk/` — **the plugin SDK** (what the future `pj_cloud`
  plugin compiles against). Key headers:
  - `pj_base/include/pj_base/sdk/data_source_patterns.hpp` — `PJ::FileSourceBase`
    (one-shot importer; Plan D's chosen base) and `PJ::StreamSourceBase`.
  - `pj_base/include/pj_base/sdk/data_source_host_views.hpp` — `ParserBindingRequest`
    (line ~90), `ensureParserBinding` (~191), `pushMessage` (~241) and the **payload
    lifetime contract** (closure returns `sdk::PayloadView` zero-copy + anchor, must be
    idempotent/thread-safe; lines ~211–301). This is the delegated-ingest seam Plan D
    Task 4 builds on.
  - `pj_base/include/pj_base/sdk/data_source_plugin_base.hpp` —
    `PJ_DATA_SOURCE_PLUGIN(Class, manifest)` macro (line ~239). The class **must be
    default-constructible** (factory calls `new Class()`; config arrives via
    `loadConfig()`).
  - `pj_plugins/include/pj_plugins/sdk/dialog_plugin_base.hpp` — `PJ_DIALOG_PLUGIN(...)`
    (variadic: legacy 1-arg + manifest 2-arg forms) and the real dialog callback surface
    (verify exact handler names here before implementing).
  - `pj_base/include/pj_base/sdk/toolbox_plugin_base.hpp` — `ToolboxPluginBase`
    (Mosaico's base; `pj_cloud` is a DataSource instead — see Plan D §0 note 1).
  - `pj_base/include/pj_base/sdk/plugin_data_api.hpp` — direct-ingest write API
    (`ensureTopic`/`ensureField`/`appendRecord`); NOT used by `pj_cloud` (delegated
    ingest), listed for orientation.
- Build: `./build.sh` (Conan 2 + CMake, Qt 6.11.1 from `.qt/` via `install_qt6.sh`), run via `./run.sh`
  (defaults plugin discovery to `pj-official-plugins/build`).

### 2. `/home/gn/ws/PJ4/pj-official-plugins` — the official plugin collection

How real PJ4 plugins are shaped, built, and shipped. **Note: on this machine it lives
*inside* `PJ4/`** (the original author had it as a sibling). Read first:

- `CLAUDE.md`, `PLUGIN_DEVELOPMENT.md`, `porting_guide.md` — plugin shapes
  (self-parsing vs delegating DataSource, MessageParser catalog pattern), data-write
  rules, mechanical-translation policy.
- `SDK_VERSION` — single source of truth for the SDK pin (**currently `0.11.0`**;
  this repo's copy is `plugin/SDK_VERSION`; the Conan package is `conan create`d
  from `~/ws_plotjuggler/plotjuggler_sdk-cloud` — the root `./build.sh` checks the
  cache and prints the command if absent); every plugin's `conanfile.py` reads it
  live. Never hardcode the SDK version.
- Build: `./build.sh [plugin_dir]` (Conan 2 + CMake, C++20, `-Wall -Wextra -Werror`…).
  CMake helpers: `pj_embed_ui` / `pj_embed_manifest` (local `cmake/`),
  `pj_emit_plugin_manifest` (ships with the SDK package — emits the `.pjmanifest.json`
  sidecar the host scans pre-dlopen).
- Closest analogs for this project:
  - `data_load_mcap/` — how PJ4 loads MCAP today (`McapSource : FileSourceBase`,
    delegated ingest, lazy `PayloadView` closures). **`contrib/mcap/` vendors a full
    MCAP writer too** (`writer.hpp`/`writer.inl`) — reusable for round-trip work.
  - `data_stream_foxglove_bridge/`, `data_stream_pj_bridge/` — WS-streaming sources;
    the threading discipline to copy: WS callback thread only *queues*; host calls
    (`ensureParserBinding`/`pushMessage`) only from the poll thread. pj_bridge also
    shows ZSTD-batch framing (nearest analog to our wire batches).
  - `data_stream_zmq/` — minimal delegated-ingest streaming reference.
  - Transport note **[AMENDED 2026-06-05, audit-verified]**: the design spec §9 /
    Plan B envisioned a Qt-WebSockets `client-core`; the **as-built transport is
    ixwebsocket** (the in-repo plugin convention — `src/backend_connection.*`), and
    `dexory-cloud-cli` links **zero Qt** (`ldd`-verified). There is no standalone
    `client-core` library; the transport units compile into the plugin and the CLI.
    Plan B's Qt shape is superseded — do not "restore" QtWebSockets without a new
    decision.

### 3. `/home/gn/ws/PJ4/pj-official-plugins/toolbox_mosaico` — the UI/worker design source

(Directory name has an **underscore**, not a hyphen.) The Mosaico cloud-browser plugin
whose dialog/state/worker design Plan D and the unified plan **lift**: connect-to-URI +
history, Lua metadata query engine (`src/query/`), three-panel UI
(`ui/mosaico_panel.ui`), command-queue worker + `onTick`-drained event queue
(`src/fetch_worker.{hpp,cpp}`, `src/mosaico_dialog.{hpp,cpp}`), `host_write_mu_`
serialization, `src/settings_store.hpp` (over `PJ::sdk::SettingsView`),
`src/server_history.h`, TLS/cert handling (`src/tls_utils.h`). What gets **swapped**:
its Arrow-Flight transport (`MosaicoClient`) → our `client-core` WS+Protobuf, and its
Arrow ingest (`src/arrow_ingest.*`) → raw-record forwarding to host MessageParsers.

## Documents (read in this order)

**Current plans (in `docs/` — the up-to-date forward work):**
- `docs/auryn-catalog-migration-plan.md` — the catalog migration plan (**EXECUTED —
  all blocks complete 2026-07-06**; kept as the design record). `docs/CATALOG_CONTRACT.md`
  is the LIVE cross-language contract (schema v3, publish/reopen §9, tag IPC §10;
  byte-identical copy in `mcap_catalog/` — always update both).
- `docs/catalog-vocabulary-rpc.md` — the `GetVocabulary` filter-RPC design: a strict
  cascading customer→site→robot tree + flat `source` + tag facets, filtered server-side;
  resolves the migration's D3.
- `docs/gce-deploy-smoke.md` — the Asensus GCE/ADC deploy-smoke runbook (the pending
  real-bucket M1 gate).
- `docs/ec2-deploy.md` — the **Dexory** EC2 deploy runbook (Docker Compose, IAM
  instance role, IMDS hop-limit); paired artifacts
  `server/deploy/{docker-compose.dexory.yml,config.dexory-ec2.yaml}`.

**Canonical references (kept in `arch/`):**
1. `arch/2026-05-28-pj-cloud-connector-design.md` — **the canonical design spec (single
   source of truth).** 14 sections: architecture, repo layout, catalog/SQLite model, wire
   protocol, Go server design, Qt client design, failure/resume, testing, phased build order.
2. `arch/2026-06-03-unified-cloud-connector-plan.md` — **the unified plan** (Dexory S3 +
   Asensus GCS, one codebase): the six abstraction seams (`BlobStore`, `FormatCodec`,
   `ClientAuthenticator`, …), milestones M0–M2c, testing matrix, risks, open commercial items.
3. `arch/2026-06-01-dexory-proposal.md` — the commercial proposal / SOW (source `*.md`;
   the rendered html/pdf are generated artifacts — do not hand-edit).

NOTE: the per-component implementation plans (Plan A Go server, Plan B Qt client, Plan C
integration, Plan D PJ4 plugin) and the 2026-06-04 two-endpoints approach doc were ARCHIVED
and removed once their work landed (Slices 1–16) — recover from git history if ever needed.
The narrative "Plan A/B/C/D Task N" references throughout this file are historical pointers
to that completed work.

`proto/pj_cloud.proto` is the **canonical wire schema** — treat it as the single source of
truth for the protocol. The **Go** bindings are checked in (`server/internal/wire/pj_cloud/`);
the **C++** bindings are generated at build time by the Conan protoc (never the system
protoc — version must match the linked libprotobuf).

## The headless SDK harness — the regression gate (2026-06-04)

**All backend (server) work MUST keep `make smoke` green — run it before declaring any
slice done.** The harness proves the whole pipeline without the GUI:

- `make smoke` (= `scripts/smoke.sh`): fully self-contained, proves the TWO-PROCESS
  production shape — generates a deterministic **synthetic Hive-keyed corpus** (8 files,
  `gen-ci-fixtures -hive -hive-big` + `gen-3d-fixture`) into a dedicated `smoke-hive`
  Minio bucket → starts the **Python builder daemon** (`--no-watch --tag-socket`, venv)
  as sole writer → starts its **own** Go server on **:8081** with `-tag-ipc-socket`
  (never touches the interactive instance; reaps BOTH processes on exit) → oracle
  assertions **self-derived at runtime** via `mcaptopics` on the local originals (no
  hardcoded counts; only the C++ live gtests pin the deterministic synthetic values) →
  plugin ctest hermetic+live → both client stacks → 7 mcapdiff round-trip legs →
  server-restart persistence by (dev,inode) → step h: tag edit through the IPC that
  **survives a `--once --rebuild` mid-run** (atomic publish + live `ReopenIfSwapped` +
  `tags_override` carry-forward). Final line `SMOKE PASS` / `SMOKE FAIL: <step>`.
  **Prereq:** the builder venv at `~/.venvs/pj-catalog` (bootstrap:
  `python3 -m venv ~/.venvs/pj-catalog && ~/.venvs/pj-catalog/bin/pip install
  boto3==1.43.40 google-cloud-storage==3.12.0 mcap==1.4.0 watchdog==6.0.0`).
- `scripts/ci-integration.sh` (needs `PJ_CI_BUILDER_PYTHON=~/.venvs/pj-catalog/bin/python3`):
  the local mirror of the CI integration legs — Python builder `--once` over both
  `{s3, gcs}` emulator legs + the `scripts/sabotage-check.sh` quarantine red-team.
  Final line `CI-INTEGRATION PASS`.
- `make matrix` (= `scripts/matrix.sh`): **PENDING MIGRATION — fail-fasts with exit 2**
  and a clear message. It depended on the deleted in-process Go indexer and its
  real-corpus pins need the `/home/gn/ws/jkk_dataset02` machine; the legacy leg
  definitions (m1–m8) are retained in the script as the migration starting point.
  Do not "fix" it piecemeal — migrate it to the Python-builder pipeline like smoke.
- `dexory-cloud-cli` (built by `./build.sh toolbox_dexory_cloud`, lands under
  `build/toolbox_dexory_cloud/Release/toolbox_dexory_cloud/`): `hello` / `list [--json]`
  / `topics <sequence> [--json]` / `download <seq…> [--topics …] [--time-range …]
  --output …` (variadic = stitched; duplicate → exit 2) / `tag`, `--url` or
  `DEXORY_CLOUD_URL` (default `ws://localhost:8080`), `--insecure` for
  self-signed `wss://`. Exit 0/1/2 = ok/connection-failure/usage.
- Ground truth is pinned in TWO places that must move in lockstep when the bucket is
  reseeded: `scripts/smoke.sh` constants and
  `toolbox_dexory_cloud/tests/backend_connection_live_test.cpp` (8 sequences; 6 topics
  + imu==14904 for `nissan_zala_50_zeg_1_0.mcap`).
- `make server-start` / `make server-stop` manage the interactive `:8080` instance
  (`/tmp/pj-cloud-server.{pid,log}`).

## Commands

### One-command local bring-up (the tester-facing path, 2026-06-12)

**Root `./build.sh` + `./run.sh`** make the whole stack runnable from a fresh checkout
with **synthetic** data — no AWS, no credentials, no manual seeding (the steps that were
easy to forget). Use these unless you have a reason not to:

```bash
./build.sh   # builds server + dev tools (server/bin/, direct `go build` — NO protoc
             # needed, the wire bindings are checked in) + the plugin (+ the GUI app
             # if Qt is installed; otherwise it prints the one-time Qt install command).
./run.sh [--dexory_minio]  # LOCAL (the default): Minio up -> seed synthetic Hive-keyed MCAPs IFF the
             # bucket is empty -> PYTHON BUILDER daemon first (sole writer + tag-edit IPC on
             # /tmp/pj-cloud-tag.sock; waits for build_metadata, 'ok' OR 'partial' both count)
             # -> Go server on :8080 (read-only, -tag-ipc-socket).
./run.sh --dexory_aws      # Dexory staging bucket (AWS S3): config.dexory-staging.yaml, :8084.
./run.sh --asensus_google  # Asensus GCS: config.asensus-staging.yaml (REPLACE_ME template guard), :8085, ADC.
./run.sh <config.yaml>     # power-user escape hatch: any S3/GCS server config (this is how to use a
             # non-default port when :8080 is taken).
             # TWO processes, ONE backend at a time (/tmp/pj-cloud-{server,builder}.{pid,log});
             # `make server-stop` reaps BOTH. Stop LOCAL Minio too: (cd infra/minio && docker compose down).
             # Reuse checks are pidfile-based: a FOREIGN listener on the port (this machine runs
             # Guacamole on :8080!) is a clear hard error, never a false "Backend ready".
             # Builder venv required: ~/.venvs/pj-catalog (see the smoke section for the pinned bootstrap).
```

Seeding is a new tool, **`server/cmd/seed`** (`go build -o bin/seed ./cmd/seed`): a tiny
idempotent S3 uploader. `seed -check` exits 0 (empty/seed-needed) / 3 (has data/skip) —
`run.sh` uses it as the Minio readiness probe AND the skip gate; `seed -dir <fixtures>`
uploads `*.mcap` (path-style Minio defaults from `config.Default()`; `-bucket`/`-endpoint`
for any S3). `run.sh` generates the fixtures with the existing `gen-ci-fixtures` (browsable
multi-topic recordings) + `gen-3d-fixture` (one /tf + pointcloud recording for the 3D scene).
**`run.sh` does NOT use `make server-start`** (that target depends on `make build` → `make proto`
→ needs protoc); it starts `server/bin/pj-cloud-server` directly, writing the same
`/tmp/pj-cloud-server.{pid,log}` so `make server-stop` still works. **Verified end-to-end
2026-06-12** (skip-path against the maintainer corpus + upload-path against a throwaway bucket).

### Runnable today (the underlying manual steps `./run.sh` automates)

```bash
# END endpoint: local S3 (bucket `recordings`; console :9001, admin/password123)
cd infra/minio && docker compose up -d

# START endpoint: build the connector plugin (standalone, in THIS repo).
# Easiest: ./build.sh from the repo root builds server + SDK pkg + official
# plugins (from the fork) + the connector + the app, and stages the .so. Or just
# the connector:
cd plugin/toolbox_dexory_cloud \
  && conan install . --output-folder=build --build=missing -s compiler.cppstd=20 \
  && cmake -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release \
  && cmake --build build -j"$(nproc)"   # -> plugin/toolbox_dexory_cloud/build/bin/

# Run PJ4 — ALWAYS the CLOUD-FORK app (the sibling checkout ~/ws_plotjuggler/PJ4-cloud;
# its run.sh auto-loads plugins from its sibling pj-official-plugins build dir). NEVER
# the PRISTINE upstream PJ4 checkout: it carries NONE of the host-side changes (SDK
# tail slots, RangeSlider markers, widget bindings, …) — the plugin .so still loads
# there, so plugin features appear while host features silently vanish, which looks
# like "nothing changed".
cd ~/ws_plotjuggler/PJ4-cloud && ./run.sh
# After a host edit: rebuild the fork app (./build.sh in PJ4-cloud) and commit to its
# `cloud` branch. After a connector edit: rebuild plugin/toolbox_dexory_cloud (this
# repo's root ./build.sh does it and re-stages the .so).
```

Render the proposal `*.md` → self-contained `*.html`:

```bash
python3 _render_proposal.py          # needs the `markdown` pip package
```

PDF is then produced from that HTML with headless Chrome
(`google-chrome --headless --print-to-pdf=docs/2026-06-01-dexory-proposal.pdf docs/2026-06-01-dexory-proposal.html`).
Editing the proposal = edit the `.md`, re-run the script, regenerate the PDF.

### Once implementation lands in this repo (per the plans)

- Go server (Plan A, top-level `Makefile`): `make proto` (generate bindings) ·
  `make build` · `make test` (unit + race) · `make integration` (needs Docker for Minio) ·
  `make bench` (v1 benchmark gate). Single test: `cd server && go test ./internal/<pkg>/ -run <Name>`.
- Qt C++ client (Plan B): Conan 2 + CMake ≥ 3.21, C++20. Two targets `client-core`
  (Qt-aware static lib, **no `Qt6::Widgets`**) and `client-cli` (`QCoreApplication` exe),
  tests via gtest/ctest.
- Integration (Plan C): `docker-compose up -d --build` (Minio + fake-gcs + server),
  `go run ./cmd/gen-fixtures --out fixtures`, then the `go test` matrix driver
  (`PJCLOUD_BACKEND` pins one of `{s3,gcs}`; unset runs both legs).

## Architecture being designed (the big picture)

A single WebSocket per client carries everything, multiplexed via a Protobuf envelope:
catalog RPCs **and** session data streaming on one connection. Bulky **catalog RPC
responses** are additionally ZSTD-compressed at the envelope layer when the client
opts in at Hello (`EncodedServerMessage`; server-side allowlist, hardened client
decoder — see the pin below); **session data batches** carry their own inner ZSTD
frame independently. Streaming is **bounded-horizon, as-fast-as-possible** (a bulk
download with a known size), *not* wall-clock-paced playback. "Streaming" here means *incremental/progressive download* — bytes arrive
in batches so the client can show already-received data while the rest downloads in the background —
**not** real-time pacing. Supports reconnect-and-resume (short retain window) and
cancel-mid-stream.

- **Two-process backend** (the "one static binary" property is GONE — M6):
  - **Python `mcap_catalog` builder** (vendored; sole SQLite writer): discovers bucket
    objects (S3 SQS events / rescan; `--no-watch` = rescan-only), extracts MCAP
    footer/summary metadata (1–2 range-GETs), writes the auryn schema (v3: Hive
    dimensions, topic-set dedup, `tags_effective` override layer, `catalog_failures`
    quarantine, `build_metadata` freshness), publishes rebuilds ATOMICALLY (temp +
    checkpoint-gate + rename; `tags_override` carried forward), and serves the tag-edit
    IPC on a unix socket (bounded, deadline-checked, single-writer-queued).
  - **Go server** (read-only reader + streamer), subsystems on one TCP listener: Catalog
    (WS RPC; `OpenReadOnly` ONLY — pool pinned to one verified connection, (dev,inode)
    swap detection + `ReopenIfSwapped`, generation-pinned multi-query reads), Session
    (WS streaming, producer/consumer split for resume — UNCHANGED by the migration),
    tag-IPC forwarder (`internal/tagipc`; `UpdateTags` is key-addressed, capability =
    IPC configured), chunk-index warmer, Dashboard (HTML + Prometheus + `/health`,
    catalog freshness + quarantine). Pure-Go SQLite (no cgo). Seams unchanged:
    `BlobStore` (S3 + GCS), `FormatCodec` (MCAP), `ClientAuthenticator`. Only
    `internal/storage` may import a cloud SDK **in the server binary's path**; the
    standalone dev tool `cmd/seed` (a throwaway S3 fixture uploader) imports the AWS
    SDK directly by design — it is not part of the serving path. The tag socket bypasses WS bearer auth —
    the Go layer is the auth boundary (deploy mounts the socket volume into only the
    two processes; never on NFS/EFS — SQLite WAL).
- **Qt C++ client** — `client-core` owns the WS/protocol/decompression and exposes a
  `SessionSink` seam; `client-cli`'s `McapWriterSink` reconstructs the streamed session as a
  local MCAP file. **`client-core` is deliberately Widgets-free so the PJ4 DataSource
  plugin (Plan D, deferred to M2b) can lift it in unchanged.**
- **Integration harness** — deterministic MCAP fixture matrix (compression, payload sizes,
  multi-file stitching, tags, time-range edges), run on **both** `{s3,gcs}` legs. The v1
  gate: original MCAP → server → CLI → reconstructed MCAP must be **logically equal** on
  `(topic, log_time, payload, publish_time, schema name/encoding/data)`.

Server-side **stitching**: selecting N consecutive MCAPs presents one continuous logical
session (one time range, union of topics, ordered stream). The client commits to
`(s3_keys[], topic_names[], time_range)` before streaming (wire v2: key-addressed —
catalog rowids renumber across builder rebuilds, so ids never cross the wire for
session opens); the server returns pre-flight
estimates (`estimated_chunk_bytes`, `approximate_messages`).

## Working conventions

- The plans are written **for agentic execution**: each starts with a required
  sub-skill (`superpowers:subagent-driven-development` or `superpowers:executing-plans`) and
  uses `- [ ]` checkboxes for task tracking. Follow that workflow when implementing them, and
  follow the spec's **§13 phased build order** (refined by the unified plan's §5 milestones)
  for sequencing across plans.
- **Before implementing anything that touches PJ4 or the plugin SDK, consult the
  "Reference codebases" section above and read the real headers** — Plan D §0 exists
  precisely because the spec named APIs that don't exist in the SDK.
- Engagement shape (from the proposal + unified plan): **Milestone 1** = PoC (Go server +
  Qt CLI, round-trip validated), gated on written client approval before **Milestone 2** =
  hardening + PlotJuggler plugin integration (with a browser/WASM bonus). Keep v1 work
  inside the M1 non-goals (no multi-tenancy, no realtime pacing, no PJ4 plugin, single
  shared bearer token; Dexory M1 gate = whole-file topic filtering — tag editing and
  intra-file time windows are M2 scope for Dexory).
