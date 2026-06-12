# PJ4 fork-submodule vendoring — design spec

**Date:** 2026-06-12
**Status:** Approved design (ready for implementation plan)
**Topic:** Replace the plain-source vendored `PJ4/` tree with version-pinned **private
fork submodules**, so the vendored PlotJuggler world has a git-enforced version
guarantee against mainstream and a normal `git merge` sync path.

---

## 1. Problem & motivation

Today `PJ4/` (the PlotJuggler 4 app), `PJ4/plotjuggler_sdk/` (the SDK), and
`PJ4/pj-official-plugins/` (the plugin collection) are vendored into `pj-mcap-server`
as **plain committed source** — none is a git checkout. Consequences:

- **No version record.** Nothing in the repo states which mainstream commit each tree
  corresponds to. You read CLAUDE.md prose ("synced to upstream") and trust it.
- **Painful sync.** Updating to a new upstream means a manual `rsync` +
  worktree-`rebase` dance (base commit → copy vendored changes on top → rebase
  `--onto` the new upstream → copy back), done by hand each time.
- **No git-enforced delta.** "What did we change vs mainstream?" is answerable only by
  ad-hoc `rsync --itemize-changes` against a pristine clone.

**Goal:** make each vendored tree a submodule pinned to a **fork** of its mainstream
repo, where the fork is `upstream-pin + our-commits-on-a-branch`. Then:

- `git submodule status` records the exact commit (the version guarantee).
- `git log <upstream-tag>..cloud` in each fork is *exactly* our delta.
- Syncing is `git fetch upstream && git merge <new-tag>` on the fork branch, then a
  one-line pointer bump in `pj-mcap-server`.

## 2. Goals / non-goals

**Goals**
- Git-enforced version pin of each vendored tree against its mainstream repo.
- A clean, repeatable upstream-sync workflow (merge, not rsync).
- Keep all commercial/connector code consolidated in `pj-mcap-server`.
- `make smoke` + `make matrix` stay green across the migration (behavior-preserving).

**Non-goals**
- No functional change to the server, plugin, or app. This is a vendoring/structure
  change only.
- Not upstreaming our host-side changes to the public PlotJuggler repos (separate
  decision, out of scope here).
- No change to the build toolchain (Conan/CMake) beyond path rebindings.

## 3. Decisions (locked)

| # | Decision | Choice |
|---|---|---|
| 1 | Fork hosting | New repos under the **PlotJuggler GitHub org** |
| 2 | **Repository visibility** | **PRIVATE — every repo created is private, NEVER public.** Pinning an existing public upstream as a submodule is allowed (that is not *creating* a public repo). |
| 3 | Structure | **Three per-repo forks** mirroring the three upstreams (not one combined superrepo) |
| 4 | Connector plugin home | `toolbox_dexory_cloud` moves **into `pj-mcap-server`** (out of the vendored plugins tree), beside the `proto/` it depends on |
| 5 | `pj-official-plugins` mount | **Sibling of `PJ4/`** under `pj-mcap-server` (matches mainstream; avoids submodule-nested-in-submodule-dir) |
| 6 | Plugins fork | Drive its delta toward **zero**; if zero, pin **pristine public mainstream** and create no fork. If non-zero, a **thin private fork**. |

## 4. Target architecture

```
pj-mcap-server/                          (PlotJuggler/pj-mcap-server — private)
├── .gitmodules
├── PJ4/                  ── submodule ─→ PlotJuggler/PJ4-cloud            @ branch `cloud`  [PRIVATE]
│   └── plotjuggler_sdk/  ── submodule ─→ PlotJuggler/plotjuggler_sdk-cloud @ branch `cloud`  [PRIVATE, nested]
├── pj-official-plugins/  ── submodule ─→ pristine PlotJuggler/pj-official-plugins @ 317ea24
│                                         (or PlotJuggler/pj-official-plugins-cloud [PRIVATE] if delta > 0)
├── plugin/
│   └── toolbox_dexory_cloud/            (RELOCATED, in-repo — the connector plugin)
│       ├── cmake/EmbedUi.cmake          (vendored from the plugins repo)
│       ├── cmake/EmbedManifest.cmake    (vendored)
│       └── third_party/mcap/            (vendored MCAP writer; see §6)
├── proto/pj_cloud.proto
├── server/  infra/  scripts/  docs (root *.md)/
```

Rationale recap:
- `plotjuggler_sdk` stays a **nested submodule of `PJ4-cloud`** because mainstream PJ4
  already structures it that way; `git submodule update --init --recursive` brings the
  whole tree.
- `pj-official-plugins` mounts as a **sibling** of `PJ4/` (mainstream has it as a sibling
  repo, not inside PJ4); this avoids registering a submodule at a path that lives inside
  another submodule's working tree.

## 5. The forks and their `cloud`-branch deltas

All three forks branch from the **current upstream HEAD** (we are current after the
2026-06-12 sync). Each `cloud` branch = upstream-pin + the commits below.

### 5.1 `PlotJuggler/PJ4-cloud` (private) — base `37a6aea`

18 in-place host-hook edits + 1 new test. These are the host-side changes that let a
Toolbox reach the parser-ingest pipeline and that the connector dialog/markers need:

```
pj_app/src/MainWindow.cpp                                  (toolbox callbacks, ExtensionCatalog wiring)
pj_runtime/src/AppSession.cpp                              (parser-ingest deps)
pj_runtime/src/DataSourceRuntimeHost.cpp
pj_runtime/src/ToolboxRuntimeHost.cpp                      (create/release_parser_ingest tail slots)
pj_runtime/include/pj_runtime/{AppSession,DataSourceRuntimeHost,ToolboxRuntimeHost}.h
pj_runtime/CMakeLists.txt
pj_runtime/CLAUDE.md
pj_runtime/tests/{app_session_test,toolbox_runtime_host_test}.cpp
pj_runtime/tests/toolbox_parser_ingest_real_ros_test.cpp  (NEW)
pj_dialog_host/src/{pj_ui_loader,widget_binding}.cpp       (PanelEngine widget bindings)
pj_dialog_host/tests/widget_binding_test.cpp
pj_widgets/{include/pj_widgets/RangeSlider.h,src/RangeSlider.cpp}  (slider markers)
pj_scene3D/widgets/{include/pj_scene3d_widgets/Scene3DDockWidget.h,src/Scene3DDockWidget.cpp}  (FrameTransforms ingest)
```
Plus a `run.sh` plugin-discovery-path edit (the app loads the connector `.so` from the
new in-repo build output). Scratch files (e.g. `review.md`) are NOT committed to the fork.

### 5.2 `PlotJuggler/plotjuggler_sdk-cloud` (private) — base `9003e55` (v0.7.0)

9 in-place edits carrying the ABI-appendable parser-ingest tail slots plus related
ABI/dialog changes; SDK version → 0.7.1:

```
pj_base/include/pj_base/toolbox_protocol.h
pj_base/include/pj_base/sdk/toolbox_plugin_base.hpp        (create_parser_ingest / release_parser_ingest)
pj_base/tests/abi_layout_sentinels_test.cpp               (offsets 24/32 sentinels)
pj_plugins/dialog_protocol/include/pj_plugins/sdk/widget_data.hpp
pj_plugins/dialog_protocol/include/pj_plugins/host/widget_data_view.hpp
pj_plugins/include/pj_plugins/testing/toolbox_test_store.hpp
pj_plugins/tests/toolbox_plugin_test.cpp
CMakeLists.txt
conanfile.py
```
The SDK package is `conan create`'d from this submodule (version 0.7.1); the relocated
connector plugin and the `PJ4-cloud` app both build against it.

### 5.3 `pj-official-plugins` — base `317ea24`

Our current delta is 3 config edits **plus** the `toolbox_dexory_cloud/` dir. Once the
connector plugin is relocated to `pj-mcap-server` (§6), the remaining delta is at most:

```
SDK_VERSION                 (0.7.0 → 0.7.1; needed if the upstream plugins must build against our forked SDK)
CMakeLists.txt / conanfile.py   (the arrow-gating + protobuf tool_requires existed ONLY to build our
                                 plugin in-tree — expected to drop entirely)
```
**Resolution rule:** if the residual delta is zero, pin **pristine public
`PlotJuggler/pj-official-plugins@317ea24`** and create no fork. If a non-zero delta is
genuinely required (e.g. the `SDK_VERSION` bump is unavoidable for ABI parity), create a
**thin private `pj-official-plugins-cloud`** fork. Determined empirically during
implementation (does parser_ros built against pristine SDK 0.7.0 load cleanly into the
0.7.1 host? tail slots are appended/`struct_size`-gated, so ABI should be compatible).

## 6. Relocating `toolbox_dexory_cloud`

The plugin is not self-contained — it reaches out three ways. Each is rebound:

| Current reach | New binding |
|---|---|
| `include(../cmake/EmbedUi.cmake)`, `../cmake/EmbedManifest.cmake` | **Vendor** both into `plugin/toolbox_dexory_cloud/cmake/` (small, self-contained) |
| `../data_load_mcap/contrib` (MCAP writer) | **Vendor** into `plugin/toolbox_dexory_cloud/third_party/mcap/` (decouples from the plugins submodule; chosen over cross-submodule reference) |
| `../../../proto/pj_cloud.proto` | `../../proto/pj_cloud.proto` (plugin now at `plugin/toolbox_dexory_cloud/`, two levels under repo root) |

The plugin keeps its own `conanfile.py` + `CMakeLists.txt` and builds **standalone**
against the forked SDK Conan package (0.7.1). Build/run/scripts updates:

- `build.sh` / `run.sh` / `Makefile` / `scripts/*` / smoke + matrix: replace
  `PJ4/pj-official-plugins/toolbox_dexory_cloud` paths and the
  `cd PJ4/pj-official-plugins && ./build.sh toolbox_dexory_cloud` invocation with a
  standalone build of `plugin/toolbox_dexory_cloud` (Conan install + CMake), and copy
  the resulting `.so` into the app's plugin-discovery dir.
- CLAUDE.md "Reference codebases" + "Commands" sections updated for the new layout.

## 7. Migration flow (one-time, on a branch)

**Phase 0 — create the forks (private).**
For `PJ4` and `plotjuggler_sdk` (and `pj-official-plugins` only if §5.3 keeps a delta):
1. `gh repo create PlotJuggler/<name>-cloud --private`.
2. Push mainstream history; create branch `cloud` at the exact upstream base commit
   (§5 — confirm each base by diffing the vendored tree against candidate upstream
   commits before branching).
3. Apply our delta as commit(s) on `cloud` (reconstructed from the current vendored
   tree: check out upstream@base, copy our changed files on top, commit). Point
   `PJ4-cloud`'s `.gitmodules` SDK url at `plotjuggler_sdk-cloud` and pin its nested
   pointer to the SDK fork's `cloud` commit.
4. Push `cloud`. **Verify each new repo is private.**

**Phase 1 — rewire `pj-mcap-server`** (new branch, e.g. `gor/submodule-vendoring`):
1. `git rm -r` the vendored `PJ4/` plain source.
2. `git submodule add -b cloud <PJ4-cloud-url> PJ4`; recurse the SDK submodule.
3. `git submodule add -b cloud <pj-official-plugins-url> pj-official-plugins`
   (or pristine mainstream url @ 317ea24 if no fork).
4. Relocate `toolbox_dexory_cloud` → `plugin/toolbox_dexory_cloud` + §6 path/vendor fixes.
5. Update `build.sh` / `run.sh` / `Makefile` / `scripts/*` / CLAUDE.md / root docs.

**Phase 2 — verify.** Build the app (vendored), build the relocated plugin, `./run.sh`,
`make smoke`, `make matrix`. All must pass before commit.

**Phase 3 — commit & document.** Commit the rewiring; update CLAUDE.md's vendoring and
sync sections. Clones henceforth need `git submodule update --init --recursive`.

## 8. Upstream-sync workflow (the payoff)

To update a component to a new mainstream release:
```
cd PJ4 (or PJ4/plotjuggler_sdk, or pj-official-plugins)
git fetch upstream
git checkout cloud
git merge <new-upstream-tag>        # resolve conflicts in our ~18/9/0 delta files only
git push origin cloud
cd <repo root>
git add PJ4                          # bump the submodule pointer
git commit -m "vendor: bump PJ4 to <tag>"
```
At any time, `git log <upstream-tag>..cloud` is exactly our delta;
`git merge-base --is-ancestor <tag> cloud` proves which mainstream we build on.

## 9. Private-submodule access (note, not a blocker)

Private submodules mean a fresh `git clone --recursive` and CI both need credentials for
each private fork. Provisioning (deploy keys or a PAT/`GH_TOKEN` with org read) is folded
into the implementation plan's CI section; it does not block the local migration.

## 10. Risks & rollback

- **Big structural rewrite of a repo with implementation history.** Do it on a dedicated
  branch; keep the pre-migration commit as a tag (`_pre_submodule`) for instant rollback.
  Nothing is force-pushed until verified.
- **Base-commit drift.** Each fork's upstream base must be confirmed by diff before
  branching (§7 Phase 0.2) so `git log upstream..cloud` is truly just our delta.
- **Plugin relocation regressions.** `make smoke` + `make matrix` are the gates; the
  plugin must build standalone and the `.so` must load in the app exactly as before.
- **Pristine-plugins ABI risk.** If we pin pristine SDK-0.7.0 plugins into a 0.7.1 host,
  confirm parser_ros loads (tail slots are appended/`struct_size`-gated → expected OK);
  if not, fall back to the thin private plugins fork.

## 11. Verification gates (definition of done)

1. `git submodule status` shows three pinned commits (or two + pristine plugins).
2. Each fork is **private** (verified via `gh repo view --json visibility`).
3. `git log <upstream>..cloud` per fork = exactly the §5 delta, no extra files.
4. Vendored app builds; relocated `plugin/toolbox_dexory_cloud` builds standalone.
5. `./run.sh` loads the connector plugin; host-side features present (the vendored-app
   check).
6. `make smoke` → `SMOKE PASS`; `make matrix` → `MATRIX PASS`.
7. CLAUDE.md + root docs describe the new layout and the §8 sync workflow.
