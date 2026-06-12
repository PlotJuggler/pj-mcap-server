# PJ4 fork-submodule vendoring — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the plain-source vendored `PJ4/` tree with version-pinned **private fork submodules**, relocate the `toolbox_dexory_cloud` connector plugin into `pj-mcap-server/plugin/`, and adopt a `git merge` upstream-sync workflow — with no behavior change (`make smoke` + `make matrix` stay green).

**Architecture:** Three private forks under the PlotJuggler org (`PJ4-cloud` with nested `plotjuggler_sdk-cloud`; `pj-official-plugins` pinned pristine or as a thin private fork), each `cloud` branch = `upstream-HEAD + our-delta`. The connector plugin moves out of the vendored plugins tree into `pj-mcap-server/plugin/` (beside the `proto/` it consumes), vendoring its two cmake helpers + the MCAP writer.

**Tech Stack:** git submodules, `gh` CLI (authed as GNERSIS, `repo` scope), Conan 2 + CMake, the existing `make smoke` / `make matrix` harness.

**Source spec:** `2026-06-12-pj4-fork-submodule-vendoring-design.md`

**Confirmed facts (preflight):** upstream bases are current HEADs — PJ4 `37a6aea`, SDK `9003e55` (v0.7.0), plugins `317ea24`; all default branch `main`. `PJ4/.qt` → `/home/gn/ws/PJ4/.qt`. Plugin reach-outs: `../cmake/Embed{Ui,Manifest}.cmake` (2.1KB each), `../data_load_mcap/contrib/mcap` (304KB), `../../../proto/pj_cloud.proto`, and `../SDK_VERSION` (in `conanfile.py`).

---

## File structure

**New in `pj-mcap-server`:**
- `plugin/toolbox_dexory_cloud/` — relocated connector plugin (was `PJ4/pj-official-plugins/toolbox_dexory_cloud/`)
- `plugin/toolbox_dexory_cloud/cmake/Embed{Ui,Manifest}.cmake` — vendored helpers
- `plugin/toolbox_dexory_cloud/third_party/mcap/` — vendored MCAP writer
- `plugin/SDK_VERSION` — SDK pin the plugin's `conanfile.py` reads (lockstep with the SDK fork)
- `.gitmodules` — three (or two) submodule entries

**Becomes a submodule (was committed source):**
- `PJ4/` → `PlotJuggler/PJ4-cloud` @ `cloud`; nested `PJ4/plotjuggler_sdk/` → `PlotJuggler/plotjuggler_sdk-cloud` @ `cloud`
- `pj-official-plugins/` (sibling of `PJ4/`) → pristine `PlotJuggler/pj-official-plugins@317ea24` or `pj-official-plugins-cloud` @ `cloud`

**Modified (path updates):** `build.sh`, `run.sh`, `scripts/smoke.sh`, `scripts/matrix.sh`, `scripts/RUNBOOK.md`, `README.md`, `CLAUDE.md`.

**New private GitHub repos (ALL `--private`, NEVER public):** `PlotJuggler/plotjuggler_sdk-cloud`, `PlotJuggler/PJ4-cloud`, and conditionally `PlotJuggler/pj-official-plugins-cloud`.

---

## Task 0: Preflight & rollback safety

**Files:** none (git tags + captured variables)

- [ ] **Step 1: Confirm clean-ish state and capture the rollback point**

Run:
```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
git rev-parse --abbrev-ref HEAD          # expect: gor/clouds
git tag _pre_submodule HEAD
git tag --list _pre_submodule            # expect: _pre_submodule
```
Expected: tag created at current HEAD (`49ef202` or later). This is the instant-rollback point: `git reset --hard _pre_submodule` undoes the whole migration.

- [ ] **Step 2: Capture the `.qt` symlink target**

Run:
```bash
readlink PJ4/.qt                          # expect: /home/gn/ws/PJ4/.qt
```
Record the value; it is recreated after the submodule swap (Task 6).

- [ ] **Step 3: Re-confirm the three upstream base commits match the vendored non-delta tree**

Run:
```bash
rsync -rn --checksum --itemize-changes PJ4/plotjuggler_sdk/ /home/gn/ws/PJ4/plotjuggler_sdk/ \
  --exclude='.git' --exclude='build' --exclude='.cache' 2>/dev/null \
  | grep '^[<>]f' | grep -v '^>f+++' | wc -l      # expect: 9
rsync -rn --checksum --itemize-changes PJ4/ /home/gn/ws/PJ4/ \
  --exclude='.git' --exclude='build' --exclude='.qt' --exclude='.cache' \
  --exclude='pj-official-plugins' --exclude='plotjuggler_sdk' --exclude='docs/superpowers' 2>/dev/null \
  | grep '^[<>]f' | grep -v '^>f+++' | wc -l      # expect: 18
```
Expected: 9 and 18. If different, STOP — the vendored tree drifted from the assumed bases; reconcile before forking.

- [ ] **Step 4: Confirm `gh` can create org repos**

Run:
```bash
gh auth status                            # expect: Logged in ... account GNERSIS, scopes incl. 'repo'
gh repo list PlotJuggler --limit 3 >/dev/null && echo "org access OK"
```
Expected: `org access OK`. No commit (preflight only).

---

## Task 1: Create `plotjuggler_sdk-cloud` private fork

**Files:** new GitHub repo + a temp working clone (not in `pj-mcap-server`)

- [ ] **Step 1: Create the private repo**

Run:
```bash
gh repo create PlotJuggler/plotjuggler_sdk-cloud --private \
  --description "Private fork of plotjuggler_sdk carrying the PJ Cloud Connector parser-ingest tail slots"
```
Expected: `✓ Created repository PlotJuggler/plotjuggler_sdk-cloud`.

- [ ] **Step 2: Verify it is PRIVATE (hard gate)**

Run:
```bash
gh repo view PlotJuggler/plotjuggler_sdk-cloud --json visibility -q .visibility   # expect: PRIVATE
```
Expected: `PRIVATE`. If `PUBLIC`, immediately `gh repo edit PlotJuggler/plotjuggler_sdk-cloud --visibility private --accept-visibility-change-consequences` and re-verify.

- [ ] **Step 3: Build the fork content (seed `main`, branch `cloud`, apply our 9-file delta)**

Run:
```bash
TMP=$(mktemp -d); echo "TMP=$TMP"
git clone git@github.com:PlotJuggler/plotjuggler_sdk.git "$TMP/sdk"
cd "$TMP/sdk"
git remote add fork git@github.com:PlotJuggler/plotjuggler_sdk-cloud.git
git push fork 9003e55:refs/heads/main                       # seed main = upstream history up to base
git checkout -b cloud 9003e55
rsync -a --checksum /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4/plotjuggler_sdk/ ./ \
  --exclude='.git' --exclude='build' --exclude='.cache'
git add -A
git status --short                                          # expect: exactly the 9 delta files (M)
```
Expected: `git status` lists the 9 files from spec §5.2 (`toolbox_protocol.h`, `toolbox_plugin_base.hpp`, `abi_layout_sentinels_test.cpp`, `widget_data.hpp`, `widget_data_view.hpp`, `toolbox_test_store.hpp`, `toolbox_plugin_test.cpp`, `CMakeLists.txt`, `conanfile.py`) — no extras.

- [ ] **Step 4: Commit and push the `cloud` branch**

Run:
```bash
git commit -m "cloud: parser-ingest tail slots + dialog/ABI changes (SDK 0.7.1)

Carries the PJ Cloud Connector delta on top of upstream 9003e55 (v0.7.0):
create_parser_ingest/release_parser_ingest toolbox host tail slots,
ABI layout sentinels, and the SDK version bump to 0.7.1."
git push fork cloud
SDK_CLOUD_COMMIT=$(git rev-parse HEAD); echo "SDK_CLOUD_COMMIT=$SDK_CLOUD_COMMIT"
```
Expected: push succeeds; record `SDK_CLOUD_COMMIT` (needed by Task 2).

- [ ] **Step 5: Verify the delta is exactly ours**

Run:
```bash
git log --oneline 9003e55..cloud          # expect: 1 commit (the cloud: ... commit)
git diff --stat 9003e55..cloud | tail -1  # expect: 9 files changed
```
Expected: 1 commit, 9 files. No commit in `pj-mcap-server` this task (the work is in the fork).

---

## Task 2: Create `PJ4-cloud` private fork (nested SDK repointed)

**Files:** new GitHub repo + temp working clone

- [ ] **Step 1: Create and verify-private the repo**

Run:
```bash
gh repo create PlotJuggler/PJ4-cloud --private \
  --description "Private fork of PJ4 carrying the PJ Cloud Connector host-side hooks"
gh repo view PlotJuggler/PJ4-cloud --json visibility -q .visibility   # expect: PRIVATE
```
Expected: `PRIVATE` (same hard gate as Task 1 Step 2).

- [ ] **Step 2: Seed `main` and branch `cloud` at the base**

Run:
```bash
git clone git@github.com:PlotJuggler/PJ4.git "$TMP/pj4"
cd "$TMP/pj4"
git remote add fork git@github.com:PlotJuggler/PJ4-cloud.git
git push fork 37a6aea:refs/heads/main
git checkout -b cloud 37a6aea
```
Expected: `cloud` branch at `37a6aea`.

- [ ] **Step 3: Apply our 18+1 app delta (NOT the submodules)**

Run:
```bash
rsync -a --checksum /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4/ ./ \
  --exclude='.git' --exclude='build' --exclude='.qt' --exclude='.cache' \
  --exclude='plotjuggler_sdk' --exclude='pj-official-plugins' --exclude='docs/superpowers' \
  --exclude='review.md'
git add -A
git status --short                          # expect: the 18 modified + 1 new test from spec §5.1
```
Expected: exactly the spec §5.1 files (`MainWindow.cpp`, `pj_runtime/*`, `pj_dialog_host/*`, `RangeSlider.*`, `Scene3DDockWidget.*`, the new `toolbox_parser_ingest_real_ros_test.cpp`). `review.md` and scratch are excluded.

- [ ] **Step 4: Repoint the nested SDK submodule at our SDK fork**

Run:
```bash
git config -f .gitmodules submodule.plotjuggler_sdk.url git@github.com:PlotJuggler/plotjuggler_sdk-cloud.git
git config -f .gitmodules submodule.plotjuggler_sdk.branch cloud
git submodule sync plotjuggler_sdk
git -C plotjuggler_sdk remote set-url origin git@github.com:PlotJuggler/plotjuggler_sdk-cloud.git
git -C plotjuggler_sdk fetch origin cloud
git -C plotjuggler_sdk checkout "$SDK_CLOUD_COMMIT"
git add .gitmodules plotjuggler_sdk
```
Expected: `.gitmodules` SDK url now points at `plotjuggler_sdk-cloud`; the gitlink for `plotjuggler_sdk` is staged at `$SDK_CLOUD_COMMIT`.

- [ ] **Step 5: Update the app's plugin-discovery path in `run.sh` for the new layout**

The vendored app's `run.sh` auto-loads plugins from a path relative to PJ4. After migration, `pj-official-plugins` is a sibling of `PJ4` (one level up), and the connector `.so` comes from `pj-mcap-server/plugin/...`. Edit `PJ4-cloud`'s `run.sh` so its default `--extensions`/plugin-dir resolves to `../pj-official-plugins/build/all/Release/bin` (and document that the connector `.so` is copied in by the root `build.sh`).

Run (inspect, then edit):
```bash
grep -n "pj-official-plugins\|extensions\|plugin" run.sh | head
```
Apply the path change (sibling instead of in-tree), then:
```bash
git add run.sh
```
Expected: `run.sh` references `../pj-official-plugins/...` rather than `pj-official-plugins/...`.

- [ ] **Step 6: Commit and push `cloud`**

Run:
```bash
git commit -m "cloud: host parser-ingest hooks + point SDK at plotjuggler_sdk-cloud

Carries the PJ Cloud Connector delta on top of upstream 37a6aea: pj_runtime
toolbox parser-ingest tail-slot wiring, MainWindow callbacks, dialog widget
bindings, RangeSlider markers, Scene3D FrameTransforms ingest, and the nested
plotjuggler_sdk submodule repointed at the private SDK fork (cloud branch)."
git push fork cloud
PJ4_CLOUD_COMMIT=$(git rev-parse HEAD); echo "PJ4_CLOUD_COMMIT=$PJ4_CLOUD_COMMIT"
git log --oneline 37a6aea..cloud           # expect: 1 commit
```
Expected: push succeeds; `git log 37a6aea..cloud` = 1 commit. No `pj-mcap-server` commit this task.

---

## Task 3: Relocate `toolbox_dexory_cloud` into `pj-mcap-server/plugin/`

**Files:**
- Move: `PJ4/pj-official-plugins/toolbox_dexory_cloud/` → `plugin/toolbox_dexory_cloud/`
- Create: `plugin/toolbox_dexory_cloud/cmake/Embed{Ui,Manifest}.cmake`, `plugin/toolbox_dexory_cloud/third_party/mcap/`, `plugin/SDK_VERSION`
- Modify: `plugin/toolbox_dexory_cloud/CMakeLists.txt`, `plugin/toolbox_dexory_cloud/conanfile.py` (if needed)

This task runs **while `PJ4/` is still plain source** (so the helper/MCAP sources are present to copy). Do NOT remove `PJ4/` yet.

- [ ] **Step 1: Move the plugin dir (tracked files) and copy in the vendored deps**

Run:
```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
mkdir -p plugin
git mv PJ4/pj-official-plugins/toolbox_dexory_cloud plugin/toolbox_dexory_cloud
cp PJ4/pj-official-plugins/cmake/EmbedUi.cmake       plugin/toolbox_dexory_cloud/cmake/
cp PJ4/pj-official-plugins/cmake/EmbedManifest.cmake plugin/toolbox_dexory_cloud/cmake/
mkdir -p plugin/toolbox_dexory_cloud/third_party
cp -r PJ4/pj-official-plugins/data_load_mcap/contrib/mcap plugin/toolbox_dexory_cloud/third_party/mcap
printf '0.7.1\n' > plugin/SDK_VERSION
```
Expected: `plugin/toolbox_dexory_cloud/` exists with `cmake/EmbedUi.cmake`, `cmake/EmbedManifest.cmake`, `cmake/check_no_qt.cmake` (its own), `third_party/mcap/`; `plugin/SDK_VERSION` = `0.7.1`.

- [ ] **Step 2: Rebind the four reach-out paths in the plugin `CMakeLists.txt`**

Edit `plugin/toolbox_dexory_cloud/CMakeLists.txt`:

```
# line ~16-17: helpers now local to the plugin
-include(${CMAKE_CURRENT_LIST_DIR}/../cmake/EmbedUi.cmake)
-include(${CMAKE_CURRENT_LIST_DIR}/../cmake/EmbedManifest.cmake)
+include(${CMAKE_CURRENT_LIST_DIR}/cmake/EmbedUi.cmake)
+include(${CMAKE_CURRENT_LIST_DIR}/cmake/EmbedManifest.cmake)

# line ~28: proto now two levels up (plugin/toolbox_dexory_cloud -> pj-mcap-server/proto)
-set(DEXORY_CLOUD_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/../../../proto/pj_cloud.proto)
+set(DEXORY_CLOUD_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/../../proto/pj_cloud.proto)

# line ~36: proto import dir
-  IMPORT_DIRS     ${CMAKE_CURRENT_SOURCE_DIR}/../../../proto
+  IMPORT_DIRS     ${CMAKE_CURRENT_SOURCE_DIR}/../../proto

# line ~142: MCAP writer now vendored under third_party/
-set(DEXORY_CLOUD_VENDORED_MCAP ${CMAKE_CURRENT_SOURCE_DIR}/../data_load_mcap/contrib)
+set(DEXORY_CLOUD_VENDORED_MCAP ${CMAKE_CURRENT_SOURCE_DIR}/third_party)
```
Expected: no remaining `../../../` or `../cmake/` or `../data_load_mcap` references. Verify:
```bash
grep -nE "\.\./\.\./\.\.|\.\./cmake|\.\./data_load_mcap" plugin/toolbox_dexory_cloud/CMakeLists.txt   # expect: no output
```

- [ ] **Step 3: Confirm the plugin `conanfile.py` SDK pin resolves to the new `plugin/SDK_VERSION`**

The `conanfile.py` reads `os.path.join(dirname(__file__), os.pardir, "SDK_VERSION")` = `plugin/SDK_VERSION`. The Step 1 file satisfies it. Verify:
```bash
python3 -c "import os; p=os.path.join('plugin/toolbox_dexory_cloud', os.pardir, 'SDK_VERSION'); print(open(p).read().strip())"   # expect: 0.7.1
```
Expected: `0.7.1`. No code change needed (only the new file).

- [ ] **Step 4: Commit the relocation (the move + path fixes are one logical change)**

Run:
```bash
git add -A
git commit -m "refactor: relocate toolbox_dexory_cloud into pj-mcap-server/plugin/

Move the connector plugin out of the vendored plugins tree to plugin/, beside
the proto it consumes. Vendor its two cmake helpers + the MCAP writer; rebind
the proto path (../../proto) and SDK_VERSION pin (plugin/SDK_VERSION). No
behavior change; verified by a standalone build in the next task."
```
Expected: clean commit. (`PJ4/pj-official-plugins/toolbox_dexory_cloud` no longer tracked; the rest of `PJ4/` still plain source.)

---

## Task 4: Build the relocated plugin standalone (verify the relocation before rewiring)

**Files:** none (build only). Precondition: the `plotjuggler_sdk/0.7.1` Conan package is in the local cache (true on this machine from prior builds; on a fresh machine, `conan create PJ4/plotjuggler_sdk` first — folded into `build.sh` in Task 7).

- [ ] **Step 1: Standalone Conan install + CMake build**

Run:
```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/plugin/toolbox_dexory_cloud
conan install . --output-folder=build --build=missing -s compiler.cppstd=20
cmake -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```
Expected: build succeeds; `build/bin/libtoolbox_dexory_cloud_plugin.so` and `build/.../dexory-cloud-cli` produced. If the proto/embed/MCAP paths are wrong, this fails here — fix Task 3 Step 2 and rebuild.

- [ ] **Step 2: Run the hermetic ctests (live tests skip without a server)**

Run:
```bash
ctest --test-dir build --output-on-failure
```
Expected: all non-live tests pass, live tests SKIP (no server on this path). Matches the smoke harness's hermetic mode.

- [ ] **Step 3: Confirm the CLI still links zero Qt**

Run:
```bash
ldd build/*/dexory-cloud-cli 2>/dev/null | grep -i qt && echo "FAIL: Qt linked" || echo "OK: no Qt"
```
Expected: `OK: no Qt`. No commit (verification only).

---

## Task 5: Resolve `pj-official-plugins` — pristine first, thin fork only if needed

**Files:** conditionally a new private repo. This task determines whether the upstream plugins build cleanly against our 0.7.1 host with the **pristine** submodule (delta zero) or needs a thin fork (the `SDK_VERSION` bump).

- [ ] **Step 1: Determine the residual plugins delta after the connector left**

Run:
```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
rsync -rn --checksum --itemize-changes PJ4/pj-official-plugins/ /home/gn/ws/PJ4/pj-official-plugins/ \
  --exclude='.git' --exclude='build' --exclude='.cache' --exclude='.cpm-cache' 2>/dev/null \
  | grep '^[<>]f' | grep -v '^>f+++'
```
Expected: now that `toolbox_dexory_cloud` is gone, only `CMakeLists.txt`, `conanfile.py`, `SDK_VERSION` may show. The `CMakeLists.txt`/`conanfile.py` edits existed ONLY to build our plugin in-tree (the `add_subdirectory(toolbox_dexory_cloud)`, arrow-gating, protobuf tool_requires) — they are NOT needed once the plugin builds standalone. So the only *candidate* required delta is `SDK_VERSION` (0.7.0 → 0.7.1).

- [ ] **Step 2: Decide pristine vs thin fork (ABI test)**

The tail slots are appended/`struct_size`-gated, so `parser_ros` built against pristine SDK 0.7.0 (cloudsmith package) should load in the 0.7.1 host. Decision rule:
- Proceed with **pristine** `PlotJuggler/pj-official-plugins@317ea24` (no new repo) and let Task 8's `make smoke` be the ABI test.
- IF Task 8 fails because `parser_ros` won't load / version-mismatches, fall back to a thin fork: `gh repo create PlotJuggler/pj-official-plugins-cloud --private`, seed `main` at `317ea24`, branch `cloud`, bump `SDK_VERSION` → `0.7.1` (one-file delta), push, and use that submodule URL in Task 6 Step 4. Verify `PRIVATE`.

Record the choice. No commit (decision recorded; submodule added in Task 6).

---

## Task 6: Rewire `pj-mcap-server` — remove vendored `PJ4/`, add submodules

**Files:** `.gitmodules` (new), `PJ4/` (now submodule), `pj-official-plugins/` (new submodule), `PJ4/.qt` (recreated symlink)

This is the irreversible-feeling step; `_pre_submodule` (Task 0) is the rollback.

- [ ] **Step 1: Remove the vendored `PJ4/` plain source**

Run:
```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
git rm -r --quiet PJ4
rm -rf PJ4                                 # clear untracked build/, .qt symlink, .cache, the nested repos
ls PJ4 2>/dev/null && echo "STILL EXISTS — investigate" || echo "PJ4 cleared"
```
Expected: `PJ4 cleared`.

- [ ] **Step 2: Add the `PJ4-cloud` submodule and pin it to `cloud`**

Run:
```bash
git submodule add -b cloud git@github.com:PlotJuggler/PJ4-cloud.git PJ4
git submodule update --init --recursive PJ4
git -C PJ4 rev-parse HEAD                  # expect: $PJ4_CLOUD_COMMIT (from Task 2)
git -C PJ4/plotjuggler_sdk rev-parse HEAD  # expect: $SDK_CLOUD_COMMIT (from Task 1)
```
Expected: `PJ4/` checked out at the cloud commit; nested `plotjuggler_sdk/` at the SDK cloud commit.

- [ ] **Step 3: Recreate the `.qt` symlink**

Run:
```bash
ln -s /home/gn/ws/PJ4/.qt PJ4/.qt
readlink PJ4/.qt                           # expect: /home/gn/ws/PJ4/.qt
```
Expected: symlink restored (build.sh finds Qt 6.8.3). `.qt` is gitignored in the PJ4 fork, so it stays untracked.

- [ ] **Step 4: Add the `pj-official-plugins` submodule (sibling of `PJ4/`)**

Run (pristine path — per Task 5 default):
```bash
git submodule add git@github.com:PlotJuggler/pj-official-plugins.git pj-official-plugins
git -C pj-official-plugins checkout 317ea24
git add pj-official-plugins
```
(If Task 5 chose the thin fork, use `-b cloud git@github.com:PlotJuggler/pj-official-plugins-cloud.git` instead.)
Expected: `pj-official-plugins/` at `317ea24` (or the fork's `cloud`).

- [ ] **Step 5: Sanity-check `.gitmodules`**

Run:
```bash
cat .gitmodules
git submodule status --recursive
```
Expected: entries for `PJ4` (branch cloud) and `pj-official-plugins`; recursive status shows the nested SDK. No commit yet (committed after the build verifies in Task 8).

---

## Task 7: Update `build.sh` / `run.sh` / scripts for the new layout

**Files:** `build.sh`, `scripts/smoke.sh`, `scripts/matrix.sh`, `run.sh`, `scripts/RUNBOOK.md`, `README.md`

The mechanical transform: the connector plugin is built **standalone from `plugin/toolbox_dexory_cloud`** (not via `cd PJ4/pj-official-plugins && ./build.sh toolbox_dexory_cloud`), and the official plugins build from the `pj-official-plugins/` submodule.

- [ ] **Step 1: Locate every reference to the old plugin/plugins paths**

Run:
```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
grep -rn "PJ4/pj-official-plugins\|pj-official-plugins && ./build.sh\|build.sh toolbox_dexory_cloud" \
  build.sh run.sh scripts/smoke.sh scripts/matrix.sh scripts/RUNBOOK.md README.md
```
Expected: a finite list of lines. Each is updated in Steps 2-4.

- [ ] **Step 2: Add SDK-package bootstrap + standalone plugin build to `build.sh`**

In `build.sh`, ensure (in order): (a) `conan create PJ4/plotjuggler_sdk` produces `plotjuggler_sdk/0.7.1` if absent; (b) the official plugins build from `pj-official-plugins/`; (c) the connector plugin builds from `plugin/toolbox_dexory_cloud` via its standalone Conan+CMake (Task 4 Step 1 commands); (d) the built `libtoolbox_dexory_cloud_plugin.so` is copied into the app's plugin-discovery dir (`pj-official-plugins/build/all/Release/bin`, matching `run.sh`). Replace any `cd PJ4/pj-official-plugins && ./build.sh toolbox_dexory_cloud` with the standalone build.

Expected: `./build.sh` (no args) builds server + SDK package + official plugins + connector plugin (+ app if Qt present), with the connector `.so` landing in the discovery dir.

- [ ] **Step 3: Update `scripts/smoke.sh` and `scripts/matrix.sh` plugin-build + ctest paths**

Replace the in-tree plugin build (`build.sh toolbox_dexory_cloud` under the plugins dir) with the standalone build of `plugin/toolbox_dexory_cloud`, and point the ctest invocations at `plugin/toolbox_dexory_cloud/build`. The `dexory-cloud-cli` path becomes `plugin/toolbox_dexory_cloud/build/<...>/dexory-cloud-cli`.

Expected: smoke/matrix build the connector from `plugin/` and find the CLI at the new path.

- [ ] **Step 4: Update `run.sh`, `README.md`, `scripts/RUNBOOK.md` paths**

Replace `PJ4/pj-official-plugins/...` references and the build/run command snippets with the new layout (`plugin/toolbox_dexory_cloud`, sibling `pj-official-plugins/`). The "Run PJ4" instruction stays `cd PJ4 && ./run.sh` (the submodule's run.sh now resolves the sibling plugins dir + the copied connector `.so`).

- [ ] **Step 5: Commit the script updates**

Run:
```bash
git add build.sh run.sh scripts/smoke.sh scripts/matrix.sh scripts/RUNBOOK.md README.md
git commit -m "build: retarget plugin build + paths for the submodule layout

Connector plugin now builds standalone from plugin/toolbox_dexory_cloud; official
plugins build from the pj-official-plugins submodule; SDK 0.7.1 package is
conan-created from the nested SDK submodule."
```
Expected: clean commit.

---

## Task 8: Full verification (the behavior-preserving gate)

**Files:** none (verification). This is the definition-of-done gate.

- [ ] **Step 1: Build everything from the new layout**

Run:
```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
./build.sh
```
Expected: server + SDK package + official plugins + connector plugin all build; app builds if Qt present.

- [ ] **Step 2: Run the smoke gate**

Run:
```bash
make smoke
```
Expected: final line `SMOKE PASS`. If it fails because `parser_ros` won't load (ABI), execute Task 5 Step 2's thin-fork fallback, redo Task 6 Step 4 with the fork URL, and re-run.

- [ ] **Step 3: Run the matrix gate**

Run:
```bash
make matrix
```
Expected: final line `MATRIX PASS`.

- [ ] **Step 4: Verify the vendored app loads host-side features**

Run:
```bash
cd PJ4 && ./build.sh && ./run.sh    # smoke-launch; confirm the Dexory Cloud toolbox + host hooks present
```
Expected: app launches with the connector toolbox and host changes (RangeSlider markers, parser-ingest). Close after confirming. No commit (verification).

---

## Task 9: Commit the rewiring + update docs

**Files:** `.gitmodules`, `PJ4`, `pj-official-plugins` (gitlinks), `CLAUDE.md`

- [ ] **Step 1: Commit the submodule rewiring**

Run:
```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
git add .gitmodules PJ4 pj-official-plugins
git commit -m "vendor: convert PJ4 + plugins to version-pinned private fork submodules

PJ4/ -> PlotJuggler/PJ4-cloud@cloud (nested plotjuggler_sdk -> plotjuggler_sdk-cloud@cloud);
pj-official-plugins/ -> pristine PlotJuggler/pj-official-plugins@317ea24 (sibling of PJ4).
git submodule status now records the exact upstream-tracked commit; sync is a
git merge per spec 2026-06-12-pj4-fork-submodule-vendoring-design.md §8."
```
Expected: clean commit; `git submodule status` shows pinned commits.

- [ ] **Step 2: Update `CLAUDE.md` — vendoring model + sync workflow + paths**

Rewrite the "Working tree vs pristine reference" and "Commands" sections: PJ4/plugins/SDK are now **private fork submodules** (clone needs `git submodule update --init --recursive` + org credentials); the connector plugin lives at `plugin/toolbox_dexory_cloud`; document the §8 `git fetch upstream && git merge <tag>` sync workflow and the `_pre_submodule` rollback tag. Update every `PJ4/pj-official-plugins/toolbox_dexory_cloud` reference to `plugin/toolbox_dexory_cloud`.

Run:
```bash
git add CLAUDE.md
git commit -m "docs: CLAUDE.md — submodule vendoring model, sync workflow, plugin path"
```
Expected: clean commit. Docs match reality (the freshness-discipline gate).

- [ ] **Step 3: Final state check**

Run:
```bash
git submodule status --recursive
gh repo view PlotJuggler/PJ4-cloud --json visibility -q .visibility            # PRIVATE
gh repo view PlotJuggler/plotjuggler_sdk-cloud --json visibility -q .visibility # PRIVATE
git log --oneline -6
```
Expected: pinned submodules; both forks `PRIVATE`; the migration commits present. **Do NOT push** — pushing `pj-mcap-server` is a separate, explicit user decision (the local clone is already ahead of origin).

---

## Self-review notes (coverage vs spec)

- Spec §3 decisions → Tasks 1-2 (forks, private gate), 3 (plugin home), 5-6 (plugins sibling/pristine). ✓
- Spec §5 deltas → Task 1 Step 3 (9 files), Task 2 Step 3 (18+1 files), Task 5 Step 1 (plugins residual). ✓
- Spec §6 plugin relocation (4 reach-outs + conanfile SDK pin) → Task 3 Steps 1-3. ✓
- Spec §7 migration flow → Tasks 0,1,2,6. ✓
- Spec §8 sync workflow → documented in Task 9 Step 2 (CLAUDE.md). ✓
- Spec §9 private-submodule access → noted in Task 9 Step 2 (clone credentials). ✓
- Spec §10 rollback (`_pre_submodule`) → Task 0 Step 1. ✓
- Spec §11 gates → Task 8 (smoke/matrix/app) + Task 9 Step 3 (visibility, submodule status). ✓

**Outward-facing/irreversible actions requiring stop-and-surface at execution:** creating the GitHub repos (Tasks 1-2-5), pushing fork branches (Tasks 1-2), and the `PJ4/` removal (Task 6). None push `pj-mcap-server` to origin (Task 9 Step 3 explicitly defers that).
