# Execution approach — two endpoints first, then the plumbing

| | |
|---|---|
| Date | 2026-06-04 |
| Status | ACTIVE — this is the current build order; it sequences (not replaces) the plans |
| Decided by | User, 2026-06-04 |

## The approach

Instead of marching straight through spec §13 (proto → server → client), we stand up the
two **ends** of the pipeline first, then fill in the middle:

```
[START endpoint]                      [the plumbing]                       [END endpoint]
Dexory Cloud plugin     ←—  Go server + wire protocol + client-core  —→   Minio (local S3)
(1:1 Mosaico UI shell)        (Plans A/B/C, largely autonomous)           bucket: recordings
```

1. **START — `toolbox_dexory_cloud`** (exists): a new PJ4 Toolbox plugin that is a
   **one-to-one visual copy of `toolbox_mosaico`** — same three-panel dialog, Lua
   metadata query engine, filters, date picker, range slider, settings persistence,
   server history, cert dialog — with **all Apache Arrow / Arrow Flight / gRPC /
   `mosaico_sdk` usage removed** and the transport replaced by an **inert stub**
   (Connect reports "backend not implemented"; panels render but stay empty). It lives
   in the **vendored** tree at `PJ4/pj-official-plugins/toolbox_dexory_cloud/` — NOT in
   the official upstream repos. Mosaico itself is untouched and coexists.
2. **END — Minio** (exists): `infra/minio/docker-compose.yml` — local S3-compatible
   store, bucket `recordings`, identifiers pinned to Plan C. Synthetic MCAP data gets
   seeded later via Plan C's `gen-fixtures`.
3. **PLUMBING** (next, largely autonomous once both ends exist): Plans A → B → C as
   written (proto → Go server against Minio → `client-core` → round-trip harness),
   then swap the plugin's inert stub for `client-core` behind the same worker seam.

## Why this order

Both endpoints are **independently verifiable today**: the plugin proves the 1:1 UI in a
real PJ4 build before any protocol exists; Minio proves the storage contract without AWS.
The plumbing between them is the well-specified part (the plans are written for agentic
execution) — with both ends fixed, it can proceed with minimal supervision.

## Vendored working tree

PJ4 + pj-official-plugins were **copied into this repo** (source-only: no `build/`, no
`.git`, no `.qt` — the Qt toolchain is a gitignored symlink to the original):

| Tree | Role |
|---|---|
| `<repo>/PJ4/` (= `/home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4/`) | **The working tree.** All PJ4/plugin changes happen here. |
| `/home/gn/ws/PJ4` | Pristine upstream. **Read-only reference — never modify.** |

Build the plugin: `cd PJ4/pj-official-plugins && ./build.sh toolbox_dexory_cloud`.
Run it inside PJ4: **the vendored app** (`cd <repo>/PJ4 && ./run.sh` — its run.sh
auto-loads the vendored plugins from `build/all/Release/bin`). **[AMENDED 2026-06-10]**
The original guidance ("use the original tree's built app") is superseded: the vendored
tree now carries host-side changes (SDK parser-ingest tail slots, RangeSlider markers,
widget bindings), so the pristine app is a *different program* — plugin features load
into it but host features silently vanish. Never run the pristine app for this project.

## Plugin-shape note (deviation from Plan D, deliberate)

Plan D (`2026-06-03-pj-cloud-pj4-plugin.md`) specifies the final `pj_cloud` plugin as a
**DataSource** (`FileSourceBase` + embedded dialog + delegated ingest). The Dexory Cloud
endpoint built now is a **Toolbox** (`ToolboxPluginBase`), because it is a literal Mosaico
copy and the goal is a pixel-faithful UI shell immediately.

**RECONCILED 2026-06-04 (Slice 3 grounding — hard SDK evidence, do not re-litigate):**
the raw→MessageParser dispatch (`ensureParserBinding`/`pushMessage`,
`data_source_host_views.hpp:191/:241`) is reachable ONLY via the `"pj.runtime.v1"`
service, which `ToolboxRuntimeHost::registerServices` never registers for a Toolbox
(`pj_runtime/src/ToolboxRuntimeHost.cpp:31-35` — only toolbox_write/toolbox_runtime/
settings). The toolbox write surface (`plugin_data_api.hpp:1012-1239`) has no parser
path; Mosaico is no counterexample (its server shipped pre-decoded Arrow). Therefore
**Plan D's DataSource migration is REQUIRED for full-topic ingest**, not a stylistic
choice. Interim shipped in Slice 3: `std_msgs/msg/Float32` topics ingest via the
toolbox scalar API (hard-gated fixed-layout read, one schema — not a decoder); all
other topics surface *"requires parser integration (Plan D)"* per-topic in the dialog.

## The headless SDK harness (added 2026-06-04, after Slice 1)

Before further backend work, the plumbing was made testable **without the GUI**:
`make smoke` orchestrates Minio → its own server instance (`:8081`) → ground-truth
assertions through **both** client stacks (`dexory-cloud-cli`, a plain-C++ driver over
the plugin's real `BackendConnection`, and the Go `devprobe`) → the plugin's hermetic +
live ctest suites. **Every backend slice from here on is built against this harness and
must keep `make smoke` green** (see CLAUDE.md § "The headless SDK harness"). The GUI
becomes a presentation layer over an independently verified SDK path.

## What "available endpoints" unlock (the autonomous stretch)

With the UI shell and the bucket fixed, the remaining work is bounded by the plans:

- Plan A Tasks 1–6 (proto + config) → Tasks 7–46 (server) against `infra/minio`.
- Plan B (client-core + CLI) against the running server.
- Plan C (fixtures → seed Minio → round-trip matrix).
- Swap `toolbox_dexory_cloud`'s inert `BackendConnection` stub for `client-core`
  (the worker-thread/command-queue/onTick seam was preserved exactly for this).
