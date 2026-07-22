# toolbox_mcap_cloud — PlotJuggler cloud connector plugin

A PlotJuggler 4 **Toolbox** plugin that browses MCAP recordings served from a
cloud bucket (via the self-hosted *PJ Cloud Connector* Go server) and fetches
them on demand into the PlotJuggler datastore. It is the **START endpoint** of
the connector pipeline (server → WebSocket/Protobuf → this plugin).

> **Shape note.** The cloud connector **IS a cloud TOOLBOX** (a Mosaico-style
> non-modal panel: browse catalog, Lua-filter, select sequences + topics +
> time-range, Fetch). It is *not* a Streaming/File DataSource. This is a
> deliberate, verified product decision — see the "Decisions & pins" section in the
> repo-root `CLAUDE.md`. Plan D's DataSource shape is superseded for now.

## What it does

- Connects to a PJ Cloud server over a single WebSocket multiplexing catalog RPCs
  and bounded-horizon session streaming (incremental download, not wall-clock
  playback).
- Lists sequences (cloud MCAP files), their topics, time ranges, sizes, and
  metadata/tags; supports a Lua metadata query filter.
- Selects N consecutive sequences and presents them as one **stitched** logical
  session (union of topics, one continuous time range).
- Fetches the selection and ingests decoded scalar series into the datastore.
  ROS2/CDR messages are decoded **in the plugin** (see "Decoder rationale").
- Edits a file's override **tags** (the server keeps an effective-tags view).
- Survives a mid-download WebSocket drop via reconnect-and-resume; repeat fetches
  of the same selection are served from an in-memory session cache.

## Build

The plugin builds **standalone** in this repo (Conan 2 + CMake, C++20, `-Werror`).
It requires the `plotjuggler_sdk/0.11.0` Conan package in your local cache — if it
is missing, the repo-root `./build.sh` prints the exact `conan create` command to
build it from the `plotjuggler_sdk-cloud` checkout.

Easiest — from the **repo root**, build the server + this plugin and stage the `.so`:

```bash
./build.sh
```

Or build just the plugin:

```bash
cd plugin/toolbox_mcap_cloud
conan install . --output-folder=build --build=missing -s compiler.cppstd=20
cmake -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

Artifacts land under `plugin/toolbox_mcap_cloud/build/bin/`:

- `libtoolbox_mcap_cloud_plugin.so` — the plugin (toolbox + borrowed dialog).
- `mcap-cloud-cli` — the headless CLI (below).

Run PlotJuggler pointed at that build directory. Use a PlotJuggler build that carries
the cloud host-side changes (SDK parser-ingest tail slots, RangeSlider markers, widget
bindings) — on this machine that is `~/ws_plotjuggler/PJ4`; a pristine upstream app
loads the `.so` but those host features silently vanish:

```bash
PJ_PLUGIN_DIR="$PWD/plugin/toolbox_mcap_cloud/build/bin" ~/ws_plotjuggler/PJ4/run.sh
```

## CLI — `mcap-cloud-cli`

A headless command-line driver over the **exact** `BackendConnection` class the
GUI uses (no parallel transport), so connectivity + catalog + download
correctness can be exercised without launching the GUI. It links **zero Qt**
(ixwebsocket transport; enforced by the `McapCloudCliNoQtGuard` ctest).

| Verb | Purpose |
|---|---|
| `hello` | connect + print server version and the server's `BackendCapabilities` (file-hierarchy flag, metadata-key vocabulary) |
| `list [--json]` | sequences: name, time range, size, message count, metadata |
| `topics <sequence> [--json]` | per-topic name, schema, encoding, message count |
| `download <seq1> [<seq2> …] --output out.mcap [--topics a,b] [--time-range s,e] [--json]` | open a session and reconstruct a local MCAP (multiple sequences are stitched, time-ordered) |
| `tag <sequence> [--set k=v]… [--unset k]… [--json]` | edit override tags, then print refreshed effective metadata |
| `debug <seq1> [<seq2> …] [--topics a,b] [--time-range s,e] [--limit N] [--json]` | open a session and print the first N decoded messages (topic, log_time, payload size) **without writing a file** |

Exit codes: `0` success · `1` connection/RPC failure · `2` usage error.

### Environment variables and flags

| Variable | Flag | Default | Meaning |
|---|---|---|---|
| `MCAP_CLOUD_URL` | `--url URL` | `ws://localhost:8080` | WS base URI (`ws://` or `wss://`) |
| `MCAP_CLOUD_API_KEY` | `--token TOKEN` | *(empty)* | bearer token; empty = dev anonymous |
| — | `--insecure` | off | `wss://`: skip TLS cert verification (self-signed dev certs) |

Precedence is **explicit flag > environment > built-in default**, implemented by
the pure resolver in `tools/cli_url_resolve.hpp` and pinned by the
`McapCloudCliUrlResolveTest` unit test (so `MCAP_CLOUD_URL` is honored when
`--url` is absent, and `--url` overrides the env).

## Tests (ctest)

```bash
ctest --test-dir plugin/toolbox_mcap_cloud/build            # hermetic: live tests SKIP
MCAP_CLOUD_LIVE_URL=ws://localhost:8081 \
  ctest --test-dir plugin/toolbox_mcap_cloud/build          # live: all run
```

Two modes, gated by `MCAP_CLOUD_LIVE_URL`:

- **Hermetic** (default, no server): unit/header tests, wire-mapping and session
  decode round-trips, parser-ingest driver contract, the plugin-load smoke
  (`McapCloudPluginLoadSmokeTest` — dlopens the built `.so` and asserts both
  the toolbox and dialog entry vtables resolve), the no-Qt guard, and the
  URL-resolution unit test. Live tests self-**skip**.
- **Live** (`MCAP_CLOUD_LIVE_URL` set to a running server, e.g. the `make
  smoke` harness on `:8081`): everything above plus the live transport/session/
  resume tests against the seeded corpus, asserting exact ground-truth message
  counts (pinned in lockstep with `scripts/smoke.sh` and
  `tests/backend_connection_live_test.cpp`).

## Ingest path — host-delegated parsing

Parsing is delegated to the **host's MessageParser plugins** via the toolbox
parser-ingest tail slots (`create_parser_ingest` / `release_parser_ingest` +
the data-source runtime `ensure_parser_binding` / `push_message`) — the SDK
0.11.0 toolbox parser-ingest path implemented in `src/parser_ingest_driver.*`.
tf/pointclouds/images arrive as ObjectStore object topics with render-time
parsers registered by the host (3D-draggable AND renderable). The plugin
contains **zero message decoders**; decode correctness is owned by
`pj_runtime` (`ToolboxRuntimeHostTest`, `ToolboxParserIngestRealRosTest`).

## Deferred / not as-built

The original Plan D (archived — recover from git history) mentioned a DataSource
shape, `qtkeychain`-backed secret storage, a `pjcloud://` URI scheme, and
file-hierarchy browsing. None are as-built: the plugin is a Toolbox, secrets are
per-URI tokens supplied via env/flag, and the catalog is flat (the server reports
`supports_file_hierarchy=false`). These remain deferred.

## Pointers

- Repo-root `CLAUDE.md` — project handbook, decisions & pins, slice history.
- `arch/2026-05-28-pj-cloud-connector-design.md` — canonical design spec (wire
  protocol, sessions, resume, testing).
