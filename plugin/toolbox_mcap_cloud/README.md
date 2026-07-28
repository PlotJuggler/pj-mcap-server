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
- Browses the catalog through a required customer/site gate (see "Browse
  flow" below), then lists that site's sequences (cloud MCAP files), their
  topics, time ranges, sizes, and metadata/tags; supports a Lua metadata
  query filter over the loaded rows.
- Selects N consecutive sequences and presents them as one **stitched** logical
  session (union of topics, one continuous time range).
- Fetches the selection and ingests decoded scalar series into the datastore.
  ROS2/CDR messages are decoded **in the plugin** (see "Decoder rationale").
- Optionally reconstructs every fetched session as one local, chunked +
  Zstd-compressed MCAP while it is ingested.
- Edits a file's override **tags** (the server keeps an effective-tags view).
- Survives a mid-download WebSocket drop via reconnect-and-resume; repeat fetches
  of the same selection are served from an in-memory session cache.

## Browse flow: the customer/site gate

Connecting to a production server does **not** fetch the file list -- a real
catalog can hold tens of thousands of files, and pulling all of them up front
made the browse table sit blank for a long time. Instead:

- On connect the plugin fetches only the **filter vocabulary**
  (`GetVocabulary`): a customer -> site tree with a per-node file count and a
  catalog generation. Nothing else loads yet.
- The **Customer** and **Site** combos are the first two rows of the Basic
  filter grid (above Robot/Source, sharing their column alignment), and they
  stay visible in **Advanced** mode too — only the Robot/Source rows hide with
  the mode swap, so the mandatory gate is always reachable. The customer combo
  auto-selects when the vocabulary has exactly one customer; otherwise the user
  must pick one. No recordings load until a customer AND a site are both
  chosen.
- Picking a site issues a **server-filtered** `ListFiles` request
  (`FileFilter.customer_id`/`site_id`, with the vocabulary's generation echoed
  on the first page) and renders it **progressively** -- the table fills in
  as pages arrive rather than only after the whole sweep completes.
- The (customer, site) selection persists **per server**, keyed by the
  canonical `ws://`/`wss://` server URI: a returning user reconnects straight
  into their last site. A one-shot migration seeds this from the old global
  filter settings the first time, then clears them.
- The robot/source combos, the date filter (default **All**), the free-text
  name filter, and the Lua/Advanced query are UNCHANGED: they keep narrowing
  the rows already loaded for the selected site, client-side.
- The old "(any)/(any)" merged-all-sites view is removed by design --
  browsing is always scoped to one customer + site.
- `mcap-cloud-cli` is unaffected: `list` still returns the full, unfiltered
  catalog (there is no gate on the CLI).

Measured on a real 25,550-file catalog: the vocabulary fetch takes 167 ms; a
473-file site loads completely in 147 ms; a 14,480-file site shows its first
rows in 127 ms and completes in 3.7 s. The previous unfiltered browse of the
whole catalog took about 11.9 s with a blank table the entire time.

Empty-state pill (one message shown at a time, in the table's grid cell, so
the state is always explicit): disconnected; vocabulary loading (silent);
vocabulary error; empty catalog; needs selection (with the corpus size); list
loading (silent); list error; no matches for the current selection.

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

## Exporting Toolbox downloads

The **Export MCAP** checkbox is **off by default** (an involuntary full-session
file per fetch is not a sane default; enabling it is persisted per user as
`mcap_cloud/export_*`). The adjacent **Directory** field defaults to
PlotJuggler's per-user data directory under `mcap_cloud/downloads`.

Each Download click creates one collision-safe file. A single recording uses
`<source>_download_<UTC>.mcap`; stitched selections use
`<first-source>_plus_<N>_download_<UTC>.mcap`. The worker reserves and writes a
sibling `<name>.mcap.partial` (never `*.mcap`-suffixed — a truncated file must
not look loadable) while data is arriving and renames it only after a clean
session completion. Cancellation or a transport drop retains the finalized,
readable partial and reports its path; a disk/write failure removes the
unreadable partial and reports the cause. **The export is strictly secondary:
no export failure ever aborts the download or the imported data.** With the
checkbox enabled, repeat fetches bypass the count-only session cache (the
export needs the raw bytes); disable it to keep cache hits.

The reconstructed file contains the session protocol's messages, schemas,
channels, and log/publish timestamps. Attachments and source MCAP metadata are
not transmitted by the server and therefore cannot be reconstructed.

### Environment variables and flags

| Variable | Flag | Default | Meaning |
|---|---|---|---|
| `MCAP_CLOUD_URL` | `--url URL` | `ws://localhost:8080` | WS base URI (`ws://` or `wss://`) |
| `MCAP_CLOUD_API_KEY` | `--token TOKEN` | *(empty)* | bearer token; empty = dev anonymous |
| — | `--insecure` | off | `wss://`: skip TLS cert verification (self-signed dev certs) |
| `MCAP_CLOUD_CACERT` | `--cert FILE` | *(auto-detect)* | `wss://`: CA bundle verifying the server; only needed for a private CA |
| `SSL_CERT_FILE` | — | *(unset)* | `wss://`: standard override consulted before the built-in path list |

### `wss://` and certificate discovery

**A publicly-trusted `wss://` server needs no flags at all** — just `--url` and
`--token`. The CA bundle is located automatically by `detectSystemCaBundle()`
(`src/tls_utils.h`), which consults, in order:

1. `SSL_CERT_FILE`, when it points at a readable file;
2. the well-known per-distro bundles — `/etc/ssl/certs/ca-certificates.crt`
   (Debian/Ubuntu/Alpine), `/etc/pki/tls/certs/ca-bundle.crt` (Fedora/RHEL),
   `/etc/ssl/ca-bundle.pem` (openSUSE),
   `/etc/ca-certificates/extracted/tls-ca-bundle.pem` (Arch), `/etc/ssl/cert.pem`;
3. ixwebsocket's `"SYSTEM"` keyword as a last resort.

Doing this ourselves is **not** optional. ixwebsocket's `caFile = "SYSTEM"` default
implements system-certificate loading on Windows only; every other platform takes
a `return false` branch that never assigns an error string, so the whole handshake
fails as the misleading `error: no error`. Step 3 exists purely because that
default *is* correct on macOS/Windows.

`SSL_CERT_DIR` is deliberately ignored: it names a hashed-symlink *directory*, and
ixwebsocket only ever calls `mbedtls_x509_crt_parse_file()` on `caFile`, never
`mbedtls_x509_crt_parse_path()`.

`--insecure` disables verification via `caFile = "NONE"` **and nothing else**.
ixwebsocket gates SNI on `disable_hostname_validation`, so setting that flag would
stop sending SNI entirely and any SNI-dependent TLS front-end (Tailscale Serve,
Cloudflare, shared-IP terminators) would abort the handshake with a fatal alert.

**AppImage note:** an AppImage is a self-mounting SquashFS, not a sandbox, so the
host's `/etc` is visible and the path list above resolves normally. (PlotJuggler's
own `AppRun` probes the same paths to set `GRPC_DEFAULT_SSL_ROOTS_FILE_PATH`.) On
a distro with none of these layouts — NixOS, minimal containers — export
`SSL_CERT_FILE`, or pass `--cert` / set the GUI's certificate field.

In the GUI plugin the same three inputs live behind the connect row's
**"Cert / API Key…"** button (`ui/cert_dialog.ui`): certificate path (leave EMPTY
for auto-detect), API key, and an "allow insecure" checkbox.

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
