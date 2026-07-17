# Cloud connector — WASM decode-core compile path (M2c-DEX "WASM bonus")

This directory holds the **reproducible Emscripten build that compiles the as-built
client protocol/decode core to WebAssembly**, the smoke that runs it under node, and
the honest record of the transport/TLS/decode spike findings.

It exists to satisfy the unified plan's **M2c-DEX** deliverable and **risk 8**:

> "WASM bonus: client-core->WASM ... a CI job that **at minimum COMPILES client-core
> to WASM so the path cannot silently rot**."
>
> "Keep WASM a **demo (not gating)** deliverable; add a compile-to-WASM CI job; run a
> transport/TLS/decode spike before the demo; **surface the constraints** rather than
> asserting unmodified."

This is **not** a functional browser demo. The PlotJuggler WASM UI itself is an
**external** work stream (M2 scope) and is out of scope for this repo. What lives here
is exactly: (a) a reproducible build that compiles the pure protocol/decode core to
`.wasm`, (b) a non-gating CI job that runs it (`.github/workflows/wasm.yml`), and (c)
this constraints document.

## What IS proven (the compiled + run core)

`build.sh` compiles the **pure, dependency-light, socket-free** units of the as-built
client into one `.wasm` and runs a smoke under node. All of these compile under the
project's full `-std=c++20 -O2 -Wall -Wextra -Werror` bar and **execute correctly in
wasm32-emscripten**:

| unit | what it is |
|---|---|
| `src/session_key.hpp` | SessionKey normalization + FNV-1a hash (Plan B T8a) |
| `src/session_cache.hpp` | COMPLETE-only LRU cache + existence-predicate semantics |
| `src/hierarchy_prefix.h` | `/`-prefix hierarchy derivation (D8 client half) |
| `src/stitch_select.h` | stitched multi-file selection merge + overlap validation (Slice 7) |
| `tools/cli_url_resolve.hpp` | URL/token precedence resolution (D12) |
| `third_party/zstd/zstddeclib.c` | official single-file zstd **decoder** amalgamation (1.5.7) |

The smoke (`wasm_smoke_main.cpp`) re-runs the exact native unit-test assertions for the
pure logic AND decodes a **real ZSTD frame** — produced by the native conan libzstd
1.5.7 encoder at build time (`gen_test_frame.cpp`) — through a decode path that is
**byte-identical to `session_decode.cpp`'s `zstdDecodeAll`** (`ZSTD_getFrameContentSize`
+ `ZSTD_decompress`, then verify `ZSTD_isError`). The frame layout (content-size header,
one-shot per batch) is exactly what the v1 server emits for `BODY_ENCODING_ZSTD` batch
bodies (design spec §6.4). It also asserts a corrupt frame is rejected cleanly. This is
genuine **cross-build functional evidence** (native encode → wasm decode), not a
self-fulfilling round-trip.

Run it:

```bash
cd PJ4/pj-official-plugins/toolbox_dexory_cloud/wasm
./build.sh            # builds + runs under node; last line: WASM BUILD PASS
./build.sh --no-run   # compile-only
EMSDK=/path/to/emsdk ./build.sh   # if emcc is not already on PATH
```

The script is idempotent (cleans `build/` each run), auto-sources a discoverable
user-local emsdk (`$EMSDK`, `~/emsdk`, `~/.local/emsdk`, `wasm/.emsdk`) if `em++` is not
already on PATH, and prints `WASM BUILD PASS` / `WASM BUILD FAIL: <reason>` as its last
line (exit code matches). Verified locally: emsdk 5.0.4, node 22.16.0 — the smoke prints
`WASM SMOKE PASS` and the produced `.wasm` is ~146 KB.

## What is NOT proven here (the constraints — risk-8 spike findings)

These are the honest reasons the **whole** client cannot be compiled to a browser binary
today. They are constraints to **surface**, not problems hidden behind an "unmodified
client-core runs in the browser" claim.

### 1. Transport: ixwebsocket cannot target the browser (the core constraint)

`src/backend_connection.*` — the as-built WS transport — is built on **ixwebsocket**
(raw TCP sockets) and **`std::thread`**. Neither exists in a browser sandbox: there are
no raw sockets, and the main thread cannot block on a socket read. So the transport is
**excluded** from the wasm core.

A browser build needs a **future JS-WebSocket binding**: the Emscripten WebSocket API
(or `Module.WebSocket` feeding a JS `WebSocket`) on the JS side, queuing inbound frames
into the existing decode core. The decode core compiled here is precisely the part that
such a binding would feed; the socket plumbing is the part that must be rewritten for the
browser. **This is the transport impossibility risk 8 asks to surface — it is not a bug,
it is the architecture of browser networking.**

### 2. TLS / `--insecure`: not translatable, and absent from the wasm path

TLS belongs to the transport layer (`src/tls_utils.h`, the CLI `--insecure` /
`allow_insecure` self-signed-cert handling), which is excluded. In a browser, `wss://`
TLS is handled by the **browser's own WebSocket stack**, not by client-core. There is no
way to express "trust this self-signed cert" (`--insecure`) from page JS — the browser
enforces its own trust store. So **no TLS code is compiled for wasm**, and the native
cert handling is irrelevant to the browser path. A browser deployment must use a properly
trusted `wss://` endpoint.

### 3. Protobuf: GO to compile, NO-GO to link (so excluded from the gating job)

`session_decode.cpp` and `wire_mapping.cpp` depend on the generated `pj_cloud.pb.*`
(full protobuf runtime — the proto has **no** `optimize_for = LITE_RUNTIME`).

- **Compile**: PROVEN. Both `.cpp` files compile to wasm objects with zero errors (only
  a harmless `RepeatedPtrField`-deprecation warning from abseil under emscripten). The
  build script runs this as a **non-gating compile-to-object tracking step** (skipped
  silently when the generated proto / conan headers are absent, e.g. a fresh checkout
  without a native build) so source rot in these units is still caught.
- **Link**: NO-GO for this slice. A no-lib link yields ~20 undefined `google::protobuf::*`
  / `absl::lts_*` symbols. Linking requires **cross-building libprotobuf + abseil for
  wasm32-emscripten**, which was time-boxed out (multi-hour build; conancenter's emsdk
  tops out at 3.1.73, so a wasm conan profile is needed). It is **not run** here.

So the **gating wasm target EXCLUDES the protobuf-dependent TUs** and compiles only the
pure set above. Two clean follow-ups (both documented as **future work, NOT done here**):
1. cross-build **protobuf-lite** (`libprotobuf-lite.a` ≈ 1.3 MB vs full 7.4 MB) + abseil
   via a wasm conan profile, then link `session_decode.cpp` / `wire_mapping.cpp` — the
   real browser wire-decode path;
2. adding `option optimize_for = LITE_RUNTIME` to the proto would shrink the runtime but
   is a **PROTO CHANGE** — forbidden this slice (the proto is the frozen wire schema).

### 4. zstd: decoder only (encoder is not available for wasm)

The wasm core links the **official single-file zstd DECODER amalgamation**
(`third_party/zstd/zstddeclib.c`), generated from the **zstd 1.5.7** source
(`build/single_file_libs/create_single_file_decoder.sh`, "Combine script: PASSED") so it
matches the native pin (`conanfile.py`: `zstd/1.5.7`). It can **decode** (verified
byte-for-byte against a native-encoder frame) but cannot **encode** (no `ZSTD_compress`).
A browser decode core only ever decodes inbound batches, so this is sufficient. The
public `zstd.h` + `zstd_errors.h` (also 1.5.7) are shipped alongside so
`session_decode.cpp`'s `#include <zstd.h>` resolves unchanged.

Provenance / regeneration:

```bash
curl -sSL -o zstd-1.5.7.tar.gz \
  https://github.com/facebook/zstd/releases/download/v1.5.7/zstd-1.5.7.tar.gz
# sha256 = eb33e51f49a15e023950cd7825ca74a4a2b43db8354825ac24fc1b7ee09e6fa3
tar xzf zstd-1.5.7.tar.gz && cd zstd-1.5.7/build/single_file_libs
sh ./create_single_file_decoder.sh   # -> zstddeclib.c (decoder-only)
# then vendor zstddeclib.c + ../../lib/zstd.h + ../../lib/zstd_errors.h here.
```

### 5. LZ4: compiled out of the wasm build (additive `__EMSCRIPTEN__` guard)

`session_decode.cpp`'s legacy-singleton inbound LZ4 branch (`<lz4frame.h>`) is **not**
covered by the zstd decoder amalgamation, and the v1 server **never emits LZ4**. Rather
than link lz4 for wasm, that branch is guarded out under `#ifdef __EMSCRIPTEN__`:

- the `<lz4frame.h>` include and `lz4DecodeAll()` are `#ifndef __EMSCRIPTEN__`;
- the `PAYLOAD_ENCODING_LZ4` case rejects cleanly (`"LZ4 payload decode not available in
  wasm build"`) under wasm — defensive parsing, never a silent mis-decode.

**This guard is additive and native-safe.** `__EMSCRIPTEN__` is undefined on every native
build, so the native preprocessor takes the original branches verbatim — the native
plugin `.so`, the `dexory-cloud-cli`, the ctest suite (incl. `DexoryCloudSessionDecodeTest`),
`make smoke`, and `make matrix` are **byte-for-byte behaviorally unchanged** (verified:
native `./build.sh toolbox_dexory_cloud` green under full `-Werror`, ctest 31/31 hermetic,
`SMOKE PASS`).

### 6. MCAP writer: out of scope for the browser decode core

`tools/session_download.cpp`'s vendored Foxglove MCAP writer compiles to a wasm object
(MEMFS works) but needs the zstd **encoder** + lz4 to link, and it is a **CLI
file-reconstruction** concern. A browser UI renders decoded scalars directly; it does not
write MCAP files. Excluded.

### 7. Main-thread decode CPU (future workerization)

The decode core runs synchronously. A real browser build of the wire path would decode on
the main thread, which can jank the UI for large sessions. The future-work shape is a
**Web Worker** running the decode core, posting decoded scalars back to the UI thread
(mirrors the native `FetchWorker`'s off-UI-thread discipline). Not addressed here — noted
so the demo work stream plans for it.

## CI placement (non-gating by construction)

`.github/workflows/wasm.yml` is a **separate workflow** (`wasm-compile` job) that runs
this script on push/PR via the pinned `mymindstorm/setup-emsdk@v14` action (emsdk
`3.1.73` — newest on conancenter; the decode-core API surface is stable across 3.1.x and
the dev box's 5.x), uploads the `.wasm` artifact, and is bounded at 20 min.

It is **not** in branch-protection's required checks — a wasm break never blocks a merge.
Its sole purpose is "the compile path cannot silently rot." The native plugin build,
plugin ctest (both modes), `make smoke`, `make matrix`, and the Go server CI (`ci.yml`)
are untouched.

## Files

| file | role |
|---|---|
| `build.sh` | the reproducible build (native frame gen → wasm decoder obj → wasm smoke → node run) |
| `wasm_smoke_main.cpp` | the decode-core smoke entry point (pure-logic assertions + real ZSTD decode) |
| `gen_test_frame.cpp` | native (host) helper: compress a known plaintext → embed frame + plaintext as a header |
| `third_party/zstd/zstddeclib.c` | vendored zstd 1.5.7 single-file **decoder** amalgamation |
| `third_party/zstd/zstd.h`, `zstd_errors.h` | vendored zstd 1.5.7 public headers |
| `build/` | generated outputs (gitignored) |
