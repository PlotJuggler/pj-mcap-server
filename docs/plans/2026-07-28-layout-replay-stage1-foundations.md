# Layout-Replay Stage 1 — Plugin Foundations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps
> use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the no-host-dependency plugin foundations of the canonical-layout-replay
v3.3 design (`docs/canonical-layout-replay.md` §9/§13 stage 1): the canonical replay
descriptor + digest with shared cross-repo vectors, the `sendAndWait` cancel fix, credential
origin binding, the trusted-origin ledger, the writer seams for cache duty, and the
request-addressed session file cache core.

**Architecture:** All work is inside `plugin/toolbox_mcap_cloud/` on branch
`feat/layout-replay-foundations` (worktree `.worktrees/layout-replay-foundations`). Every
task is a self-contained module + hermetic ctest additions; nothing here touches the PJ4
host or the SDK pin. The spec is the authority — where this plan and the spec disagree,
the spec wins and the plan gets fixed.

**Tech stack:** C++20, CMake + Conan 2 (deps already in `conanfile.py`: nlohmann_json,
mcap 2.1.1, zstd, lz4, gtest, ixwebsocket, protobuf). `-Wall -Wextra -Werror`. Tests:
gtest via ctest, hermetic (no network, no env-gated additions in this plan).

**Working directory for all commands:**
`/home/davide/ws_plotjuggler/mcap_server/.worktrees/layout-replay-foundations/plugin/toolbox_mcap_cloud`

**One-time build bootstrap (Task 0):**
```bash
conan install . --output-folder=build --build=missing -s compiler.cppstd=20
cmake -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)" && ctest --test-dir build --output-on-failure
```
Expected: configure + build clean, **41/41 tests pass** (the count on main after PR #4).
Commit nothing for Task 0.

---

### Task 1: Vendored SHA-256 (`src/core/sha256.{h,cpp}`)

The digest needs SHA-256 with zero dependency ambiguity (mbedtls arrives only transitively
via ixwebsocket and is not a declared dependency — do NOT link it). Vendor a minimal,
public-domain-style implementation.

**Files:**
- Create: `src/core/sha256.h`
- Create: `src/core/sha256.cpp`
- Create: `tests/sha256_test.cpp`
- Modify: `CMakeLists.txt` (new hermetic test target `toolbox_mcap_cloud_sha256_test`,
  registered as `McapCloudSha256Test`; copy the pattern of
  `toolbox_mcap_cloud_mcap_save_path_test`)

**API (exact):**
```cpp
// src/core/sha256.h
// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#pragma once
#include <array>
#include <cstdint>
#include <string>
#include <string_view>

namespace mcap_cloud {
/// Minimal FIPS 180-4 SHA-256 (vendored: the plugin declares no crypto
/// dependency; mbedtls is only a transitive detail of ixwebsocket).
[[nodiscard]] std::array<std::uint8_t, 32> sha256(std::string_view data);
/// Lowercase hex of the FIRST `bytes` bytes of sha256(data). bytes <= 32.
[[nodiscard]] std::string sha256HexPrefix(std::string_view data, std::size_t bytes);
}  // namespace mcap_cloud
```

- [x] **Step 1.1: Write the failing test** — `tests/sha256_test.cpp` with the FIPS/NIST
  vectors (these exact strings and digests):

```cpp
// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "core/sha256.h"
#include <gtest/gtest.h>
#include <string>

namespace {
std::string hex(const std::array<std::uint8_t, 32>& d) {
  static const char* k = "0123456789abcdef";
  std::string out;
  for (auto b : d) { out += k[b >> 4]; out += k[b & 0xF]; }
  return out;
}
}  // namespace

TEST(Sha256, NistVectors) {
  EXPECT_EQ(hex(mcap_cloud::sha256("")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  EXPECT_EQ(hex(mcap_cloud::sha256("abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  EXPECT_EQ(hex(mcap_cloud::sha256(
                "abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1");
  // One-million 'a' (streaming/block-boundary coverage).
  EXPECT_EQ(hex(mcap_cloud::sha256(std::string(1'000'000, 'a'))),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

TEST(Sha256, HexPrefix) {
  EXPECT_EQ(mcap_cloud::sha256HexPrefix("abc", 16),
            "ba7816bf8f01cfea414140de5dae2223");  // first 16 bytes = 32 hex chars
  EXPECT_EQ(mcap_cloud::sha256HexPrefix("abc", 32).size(), 64u);
}
```

- [x] **Step 1.2: Register the test target in CMakeLists.txt, run it, verify it FAILS to
  compile** (`sha256.h` missing). Run:
  `cmake -B build ... && cmake --build build --target toolbox_mcap_cloud_sha256_test`
- [x] **Step 1.3: Implement `src/core/sha256.cpp`** — straightforward FIPS 180-4: message
  schedule W[64], the standard K constants table, init vector H0..H7, 512-bit block loop,
  length padding. ~120 lines. No heap, no endian assumptions (assemble words with shifts).
- [x] **Step 1.4: Build + run:**
  `ctest --test-dir build -R McapCloudSha256Test --output-on-failure` → 2 tests PASS.
- [x] **Step 1.5: Commit** — `feat(replay): vendor FIPS 180-4 sha256 (digest foundation)`

---

### Task 2: Replay descriptor module (`src/replay_descriptor.{hpp,cpp}`) + shared vectors

The public, allowlisted, versioned descriptor (spec §4): parse/validate → canonical
serialization → identity digest. The **canonicalization vectors are a cross-repo
contract** (PJ4 will consume them byte-identically — CATALOG_CONTRACT discipline), so
they live in `docs/` at the repo root AND are read by the test.

**Files:**
- Create: `src/replay_descriptor.hpp`, `src/replay_descriptor.cpp`
- Create: `../../docs/replay-descriptor-vectors.json` (repo-root docs/, the contract copy)
- Create: `tests/replay_descriptor_test.cpp`
- Modify: `CMakeLists.txt` (test target `toolbox_mcap_cloud_replay_descriptor_test` /
  `McapCloudReplayDescriptorTest`; links nlohmann_json + gtest; sources:
  `src/replay_descriptor.cpp src/core/sha256.cpp`; pass the vectors path as a compile
  definition: `-DMCAP_CLOUD_VECTORS_JSON="${CMAKE_CURRENT_SOURCE_DIR}/../../docs/replay-descriptor-vectors.json"`)

**API (exact):**
```cpp
// src/replay_descriptor.hpp
#pragma once
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace mcap_cloud {

/// Spec docs/canonical-layout-replay.md §4. Timestamps are decimal strings on
/// the wire; parsed to int64 here. topics empty = all. "0"/"0" = whole range.
struct ReplayDescriptor {
  int version = 1;                       // "v"
  std::string kind;                      // "mcap-cloud-session"
  std::string server_uri;
  std::vector<std::string> s3_keys;      // order preserved (wire order)
  std::vector<std::string> topics;
  std::int64_t start_ns = 0;
  std::int64_t end_ns = 0;
  bool include_latched = true;
  std::string display_name;              // EXCLUDED from identity
};

/// Hard validation limits (spec §4/§7 resource guard, layer 1).
inline constexpr std::size_t kMaxDescriptorBytes = 64 * 1024;
inline constexpr std::size_t kMaxKeys = 512;
inline constexpr std::size_t kMaxTopics = 4096;
inline constexpr std::size_t kMaxStringBytes = 4096;

/// Parse + validate. Rejects: wrong v/kind, missing/mistyped fields, unknown
/// fields (allowlist!), over-limit sizes, non-ws/wss scheme, URI userinfo
/// ('@' before host), URI query ('?') or fragment ('#'), non-numeric ns
/// strings, end < start (unless both 0). Error is human-readable.
[[nodiscard]] std::optional<ReplayDescriptor> parseReplayDescriptor(
    std::string_view json, std::string* error);

/// Canonical serialization: the EXACT byte string the identity is computed
/// over. Sorted keys, compact (no whitespace), UTF-8, ns as decimal strings,
/// display_name OMITTED. Pinned by docs/replay-descriptor-vectors.json.
[[nodiscard]] std::string canonicalReplayJson(const ReplayDescriptor& d);

/// "mcap-cloud:v1:sha256/128:<32 lowercase hex>" over canonicalReplayJson.
[[nodiscard]] std::string replayIdentity(const ReplayDescriptor& d);

/// Serialize for embedding in a layout (canonical fields + display_name).
[[nodiscard]] std::string toReplayJson(const ReplayDescriptor& d);

}  // namespace mcap_cloud
```

**Implementation notes (binding):** build canonical output with `nlohmann::ordered_json`
populated in ALPHABETICAL key order explicitly (`end_ns, include_latched, kind, s3_keys,
server_uri, start_ns, topics, v`) then `dump()` — do NOT rely on map ordering implicitly;
the explicit insert order IS the contract. `toReplayJson` = same + `display_name` (in
alphabetical position). Parse with `nlohmann::json::parse(..., nullptr, false)` and reject
`is_discarded()`. Unknown-field rejection: iterate keys against the allowlist.

**Vectors file** `docs/replay-descriptor-vectors.json` (repo root; create with EXACTLY
this structure, then fill `canonical` and `identity` from the implementation ONCE and
hand-verify the first case with `python3 - <<'EOF'` + hashlib before committing):
```json
{
  "comment": "Cross-repo contract: canonical serialization + identity vectors for the replay descriptor (docs/canonical-layout-replay.md §4). PJ4-side tests must consume this file byte-identically. Regenerating requires bumping the descriptor version.",
  "cases": [
    { "name": "minimal-whole-range-all-topics",
      "descriptor": {"v":1,"kind":"mcap-cloud-session","server_uri":"ws://localhost:8080","s3_keys":["a.mcap"],"topics":[],"start_ns":"0","end_ns":"0","include_latched":true,"display_name":"A"},
      "canonical": "<filled in step 2.4>",
      "identity": "<filled in step 2.4>" },
    { "name": "stitched-window-custom-topics",
      "descriptor": {"v":1,"kind":"mcap-cloud-session","server_uri":"wss://mcap.example.com","s3_keys":["cust=x/site=y/2026/a.mcap","cust=x/site=y/2026/b.mcap"],"topics":["/imu","/tf","/tf_static"],"start_ns":"1780012345000000000","end_ns":"1780012399000000000","include_latched":true,"display_name":"Run 42"},
      "canonical": "<filled in step 2.4>",
      "identity": "<filled in step 2.4>" },
    { "name": "display-name-does-not-change-identity",
      "descriptor": {"v":1,"kind":"mcap-cloud-session","server_uri":"wss://mcap.example.com","s3_keys":["k.mcap"],"topics":["/a"],"start_ns":"5","end_ns":"9","include_latched":false,"display_name":"RENAMED"},
      "canonical": "<filled in step 2.4>",
      "identity": "<same as a twin case with display_name X — asserted in test>" }
  ]
}
```

- [x] **Step 2.1: Write failing tests** — `tests/replay_descriptor_test.cpp`:
  - Round-trip: parse(toReplayJson(d)) == d for a fully-populated descriptor.
  - Vector conformance: load `MCAP_CLOUD_VECTORS_JSON`, for each case parse
    `descriptor` (re-serialized compactly to feed the parser), assert
    `canonicalReplayJson` == `canonical` and `replayIdentity` == `identity`.
  - Identity invariance: two descriptors differing only in `display_name` →
    same identity; differing in `include_latched` → different identity.
  - Rejection matrix (each with a distinct error substring):
    `{"v":2,...}` (version) · missing `kind` · unknown field `"token":"x"` ·
    `server_uri:"wss://user:pw@h/"` (userinfo) · `"wss://h/?token=1"` (query) ·
    `"wss://h/#f"` (fragment) · `"http://h"` (scheme) · 513 keys (limit) ·
    `start_ns:"abc"` (non-numeric) · `end_ns:"5"` with `start_ns:"9"` (order) ·
    input larger than `kMaxDescriptorBytes`.
- [x] **Step 2.2: Register test target; verify compile FAILS** (missing header).
- [x] **Step 2.3: Implement** `replay_descriptor.cpp` per the notes above.
- [x] **Step 2.4: Fill the vectors**: temporarily print `canonicalReplayJson` +
  `replayIdentity` for the vector descriptors (a throwaway `TEST` or `std::cerr` in the
  test), paste into `docs/replay-descriptor-vectors.json`, **independently verify case 1**:
  `python3 -c "import hashlib;print(hashlib.sha256(open('canonical-bytes.txt','rb').read()).hexdigest()[:32])"`.
  Remove the throwaway printing.
- [x] **Step 2.5: Run** `ctest --test-dir build -R McapCloudReplayDescriptorTest` → PASS.
- [x] **Step 2.6: Commit** — `feat(replay): canonical descriptor module + cross-repo vectors`

---

### Task 3: `sendAndWait` cancel predicate (fix the 120 s cancel hang)

**Files:**
- Modify: `src/backend_connection.cpp` (the `sendAndWait` wait-predicate — currently waits
  on response/closed with `kOpenSessionTimeout` = 120 s, `backend_connection.hpp:355-371`
  area; `cancelSession()` wakes the FRAME wait but not this one — read both first)
- Modify: `src/backend_connection.hpp` (only if the cancel flag/cv needs exposing)
- Create: `tests/backend_cancel_wait_test.cpp`
- Modify: `CMakeLists.txt` (target `toolbox_mcap_cloud_backend_cancel_wait_test` /
  `McapCloudBackendCancelWaitTest`; copy the source list + links of the existing hermetic
  `toolbox_mcap_cloud_backend_error_test` — it already demonstrates a loopback/fake-server
  harness; REUSE that harness pattern, do not invent a new one)

- [x] **Step 3.1: Read** `src/backend_connection.cpp` `sendAndWait` + `cancelSession` and
  `tests/backend_error_test.cpp` (the existing hermetic harness). Identify the condition
  variable + predicate.
- [x] **Step 3.2: Write the failing test**: start the harness's fake server variant that
  ACCEPTS the WebSocket but never answers an `OpenFresh` request; call
  `openSessionFresh` on a worker `std::thread`; after 200 ms call `cancelSession()`;
  `join` with a deadline: assert the call returned within **2 s** with an error mentioning
  cancel (exact text chosen in 3.3), not after the 120 s timeout. Use
  `std::chrono::steady_clock` around the join.
  Run it: it must FAIL by timing out past the assertion bound (bound the test itself with
  the ctest `TIMEOUT 30` property so a regression fails fast rather than hanging CI).
- [x] **Step 3.3: Implement**: add cancellation to the wait predicate
  (`cv_.wait_for(..., [&]{ return response_ready || closed_ || cancel_requested_; })`)
  and make `cancelSession()` set the flag + `notify_all` on the same cv. Return a
  distinct error `"cancelled"` so callers keep their existing closed-vs-error handling.
  Check every OTHER `sendAndWait` caller (ListFiles/GetVocabulary/…) still behaves: a
  cancel outside a session must NOT wake browse RPCs spuriously — scope the flag to the
  session request path (member reset at request start, exactly like the frame-wait flag).
- [x] **Step 3.4: Run** the new test + the full suite: 41+new tests PASS.
- [x] **Step 3.5: Commit** — `fix(backend): cancel joins sendAndWait's wake predicate (no 120s hang)`

---

### Task 4: Credential origin binding (`src/origin_match.{hpp,cpp}`)

`MCAP_CLOUD_API_KEY` must only apply when `MCAP_CLOUD_URL`'s parsed origin equals the
target's (spec §7 guard 2). `normalizeServerKey()` stays untouched (it's a storage key,
not an origin parser).

**Files:**
- Create: `src/origin_match.hpp`, `src/origin_match.cpp`
- Modify: `src/mcap_cloud_dialog.cpp` (`resolveCredentials`, ~line 102: env-token branch
  becomes conditional on `sameOrigin(getenv("MCAP_CLOUD_URL"), uri)`)
- Modify: `tools/cli_url_resolve.hpp` (`resolveCliToken`: same rule — the env token
  applies only when the effective URL's origin matches `MCAP_CLOUD_URL`'s; when the user
  passed `--url` with a different origin, the env token is ignored)
- Create: `tests/origin_match_test.cpp`; Modify: `tests/cli_url_resolve_test.cpp` (extend)
- Modify: `CMakeLists.txt` (new test target; add `src/origin_match.cpp` to the CLI target
  and plugin target source lists)

**API (exact):**
```cpp
// src/origin_match.hpp
#pragma once
#include <optional>
#include <string>
#include <string_view>
namespace mcap_cloud {
struct Origin {
  std::string scheme;  // "ws" | "wss" (lowercase)
  std::string host;    // lowercase; no IDNA/punycode normalization (M1 scope)
  std::uint16_t port;  // effective: explicit, else 80 (ws) / 443 (wss)
};
/// Strict parse for origin comparison. Returns nullopt for: non-ws/wss scheme,
/// userinfo ('@' before the authority ends), query ('?'), fragment ('#'),
/// empty host, unparsable port. Paths are allowed and ignored.
[[nodiscard]] std::optional<Origin> parseWsOrigin(std::string_view uri);
/// True iff both parse and all three fields match.
[[nodiscard]] bool sameWsOrigin(std::string_view a, std::string_view b);
}  // namespace mcap_cloud
```

- [x] **Step 4.1: Write failing tests** — `tests/origin_match_test.cpp`:
  match: (`ws://h:8080`, `ws://H:8080/path`) · (`wss://h`, `wss://h:443`).
  mismatch: scheme (`ws` vs `wss`) · host · port (`wss://h` vs `wss://h:8443`).
  reject (nullopt → never matches, even against itself): `wss://u:p@h` ·
  `wss://h/?q=1` · `wss://h/#f` · `http://h` · `wss://:1` · `wss://h:bad`.
- [x] **Step 4.2: Verify FAIL, implement, verify PASS.**
- [x] **Step 4.3: Wire the dialog + CLI** per Files above. Dialog rule (exact):
  env token used ⇔ `MCAP_CLOUD_URL` is set AND `sameWsOrigin(env_url, target_uri)`;
  otherwise fall through to the stored per-server token (existing chain unchanged).
  Extend `tests/cli_url_resolve_test.cpp`: env URL + env token + `--url` different origin
  → no token; same origin (case/port-normalized) → token.
- [x] **Step 4.4: Full suite PASS.**
- [x] **Step 4.5: Commit** — `feat(security): bind MCAP_CLOUD_API_KEY to MCAP_CLOUD_URL's origin`

---

### Task 5: Trusted-origin ledger (`src/trusted_origins.{hpp,cpp}`)

Spec §7 guard 1: *trusted = origin recorded after a successful interactive Hello* — a
dedicated ledger, deliberately NOT the credential store (credentials can exist before any
successful connection).

**Files:**
- Create: `src/trusted_origins.hpp`, `src/trusted_origins.cpp`
- Modify: `src/mcap_cloud_dialog.cpp` (`onConnectFinished`, ok==true branch ~line 2536:
  record the origin alongside the existing MRU-history write)
- Create: `tests/trusted_origins_test.cpp`
- Modify: `CMakeLists.txt` (test target; add the .cpp to the plugin source list)

**API (exact; mirror `credential_store.hpp`'s shape — same config root, same 0600/0700
discipline, same atomic-write pattern; READ `src/credential_store.hpp/.cpp` first and
copy its conventions, including the injectable root for tests):**
```cpp
// src/trusted_origins.hpp
#pragma once
#include <filesystem>
#include <string>
#include <string_view>
namespace mcap_cloud {
/// Ledger of origins that completed a successful interactive Hello on this
/// machine (file: <config_root>/trusted_origins.json, 0600, atomic replace).
/// This is the auto-replay trust source (spec §7 guard 1) — NOT the
/// credential store: credentials may be saved before any successful connect.
class TrustedOrigins {
 public:
  explicit TrustedOrigins(std::filesystem::path config_root);  // tests inject
  static TrustedOrigins standard();  // defaultConfigRoot(), like the cred store
  /// Record the origin of `uri` (parseWsOrigin; no-op on unparsable).
  void recordSuccessfulHello(std::string_view uri);
  /// True iff `uri`'s origin was ever recorded.
  [[nodiscard]] bool isTrusted(std::string_view uri) const;
 private:
  std::filesystem::path path_;
};
}  // namespace mcap_cloud
```
Storage format: `{"v":1,"origins":["wss://host:443","ws://host:8080", ...]}` — the
serialized origin is `scheme://host:port` with the EFFECTIVE port always explicit
(so default-port variants compare equal).

- [x] **Step 5.1: Write failing tests**: record→trusted; unrecorded→false; default-port
  equivalence (`wss://h` recorded → `wss://h:443` trusted); persistence across two
  instances on the same root; unparsable uri no-op; corrupt file tolerated as empty
  (same tolerance the credential store pins); file mode 0600 on POSIX.
- [x] **Step 5.2: Verify FAIL, implement (reuse the credential store's atomic-write
  helper pattern), verify PASS.**
- [x] **Step 5.3: Wire `onConnectFinished`** (ok branch): `TrustedOrigins::standard()
  .recordSuccessfulHello(uri)` — construct lazily like `credentialStore()` does.
- [x] **Step 5.4: Full suite PASS.**
- [x] **Step 5.5: Commit** — `feat(security): trusted-origin ledger (successful-Hello only)`

---

### Task 6: Writer seams for cache duty (`SessionMcapWriter`)

Spec §9.0 remaining: (a) an `open` overload taking a caller-owned sink so
creation policy (exclusive create, 0600, fsync) can live OUTSIDE the writer; (b) a
provenance hook writing the canonical descriptor as an MCAP Metadata record.

**Files:**
- Modify: `src/session_mcap_writer.hpp`, `src/session_mcap_writer.cpp`
- Modify: `tests/session_mcap_writer_test.cpp` (extend)

**API additions (exact):**
```cpp
/// Open over a caller-owned sink (the caller owns file creation policy —
/// exclusive create, permissions, fsync — and MUST keep `sink` alive until
/// after close()). Used by the future cache tee; the path overload remains
/// the convenience for export/CLI.
[[nodiscard]] bool open(mcap::IWritable& sink, const SessionInfo& info, std::string* error);
/// Embed a named metadata record (e.g. the canonical replay descriptor,
/// name "mcap_cloud/replay_descriptor"). Call between open() and the first
/// write() — the record participates in the summary's MetadataIndex.
[[nodiscard]] bool writeMetadata(const std::string& name,
                                 const std::string& value_json, std::string* error);
```
Implementation: refactor the existing `open(path,...)` to open the internal
`CheckedFileWriter` then delegate to the new overload (single init path — schema/channel
dictionaries built in exactly one place). `writeMetadata` maps to `mcap::Metadata` with
one KV pair `{"json": value_json}` via `mcap::McapWriter::write(const Metadata&)`.

- [ ] **Step 6.1: Write failing tests**: (a) round-trip via the sink overload writing to a
  `CheckedFileWriter`-like local sink... simpler: reuse the path overload for I/O and test
  the NEW overload with a small in-test `mcap::IWritable` that wraps `std::ofstream` —
  then read the file back and assert messages+summary as the existing round-trip does;
  (b) `writeMetadata("mcap_cloud/replay_descriptor", "{\"v\":1}")` → reader finds exactly
  one metadata record with that name and value via `reader.readMetadata()` /
  metadata index; (c) `writeMetadata` after first `write()` returns false.
- [ ] **Step 6.2: FAIL → implement → PASS** (whole suite).
- [ ] **Step 6.3: Commit** — `feat(writer): caller-owned-sink open + metadata provenance hook`

---

### Task 7: Session file cache core (`src/session_file_cache.{hpp,cpp}`)

The request-addressed store (spec §5), WITHOUT its future consumers (the tee re-target
and adoption are stage 4): root resolution, identity→paths, cross-platform exclusive
lock, validated finalization, orphan cleanup, LRU eviction with free-space reserve.
Shared leases for live datasets are stage 4 (no consumer exists yet) — the lock module
API leaves room (`Exclusive` today; document `Shared` as reserved).

**Files:**
- Create: `src/session_file_cache.hpp`, `src/session_file_cache.cpp`
- Create: `src/core/file_lock.{h,cpp}` (POSIX `flock` / Windows `LockFileEx` wrapper,
  RAII, non-blocking try-acquire)
- Create: `tests/session_file_cache_test.cpp`
- Modify: `CMakeLists.txt` (test target; **this test target needs the READER too** — use
  `tests/mcap_roundtrip_implementation.cpp` like the writer test does; note the product
  plugin does NOT yet link a reader: validation lives in this module but its full
  `validate()` is only exercised from tests + future stage-4 code, so put the
  reader-dependent validation in the module but compile the reader TU into consumers —
  concretely: `session_file_cache.cpp` uses `mcap::McapReader` and therefore the PLUGIN
  target gains `src/mcap_reader_implementation.cpp` (a reader-only
  `MCAP_IMPLEMENTATION` TU) — update `src/mcap_implementation.cpp`'s "product targets
  are WRITE-only" comment, which explicitly anticipated this revisit)

**API (exact):**
```cpp
// src/session_file_cache.hpp
#pragma once
#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <string_view>
namespace mcap_cloud {

/// Request-addressed session cache (spec docs/canonical-layout-replay.md §5).
/// Files: <root>/<128-bit-hex>.mcap; partials: <name>.mcap.partial.<pid>.
/// Root: MCAP_CLOUD_CACHE_DIR || $XDG_CACHE_HOME/mcap_cloud/sessions ||
/// ~/.cache/mcap_cloud/sessions. Directory 0700, files 0600.
class SessionFileCache {
 public:
  explicit SessionFileCache(std::filesystem::path root);   // tests inject
  static SessionFileCache standard(std::string* error);    // env resolution

  struct Config {
    std::uintmax_t max_total_bytes = 20ull * 1024 * 1024 * 1024;  // 20 GiB
    std::uintmax_t min_free_bytes = 2ull * 1024 * 1024 * 1024;    // reserve
    std::chrono::hours orphan_partial_age{24};
  };

  /// identity = the full "mcap-cloud:v1:sha256/128:<hex>" string (validated).
  [[nodiscard]] std::filesystem::path pathFor(std::string_view identity) const;
  /// Existing + structurally valid (footer magic + summary readable +
  /// embedded descriptor identity matches when present). Touches the LRU
  /// stamp on hit. NO network, bounded I/O (footer + summary only).
  [[nodiscard]] bool lookup(std::string_view identity, std::filesystem::path* out);

  /// Exclusive per-identity materialization guard (RAII; non-blocking).
  class MaterializeLock;
  [[nodiscard]] std::optional<MaterializeLock> tryLockForMaterialize(
      std::string_view identity, std::string* error);
  /// The partial path this process must write under `lock`.
  [[nodiscard]] std::filesystem::path partialPathFor(const MaterializeLock&) const;
  /// Validate the finished partial (reader: footer, summary, statistics,
  /// embedded descriptor == identity), fsync file, atomic rename to
  /// pathFor(identity), fsync directory (POSIX). On any failure: remove the
  /// partial, return false with the reason.
  [[nodiscard]] bool finalize(const MaterializeLock&, std::string* error);

  /// Startup/maintenance: remove orphaned partials older than
  /// orphan_partial_age whose lock is free; then LRU-evict unlocked files
  /// (touch-file order) until under max_total_bytes AND min_free_bytes holds.
  void cleanup(const Config& cfg);
 private:
  std::filesystem::path root_;
};
}  // namespace mcap_cloud
```
Touch stamps: sidecar `<name>.touch` files updated on `lookup` hit and `finalize`
(atime is unreliable under relatime/noatime). Eviction order = touch mtime ascending.

- [ ] **Step 7.1: Write failing tests** (inject a temp root; build real tiny MCAPs with
  `SessionMcapWriter` + `writeMetadata` provenance):
  - `pathFor` shape + identity validation (garbage identity → distinct error).
  - materialize→finalize→lookup round-trip; lookup touches the stamp (mtime advances).
  - `finalize` rejects: truncated file (write bytes, chop the tail), missing summary,
    descriptor-identity mismatch (write metadata for a DIFFERENT identity) — partial
    removed in each case.
  - second `tryLockForMaterialize` on the same identity while held → nullopt;
    released → succeeds.
  - `cleanup`: stale orphan partial (backdate its mtime via `fs::last_write_time`)
    removed; fresh partial kept; LRU eviction removes oldest-touched first and stops
    at the byte cap (use a tiny `max_total_bytes` like 8 KiB with two ~4 KiB files).
- [ ] **Step 7.2: FAIL → implement `file_lock` (flock/LockFileEx; lock file =
  `<name>.lock` beside the partial) → implement the cache → PASS.** Windows caveats get
  `#if defined(_WIN32)` branches; POSIX-only assertions (0600) guarded in tests.
- [ ] **Step 7.3: Full suite PASS** (expect 41 + 6 new targets' tests).
- [ ] **Step 7.4: Commit** — `feat(replay): request-addressed session file cache core`

---

### Task 8: Wrap-up

- [ ] **Step 8.1:** Full build + `ctest` (all targets) + `git diff --check`.
- [ ] **Step 8.2:** Run `make smoke` from the repo ROOT of this worktree (server + plugin
  legs; needs the builder venv — see repo CLAUDE.md). Expected `SMOKE PASS`.
- [ ] **Step 8.3:** Update `docs/canonical-layout-replay.md` §13 stage-1 line: mark the
  landed items (descriptor+vectors, cancel fix, origin binding, ledger, writer seams,
  cache core) as done-with-commit-hashes. Commit —
  `docs(layout-replay): stage 1 foundations landed`.
- [ ] **Step 8.4:** Push branch, open PR titled
  `feat(replay): stage-1 foundations — descriptor, trust, cancel fix, cache core`,
  body: summary + spec reference + test counts; note that Codex review is required
  before merge (team rule).
