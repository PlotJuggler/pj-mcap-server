// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// wasm_smoke_main — the smoke entry point for the WASM decode-core compile job
// (unified plan M2c-DEX "WASM bonus" + risk 8: "a CI job that at minimum COMPILES
// client-core to WASM so the path cannot silently rot"). It exercises the PURE
// protocol/decode core — the dependency-light, socket-free units that genuinely
// belong in a future browser build — and prints a single PASS/FAIL line.
//
// What it proves runs under wasm32-emscripten:
//   1. SessionKey normalization + FNV-1a hashing (src/session_key.hpp) — the
//      EXACT reorder-collide / different-uri / different-range cases from the
//      native unit test.
//   2. The SessionCache LRU + existence-predicate semantics (src/session_cache.hpp).
//   3. The '/'-prefix hierarchy derivation (src/hierarchy_prefix.h).
//   4. The stitched multi-file selection merge + overlap validation
//      (src/stitch_select.h).
//   5. CLI/URL precedence resolution (tools/cli_url_resolve.hpp).
//   6. The REAL one-shot ZSTD batch-body decode path — byte-identical to
//      session_decode.cpp's zstdDecodeAll (ZSTD_getFrameContentSize +
//      ZSTD_decompress) — against a frame produced by the NATIVE conan libzstd
//      1.5.7 encoder (embedded via wasm_test_frame.h). Decoder = the single-file
//      zstddeclib amalgamation, also 1.5.7. This is FUNCTIONAL evidence that the
//      browser decode path links + runs and reinflates a server-shaped frame.
//
// What it deliberately does NOT touch (see wasm/README.md for the risk-8 spike):
//   - backend_connection.* (ixwebsocket raw sockets / std::thread — no browser
//     equivalent; a browser build needs a future JS-WebSocket binding).
//   - session_decode.cpp / wire_mapping.cpp (compile to wasm objects, but LINK
//     needs libprotobuf + abseil cross-built for wasm32 — out of scope here).
//
// No GTest (keeps the wasm link std-only). Plain asserts via a tiny CHECK; any
// failure prints "WASM SMOKE FAIL: <what>" and returns non-zero so node + the
// build script both observe it.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "cli_url_resolve.hpp"
#include "hierarchy_prefix.h"
#include "session_cache.hpp"
#include "session_key.hpp"
#include "stitch_select.h"
#include "wasm_test_frame.h"
#include "zstd.h"

namespace {

int g_failures = 0;

void check(bool ok, const char* what) {
  if (!ok) {
    std::printf("WASM SMOKE FAIL: %s\n", what);
    ++g_failures;
  }
}

// Verbatim re-implementation of session_decode.cpp's zstdDecodeAll one-shot path.
// We re-state it here (rather than linking session_decode.cpp) precisely because
// that .cpp pulls in pj_cloud.pb.h -> libprotobuf, which does not link for wasm
// in this gating job. The DECODE LOGIC under test is identical: read the frame
// content size, decompress once, verify ZSTD_isError. If this links + runs, the
// real session-batch ZSTD body decode links + runs in the browser too.
bool zstdDecodeAll(const std::string& in, std::string* out, std::string* error) {
  const unsigned long long content = ZSTD_getFrameContentSize(in.data(), in.size());
  if (content == ZSTD_CONTENTSIZE_ERROR) {
    *error = "invalid ZSTD frame";
    return false;
  }
  std::size_t cap = 0;
  if (content == ZSTD_CONTENTSIZE_UNKNOWN) {
    cap = in.size() * 8 + 1024;
  } else {
    cap = static_cast<std::size_t>(content);
  }
  out->resize(cap);
  const std::size_t n = ZSTD_decompress(out->data(), out->size(), in.data(), in.size());
  if (ZSTD_isError(n)) {
    *error = std::string("ZSTD decode failed: ") + ZSTD_getErrorName(n);
    return false;
  }
  out->resize(n);
  return true;
}

void testSessionKey() {
  using namespace PJ::cloud;
  // Reordered file_ids + topics produce the same normalized key + hash.
  SessionKey a = computeSessionKey("wss://h/api/ws", {3, 1, 2}, {"/b", "/a"}, {100, 200});
  SessionKey b = computeSessionKey("wss://h/api/ws", {1, 2, 3}, {"/a", "/b"}, {100, 200});
  check(a == b, "session_key reordered-equal");
  check(a.hash == b.hash, "session_key reordered-hash-equal");

  SessionKey c = computeSessionKey("wss://h1/api/ws", {1}, {"/a"}, {0, 0});
  SessionKey d = computeSessionKey("wss://h2/api/ws", {1}, {"/a"}, {0, 0});
  check(c != d, "session_key different-uri");

  SessionKey e = computeSessionKey("wss://h/api/ws", {1}, {"/a"}, {100, 200});
  SessionKey f = computeSessionKey("wss://h/api/ws", {1}, {"/a"}, {100, 201});
  check(e != f, "session_key different-range");
}

void testSessionCache() {
  using namespace PJ::cloud;
  using dexory_cloud::CachedSession;
  using dexory_cloud::SessionCache;

  SessionCache cache(2);
  SessionKey k1 = computeSessionKey("wss://h/api/ws", {1}, {"/a"}, {0, 0});
  CachedSession v1;
  v1.display_name = "ds1";
  v1.total_messages = 42;
  cache.store(k1, v1);
  check(cache.size() == 1, "session_cache size-after-store");

  // Present predicate -> HIT.
  auto present = [](const std::string&) { return true; };
  auto hit = cache.lookup(k1, present);
  check(hit.has_value() && hit->total_messages == 42, "session_cache hit");

  // Gone predicate -> MISS + eviction.
  auto gone = [](const std::string&) { return false; };
  auto miss = cache.lookup(k1, gone);
  check(!miss.has_value(), "session_cache gone-miss");
  check(cache.size() == 0, "session_cache evicts-gone");
}

void testHierarchy() {
  using dexory_cloud::buildPrefixComboItems;
  using dexory_cloud::deriveTopLevelPrefixes;
  using dexory_cloud::nameUnderPrefix;
  std::vector<std::string> names = {"runA/x.mcap", "runA/y.mcap", "runB/z.mcap", "loose.mcap"};
  auto prefixes = deriveTopLevelPrefixes(names);
  check(prefixes.size() == 2 && prefixes[0] == "runA/" && prefixes[1] == "runB/", "hierarchy prefixes");
  auto items = buildPrefixComboItems(names);
  check(!items.empty() && items[0] == "All", "hierarchy combo-all-sentinel");
  check(nameUnderPrefix("runA/x.mcap", "runA/"), "hierarchy under-prefix");
  check(!nameUnderPrefix("runB/z.mcap", "runA/"), "hierarchy not-under-prefix");
  check(nameUnderPrefix("loose.mcap", "All"), "hierarchy all-matches");
}

void testStitch() {
  using dexory_cloud::buildStitchedSelection;
  using dexory_cloud::SelInput;
  using dexory_cloud::validateNonOverlapping;
  // Reordered selection -> identical ordered_names (sorted by min_ts, name).
  std::vector<SelInput> sel_ab = {{"a", 100, 200, 10, 5}, {"b", 300, 400, 20, 7}};
  std::vector<SelInput> sel_ba = {{"b", 300, 400, 20, 7}, {"a", 100, 200, 10, 5}};
  auto ab = buildStitchedSelection(sel_ab);
  auto ba = buildStitchedSelection(sel_ba);
  check(ab.ordered_names == ba.ordered_names, "stitch reorder-stable");
  check(ab.ordered_names.size() == 2 && ab.ordered_names[0] == "a", "stitch ordering");
  check(ab.union_min_ts_ns == 100 && ab.union_max_ts_ns == 400, "stitch union-range");
  check(ab.total_message_count == 12, "stitch total-count");
  check(validateNonOverlapping(sel_ab).empty(), "stitch non-overlap-ok");
  // Overlapping pair -> non-empty message.
  std::vector<SelInput> overlap = {{"a", 100, 350, 10, 5}, {"b", 300, 400, 20, 7}};
  check(!validateNonOverlapping(overlap).empty(), "stitch overlap-detected");
}

void testCliUrl() {
  using dexory_cloud::kDefaultCliUrl;
  using dexory_cloud::resolveCliToken;
  using dexory_cloud::resolveCliUrl;
  check(resolveCliUrl(std::string("ws://explicit"), std::string("ws://env")) == "ws://explicit",
        "cli_url explicit-wins");
  check(resolveCliUrl(std::nullopt, std::string("ws://env")) == "ws://env", "cli_url env-fallback");
  check(resolveCliUrl(std::nullopt, std::nullopt) == kDefaultCliUrl, "cli_url default");
  check(resolveCliToken(std::string(""), std::string("envtok")) == "", "cli_token explicit-empty-wins");
  check(resolveCliToken(std::nullopt, std::string("envtok")) == "envtok", "cli_token env-fallback");
}

void testZstdDecode() {
  // The REAL one-shot decode path against a native-encoder frame.
  std::string frame(reinterpret_cast<const char*>(dexory_cloud_wasm_smoke::kZstdFrame),
                    dexory_cloud_wasm_smoke::kZstdFrame_len);
  std::string expected(reinterpret_cast<const char*>(dexory_cloud_wasm_smoke::kPlaintext),
                       dexory_cloud_wasm_smoke::kPlaintext_len);
  std::string out;
  std::string error;
  const bool ok = zstdDecodeAll(frame, &out, &error);
  check(ok, ok ? "zstd decode ok" : error.c_str());
  check(out.size() == expected.size(), "zstd decode size");
  check(out == expected, "zstd decode byte-for-byte");

  // The server VERIFIES the decompressed length equals body_uncompressed_size;
  // mirror that the announced size matches (spec §6.4 hard check).
  check(out.size() == dexory_cloud_wasm_smoke::kPlaintext_len, "zstd uncompressed-size-match");

  // A corrupt frame must be rejected cleanly, not crash (defensive parsing).
  std::string corrupt = frame;
  if (corrupt.size() > 4) {
    corrupt[4] ^= 0xFF;  // flip a byte inside the frame body
  }
  std::string cout, cerr;
  const bool corrupt_ok = zstdDecodeAll(corrupt, &cout, &cerr);
  check(!corrupt_ok, "zstd corrupt-rejected");
}

}  // namespace

int main() {
  std::printf("dexory-cloud wasm decode-core smoke (zstd %d.%d.%d decoder)\n", ZSTD_VERSION_MAJOR,
              ZSTD_VERSION_MINOR, ZSTD_VERSION_RELEASE);
  testSessionKey();
  testSessionCache();
  testHierarchy();
  testStitch();
  testCliUrl();
  testZstdDecode();

  if (g_failures == 0) {
    std::printf("WASM SMOKE PASS\n");
    return 0;
  }
  std::printf("WASM SMOKE FAIL: %d check(s) failed\n", g_failures);
  return 1;
}
