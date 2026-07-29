// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// The canonical source descriptor (spec docs/canonical-layout-import.md §4):
// the PUBLIC, allowlisted, versioned description of a fetch request that a
// layout may embed and a provider may import. Deliberately distinct from the
// process-local FNV SessionKey (src/session_key.hpp), which never crosses a
// file boundary. The canonical serialization + identity are a CROSS-REPO
// contract pinned by docs/source-descriptor-vectors.json (PJ4-side tests
// consume the same file byte-identically) — changing either requires bumping
// the descriptor version, never silently regenerating vectors.
#pragma once
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace mcap_cloud {

/// Spec docs/canonical-layout-import.md §4. Timestamps are decimal strings on
/// the wire; parsed to int64 here. topics empty = all. "0"/"0" = whole range.
struct SourceDescriptor {
  int version = 1;                   // "v"
  std::string kind;                  // "mcap-cloud-session"
  std::string server_uri;
  std::vector<std::string> s3_keys;  // order preserved (wire order)
  std::vector<std::string> topics;
  std::int64_t start_ns = 0;
  std::int64_t end_ns = 0;
  bool include_latched = true;
  std::string display_name;          // EXCLUDED from identity
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
[[nodiscard]] std::optional<SourceDescriptor> parseSourceDescriptor(
    std::string_view json, std::string* error);

/// Canonical serialization: the EXACT byte string the identity is computed
/// over. Sorted keys, compact (no whitespace), UTF-8, ns as decimal strings,
/// display_name OMITTED. Pinned by docs/source-descriptor-vectors.json.
[[nodiscard]] std::string canonicalSourceDescriptorJson(const SourceDescriptor& d);

/// "mcap-cloud:v1:sha256/128:<32 lowercase hex>" over canonicalSourceDescriptorJson.
[[nodiscard]] std::string descriptorIdentity(const SourceDescriptor& d);

/// Serialize for embedding in a layout (canonical fields + display_name).
[[nodiscard]] std::string toSourceDescriptorJson(const SourceDescriptor& d);

}  // namespace mcap_cloud
