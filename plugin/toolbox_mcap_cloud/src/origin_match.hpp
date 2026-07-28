// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Strict ws/wss origin parsing + comparison for credential origin binding
// (spec docs/canonical-layout-replay.md §7 guard 2): the MCAP_CLOUD_API_KEY
// env token may only apply to a target whose origin equals MCAP_CLOUD_URL's.
// Deliberately NOT normalizeServerKey() — that is a storage key (lossy,
// collision-tolerant), not an origin parser; a credential-release decision
// needs the strict fail-closed parse below.
#pragma once
#include <cstdint>
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
