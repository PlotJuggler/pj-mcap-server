// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Trusted-origin ledger (spec docs/canonical-layout-import.md §7 guard 1):
// *trusted = origin recorded after a successful interactive Hello*. This is a
// dedicated ledger, deliberately NOT the credential store — credentials can
// exist before any successful connection, so "has a stored token" must never
// imply "safe to auto-import against".
//
// Storage format: {"v":1,"origins":["wss://host:443","ws://host:8080", ...]}
// where each serialized origin is scheme://host:port with the EFFECTIVE port
// always explicit (parseWsOrigin), so default-port spellings of one origin
// compare equal. Same file discipline as the credential store: config root,
// 0700 dir, 0600 file, atomic replace, corrupt-file-reads-as-empty.

#pragma once

#include <filesystem>
#include <string>
#include <string_view>

namespace mcap_cloud {

// Ledger of origins that completed a successful interactive Hello on this
// machine (file: <config_root>/trusted_origins.json, 0600, atomic replace).
// This is the auto-import trust source (spec §7 guard 1) — NOT the
// credential store: credentials may be saved before any successful connect.
class TrustedOrigins {
 public:
  explicit TrustedOrigins(std::filesystem::path config_root);  // tests inject
  static TrustedOrigins standard();  // defaultConfigRoot(), like the cred store

  // Record the origin of `uri` (parseWsOrigin; no-op on unparsable).
  void recordSuccessfulHello(std::string_view uri);
  // True iff `uri`'s origin was ever recorded.
  [[nodiscard]] bool isTrusted(std::string_view uri) const;

 private:
  std::filesystem::path path_;
};

}  // namespace mcap_cloud
