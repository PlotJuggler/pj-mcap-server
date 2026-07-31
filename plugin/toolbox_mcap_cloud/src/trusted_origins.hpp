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
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace mcap_cloud {

// The canonical serialized trust key for `uri` — "scheme://host:port" with the
// EFFECTIVE port always explicit (the exact string the ledger stores, so
// default-port spellings collide on one entry). nullopt for shapes
// parseWsOrigin rejects. Shared with ImportRuntime's in-memory trust set so
// the two can never drift on normalization.
[[nodiscard]] std::optional<std::string> trustedOriginKey(std::string_view uri);

// Ledger of origins that completed a successful interactive Hello on this
// machine (file: <config_root>/trusted_origins.json, 0600, atomic replace).
// This is the auto-import trust source (spec §7 guard 1) — NOT the
// credential store: credentials may be saved before any successful connect.
class TrustedOrigins {
 public:
  explicit TrustedOrigins(std::filesystem::path config_root);  // tests inject
  static TrustedOrigins standard();  // defaultConfigRoot(), like the cred store

  // Record the origin of `uri` (parseWsOrigin; false on unparsable).
  // DURABLE-OR-FALSE (adversarial F14): the whole read-modify-write is
  // serialized across processes by an exclusive lock on <ledger>.lock
  // (bounded retry), the ledger is re-read UNDER the lock, written to a
  // UNIQUE temp, fsynced (file + directory) and atomically renamed — a
  // rename failure is a FAILURE (no direct-overwrite fallback that a crash
  // could truncate). False = nothing durable happened; the caller must not
  // treat the origin as trusted.
  [[nodiscard]] bool recordSuccessfulHello(std::string_view uri);
  // True iff `uri`'s origin was ever recorded. NOTE: re-reads the ledger file
  // per call — bounded-query consumers (§6.3) go through ImportRuntime's
  // in-memory trust set instead of calling this on a hot path.
  [[nodiscard]] bool isTrusted(std::string_view uri) const;
  // Every recorded serialized origin (trustedOriginKey shape). The
  // ImportRuntime construction-time preload for its in-memory trust set.
  [[nodiscard]] std::vector<std::string> allOrigins() const;

 private:
  std::filesystem::path path_;
};

}  // namespace mcap_cloud
