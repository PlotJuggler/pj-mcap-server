// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "trusted_origins.hpp"

#include <fstream>
#include <nlohmann/json.hpp>
#include <system_error>

#include "credential_store.hpp"  // defaultConfigRoot()
#include "origin_match.hpp"

namespace mcap_cloud {

namespace fs = std::filesystem;

namespace {

// The serialized ledger entry: scheme://host:port with the EFFECTIVE port
// always explicit, so "wss://h" and "wss://h:443" collide on one entry.
std::string serializeOrigin(const Origin& origin) {
  return origin.scheme + "://" + origin.host + ":" + std::to_string(origin.port);
}

// Load the recorded-origins array from disk. A missing, unreadable, or
// malformed file reads as empty (never throws): the ledger degrades to
// "nothing trusted" and the next record replaces the bad content. Same
// tolerance the credential store pins for its file.
nlohmann::json loadOrigins(const fs::path& file) {
  std::error_code ec;
  if (!fs::exists(file, ec) || ec) {
    return nlohmann::json::array();
  }
  std::ifstream in(file, std::ios::binary);
  if (!in) {
    return nlohmann::json::array();
  }
  nlohmann::json parsed = nlohmann::json::parse(in, /*cb=*/nullptr, /*allow_exceptions=*/false);
  if (!parsed.is_object()) {
    return nlohmann::json::array();
  }
  auto it = parsed.find("origins");
  if (it == parsed.end() || !it->is_array()) {
    return nlohmann::json::array();
  }
  return *it;
}

// Apply 0600 to a file (owner read/write only). Best-effort: failures are
// swallowed (e.g. on filesystems that don't carry POSIX bits).
void chmod0600(const fs::path& file) {
  std::error_code ec;
  fs::permissions(file, fs::perms::owner_read | fs::perms::owner_write, fs::perm_options::replace, ec);
}

// Create `dir` (and parents) and tighten it to 0700 (owner only). Best-effort.
void ensureDir0700(const fs::path& dir) {
  std::error_code ec;
  fs::create_directories(dir, ec);
  fs::permissions(dir, fs::perms::owner_all, fs::perm_options::replace, ec);
}

// Atomically persist the ledger to `file` with 0600 perms: write a sibling
// temp, chmod it, then rename over the target so a crash never leaves a
// half-written ledger (mirrors the credential store's writeObject).
void writeOrigins(const fs::path& file, const nlohmann::json& origins) {
  ensureDir0700(file.parent_path());
  nlohmann::json obj;
  obj["v"] = 1;
  obj["origins"] = origins;
  const fs::path tmp = file.parent_path() / (file.filename().string() + ".tmp");
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) {
      return;  // cannot write — silently give up (non-throwing contract)
    }
    out << obj.dump(2);
  }
  chmod0600(tmp);
  std::error_code ec;
  fs::rename(tmp, file, ec);
  if (ec) {
    // Rename failed (e.g. cross-device); fall back to a direct overwrite.
    std::ofstream out(file, std::ios::binary | std::ios::trunc);
    if (out) {
      out << obj.dump(2);
    }
    fs::remove(tmp, ec);
  }
  chmod0600(file);
}

}  // namespace

std::optional<std::string> trustedOriginKey(std::string_view uri) {
  const auto origin = parseWsOrigin(uri);
  if (!origin.has_value()) {
    return std::nullopt;
  }
  return serializeOrigin(*origin);
}

TrustedOrigins::TrustedOrigins(fs::path config_root) : path_(std::move(config_root)) {
  path_ /= "trusted_origins.json";
}

TrustedOrigins TrustedOrigins::standard() {
  return TrustedOrigins(defaultConfigRoot());
}

void TrustedOrigins::recordSuccessfulHello(std::string_view uri) {
  const auto origin = parseWsOrigin(uri);
  if (!origin.has_value()) {
    return;  // unparsable uri: fail closed, record nothing
  }
  const std::string entry = serializeOrigin(*origin);
  nlohmann::json origins = loadOrigins(path_);
  for (const auto& existing : origins) {
    if (existing.is_string() && existing.get<std::string>() == entry) {
      return;  // already recorded — idempotent, no rewrite
    }
  }
  origins.push_back(entry);
  writeOrigins(path_, origins);
}

std::vector<std::string> TrustedOrigins::allOrigins() const {
  std::vector<std::string> out;
  const nlohmann::json origins = loadOrigins(path_);
  out.reserve(origins.size());
  for (const auto& existing : origins) {
    if (existing.is_string()) {
      out.push_back(existing.get<std::string>());
    }
  }
  return out;
}

bool TrustedOrigins::isTrusted(std::string_view uri) const {
  const auto origin = parseWsOrigin(uri);
  if (!origin.has_value()) {
    return false;  // rejected shapes never match, not even themselves
  }
  const std::string entry = serializeOrigin(*origin);
  const nlohmann::json origins = loadOrigins(path_);
  for (const auto& existing : origins) {
    if (existing.is_string() && existing.get<std::string>() == entry) {
      return true;
    }
  }
  return false;
}

}  // namespace mcap_cloud
