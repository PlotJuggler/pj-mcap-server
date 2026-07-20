// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "credential_store.hpp"

#include <pj_base/sdk/platform.hpp>

#include <fstream>
#include <nlohmann/json.hpp>
#include <system_error>

#include "server_history.h"

namespace mcap_cloud {

namespace fs = std::filesystem;

namespace {

// Load the credentials JSON object from disk. A missing, unreadable, or
// malformed file reads as an empty object (never throws): the store degrades
// to "no credentials" and the next write replaces the bad content.
nlohmann::json loadObject(const fs::path& file) {
  std::error_code ec;
  if (!fs::exists(file, ec) || ec) {
    return nlohmann::json::object();
  }
  std::ifstream in(file, std::ios::binary);
  if (!in) {
    return nlohmann::json::object();
  }
  nlohmann::json parsed = nlohmann::json::parse(in, /*cb=*/nullptr, /*allow_exceptions=*/false);
  if (!parsed.is_object()) {
    return nlohmann::json::object();
  }
  return parsed;
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

// Atomically persist `obj` to `file` with 0600 perms: write a sibling temp,
// chmod it, then rename over the target so a crash never leaves a half-written
// (or world-readable) secrets file.
void writeObject(const fs::path& file, const nlohmann::json& obj) {
  ensureDir0700(file.parent_path());
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

std::optional<std::string> FileCredentialStore::get(const std::string& server_url) const {
  const std::string key = normalizeServerKey(server_url);
  if (key.empty()) {
    return std::nullopt;
  }
  const nlohmann::json obj = loadObject(filePath());
  auto it = obj.find(key);
  if (it == obj.end() || !it->is_string()) {
    return std::nullopt;
  }
  return it->get<std::string>();
}

void FileCredentialStore::set(const std::string& server_url, const std::string& token) {
  const std::string key = normalizeServerKey(server_url);
  if (key.empty()) {
    return;
  }
  nlohmann::json obj = loadObject(filePath());
  obj[key] = token;
  writeObject(filePath(), obj);
}

void FileCredentialStore::erase(const std::string& server_url) {
  const std::string key = normalizeServerKey(server_url);
  if (key.empty()) {
    return;
  }
  nlohmann::json obj = loadObject(filePath());
  if (obj.contains(key)) {
    obj.erase(key);
    writeObject(filePath(), obj);
  }
}

fs::path defaultConfigRoot() {
  if (auto v = PJ::sdk::getEnv("XDG_CONFIG_HOME")) {
    return fs::path(*v) / "mcap_cloud";
  }
  if (auto v = PJ::sdk::getEnv("HOME")) {
    return fs::path(*v) / ".config" / "mcap_cloud";
  }
  return fs::temp_directory_path() / "mcap_cloud";
}

}  // namespace mcap_cloud
