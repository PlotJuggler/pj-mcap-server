// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "trusted_origins.hpp"

#include "core/file_lock.h"

#include <chrono>
#include <fstream>
#include <optional>
#include <thread>

#if defined(_WIN32)
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif
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
  // Regular-file check (F14 test-caught): a DIRECTORY squatting on the
  // ledger path made the ifstream read throw from underflow — the
  // corrupt-reads-as-empty contract must cover that shape too.
  if (!fs::is_regular_file(file, ec) || ec) {
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

// fsync helpers (adversarial F14 — a trusted-origin write must be durable
// before the origin is treated as trusted anywhere).
bool syncLedgerFile(const fs::path& path) {
#if defined(_WIN32)
  const HANDLE handle = ::CreateFileW(path.c_str(), GENERIC_WRITE, FILE_SHARE_READ, nullptr,
                                      OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
  if (handle == INVALID_HANDLE_VALUE) {
    return false;
  }
  const bool ok = ::FlushFileBuffers(handle) != 0;
  ::CloseHandle(handle);
  return ok;
#else
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    return false;
  }
  const bool ok = ::fsync(fd) == 0;
  ::close(fd);
  return ok;
#endif
}

// R5 test seam: force the directory fsync to report failure (a real dir
// whose fsync fails is not constructible portably from a test).
bool g_fail_dir_sync_for_test = false;

// Re-verify R5: durable-or-false includes the DIRECTORY fsync — a rename
// whose directory entry never reached stable storage can vanish on crash,
// so an open()/fsync() failure here fails the whole write. Windows has no
// directory-fsync equivalent (metadata durability rides the NTFS journal);
// documented no-op success there.
[[nodiscard]] bool syncLedgerDir(const fs::path& dir) {
  if (g_fail_dir_sync_for_test) {
    return false;
  }
#if !defined(_WIN32)
  const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd < 0) {
    return false;
  }
  const bool ok = ::fsync(fd) == 0;
  ::close(fd);
  return ok;
#else
  (void)dir;
  return true;
#endif
}

// Durably persist the ledger (adversarial F14): UNIQUE temp (pid-suffixed —
// two processes must never share one), 0600, fsync, atomic rename, directory
// fsync. A rename failure is a FAILURE (the old direct-overwrite fallback
// could be truncated mid-crash); false = nothing durable happened.
[[nodiscard]] bool writeOrigins(const fs::path& file, const nlohmann::json& origins) {
  ensureDir0700(file.parent_path());
  nlohmann::json obj;
  obj["v"] = 1;
  obj["origins"] = origins;
#if defined(_WIN32)
  const unsigned long pid = static_cast<unsigned long>(::GetCurrentProcessId());
#else
  const unsigned long pid = static_cast<unsigned long>(::getpid());
#endif
  const fs::path tmp =
      file.parent_path() / (file.filename().string() + ".tmp." + std::to_string(pid));
  {
    std::ofstream out(tmp, std::ios::binary | std::ios::trunc);
    if (!out) {
      return false;
    }
    out << obj.dump(2);
    out.flush();
    if (!out) {
      std::error_code ec;
      fs::remove(tmp, ec);
      return false;
    }
  }
  chmod0600(tmp);
  if (!syncLedgerFile(tmp)) {
    std::error_code ec;
    fs::remove(tmp, ec);
    return false;
  }
  std::error_code ec;
  fs::rename(tmp, file, ec);
  if (ec) {
    fs::remove(tmp, ec);
    return false;
  }
  chmod0600(file);
  if (!syncLedgerDir(file.parent_path())) {
    return false;  // R5: the rename may not survive a crash — not durable
  }
  return true;
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

bool TrustedOrigins::recordSuccessfulHello(std::string_view uri) {
  const auto origin = parseWsOrigin(uri);
  if (!origin.has_value()) {
    return false;  // unparsable uri: fail closed, record nothing
  }
  const std::string entry = serializeOrigin(*origin);
  // Serialize the WHOLE read-modify-write across processes (adversarial
  // F14): two processes appending different origins previously raced on one
  // load/one temp and could lose an update. Bounded retry on the exclusive
  // sidecar lock; the ledger is RE-READ under the lock so a concurrent
  // append is merged, never overwritten.
  ensureDir0700(path_.parent_path());
  const fs::path lock_path = path_.parent_path() / (path_.filename().string() + ".lock");
  std::optional<FileLock> lock;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
  for (;;) {
    lock = FileLock::tryExclusive(lock_path, nullptr);
    if (lock.has_value()) {
      break;
    }
    if (std::chrono::steady_clock::now() >= deadline) {
      return false;  // could not serialize — nothing durable happened
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  nlohmann::json origins = loadOrigins(path_);  // RE-read under the lock
  for (const auto& existing : origins) {
    if (existing.is_string() && existing.get<std::string>() == entry) {
      return true;  // already recorded — idempotent, durably present
    }
  }
  origins.push_back(entry);
  return writeOrigins(path_, origins);
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

namespace testing {
void setTrustedOriginsDirSyncFailForTest(bool fail) {
  g_fail_dir_sync_for_test = fail;
}
}  // namespace testing

}  // namespace mcap_cloud
