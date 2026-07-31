// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "session_file_cache.hpp"

#include <mcap/reader.hpp>
#include <pj_base/sdk/platform.hpp>

#include <algorithm>
#include <fstream>
#include <system_error>
#include <vector>

#include "core/sha256.h"

#if defined(_WIN32)
#include <windows.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace mcap_cloud {

namespace fs = std::filesystem;

namespace {

constexpr std::string_view kIdentityPrefix = "mcap-cloud:v1:sha256/128:";
constexpr std::size_t kDigestHexChars = 32;  // 128 bits, lowercase hex
// The provenance record SessionMcapWriter::writeMetadata embeds (spec §5:
// a cache file self-describes which request produced it).
constexpr const char* kProvenanceName = "mcap_cloud/source_descriptor";

// Extract the digest component from a full identity string; nullopt for
// anything that is not EXACTLY "mcap-cloud:v1:sha256/128:<32 lowercase hex>".
std::optional<std::string> identityHex(std::string_view identity) {
  if (identity.size() != kIdentityPrefix.size() + kDigestHexChars) {
    return std::nullopt;
  }
  if (identity.substr(0, kIdentityPrefix.size()) != kIdentityPrefix) {
    return std::nullopt;
  }
  const std::string_view hex = identity.substr(kIdentityPrefix.size());
  for (const char c : hex) {
    const bool ok = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
    if (!ok) {
      return std::nullopt;
    }
  }
  return std::string(hex);
}

// Apply 0600 to a file (owner read/write only). Best-effort: failures are
// swallowed (e.g. on filesystems that don't carry POSIX bits).
void chmod0600(const fs::path& file) {
  std::error_code ec;
  fs::permissions(file, fs::perms::owner_read | fs::perms::owner_write,
                  fs::perm_options::replace, ec);
}

// Create `dir` (and parents) and tighten it to 0700 (owner only). Best-effort.
void ensureDir0700(const fs::path& dir) {
  std::error_code ec;
  fs::create_directories(dir, ec);
  fs::permissions(dir, fs::perms::owner_all, fs::perm_options::replace, ec);
}

fs::path touchPathFor(const fs::path& file) {
  return fs::path(file.string() + ".touch");
}

fs::path lockPathFor(const fs::path& file) {
  return fs::path(file.string() + ".lock");
}

// Update (or create) the LRU stamp beside `file`. The sidecar's mtime is the
// eviction order; lookup hits and finalization both move it to now.
void touchStamp(const fs::path& file) {
  const fs::path stamp = touchPathFor(file);
  {
    std::ofstream out(stamp, std::ios::binary | std::ios::trunc);
  }
  std::error_code ec;
  fs::last_write_time(stamp, fs::file_time_type::clock::now(), ec);
  chmod0600(stamp);
}

// Reader-validate `path` against the identity digest (spec §5). Bounded I/O:
// footer + summary section only, never a message scan. Statistics AND the
// embedded descriptor are REQUIRED unconditionally — every legitimately
// finalized cache file carries both (the tee always embeds the descriptor),
// so absence is either a truncated write or a foreign file, and both must be
// a miss/rejection (review-caught: a lenient lookup classified any unrelated
// valid MCAP dropped at <digest>.mcap as a hit for that request).
// `expected`, when provided (the finalize gate with producer-known counts),
// additionally pins Statistics against the session's expected message/channel
// counts — the only check that catches a cleanly-closed PREFIX of a stream.
//
// The identity check hashes the embedded canonical-descriptor bytes directly:
// the identity is DEFINED as sha256/128 over those exact bytes (see
// descriptorIdentity), so byte-hashing detects wrong-file substitution and
// name collisions from the file alone, without a JSON parse here.
bool validateMcap(const fs::path& path, const std::string& hex,
                  const std::optional<SessionFileCache::ExpectedContent>& expected,
                  std::string* error) {
  mcap::McapReader reader;
  mcap::Status status = reader.open(path.string());
  if (!status.ok()) {
    *error = "not a readable MCAP: " + status.message;
    return false;
  }
  status = reader.readSummary(mcap::ReadSummaryMethod::NoFallbackScan);
  if (!status.ok()) {
    reader.close();
    *error = "summary unreadable: " + status.message;
    return false;
  }
  if (!reader.statistics().has_value()) {
    reader.close();
    *error = "missing Statistics record";
    return false;
  }
  if (expected.has_value()) {
    const auto& stats = *reader.statistics();
    if (stats.messageCount != expected->message_count ||
        static_cast<std::uint64_t>(stats.channelCount) != expected->channel_count) {
      reader.close();
      *error = "Statistics mismatch: file has " + std::to_string(stats.messageCount) +
               " message(s) / " + std::to_string(stats.channelCount) + " channel(s), expected " +
               std::to_string(expected->message_count) + " / " +
               std::to_string(expected->channel_count) +
               " (incomplete or foreign session content)";
      return false;
    }
  }
  const auto index = reader.metadataIndexes().find(kProvenanceName);
  if (index == reader.metadataIndexes().end()) {
    reader.close();
    *error = std::string("missing embedded source descriptor (") + kProvenanceName + ")";
    return false;
  }
  mcap::Record record;
  status = mcap::McapReader::ReadRecord(*reader.dataSource(), index->second.offset, &record);
  if (!status.ok()) {
    reader.close();
    *error = "embedded descriptor unreadable: " + status.message;
    return false;
  }
  mcap::Metadata metadata;
  status = mcap::McapReader::ParseMetadata(record, &metadata);
  reader.close();
  if (!status.ok()) {
    *error = "embedded descriptor unparsable: " + status.message;
    return false;
  }
  const auto value = metadata.metadata.find("json");
  if (value == metadata.metadata.end()) {
    *error = "embedded descriptor record has no json entry";
    return false;
  }
  if (sha256HexPrefix(value->second, kDigestHexChars / 2) != hex) {
    *error = "embedded descriptor identity mismatch";
    return false;
  }
  return true;
}

// fsync `path`'s contents to stable storage before the publishing rename.
bool syncFile(const fs::path& path, std::string* error) {
#if defined(_WIN32)
  const HANDLE handle = ::CreateFileW(path.c_str(), GENERIC_WRITE, FILE_SHARE_READ, nullptr,
                                      OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
  if (handle == INVALID_HANDLE_VALUE) {
    *error = "could not reopen for flush: " + path.string();
    return false;
  }
  const bool ok = ::FlushFileBuffers(handle) != 0;
  ::CloseHandle(handle);
  if (!ok) {
    *error = "FlushFileBuffers failed: " + path.string();
  }
  return ok;
#else
  const int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
  if (fd < 0) {
    *error = "could not reopen for fsync: " + path.string() + ": " +
             std::error_code(errno, std::generic_category()).message();
    return false;
  }
  const bool ok = ::fsync(fd) == 0;
  const int fsync_errno = errno;
  ::close(fd);
  if (!ok) {
    *error = "fsync failed: " + path.string() + ": " +
             std::error_code(fsync_errno, std::generic_category()).message();
  }
  return ok;
#endif
}

// Make the publishing rename itself durable (POSIX: fsync the directory;
// no-op where unsupported — spec §5 "where supported"). Best-effort.
void syncDir(const fs::path& dir) {
#if !defined(_WIN32)
  const int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
  if (fd >= 0) {
    ::fsync(fd);
    ::close(fd);
  }
#else
  (void)dir;
#endif
}

}  // namespace

SessionFileCache::MaterializeLock::MaterializeLock(FileLock lock, std::string hex,
                                                   fs::path partial)
    : lock_(std::move(lock)), hex_(std::move(hex)), partial_(std::move(partial)) {}

SessionFileCache::SessionFileCache(fs::path root) : root_(std::move(root)) {}

SessionFileCache SessionFileCache::standard(std::string* error) {
  if (auto v = PJ::sdk::getEnv("MCAP_CLOUD_CACHE_DIR")) {
    return SessionFileCache(fs::path(*v));
  }
  if (auto v = PJ::sdk::getEnv("XDG_CACHE_HOME")) {
    return SessionFileCache(fs::path(*v) / "mcap_cloud" / "sessions");
  }
  if (auto v = PJ::sdk::getEnv("HOME")) {
    return SessionFileCache(fs::path(*v) / ".cache" / "mcap_cloud" / "sessions");
  }
  if (error) {
    *error = "cache root unresolvable (MCAP_CLOUD_CACHE_DIR, XDG_CACHE_HOME and HOME all unset)";
  }
  return SessionFileCache(fs::path());
}

fs::path SessionFileCache::pathFor(std::string_view identity) const {
  const auto hex = identityHex(identity);
  if (!hex.has_value() || root_.empty()) {
    return {};
  }
  return root_ / (*hex + ".mcap");
}

bool SessionFileCache::lookup(std::string_view identity, fs::path* out) {
  const auto hex = identityHex(identity);
  if (!hex.has_value() || root_.empty()) {
    return false;
  }
  const fs::path file = root_ / (*hex + ".mcap");
  std::error_code ec;
  if (!fs::is_regular_file(file, ec) || ec) {
    return false;
  }
  std::string error;
  if (!validateMcap(file, *hex, /*expected=*/std::nullopt, &error)) {
    return false;
  }
  touchStamp(file);
  if (out) {
    *out = file;
  }
  return true;
}

std::optional<SessionFileCache::MaterializeLock> SessionFileCache::tryLockForMaterialize(
    std::string_view identity, std::string* error) {
  const auto hex = identityHex(identity);
  if (!hex.has_value()) {
    if (error) {
      *error = "invalid descriptor identity (want mcap-cloud:v1:sha256/128:<32 lowercase hex>)";
    }
    return std::nullopt;
  }
  if (root_.empty()) {
    if (error) {
      *error = "cache root is not configured";
    }
    return std::nullopt;
  }
  ensureDir0700(root_);
  std::string lock_error;
  auto lock = FileLock::tryExclusive(root_ / (*hex + ".mcap.lock"), &lock_error);
  if (!lock.has_value()) {
    if (error) {
      *error = "cache materialize lock unavailable: " + lock_error;
    }
    return std::nullopt;
  }
#if defined(_WIN32)
  const auto pid = static_cast<long long>(::GetCurrentProcessId());
#else
  const auto pid = static_cast<long long>(::getpid());
#endif
  fs::path partial = root_ / (*hex + ".mcap.partial." + std::to_string(pid));
  return MaterializeLock(std::move(*lock), *hex, std::move(partial));
}

fs::path SessionFileCache::partialPathFor(const MaterializeLock& lock) const {
  return lock.partial_;
}

std::optional<FileLock> SessionFileCache::acquireReadLease(std::string_view identity,
                                                           std::string* error) {
  const auto hex = identityHex(identity);
  if (!hex.has_value()) {
    if (error) {
      *error = "invalid descriptor identity (want mcap-cloud:v1:sha256/128:<32 lowercase hex>)";
    }
    return std::nullopt;
  }
  if (root_.empty()) {
    if (error) {
      *error = "cache root is not configured";
    }
    return std::nullopt;
  }
  ensureDir0700(root_);
  std::string lock_error;
  auto lease = FileLock::tryShared(root_ / (*hex + ".mcap.lock"), &lock_error);
  if (!lease.has_value()) {
    if (error) {
      *error = "cache read lease unavailable: " + lock_error;
    }
    return std::nullopt;
  }
  return lease;
}

bool SessionFileCache::finalize(const MaterializeLock& lock,
                                const std::optional<ExpectedContent>& expected,
                                std::string* error) {
  const fs::path& partial = lock.partial_;
  const fs::path final_path = root_ / (lock.hex_ + ".mcap");
  const auto fail = [&](const std::string& reason) {
    std::error_code ec;
    fs::remove(partial, ec);
    if (error) {
      *error = reason;
    }
    return false;
  };
  std::string reason;
  if (!validateMcap(partial, lock.hex_, expected, &reason)) {
    return fail("cache finalize rejected " + partial.filename().string() + ": " + reason);
  }
  chmod0600(partial);
  if (!syncFile(partial, &reason)) {
    return fail(reason);
  }
  std::error_code ec;
  fs::rename(partial, final_path, ec);
  if (ec) {
    return fail("atomic rename failed: " + ec.message());
  }
  syncDir(root_);
  touchStamp(final_path);
  return true;
}

void SessionFileCache::cleanup(const Config& cfg) {
  std::error_code ec;
  if (root_.empty() || !fs::is_directory(root_, ec) || ec) {
    return;
  }

  // Pass 1: orphaned partials — old enough AND identity lock free. BOTH
  // guards matter: flock dies with its process (so a crashed writer's partial
  // becomes collectable), while the age threshold keeps process B from ever
  // deleting process A's live partial in a lock-handoff instant (spec §5).
  const auto now = fs::file_time_type::clock::now();
  for (const auto& entry : fs::directory_iterator(root_, ec)) {
    std::error_code entry_ec;
    if (!entry.is_regular_file(entry_ec) || entry_ec) {
      continue;
    }
    const std::string name = entry.path().filename().string();
    const auto partial_pos = name.find(".mcap.partial.");
    if (partial_pos == std::string::npos) {
      continue;
    }
    const auto mtime = fs::last_write_time(entry.path(), entry_ec);
    if (entry_ec || now - mtime < cfg.orphan_partial_age) {
      continue;
    }
    const std::string base = name.substr(0, partial_pos) + ".mcap";
    std::string lock_error;
    const auto lock = FileLock::tryExclusive(root_ / (base + ".lock"), &lock_error);
    if (!lock.has_value()) {
      continue;  // a live materialization owns this identity
    }
    fs::remove(entry.path(), entry_ec);
  }

  // Pass 2: LRU eviction by touch-stamp order until BOTH budgets hold. Each
  // victim's identity lock is taken non-blocking first — busy means a live
  // materialization (or, in stage 4, a shared dataset lease): skip it.
  struct Candidate {
    fs::path file;
    std::uintmax_t size;
    fs::file_time_type stamp;
  };
  std::vector<Candidate> candidates;
  std::uintmax_t total = 0;
  for (const auto& entry : fs::directory_iterator(root_, ec)) {
    std::error_code entry_ec;
    if (!entry.is_regular_file(entry_ec) || entry_ec) {
      continue;
    }
    if (!entry.path().filename().string().ends_with(".mcap")) {
      continue;  // partials, .touch and .lock sidecars are not evictable
    }
    const std::uintmax_t size = entry.file_size(entry_ec);
    if (entry_ec) {
      continue;
    }
    total += size;
    auto stamp = fs::last_write_time(touchPathFor(entry.path()), entry_ec);
    if (entry_ec) {
      // No touch sidecar (pre-stamp file or a deleted stamp): fall back to
      // the file's own mtime so it still participates in the order.
      stamp = fs::last_write_time(entry.path(), entry_ec);
      if (entry_ec) {
        stamp = fs::file_time_type::min();
      }
    }
    candidates.push_back({entry.path(), size, stamp});
  }
  std::sort(candidates.begin(), candidates.end(),
            [](const Candidate& a, const Candidate& b) { return a.stamp < b.stamp; });
  const auto overBudget = [&] {
    if (total > cfg.max_total_bytes) {
      return true;
    }
    std::error_code space_ec;
    const fs::space_info space = fs::space(root_, space_ec);
    return !space_ec && space.available < cfg.min_free_bytes;
  };
  for (const Candidate& victim : candidates) {
    if (!overBudget()) {
      break;
    }
    std::string lock_error;
    const auto lock = FileLock::tryExclusive(lockPathFor(victim.file), &lock_error);
    if (!lock.has_value()) {
      continue;  // leased or re-materializing: never evict under a holder
    }
    std::error_code remove_ec;
    if (!fs::remove(victim.file, remove_ec) || remove_ec) {
      continue;
    }
    total -= victim.size;
    fs::remove(touchPathFor(victim.file), remove_ec);
  }
}

}  // namespace mcap_cloud
