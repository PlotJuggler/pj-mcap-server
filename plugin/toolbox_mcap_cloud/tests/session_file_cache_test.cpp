// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Session file cache core (spec docs/canonical-layout-import.md §5), fully
// hermetic: a private temp root is injected (never the real $XDG_CACHE_HOME),
// and every valid fixture is a REAL MCAP built with SessionMcapWriter carrying
// the canonical source descriptor as embedded provenance — exactly what the
// stage-4 cache tee will produce.
#include "session_file_cache.hpp"

#include <gtest/gtest.h>
#include <mcap/writer.hpp>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>

#include "source_descriptor.hpp"
#include "session_mcap_writer.hpp"

namespace {

namespace fs = std::filesystem;

// Private per-test cache root, wiped on both ends.
struct TempRoot {
  explicit TempRoot(const std::string& name) {
    path = fs::temp_directory_path() / ("mcap-cloud-cache-test-" + name);
    std::error_code ec;
    fs::remove_all(path, ec);
    fs::create_directories(path);
  }
  ~TempRoot() {
    std::error_code ec;
    fs::remove_all(path, ec);
  }
  fs::path path;
};

mcap_cloud::SourceDescriptor descriptor(const std::string& key) {
  mcap_cloud::SourceDescriptor d;
  d.version = 1;
  d.kind = "mcap-cloud-session";
  d.server_uri = "ws://localhost:8080";
  d.s3_keys = {key};
  d.topics = {};
  d.start_ns = 0;
  d.end_ns = 0;
  d.include_latched = true;
  d.display_name = "cache-test";
  return d;
}

mcap_cloud::SessionInfo sessionInfo() {
  mcap_cloud::SessionInfo info;
  info.schemas = {
      {.schema_id = 5, .name = "demo/msg/One", .encoding = "ros2msg", .data = "int32 value"},
  };
  info.topics = {
      {.topic_id = 11, .topic_name = "/one", .schema_id = 5, .message_encoding = "cdr"},
  };
  return info;
}

// Write a valid, summarized session MCAP at `path` embedding
// `embedded_canonical` as the source-descriptor provenance record.
void writeSessionMcap(const fs::path& path, const std::string& embedded_canonical) {
  mcap_cloud::SessionMcapWriter writer;
  std::string error;
  ASSERT_TRUE(writer.open(path, sessionInfo(), &error)) << error;
  ASSERT_TRUE(writer.writeMetadata("mcap_cloud/source_descriptor", embedded_canonical, &error))
      << error;
  ASSERT_TRUE(writer.write(
      {.topic_id = 11, .schema_id = 5, .log_time_ns = 100, .publish_time_ns = 90,
       .payload = std::string(2048, 'x')},
      &error))
      << error;
  ASSERT_TRUE(writer.close(&error)) << error;
}

// Materialize `d` into the cache through the real lock -> partial -> finalize
// path; returns the finalized path.
fs::path materialize(mcap_cloud::SessionFileCache& cache, const mcap_cloud::SourceDescriptor& d) {
  const std::string identity = mcap_cloud::descriptorIdentity(d);
  std::string error;
  auto lock = cache.tryLockForMaterialize(identity, &error);
  EXPECT_TRUE(lock.has_value()) << error;
  if (!lock.has_value()) {
    return {};
  }
  writeSessionMcap(cache.partialPathFor(*lock), mcap_cloud::canonicalSourceDescriptorJson(d));
  EXPECT_TRUE(cache.finalize(
      *lock, mcap_cloud::SessionFileCache::ExpectedContent{.message_count = 1, .channel_count = 1},
      &error))
      << error;
  return cache.pathFor(identity);
}

const std::string kValidHexA(32, 'a');
const std::string kValidHexB(32, 'b');

std::string identityFor(const std::string& hex) {
  return "mcap-cloud:v1:sha256/128:" + hex;
}

}  // namespace

TEST(SessionFileCache, PathForShapeAndIdentityValidation) {
  TempRoot root("pathfor");
  mcap_cloud::SessionFileCache cache(root.path);

  EXPECT_EQ(cache.pathFor(identityFor(kValidHexA)), root.path / (kValidHexA + ".mcap"));
  // The identity computed by the descriptor module is accepted as-is.
  const std::string identity = mcap_cloud::descriptorIdentity(descriptor("a.mcap"));
  EXPECT_FALSE(cache.pathFor(identity).empty());

  // Malformed identities never map to a path (and can never escape the root).
  EXPECT_TRUE(cache.pathFor("").empty());
  EXPECT_TRUE(cache.pathFor("garbage").empty());
  EXPECT_TRUE(cache.pathFor("mcap-cloud:v2:sha256/128:" + kValidHexA).empty());     // version
  EXPECT_TRUE(cache.pathFor("mcap-cloud:v1:sha256/256:" + kValidHexA).empty());     // algo tag
  EXPECT_TRUE(cache.pathFor(identityFor(std::string(31, 'a'))).empty());            // short
  EXPECT_TRUE(cache.pathFor(identityFor(std::string(33, 'a'))).empty());            // long
  EXPECT_TRUE(cache.pathFor(identityFor(std::string(32, 'A'))).empty());            // uppercase
  EXPECT_TRUE(cache.pathFor(identityFor(std::string(32, 'g'))).empty());            // non-hex
  EXPECT_TRUE(cache.pathFor("mcap-cloud:v1:sha256/128:../../" + kValidHexA).empty());

  // The lock path surfaces a DISTINCT error for a garbage identity.
  std::string error;
  EXPECT_FALSE(cache.tryLockForMaterialize("garbage", &error).has_value());
  EXPECT_NE(error.find("identity"), std::string::npos) << error;

  // A malformed identity is a plain lookup miss.
  fs::path out;
  EXPECT_FALSE(cache.lookup("garbage", &out));
}

TEST(SessionFileCache, MaterializeFinalizeLookupRoundTrip) {
  TempRoot root("roundtrip");
  mcap_cloud::SessionFileCache cache(root.path);
  const auto d = descriptor("a.mcap");
  const std::string identity = mcap_cloud::descriptorIdentity(d);

  std::string error;
  auto lock = cache.tryLockForMaterialize(identity, &error);
  ASSERT_TRUE(lock.has_value()) << error;
  const fs::path partial = cache.partialPathFor(*lock);
  EXPECT_EQ(partial.parent_path(), root.path);
  // Per-process partial: <hex>.mcap.partial.<pid>.
  EXPECT_NE(partial.filename().string().find(".mcap.partial."), std::string::npos);

  writeSessionMcap(partial, mcap_cloud::canonicalSourceDescriptorJson(d));
  ASSERT_TRUE(cache.finalize(
      *lock, mcap_cloud::SessionFileCache::ExpectedContent{.message_count = 1, .channel_count = 1},
      &error))
      << error;
  EXPECT_FALSE(fs::exists(partial));

  const fs::path final_path = cache.pathFor(identity);
  ASSERT_TRUE(fs::exists(final_path));
#if !defined(_WIN32)
  // Private cache: file 0600 (dir tightening is exercised implicitly — the
  // injected root pre-exists here).
  const fs::perms perms = fs::status(final_path).permissions();
  EXPECT_EQ(perms & (fs::perms::group_all | fs::perms::others_all), fs::perms::none);
#endif

  // finalize stamped the touch sidecar; a lookup hit advances it.
  const fs::path stamp(final_path.string() + ".touch");
  ASSERT_TRUE(fs::exists(stamp));
  const auto t0 = fs::last_write_time(stamp);
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  fs::path out;
  ASSERT_TRUE(cache.lookup(identity, &out));
  EXPECT_EQ(out, final_path);
  EXPECT_GT(fs::last_write_time(stamp), t0);

  // A different identity is a miss; so is a corrupt file — which lookup does
  // NOT delete (deletion policy belongs to the provider flow, spec §5).
  EXPECT_FALSE(cache.lookup(mcap_cloud::descriptorIdentity(descriptor("b.mcap")), &out));
  {
    std::ofstream corrupt(final_path, std::ios::binary | std::ios::trunc);
    corrupt << "not an mcap";
  }
  EXPECT_FALSE(cache.lookup(identity, &out));
  EXPECT_TRUE(fs::exists(final_path));
}

TEST(SessionFileCache, FinalizeRejectsTruncatedFile) {
  TempRoot root("truncated");
  mcap_cloud::SessionFileCache cache(root.path);
  const auto d = descriptor("a.mcap");
  std::string error;
  auto lock = cache.tryLockForMaterialize(mcap_cloud::descriptorIdentity(d), &error);
  ASSERT_TRUE(lock.has_value()) << error;
  const fs::path partial = cache.partialPathFor(*lock);
  writeSessionMcap(partial, mcap_cloud::canonicalSourceDescriptorJson(d));
  fs::resize_file(partial, fs::file_size(partial) - 64);  // chop the footer

  EXPECT_FALSE(cache.finalize(*lock, std::nullopt, &error));
  EXPECT_FALSE(error.empty());
  EXPECT_FALSE(fs::exists(partial));  // failed finalize removes the partial
  EXPECT_FALSE(fs::exists(cache.pathFor(mcap_cloud::descriptorIdentity(d))));
}

TEST(SessionFileCache, FinalizeRejectsNonMcapJunk) {
  TempRoot root("junk");
  mcap_cloud::SessionFileCache cache(root.path);
  std::string error;
  auto lock = cache.tryLockForMaterialize(identityFor(kValidHexA), &error);
  ASSERT_TRUE(lock.has_value()) << error;
  const fs::path partial = cache.partialPathFor(*lock);
  {
    std::ofstream out(partial, std::ios::binary);
    out << "definitely not an mcap";
  }
  EXPECT_FALSE(cache.finalize(*lock, std::nullopt, &error));
  EXPECT_FALSE(fs::exists(partial));
}

TEST(SessionFileCache, FinalizeRejectsMissingSummary) {
  TempRoot root("nosummary");
  mcap_cloud::SessionFileCache cache(root.path);
  std::string error;
  auto lock = cache.tryLockForMaterialize(identityFor(kValidHexA), &error);
  ASSERT_TRUE(lock.has_value()) << error;
  const fs::path partial = cache.partialPathFor(*lock);
  {
    // A structurally valid MCAP WITHOUT a summary section: readSummary
    // (NoFallbackScan) must fail it — the validate-reopen step is mandatory
    // because the vendored writer path ignores short writes (spec §5).
    mcap::McapWriterOptions options("");
    options.noSummary = true;
    mcap::McapWriter raw;
    const mcap::Status open_status = raw.open(partial.string(), options);
    ASSERT_TRUE(open_status.ok()) << open_status.message;
    mcap::Schema schema("demo/msg/One", "ros2msg", "int32 value");
    raw.addSchema(schema);
    mcap::Channel channel("/one", "cdr", schema.id);
    raw.addChannel(channel);
    mcap::Message message;
    message.channelId = channel.id;
    message.logTime = 1;
    message.publishTime = 1;
    const std::string payload = "x";
    message.data = reinterpret_cast<const std::byte*>(payload.data());
    message.dataSize = payload.size();
    ASSERT_TRUE(raw.write(message).ok());
    raw.close();
  }
  EXPECT_FALSE(cache.finalize(*lock, std::nullopt, &error));
  EXPECT_FALSE(fs::exists(partial));
}

TEST(SessionFileCache, FinalizeRejectsDescriptorIdentityMismatchAndAbsence) {
  TempRoot root("mismatch");
  mcap_cloud::SessionFileCache cache(root.path);
  const auto d1 = descriptor("a.mcap");
  const auto d2 = descriptor("b.mcap");
  std::string error;

  // Embedded provenance for a DIFFERENT request -> wrong-file substitution
  // is detected from the file alone.
  {
    auto lock = cache.tryLockForMaterialize(mcap_cloud::descriptorIdentity(d1), &error);
    ASSERT_TRUE(lock.has_value()) << error;
    const fs::path partial = cache.partialPathFor(*lock);
    writeSessionMcap(partial, mcap_cloud::canonicalSourceDescriptorJson(d2));
    EXPECT_FALSE(cache.finalize(*lock, std::nullopt, &error));
    EXPECT_NE(error.find("identity"), std::string::npos) << error;
    EXPECT_FALSE(fs::exists(partial));
  }

  // No provenance at all: a cache file must self-describe (spec §5) — the
  // tee always embeds the descriptor, so absence at finalize is a defect.
  {
    auto lock = cache.tryLockForMaterialize(mcap_cloud::descriptorIdentity(d1), &error);
    ASSERT_TRUE(lock.has_value()) << error;
    const fs::path partial = cache.partialPathFor(*lock);
    mcap_cloud::SessionMcapWriter writer;
    ASSERT_TRUE(writer.open(partial, sessionInfo(), &error)) << error;
    ASSERT_TRUE(writer.write(
        {.topic_id = 11, .schema_id = 5, .log_time_ns = 100, .publish_time_ns = 90,
         .payload = "one"},
        &error))
        << error;
    ASSERT_TRUE(writer.close(&error)) << error;
    EXPECT_FALSE(cache.finalize(*lock, std::nullopt, &error));
    EXPECT_FALSE(fs::exists(partial));
  }
}

// Semantic completeness (review-caught): a cleanly-closed writer over a PREFIX
// of a stream is a structurally valid, summarized MCAP with matching
// provenance — only the expected-count comparison can reject it.
TEST(SessionFileCache, FinalizeRejectsStatisticsCountMismatch) {
  TempRoot root("countmismatch");
  mcap_cloud::SessionFileCache cache(root.path);
  const auto d = descriptor("a.mcap");
  std::string error;
  auto lock = cache.tryLockForMaterialize(mcap_cloud::descriptorIdentity(d), &error);
  ASSERT_TRUE(lock.has_value()) << error;
  const fs::path partial = cache.partialPathFor(*lock);
  writeSessionMcap(partial, mcap_cloud::canonicalSourceDescriptorJson(d));  // 1 msg / 1 channel

  EXPECT_FALSE(cache.finalize(
      *lock, mcap_cloud::SessionFileCache::ExpectedContent{.message_count = 2, .channel_count = 1},
      &error));
  EXPECT_NE(error.find("Statistics mismatch"), std::string::npos) << error;
  EXPECT_FALSE(fs::exists(partial));  // failed finalize removes the partial
  EXPECT_FALSE(fs::exists(cache.pathFor(mcap_cloud::descriptorIdentity(d))));
}

// Lookup fail-closed (review-caught): a valid summarized MCAP WITHOUT the
// embedded source-descriptor provenance dropped at <digest>.mcap (e.g. under
// an overridden MCAP_CLOUD_CACHE_DIR) must be a MISS, not a hit — every
// legitimately finalized cache file carries the provenance record.
TEST(SessionFileCache, LookupRejectsForeignFileWithoutProvenance) {
  TempRoot root("foreignfile");
  mcap_cloud::SessionFileCache cache(root.path);
  const auto d = descriptor("a.mcap");
  const std::string identity = mcap_cloud::descriptorIdentity(d);

  // A real, summarized session MCAP — just no provenance record.
  const fs::path file = cache.pathFor(identity);
  {
    mcap_cloud::SessionMcapWriter writer;
    std::string error;
    ASSERT_TRUE(writer.open(file, sessionInfo(), &error)) << error;
    ASSERT_TRUE(writer.write(
        {.topic_id = 11, .schema_id = 5, .log_time_ns = 100, .publish_time_ns = 90,
         .payload = "one"},
        &error))
        << error;
    ASSERT_TRUE(writer.close(&error)) << error;
  }
  ASSERT_TRUE(fs::exists(file));

  fs::path out;
  EXPECT_FALSE(cache.lookup(identity, &out))
      << "a provenance-less MCAP at the digest path must be a miss";
}

TEST(SessionFileCache, SecondMaterializeLockOnSameIdentityFailsWhileHeld) {
  TempRoot root("lock");
  mcap_cloud::SessionFileCache cache(root.path);
  const std::string identity = identityFor(kValidHexA);

  std::string error;
  auto lock1 = cache.tryLockForMaterialize(identity, &error);
  ASSERT_TRUE(lock1.has_value()) << error;

  std::string error2;
  EXPECT_FALSE(cache.tryLockForMaterialize(identity, &error2).has_value());
  EXPECT_FALSE(error2.empty());

  // A DIFFERENT identity is independent.
  EXPECT_TRUE(cache.tryLockForMaterialize(identityFor(kValidHexB), &error2).has_value())
      << error2;

  // Released -> re-acquirable.
  lock1.reset();
  EXPECT_TRUE(cache.tryLockForMaterialize(identity, &error2).has_value()) << error2;
}

TEST(SessionFileCache, CleanupRemovesStaleOrphanPartialsOnly) {
  TempRoot root("orphans");
  mcap_cloud::SessionFileCache cache(root.path);
  const auto stale_time = fs::file_time_type::clock::now() - std::chrono::hours(48);

  // Stale orphan from a dead foreign process: lock free + older than the
  // threshold -> removed.
  const fs::path stale = root.path / (kValidHexA + ".mcap.partial.4242");
  {
    std::ofstream out(stale, std::ios::binary);
    out << "junk";
  }
  fs::last_write_time(stale, stale_time);

  // Fresh orphan (same dead-process shape, recent mtime) -> kept.
  const fs::path fresh = root.path / (kValidHexB + ".mcap.partial.4242");
  {
    std::ofstream out(fresh, std::ios::binary);
    out << "junk";
  }

  // Stale partial whose identity lock is HELD (a live materialization) ->
  // kept regardless of age.
  const std::string held_identity = identityFor(std::string(32, 'c'));
  std::string error;
  auto held = cache.tryLockForMaterialize(held_identity, &error);
  ASSERT_TRUE(held.has_value()) << error;
  const fs::path live = cache.partialPathFor(*held);
  {
    std::ofstream out(live, std::ios::binary);
    out << "junk";
  }
  fs::last_write_time(live, stale_time);

  mcap_cloud::SessionFileCache::Config cfg;
  cfg.min_free_bytes = 0;
  cache.cleanup(cfg);

  EXPECT_FALSE(fs::exists(stale));
  EXPECT_TRUE(fs::exists(fresh));
  EXPECT_TRUE(fs::exists(live));
}

TEST(SessionFileCache, CleanupEvictsOldestTouchedFirstAndStopsAtCap) {
  TempRoot root("lru");
  mcap_cloud::SessionFileCache cache(root.path);
  const auto d_old = descriptor("old.mcap");
  const auto d_new = descriptor("new.mcap");
  const fs::path file_old = materialize(cache, d_old);
  const fs::path file_new = materialize(cache, d_new);
  ASSERT_TRUE(fs::exists(file_old));
  ASSERT_TRUE(fs::exists(file_new));

  // Order the LRU stamps explicitly (mtime of the .touch sidecars).
  const auto now = fs::file_time_type::clock::now();
  fs::last_write_time(fs::path(file_old.string() + ".touch"), now - std::chrono::hours(2));
  fs::last_write_time(fs::path(file_new.string() + ".touch"), now - std::chrono::hours(1));

  // One byte under the pair total: exactly one eviction (the oldest) gets
  // back under the cap, then eviction stops.
  mcap_cloud::SessionFileCache::Config cfg;
  cfg.max_total_bytes = fs::file_size(file_old) + fs::file_size(file_new) - 1;
  cfg.min_free_bytes = 0;
  cache.cleanup(cfg);

  EXPECT_FALSE(fs::exists(file_old));
  EXPECT_FALSE(fs::exists(fs::path(file_old.string() + ".touch")));
  EXPECT_TRUE(fs::exists(file_new));
  EXPECT_TRUE(fs::exists(fs::path(file_new.string() + ".touch")));
}

// Stage-4 shared read leases (spec §5): a live cache-backed consumer holds a
// SHARED lease on the identity's lock sidecar for the file's whole lifetime,
// so eviction (which takes the exclusive lock per victim) must skip it and
// resume once the lease is released.
TEST(SessionFileCache, CleanupSkipsSharedLeasedFileAndEvictsAfterRelease) {
  TempRoot root("lease-evict");
  mcap_cloud::SessionFileCache cache(root.path);
  const auto d = descriptor("leased.mcap");
  const std::string identity = mcap_cloud::descriptorIdentity(d);
  const fs::path file = materialize(cache, d);
  ASSERT_TRUE(fs::exists(file));

  std::string error;
  auto lease = cache.acquireReadLease(identity, &error);
  ASSERT_TRUE(lease.has_value()) << error;

  // A second shared lease on the SAME identity coexists (read leases stack).
  std::string error2;
  auto lease2 = cache.acquireReadLease(identity, &error2);
  EXPECT_TRUE(lease2.has_value()) << error2;

  mcap_cloud::SessionFileCache::Config cfg;
  cfg.max_total_bytes = 0;  // evict everything evictable
  cfg.min_free_bytes = 0;
  cache.cleanup(cfg);
  EXPECT_TRUE(fs::exists(file)) << "a shared-leased file must never be evicted";

  // While a shared lease is live, a materialization of the same identity is
  // refused (the lease holder is reading the file the writer would replace).
  std::string mat_error;
  EXPECT_FALSE(cache.tryLockForMaterialize(identity, &mat_error).has_value());

  lease.reset();
  lease2.reset();
  cache.cleanup(cfg);
  EXPECT_FALSE(fs::exists(file)) << "eviction must resume after the lease is released";
}

TEST(SessionFileCache, ReadLeaseRejectsMalformedIdentity) {
  TempRoot root("lease-identity");
  mcap_cloud::SessionFileCache cache(root.path);
  std::string error;
  EXPECT_FALSE(cache.acquireReadLease("garbage", &error).has_value());
  EXPECT_NE(error.find("identity"), std::string::npos) << error;
}

// FileLock-level shared/exclusive contract (POSIX flock LOCK_SH / Windows
// LockFileEx without LOCKFILE_EXCLUSIVE_LOCK): shared locks stack; an
// exclusive try fails while any shared holder is live and succeeds after.
TEST(SessionFileCache, FileLockSharedExclusiveContract) {
  TempRoot root("filelock-shared");
  const fs::path lock_path = root.path / "contract.lock";

  std::string error;
  auto shared1 = mcap_cloud::FileLock::tryShared(lock_path, &error);
  ASSERT_TRUE(shared1.has_value()) << error;
  auto shared2 = mcap_cloud::FileLock::tryShared(lock_path, &error);
  EXPECT_TRUE(shared2.has_value()) << "shared locks must coexist: " << error;

  EXPECT_FALSE(mcap_cloud::FileLock::tryExclusive(lock_path, &error).has_value())
      << "exclusive must fail while shared holders are live";

  shared1.reset();
  shared2.reset();
  EXPECT_TRUE(mcap_cloud::FileLock::tryExclusive(lock_path, &error).has_value()) << error;

  // And the inverse: an exclusive holder blocks a shared try.
  auto exclusive = mcap_cloud::FileLock::tryExclusive(lock_path, &error);
  ASSERT_TRUE(exclusive.has_value()) << error;
  EXPECT_FALSE(mcap_cloud::FileLock::tryShared(lock_path, &error).has_value());
}

TEST(SessionFileCache, CleanupSkipsLockedVictims) {
  TempRoot root("lru-locked");
  mcap_cloud::SessionFileCache cache(root.path);
  const auto d_old = descriptor("old.mcap");
  const auto d_new = descriptor("new.mcap");
  const fs::path file_old = materialize(cache, d_old);
  const fs::path file_new = materialize(cache, d_new);

  const auto now = fs::file_time_type::clock::now();
  fs::last_write_time(fs::path(file_old.string() + ".touch"), now - std::chrono::hours(2));
  fs::last_write_time(fs::path(file_new.string() + ".touch"), now - std::chrono::hours(1));

  // The oldest entry's identity lock is held (stage-4 leases share this lock
  // file): eviction must skip it and take the next victim instead.
  std::string error;
  auto held = cache.tryLockForMaterialize(mcap_cloud::descriptorIdentity(d_old), &error);
  ASSERT_TRUE(held.has_value()) << error;

  mcap_cloud::SessionFileCache::Config cfg;
  cfg.max_total_bytes = 0;  // evict everything evictable
  cfg.min_free_bytes = 0;
  cache.cleanup(cfg);

  EXPECT_TRUE(fs::exists(file_old));   // locked -> skipped
  EXPECT_FALSE(fs::exists(file_new));  // unlocked -> evicted
}

#if !defined(_WIN32)
TEST(SessionFileCache, StandardHonoursCacheDirOverride) {
  TempRoot root("standard");
  ASSERT_EQ(::setenv("MCAP_CLOUD_CACHE_DIR", root.path.string().c_str(), 1), 0);
  std::string error;
  mcap_cloud::SessionFileCache cache = mcap_cloud::SessionFileCache::standard(&error);
  EXPECT_EQ(cache.pathFor(identityFor(kValidHexA)), root.path / (kValidHexA + ".mcap"));
  ::unsetenv("MCAP_CLOUD_CACHE_DIR");
}
#endif

// Adversarial F13: a forged <digest>.mcap whose footer points at a
// multi-gigabyte summary span must be refused by raw-footer preflight BEFORE
// the MCAP summary parser allocates anything — a plain miss, on the GUI
// thread's fixed query budget. (Sparse file: huge st_size, tiny disk use.)
TEST(SessionFileCache, ForgedOversizedSummarySpanIsRefusedBeforeParsing) {
  TempRoot root("forged-summary");
  mcap_cloud::SessionFileCache cache(root.path);
  const std::string identity = "mcap-cloud:v1:sha256/128:22222222222222222222222222222222";
  const std::filesystem::path target = cache.pathFor(identity);
  {
    std::ofstream out(target, std::ios::binary);
    out.write("\x89MCAP0\r\n", 8);  // start magic
  }
  // Grow sparsely to ~64 MiB, then write a footer claiming summary_start=8:
  // span = size - tail - 8 >> the 16 MiB query budget.
  std::error_code ec;
  std::filesystem::resize_file(target, 64ull * 1024 * 1024, ec);
  ASSERT_FALSE(ec);
  {
    std::fstream out(target, std::ios::binary | std::ios::in | std::ios::out);
    // Footer record: op 0x02, len=20 (LE u64), summary_start=8 (LE u64),
    // summary_offset_start=0, summary_crc=0, then the end magic.
    unsigned char tail[37] = {0};
    tail[0] = 0x02;
    tail[1] = 20;                       // length LE
    tail[9] = 8;                        // summary_start LE = 8
    const char magic[8] = {'\x89', 'M', 'C', 'A', 'P', '0', '\r', '\n'};
    std::memcpy(tail + 29, magic, 8);
    out.seekp(-37, std::ios::end);
    out.write(reinterpret_cast<const char*>(tail), sizeof(tail));
  }
  std::filesystem::path out_path;
  EXPECT_FALSE(cache.lookup(identity, &out_path))
      << "a forged oversized summary span must be a miss (F13)";
}

// Adversarial F9: cross-process contention is DISTINGUISHABLE from OS
// failure. Two SessionFileCache instances over one root contend exactly like
// two processes (flock treats each FileLock's descriptor as an independent
// holder — see FileLock::tryExclusive's doc).
TEST(SessionFileCache, MaterializeLockContentionIsDistinguished) {
  TempRoot root("lock-contention");
  mcap_cloud::SessionFileCache a(root.path);
  mcap_cloud::SessionFileCache b(root.path);
  const std::string identity = "mcap-cloud:v1:sha256/128:33333333333333333333333333333333";

  std::string err;
  bool contended = true;
  auto lock_a = a.tryLockForMaterialize(identity, &err, &contended);
  ASSERT_TRUE(lock_a.has_value()) << err;
  EXPECT_FALSE(contended);

  auto lock_b = b.tryLockForMaterialize(identity, &err, &contended);
  EXPECT_FALSE(lock_b.has_value());
  EXPECT_TRUE(contended) << "held-elsewhere must classify as contention (F9)";

  // A malformed identity is an ERROR, never contention.
  contended = true;
  auto bad = b.tryLockForMaterialize("not-an-identity", &err, &contended);
  EXPECT_FALSE(bad.has_value());
  EXPECT_FALSE(contended);
}
