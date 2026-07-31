// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// ImportRuntime (stage-4 PR-1, HERMETIC): the per-toolbox-instance runtime
// shared by the interactive dialog path and the future descriptor-import
// provider. Pins:
//   - the IN-MEMORY trust set: preloaded from the TrustedOrigins ledger at
//     construction, write-through on recordSuccessfulHello, and BOUNDED
//     isTrusted (no file I/O per call — proven by deleting the ledger file
//     after preload; trusted_origins.cpp used to re-read it per call);
//   - the keyed active-materialization registry (in-process contention);
//   - the shared thread-safe SessionCache (concurrency smoke);
//   - the CacheTee single-encoder path: materialize through registry + file
//     lock + exclusive 0600 sink + provenance record + bounded writer queue,
//     finalize on the real SessionFileCache (incl. the wrong-ExpectedContent
//     rejection: no cache file, partial removed), and the abort path (cache
//     partials never survive, spec §10).
// Roots are injected private temp dirs — never the real config/cache dirs.
#include "import_runtime.hpp"

#include <gtest/gtest.h>

#include <atomic>
#include <filesystem>
#include <functional>
#include <system_error>
#include <fstream>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "session_file_cache.hpp"
#include "source_descriptor.hpp"
#include "trusted_origins.hpp"

namespace {

namespace fs = std::filesystem;

struct TempRoot {
  explicit TempRoot(const std::string& name) {
    path = fs::temp_directory_path() / ("mcap-cloud-import-runtime-test-" + name);
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
  d.display_name = "runtime-test";
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

mcap_cloud::DecodedMessage message(std::uint32_t topic_id, std::int64_t t) {
  return {.topic_id = topic_id, .schema_id = 5, .log_time_ns = t, .publish_time_ns = t,
          .payload = std::string(512, 'x')};
}

mcap_cloud::ImportRuntime makeRuntime(const TempRoot& cache_root, const TempRoot& config_root) {
  return mcap_cloud::ImportRuntime(mcap_cloud::SessionFileCache(cache_root.path),
                                   mcap_cloud::TrustedOrigins(config_root.path));
}

}  // namespace

// Trust set: preloaded at construction; queries are pure in-memory (bounded,
// §6.3) — deleting the ledger file after preload must not change answers.
TEST(McapCloudImportRuntime, TrustSetIsPreloadedAndBounded) {
  TempRoot cache_root("trust-cache");
  TempRoot config_root("trust-config");

  // Seed the ledger BEFORE the runtime exists (a prior interactive session).
  mcap_cloud::TrustedOrigins seed(config_root.path);
  ASSERT_TRUE(seed.recordSuccessfulHello("ws://seeded:8080"));

  mcap_cloud::ImportRuntime rt = makeRuntime(cache_root, config_root);
  EXPECT_TRUE(rt.isTrusted("ws://seeded:8080"));
  // Default-port normalization rides the same origin key as the ledger.
  EXPECT_FALSE(rt.isTrusted("ws://unseeded:8080"));
  EXPECT_FALSE(rt.isTrusted("not a uri"));

  // Remove the ledger file: answers must NOT change (no per-call file I/O).
  std::error_code ec;
  fs::remove(config_root.path / "trusted_origins.json", ec);
  ASSERT_FALSE(fs::exists(config_root.path / "trusted_origins.json"));
  EXPECT_TRUE(rt.isTrusted("ws://seeded:8080"));
  EXPECT_FALSE(rt.isTrusted("ws://unseeded:8080"));
}

// recordSuccessfulHello updates the in-memory set AND writes through to the
// ledger (a fresh TrustedOrigins instance sees it).
TEST(McapCloudImportRuntime, RecordSuccessfulHelloWritesThrough) {
  TempRoot cache_root("record-cache");
  TempRoot config_root("record-config");
  mcap_cloud::ImportRuntime rt = makeRuntime(cache_root, config_root);

  EXPECT_FALSE(rt.isTrusted("wss://new-server"));
  ASSERT_TRUE(rt.recordSuccessfulHello("wss://new-server"));
  EXPECT_TRUE(rt.isTrusted("wss://new-server"));
  // Effective-port normalization: the default-port spelling is the same origin.
  EXPECT_TRUE(rt.isTrusted("wss://new-server:443"));

  mcap_cloud::TrustedOrigins fresh(config_root.path);
  EXPECT_TRUE(fresh.isTrusted("wss://new-server")) << "write-through to the ledger is mandatory";
}

// Keyed active-materialization registry: in-process contention detection.
TEST(McapCloudImportRuntime, MaterializeRegistryDetectsInProcessContention) {
  TempRoot cache_root("registry-cache");
  TempRoot config_root("registry-config");
  mcap_cloud::ImportRuntime rt = makeRuntime(cache_root, config_root);

  const std::string id_a = mcap_cloud::descriptorIdentity(descriptor("a.mcap"));
  const std::string id_b = mcap_cloud::descriptorIdentity(descriptor("b.mcap"));

  auto ticket_a = rt.tryBeginMaterialize(id_a);
  ASSERT_TRUE(ticket_a.has_value());
  EXPECT_FALSE(rt.tryBeginMaterialize(id_a).has_value()) << "same identity must be busy";
  EXPECT_TRUE(rt.tryBeginMaterialize(id_b).has_value()) << "different identity is independent";

  ticket_a.reset();
  EXPECT_TRUE(rt.tryBeginMaterialize(id_a).has_value()) << "released identity is re-acquirable";
}

// Shared SessionCache concurrency smoke: hammer store/lookup/evict from
// several threads; thread-safety is D7's contract for the shared instance.
TEST(McapCloudImportRuntime, SessionCacheConcurrencySmoke) {
  TempRoot cache_root("cc-cache");
  TempRoot config_root("cc-config");
  mcap_cloud::ImportRuntime rt = makeRuntime(cache_root, config_root);

  auto present = [](const mcap_cloud::CachedSession&) { return true; };
  std::atomic<bool> failed{false};
  std::vector<std::thread> threads;
  for (int t = 0; t < 4; ++t) {
    threads.emplace_back([&, t] {
      for (int i = 0; i < 500; ++i) {
        const PJ::cloud::SessionKey key = PJ::cloud::computeSessionKey(
            "ws://h", {"file_" + std::to_string((t * 500 + i) % 16)}, {"/a"}, {0, 0}, true);
        mcap_cloud::CachedSession e;
        e.display_name = "d" + std::to_string(t);
        e.dataset_id = static_cast<std::uint32_t>(t + 1);
        e.total_messages = static_cast<std::uint64_t>(i);
        rt.sessionCache().store(key, e);
        auto hit = rt.sessionCache().lookup(key, present);
        if (hit.has_value() && hit->dataset_id == 0) {
          failed.store(true);
        }
        if (i % 7 == 0) {
          rt.sessionCache().evict(key);
        }
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  EXPECT_FALSE(failed.load());
  EXPECT_LE(rt.sessionCache().size(), rt.sessionCache().maxEntries());
}

// CacheTee happy path: begin -> openWriter (exclusive 0600 sink + embedded
// provenance) -> enqueue -> drainAndClose -> finalize; the published file
// passes the cache's own validated lookup.
TEST(McapCloudImportRuntime, CacheTeeMaterializesAValidCacheFile) {
  TempRoot cache_root("tee-ok-cache");
  TempRoot config_root("tee-ok-config");
  mcap_cloud::ImportRuntime rt = makeRuntime(cache_root, config_root);

  const auto d = descriptor("tee.mcap");
  const std::string identity = mcap_cloud::descriptorIdentity(d);

  mcap_cloud::CacheTee tee(rt);
  std::string error;
  ASSERT_TRUE(tee.begin(identity, &error)) << error;
  ASSERT_TRUE(tee.openWriter(sessionInfo(), mcap_cloud::canonicalSourceDescriptorJson(d),
                             /*estimated_bytes=*/1024, &error))
      << error;
  for (int i = 0; i < 8; ++i) {
    ASSERT_TRUE(tee.enqueue(message(11, 100 + i)));
  }
  ASSERT_TRUE(tee.drainAndClose(&error)) << error;
  ASSERT_TRUE(tee.finalize(std::nullopt, &error)) << error;

  fs::path out;
  EXPECT_TRUE(rt.fileCache().lookup(identity, &out)) << "finalized tee file must validate";
  EXPECT_EQ(out, tee.finalPath());
#if !defined(_WIN32)
  const fs::perms perms = fs::status(out).permissions();
  EXPECT_EQ(perms & (fs::perms::group_all | fs::perms::others_all), fs::perms::none)
      << "cache files are private (0600, exclusive-create sink)";
#endif
}

// finalize with a wrong ExpectedContent (the semantic-completeness pin): no
// cache file appears and the partial is removed.
TEST(McapCloudImportRuntime, CacheTeeFinalizeRejectsWrongExpectedContent) {
  TempRoot cache_root("tee-expected-cache");
  TempRoot config_root("tee-expected-config");
  mcap_cloud::ImportRuntime rt = makeRuntime(cache_root, config_root);

  const auto d = descriptor("wrong-expected.mcap");
  const std::string identity = mcap_cloud::descriptorIdentity(d);

  mcap_cloud::CacheTee tee(rt);
  std::string error;
  ASSERT_TRUE(tee.begin(identity, &error)) << error;
  ASSERT_TRUE(tee.openWriter(sessionInfo(), mcap_cloud::canonicalSourceDescriptorJson(d), 0, &error))
      << error;
  ASSERT_TRUE(tee.enqueue(message(11, 100)));
  ASSERT_TRUE(tee.enqueue(message(11, 101)));
  ASSERT_TRUE(tee.drainAndClose(&error)) << error;
  const fs::path partial = tee.partialPath();
  ASSERT_TRUE(fs::exists(partial));

  EXPECT_FALSE(tee.finalize(
      mcap_cloud::SessionFileCache::ExpectedContent{.message_count = 99, .channel_count = 1},
      &error));
  EXPECT_NE(error.find("Statistics mismatch"), std::string::npos) << error;
  EXPECT_FALSE(fs::exists(partial)) << "failed finalize removes the partial";
  EXPECT_FALSE(fs::exists(rt.fileCache().pathFor(identity))) << "no cache file may appear";
}

// Abort mid-materialization (the cancel path): the partial never survives and
// the identity lock + registry slot are released for the next attempt.
TEST(McapCloudImportRuntime, CacheTeeAbortRemovesPartialAndReleasesLocks) {
  TempRoot cache_root("tee-abort-cache");
  TempRoot config_root("tee-abort-config");
  mcap_cloud::ImportRuntime rt = makeRuntime(cache_root, config_root);

  const auto d = descriptor("abort.mcap");
  const std::string identity = mcap_cloud::descriptorIdentity(d);

  fs::path partial;
  {
    mcap_cloud::CacheTee tee(rt);
    std::string error;
    ASSERT_TRUE(tee.begin(identity, &error)) << error;
    ASSERT_TRUE(
        tee.openWriter(sessionInfo(), mcap_cloud::canonicalSourceDescriptorJson(d), 0, &error))
        << error;
    ASSERT_TRUE(tee.enqueue(message(11, 100)));
    partial = tee.partialPath();
    tee.abortAndCleanup();
    EXPECT_FALSE(fs::exists(partial)) << "cache partials never survive (spec §10)";
    // Idempotent: destructor runs after an explicit abort without harm.
  }
  EXPECT_FALSE(fs::exists(partial));
  EXPECT_FALSE(fs::exists(rt.fileCache().pathFor(identity)));

  // Both the in-process registry slot and the cross-process file lock are free.
  EXPECT_TRUE(rt.tryBeginMaterialize(identity).has_value());
  std::string error;
  EXPECT_TRUE(rt.fileCache().tryLockForMaterialize(identity, &error).has_value()) << error;
}

// In-process contention through the tee itself: a second CacheTee.begin on
// the same identity fails while the first is live (registry, not file lock).
TEST(McapCloudImportRuntime, CacheTeeBeginDetectsInProcessContention) {
  TempRoot cache_root("tee-contend-cache");
  TempRoot config_root("tee-contend-config");
  mcap_cloud::ImportRuntime rt = makeRuntime(cache_root, config_root);

  const auto d = descriptor("contend.mcap");
  const std::string identity = mcap_cloud::descriptorIdentity(d);

  mcap_cloud::CacheTee first(rt);
  std::string error;
  ASSERT_TRUE(first.begin(identity, &error)) << error;

  mcap_cloud::CacheTee second(rt);
  std::string error2;
  EXPECT_FALSE(second.begin(identity, &error2));
  EXPECT_FALSE(error2.empty());

  first.abortAndCleanup();
  mcap_cloud::CacheTee third(rt);
  EXPECT_TRUE(third.begin(identity, &error)) << error;
}

// IMPORTANT-2 (quality review): the enqueue backpressure wait MUST be
// cancellable cross-thread. A producer genuinely blocked on a full queue
// behind a stalled writer (the gated hook stands in for a hung disk) can
// only be freed by requestAbort() — before the fix, only the writer thread
// could signal queue_not_full_, so a hung disk wedged the pull thread AND
// the GUI cancel behind it.
TEST(McapCloudImportRuntime, CacheTeeRequestAbortUnblocksABackpressuredProducer) {
  TempRoot cache_root("abort-backpressure-cache");
  TempRoot config_root("abort-backpressure-config");
  mcap_cloud::ImportRuntime rt = makeRuntime(cache_root, config_root);

  const auto d = descriptor("backpressure.mcap");
  const std::string identity = mcap_cloud::descriptorIdentity(d);

  mcap_cloud::CacheTee tee(rt);
  std::string error;
  ASSERT_TRUE(tee.begin(identity, &error)) << error;

  // Stall the writer thread on its FIRST message until released; shrink the
  // queue so the producer hits backpressure after a couple of enqueues.
  std::atomic<bool> release_writer{false};
  tee.setQueueCapacityForTest(1);
  tee.setWriterHookForTest([&](const mcap_cloud::DecodedMessage&) {
    while (!release_writer.load()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
  });
  ASSERT_TRUE(tee.openWriter(sessionInfo(), mcap_cloud::canonicalSourceDescriptorJson(d), 0, &error))
      << error;

  std::atomic<int> enqueued{0};
  std::atomic<bool> last_enqueue_ok{true};
  std::thread producer([&] {
    for (int i = 0; i < 8; ++i) {
      const bool ok = tee.enqueue(message(11, 100 + i));
      last_enqueue_ok.store(ok);
      if (!ok) {
        return;  // the abort freed us
      }
      enqueued.fetch_add(1);
    }
  });

  // Wait until the producer is genuinely wedged (progress stalls while the
  // thread is still running: writer holds msg 1 in the hook, the queue holds
  // 1, and the next enqueue blocks on queue_not_full_).
  const auto stall_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  int last_seen = -1;
  int stable_polls = 0;
  while (std::chrono::steady_clock::now() < stall_deadline && stable_polls < 10) {
    const int now_count = enqueued.load();
    stable_polls = (now_count == last_seen) ? stable_polls + 1 : 0;
    last_seen = now_count;
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
  ASSERT_GE(stable_polls, 10) << "producer never stalled — the backpressure precondition failed";
  ASSERT_LT(last_seen, 8) << "producer finished without ever blocking";

  const auto abort_at = std::chrono::steady_clock::now();
  tee.requestAbort();
  producer.join();
  const auto elapsed = std::chrono::steady_clock::now() - abort_at;
  EXPECT_LT(elapsed, std::chrono::seconds(2))
      << "requestAbort must free a backpressured producer promptly";
  EXPECT_FALSE(last_enqueue_ok.load()) << "the freed enqueue must report the tee dropped";

  // Release the stalled writer so cleanup can join it; the abort path then
  // deletes the partial and releases both locks.
  release_writer.store(true);
  const std::filesystem::path partial = tee.partialPath();
  tee.abortAndCleanup();
  EXPECT_FALSE(fs::exists(partial)) << "cache partials never survive an abort";
  EXPECT_TRUE(rt.tryBeginMaterialize(identity).has_value());
  EXPECT_TRUE(rt.fileCache().tryLockForMaterialize(identity, &error).has_value()) << error;
}

// An unusable cache root (a FILE where the directory should be) fails the tee
// at begin() with a clear error — the caller (fetch) continues tee-less.
TEST(McapCloudImportRuntime, CacheTeeBeginFailsCleanlyOnUnusableRoot) {
  TempRoot config_root("badroot-config");
  TempRoot holder("badroot-holder");
  const fs::path bad_root = holder.path / "not-a-dir";
  {
    std::ofstream out(bad_root, std::ios::binary);
    out << "occupied";
  }
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(bad_root),
                               mcap_cloud::TrustedOrigins(config_root.path));

  const auto d = descriptor("badroot.mcap");
  mcap_cloud::CacheTee tee(rt);
  std::string error;
  EXPECT_FALSE(tee.begin(mcap_cloud::descriptorIdentity(d), &error));
  EXPECT_FALSE(error.empty());
}

// Adversarial F8 (the tee half): writer-thread spawn failure must degrade to
// the documented NONFATAL tee failure — openWriter returns false with the
// cause, no partial survives, the lock is released at cleanup, and nothing
// throws into the caller.
TEST(McapCloudCacheTee, WriterThreadSpawnFailureIsNonfatal) {
  TempRoot cache_root("spawnfail-cache");
  TempRoot config_root("spawnfail-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));
  const std::string identity =
      "mcap-cloud:v1:sha256/128:11111111111111111111111111111111";

  mcap_cloud::SessionInfo info;
  mcap_cloud::SessionTopic topic;
  topic.topic_id = 1;
  topic.topic_name = "/one";
  topic.schema_id = 5;
  info.topics.push_back(topic);
  mcap_cloud::SessionSchema schema;
  schema.schema_id = 5;
  schema.name = "demo/msg/One";
  schema.encoding = "ros2msg";
  schema.data = "int32 value";
  info.schemas.push_back(schema);

  {
    mcap_cloud::CacheTee tee(rt);
    std::string begin_error;
    ASSERT_TRUE(tee.begin(identity, &begin_error)) << begin_error;
    tee.setThreadFactoryForTest([](std::function<void()>) -> std::thread {
      throw std::system_error(std::make_error_code(std::errc::resource_unavailable_try_again),
                              "injected spawn failure");
    });
    std::string open_error;
    EXPECT_FALSE(tee.openWriter(info, "{}", 0, &open_error))
        << "spawn failure must be a reported tee failure, not success";
    EXPECT_NE(open_error.find("writer thread"), std::string::npos) << open_error;
    tee.abortAndCleanup();
  }
  // Nothing survives: no partial, and the exclusive lock is free again.
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator(cache_root.path, ec)) {
    EXPECT_EQ(entry.path().filename().string().find(".mcap.partial."), std::string::npos)
        << "leftover partial: " << entry.path();
  }
  std::string err;
  EXPECT_TRUE(rt.fileCache().tryLockForMaterialize(identity, &err).has_value()) << err;
}

// Adversarial F10: the pull-admission gate is FIFO and cancelable — the
// holder's release hands the gate to the LONGEST-waiting acquirer, and a
// queued waiter whose cancel predicate fires leaves without disturbing the
// order.
TEST(McapCloudImportRuntime, AdmissionGateIsFifoAndCancelable) {
  TempRoot cache_root("admission-cache");
  TempRoot config_root("admission-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  auto first = rt.acquireAdmission({}, std::chrono::milliseconds(5));
  ASSERT_TRUE(first.has_value());

  std::vector<int> order;
  std::mutex order_mu;
  std::atomic<bool> cancel_c{false};
  std::atomic<int> queued{0};

  auto waiter = [&](int id, std::atomic<bool>* cancel) {
    queued.fetch_add(1);
    auto ticket = rt.acquireAdmission(
        [cancel]() { return cancel != nullptr && cancel->load(); },
        std::chrono::milliseconds(5));
    if (ticket.has_value()) {
      const std::lock_guard<std::mutex> lock(order_mu);
      order.push_back(id);
    }
  };
  // Deterministic arrival order: start each waiter only after the previous
  // one is queued (queued count + a settle sleep).
  std::thread tb([&]() { waiter(2, nullptr); });
  while (queued.load() < 1) { std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  std::thread tc([&]() { waiter(3, &cancel_c); });
  while (queued.load() < 2) { std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
  std::this_thread::sleep_for(std::chrono::milliseconds(30));
  std::thread td([&]() { waiter(4, nullptr); });
  while (queued.load() < 3) { std::this_thread::sleep_for(std::chrono::milliseconds(1)); }
  std::this_thread::sleep_for(std::chrono::milliseconds(30));

  cancel_c.store(true);  // the middle waiter leaves the queue
  tc.join();
  first.reset();  // release: FIFO hands to 2, then 2's release hands to 4
  tb.join();
  td.join();

  ASSERT_EQ(order.size(), 2u);
  EXPECT_EQ(order[0], 2) << "FIFO: the longest-waiting acquirer is admitted first";
  EXPECT_EQ(order[1], 4);
}
