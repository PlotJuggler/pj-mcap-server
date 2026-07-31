// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC interactive save+cache matrix (stage-4 PR-1, D5-as-amended): the
// CACHE IS THE SOLE ENCODER — every runtime-bound fetch tees once into the
// SessionFileCache partial and export destinations receive byte COPIES
// (spec docs/canonical-layout-import.md §9 "cache sole encoder; exports byte
// copies"). Pins, against an in-process fake WS server that actually streams
// session batches:
//   - complete fetch: cache file finalized + valid (lookup passes), export
//     final byte-identical to the cache file, exactly-one Complete result;
//   - cancel mid-download: export keeps a READABLE .partial COPY (today's
//     deliberate retention), the cache partial NEVER survives (spec §10);
//   - tee failure: the fetch completes untouched (§9.6), outcome recorded;
//   - memory-hit rules (§6.1): valid disk -> served from memory + export by
//     copy with ZERO new sessions; missing disk -> entry evicted + refetch;
//   - RAII partial guard: session-connect failure and empty-plan exits leave
//     no cache partial and release the materialize lock;
//   - raw-tee parser independence (§9.0): a no-decoder session still
//     materializes the cache file (writer setup precedes the hasDecodable
//     early-exit).
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include <ixwebsocket/IXWebSocketServer.h>
#include <mcap/reader.hpp>

#include "fake_toolbox_host.hpp"
#include "fetch_worker.hpp"
#include "find_free_port.hpp"
#include "import_runtime.hpp"
#include "parser_ingest_test_support.hpp"
#include "pj_cloud.pb.h"
#include "session_file_cache.hpp"
#include "source_descriptor.hpp"
#include "trusted_origins.hpp"

namespace {

namespace fs = std::filesystem;
using mcap_cloud_test::findFreePort;

struct TempRoot {
  explicit TempRoot(const std::string& name) {
    path = fs::temp_directory_path() / ("mcap-cloud-tee-test-" + name);
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

constexpr std::uint32_t kTopicId = 1;
constexpr std::uint32_t kSchemaId = 5;
constexpr int kBatches = 3;
constexpr int kMessagesPerBatch = 4;
constexpr std::size_t kPayloadBytes = 1024;

// A fake pj_cloud server that ACTUALLY STREAMS a session: Hello ->
// HelloResponse; OpenSession -> plan (1 topic / 1 schema) then, per mode,
// either kBatches NONE-encoded batches + Eos{COMPLETE} or two batches
// followed by silence (the stall the cancel test needs).
class FakeStreamingServer {
 public:
  enum class Mode { kComplete, kStallAfterTwoBatches, kEmptyPlan };

  explicit FakeStreamingServer(Mode mode) : mode_(mode), port_(findFreePort()), server_(port_, "127.0.0.1") {
    server_.setOnClientMessageCallback([this](std::shared_ptr<ix::ConnectionState>,
                                              ix::WebSocket& ws,
                                              const ix::WebSocketMessagePtr& msg) {
      if (msg->type != ix::WebSocketMessageType::Message) {
        return;
      }
      pj_cloud::v1::ClientMessage request;
      if (!request.ParseFromString(msg->str)) {
        return;
      }
      if (request.has_hello()) {
        pj_cloud::v1::ServerMessage response;
        response.set_request_id(request.request_id());
        response.mutable_hello_response()->set_server_version("test-fake-1.0");
        send(ws, response);
        return;
      }
      if (!request.has_open_session()) {
        return;  // acks/cancels need no reply here
      }
      open_sessions_.fetch_add(1);
      pj_cloud::v1::ServerMessage response;
      response.set_request_id(request.request_id());
      auto* open = response.mutable_open_session();
      open->set_subscription_id(7);
      open->set_estimated_chunk_bytes(kBatches * kMessagesPerBatch * kPayloadBytes);
      open->set_approximate_messages(kBatches * kMessagesPerBatch);
      if (mode_ != Mode::kEmptyPlan) {
        auto* topic = open->add_topic_id_map();
        topic->set_topic_id(kTopicId);
        topic->set_topic_name("/one");
        topic->set_schema_id(kSchemaId);
        topic->set_message_encoding("cdr");
        auto* schema = open->add_schemas();
        schema->set_schema_id(kSchemaId);
        schema->set_name("demo/msg/One");
        schema->set_encoding("ros2msg");
        schema->set_data("int32 value");
      }
      send(ws, response);
      if (mode_ == Mode::kEmptyPlan) {
        return;
      }
      const int batches = (mode_ == Mode::kComplete) ? kBatches : 2;
      std::uint64_t sent = 0;
      for (int b = 0; b < batches; ++b) {
        pj_cloud::v1::ServerMessage frame;
        // Session frames MUST carry the subscription id on the envelope: the
        // client's subscription filter DROPS a zero-id frame once it has
        // learned the nonzero id from the OpenSessionResponse (the real
        // server always stamps it; an unstamped fake hangs the download
        // whenever the response is processed before the first batch).
        frame.set_subscription_id(7);
        auto* batch = frame.mutable_batch();
        batch->set_seq(static_cast<std::uint64_t>(b) + 1);
        batch->set_body_encoding(pj_cloud::v1::BODY_ENCODING_NONE);
        for (int m = 0; m < kMessagesPerBatch; ++m) {
          auto* message = batch->add_messages();
          message->set_topic_id(kTopicId);
          message->set_schema_id(kSchemaId);
          message->set_log_time_ns(1000 + static_cast<std::int64_t>(sent));
          message->set_publish_time_ns(990 + static_cast<std::int64_t>(sent));
          message->set_payload_encoding(pj_cloud::v1::PAYLOAD_ENCODING_RAW);
          message->set_payload(std::string(kPayloadBytes, 'x'));
          ++sent;
        }
        send(ws, frame);
      }
      if (mode_ == Mode::kComplete) {
        pj_cloud::v1::ServerMessage eos_frame;
        eos_frame.set_subscription_id(7);  // see the batch-frame comment above
        auto* eos = eos_frame.mutable_eos();
        eos->set_reason(pj_cloud::v1::EOS_REASON_COMPLETE);
        eos->set_total_messages_sent(sent);
        eos->set_total_bytes_sent(sent * kPayloadBytes);
        send(ws, eos_frame);
      }
      // kStallAfterTwoBatches: silence — the client's cancel wakes the wait.
    });
    auto res = server_.listen();
    ok_ = res.first;
    if (ok_) {
      server_.start();
    }
  }

  ~FakeStreamingServer() { server_.stop(); }

  void stop() { server_.stop(); }

  [[nodiscard]] bool ok() const { return ok_; }
  [[nodiscard]] std::string uri() const { return "ws://127.0.0.1:" + std::to_string(port_); }
  [[nodiscard]] int openSessions() const { return open_sessions_.load(); }

 private:
  static void send(ix::WebSocket& ws, const pj_cloud::v1::ServerMessage& message) {
    std::string payload;
    message.SerializeToString(&payload);
    ws.sendBinary(payload);
  }

  Mode mode_;
  int port_;
  ix::WebSocketServer server_;
  bool ok_ = false;
  std::atomic<int> open_sessions_{0};
};

// The tuple the worker sends -> the descriptor identity it must tee under.
std::string identityFor(const std::string& uri) {
  mcap_cloud::SourceDescriptor d;
  d.version = 1;
  d.kind = "mcap-cloud-session";
  d.server_uri = uri;
  d.s3_keys = {"a.mcap"};
  d.topics = {"/one"};
  d.start_ns = 0;
  d.end_ns = 0;
  d.include_latched = true;
  return mcap_cloud::descriptorIdentity(d);
}

std::string readFile(const fs::path& path) {
  std::ifstream in(path, std::ios::binary);
  return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

bool cacheRootHasPartial(const fs::path& root) {
  std::error_code ec;
  for (const auto& entry : fs::directory_iterator(root, ec)) {
    if (entry.path().filename().string().find(".mcap.partial.") != std::string::npos) {
      return true;
    }
  }
  return false;
}

struct TeeReport {
  mcap_cloud::TeeOutcome outcome = mcap_cloud::TeeOutcome::kNone;
  std::string identity;
  std::string error;
  int count = 0;
};

// One fully-wired worker: hosts, runtime, callbacks. Owns the recorders the
// assertions read AFTER the (synchronous) pull returns.
struct Harness {
  Harness(mcap_cloud::ImportRuntime& rt, const std::string& uri, bool decodable = true)
      : ingest(/*with_progress_slots=*/true) {
    if (!decodable) {
      ingest.refuse_create = true;  // no-decoder host: every topic undecodable
    }
    worker.setImportRuntime(&rt);
    worker.setHostProvider([this]() { return host.view(); });
    worker.setRuntimeHostProvider([this]() { return PJ::ToolboxRuntimeHostView{ingest.toolboxRuntime()}; });
    worker.setDatasetExistsForTest([](const mcap_cloud::CachedSession&) { return true; });
    worker.mcapSaveFinished = [this](mcap_cloud::McapSaveResult r) { save_results.push_back(std::move(r)); };
    worker.teeFinished = [this](mcap_cloud::TeeOutcome outcome, std::string identity, std::string error) {
      tee.outcome = outcome;
      tee.identity = std::move(identity);
      tee.error = std::move(error);
      ++tee.count;
    };
    worker.pullFinished = [this](std::string, std::string topic, bool ok, std::string error) {
      topic_results.emplace_back(std::move(topic), ok, std::move(error));
    };
    worker.pullServedFromCache = [this](std::string) { ++served_from_cache; };
    worker.pullProgress = [this](std::string, std::int64_t) { progress_events.fetch_add(1); };

    bool connected = false;
    worker.connectFinished = [&](bool ok, std::string, std::string, std::string) { connected = ok; };
    worker.connectAsync(uri, "", "", false);
    EXPECT_TRUE(connected) << "browse connect against the fake server must succeed";
  }

  void pull(const std::string& save_directory = {}) {
    worker.pullTopicsAsync({"a.mcap"}, "a.mcap", {"/one"}, 0, 0, save_directory);
  }

  mcap_cloud::testsupport::FakeToolboxHost host;
  pj_ingest_test::FakeIngestHost ingest;
  mcap_cloud::FetchWorker worker;
  std::vector<mcap_cloud::McapSaveResult> save_results;
  std::vector<std::tuple<std::string, bool, std::string>> topic_results;
  TeeReport tee;
  int served_from_cache = 0;
  std::atomic<std::uint64_t> progress_events{0};
};

}  // namespace

TEST(McapCloudFetchWorkerCacheTee, CompleteFetchFinalizesCacheAndExportIsAByteCopy) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("complete-cache");
  TempRoot config_root("complete-config");
  TempRoot export_dir("complete-export");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  Harness h(rt, server.uri());
  h.pull(export_dir.path.string());

  // Ingest untouched by the tee: the topic completed OK.
  ASSERT_EQ(h.topic_results.size(), 1u);
  EXPECT_TRUE(std::get<1>(h.topic_results[0])) << std::get<2>(h.topic_results[0]);

  // Cache finalized + valid.
  const std::string identity = identityFor(server.uri());
  EXPECT_EQ(h.tee.count, 1) << "teeFinished fires exactly once per runtime-bound pull";
  EXPECT_EQ(h.tee.outcome, mcap_cloud::TeeOutcome::kFinalized) << h.tee.error;
  EXPECT_EQ(h.tee.identity, identity);
  fs::path cache_file;
  ASSERT_TRUE(rt.fileCache().lookup(identity, &cache_file)) << "cache file must pass validation";
  EXPECT_FALSE(cacheRootHasPartial(cache_root.path));

  // Export = byte copy of the cache file, published under the final name.
  ASSERT_EQ(h.save_results.size(), 1u);
  EXPECT_EQ(h.save_results[0].status, mcap_cloud::McapSaveStatus::Complete) << h.save_results[0].error;
  const fs::path export_path(h.save_results[0].path);
  ASSERT_TRUE(fs::exists(export_path));
  EXPECT_EQ(export_path.extension(), ".mcap");
  EXPECT_EQ(readFile(export_path), readFile(cache_file)) << "export must be a byte copy";

  // The shared in-memory cache holds the completed entry with tee state.
  const PJ::cloud::SessionKey key =
      PJ::cloud::computeSessionKey(server.uri(), {"a.mcap"}, {"/one"}, {0, 0});
  auto entry = rt.sessionCache().lookup(key, [](const mcap_cloud::CachedSession&) { return true; });
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->tee_outcome, mcap_cloud::TeeOutcome::kFinalized);
  EXPECT_EQ(entry->cache_identity, identity);
  EXPECT_EQ(entry->total_messages, static_cast<std::uint64_t>(kBatches * kMessagesPerBatch));
}

TEST(McapCloudFetchWorkerCacheTee, CancelMidDownloadCopiesExportPartialAndDeletesCachePartial) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kStallAfterTwoBatches);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("cancel-cache");
  TempRoot config_root("cancel-config");
  TempRoot export_dir("cancel-export");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  Harness h(rt, server.uri());
  std::thread pull([&] { h.pull(export_dir.path.string()); });
  // Join guard: an ASSERT failure below would otherwise unwind past a
  // joinable std::thread (std::terminate). Cancels first so the guarded
  // join cannot itself hang out the frame timeout.
  struct PullJoinGuard {
    mcap_cloud::FetchWorker& worker;
    std::thread& thread;
    ~PullJoinGuard() {
      if (thread.joinable()) {
        worker.requestCancel();
        thread.join();
      }
    }
  } pull_join_guard{h.worker, pull};
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
  while (h.progress_events.load() == 0 && std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  ASSERT_GT(h.progress_events.load(), 0u) << "messages must flow before the cancel";
  h.worker.requestCancel();
  pull.join();

  // Export keeps a READABLE partial COPY (deliberate retention).
  ASSERT_EQ(h.save_results.size(), 1u);
  EXPECT_EQ(h.save_results[0].status, mcap_cloud::McapSaveStatus::Partial);
  const fs::path export_partial(h.save_results[0].path);
  ASSERT_TRUE(fs::exists(export_partial));
  EXPECT_NE(export_partial.string().find(".mcap.partial"), std::string::npos);
  {
    mcap::McapReader reader;
    ASSERT_TRUE(reader.open(export_partial.string()).ok()) << "export partial must be readable";
    EXPECT_TRUE(reader.readSummary(mcap::ReadSummaryMethod::NoFallbackScan).ok());
    reader.close();
  }

  // The cache partial NEVER survives; nothing was published.
  EXPECT_FALSE(cacheRootHasPartial(cache_root.path));
  const std::string identity = identityFor(server.uri());
  fs::path unused;
  EXPECT_FALSE(rt.fileCache().lookup(identity, &unused));
  EXPECT_EQ(h.tee.outcome, mcap_cloud::TeeOutcome::kAborted);

  // No half-cached memory entry either (COMPLETE-only store).
  EXPECT_EQ(rt.sessionCache().size(), 0u);
}

TEST(McapCloudFetchWorkerCacheTee, TeeFailureNeverAbortsTheFetch) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot config_root("teefail-config");
  TempRoot holder("teefail-holder");
  const fs::path bad_root = holder.path / "not-a-dir";
  {
    std::ofstream out(bad_root, std::ios::binary);
    out << "occupied";
  }
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(bad_root),
                               mcap_cloud::TrustedOrigins(config_root.path));

  Harness h(rt, server.uri());
  h.pull();

  ASSERT_EQ(h.topic_results.size(), 1u);
  EXPECT_TRUE(std::get<1>(h.topic_results[0]))
      << "a tee failure must never abort the ingest (§9.6): " << std::get<2>(h.topic_results[0]);
  EXPECT_EQ(h.tee.count, 1);
  EXPECT_EQ(h.tee.outcome, mcap_cloud::TeeOutcome::kFailed);
  EXPECT_FALSE(h.tee.error.empty());

  // The completed entry is still stored (rows live in the host datastore),
  // carrying the failed tee outcome for promotion suppression.
  const PJ::cloud::SessionKey key =
      PJ::cloud::computeSessionKey(server.uri(), {"a.mcap"}, {"/one"}, {0, 0});
  auto entry = rt.sessionCache().lookup(key, [](const mcap_cloud::CachedSession&) { return true; });
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->tee_outcome, mcap_cloud::TeeOutcome::kFailed);
  EXPECT_TRUE(entry->cache_identity.empty());
}

TEST(McapCloudFetchWorkerCacheTee, MemoryHitWithMissingDiskFileEvictsAndRefetches) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("diskmiss-cache");
  TempRoot config_root("diskmiss-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  Harness h(rt, server.uri());
  h.pull();
  ASSERT_EQ(server.openSessions(), 1);
  const std::string identity = identityFor(server.uri());
  fs::path cache_file;
  ASSERT_TRUE(rt.fileCache().lookup(identity, &cache_file));

  // Sabotage: the disk file vanishes (user wiped the cache dir).
  std::error_code ec;
  fs::remove(cache_file, ec);
  fs::remove(fs::path(cache_file.string() + ".touch"), ec);

  h.pull();
  EXPECT_EQ(server.openSessions(), 2)
      << "memory hit + missing disk file must evict and refetch (§6.1), never re-tee from memory";
  EXPECT_EQ(h.served_from_cache, 0);
  ASSERT_TRUE(rt.fileCache().lookup(identity, &cache_file)) << "refetch re-materializes";

  const PJ::cloud::SessionKey key =
      PJ::cloud::computeSessionKey(server.uri(), {"a.mcap"}, {"/one"}, {0, 0});
  auto entry = rt.sessionCache().lookup(key, [](const mcap_cloud::CachedSession&) { return true; });
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->last_hit_case, mcap_cloud::MemoryHitCase::kRefetchedDiskMiss);
}

TEST(McapCloudFetchWorkerCacheTee, MemoryHitWithValidDiskServesFromMemoryAndCopiesExport) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("diskhit-cache");
  TempRoot config_root("diskhit-config");
  TempRoot export_dir("diskhit-export");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  Harness h(rt, server.uri());
  h.pull();
  ASSERT_EQ(server.openSessions(), 1);

  // Second pull WITH an export: served from memory, export satisfied by COPY,
  // zero new sessions (never re-tee'd from memory).
  h.save_results.clear();
  h.topic_results.clear();
  h.pull(export_dir.path.string());
  EXPECT_EQ(server.openSessions(), 1) << "a memory+disk hit must not open a session";
  EXPECT_EQ(h.served_from_cache, 1);
  ASSERT_EQ(h.save_results.size(), 1u);
  EXPECT_EQ(h.save_results[0].status, mcap_cloud::McapSaveStatus::Complete) << h.save_results[0].error;
  const std::string identity = identityFor(server.uri());
  fs::path cache_file;
  ASSERT_TRUE(rt.fileCache().lookup(identity, &cache_file));
  EXPECT_EQ(readFile(fs::path(h.save_results[0].path)), readFile(cache_file));
  EXPECT_EQ(h.tee.outcome, mcap_cloud::TeeOutcome::kExistingValid);

  const PJ::cloud::SessionKey key =
      PJ::cloud::computeSessionKey(server.uri(), {"a.mcap"}, {"/one"}, {0, 0});
  auto entry = rt.sessionCache().lookup(key, [](const mcap_cloud::CachedSession&) { return true; });
  ASSERT_TRUE(entry.has_value());
  EXPECT_EQ(entry->last_hit_case, mcap_cloud::MemoryHitCase::kServedValidDisk);
}

TEST(McapCloudFetchWorkerCacheTee, SessionConnectFailureLeavesNoPartialAndReleasesLock) {
  auto server = std::make_unique<FakeStreamingServer>(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server->ok());
  TempRoot cache_root("connfail-cache");
  TempRoot config_root("connfail-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  Harness h(rt, server->uri());
  const std::string uri = server->uri();
  server.reset();  // the session connection the pull opens will fail

  h.pull();
  ASSERT_EQ(h.topic_results.size(), 1u);
  EXPECT_FALSE(std::get<1>(h.topic_results[0]));

  EXPECT_FALSE(cacheRootHasPartial(cache_root.path)) << "RAII guard must remove any partial";
  const std::string identity = identityFor(uri);
  std::string error;
  EXPECT_TRUE(rt.fileCache().tryLockForMaterialize(identity, &error).has_value())
      << "the materialize lock must be released on the connect-failure exit: " << error;
  EXPECT_EQ(h.tee.outcome, mcap_cloud::TeeOutcome::kNone) << "the tee never ran";
}

TEST(McapCloudFetchWorkerCacheTee, EmptyPlanLeavesNoPartialAndReleasesLock) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kEmptyPlan);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("emptyplan-cache");
  TempRoot config_root("emptyplan-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  Harness h(rt, server.uri());
  h.pull();
  ASSERT_EQ(h.topic_results.size(), 1u);
  EXPECT_FALSE(std::get<1>(h.topic_results[0]));

  EXPECT_FALSE(cacheRootHasPartial(cache_root.path));
  std::string error;
  EXPECT_TRUE(rt.fileCache().tryLockForMaterialize(identityFor(server.uri()), &error).has_value())
      << error;
}

// Raw-tee parser independence (spec §9.0): a session with NO decodable topic
// must still materialize the cache file — writer setup precedes the
// hasDecodable early-exit, and the download runs for the tee alone.
TEST(McapCloudFetchWorkerCacheTee, NoDecoderSessionStillMaterializesTheCacheFile) {
  FakeStreamingServer server(FakeStreamingServer::Mode::kComplete);
  ASSERT_TRUE(server.ok());
  TempRoot cache_root("nodecoder-cache");
  TempRoot config_root("nodecoder-config");
  mcap_cloud::ImportRuntime rt(mcap_cloud::SessionFileCache(cache_root.path),
                               mcap_cloud::TrustedOrigins(config_root.path));

  Harness h(rt, server.uri(), /*decodable=*/false);
  h.pull();

  ASSERT_EQ(h.topic_results.size(), 1u);
  EXPECT_FALSE(std::get<1>(h.topic_results[0])) << "the topic itself still reports no-parser";

  EXPECT_EQ(h.tee.outcome, mcap_cloud::TeeOutcome::kFinalized) << h.tee.error;
  fs::path cache_file;
  EXPECT_TRUE(rt.fileCache().lookup(identityFor(server.uri()), &cache_file))
      << "a no-decoder session must still materialize the cache file";

  // COMPLETE-only memory store still refuses a nothing-decodable session (a
  // zero-count entry would false-HIT a future fetch).
  EXPECT_EQ(rt.sessionCache().size(), 0u);
}
