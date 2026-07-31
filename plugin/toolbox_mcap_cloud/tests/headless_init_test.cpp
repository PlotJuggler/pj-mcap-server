// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC headless one-shot init matrix (stage-4 PR-2, D1-as-amended): a
// provider-mode bind() must NOT run the interactive dialog initialization —
// spec docs/canonical-layout-import.md §6.3: "binding the provider for
// query/materialize must not run the interactive dialog initialization …
// no auto-connect; network only inside an authorized materialize". The
// interactive init (persisted-state restore + auto-connect) moves to the
// FIRST getDialog() call, as an explicit ONCE-PER-PLUGIN-LIFETIME latch.
//
// Drives the REAL McapCloudToolbox through a fake service registry providing
// the two mandatory toolbox services + pj.settings.v1, with the settings
// store carrying a recent-server entry that points at an in-process fake WS
// server which counts connection attempts (Hello messages). Pins:
//   - bind() alone -> ZERO connection attempts;
//   - the FIRST getDialog() fires the auto-connect exactly once;
//   - a second getDialog() never re-connects (the latch);
//   - a re-bind AFTER initialization updates the stored settings view
//     (observable via the destructor's persistState write-through) but
//     never implicitly reconnects.
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <ixwebsocket/IXWebSocketServer.h>
#include <pj_base/descriptor_import_protocol.h>
#include <pj_base/plugin_data_api.h>
#include <pj_base/sdk/plugin_data_api.hpp>
#include <pj_base/sdk/service_registry.hpp>
#include <pj_base/sdk/settings_store_host.hpp>

#include "fake_toolbox_host.hpp"
#include "fetch_worker.hpp"
#include "find_free_port.hpp"
#include "mcap_cloud_toolbox.hpp"
#include "parser_ingest_test_support.hpp"
#include "pj_cloud.pb.h"
#include "source_descriptor.hpp"
#include "test_support_env.hpp"
#include "test_support_fs.hpp"

namespace {

namespace fs = std::filesystem;
using mcap_cloud_test::findFreePort;

// Shared RAII temp-dir base with this suite's unique prefix.
struct TempRoot : mcap_cloud_test::ScopedTempDir {
  explicit TempRoot(const std::string& name) : ScopedTempDir("mcap-cloud-headless-init-" + name) {}
};

// A fake pj_cloud server that counts CONNECTION ATTEMPTS (Hello messages) and
// answers each with a minimal HelloResponse so an attempted auto-connect
// completes instead of hanging the dialog's worker thread on the Hello wait.
class CountingHelloServer {
 public:
  CountingHelloServer() : port_(findFreePort()), server_(port_, "127.0.0.1") {
    server_.setOnClientMessageCallback(
        [this](std::shared_ptr<ix::ConnectionState>, ix::WebSocket& ws, const ix::WebSocketMessagePtr& msg) {
          if (msg->type != ix::WebSocketMessageType::Message) {
            return;
          }
          pj_cloud::v1::ClientMessage request;
          if (!request.ParseFromString(msg->str) || !request.has_hello()) {
            return;
          }
          hellos_.fetch_add(1);
          pj_cloud::v1::ServerMessage response;
          response.set_request_id(request.request_id());
          response.mutable_hello_response()->set_server_version("test-fake-1.0");
          std::string payload;
          response.SerializeToString(&payload);
          ws.sendBinary(payload);
        });
    auto res = server_.listen();
    ok_ = res.first;
    if (ok_) {
      server_.start();
    }
  }

  ~CountingHelloServer() { server_.stop(); }

  [[nodiscard]] bool ok() const { return ok_; }
  [[nodiscard]] std::string uri() const { return "ws://127.0.0.1:" + std::to_string(port_); }
  [[nodiscard]] int helloCount() const { return hellos_.load(); }

 private:
  int port_;
  ix::WebSocketServer server_;
  bool ok_ = false;
  std::atomic<int> hellos_{0};
};

// Minimal PJ_service_registry_t over the three services McapCloudToolbox
// binds: the two mandatory toolbox hosts + the optional pj.settings.v1 store.
class FakeServiceRegistry {
 public:
  FakeServiceRegistry(PJ_toolbox_host_t write_host, PJ_toolbox_runtime_host_t runtime_host,
                      PJ_settings_store_t settings)
      : write_host_(write_host), runtime_host_(runtime_host), settings_(settings) {
    vtable_.protocol_version = 1;
    vtable_.struct_size = sizeof(PJ_service_registry_vtable_t);
    vtable_.get_service = &FakeServiceRegistry::getService;
  }

  [[nodiscard]] PJ::sdk::ServiceRegistry registry() {
    return PJ::sdk::ServiceRegistry(PJ_service_registry_t{this, &vtable_});
  }

  // Optionally serve pj.source_promotion.v1 too (the D6 wiring test below).
  void setPromotionService(PJ_source_promotion_host_t host) { promotion_ = host; }

 private:
  static bool getService(void* ctx, PJ_string_view_t name, uint32_t /*min_version*/, PJ_service_t* out,
                         PJ_error_t* err) noexcept {
    auto* self = static_cast<FakeServiceRegistry*>(ctx);
    const std::string n(name.data == nullptr ? "" : name.data, name.size);
    if (n == "pj.toolbox_write.v1") {
      *out = PJ_service_t{self->write_host_.ctx, self->write_host_.vtable};
      return true;
    }
    if (n == "pj.toolbox_runtime.v1") {
      *out = PJ_service_t{self->runtime_host_.ctx, self->runtime_host_.vtable};
      return true;
    }
    if (n == "pj.settings.v1") {
      *out = PJ_service_t{self->settings_.ctx, self->settings_.vtable};
      return true;
    }
    if (n == PJ_SOURCE_PROMOTION_HOST_SERVICE_V1 && self->promotion_.has_value()) {
      *out = PJ_service_t{self->promotion_->ctx, self->promotion_->vtable};
      return true;
    }
    PJ::sdk::fillError(err, 1, "fake_registry", "service not provided by this fake");
    return false;
  }

  PJ_service_registry_vtable_t vtable_{};
  PJ_toolbox_host_t write_host_{};
  PJ_toolbox_runtime_host_t runtime_host_{};
  PJ_settings_store_t settings_{};
  std::optional<PJ_source_promotion_host_t> promotion_;
};

// One bindable environment: fake hosts + an in-memory settings backend whose
// server history points at the fake server (the auto-connect target).
struct BindEnv {
  explicit BindEnv(const std::string& server_uri) {
    if (!server_uri.empty()) {
      backend.setStringList("mcap_cloud/server_history", {server_uri});
    }
  }

  [[nodiscard]] PJ::sdk::ServiceRegistry registry() { return fake_registry.registry(); }

  mcap_cloud::testsupport::FakeToolboxHost write_host;
  pj_ingest_test::FakeIngestHost ingest_host;
  PJ::sdk::InMemorySettingsBackend backend;
  PJ::sdk::SettingsStoreHost settings_host{backend};
  FakeServiceRegistry fake_registry{write_host.makeHost(), ingest_host.toolboxRuntime(), settings_host.view()};
};

// Hermetic env roots: hoisted VERBATIM into test_support_env.hpp (shared
// with the PR-3 provider suites); this alias keeps the suite body unchanged.
using HermeticEnvBase = mcap_cloud_test::HermeticEnv;
struct HermeticEnv : HermeticEnvBase {
  HermeticEnv() : HermeticEnvBase("mcap-cloud-headless-init") {}
};

// True once `count()` reaches `expected` within `timeout`.
bool waitForCount(const CountingHelloServer& server, int expected, std::chrono::milliseconds timeout) {
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (std::chrono::steady_clock::now() < deadline) {
    if (server.helloCount() >= expected) {
      return true;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  return server.helloCount() >= expected;
}

// Observation window for the NEGATIVE assertions ("no connect happens"): long
// enough that a wrongly-queued localhost auto-connect (which lands in a few
// ms) is reliably caught, short enough to keep the suite fast.
void expectNoConnectWithin(const CountingHelloServer& server, int baseline, std::chrono::milliseconds window) {
  const auto deadline = std::chrono::steady_clock::now() + window;
  while (std::chrono::steady_clock::now() < deadline) {
    ASSERT_EQ(server.helloCount(), baseline) << "unexpected connection attempt";
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  }
}

}  // namespace

// Harness self-check: the fake registry serves pj.settings.v1 and the seeded
// history round-trips (a broken registry would make every negative assertion
// below pass vacuously).
TEST(McapCloudHeadlessInitHarness, RegistryServesSeededSettings) {
  CountingHelloServer server;
  ASSERT_TRUE(server.ok());
  BindEnv env(server.uri());
  PJ::sdk::ServiceRegistry reg = env.registry();
  auto view = reg.get<PJ::sdk::SettingsStoreService>();
  ASSERT_TRUE(view.has_value());
  auto list = view->valueStringList("mcap_cloud/server_history");
  ASSERT_TRUE(list.has_value());
  ASSERT_EQ(list->size(), 1u);
  EXPECT_EQ(list->front(), server.uri());
}

// Harness self-check: a raw FetchWorker connect against the counting server
// counts exactly one Hello (proves the zero-count assertions can't be a
// server that never counts).
TEST(McapCloudHeadlessInitHarness, CountingServerCountsAConnect) {
  CountingHelloServer server;
  ASSERT_TRUE(server.ok());
  mcap_cloud::FetchWorker worker;
  bool connected = false;
  std::string err;
  worker.connectFinished = [&](bool ok, std::string, std::string, std::string e) {
    connected = ok;
    err = std::move(e);
  };
  worker.connectAsync(server.uri(), "", "", false);
  EXPECT_TRUE(connected) << err;
  EXPECT_EQ(server.helloCount(), 1);
}

// Dialog-level twin of the toolbox matrix: setSettings() is STORE-ONLY and
// the one-shot init lives behind ensureInitFromSettings()'s latch.
TEST(McapCloudHeadlessInit, SetSettingsStoresOnlyAndEnsureInitConnectsOnce) {
  HermeticEnv env;
  CountingHelloServer server;
  ASSERT_TRUE(server.ok());
  BindEnv bind_env(server.uri());
  mcap_cloud::McapCloudDialog dialog;
  dialog.setSettings(PJ::sdk::SettingsView{bind_env.settings_host.view()});
  expectNoConnectWithin(server, 0, std::chrono::milliseconds(500));

  dialog.ensureInitFromSettings();
  EXPECT_TRUE(waitForCount(server, 1, std::chrono::seconds(10)))
      << "ensureInitFromSettings must run the auto-connect";

  dialog.ensureInitFromSettings();  // the latch: a second call is a no-op
  expectNoConnectWithin(server, 1, std::chrono::milliseconds(300));
  EXPECT_EQ(server.helloCount(), 1);
}

// §6.3: bind() must be network-free — no initFromSettings, no auto-connect.
// RED before the fix: bind() -> setSettings() -> initFromSettings() queues
// connectAsync and the fake server counts a Hello within milliseconds.
TEST(McapCloudHeadlessInit, BindAloneNeverConnects) {
  HermeticEnv env;
  CountingHelloServer server;
  ASSERT_TRUE(server.ok());
  BindEnv bind_env(server.uri());

  mcap_cloud::McapCloudToolbox toolbox;
  auto status = toolbox.bind(bind_env.registry());
  ASSERT_TRUE(status.has_value()) << status.error();

  expectNoConnectWithin(server, 0, std::chrono::milliseconds(750));
  EXPECT_EQ(server.helloCount(), 0) << "bind() must not auto-connect (spec §6.3)";
}

// The interactive path: the FIRST getDialog() runs the one-shot init (restore
// + auto-connect, exactly one attempt); a SECOND getDialog() must not re-init
// or reconnect (the once-per-plugin-lifetime latch).
TEST(McapCloudHeadlessInit, FirstGetDialogAutoConnectsExactlyOnce) {
  HermeticEnv env;
  CountingHelloServer server;
  ASSERT_TRUE(server.ok());
  BindEnv bind_env(server.uri());

  mcap_cloud::McapCloudToolbox toolbox;
  auto status = toolbox.bind(bind_env.registry());
  ASSERT_TRUE(status.has_value()) << status.error();
  ASSERT_EQ(server.helloCount(), 0) << "bind() must not auto-connect (spec §6.3)";

  const PJ_borrowed_dialog_t first = toolbox.getDialog();
  EXPECT_NE(first.ctx, nullptr);
  EXPECT_NE(first.vtable, nullptr);
  EXPECT_TRUE(waitForCount(server, 1, std::chrono::seconds(10)))
      << "the first getDialog() must fire the auto-connect";
  EXPECT_EQ(server.helloCount(), 1);

  const PJ_borrowed_dialog_t second = toolbox.getDialog();
  EXPECT_EQ(second.ctx, first.ctx) << "getDialog() must keep borrowing the same dialog";
  expectNoConnectWithin(server, 1, std::chrono::milliseconds(500));
  EXPECT_EQ(server.helloCount(), 1) << "a repeated getDialog() must never re-connect";
}

// A re-bind AFTER initialization updates the stored settings view but never
// implicitly reconnects. The view swap is observable through the dialog
// destructor's persistState(): its writes must land in the SECOND backend
// (and never in the first, which saw no writes at all in this scenario).
TEST(McapCloudHeadlessInit, RebindAfterInitUpdatesSettingsViewWithoutReconnect) {
  HermeticEnv env;
  CountingHelloServer server;
  ASSERT_TRUE(server.ok());
  BindEnv first_env(server.uri());
  BindEnv second_env(server.uri());

  {
    mcap_cloud::McapCloudToolbox toolbox;
    auto status = toolbox.bind(first_env.registry());
    ASSERT_TRUE(status.has_value()) << status.error();
    (void)toolbox.getDialog();
    ASSERT_TRUE(waitForCount(server, 1, std::chrono::seconds(10)));

    auto rebind = toolbox.bind(second_env.registry());
    ASSERT_TRUE(rebind.has_value()) << rebind.error();
    expectNoConnectWithin(server, 1, std::chrono::milliseconds(500));
    EXPECT_EQ(server.helloCount(), 1) << "a re-bind after init must never reconnect";

    // The latch also survives the re-bind: getDialog() stays a plain borrow.
    (void)toolbox.getDialog();
    expectNoConnectWithin(server, 1, std::chrono::milliseconds(300));
    EXPECT_EQ(server.helloCount(), 1);
  }  // toolbox destruction -> dialog dtor -> persistState() through settings_

  EXPECT_TRUE(second_env.backend.contains("mcap_cloud/metadata_query"))
      << "persistState must write through the RE-BOUND settings view";
  EXPECT_FALSE(first_env.backend.contains("mcap_cloud/metadata_query"))
      << "the pre-rebind view must no longer receive writes";
}

namespace {

// Minimal accepting pj.source_promotion.v1 host for the D6 wiring test.
struct MinimalPromotionHost {
  static bool promoteThunk(void*, const PJ_source_promotion_request_v1_t*,
                           PJ_source_promotion_result_fn result_cb, void* callback_ctx,
                           PJ_error_t*) noexcept {
    const char* msg = "promoted";
    result_cb(callback_ctx, true, PJ_string_view_t{msg, 8});
    return true;
  }
  [[nodiscard]] PJ_source_promotion_host_t view() {
    static constexpr PJ_source_promotion_host_vtable_t kVtable{
        1, sizeof(PJ_source_promotion_host_vtable_t), &MinimalPromotionHost::promoteThunk};
    return PJ_source_promotion_host_t{this, &kVtable};
  }
};

}  // namespace

// PR-3 wiring: pluginExtension exposes the pj.descriptor_import.v1 table
// (nullptr for anything else), the C thunks resolve plugin_ctx == the
// toolbox instance, and the WHOLE query path works on an instance whose
// getDialog() was NEVER called (the PR-2 latch stays cold) with ZERO network.
TEST(McapCloudHeadlessInit, PluginExtensionServesTheDescriptorImportProviderCold) {
  HermeticEnv env;
  CountingHelloServer server;
  ASSERT_TRUE(server.ok());
  BindEnv bind_env(server.uri());

  mcap_cloud::McapCloudToolbox toolbox;
  auto status = toolbox.bind(bind_env.registry());
  ASSERT_TRUE(status.has_value()) << status.error();

  EXPECT_EQ(toolbox.pluginExtension("pj.unknown.v1"), nullptr);
  const void* ext = toolbox.pluginExtension(PJ_DESCRIPTOR_IMPORT_EXTENSION_V1);
  ASSERT_NE(ext, nullptr);

  // Drive the raw C surface exactly like the host: plugin_ctx = the toolbox.
  const auto* table = static_cast<const PJ_descriptor_import_provider_v1_t*>(ext);
  ASSERT_GE(table->struct_size, sizeof(PJ_descriptor_import_provider_v1_t));
  ASSERT_NE(table->query_descriptor, nullptr);
  ASSERT_NE(table->start_import, nullptr);

  // Malformed -> contract failure.
  PJ_descriptor_query_result_v1_t out{};
  out.struct_size = sizeof(out);
  PJ_error_t err{};
  const char* bad = "not json";
  EXPECT_FALSE(table->query_descriptor(&toolbox, PJ_string_view_t{bad, 8}, &out, &err));

  // Well-formed -> identity + needs-confirmation, with the dialog latch COLD
  // and zero network (the fake server counts any Hello).
  mcap_cloud::SourceDescriptor d;
  d.version = 1;
  d.kind = "mcap-cloud-session";
  d.server_uri = server.uri();
  d.s3_keys = {"a.mcap"};
  d.topics = {"/one"};
  const std::string json = mcap_cloud::toSourceDescriptorJson(d);
  out = PJ_descriptor_query_result_v1_t{};
  out.struct_size = sizeof(out);
  ASSERT_TRUE(table->query_descriptor(&toolbox, PJ_string_view_t{json.data(), json.size()}, &out, &err));
  EXPECT_EQ(out.trust, PJ_DESCRIPTOR_TRUST_NEEDS_CONFIRMATION);
  EXPECT_EQ(out.is_materialized, 0u);
  EXPECT_EQ(std::string(out.source_identity.data, out.source_identity.size),
            mcap_cloud::descriptorIdentity(d));
  expectNoConnectWithin(server, 0, std::chrono::milliseconds(500));
  EXPECT_EQ(server.helloCount(), 0) << "a provider query must never touch the network (spec §6.3)";
}

// D6 wiring: bind() resolves the OPTIONAL pj.source_promotion.v1 service into
// the shared ImportRuntime (get<>, never require<>); absence leaves the
// runtime promotion-less (every completed materialize EAGER_ONLY-equivalent)
// and must not fail the bind.
TEST(McapCloudHeadlessInit, BindResolvesTheOptionalPromotionService) {
  HermeticEnv env;
  // Absent service -> bind succeeds, no promotion host.
  {
    BindEnv bind_env("");
    mcap_cloud::McapCloudToolbox toolbox;
    auto status = toolbox.bind(bind_env.registry());
    ASSERT_TRUE(status.has_value()) << status.error();
    EXPECT_FALSE(toolbox.importRuntime().hasPromotionHost());
  }
  // Registered service -> the runtime holds the bound view.
  {
    BindEnv bind_env("");
    MinimalPromotionHost promo;
    bind_env.fake_registry.setPromotionService(promo.view());
    mcap_cloud::McapCloudToolbox toolbox;
    auto status = toolbox.bind(bind_env.registry());
    ASSERT_TRUE(status.has_value()) << status.error();
    EXPECT_TRUE(toolbox.importRuntime().hasPromotionHost());
  }
}
