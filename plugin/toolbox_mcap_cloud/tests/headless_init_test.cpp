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
    PJ::sdk::fillError(err, 1, "fake_registry", "service not provided by this fake");
    return false;
  }

  PJ_service_registry_vtable_t vtable_{};
  PJ_toolbox_host_t write_host_{};
  PJ_toolbox_runtime_host_t runtime_host_{};
  PJ_settings_store_t settings_{};
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

// Hermetic roots: the toolbox constructor resolves the SessionFileCache root
// (MCAP_CLOUD_CACHE_DIR) and the trust/credential config root (XDG_CONFIG_HOME)
// from the environment, and initFromSettings' credential resolution reads
// MCAP_CLOUD_URL / MCAP_CLOUD_API_KEY — pin all four before construction and
// RESTORE the prior values on destruction (capture/restore mirrors
// credential_resolve_test's EnvGuard; a bare unsetenv of XDG_CONFIG_HOME
// would destroy a commonly-set variable for the rest of the process).
struct HermeticEnv {
  HermeticEnv()
      : cache_root("cache"),
        config_root("config"),
        saved_cache_dir(capture("MCAP_CLOUD_CACHE_DIR")),
        saved_xdg_config(capture("XDG_CONFIG_HOME")),
        saved_url(capture("MCAP_CLOUD_URL")),
        saved_api_key(capture("MCAP_CLOUD_API_KEY")) {
    ::setenv("MCAP_CLOUD_CACHE_DIR", cache_root.path.string().c_str(), 1);
    ::setenv("XDG_CONFIG_HOME", config_root.path.string().c_str(), 1);
    ::unsetenv("MCAP_CLOUD_URL");
    ::unsetenv("MCAP_CLOUD_API_KEY");
  }
  ~HermeticEnv() {
    restore("MCAP_CLOUD_CACHE_DIR", saved_cache_dir);
    restore("XDG_CONFIG_HOME", saved_xdg_config);
    restore("MCAP_CLOUD_URL", saved_url);
    restore("MCAP_CLOUD_API_KEY", saved_api_key);
  }
  static std::optional<std::string> capture(const char* name) {
    const char* value = std::getenv(name);
    return value == nullptr ? std::nullopt : std::optional<std::string>(value);
  }
  static void restore(const char* name, const std::optional<std::string>& value) {
    if (value.has_value()) {
      ::setenv(name, value->c_str(), 1);
    } else {
      ::unsetenv(name);
    }
  }
  TempRoot cache_root;
  TempRoot config_root;
  std::optional<std::string> saved_cache_dir;
  std::optional<std::string> saved_xdg_config;
  std::optional<std::string> saved_url;
  std::optional<std::string> saved_api_key;
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
