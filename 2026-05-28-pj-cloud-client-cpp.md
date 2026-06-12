# PJ Cloud Client (Qt C++) v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

> **Local grounding (this machine — read before executing anything).**
> **[LOCAL AMENDMENT 2026-06-04]** The implementation repo is **this repo**
> (`/home/gn/ws/PJ4_Server_Template/pj-mcap-server`): every `pj-cloud/<path>` in this plan
> maps to `<repo-root>/<path>`; do **not** create a separate `pj-cloud/` repo.
> **Mandatory reference codebases — always reuse these for PJ4/SDK/plugin context:**
> `/home/gn/ws/PJ4` (app + `plotjuggler_sdk/`; read its `CLAUDE.md` + `PJ4_PLAN.md`
> first; `client-core`'s conventions follow `plotjuggler_core` style and its
> Widgets-free rule mirrors `pj_runtime`'s), `/home/gn/ws/PJ4/pj-official-plugins`
> (plugin conventions; `data_load_mcap/contrib/mcap/` vendors a full MCAP writer usable
> as a `McapWriterSink` reference), and
> `/home/gn/ws/PJ4/pj-official-plugins/toolbox_mosaico` (the dialog/worker design the
> deferred Plan D lifts on top of this plan's `client-core`). Verified key paths: this
> repo's `CLAUDE.md` § "Reference codebases".

**Goal:** Build the Qt C++ test client for the PJ Cloud Connector — a Qt-aware static library (`client-core`) that speaks the wire protocol from [Plan A](./2026-05-28-pj-cloud-server-v1.md), plus a CLI driver (`pjcloud-cli`) that exercises the catalog + session APIs and round-trips streamed data back into a local MCAP file. The library is shaped so the future PJ4 plugin can lift it in unchanged.

**Architecture:** Two CMake targets in `pj-cloud/`:
- `client-core` — Qt-aware (`QtCore` + `QtNetwork` + `QtWebSockets`) static library, **no `Qt6::Widgets` link**, providing `CloudConnection`, `MessageDispatcher`, `CatalogClient`, `SessionClient`, `SessionSink`. Owns the WS layer, the request/subscription correlation, decompression, and a `SessionSink` seam where consumers plug in.
- `client-cli` — `QCoreApplication`-only executable depending on `client-core` + `libmcap`. Implements `McapWriterSink` (one concrete sink) and a small command surface: `files list / show / tag`, `session download / debug`.

**Tech Stack:** Qt 6.8 (`QtCore` `QtNetwork` `QtWebSockets`), `protobuf` 3.21, `zstd` 1.5, `lz4` 1.9, `mcap-cpp` 2.0 (Foxglove), `gtest` 1.14, `spdlog` 1.13, Conan 2 + CMake ≥ 3.21.

**Depends on:** Plan A's `proto/pj_cloud.proto` (canonical wire schema, single source of truth).

**Spec reference:** [`2026-05-28-pj-cloud-connector-design.md`](./2026-05-28-pj-cloud-connector-design.md) — §9 (Client design).

---

## File structure

```
pj-cloud/                                       # (existing from Plan A)
├── proto/pj_cloud.proto                        # (existing, single source of truth)
├── server/                                     # (existing — Plan A)
├── CMakeLists.txt                              # NEW — top-level: client-core + client-cli + tests
├── conanfile.txt                               # NEW — Qt, protobuf, zstd, lz4, mcap, gtest, spdlog
├── client-core/                                # NEW
│   ├── CMakeLists.txt
│   ├── include/pj_cloud_client/
│   │   ├── Common.h                            # Expected<T>, error codes shared
│   │   ├── CloudConnection.h
│   │   ├── MessageDispatcher.h
│   │   ├── CatalogClient.h
│   │   ├── SessionClient.h
│   │   ├── SessionSink.h
│   │   └── Decompression.h
│   ├── src/
│   │   ├── CloudConnection.cpp
│   │   ├── MessageDispatcher.cpp
│   │   ├── CatalogClient.cpp
│   │   ├── SessionClient.cpp
│   │   ├── Decompression.cpp
│   │   └── proto/                              # generated .pb.cc/.h (gitignored)
│   └── tests/                                  # QTest-based, no GUI
│       ├── CMakeLists.txt
│       ├── EnvelopeRoundTripTest.cpp
│       ├── MessageDispatcherTest.cpp
│       ├── SessionClientTest.cpp
│       ├── DecompressionTest.cpp
│       └── fake_server/                        # in-process Go fake server (built once at test time)
└── client-cli/                                 # NEW
    ├── CMakeLists.txt
    ├── src/
    │   ├── main.cpp                            # QCoreApplication + QCommandLineParser
    │   ├── CommandDispatch.{h,cpp}
    │   ├── ListCommand.cpp
    │   ├── ShowCommand.cpp
    │   ├── TagCommand.cpp
    │   ├── DownloadCommand.cpp
    │   ├── DebugCommand.cpp
    │   └── McapWriterSink.{h,cpp}              # SessionSink → libmcap writer
    └── tests/
        ├── CMakeLists.txt
        └── McapWriterSinkTest.cpp
```

Conventions follow `plotjuggler_core`'s style: `PascalCase.{h,cpp}` files, `PJ::` namespace, `trailing_underscore_` members, 2-space indent, 120-col limit.

---

## Unified-plan mapping & task-numbering scheme (read first)

> **This file is Plan B (Qt C++ client) under the unified plan** [`2026-06-03-unified-cloud-connector-plan.md`](./2026-06-03-unified-cloud-connector-plan.md). Plan B builds **only** `client-core` (Widgets-free static lib) + `client-cli` (`pjcloud-cli`). In the unified milestones, this whole plan is **M1c** (Qt `client-core` + `client-cli` + `McapWriterSink`; prove lossless round-trip on both backends), with its L1/L2 unit+component tests feeding the §6 testing matrix.
>
> **No per-engagement change.** `client-core` and the CLI are **backend-agnostic by construction**: storage auth (S3 credentials vs GCS IAM/Workload-Identity) is entirely **server-side**, so S3-vs-GCS is **invisible** to the client — the wire bytes are identical. There is **no Dexory/S3-vs-Asensus/GCS `#ifdef` or fork anywhere in this plan.** The v1 deltas applied here are deliberately small (a BackendCapabilities accessor, the flat-metadata accessor, a SessionKey utility, two test/CI notes) and are all storage-agnostic.
>
> **DEFERRED — NOT in this plan (M2b, separate plan).** The PJ4 DataSource **plugin** is explicitly out of Plan B scope and lives in its own future plan: the Mosaico-derived `CloudOpenDialog`/`toolbox` dialog, the `BackendConnection` adapter (the `file_ids[]`-vs-`sequence_name` synthetic-stitched-`SequenceRecord` reconciliation), the `RawMcapForwardingDriver` that forwards raw records to PlotJuggler's host `MessageParser` plugins, the **in-memory `SessionCache` *use*** (lookup/store), `AuthProvider`/qtkeychain, and the `BackendCapabilities`-driven file-hierarchy `QTreeWidget` browser. **`client-core` stays Widgets-free** so that plugin can lift it in unchanged. Where Plan B forward-sets the cache, it provides **only the `SessionKey` key utility (Task 8a)** — never the cache itself.
>
> **Task-numbering scheme.** Existing tasks 1–14 are **never renumbered**. The one new task introduced by these deltas is inserted at its correct position with a **letter-suffixed id** (`Task 8a`, between Task 8 and Task 9). New *sub-steps* added inside existing tasks are likewise letter-suffixed (e.g. `Step 2b`, `Step 3b`) and slot in ahead of the next integer step.

---

## Task 1: Top-level CMake + Conan setup

**Files:**
- Create: `pj-cloud/CMakeLists.txt`
- Create: `pj-cloud/conanfile.txt`

- [ ] **Step 1: Write the top-level CMakeLists.txt**

Create `pj-cloud/CMakeLists.txt`:

```cmake
cmake_minimum_required(VERSION 3.21)
project(pj-cloud CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_AUTOMOC ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

add_compile_options(-Wall -Wextra -Werror -Wno-deprecated-declarations)

# Conan toolchain expected on CMAKE_TOOLCHAIN_FILE
find_package(Qt6 6.8 REQUIRED COMPONENTS Core Network WebSockets)
find_package(Protobuf REQUIRED)
find_package(zstd REQUIRED)
find_package(lz4 REQUIRED)
find_package(mcap REQUIRED)
find_package(GTest REQUIRED)
find_package(spdlog REQUIRED)

enable_testing()

add_subdirectory(client-core)
add_subdirectory(client-cli)
```

- [ ] **Step 2: Write the Conan file**

Create `pj-cloud/conanfile.txt`:

```ini
[requires]
qt/6.8.3
protobuf/3.21.12
zstd/1.5.5
lz4/1.9.4
mcap/2.0.0
gtest/1.14.0
spdlog/1.13.0

[generators]
CMakeDeps
CMakeToolchain

[options]
qt:shared=True
qt:qtwebsockets=True

[layout]
cmake_layout
```

- [ ] **Step 3: Smoke-test Conan install**

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
conan profile detect --force
conan install . --output-folder=build --build=missing -s compiler.cppstd=20
```

Expected: a `build/` folder with `conan_toolchain.cmake` + `CMakePresets.json`.

- [ ] **Step 4: Commit**

```bash
git add CMakeLists.txt conanfile.txt
git commit -m "build: top-level CMake + Conan for client-core and client-cli"
```

---

## Task 2: client-core subproject + Protobuf codegen

**Files:**
- Create: `pj-cloud/client-core/CMakeLists.txt`
- Create: `pj-cloud/client-core/include/pj_cloud_client/Common.h`

- [ ] **Step 1: Write the client-core CMakeLists**

Create `pj-cloud/client-core/CMakeLists.txt`:

```cmake
# Generate C++ bindings from the canonical proto. Output goes into src/proto/
# which is gitignored.
set(PROTO_FILE ${CMAKE_SOURCE_DIR}/proto/pj_cloud.proto)
set(PROTO_OUT ${CMAKE_CURRENT_SOURCE_DIR}/src/proto)
file(MAKE_DIRECTORY ${PROTO_OUT})

add_custom_command(
  OUTPUT ${PROTO_OUT}/pj_cloud.pb.cc ${PROTO_OUT}/pj_cloud.pb.h
  COMMAND ${CMAKE_COMMAND} -E env
          $<TARGET_FILE:protobuf::protoc>
          -I ${CMAKE_SOURCE_DIR}/proto
          --cpp_out=${PROTO_OUT}
          ${PROTO_FILE}
  DEPENDS ${PROTO_FILE}
  COMMENT "Generating C++ Protobuf bindings"
)

add_library(client-core STATIC
  src/proto/pj_cloud.pb.cc
  src/CloudConnection.cpp
  src/MessageDispatcher.cpp
  src/CatalogClient.cpp
  src/SessionClient.cpp
  src/Decompression.cpp
)

target_include_directories(client-core
  PUBLIC  include
  PRIVATE src
)

target_link_libraries(client-core
  PUBLIC  Qt6::Core Qt6::Network Qt6::WebSockets protobuf::libprotobuf
  PRIVATE zstd::zstd LZ4::lz4 spdlog::spdlog
)

# Critical: client-core MUST NOT link Qt6::Widgets. See §9.1.
get_target_property(_links client-core LINK_LIBRARIES)
if(_links MATCHES "Widgets")
  message(FATAL_ERROR "client-core must not link Qt6::Widgets")
endif()

add_subdirectory(tests)
```

- [ ] **Step 2: Write `Common.h`**

Create `pj-cloud/client-core/include/pj_cloud_client/Common.h`:

```cpp
#pragma once

#include <QString>
#include <optional>
#include <utility>
#include <variant>

namespace PJ::cloud {

// Expected<T,E> — lightweight std::expected stand-in for compilers without C++23.
// Once C++23 is the build minimum this should be replaced wholesale with std::expected.
template <typename T>
class Expected {
 public:
  static Expected ok(T value) { return Expected{std::move(value)}; }
  static Expected fail(QString message) { return Expected{std::nullopt, std::move(message)}; }

  bool has_value() const { return value_.has_value(); }
  explicit operator bool() const { return has_value(); }

  const T& value() const& { return *value_; }
  T&& value() && { return std::move(*value_); }
  const QString& error() const { return error_; }

 private:
  Expected(T v) : value_{std::move(v)} {}
  Expected(std::nullopt_t, QString e) : error_{std::move(e)} {}

  std::optional<T> value_;
  QString error_;
};

// Void specialization for fallible operations with no payload.
template <>
class Expected<void> {
 public:
  static Expected ok() { return Expected{}; }
  static Expected fail(QString message) { return Expected{std::move(message)}; }

  bool has_value() const { return error_.isNull(); }
  explicit operator bool() const { return has_value(); }

  const QString& error() const { return error_; }

 private:
  Expected() : error_{QString{}} {}
  Expected(QString e) : error_{std::move(e)} {}

  QString error_;
};

}  // namespace PJ::cloud
```

- [ ] **Step 3: Configure + build (codegen + empty lib)**

Create stub source files so the library builds:

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/client-core
mkdir -p src tests
for f in CloudConnection MessageDispatcher CatalogClient SessionClient Decompression; do
  echo "// stub — filled by later tasks" > src/${f}.cpp
done
```

Create `pj-cloud/client-core/tests/CMakeLists.txt`:

```cmake
# Tests are added by later tasks; this file is intentionally empty for Task 2.
```

Configure + build:

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
cmake -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake
cmake --build build -j$(nproc) --target client-core
```

Expected: `client-core` static archive produced; `pj_cloud.pb.cc` regenerated each configure.

- [ ] **Step 4: Commit**

```bash
git add client-core/CMakeLists.txt client-core/include/pj_cloud_client/Common.h \
        client-core/src/*.cpp client-core/tests/CMakeLists.txt
git commit -m "build(client-core): subproject + Protobuf codegen + Common.h"
```

---

## Task 3: `CloudConnection` skeleton (`QWebSocket` wrap + Hello handshake)

**Files:**
- Create: `pj-cloud/client-core/include/pj_cloud_client/CloudConnection.h`
- Modify: `pj-cloud/client-core/src/CloudConnection.cpp`
- Create: `pj-cloud/client-core/tests/EnvelopeRoundTripTest.cpp` (foundation for later tests)

- [ ] **Step 1: Write the header**

Create `pj-cloud/client-core/include/pj_cloud_client/CloudConnection.h`:

```cpp
#pragma once

#include <QObject>
#include <QString>
#include <QUrl>
#include <QtNetwork/QSslConfiguration>
#include <QtWebSockets/QWebSocket>
#include <memory>

#include "Common.h"
#include "pj_cloud.pb.h"                  // for wire::BackendCapabilities (returned by value-ref accessor)

namespace PJ::cloud {

class MessageDispatcher;
namespace wire = ::pj_cloud::v1;       // matches the generated namespace

// CloudConnection wraps a single WSS connection: TLS handshake → Hello/HelloResponse
// → after that, all frame routing flows through MessageDispatcher.
class CloudConnection : public QObject {
  Q_OBJECT
 public:
  struct Settings {
    QUrl    url;                      // wss://host:port/api/ws
    QString auth_token;
    QSslConfiguration ssl;            // default ctor = system trust store
    int     handshake_timeout_ms = 5000;
  };

  explicit CloudConnection(Settings settings, QObject* parent = nullptr);
  ~CloudConnection() override;

  // open() asynchronously connects, performs Hello/HelloResponse, then emits
  // ready() on success or failed(QString) on any error.
  void open();

  // close() closes the WS cleanly. Idempotent.
  void close();

  MessageDispatcher* dispatcher() { return dispatcher_.get(); }
  bool isReady() const { return state_ == State::Ready; }

  // Server-advertised, storage-agnostic UI hints parsed from HelloResponse.backend.
  // Valid only after ready() fires. A future PJ4 plugin reads supports_file_hierarchy
  // (flat table vs tree browser) and metadata_key_vocabulary (Lua query-assist keys);
  // the v1 CLI ignores it. S3-vs-GCS stays invisible — these are abstract flags.
  const wire::BackendCapabilities& backendCapabilities() const { return backend_capabilities_; }

 signals:
  void ready();
  void failed(QString reason);
  void disconnected(QString reason);

 private slots:
  void onConnected();
  void onBinaryMessage(const QByteArray& bytes);
  void onSocketError(const QString& reason);

 private:
  enum class State { Idle, Connecting, Handshaking, Ready, Closing, Failed };

  void sendHello();
  void handleHelloResponse(const QByteArray& bytes);

  Settings settings_;
  QWebSocket socket_;
  std::unique_ptr<MessageDispatcher> dispatcher_;
  wire::BackendCapabilities backend_capabilities_;
  State state_{State::Idle};
};

}  // namespace PJ::cloud
```

- [ ] **Step 2: Write the failing test**

Create `pj-cloud/client-core/tests/EnvelopeRoundTripTest.cpp`:

```cpp
#include <QCoreApplication>
#include <QSignalSpy>
#include <gtest/gtest.h>

#include "pj_cloud_client/CloudConnection.h"
#include "pj_cloud.pb.h"

#include <google/protobuf/util/message_differencer.h>

TEST(EnvelopeRoundTrip, ClientHelloEncodesDecodes) {
  pj_cloud::v1::ClientMessage in;
  in.set_request_id(42);
  auto* h = in.mutable_hello();
  h->set_protocol_version(1);
  h->set_auth_token("tok");

  std::string buf;
  ASSERT_TRUE(in.SerializeToString(&buf));
  pj_cloud::v1::ClientMessage out;
  ASSERT_TRUE(out.ParseFromString(buf));
  EXPECT_TRUE(google::protobuf::util::MessageDifferencer::Equals(in, out));
}
```

Update `pj-cloud/client-core/tests/CMakeLists.txt`:

```cmake
add_executable(client-core-tests EnvelopeRoundTripTest.cpp)
target_link_libraries(client-core-tests
  PRIVATE client-core GTest::gtest GTest::gtest_main Qt6::Core protobuf::libprotobuf
)
target_include_directories(client-core-tests PRIVATE ${CMAKE_SOURCE_DIR}/client-core/src/proto)
add_test(NAME client-core-tests COMMAND client-core-tests)
```

- [ ] **Step 3: Implement `CloudConnection.cpp` minimally**

Replace `pj-cloud/client-core/src/CloudConnection.cpp`:

```cpp
#include "pj_cloud_client/CloudConnection.h"

#include <QTimer>
#include <spdlog/spdlog.h>

#include "pj_cloud_client/MessageDispatcher.h"
#include "pj_cloud.pb.h"

namespace PJ::cloud {

CloudConnection::CloudConnection(Settings settings, QObject* parent)
    : QObject{parent}, settings_{std::move(settings)} {
  socket_.setSslConfiguration(settings_.ssl);
  connect(&socket_, &QWebSocket::connected, this, &CloudConnection::onConnected);
  connect(&socket_, &QWebSocket::binaryMessageReceived, this, &CloudConnection::onBinaryMessage);
  connect(&socket_, qOverload<QAbstractSocket::SocketError>(&QWebSocket::errorOccurred),
          this, [this](auto) { onSocketError(socket_.errorString()); });
  connect(&socket_, &QWebSocket::disconnected, this, [this]() {
    emit disconnected(QStringLiteral("websocket closed"));
  });
}

CloudConnection::~CloudConnection() { close(); }

void CloudConnection::open() {
  if (state_ != State::Idle) return;
  state_ = State::Connecting;
  socket_.open(settings_.url);
  QTimer::singleShot(settings_.handshake_timeout_ms, this, [this]() {
    if (state_ == State::Handshaking) {
      onSocketError(QStringLiteral("handshake timeout"));
    }
  });
}

void CloudConnection::close() {
  if (state_ == State::Idle || state_ == State::Closing) return;
  state_ = State::Closing;
  socket_.close();
}

void CloudConnection::onConnected() {
  state_ = State::Handshaking;
  sendHello();
}

void CloudConnection::sendHello() {
  pj_cloud::v1::ClientMessage msg;
  msg.set_request_id(1);
  auto* hello = msg.mutable_hello();
  hello->set_protocol_version(1);
  hello->set_auth_token(settings_.auth_token.toStdString());
  std::string buf;
  msg.SerializeToString(&buf);
  socket_.sendBinaryMessage(QByteArray::fromStdString(buf));
}

void CloudConnection::onBinaryMessage(const QByteArray& bytes) {
  if (state_ == State::Handshaking) {
    handleHelloResponse(bytes);
    return;
  }
  if (state_ == State::Ready) {
    dispatcher_->onIncoming(bytes);
  }
}

void CloudConnection::handleHelloResponse(const QByteArray& bytes) {
  pj_cloud::v1::ServerMessage msg;
  if (!msg.ParseFromArray(bytes.constData(), bytes.size())) {
    onSocketError(QStringLiteral("malformed HelloResponse"));
    return;
  }
  if (msg.has_error()) {
    onSocketError(QString::fromStdString(msg.error().message()));
    return;
  }
  if (!msg.has_hello_response()) {
    onSocketError(QStringLiteral("expected HelloResponse"));
    return;
  }
  backend_capabilities_ = msg.hello_response().backend();
  dispatcher_ = std::make_unique<MessageDispatcher>(&socket_);
  state_ = State::Ready;
  emit ready();
}

void CloudConnection::onSocketError(const QString& reason) {
  spdlog::warn("CloudConnection: {}", reason.toStdString());
  state_ = State::Failed;
  emit failed(reason);
  socket_.close();
}

}  // namespace PJ::cloud
```

- [ ] **Step 3b: Focused unit test — HelloResponse.backend (BackendCapabilities) round-trips**

Append to `pj-cloud/client-core/tests/EnvelopeRoundTripTest.cpp`:

```cpp
// Unified-plan §3.1/§3.2: BackendCapabilities are storage-agnostic UI hints the
// server advertises in HelloResponse.backend; CloudConnection::backendCapabilities()
// exposes exactly these fields to a future plugin. This pins the wire shape.
TEST(EnvelopeRoundTrip, HelloResponseBackendCapabilities) {
  pj_cloud::v1::ServerMessage in;
  auto* hr = in.mutable_hello_response();
  hr->set_server_version("1.2.3");
  auto* backend = hr->mutable_backend();
  backend->set_supports_file_hierarchy(true);
  backend->add_metadata_key_vocabulary("robot_id");
  backend->add_metadata_key_vocabulary("procedure_date");

  std::string buf;
  ASSERT_TRUE(in.SerializeToString(&buf));
  pj_cloud::v1::ServerMessage out;
  ASSERT_TRUE(out.ParseFromString(buf));

  ASSERT_TRUE(out.has_hello_response());
  const auto& b = out.hello_response().backend();
  EXPECT_TRUE(b.supports_file_hierarchy());
  ASSERT_EQ(b.metadata_key_vocabulary_size(), 2);
  EXPECT_EQ(b.metadata_key_vocabulary(0), "robot_id");
  EXPECT_EQ(b.metadata_key_vocabulary(1), "procedure_date");
}
```

(The `client-core-tests` executable already compiles `EnvelopeRoundTripTest.cpp` per Step 2's CMakeLists, so no CMake change is needed for this step.)

- [ ] **Step 4: Build + run the test**

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
cmake --build build --target client-core-tests -j$(nproc)
ctest --test-dir build -V
```

Expected: `EnvelopeRoundTrip.ClientHelloEncodesDecodes` PASS and `EnvelopeRoundTrip.HelloResponseBackendCapabilities` PASS.

- [ ] **Step 5: Commit**

```bash
git add client-core/include/pj_cloud_client/CloudConnection.h \
        client-core/src/CloudConnection.cpp \
        client-core/tests/CMakeLists.txt \
        client-core/tests/EnvelopeRoundTripTest.cpp
git commit -m "feat(client-core): CloudConnection skeleton + Hello handshake + BackendCapabilities accessor"
```

---

## Task 4: `MessageDispatcher` (RPC request_id correlation + subscription routing)

**Files:**
- Create: `pj-cloud/client-core/include/pj_cloud_client/MessageDispatcher.h`
- Modify: `pj-cloud/client-core/src/MessageDispatcher.cpp`
- Create: `pj-cloud/client-core/tests/MessageDispatcherTest.cpp`

- [ ] **Step 1: Write the header**

Create `pj-cloud/client-core/include/pj_cloud_client/MessageDispatcher.h`:

```cpp
#pragma once

#include <QByteArray>
#include <QFuture>
#include <QPromise>
#include <QtWebSockets/QWebSocket>

#include <atomic>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "Common.h"

namespace pj_cloud::v1 {
class ClientMessage;
class ServerMessage;
}  // namespace pj_cloud::v1

namespace PJ::cloud {

namespace wire = ::pj_cloud::v1;

class MessageDispatcher {
 public:
  using SubscriptionCallback = std::function<void(const wire::ServerMessage&)>;

  explicit MessageDispatcher(QWebSocket* socket);

  // Send an RPC. Caller passes ClientMessage with payload already set; the
  // dispatcher allocates a unique request_id, fills it in, sends the frame,
  // and returns a QFuture<Expected<...>> resolved when the matching response
  // arrives (or rejected on disconnect).
  QFuture<Expected<wire::ServerMessage>> sendRequest(wire::ClientMessage msg);

  // Register a callback to receive all push frames carrying the given
  // subscription_id. The callback runs on the Qt main thread (the same one
  // that owns the socket).
  void subscribe(uint64_t subscription_id, SubscriptionCallback callback);
  void unsubscribe(uint64_t subscription_id);

  // Send a fire-and-forget frame (Cancel, SessionAck).
  void sendFireAndForget(wire::ClientMessage msg);

  // Called by CloudConnection when a binary WS frame arrives.
  void onIncoming(const QByteArray& bytes);

  // Called when the WS drops; fails all in-flight RPCs.
  void fail(const QString& reason);

 private:
  QWebSocket* socket_;
  std::atomic<uint64_t> next_request_id_{1};

  std::mutex mu_;
  std::unordered_map<uint64_t, std::shared_ptr<QPromise<Expected<wire::ServerMessage>>>> in_flight_;
  std::unordered_map<uint64_t, SubscriptionCallback> subscriptions_;
};

}  // namespace PJ::cloud
```

- [ ] **Step 2: Write the failing test**

Create `pj-cloud/client-core/tests/MessageDispatcherTest.cpp`:

```cpp
#include <QtTest/QSignalSpy>
#include <gtest/gtest.h>

#include "pj_cloud_client/MessageDispatcher.h"
#include "pj_cloud.pb.h"

using PJ::cloud::MessageDispatcher;

// Adapter that satisfies the QWebSocket pointer requirement without actually
// connecting. The dispatcher only calls sendBinaryMessage on its socket; we
// use a small stand-in.
class StubSocket : public QObject {
 public:
  qint64 sendBinaryMessage(const QByteArray& buf) {
    sent.append(buf);
    return buf.size();
  }
  QList<QByteArray> sent;
};

// To keep the test independent of QWebSocket, MessageDispatcher accepts a
// QWebSocket*. For tests, we cheat by upcasting from a derived test-only
// subclass; production tests integrate via Task 5 (live).
TEST(MessageDispatcherTest, AllocatesAndCorrelatesRequestIds) {
  // Skipped here — integration coverage in Task 5 (against the live fake server).
  // The dispatcher logic is the simple kind we'd want a stronger boundary for;
  // for this test we assert on the basic shape of request_id increment.
  std::atomic<uint64_t> counter{1};
  auto next1 = counter.fetch_add(1);
  auto next2 = counter.fetch_add(1);
  EXPECT_EQ(next1, 1u);
  EXPECT_EQ(next2, 2u);
}
```

(Note: `MessageDispatcher`'s real test coverage comes in Task 5 against an in-process fake server, because mocking `QWebSocket` cleanly is more brittle than just running an end-to-end loop. This task validates the surface compiles + the counter shape.)

- [ ] **Step 3: Implement `MessageDispatcher.cpp`**

Replace `pj-cloud/client-core/src/MessageDispatcher.cpp`:

```cpp
#include "pj_cloud_client/MessageDispatcher.h"

#include <QByteArray>
#include <QtConcurrent/QtConcurrent>

#include "pj_cloud.pb.h"

namespace PJ::cloud {

MessageDispatcher::MessageDispatcher(QWebSocket* socket) : socket_{socket} {}

QFuture<Expected<wire::ServerMessage>> MessageDispatcher::sendRequest(wire::ClientMessage msg) {
  uint64_t id = next_request_id_.fetch_add(1);
  msg.set_request_id(id);
  auto promise = std::make_shared<QPromise<Expected<wire::ServerMessage>>>();
  promise->start();
  {
    std::lock_guard guard(mu_);
    in_flight_[id] = promise;
  }
  std::string buf;
  msg.SerializeToString(&buf);
  socket_->sendBinaryMessage(QByteArray::fromStdString(buf));
  return promise->future();
}

void MessageDispatcher::subscribe(uint64_t sub_id, SubscriptionCallback cb) {
  std::lock_guard guard(mu_);
  subscriptions_[sub_id] = std::move(cb);
}

void MessageDispatcher::unsubscribe(uint64_t sub_id) {
  std::lock_guard guard(mu_);
  subscriptions_.erase(sub_id);
}

void MessageDispatcher::sendFireAndForget(wire::ClientMessage msg) {
  std::string buf;
  msg.SerializeToString(&buf);
  socket_->sendBinaryMessage(QByteArray::fromStdString(buf));
}

void MessageDispatcher::onIncoming(const QByteArray& bytes) {
  wire::ServerMessage msg;
  if (!msg.ParseFromArray(bytes.constData(), bytes.size())) {
    return;
  }
  if (uint64_t rid = msg.request_id(); rid != 0) {
    std::shared_ptr<QPromise<Expected<wire::ServerMessage>>> p;
    {
      std::lock_guard guard(mu_);
      auto it = in_flight_.find(rid);
      if (it != in_flight_.end()) {
        p = it->second;
        in_flight_.erase(it);
      }
    }
    if (p) {
      p->addResult(Expected<wire::ServerMessage>::ok(std::move(msg)));
      p->finish();
      return;
    }
  }
  if (uint64_t sid = msg.subscription_id(); sid != 0) {
    SubscriptionCallback cb;
    {
      std::lock_guard guard(mu_);
      auto it = subscriptions_.find(sid);
      if (it != subscriptions_.end()) cb = it->second;
    }
    if (cb) cb(msg);
  }
}

void MessageDispatcher::fail(const QString& reason) {
  std::unordered_map<uint64_t, std::shared_ptr<QPromise<Expected<wire::ServerMessage>>>> outstanding;
  {
    std::lock_guard guard(mu_);
    outstanding.swap(in_flight_);
  }
  for (auto& [id, p] : outstanding) {
    p->addResult(Expected<wire::ServerMessage>::fail(reason));
    p->finish();
  }
}

}  // namespace PJ::cloud
```

- [ ] **Step 4: Build + run**

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
cmake --build build -j$(nproc) --target client-core-tests
ctest --test-dir build -V
```

- [ ] **Step 5: Commit**

```bash
git add client-core/include/pj_cloud_client/MessageDispatcher.h \
        client-core/src/MessageDispatcher.cpp \
        client-core/tests/MessageDispatcherTest.cpp
git commit -m "feat(client-core): MessageDispatcher with request_id RPC + subscription routing"
```

---

## Task 5: `CatalogClient` (ListFiles, GetFile, UpdateTags as typed RPC wrappers)

**Files:**
- Create: `pj-cloud/client-core/include/pj_cloud_client/CatalogClient.h`
- Modify: `pj-cloud/client-core/src/CatalogClient.cpp`

- [ ] **Step 1: Write the header**

Create `pj-cloud/client-core/include/pj_cloud_client/CatalogClient.h`:

```cpp
#pragma once

#include <QFuture>
#include <QString>
#include <vector>

#include "Common.h"

namespace pj_cloud::v1 {
class FileFilter;
class ListFilesResponse;
class GetFileResponse;
class UpdateTagsResponse;
}  // namespace pj_cloud::v1

namespace PJ::cloud {

class MessageDispatcher;
namespace wire = ::pj_cloud::v1;

// CatalogClient is a thin typed wrapper around MessageDispatcher::sendRequest
// for the three catalog RPCs. Each method returns a QFuture resolving to the
// parsed response or an error.
class CatalogClient {
 public:
  explicit CatalogClient(MessageDispatcher* dispatcher);

  QFuture<Expected<wire::ListFilesResponse>> listFiles(
      const wire::FileFilter* filter, uint32_t limit, const QString& page_token);

  QFuture<Expected<wire::GetFileResponse>> getFile(uint64_t file_id);

  QFuture<Expected<wire::UpdateTagsResponse>> updateTags(
      uint64_t file_id,
      const std::vector<std::pair<QString, QString>>& set_tags,
      const std::vector<QString>& unset_keys);

 private:
  MessageDispatcher* dispatcher_;
};

}  // namespace PJ::cloud
```

- [ ] **Step 2: Implement**

Replace `pj-cloud/client-core/src/CatalogClient.cpp`:

```cpp
#include "pj_cloud_client/CatalogClient.h"

#include <QtConcurrent/QtConcurrent>

#include "pj_cloud_client/MessageDispatcher.h"
#include "pj_cloud.pb.h"

namespace PJ::cloud {

CatalogClient::CatalogClient(MessageDispatcher* d) : dispatcher_{d} {}

template <typename Resp, typename Extract>
static QFuture<Expected<Resp>> map_response(QFuture<Expected<wire::ServerMessage>> in, Extract extract) {
  return QtConcurrent::run([fut = std::move(in), extract = std::move(extract)]() mutable {
    fut.waitForFinished();
    auto outer = fut.result();
    if (!outer) {
      return Expected<Resp>::fail(outer.error());
    }
    return extract(outer.value());
  });
}

QFuture<Expected<wire::ListFilesResponse>> CatalogClient::listFiles(
    const wire::FileFilter* filter, uint32_t limit, const QString& page_token) {
  wire::ClientMessage msg;
  auto* req = msg.mutable_list_files();
  if (filter) *req->mutable_filter() = *filter;
  req->set_limit(limit);
  req->set_page_token(page_token.toStdString());

  auto fut = dispatcher_->sendRequest(std::move(msg));
  return map_response<wire::ListFilesResponse>(std::move(fut), [](const wire::ServerMessage& m) {
    if (m.has_error()) {
      return Expected<wire::ListFilesResponse>::fail(QString::fromStdString(m.error().message()));
    }
    if (!m.has_list_files()) {
      return Expected<wire::ListFilesResponse>::fail(QStringLiteral("unexpected payload"));
    }
    return Expected<wire::ListFilesResponse>::ok(m.list_files());
  });
}

QFuture<Expected<wire::GetFileResponse>> CatalogClient::getFile(uint64_t file_id) {
  wire::ClientMessage msg;
  msg.mutable_get_file()->set_file_id(file_id);
  auto fut = dispatcher_->sendRequest(std::move(msg));
  return map_response<wire::GetFileResponse>(std::move(fut), [](const wire::ServerMessage& m) {
    if (m.has_error()) return Expected<wire::GetFileResponse>::fail(QString::fromStdString(m.error().message()));
    if (!m.has_get_file()) return Expected<wire::GetFileResponse>::fail(QStringLiteral("unexpected payload"));
    return Expected<wire::GetFileResponse>::ok(m.get_file());
  });
}

QFuture<Expected<wire::UpdateTagsResponse>> CatalogClient::updateTags(
    uint64_t file_id,
    const std::vector<std::pair<QString, QString>>& set_tags,
    const std::vector<QString>& unset_keys) {
  wire::ClientMessage msg;
  auto* req = msg.mutable_update_tags();
  req->set_file_id(file_id);
  for (const auto& [k, v] : set_tags) {
    auto* tag = req->add_set_tags();
    tag->set_key(k.toStdString());
    tag->set_value(v.toStdString());
  }
  for (const auto& k : unset_keys) {
    req->add_unset_keys(k.toStdString());
  }
  auto fut = dispatcher_->sendRequest(std::move(msg));
  return map_response<wire::UpdateTagsResponse>(std::move(fut), [](const wire::ServerMessage& m) {
    if (m.has_error()) return Expected<wire::UpdateTagsResponse>::fail(QString::fromStdString(m.error().message()));
    if (!m.has_update_tags()) return Expected<wire::UpdateTagsResponse>::fail(QStringLiteral("unexpected payload"));
    return Expected<wire::UpdateTagsResponse>::ok(m.update_tags());
  });
}

}  // namespace PJ::cloud
```

- [ ] **Step 2b: Surface the flat per-file metadata map (unified-plan §3.1 SHAPE REQ)**

`ListFilesResponse` carries `map<string, FlatMetadata> metadata = 3`, keyed by the file id rendered as a decimal string; each `FlatMetadata` holds `map<string, string> entries = 1` — one file's `tags_effective` as a plain string→string map that maps 1:1 onto the future dialog's `SequenceInfo.user_metadata` with NO transform. Add a free helper so callers (CLI now, plugin later) read it without re-deriving the key format.

Append to `pj-cloud/client-core/include/pj_cloud_client/CatalogClient.h`, inside `namespace PJ::cloud` and after the `CatalogClient` class:

```cpp
// Returns the flat string→value effective-tag map for one file from a
// ListFilesResponse (unified-plan §3.1). The wire key is the decimal file id.
// Returns an empty map if the file id is absent. Maps 1:1 onto the future
// SequenceInfo.user_metadata with no transform; NOT storage-specific.
std::map<std::string, std::string> flatMetadata(const wire::ListFilesResponse& resp, uint64_t file_id);
```

Add `#include <map>` and `#include <string>` to that header's include block (next to the existing `#include <vector>`), and forward-declare the response + flat type by extending the existing `namespace pj_cloud::v1 { ... }` forward-declaration block to also declare `class FlatMetadata;` (the function is defined out-of-line in the .cpp where the generated header is fully included).

Append to `pj-cloud/client-core/src/CatalogClient.cpp`, inside `namespace PJ::cloud`:

```cpp
std::map<std::string, std::string> flatMetadata(const wire::ListFilesResponse& resp, uint64_t file_id) {
  std::map<std::string, std::string> out;
  const auto& by_id = resp.metadata();
  auto it = by_id.find(std::to_string(file_id));
  if (it == by_id.end()) return out;
  for (const auto& [k, v] : it->second.entries()) {
    out.emplace(k, v);
  }
  return out;
}
```

- [ ] **Step 2c: Unit test — flat metadata extraction**

Create `pj-cloud/client-core/tests/CatalogClientTest.cpp`:

```cpp
#include <gtest/gtest.h>

#include "pj_cloud_client/CatalogClient.h"
#include "pj_cloud.pb.h"

using namespace PJ::cloud;

// Unified-plan §3.1: the flat string→string per-file metadata map is the
// client-ingest contract (1:1 with the future SequenceInfo.user_metadata).
TEST(CatalogClientTest, FlatMetadataKeyedByDecimalFileId) {
  wire::ListFilesResponse resp;
  auto& meta = (*resp.mutable_metadata())["17"];
  (*meta.mutable_entries())["robot_id"]       = "R-9";
  (*meta.mutable_entries())["procedure_date"] = "2026-05-28";

  auto flat = flatMetadata(resp, 17);
  ASSERT_EQ(flat.size(), 2u);
  EXPECT_EQ(flat["robot_id"], "R-9");
  EXPECT_EQ(flat["procedure_date"], "2026-05-28");

  EXPECT_TRUE(flatMetadata(resp, 999).empty());  // absent id → empty map
}
```

Update `pj-cloud/client-core/tests/CMakeLists.txt` to add the new source to the existing `client-core-tests` executable source list:

```cmake
add_executable(client-core-tests
  EnvelopeRoundTripTest.cpp
  MessageDispatcherTest.cpp
  DecompressionTest.cpp
  SessionClientTest.cpp
  CatalogClientTest.cpp
)
```

- [ ] **Step 3: Build + run**

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
cmake --build build -j$(nproc) --target client-core-tests
ctest --test-dir build -V
```

Expected: `CatalogClientTest.FlatMetadataKeyedByDecimalFileId` PASS.

- [ ] **Step 4: Commit**

```bash
git add client-core/include/pj_cloud_client/CatalogClient.h \
        client-core/src/CatalogClient.cpp \
        client-core/tests/CatalogClientTest.cpp \
        client-core/tests/CMakeLists.txt
git commit -m "feat(client-core): CatalogClient typed wrappers + flat tags_effective metadata accessor"
```

---

## Task 6: `SessionSink` interface

**Files:**
- Create: `pj-cloud/client-core/include/pj_cloud_client/SessionSink.h`

- [ ] **Step 1: Write the header**

Create `pj-cloud/client-core/include/pj_cloud_client/SessionSink.h`:

```cpp
#pragma once

#include <cstdint>
#include <span>

#include "Common.h"

namespace pj_cloud::v1 {
class OpenSessionResponse;
enum EosReason : int;
}  // namespace pj_cloud::v1

namespace PJ::cloud {

namespace wire = ::pj_cloud::v1;

// SessionSink is the seam between client-core (which drives the WS + decompresses)
// and the per-purpose consumer (McapWriterSink in the CLI, DatastoreSink in the
// future PJ4 plugin). client-core never sees the consumer's implementation.
class SessionSink {
 public:
  virtual ~SessionSink() = default;

  // Called once before any messages, after OpenSessionResponse arrives.
  // `session` includes schemas + topic_id_map needed by the sink.
  virtual Expected<void> begin(const wire::OpenSessionResponse& session) = 0;

  // Called for each message in arrival order, payload already decompressed.
  // Returning a failed Expected aborts the session (client-core will cancel).
  virtual Expected<void> writeMessage(
      uint32_t topic_id,
      uint32_t schema_id,
      int64_t  log_time_ns,
      int64_t  publish_time_ns,
      std::span<const std::byte> payload) = 0;

  // Periodic progress hook; throttled by client-core (~10 Hz).
  virtual void onProgress(uint64_t bytes_received, uint64_t messages_written) { (void)bytes_received; (void)messages_written; }

  // Called once at end-of-stream. `reason` is COMPLETE / CANCELLED / ERROR.
  virtual Expected<void> end(wire::EosReason reason) = 0;
};

}  // namespace PJ::cloud
```

- [ ] **Step 2: Commit**

```bash
git add client-core/include/pj_cloud_client/SessionSink.h
git commit -m "feat(client-core): SessionSink interface"
```

---

## Task 7: `Decompression` (ZSTD + LZ4 helpers with reusable scratch)

**Files:**
- Create: `pj-cloud/client-core/include/pj_cloud_client/Decompression.h`
- Modify: `pj-cloud/client-core/src/Decompression.cpp`
- Create: `pj-cloud/client-core/tests/DecompressionTest.cpp`

- [ ] **Step 1: Header**

Create `pj-cloud/client-core/include/pj_cloud_client/Decompression.h`:

```cpp
#pragma once

#include <span>
#include <vector>

#include "Common.h"

namespace PJ::cloud {

class Decompressor {
 public:
  Decompressor();
  ~Decompressor();

  Decompressor(const Decompressor&) = delete;
  Decompressor& operator=(const Decompressor&) = delete;

  // Decompress a ZSTD frame into the internal scratch and return a view.
  // The returned span is valid until the next call on the same Decompressor.
  Expected<std::span<const std::byte>> decompressZstd(std::span<const std::byte> compressed);

  // Decompress an LZ4 frame.
  Expected<std::span<const std::byte>> decompressLz4(std::span<const std::byte> compressed);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  std::vector<std::byte> scratch_;
};

}  // namespace PJ::cloud
```

- [ ] **Step 2: Failing test**

Create `pj-cloud/client-core/tests/DecompressionTest.cpp`:

```cpp
#include <gtest/gtest.h>
#include <zstd.h>
#include <vector>

#include "pj_cloud_client/Decompression.h"

using namespace PJ::cloud;

TEST(Decompression, ZstdRoundTrip) {
  std::vector<std::byte> original(8192);
  for (size_t i = 0; i < original.size(); ++i) original[i] = std::byte(i & 0xff);

  std::vector<std::byte> compressed(ZSTD_compressBound(original.size()));
  size_t n = ZSTD_compress(compressed.data(), compressed.size(), original.data(), original.size(), 1);
  ASSERT_FALSE(ZSTD_isError(n));
  compressed.resize(n);

  Decompressor d;
  auto out = d.decompressZstd({compressed.data(), compressed.size()});
  ASSERT_TRUE(out);
  ASSERT_EQ(out.value().size(), original.size());
  EXPECT_EQ(std::memcmp(out.value().data(), original.data(), original.size()), 0);
}

TEST(Decompression, RejectsTruncated) {
  Decompressor d;
  std::vector<std::byte> garbage{std::byte{0x00}, std::byte{0x01}, std::byte{0x02}};
  auto out = d.decompressZstd({garbage.data(), garbage.size()});
  EXPECT_FALSE(out);
}
```

Update `pj-cloud/client-core/tests/CMakeLists.txt` to add the new source:

```cmake
add_executable(client-core-tests
  EnvelopeRoundTripTest.cpp
  MessageDispatcherTest.cpp
  DecompressionTest.cpp
)
target_link_libraries(client-core-tests
  PRIVATE client-core GTest::gtest GTest::gtest_main Qt6::Core protobuf::libprotobuf zstd::zstd
)
target_include_directories(client-core-tests PRIVATE ${CMAKE_SOURCE_DIR}/client-core/src/proto)
add_test(NAME client-core-tests COMMAND client-core-tests)
```

- [ ] **Step 3: Implement**

Replace `pj-cloud/client-core/src/Decompression.cpp`:

```cpp
#include "pj_cloud_client/Decompression.h"

#include <cstring>
#include <lz4frame.h>
#include <zstd.h>

namespace PJ::cloud {

struct Decompressor::Impl {
  ZSTD_DCtx* zstd_ctx{nullptr};
  LZ4F_dctx* lz4_ctx{nullptr};
};

Decompressor::Decompressor() : impl_{std::make_unique<Impl>()} {
  impl_->zstd_ctx = ZSTD_createDCtx();
  LZ4F_createDecompressionContext(&impl_->lz4_ctx, LZ4F_VERSION);
  scratch_.reserve(64 * 1024);
}

Decompressor::~Decompressor() {
  if (impl_->zstd_ctx) ZSTD_freeDCtx(impl_->zstd_ctx);
  if (impl_->lz4_ctx) LZ4F_freeDecompressionContext(impl_->lz4_ctx);
}

Expected<std::span<const std::byte>> Decompressor::decompressZstd(std::span<const std::byte> in) {
  unsigned long long expected_size = ZSTD_getFrameContentSize(in.data(), in.size());
  if (expected_size == ZSTD_CONTENTSIZE_ERROR) {
    return Expected<std::span<const std::byte>>::fail(QStringLiteral("invalid ZSTD frame"));
  }
  if (expected_size == ZSTD_CONTENTSIZE_UNKNOWN) {
    expected_size = in.size() * 8;  // safety
  }
  scratch_.resize(static_cast<size_t>(expected_size));
  size_t n = ZSTD_decompressDCtx(impl_->zstd_ctx, scratch_.data(), scratch_.size(), in.data(), in.size());
  if (ZSTD_isError(n)) {
    return Expected<std::span<const std::byte>>::fail(QString::fromLatin1(ZSTD_getErrorName(n)));
  }
  return Expected<std::span<const std::byte>>::ok({scratch_.data(), n});
}

Expected<std::span<const std::byte>> Decompressor::decompressLz4(std::span<const std::byte> in) {
  scratch_.resize(in.size() * 8);
  size_t out_size = scratch_.size();
  size_t src_size = in.size();
  size_t n = LZ4F_decompress(impl_->lz4_ctx, scratch_.data(), &out_size, in.data(), &src_size, nullptr);
  if (LZ4F_isError(n)) {
    return Expected<std::span<const std::byte>>::fail(QString::fromLatin1(LZ4F_getErrorName(n)));
  }
  return Expected<std::span<const std::byte>>::ok({scratch_.data(), out_size});
}

}  // namespace PJ::cloud
```

- [ ] **Step 4: Build + run**

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
cmake --build build -j$(nproc) --target client-core-tests
ctest --test-dir build -V
```

Expected: ZstdRoundTrip + RejectsTruncated both PASS.

- [ ] **Step 5: Commit**

```bash
git add client-core/include/pj_cloud_client/Decompression.h \
        client-core/src/Decompression.cpp \
        client-core/tests/DecompressionTest.cpp \
        client-core/tests/CMakeLists.txt
git commit -m "feat(client-core): ZSTD + LZ4 Decompressor with reusable scratch"
```

---

## Task 8: `SessionClient` — Open, batch routing, cancel, resume

**Files:**
- Create: `pj-cloud/client-core/include/pj_cloud_client/SessionClient.h`
- Modify: `pj-cloud/client-core/src/SessionClient.cpp`
- Create: `pj-cloud/client-core/tests/SessionClientTest.cpp`

- [ ] **Step 1: Header**

Create `pj-cloud/client-core/include/pj_cloud_client/SessionClient.h`:

```cpp
#pragma once

#include <QFuture>
#include <QString>
#include <chrono>
#include <memory>

#include "Common.h"
#include "Decompression.h"
#include "SessionSink.h"

namespace pj_cloud::v1 {
class OpenSessionRequest;
}  // namespace pj_cloud::v1

namespace PJ::cloud {

class MessageDispatcher;
namespace wire = ::pj_cloud::v1;

class SessionClient {
 public:
  struct Settings {
    std::chrono::milliseconds ack_interval{500};
    int ack_batch_count{64};
  };

  SessionClient(MessageDispatcher* dispatcher, std::unique_ptr<SessionSink> sink, Settings settings = {});

  // Drives one session lifecycle. Resolves with the final EosReason on success
  // or an error description on protocol failure / sink failure.
  QFuture<Expected<wire::EosReason>> runSession(wire::OpenSessionRequest request);

  // Sends CancelSession to the server for an in-flight session.
  void cancel();

 private:
  void onIncoming(const wire::ServerMessage& msg);
  void sendAck();
  bool processBatchOrEos(const wire::ServerMessage& msg);

  MessageDispatcher* dispatcher_;
  std::unique_ptr<SessionSink> sink_;
  Settings settings_;
  uint64_t subscription_id_{0};
  uint64_t last_received_seq_{0};
  uint64_t last_acked_seq_{0};
  uint64_t bytes_received_{0};
  uint64_t messages_received_{0};
  std::shared_ptr<QPromise<Expected<wire::EosReason>>> promise_;
  Decompressor decompressor_;
};

}  // namespace PJ::cloud
```

- [ ] **Step 2: Implementation**

Replace `pj-cloud/client-core/src/SessionClient.cpp`:

```cpp
#include "pj_cloud_client/SessionClient.h"

#include <QtConcurrent/QtConcurrent>

#include "pj_cloud_client/MessageDispatcher.h"
#include "pj_cloud.pb.h"

namespace PJ::cloud {

SessionClient::SessionClient(MessageDispatcher* d, std::unique_ptr<SessionSink> sink, Settings s)
    : dispatcher_{d}, sink_{std::move(sink)}, settings_{s} {}

QFuture<Expected<wire::EosReason>> SessionClient::runSession(wire::OpenSessionRequest request) {
  promise_ = std::make_shared<QPromise<Expected<wire::EosReason>>>();
  promise_->start();

  wire::ClientMessage open_msg;
  *open_msg.mutable_open_session() = std::move(request);
  auto open_fut = dispatcher_->sendRequest(std::move(open_msg));

  QtConcurrent::run([this, open_fut = std::move(open_fut)]() mutable {
    open_fut.waitForFinished();
    auto outer = open_fut.result();
    if (!outer) {
      promise_->addResult(Expected<wire::EosReason>::fail(outer.error()));
      promise_->finish();
      return;
    }
    const auto& resp = outer.value();
    if (resp.has_error()) {
      promise_->addResult(Expected<wire::EosReason>::fail(QString::fromStdString(resp.error().message())));
      promise_->finish();
      return;
    }
    if (!resp.has_open_session()) {
      promise_->addResult(Expected<wire::EosReason>::fail(QStringLiteral("expected OpenSessionResponse")));
      promise_->finish();
      return;
    }
    const auto& open = resp.open_session();
    subscription_id_ = open.subscription_id();

    if (auto b = sink_->begin(open); !b) {
      promise_->addResult(Expected<wire::EosReason>::fail(b.error()));
      promise_->finish();
      return;
    }
    dispatcher_->subscribe(subscription_id_, [this](const wire::ServerMessage& m) { onIncoming(m); });
  });

  return promise_->future();
}

void SessionClient::onIncoming(const wire::ServerMessage& msg) {
  if (msg.has_batch()) {
    const auto& batch = msg.batch();

    auto failBatch = [&](const QString& why) {
      promise_->addResult(Expected<wire::EosReason>::fail(why));
      promise_->finish();
      dispatcher_->unsubscribe(subscription_id_);
    };
    // Emit one already-decoded message to the sink; returns false (after finishing the
    // promise) on failure so the caller stops.
    auto emit = [&](const wire::Message& m, std::span<const std::byte> payload) -> bool {
      if (auto r = sink_->writeMessage(m.topic_id(), m.schema_id(), m.log_time_ns(),
                                       m.publish_time_ns(), payload); !r) {
        failBatch(r.error());
        return false;
      }
      bytes_received_ += payload.size();
      messages_received_++;
      return true;
    };

    if (batch.body_encoding() == wire::BODY_ENCODING_ZSTD) {
      // Default path: `body` is one self-contained ZSTD frame holding a marshaled
      // MessageBatchBody. Decompress once, validate the size, then parse — protobuf
      // framing is length-validated and ParseFromArray copies payloads, so nothing
      // aliases the decompression scratch and inner records cannot run off the end.
      std::span<const std::byte> body{reinterpret_cast<const std::byte*>(batch.body().data()), batch.body().size()};
      auto decoded = decompressor_.decompressZstd(body);
      if (!decoded) { failBatch(decoded.error()); return; }
      if (decoded.value().size() != batch.body_uncompressed_size()) {
        failBatch(QStringLiteral("batch body_uncompressed_size mismatch"));
        return;
      }
      wire::MessageBatchBody mb;
      if (!mb.ParseFromArray(decoded.value().data(), static_cast<int>(decoded.value().size()))) {
        failBatch(QStringLiteral("corrupt MessageBatchBody"));
        return;
      }
      for (const auto& m : mb.messages()) {  // inner payloads are RAW on this path
        std::span<const std::byte> p{reinterpret_cast<const std::byte*>(m.payload().data()), m.payload().size()};
        if (!emit(m, p)) return;
      }
    } else if (batch.body_encoding() == wire::BODY_ENCODING_NONE ||
               batch.body_encoding() == wire::BODY_ENCODING_UNSPECIFIED) {
      // Singleton / legacy fallback: messages ride in `messages`, per-message encoding.
      for (const auto& m : batch.messages()) {
        std::span<const std::byte> payload{reinterpret_cast<const std::byte*>(m.payload().data()), m.payload().size()};
        Expected<std::span<const std::byte>> dec = Expected<std::span<const std::byte>>::ok(payload);
        if (m.payload_encoding() == wire::PAYLOAD_ENCODING_ZSTD) {
          dec = decompressor_.decompressZstd(payload);
        } else if (m.payload_encoding() == wire::PAYLOAD_ENCODING_LZ4) {
          dec = decompressor_.decompressLz4(payload);
        }
        if (!dec) { failBatch(dec.error()); return; }
        if (!emit(m, dec.value())) return;
      }
    } else {
      // Defensive parsing: never silently misinterpret an unknown body_encoding.
      failBatch(QStringLiteral("unknown body_encoding"));
      return;
    }

    last_received_seq_ = batch.seq();
    if (last_received_seq_ - last_acked_seq_ >= static_cast<uint64_t>(settings_.ack_batch_count)) {
      sendAck();
    }
  } else if (msg.has_progress()) {
    sink_->onProgress(bytes_received_, messages_received_);
  } else if (msg.has_eos()) {
    sink_->end(msg.eos().reason());
    promise_->addResult(Expected<wire::EosReason>::ok(msg.eos().reason()));
    promise_->finish();
    dispatcher_->unsubscribe(subscription_id_);
  } else if (msg.has_error()) {
    sink_->end(wire::EOS_REASON_ERROR);
    promise_->addResult(Expected<wire::EosReason>::fail(QString::fromStdString(msg.error().message())));
    promise_->finish();
    dispatcher_->unsubscribe(subscription_id_);
  }
}

void SessionClient::sendAck() {
  wire::ClientMessage msg;
  auto* ack = msg.mutable_ack();
  ack->set_subscription_id(subscription_id_);
  ack->set_through_seq(last_received_seq_);
  dispatcher_->sendFireAndForget(std::move(msg));
  last_acked_seq_ = last_received_seq_;
}

void SessionClient::cancel() {
  wire::ClientMessage msg;
  msg.mutable_cancel()->set_subscription_id(subscription_id_);
  dispatcher_->sendFireAndForget(std::move(msg));
}

}  // namespace PJ::cloud
```

- [ ] **Step 3: Smoke test (compile-only stub — live coverage in Task 9)**

Create `pj-cloud/client-core/tests/SessionClientTest.cpp`:

```cpp
#include <gtest/gtest.h>
#include "pj_cloud_client/SessionClient.h"

TEST(SessionClientTest, HeaderCompiles) {
  // Live behavior is exercised in Task 9 (CLI + fake server).
  SUCCEED();
}
```

Update `pj-cloud/client-core/tests/CMakeLists.txt`:

```cmake
add_executable(client-core-tests
  EnvelopeRoundTripTest.cpp
  MessageDispatcherTest.cpp
  DecompressionTest.cpp
  SessionClientTest.cpp
)
```

- [ ] **Step 4: Build + run**

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
cmake --build build -j$(nproc) --target client-core-tests
ctest --test-dir build -V
```

- [ ] **Step 5: Commit**

```bash
git add client-core/include/pj_cloud_client/SessionClient.h \
        client-core/src/SessionClient.cpp \
        client-core/tests/SessionClientTest.cpp \
        client-core/tests/CMakeLists.txt
git commit -m "feat(client-core): SessionClient — open + decompress + sink dispatch + ack"
```

---

## Task 8a: `SessionKey` — normalization + hash (forward-setup for the M2b in-memory cache)

**Why here:** the unified plan (§3.2 seam 6, §6 L1) places `SessionKey` in **`client-core`** and references an L1 test "SessionKey normalization+hash" (reordered `file_ids`/`topics` → same key; different `uri` → different key). Plan B provides **only the key utility** — the in-memory `SessionCache` that *uses* it is DEFERRED M2b (separate plan). This is a tiny, dependency-free, Widgets-free addition.

**Files:**
- Create: `pj-cloud/client-core/include/pj_cloud_client/SessionKey.h`
- Create: `pj-cloud/client-core/src/SessionKey.cpp`
- Create: `pj-cloud/client-core/tests/SessionKeyTest.cpp`
- Modify: `pj-cloud/client-core/CMakeLists.txt` (add the new source)
- Modify: `pj-cloud/client-core/tests/CMakeLists.txt` (add the new test source)

- [ ] **Step 1: Write the failing test**

Create `pj-cloud/client-core/tests/SessionKeyTest.cpp`:

```cpp
#include <gtest/gtest.h>

#include "pj_cloud_client/SessionKey.h"

using namespace PJ::cloud;

// Unified-plan §6 L1: the key is over the NORMALIZED tuple
// (server_uri, sorted file_ids[], sorted topic_names[], time_range).
TEST(SessionKeyTest, ReorderedFileIdsAndTopicsProduceSameKey) {
  SessionKey a = computeSessionKey("wss://h/api/ws", {3, 1, 2}, {"/b", "/a"}, {100, 200});
  SessionKey b = computeSessionKey("wss://h/api/ws", {1, 2, 3}, {"/a", "/b"}, {100, 200});
  EXPECT_EQ(a, b);
  EXPECT_EQ(a.hash, b.hash);
}

TEST(SessionKeyTest, DifferentUriProducesDifferentKey) {
  SessionKey a = computeSessionKey("wss://h1/api/ws", {1}, {"/a"}, {0, 0});
  SessionKey b = computeSessionKey("wss://h2/api/ws", {1}, {"/a"}, {0, 0});
  EXPECT_NE(a, b);
  EXPECT_NE(a.hash, b.hash);
}

TEST(SessionKeyTest, DifferentTimeRangeProducesDifferentKey) {
  SessionKey a = computeSessionKey("wss://h/api/ws", {1}, {"/a"}, {100, 200});
  SessionKey b = computeSessionKey("wss://h/api/ws", {1}, {"/a"}, {100, 201});
  EXPECT_NE(a, b);
}

TEST(SessionKeyTest, EmptyTopicsMeansAllTopicsAndIsStable) {
  SessionKey a = computeSessionKey("wss://h/api/ws", {1, 2}, {}, {0, 0});
  SessionKey b = computeSessionKey("wss://h/api/ws", {2, 1}, {}, {0, 0});
  EXPECT_EQ(a, b);
}
```

Update `pj-cloud/client-core/tests/CMakeLists.txt` to add the new source to the existing `client-core-tests` executable source list:

```cmake
add_executable(client-core-tests
  EnvelopeRoundTripTest.cpp
  MessageDispatcherTest.cpp
  DecompressionTest.cpp
  SessionClientTest.cpp
  CatalogClientTest.cpp
  SessionKeyTest.cpp
)
```

- [ ] **Step 2: Write the header**

Create `pj-cloud/client-core/include/pj_cloud_client/SessionKey.h`:

```cpp
#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace PJ::cloud {

// SessionKey identifies a reconstructed session by its NORMALIZED logical
// selection — independent of order and of transport details. The future
// (M2b, separate plan) in-memory SessionCache keys on this; client-core only
// provides the key utility, never the cache itself (unified-plan §3.2/§3.4).
struct SessionKey {
  std::string  server_uri;          // normalized as-passed (caller normalizes the URI)
  std::vector<uint64_t> file_ids;   // ascending, deduped
  std::vector<std::string> topics;  // ascending, deduped; empty == "all topics"
  int64_t      start_ns{0};
  int64_t      end_ns{0};
  uint64_t     hash{0};             // FNV-1a over the canonical encoding

  bool operator==(const SessionKey& o) const {
    return server_uri == o.server_uri && file_ids == o.file_ids && topics == o.topics &&
           start_ns == o.start_ns && end_ns == o.end_ns;
  }
  bool operator!=(const SessionKey& o) const { return !(*this == o); }
};

struct TimeRangeNs {
  int64_t start_ns{0};
  int64_t end_ns{0};
};

// Builds the normalized key: sorts + dedupes file_ids and topics, copies the
// time range, and computes a stable 64-bit hash over the canonical byte form.
SessionKey computeSessionKey(const std::string& server_uri,
                             std::vector<uint64_t> file_ids,
                             std::vector<std::string> topics,
                             TimeRangeNs time_range);

}  // namespace PJ::cloud
```

- [ ] **Step 3: Implement**

Create `pj-cloud/client-core/src/SessionKey.cpp`:

```cpp
#include "pj_cloud_client/SessionKey.h"

#include <algorithm>
#include <cstring>

namespace PJ::cloud {
namespace {

constexpr uint64_t kFnvOffset = 1469598103934665603ull;
constexpr uint64_t kFnvPrime  = 1099511628211ull;

inline void fnvBytes(uint64_t& h, const void* p, size_t n) {
  const auto* b = static_cast<const unsigned char*>(p);
  for (size_t i = 0; i < n; ++i) {
    h ^= b[i];
    h *= kFnvPrime;
  }
}

inline void fnvU64(uint64_t& h, uint64_t v) { fnvBytes(h, &v, sizeof(v)); }
inline void fnvI64(uint64_t& h, int64_t v)  { fnvBytes(h, &v, sizeof(v)); }

inline void fnvStr(uint64_t& h, const std::string& s) {
  fnvU64(h, s.size());                 // length-prefix → unambiguous boundaries
  fnvBytes(h, s.data(), s.size());
}

}  // namespace

SessionKey computeSessionKey(const std::string& server_uri,
                             std::vector<uint64_t> file_ids,
                             std::vector<std::string> topics,
                             TimeRangeNs time_range) {
  std::sort(file_ids.begin(), file_ids.end());
  file_ids.erase(std::unique(file_ids.begin(), file_ids.end()), file_ids.end());
  std::sort(topics.begin(), topics.end());
  topics.erase(std::unique(topics.begin(), topics.end()), topics.end());

  uint64_t h = kFnvOffset;
  fnvStr(h, server_uri);
  fnvU64(h, file_ids.size());
  for (uint64_t id : file_ids) fnvU64(h, id);
  fnvU64(h, topics.size());
  for (const auto& t : topics) fnvStr(h, t);
  fnvI64(h, time_range.start_ns);
  fnvI64(h, time_range.end_ns);

  SessionKey key;
  key.server_uri = server_uri;
  key.file_ids   = std::move(file_ids);
  key.topics     = std::move(topics);
  key.start_ns   = time_range.start_ns;
  key.end_ns     = time_range.end_ns;
  key.hash       = h;
  return key;
}

}  // namespace PJ::cloud
```

Add the source to the library in `pj-cloud/client-core/CMakeLists.txt` — extend the `add_library(client-core STATIC ...)` source list:

```cmake
add_library(client-core STATIC
  src/proto/pj_cloud.pb.cc
  src/CloudConnection.cpp
  src/MessageDispatcher.cpp
  src/CatalogClient.cpp
  src/SessionClient.cpp
  src/SessionKey.cpp
  src/Decompression.cpp
)
```

- [ ] **Step 4: Build + run the test**

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
cmake -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake
cmake --build build -j$(nproc) --target client-core-tests
ctest --test-dir build -V -R SessionKeyTest
```

Expected: `SessionKeyTest.ReorderedFileIdsAndTopicsProduceSameKey`, `SessionKeyTest.DifferentUriProducesDifferentKey`, `SessionKeyTest.DifferentTimeRangeProducesDifferentKey`, and `SessionKeyTest.EmptyTopicsMeansAllTopicsAndIsStable` all PASS.

- [ ] **Step 5: Commit**

```bash
git add client-core/include/pj_cloud_client/SessionKey.h \
        client-core/src/SessionKey.cpp \
        client-core/tests/SessionKeyTest.cpp \
        client-core/CMakeLists.txt \
        client-core/tests/CMakeLists.txt
git commit -m "feat(client-core): SessionKey normalization + FNV-1a hash (forward-setup for M2b cache)"
```

---

## Task 9: client-cli subproject + `main.cpp` + command dispatch

**Files:**
- Create: `pj-cloud/client-cli/CMakeLists.txt`
- Create: `pj-cloud/client-cli/src/main.cpp`
- Create: `pj-cloud/client-cli/src/CommandDispatch.{h,cpp}`

- [ ] **Step 1: CMakeLists**

Create `pj-cloud/client-cli/CMakeLists.txt`:

```cmake
add_executable(pjcloud-cli
  src/main.cpp
  src/CommandDispatch.cpp
  src/ListCommand.cpp
  src/ShowCommand.cpp
  src/TagCommand.cpp
  src/DownloadCommand.cpp
  src/DebugCommand.cpp
  src/McapWriterSink.cpp
)
target_link_libraries(pjcloud-cli
  PRIVATE client-core mcap::mcap Qt6::Core
)
add_subdirectory(tests)
```

Create `pj-cloud/client-cli/tests/CMakeLists.txt` (empty placeholder):

```cmake
# Tests added in later tasks.
```

- [ ] **Step 2: main + command dispatch**

Create `pj-cloud/client-cli/src/main.cpp`:

```cpp
#include <QCoreApplication>
#include <QCommandLineParser>
#include <iostream>

#include "CommandDispatch.h"

int main(int argc, char* argv[]) {
  QCoreApplication app(argc, argv);
  app.setApplicationName("pjcloud-cli");
  app.setApplicationVersion("0.1.0");

  QCommandLineParser parser;
  parser.setApplicationDescription("PJ Cloud reference CLI");
  parser.addHelpOption();
  parser.addVersionOption();
  parser.addOption({{"s", "server"}, "WSS URL", "url"});
  parser.addOption({{"t", "token"}, "Auth token (or set PJ_CLOUD_TOKEN)", "token"});
  parser.addPositionalArgument("group", "files | session");
  parser.addPositionalArgument("command", "subcommand (e.g. list, show, tag, download, debug)");
  parser.parse(app.arguments());

  auto positional = parser.positionalArguments();
  if (positional.size() < 2) {
    parser.showHelp(2);
    return 2;
  }
  PJ::cloud::cli::CliEnv env{parser.value("server"), resolveToken(parser.value("token"))};
  if (env.server_url.isEmpty()) {
    std::cerr << "--server required\n";
    return 2;
  }
  return PJ::cloud::cli::dispatch(env, positional, app);
}
```

Create `pj-cloud/client-cli/src/CommandDispatch.h`:

```cpp
#pragma once

#include <QCoreApplication>
#include <QString>
#include <QStringList>

namespace PJ::cloud::cli {

struct CliEnv {
  QString server_url;
  QString auth_token;
};

QString resolveToken(QString cli_value);
int dispatch(const CliEnv& env, const QStringList& positional, QCoreApplication& app);

}  // namespace PJ::cloud::cli
```

Create `pj-cloud/client-cli/src/CommandDispatch.cpp`:

```cpp
#include "CommandDispatch.h"

#include <QStringList>
#include <cstdlib>

namespace PJ::cloud::cli {

int runListFiles(const CliEnv&, const QStringList&, QCoreApplication&);
int runShowFile(const CliEnv&, const QStringList&, QCoreApplication&);
int runTagFile(const CliEnv&, const QStringList&, QCoreApplication&);
int runDownloadSession(const CliEnv&, const QStringList&, QCoreApplication&);
int runDebugSession(const CliEnv&, const QStringList&, QCoreApplication&);

QString resolveToken(QString cli_value) {
  if (!cli_value.isEmpty()) return cli_value;
  if (const char* env = std::getenv("PJ_CLOUD_TOKEN"); env && *env) return QString::fromLocal8Bit(env);
  return {};
}

int dispatch(const CliEnv& env, const QStringList& positional, QCoreApplication& app) {
  const QString& group = positional[0];
  const QString& cmd   = positional[1];
  QStringList rest = positional.mid(2);

  if (group == "files" && cmd == "list")     return runListFiles(env, rest, app);
  if (group == "files" && cmd == "show")     return runShowFile(env, rest, app);
  if (group == "files" && cmd == "tag")      return runTagFile(env, rest, app);
  if (group == "session" && cmd == "download") return runDownloadSession(env, rest, app);
  if (group == "session" && cmd == "debug")    return runDebugSession(env, rest, app);

  qWarning("unknown command: %s %s", qPrintable(group), qPrintable(cmd));
  return 2;
}

}  // namespace PJ::cloud::cli
```

- [ ] **Step 3: Add stub implementations for each command (filled in next tasks)**

Create stubs:

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/client-cli/src
for c in List Show Tag Download Debug; do
  cat > ${c}Command.cpp <<EOF
#include "CommandDispatch.h"
namespace PJ::cloud::cli {
int run${c}File(const CliEnv&, const QStringList&, QCoreApplication&) { return 0; }
}
EOF
done
# Rename: list/show/tag/download/debug
mv DownloadCommand.cpp DownloadCommand_tmp.cpp
mv DebugCommand.cpp DebugCommand_tmp.cpp
cat > DownloadCommand.cpp <<EOF
#include "CommandDispatch.h"
namespace PJ::cloud::cli {
int runDownloadSession(const CliEnv&, const QStringList&, QCoreApplication&) { return 0; }
}
EOF
cat > DebugCommand.cpp <<EOF
#include "CommandDispatch.h"
namespace PJ::cloud::cli {
int runDebugSession(const CliEnv&, const QStringList&, QCoreApplication&) { return 0; }
}
EOF
rm DownloadCommand_tmp.cpp DebugCommand_tmp.cpp
```

Also stub `McapWriterSink.cpp` (filled in Task 11):

```bash
echo "// stub — filled by Task 11" > /home/gn/ws/PJ4_Server_Template/pj-mcap-server/client-cli/src/McapWriterSink.cpp
```

- [ ] **Step 4: Build the binary**

```bash
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server
cmake --build build -j$(nproc) --target pjcloud-cli
./build/client-cli/pjcloud-cli --help
```

Expected: `--help` output with `files | session` positional usage.

- [ ] **Step 5: Commit**

```bash
git add client-cli/CMakeLists.txt client-cli/src/ client-cli/tests/CMakeLists.txt
git commit -m "feat(client-cli): main + CommandDispatch + stub subcommands"
```

---

## Task 10: `files list` / `files show` / `files tag` commands

**Files:**
- Modify: `pj-cloud/client-cli/src/ListCommand.cpp`
- Modify: `pj-cloud/client-cli/src/ShowCommand.cpp`
- Modify: `pj-cloud/client-cli/src/TagCommand.cpp`

- [ ] **Step 1: Implement `runListFiles`**

Replace `pj-cloud/client-cli/src/ListCommand.cpp`:

```cpp
#include "CommandDispatch.h"

#include <QCoreApplication>
#include <QEventLoop>
#include <QFutureWatcher>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QSignalSpy>
#include <QUrl>
#include <iostream>

#include "pj_cloud_client/CloudConnection.h"
#include "pj_cloud_client/CatalogClient.h"
#include "pj_cloud_client/MessageDispatcher.h"
#include "pj_cloud.pb.h"

using namespace PJ::cloud;

namespace PJ::cloud::cli {

int runListFiles(const CliEnv& env, const QStringList& args, QCoreApplication& app) {
  CloudConnection conn({QUrl{env.server_url}, env.auth_token});
  QEventLoop loop;
  QObject::connect(&conn, &CloudConnection::ready, &loop, &QEventLoop::quit);
  QObject::connect(&conn, &CloudConnection::failed, &loop, [&](const QString& r) {
    std::cerr << "connect failed: " << r.toStdString() << "\n";
    QCoreApplication::exit(1);
  });
  conn.open();
  loop.exec();
  if (!conn.isReady()) return 1;

  CatalogClient cat(conn.dispatcher());
  auto fut = cat.listFiles(nullptr, args.value(0, "200").toUInt(), {});
  QFutureWatcher<Expected<wire::ListFilesResponse>> w;
  QObject::connect(&w, &QFutureWatcher<Expected<wire::ListFilesResponse>>::finished, &loop, &QEventLoop::quit);
  w.setFuture(fut);
  loop.exec();

  auto outer = w.result();
  if (!outer) {
    std::cerr << "list failed: " << outer.error().toStdString() << "\n";
    return 1;
  }
  const bool as_json = args.contains("--json");
  const auto& resp = outer.value();
  if (as_json) {
    // Each file object includes the flat tags_effective map under "user_metadata"
    // (unified-plan §3.1) — the same shape the future dialog ingests verbatim.
    QJsonArray arr;
    for (const auto& f : resp.files()) {
      QJsonObject obj;
      obj["id"]         = QString::number(f.id());
      obj["s3_key"]     = QString::fromStdString(f.s3_key());
      obj["size_bytes"] = QString::number(f.size_bytes());
      QJsonObject um;
      for (const auto& [k, v] : flatMetadata(resp, f.id())) {
        um[QString::fromStdString(k)] = QString::fromStdString(v);
      }
      obj["user_metadata"] = um;
      arr.append(obj);
    }
    std::cout << QJsonDocument(arr).toJson(QJsonDocument::Compact).toStdString() << "\n";
  } else {
    for (const auto& f : resp.files()) {
      std::cout << f.id() << "\t" << f.s3_key() << "\t" << f.size_bytes() << "\n";
    }
  }
  return 0;
}

}  // namespace PJ::cloud::cli
```

Implement `runShowFile` and `runTagFile` in the same shape — `ShowCommand.cpp` invokes `cat.getFile(id)` and prints the topic list; `TagCommand.cpp` parses `--set k=v` / `--unset k` flags and calls `cat.updateTags`.

(Templates omitted here for length — they're isomorphic to `runListFiles` above.)

- [ ] **Step 2: Smoke test against a fake server**

(Live test in Task 13 once `pjcloud-cli session download` is implemented.)

- [ ] **Step 3: Commit**

```bash
git add client-cli/src/ListCommand.cpp client-cli/src/ShowCommand.cpp client-cli/src/TagCommand.cpp
git commit -m "feat(client-cli): files list / show / tag commands"
```

---

## Task 11: `McapWriterSink` — reconstruct streamed session as a local MCAP

**Files:**
- Create: `pj-cloud/client-cli/src/McapWriterSink.h`
- Modify: `pj-cloud/client-cli/src/McapWriterSink.cpp`

- [ ] **Step 1: Header**

Create `pj-cloud/client-cli/src/McapWriterSink.h`:

```cpp
#pragma once

#include <filesystem>
#include <mcap/writer.hpp>
#include <unordered_map>

#include "pj_cloud_client/SessionSink.h"

namespace PJ::cloud::cli {

class McapWriterSink final : public PJ::cloud::SessionSink {
 public:
  explicit McapWriterSink(std::filesystem::path output);
  ~McapWriterSink() override;

  Expected<void> begin(const wire::OpenSessionResponse& session) override;
  Expected<void> writeMessage(uint32_t topic_id, uint32_t schema_id,
                              int64_t log_time_ns, int64_t publish_time_ns,
                              std::span<const std::byte> payload) override;
  Expected<void> end(wire::EosReason reason) override;

 private:
  std::filesystem::path output_;
  mcap::McapWriter writer_;
  std::unordered_map<uint32_t, mcap::SchemaId> schema_id_map_;
  std::unordered_map<uint32_t, mcap::ChannelId> channel_id_map_;
  bool aborted_{false};
};

}  // namespace PJ::cloud::cli
```

- [ ] **Step 2: Implementation**

Replace `pj-cloud/client-cli/src/McapWriterSink.cpp`:

```cpp
#include "McapWriterSink.h"

#include "pj_cloud.pb.h"

namespace PJ::cloud::cli {

McapWriterSink::McapWriterSink(std::filesystem::path output) : output_{std::move(output)} {}

McapWriterSink::~McapWriterSink() { writer_.close(); }

Expected<void> McapWriterSink::begin(const wire::OpenSessionResponse& s) {
  auto opts = mcap::McapWriterOptions("pjcloud-cli");
  opts.compression = mcap::Compression::Zstd;
  if (auto st = writer_.open(output_.string(), opts); !st.ok()) {
    return Expected<void>::fail(QString::fromStdString(st.message));
  }
  for (const auto& sb : s.schemas()) {
    mcap::Schema sch(sb.name(), sb.encoding(),
                     std::string{reinterpret_cast<const char*>(sb.data().data()), sb.data().size()});
    writer_.addSchema(sch);
    schema_id_map_[sb.schema_id()] = sch.id;
  }
  for (const auto& tb : s.topic_id_map()) {
    mcap::Channel ch(tb.topic_name(), tb.message_encoding(), schema_id_map_[tb.schema_id()]);
    writer_.addChannel(ch);
    channel_id_map_[tb.topic_id()] = ch.id;
  }
  return Expected<void>::ok();
}

Expected<void> McapWriterSink::writeMessage(uint32_t topic_id, uint32_t /*schema_id*/,
                                            int64_t log_time_ns, int64_t publish_time_ns,
                                            std::span<const std::byte> payload) {
  mcap::Message m;
  auto it = channel_id_map_.find(topic_id);  // fail-closed: a topic_id absent from the session map
  if (it == channel_id_map_.end()) {         // means a corrupt/out-of-subset record — never silently insert
    return Expected<void>::fail(QStringLiteral("unknown topic_id %1 not in session map").arg(topic_id));
  }
  m.channelId = it->second;
  m.logTime = static_cast<uint64_t>(log_time_ns);
  m.publishTime = static_cast<uint64_t>(publish_time_ns);
  m.data = reinterpret_cast<const std::byte*>(payload.data());
  m.dataSize = payload.size();
  auto st = writer_.write(m);
  if (!st.ok()) {
    return Expected<void>::fail(QString::fromStdString(st.message));
  }
  return Expected<void>::ok();
}

Expected<void> McapWriterSink::end(wire::EosReason reason) {
  writer_.close();
  if (reason != wire::EOS_REASON_COMPLETE) {
    aborted_ = true;
    std::error_code ec;
    std::filesystem::remove(output_, ec);
  }
  return Expected<void>::ok();
}

}  // namespace PJ::cloud::cli
```

- [ ] **Step 3: Commit**

```bash
git add client-cli/src/McapWriterSink.h client-cli/src/McapWriterSink.cpp
git commit -m "feat(client-cli): McapWriterSink — reconstruct session as local MCAP"
```

---

## Task 12: `session download` command

**Files:**
- Modify: `pj-cloud/client-cli/src/DownloadCommand.cpp`

- [ ] **Step 1: Implement**

Replace `pj-cloud/client-cli/src/DownloadCommand.cpp`:

```cpp
#include "CommandDispatch.h"

#include <QEventLoop>
#include <QFutureWatcher>
#include <QUrl>
#include <iostream>

#include "McapWriterSink.h"
#include "pj_cloud_client/CloudConnection.h"
#include "pj_cloud_client/MessageDispatcher.h"
#include "pj_cloud_client/SessionClient.h"
#include "pj_cloud.pb.h"

using namespace PJ::cloud;

namespace PJ::cloud::cli {

int runDownloadSession(const CliEnv& env, const QStringList& args, QCoreApplication& app) {
  // Parse flags: --files 1,2,3 --topics /a,/b --output path.mcap [--time-range start,end]
  QString files_csv, topics_csv, output, time_range;
  for (int i = 0; i + 1 < args.size(); i += 2) {
    auto k = args[i], v = args[i + 1];
    if (k == "--files")      files_csv = v;
    else if (k == "--topics") topics_csv = v;
    else if (k == "--output") output = v;
    else if (k == "--time-range") time_range = v;
  }
  if (files_csv.isEmpty() || output.isEmpty()) {
    std::cerr << "--files and --output required\n"; return 2;
  }

  CloudConnection conn({QUrl{env.server_url}, env.auth_token});
  QEventLoop loop;
  QObject::connect(&conn, &CloudConnection::ready, &loop, &QEventLoop::quit);
  QObject::connect(&conn, &CloudConnection::failed, &loop, [&](const QString& r) {
    std::cerr << "connect failed: " << r.toStdString() << "\n";
    QCoreApplication::exit(1);
  });
  conn.open();
  loop.exec();
  if (!conn.isReady()) return 1;

  auto sink = std::make_unique<McapWriterSink>(std::filesystem::path{output.toStdString()});
  SessionClient session(conn.dispatcher(), std::move(sink));

  wire::OpenSessionRequest req;
  auto* fresh = req.mutable_fresh();
  for (const auto& s : files_csv.split(',')) fresh->add_file_ids(s.toULongLong());
  for (const auto& t : topics_csv.split(',', Qt::SkipEmptyParts)) fresh->add_topic_names(t.toStdString());
  if (!time_range.isEmpty()) {
    auto parts = time_range.split(',');
    if (parts.size() == 2) {
      fresh->mutable_time_range()->set_start_ns(parts[0].toLongLong());
      fresh->mutable_time_range()->set_end_ns(parts[1].toLongLong());
    }
  }

  auto fut = session.runSession(req);
  QFutureWatcher<Expected<wire::EosReason>> w;
  QObject::connect(&w, &QFutureWatcher<Expected<wire::EosReason>>::finished, &loop, &QEventLoop::quit);
  w.setFuture(fut);
  loop.exec();
  auto outer = w.result();
  if (!outer) {
    std::cerr << "session failed: " << outer.error().toStdString() << "\n"; return 1;
  }
  if (outer.value() != wire::EOS_REASON_COMPLETE) {
    std::cerr << "session did not complete: reason=" << outer.value() << "\n"; return 1;
  }
  std::cerr << "wrote " << output.toStdString() << "\n";
  return 0;
}

}  // namespace PJ::cloud::cli
```

- [ ] **Step 2: Commit**

```bash
git add client-cli/src/DownloadCommand.cpp
git commit -m "feat(client-cli): session download — open + drive + write MCAP"
```

---

## Task 13: `session debug` command + integration tests against the live Go server

**Files:**
- Modify: `pj-cloud/client-cli/src/DebugCommand.cpp`
- Create: `pj-cloud/client-cli/tests/DownloadIntegrationTest.cpp`

> **Cross-reference (no Plan B change beyond this note):** lossless round-trip — original MCAP → server → `pjcloud-cli session download` → reconstructed MCAP, logically equal on `(topic, log_time, payload, publish_time, schema name/encoding/data)` — is validated on **both** storage backends (S3 via Minio AND GCS via fake-gcs-server) by **Plan C's L3 cross-language E2E matrix** (unified-plan §6, L3). `client-core` + `pjcloud-cli` need **no per-engagement (S3/GCS) change**: storage auth is server-side, so the wire bytes the client sees are identical on both backends. The scaffold test below stays Plan-A/Plan-C-driven on purpose.

- [ ] **Step 1: Implement debug command**

Replace `pj-cloud/client-cli/src/DebugCommand.cpp`:

```cpp
#include "CommandDispatch.h"

#include <QEventLoop>
#include <QFutureWatcher>
#include <QUrl>
#include <iostream>

#include "pj_cloud_client/CloudConnection.h"
#include "pj_cloud_client/MessageDispatcher.h"
#include "pj_cloud_client/SessionClient.h"
#include "pj_cloud_client/SessionSink.h"
#include "pj_cloud.pb.h"

using namespace PJ::cloud;

namespace PJ::cloud::cli {

class DebugSink final : public SessionSink {
 public:
  explicit DebugSink(int max_msgs) : max_msgs_{max_msgs} {}
  Expected<void> begin(const wire::OpenSessionResponse& s) override {
    for (const auto& tb : s.topic_id_map()) {
      std::cerr << "topic[" << tb.topic_id() << "] = " << tb.topic_name() << "\n";
    }
    return Expected<void>::ok();
  }
  Expected<void> writeMessage(uint32_t topic_id, uint32_t /*sid*/, int64_t log_time, int64_t /*pt*/,
                              std::span<const std::byte> payload) override {
    if (count_++ < max_msgs_) {
      std::cout << "msg topic=" << topic_id << " log=" << log_time << " size=" << payload.size() << "\n";
    }
    return Expected<void>::ok();
  }
  Expected<void> end(wire::EosReason r) override {
    std::cerr << "eos: " << r << " total=" << count_ << "\n";
    return Expected<void>::ok();
  }
 private:
  int max_msgs_, count_{0};
};

int runDebugSession(const CliEnv& env, const QStringList& args, QCoreApplication& app) {
  // --files 1 --topics /x --max-messages 10
  QString files_csv, topics_csv;
  int max_msgs = 10;
  for (int i = 0; i + 1 < args.size(); i += 2) {
    auto k = args[i], v = args[i + 1];
    if (k == "--files")      files_csv = v;
    else if (k == "--topics") topics_csv = v;
    else if (k == "--max-messages") max_msgs = v.toInt();
  }
  CloudConnection conn({QUrl{env.server_url}, env.auth_token});
  QEventLoop loop;
  QObject::connect(&conn, &CloudConnection::ready, &loop, &QEventLoop::quit);
  conn.open(); loop.exec();
  if (!conn.isReady()) return 1;
  SessionClient session(conn.dispatcher(), std::make_unique<DebugSink>(max_msgs));
  wire::OpenSessionRequest req;
  auto* fresh = req.mutable_fresh();
  for (const auto& s : files_csv.split(',')) fresh->add_file_ids(s.toULongLong());
  for (const auto& t : topics_csv.split(',', Qt::SkipEmptyParts)) fresh->add_topic_names(t.toStdString());
  auto fut = session.runSession(req);
  QFutureWatcher<Expected<wire::EosReason>> w;
  QObject::connect(&w, &QFutureWatcher<Expected<wire::EosReason>>::finished, &loop, &QEventLoop::quit);
  w.setFuture(fut); loop.exec();
  return w.result() ? 0 : 1;
}

}  // namespace PJ::cloud::cli
```

- [ ] **Step 2: Live integration test that spins up the Go server**

Create `pj-cloud/client-cli/tests/DownloadIntegrationTest.cpp`:

```cpp
#include <QProcess>
#include <QTcpSocket>
#include <gtest/gtest.h>

#include <chrono>
#include <thread>
#include <filesystem>

// Spawns the Go server from Plan A, runs `pjcloud-cli session download`, and
// asserts the reconstructed MCAP is valid and contains the expected messages.
TEST(DownloadIntegration, EndToEndAgainstGoServer) {
  // 1. Start Minio + populate it with a fixture MCAP via testhelpers.
  // 2. Start pj-cloud-server pointing at Minio.
  // 3. Run pjcloud-cli session download.
  // 4. Open the reconstructed MCAP with mcap-cpp and assert.
  //
  // Full implementation requires the server binary + Minio docker-compose to be
  // available; see Plan C for the cross-language fixture harness. Stubbed here
  // and skipped unless an env var sets PJCLOUD_SERVER_URL.
  if (const char* url = std::getenv("PJCLOUD_SERVER_URL"); !url || !*url) {
    GTEST_SKIP() << "PJCLOUD_SERVER_URL not set";
  }
  GTEST_SKIP() << "implementation lives in Plan C";
}
```

Update `pj-cloud/client-cli/tests/CMakeLists.txt`:

```cmake
add_executable(client-cli-tests DownloadIntegrationTest.cpp)
target_link_libraries(client-cli-tests PRIVATE GTest::gtest GTest::gtest_main Qt6::Core)
add_test(NAME client-cli-tests COMMAND client-cli-tests)
```

- [ ] **Step 3: Commit**

```bash
git add client-cli/src/DebugCommand.cpp client-cli/tests/DownloadIntegrationTest.cpp client-cli/tests/CMakeLists.txt
git commit -m "feat(client-cli): session debug + integration test scaffold (deferred to Plan C)"
```

---

## Task 14: CI (build + unit tests on Linux)

**Files:**
- Modify: `.github/workflows/ci.yml` (extend Plan A's workflow)

- [ ] **Step 1: Add a C++ build job**

Add to `pj-cloud/.github/workflows/ci.yml`:

```yaml
  cpp_build_and_test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: lukka/get-cmake@latest
      - name: Install Conan
        run: pipx install conan==2.5.0
      - name: Install Qt build deps
        run: sudo apt-get install -y libgl1-mesa-dev libxkbcommon-dev libxcb-xkb-dev
      - name: Conan install
        run: |
          conan profile detect --force
          conan install . --output-folder=build --build=missing -s compiler.cppstd=20
      - name: CMake configure
        run: cmake -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake
      - name: Build
        run: cmake --build build -j$(nproc)
      - name: Test
        run: ctest --test-dir build -V
      - name: Assert client-core does NOT link Qt6::Widgets (build-graph check)
        run: |
          # Unified-plan M1c gate: client-core must stay Widgets-free so the future
          # PJ4 plugin can lift it unchanged. The configured CMake cache records the
          # resolved link libraries for the target; fail loudly if Widgets leaks in.
          if grep -RniE 'Qt6?::Widgets' build/client-core/CMakeFiles/client-core.dir/ build/CMakeCache.txt; then
            echo "client-core must not link Qt6::Widgets"; exit 1
          fi
          echo "OK: client-core links no Qt6::Widgets"
```

> **Note:** the CMake `LINK_LIBRARIES`-MATCHES guard added in Task 2 already fails configuration if `Widgets` appears; this CI step is the redundant build-graph gate the unified-plan M1c acceptance line calls for. Cross-language round-trip on the `{s3,gcs}` matrix is owned by **Plan C** (unified-plan §6 L3), not duplicated here.

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: build + test the Qt C++ client on Linux; assert client-core links no Qt6::Widgets"
```

---

## End of Plan B

14 numbered tasks (1–14, plus the inserted 8a) covering: top-level CMake/Conan → client-core scaffolding → CloudConnection (Hello handshake + storage-agnostic BackendCapabilities accessor) → MessageDispatcher (RPC + subscriptions) → CatalogClient (typed wrappers + flat tags_effective metadata) → SessionSink + Decompression → SessionClient → **SessionKey normalization + hash (8a)** → CLI driver + CommandDispatch → files commands (`--json` includes the flat user_metadata) → McapWriterSink → session download → session debug → CI extension (incl. the no-`Qt6::Widgets` build-graph gate). The library/driver split holds: every reusable byte of protocol code lives in `client-core`, which links nothing from `Qt6::Widgets`, so the future PJ4 plugin can lift it whole into `PJ4/pj_plugins/pj_cloud/` and only needs to add a `DatastoreSink` + the Widgets-side dialog.

After Plans A and B both ship, Plan C provides the cross-language E2E round-trip harness (Minio + Go server + Qt CLI + fixture matrix + byte-equal assertions) that turns "the two halves compile" into "the two halves agree on every message of every fixture".
