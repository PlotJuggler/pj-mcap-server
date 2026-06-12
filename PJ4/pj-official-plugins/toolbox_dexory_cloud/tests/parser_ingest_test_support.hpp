// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Fake toolbox-runtime + data-source-runtime hosts for ParserIngestDriver
// tests: records every ensureParserBinding request and every pushed message
// (after invoking the fetcher), implements the release contract. Used by
// the hermetic driver test (and designed for reuse by the upcoming live
// worker test).
#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include <pj_base/data_source_protocol.h>
#include <pj_base/toolbox_protocol.h>

namespace pj_ingest_test {

struct RecordedBinding {
  std::string topic_name;
  std::string parser_encoding;
  std::string type_name;
  std::string schema;
  std::string config;
  uint32_t handle = 0;
};

struct RecordedPush {
  uint32_t handle = 0;
  int64_t ts = 0;
  std::vector<uint8_t> bytes;
};

struct FakeIngestHost {
  std::vector<RecordedBinding> bindings;
  std::vector<RecordedPush> pushes;
  // Monotonic count of RECORDED pushes (== pushes.size()), updated atomically.
  // The recorder vectors are owned by the pushing thread; a live test's
  // background canceller thread may read ONLY this counter (thread-safe
  // observation of "have pushes started?") — never the vectors themselves.
  std::atomic<uint64_t> push_events{0};
  std::vector<uint32_t> created;   // data_source_ids passed to create
  std::vector<uint32_t> released;  // data_source_ids passed to release
  bool refuse_create = false;      // simulate an older/unconfigured host
  // Bind requests whose type_name matches this string are refused (per-topic
  // "no parser" simulation).
  std::string refuse_type;
  // When set, pushMessage properly releases all resources then returns false
  // without recording the push (simulates a host-side push rejection).
  bool refuse_push = false;

  PJ_data_source_runtime_host_vtable_t ds_vtable{};
  PJ_toolbox_runtime_host_vtable_t tb_vtable{};

  FakeIngestHost() {
    // The REAL host hands out 1 (DataSourceRuntimeHost.cpp:197 —
    // protocol_version comment: "= 1 for the v4-era runtime host"), not
    // PJ_DATA_SOURCE_PROTOCOL_VERSION (which is the plugin-side major = 4).
    ds_vtable.protocol_version = 1;
    ds_vtable.struct_size = sizeof(PJ_data_source_runtime_host_vtable_t);
    ds_vtable.ensure_parser_binding = &FakeIngestHost::ensureBinding;
    ds_vtable.push_message = &FakeIngestHost::pushMessage;
    tb_vtable.protocol_version = PJ_TOOLBOX_PLUGIN_PROTOCOL_VERSION;
    tb_vtable.struct_size = sizeof(PJ_toolbox_runtime_host_vtable_t);
    tb_vtable.create_parser_ingest = &FakeIngestHost::createIngest;
    tb_vtable.release_parser_ingest = &FakeIngestHost::releaseIngest;
  }

  FakeIngestHost(const FakeIngestHost&) = delete;
  FakeIngestHost& operator=(const FakeIngestHost&) = delete;

  [[nodiscard]] PJ_toolbox_runtime_host_t toolboxRuntime() {
    return PJ_toolbox_runtime_host_t{this, &tb_vtable};
  }

  static void fill(PJ_error_t* err, const char* msg) {
    if (err != nullptr) {
      *err = PJ_error_t{};
      err->code = 1;
      std::snprintf(err->domain, sizeof(err->domain), "%s", "fake_host");
      std::snprintf(err->message, sizeof(err->message), "%s", msg);
    }
  }
  static std::string sv(PJ_string_view_t s) {
    return s.data != nullptr ? std::string(s.data, s.size) : std::string();
  }

  static bool createIngest(
      void* ctx, uint32_t id, PJ_data_source_runtime_host_t* out, PJ_error_t* err) noexcept {
    auto* self = static_cast<FakeIngestHost*>(ctx);
    if (self->refuse_create) {
      fill(err, "parser ingest is not configured on this host");
      return false;
    }
    self->created.push_back(id);
    *out = PJ_data_source_runtime_host_t{self, &self->ds_vtable};
    return true;
  }
  static bool releaseIngest(void* ctx, uint32_t id, PJ_error_t* /*err*/) noexcept {
    static_cast<FakeIngestHost*>(ctx)->released.push_back(id);
    return true;
  }
  static bool ensureBinding(
      void* ctx, const PJ_parser_binding_request_t* req, PJ_parser_binding_handle_t* out,
      PJ_error_t* err) noexcept {
    auto* self = static_cast<FakeIngestHost*>(ctx);
    RecordedBinding b;
    b.topic_name = sv(req->topic_name);
    b.parser_encoding = sv(req->parser_encoding);
    b.type_name = sv(req->type_name);
    if (req->schema.data != nullptr) {
      b.schema.assign(reinterpret_cast<const char*>(req->schema.data), req->schema.size);
    }
    b.config = sv(req->parser_config_json);
    if (!self->refuse_type.empty() && b.type_name == self->refuse_type) {
      fill(err, "no parser found for type");
      return false;
    }
    b.handle = static_cast<uint32_t>(self->bindings.size()) + 1;
    out->id = b.handle;
    self->bindings.push_back(std::move(b));
    return true;
  }
  static bool pushMessage(
      void* ctx, PJ_parser_binding_handle_t handle, int64_t ts, PJ_message_data_fetcher_t fetch,
      PJ_error_t* err) noexcept {
    auto* self = static_cast<FakeIngestHost*>(ctx);

    // A2: validate handle — must refer to a known binding.
    if (handle.id < 1 || static_cast<std::size_t>(handle.id) > self->bindings.size()) {
      fill(err, "invalid parser binding handle");
      // Still must release the fetcher per the ABI contract (host always releases).
      if (fetch.release != nullptr) {
        fetch.release(fetch.ctx);
      }
      return false;
    }

    // A1: Exercise the anchor-lifetime contract.
    //
    // (i) Invoke the fetcher TWICE and verify both invocations return identical
    //     bytes. A compliant plugin closure is idempotent and thread-safe; a
    //     missing or one-shot fetcher will produce a mismatch (or a crash under
    //     ASAN when the anchor-release step below destroys the backing buffer
    //     before the copy).
    PJ_payload_t payload1{};
    PJ_payload_t payload2{};
    const bool ok1 =
        fetch.fetchMessageData != nullptr && fetch.fetchMessageData(fetch.ctx, &payload1, err);
    const bool ok2 = ok1 && fetch.fetchMessageData(fetch.ctx, &payload2, nullptr);
    if (ok1 && ok2) {
      // Check idempotency: same size and same bytes.
      if (payload1.size != payload2.size ||
          (payload1.size > 0 &&
           std::memcmp(payload1.data, payload2.data, static_cast<std::size_t>(payload1.size)) !=
               0)) {
        fill(err, "fetcher not idempotent");
        // Release everything before returning — the host always releases.
        if (payload1.anchor.release != nullptr) {
          payload1.anchor.release(payload1.anchor.ctx);
        }
        if (payload2.anchor.release != nullptr) {
          payload2.anchor.release(payload2.anchor.ctx);
        }
        if (fetch.release != nullptr) {
          fetch.release(fetch.ctx);
        }
        return false;
      }
    }

    // A3: refuse_push knob — release everything properly, then fail.
    if (self->refuse_push) {
      fill(err, "push refused");
      if (payload1.anchor.release != nullptr) {
        payload1.anchor.release(payload1.anchor.ctx);
      }
      if (payload2.anchor.release != nullptr) {
        payload2.anchor.release(payload2.anchor.ctx);
      }
      if (fetch.release != nullptr) {
        fetch.release(fetch.ctx);
      }
      return false;
    }

    if (ok1) {
      // (ii) Release the FETCHER closure BEFORE copying out of payload1.data.
      //      This destroys the closure and its captured shared_ptr, so the
      //      bytes referenced by payload1.data are now kept alive SOLELY by
      //      payload1.anchor. A missing anchor becomes a hard ASAN
      //      use-after-free here — that is the deliberate testing intention.
      if (fetch.release != nullptr) {
        fetch.release(fetch.ctx);
      }

      // (iii) Copy bytes out of payload1 (anchor is still live — safe).
      RecordedPush p;
      p.handle = handle.id;
      p.ts = ts;
      if (payload1.data != nullptr && payload1.size > 0) {
        p.bytes.assign(payload1.data, payload1.data + static_cast<std::size_t>(payload1.size));
      }
      self->pushes.push_back(std::move(p));
      self->push_events.fetch_add(1, std::memory_order_relaxed);

      // (iv) Release the anchor(s) for both invocations.
      if (payload1.anchor.release != nullptr) {
        payload1.anchor.release(payload1.anchor.ctx);
      }
      if (payload2.anchor.release != nullptr) {
        payload2.anchor.release(payload2.anchor.ctx);
      }
    } else {
      // Fetch failed: still must release fetcher and any anchors.
      if (fetch.release != nullptr) {
        fetch.release(fetch.ctx);
      }
      if (payload1.anchor.release != nullptr) {
        payload1.anchor.release(payload1.anchor.ctx);
      }
      if (payload2.anchor.release != nullptr) {
        payload2.anchor.release(payload2.anchor.ctx);
      }
    }
    return ok1;
  }
};

// The recorded binding for `topic`, or nullptr when the topic never bound.
inline const RecordedBinding* findBinding(const FakeIngestHost& fake, const std::string& topic) {
  for (const auto& b : fake.bindings) {
    if (b.topic_name == topic) {
      return &b;
    }
  }
  return nullptr;
}

// Number of recorded pushes addressed to `topic`'s binding handle (0 when the
// topic never bound). The per-handle ground-truth count under host delegation.
inline uint64_t pushesForTopic(const FakeIngestHost& fake, const std::string& topic) {
  const RecordedBinding* b = findBinding(fake, topic);
  if (b == nullptr) {
    return 0;
  }
  uint64_t n = 0;
  for (const auto& p : fake.pushes) {
    if (p.handle == b->handle) {
      ++n;
    }
  }
  return n;
}

}  // namespace pj_ingest_test
