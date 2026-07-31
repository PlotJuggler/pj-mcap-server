// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// DeferredPromotionHost — a pj.source_promotion.v1 fake whose result callback
// is genuinely deferred: promote returns true (ACCEPTED) immediately and a
// worker thread delivers on_result only after release(). Hoisted VERBATIM
// (pure move) from fetch_worker_promotion_test.cpp so the provider suite can
// pin the outstanding-promotion-at-cancel DETACH branch with the same fake.
// (The immediate/re-entrant FakePromotionHost fakes stay per-suite — their
// capture shapes differ.)
#pragma once

#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>

#include <pj_base/descriptor_import_protocol.h>

namespace mcap_cloud_test {

struct DeferredPromotionHost {
  std::mutex mu;
  std::condition_variable cv;
  bool release_requested = false;
  PJ_source_promotion_result_fn stored_cb = nullptr;
  void* stored_ctx = nullptr;
  std::thread worker;

  DeferredPromotionHost() = default;
  DeferredPromotionHost(const DeferredPromotionHost&) = delete;
  DeferredPromotionHost& operator=(const DeferredPromotionHost&) = delete;

  ~DeferredPromotionHost() {
    release();
    joinWorker();
  }

  static bool promoteThunk(void* ctx, const PJ_source_promotion_request_v1_t* /*request*/,
                           PJ_source_promotion_result_fn result_cb, void* callback_ctx,
                           PJ_error_t* /*err*/) noexcept {
    auto* self = static_cast<DeferredPromotionHost*>(ctx);
    self->stored_cb = result_cb;
    self->stored_ctx = callback_ctx;
    self->worker = std::thread([self]() {
      {
        std::unique_lock<std::mutex> lock(self->mu);
        self->cv.wait(lock, [self] { return self->release_requested; });
      }
      const char* msg = "promoted late";
      self->stored_cb(self->stored_ctx, true, PJ_string_view_t{msg, std::char_traits<char>::length(msg)});
    });
    return true;
  }

  void release() {
    {
      const std::lock_guard<std::mutex> lock(mu);
      release_requested = true;
    }
    cv.notify_one();
  }
  void joinWorker() {
    if (worker.joinable()) {
      worker.join();
    }
  }

  [[nodiscard]] PJ_source_promotion_host_t view() {
    static constexpr PJ_source_promotion_host_vtable_t kVtable{
        1, sizeof(PJ_source_promotion_host_vtable_t), &DeferredPromotionHost::promoteThunk};
    return PJ_source_promotion_host_t{this, &kVtable};
  }
};

}  // namespace mcap_cloud_test
