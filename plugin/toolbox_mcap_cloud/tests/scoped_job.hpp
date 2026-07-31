// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// ScopedJob — RAII over the ABI fat-pointer PJ_joinable_job_t: destroy
// (cancel+join+free) on scope exit. Hoisted VERBATIM from
// descriptor_import_provider_test.cpp (PR-3 quality review IMPORTANT-4) so
// the live suite shares it: a failing ASSERT mid-test must destroy the job
// (quiescing its worker thread) BEFORE the provider/runtime/hosts it uses
// unwind — a raw PJ_joinable_job_t leaks the worker under those destructors.
// Declare a ScopedJob AFTER the provider/runtime/host fixtures so it is
// destroyed FIRST on unwind.
#pragma once

#include <pj_base/descriptor_import_protocol.h>

namespace mcap_cloud_test {

struct ScopedJob {
  PJ_joinable_job_t job{};
  ScopedJob() = default;
  ScopedJob(const ScopedJob&) = delete;
  ScopedJob& operator=(const ScopedJob&) = delete;
  ~ScopedJob() {
    if (job.vtable != nullptr && job.vtable->destroy != nullptr) {
      job.vtable->destroy(job.ctx);
    }
  }
};

}  // namespace mcap_cloud_test
