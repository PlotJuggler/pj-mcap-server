// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// See descriptor_import_provider.hpp for the contract. The job runner's
// terminal mapping — including the LOCKED v1 lock-contention/race decision —
// is documented inline at runToTerminal().
#include "descriptor_import_provider.hpp"

#include <cstddef>
#include <filesystem>
#include <optional>
#include <semaphore>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>

#include "credential_resolve.hpp"
#include "credential_store.hpp"
#include "fetch_worker.hpp"
#include "source_descriptor.hpp"

namespace mcap_cloud {

namespace {

std::string_view toView(PJ_string_view_t sv) {
  return std::string_view(sv.data == nullptr ? "" : sv.data, sv.data == nullptr ? 0 : sv.size);
}

}  // namespace

// ---------------------------------------------------------------------------
// JobState — one import job: worker thread + FetchWorker + the ABI callbacks
// ---------------------------------------------------------------------------

struct DescriptorImportProvider::JobState {
  explicit JobState(ImportRuntime& rt) : runtime(rt) {}

  // ---- immutable inputs (written by startImport BEFORE the worker starts) --
  ImportRuntime& runtime;
  HostBindings bindings;
  ConnectionSnapshot connection;  // resolved on the main thread (PR-2 contract)
  SourceDescriptor descriptor;
  std::string canonical_json;
  std::string display_json;
  std::string identity;
  std::uint64_t max_transfer_bytes = 0;
  std::chrono::milliseconds lock_poll{50};
  std::chrono::milliseconds lock_timeout{60000};
  std::function<void()> pre_terminal_hook;  // test seam

  // Copied ABI callback pointers (the ABI: copied before start returns).
  void (*on_dataset)(void*, PJ_data_source_handle_t) noexcept = nullptr;
  void (*on_terminal)(void*, PJ_descriptor_import_outcome_t, PJ_string_view_t) noexcept = nullptr;
  void* callback_ctx = nullptr;

  // ---- lifecycle -----------------------------------------------------------
  FetchWorker fetch;  // per-job (D4): owns the whole cancel wake machinery
  std::binary_semaphore start_gate{0};
  std::atomic<bool> start_released{false};
  std::atomic<bool> cancelled{false};
  std::atomic<int> terminal_count{0};
  std::thread worker;

  // At-most-once gate release (std::binary_semaphore::release on an
  // already-released semaphore is UB) — callable from startImport (the
  // normal path) and defensively from destroy.
  void releaseStartOnce() {
    bool expected = false;
    if (start_released.compare_exchange_strong(expected, true)) {
      start_gate.release();
    }
  }

  // [thread-safe] Idempotent, non-blocking: flag + the pull's cancel wake
  // machinery (socket-open/Hello/OpenSession waits, download loop, tee
  // backpressure) + the lock-wait/promotion polls read the flag.
  void requestCancel() {
    cancelled.store(true, std::memory_order_relaxed);
    fetch.requestCancel();
  }

  [[nodiscard]] bool isCancelled() const {
    return cancelled.load(std::memory_order_relaxed) || fetch.isCancelled();
  }

  // Exactly-once terminal (defensive counter; the run() flow fires it once).
  void fireTerminal(PJ_descriptor_import_outcome_t outcome, const std::string& message) {
    if (terminal_count.fetch_add(1) != 0) {
      return;
    }
    if (on_terminal != nullptr) {
      on_terminal(callback_ctx, outcome, PJ_string_view_t{message.data(), message.size()});
    }
  }

  void run() noexcept;
  PJ_descriptor_import_outcome_t runToTerminal(std::string* message);
};

void DescriptorImportProvider::JobState::run() noexcept {
  // FIRST action: block on the start gate — the ABI forbids any job callback
  // before start_import returns; startImport releases the gate as it is
  // about to return, after out_job is fully populated.
  start_gate.acquire();
  PJ_descriptor_import_outcome_t outcome = PJ_DESCRIPTOR_IMPORT_FAILED;
  std::string message;
  try {
    outcome = runToTerminal(&message);
  } catch (...) {
    outcome = PJ_DESCRIPTOR_IMPORT_FAILED;
    message = "internal error while running the import";
  }
  fireTerminal(outcome, message);
}

PJ_descriptor_import_outcome_t DescriptorImportProvider::JobState::runToTerminal(std::string* message) {
  if (isCancelled()) {
    *message = "import cancelled";
    return PJ_DESCRIPTOR_IMPORT_CANCELLED;
  }

  // ---- the in-process materialization gate (bounded, cancelable) -----------
  // Contention = the interactive fetch or another job is materializing this
  // SAME identity right now. Wait bounded + cancelable, then REVALIDATE.
  //
  // LOCKED v1 DECISION (consult-prescribed; reasoned against §8 + the host's
  // LayoutImportBatch): a job that, after waiting out a concurrent
  // materialization, finds a fresh VALID cache file must FAIL with an
  // actionable retry-classification message — NOT promote, NOT report
  // success. Why: the host classifies hit/miss BEFORE starting a job, so the
  // only way a job discovers a fresh valid cache is exactly this race; no
  // eager ingest ran here (ZERO on_dataset — §8's zero-or-one covers it), so
  // SUCCEEDED_EAGER_ONLY would describe a dataset that does not exist, and
  // promotion is impossible — it REPLACES a live dataset, and this job has
  // none. The host's per-source failure isolation surfaces the message and a
  // reload classifies the session as a plain cache hit. NEVER late-attach the
  // concurrent fetch's DatasetId (it would violate
  // on_dataset-before-first-publication).
  std::optional<ImportRuntime::MaterializeTicket> ticket = runtime.tryBeginMaterialize(identity);
  if (!ticket.has_value()) {
    const auto deadline = std::chrono::steady_clock::now() + lock_timeout;
    for (;;) {
      if (isCancelled()) {
        *message = "import cancelled";
        return PJ_DESCRIPTOR_IMPORT_CANCELLED;
      }
      if (std::chrono::steady_clock::now() >= deadline) {
        *message =
            "another materialization of this session is already in progress — retry when it "
            "completes";
        return PJ_DESCRIPTOR_IMPORT_FAILED;
      }
      std::this_thread::sleep_for(lock_poll);
      ticket = runtime.tryBeginMaterialize(identity);
      if (ticket.has_value()) {
        break;
      }
    }
    // Contended-then-acquired: the concurrent materialization ENDED while we
    // waited. Revalidate the disk cache under our fresh slot.
    std::filesystem::path disk;
    if (runtime.fileCache().lookup(identity, &disk)) {
      ticket.reset();
      *message =
          "session was materialized concurrently — reload to classify it as a cache hit";
      return PJ_DESCRIPTOR_IMPORT_FAILED;
    }
    // It failed or was cancelled: proceed with a fresh download on our slot.
  }

  // ---- the direct cancellable pull (cache-tee mode, ticket adopted) --------
  PullRequest request;
  request.connection = connection;
  request.sequence_names = descriptor.s3_keys;
  request.group_name =
      descriptor.display_name.empty() ? descriptor.s3_keys.front() : descriptor.display_name;
  request.topic_names = descriptor.topics;
  request.start_ns = descriptor.start_ns;
  request.end_ns = descriptor.end_ns;
  request.include_latched = descriptor.include_latched;  // FROM THE DESCRIPTOR (D4)
  request.max_transfer_bytes = max_transfer_bytes;
  request.runtime = &runtime;
  request.canonical_descriptor_json = canonical_json;
  request.descriptor_json = display_json;
  request.identity = identity;
  request.ticket = std::move(ticket);
  // on_dataset: zero-or-one, from the pull's dataset creation, strictly
  // before any publication/progress/promotion — the pull fires this hook at
  // exactly that point, on this job-callback thread (serialized trivially).
  request.datasetCreated = [this](PJ::sdk::DataSourceHandle handle) {
    if (on_dataset != nullptr) {
      on_dataset(callback_ctx, PJ_data_source_handle_t{handle.id});
    }
  };

  const PullResult res = fetch.pull(std::move(request));
  if (pre_terminal_hook) {
    pre_terminal_hook();  // test seam: deterministic ceiling-vs-cancel interleave
  }

  // ---- terminal mapping ----------------------------------------------------
  // Documented precedence (pinned by the adversarial suite): for a
  // NON-COMPLETE pull, CANCEL WINS — even over the byte ceiling when both
  // latched (an explicit caller cancel outranks the resource
  // classification; the partial is deleted either way; a retry reports the
  // ceiling uncontaminated), and the ceiling then wins over every other
  // failure cause (FAILED, never CANCELLED). A COMPLETE pull is different
  // (quality review IMPORTANT-2): the download finished, the tee finalized
  // and the entry stored BEFORE the cancel could land, so a cancel racing
  // in post-completion must not rewrite history — the job reports its
  // TRUTHFUL terminal (PROMOTED/EAGER_ONLY per the settled promotion; the
  // host's cancel rollback is ledger-based over produced datasets, so a
  // SUCCEEDED_* at a cancelling batch is removed cleanly either way). The
  // one exception below: a promotion still OUTSTANDING at cancel detaches
  // as CANCELLED — join() must stay unblockable.
  if (res.terminal == PullTerminal::kCancelled ||
      (res.terminal != PullTerminal::kComplete && isCancelled())) {
    *message = "import cancelled";
    return PJ_DESCRIPTOR_IMPORT_CANCELLED;  // the tee already deleted the partial
  }
  if (res.byte_ceiling_exceeded) {
    *message = res.error;
    return PJ_DESCRIPTOR_IMPORT_FAILED;
  }
  if (res.terminal == PullTerminal::kFailed) {
    *message = res.error.empty() ? "import failed" : res.error;
    return PJ_DESCRIPTOR_IMPORT_FAILED;
  }

  // Complete. EAGER_ONLY may only describe a GENUINELY usable eager dataset
  // (D4-as-amended): a dataset was created AND at least one topic decoded.
  const bool eager_usable = res.dataset.has_value() && res.any_decodable;

  if (res.tee_outcome == TeeOutcome::kFinalized && res.promotion != nullptr) {
    // Wait cancel-aware for the promotion transaction. A cancel while the
    // ACCEPTED promotion is outstanding DETACHES: the shared result state
    // outlives this job (it settles whenever on_result fires) and the
    // terminal is CANCELLED without waiting — the plugin-side half of the
    // cross-repo teardown-deadlock mitigation (PJ4's promotion-shutdown
    // reorder rides its own PR; this job must never RELY on host shutdown
    // ordering to unblock its join()).
    const bool settled =
        res.promotion->waitSettled([this]() { return isCancelled(); }, lock_poll);
    if (!settled) {
      *message = "import cancelled (promotion still pending — detached)";
      return PJ_DESCRIPTOR_IMPORT_CANCELLED;
    }
    if (res.promotion->ok().value_or(false)) {
      *message = "promoted to a file-backed source";
      return PJ_DESCRIPTOR_IMPORT_SUCCEEDED_PROMOTED;
    }
    if (eager_usable) {
      *message = "promotion did not complete (" + res.promotion->message() +
                 ") — the eager dataset remains usable";
      return PJ_DESCRIPTOR_IMPORT_SUCCEEDED_EAGER_ONLY;
    }
    *message = "download completed but no topic could be decoded and promotion failed (" +
               res.promotion->message() + ")";
    return PJ_DESCRIPTOR_IMPORT_FAILED;
  }

  // Tee failed/aborted on a COMPLETE download: §9.6 — the ingest completed
  // for the user; only promotion is suppressed.
  if (eager_usable) {
    *message = (res.tee_outcome == TeeOutcome::kFailed)
                   ? ("import completed; cache write failed so promotion was skipped (spec §9.6): " +
                      res.tee_error)
                   : "import completed without promotion";
    return PJ_DESCRIPTOR_IMPORT_SUCCEEDED_EAGER_ONLY;
  }
  *message = "download completed but no topic could be decoded into a usable dataset";
  if (!res.tee_error.empty()) {
    *message += " (cache: " + res.tee_error + ")";
  }
  return PJ_DESCRIPTOR_IMPORT_FAILED;
}

// ---------------------------------------------------------------------------
// The job vtable trio
// ---------------------------------------------------------------------------

void DescriptorImportProvider::jobCancel(void* ctx) noexcept {
  if (ctx != nullptr) {
    static_cast<JobState*>(ctx)->requestCancel();
  }
}

void DescriptorImportProvider::jobJoin(void* ctx) noexcept {
  if (ctx == nullptr) {
    return;
  }
  auto* state = static_cast<JobState*>(ctx);
  // ABI: join/destroy must never be called from a job callback (that thread
  // joining itself is deadlock-or-terminate). The host is trusted; this
  // guard documents the rule and downgrades a violation to a no-op.
  if (state->worker.joinable() && state->worker.get_id() != std::this_thread::get_id()) {
    state->worker.join();
  }
}

void DescriptorImportProvider::jobDestroy(void* ctx) noexcept {
  if (ctx == nullptr) {
    return;
  }
  // NOTE: jobJoin's self-join guard does NOT make destroy-from-a-callback
  // survivable — the guard only skips the join, and `delete state` below
  // then destroys a still-joinable std::thread member, which is
  // std::terminate. The ABI's "never call join/destroy from a job callback"
  // rule is load-bearing here, not merely advisory.
  auto* state = static_cast<JobState*>(ctx);
  jobCancel(ctx);
  // Defensive: startImport always releases the gate before returning, but a
  // gate that was somehow never released must not deadlock the join below.
  state->releaseStartOnce();
  jobJoin(ctx);
  delete state;
}

// ---------------------------------------------------------------------------
// DescriptorImportProvider
// ---------------------------------------------------------------------------

DescriptorImportProvider::DescriptorImportProvider(ImportRuntime& runtime) : runtime_(runtime) {}

DescriptorImportProvider::~DescriptorImportProvider() = default;

void DescriptorImportProvider::bind(PJ::sdk::SettingsView settings, HostBindings bindings) {
  settings_ = settings;
  bindings_ = std::move(bindings);
}

CredentialStore& DescriptorImportProvider::credentialStore() {
  // Lazily construct the default file-backed store (mirrors the dialog's
  // seam — the libsecret drop-in replaces this behind the same interface).
  if (!credentials_) {
    credentials_ = std::make_unique<FileCredentialStore>(defaultConfigRoot());
  }
  return *credentials_;
}

bool DescriptorImportProvider::queryDescriptor(PJ_string_view_t descriptor_json,
                                               PJ_descriptor_query_result_v1_t* out_result,
                                               PJ_error_t* out_error) {
  try {
    if (out_result == nullptr) {
      PJ::sdk::fillError(out_error, 1, "mcap_cloud", "null out_result");
      return false;
    }
    std::string parse_error;
    const std::optional<SourceDescriptor> d =
        parseSourceDescriptor(toView(descriptor_json), &parse_error);
    if (!d.has_value()) {
      // Malformed OR unsupported: a CONTRACT failure (query returns false),
      // never a trust verdict (D3-as-amended).
      PJ::sdk::fillError(out_error, 1, "mcap_cloud", parse_error);
      return false;
    }

    // Result strings: owned by this instance, valid until the NEXT query on
    // it (the ABI lifetime rule; main-thread only, like the query itself).
    query_identity_ = descriptorIdentity(*d);
    query_path_ = runtime_.fileCache().pathFor(query_identity_).string();
    query_message_.clear();

    // Trust: the in-memory set ONLY (§6.3 bounded-query rule). v1 emits only
    // trusted / needs-confirmation; kRefused is reserved for future policy
    // refusals.
    const bool trusted = runtime_.isTrusted(d->server_uri);
    if (!trusted) {
      query_message_ =
          "origin not trusted on this machine — connect to it once in the MCAP Cloud panel to "
          "trust it";
    }

    // is_materialized: the DISK-validated cache ONLY (bounded I/O — footer +
    // summary + provenance). The in-memory SessionCache must never answer
    // this (it stores counts, not bytes). estimated_bytes = the artifact's
    // size when materialized (local metadata; never the network), else 0.
    std::filesystem::path disk;
    const bool materialized = runtime_.fileCache().lookup(query_identity_, &disk);
    std::uint64_t estimated_bytes = 0;
    if (materialized) {
      std::error_code ec;
      const auto size = std::filesystem::file_size(disk, ec);
      if (!ec) {
        estimated_bytes = static_cast<std::uint64_t>(size);
      }
    }

    // Growth contract: write ONLY fields wholly covered by the caller's
    // struct_size (offsetof-covered, like the SDK's reference provider).
    auto covered = [out_result](std::size_t offset, std::size_t size) {
      return out_result->struct_size >= offset + size;
    };
    if (covered(offsetof(PJ_descriptor_query_result_v1_t, trust), sizeof(out_result->trust))) {
      out_result->trust =
          trusted ? PJ_DESCRIPTOR_TRUST_TRUSTED : PJ_DESCRIPTOR_TRUST_NEEDS_CONFIRMATION;
    }
    if (covered(offsetof(PJ_descriptor_query_result_v1_t, is_materialized),
                sizeof(out_result->is_materialized))) {
      out_result->is_materialized = materialized ? 1 : 0;
    }
    if (covered(offsetof(PJ_descriptor_query_result_v1_t, source_identity),
                sizeof(out_result->source_identity))) {
      out_result->source_identity = PJ_string_view_t{query_identity_.data(), query_identity_.size()};
    }
    if (covered(offsetof(PJ_descriptor_query_result_v1_t, local_path_utf8),
                sizeof(out_result->local_path_utf8))) {
      out_result->local_path_utf8 = PJ_string_view_t{query_path_.data(), query_path_.size()};
    }
    if (covered(offsetof(PJ_descriptor_query_result_v1_t, message), sizeof(out_result->message))) {
      out_result->message = PJ_string_view_t{query_message_.data(), query_message_.size()};
    }
    if (covered(offsetof(PJ_descriptor_query_result_v1_t, estimated_bytes),
                sizeof(out_result->estimated_bytes))) {
      out_result->estimated_bytes = estimated_bytes;
    }
    return true;
  } catch (...) {
    PJ::sdk::fillError(out_error, 1, "mcap_cloud", "internal error in query_descriptor");
    return false;
  }
}

bool DescriptorImportProvider::startImport(const PJ_descriptor_import_start_request_v1_t* request,
                                           const PJ_descriptor_import_callbacks_v1_t* callbacks,
                                           void* callback_ctx, PJ_joinable_job_t* out_job,
                                           PJ_error_t* out_error) {
  try {
    if (request == nullptr || out_job == nullptr) {
      PJ::sdk::fillError(out_error, 1, "mcap_cloud", "null request/out_job");
      return false;
    }
    auto req_covered = [request](std::size_t offset, std::size_t size) {
      return request->struct_size >= offset + size;
    };

    // FLAGS FAIL CLOSED, FIRST: unknown bits reject synchronously — no
    // callbacks, out_job untouched (the ABI's fail-closed spine).
    const std::uint64_t flags =
        req_covered(offsetof(PJ_descriptor_import_start_request_v1_t, flags), sizeof(request->flags))
            ? request->flags
            : PJ_DESCRIPTOR_IMPORT_START_FLAG_NONE;
    if ((flags & ~PJ_DESCRIPTOR_IMPORT_START_FLAGS_V1_MASK) != 0) {
      PJ::sdk::fillError(out_error, 1, "mcap_cloud", "unknown start_import flag bits (fail closed)");
      return false;
    }

    // Required callback surface: on_terminal is the exactly-once spine.
    const bool terminal_covered =
        callbacks != nullptr &&
        callbacks->struct_size >= offsetof(PJ_descriptor_import_callbacks_v1_t, on_terminal) +
                                      sizeof(callbacks->on_terminal);
    if (!terminal_covered || callbacks->on_terminal == nullptr) {
      PJ::sdk::fillError(out_error, 1, "mcap_cloud", "on_terminal callback is required");
      return false;
    }
    // on_dataset precedes on_terminal in the struct, so it is covered too.
    auto on_dataset = callbacks->on_dataset;  // may be null (zero-or-one)

    std::string parse_error;
    const std::string_view descriptor_json =
        req_covered(offsetof(PJ_descriptor_import_start_request_v1_t, descriptor_json),
                    sizeof(request->descriptor_json))
            ? toView(request->descriptor_json)
            : std::string_view{};
    const std::optional<SourceDescriptor> d = parseSourceDescriptor(descriptor_json, &parse_error);
    if (!d.has_value()) {
      PJ::sdk::fillError(out_error, 1, "mcap_cloud", parse_error);
      return false;
    }
    if (!bindings_.host_provider || !bindings_.runtime_host_provider) {
      PJ::sdk::fillError(out_error, 1, "mcap_cloud",
                         "descriptor import provider is not bound to a host");
      return false;
    }

    // Credentials + the immutable ConnectionSnapshot resolved ON THIS (main)
    // thread — every SettingsView call is main-thread-only (the PR-2
    // contract); the job thread never touches the view or the store.
    ConnectionSnapshot snapshot;
    snapshot.uri = d->server_uri;
    snapshot.credentials = resolveCredentials(settings_, credentialStore(), d->server_uri);

    auto* state = new JobState(runtime_);
    state->bindings = bindings_;
    state->connection = std::move(snapshot);
    state->descriptor = *d;
    state->canonical_json = canonicalSourceDescriptorJson(*d);
    state->display_json = toSourceDescriptorJson(*d);
    state->identity = descriptorIdentity(*d);
    state->max_transfer_bytes =
        req_covered(offsetof(PJ_descriptor_import_start_request_v1_t, max_transfer_bytes),
                    sizeof(request->max_transfer_bytes))
            ? request->max_transfer_bytes
            : 0;
    state->lock_poll = lock_poll_;
    state->lock_timeout = lock_timeout_;
    state->pre_terminal_hook = pre_terminal_hook_;
    state->on_dataset = on_dataset;
    state->on_terminal = callbacks->on_terminal;
    state->callback_ctx = callback_ctx;
    state->fetch.setHostProvider(bindings_.host_provider);
    state->fetch.setRuntimeHostProvider(bindings_.runtime_host_provider);

    // Spawn the GATED worker first (its first action is start_gate.acquire(),
    // so it cannot touch anything before the release below): a thread-spawn
    // failure must return false with out_job UNTOUCHED and no leaked state.
    try {
      state->worker = std::thread([state]() { state->run(); });
    } catch (...) {
      delete state;
      PJ::sdk::fillError(out_error, 1, "mcap_cloud", "could not start the import worker thread");
      return false;
    }

    // Populate out_job with the worker safely gated; the caller reads it
    // only after this returns.
    out_job->ctx = state;
    out_job->vtable = &kJobVtable;

    // The explicit post-return START GATE: released only now — after
    // out_job is fully populated and this thunk is about to return — so no
    // callback can run before start_import returns (the worker's FIRST
    // action is start_gate.acquire()).
    if (start_gate_probe_) {
      start_gate_probe_();  // test seam: observe the pre-release world
    }
    state->releaseStartOnce();
    return true;
  } catch (...) {
    PJ::sdk::fillError(out_error, 1, "mcap_cloud", "internal error in start_import");
    return false;
  }
}

}  // namespace mcap_cloud
