// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// WS+Protobuf BROWSE + in-dialog DOWNLOAD/DECODE transport (Slice 5 toolbox
// restore). The catalog path (Hello / ListFiles / GetFile) runs over the
// worker's own ixwebsocket BackendConnection; pullTopicsAsync opens a FRESH
// session connection, delegates parsing to the host via ParserIngestDriver
// (ensureParserBinding + pushMessage through the toolbox runtime host), and
// writes all scalars + object topics through the host's registered parsers.
// All host writes are serialized by host_write_mu_ (the toolbox DataWriter has
// no internal mutex). The download relocates pj_cloud_source.cpp's
// onStart/downloadLoop into the worker, the Mosaico in-dialog Fetch shape.

#include "fetch_worker.hpp"

#include <atomic>
#include <chrono>
#include <cstdio>
#include <filesystem>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "decoded_message.hpp"
#include "import_runtime.hpp"
#include "parser_ingest_driver.hpp"
#include "mcap_save_path.hpp"
#include "session_cache.hpp"
#include "session_key.hpp"
#include "session_mcap_writer.hpp"
#include "source_descriptor.hpp"
#include "vocab_select.hpp"

namespace mcap_cloud {

namespace {

struct TeeTerminalOutcome {
  TeeOutcome outcome = TeeOutcome::kNone;
  std::string error;
};

// The RUNTIME-mode terminal for one pull's cache tee (pure extraction from
// pullTopicsAsync — quality review IMPORTANT-3). SINGLE-ENCODER retention
// rules:
//   Complete  -> drain+close -> validated finalize (producer-count pinned)
//                -> export (if requested) = byte COPY of the finalized
//                cache file, atomic temp+rename;
//   otherwise -> drain+close leaves a READABLE partial; a requested export
//                receives a COPY of it at the reserved `.mcap.partial` name
//                (the user-facing export DELIBERATELY keeps readable
//                partials), then the CACHE partial is DELETED — cache
//                partials never survive (spec §10). Do not "align" the two
//                retentions: they are opposite by design.
// Ends with abortAndCleanup() (no-op after a successful finalize; otherwise
// it deletes the cache partial and releases the locks).
TeeTerminalOutcome finishCacheTee(
    CacheTee& tee, const SessionStats& stats, bool cancelled_after_download,
    const std::optional<McapOutputPaths>& save_paths,
    const std::function<void(McapSaveResult)>& emit_save_result,
    const std::function<void()>& discard_partial,
    const std::function<void(const std::filesystem::path&)>& export_by_copy,
    const std::function<bool(const std::filesystem::path&, std::string*)>& copy_to_export_partial) {
  TeeTerminalOutcome result;
  std::string close_error;
  const bool closed_ok = tee.drainAndClose(&close_error);
  const bool complete = stats.eos == SessionEos::Complete && !cancelled_after_download;
  if (!closed_ok) {
    // Writer/finalize failure: no readable file exists at all.
    result.outcome = TeeOutcome::kFailed;
    result.error = close_error;
    if (save_paths.has_value()) {
      discard_partial();
      emit_save_result(McapSaveResult{McapSaveStatus::Failed, {}, close_error});
    }
  } else if (complete) {
    std::string finalize_error;
    if (tee.finalize(std::nullopt, &finalize_error)) {
      result.outcome = TeeOutcome::kFinalized;
      if (save_paths.has_value()) {
        export_by_copy(tee.finalPath());
      }
    } else {
      result.outcome = TeeOutcome::kFailed;
      result.error = finalize_error;
      if (save_paths.has_value()) {
        discard_partial();
        emit_save_result(McapSaveResult{McapSaveStatus::Failed, {}, finalize_error});
      }
    }
  } else {
    // Cancelled / transport drop / server error: the closed partial is a
    // readable MCAP. Copy it out for a requested export BEFORE the cache
    // partial is deleted. Wording derives from the SAME terminal snapshot
    // as every other surface.
    result.outcome = TeeOutcome::kAborted;
    result.error = !stats.error.empty()
                       ? stats.error
                       : ((stats.eos == SessionEos::Cancelled || cancelled_after_download)
                              ? "download cancelled"
                              : "download ended before completion");
    if (save_paths.has_value()) {
      std::string copy_error;
      if (!copy_to_export_partial(tee.partialPath(), &copy_error)) {
        discard_partial();
        emit_save_result(McapSaveResult{McapSaveStatus::Failed, {}, copy_error});
      } else {
        emit_save_result(
            McapSaveResult{McapSaveStatus::Partial, save_paths->partial_path.string(), result.error});
      }
    }
  }
  tee.abortAndCleanup();  // no-op after finalize; else deletes the cache partial
  return result;
}

// Host-stop WATCHDOG (review-caught; pure extraction from pullTopicsAsync so
// the direct pull shares it): the in-callback isStopRequested() check only
// runs when messages flow — a stalled server or a reconnect backoff would
// ignore the host's Stop for the full frame-wait/backoff timeouts (minutes).
// This poller observes the [thread-safe] stop slot from its own thread and
// invokes `on_stop` (the owner's requestCancel), whose wake machinery
// (cancel hook + sendAndWait/frame-wait/backoff predicates) already unblocks
// every waiting phase promptly. stop() is the ordering point before the
// terminal classification; the destructor is the exception-safe join (the
// download invokes unrestricted std::function callbacks — unwinding through
// a joinable std::thread is std::terminate).
class HostStopWatchdog {
 public:
  HostStopWatchdog(ParserIngestDriver& driver, bool active, std::function<void()> on_stop,
                   const std::function<std::thread(std::function<void()>)>& factory = {}) {
    if (!active) {
      return;
    }
    std::function<void()> body = [this, &driver, on_stop = std::move(on_stop)]() {
      while (!done_.load(std::memory_order_relaxed)) {
        if (driver.datasetIngest().isStopRequested()) {
          on_stop();
          return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
      }
    };
    // Thread-spawn failure degrades to NO watchdog (adversarial F8): the
    // per-message isStopRequested check in the download loop remains, so a
    // host Stop is still honored while messages flow — the watchdog is an
    // enhancement for stalled streams, never a correctness dependency, and
    // std::system_error must not escape into the pull.
    try {
      thread_ = factory ? factory(std::move(body)) : std::thread(std::move(body));
    } catch (...) {
    }
  }

  HostStopWatchdog(const HostStopWatchdog&) = delete;
  HostStopWatchdog& operator=(const HostStopWatchdog&) = delete;

  ~HostStopWatchdog() { stop(); }

  /// Signal + join (idempotent). Called before the terminal classification /
  /// finalize() so the dataset-ingest view outlives the poller.
  void stop() {
    done_.store(true, std::memory_order_relaxed);
    if (thread_.joinable()) {
      thread_.join();
    }
  }

 private:
  std::atomic<bool> done_{false};
  std::thread thread_;
};

}  // namespace

FetchWorker::FetchWorker() = default;
FetchWorker::~FetchWorker() = default;

void FetchWorker::requestCancel() {
  cancel_flag_.store(true, std::memory_order_relaxed);
  // Signal any in-flight wire session so downloadSession() returns. The
  // pointer is guarded by cancel_mu_ (NOT a bare atomic): the worker clears it
  // BEFORE destroying the session backend, all under the same lock, so
  // cancelSession() can never run against a destroyed object (the prior bare
  // atomic left a load->call window where the worker could destroy it — a
  // use-after-free). cancelSession() only sets a flag + notifies, so holding
  // the leaf lock across it does not block.
  std::lock_guard<std::mutex> lock(cancel_mu_);
  if (backend_session_for_cancel_ != nullptr) {
    backend_session_for_cancel_->cancelSession();
  }
  // Free a pull thread blocked in the cache tee's backpressure wait (quality
  // review IMPORTANT-2): without this, only the tee's writer thread could
  // signal queue_not_full_, so a stalled disk wedged the pull AND this very
  // cancel behind it. requestAbort only flips an atomic + notifies (leaf
  // queue mutex), so holding cancel_mu_ across it cannot block; the pointer
  // rides the same publish/retire-under-cancel_mu_ discipline as the backend
  // hook above.
  if (tee_for_cancel_ != nullptr) {
    tee_for_cancel_->requestAbort();
  }
}

void FetchWorker::setSessionForCancel(BackendConnection* session) {
  std::lock_guard<std::mutex> lock(cancel_mu_);
  backend_session_for_cancel_ = session;
}

void FetchWorker::setTeeForCancel(CacheTee* tee) {
  std::lock_guard<std::mutex> lock(cancel_mu_);
  tee_for_cancel_ = tee;
}

bool FetchWorker::teeEnqueueFailed(std::unique_ptr<CacheTee>& tee, const DecodedMessage& message,
                                   std::string* error) {
  if (tee == nullptr || tee->enqueue(message)) {
    return false;
  }
  if (tee->abortRequested() && !tee->failed()) {
    // A CANCEL-freed enqueue is NOT a failure: the tee stays alive so the
    // single terminal boundary can still copy the readable partial for a
    // requested export and classify the pull kAborted consistently.
    return false;
  }
  // The writer thread hit a disk error (§9.6): never abort the ingest —
  // retire + drop the tee (partial deleted, locks released) and hand the
  // raw cause to the caller for outcome recording / export failure.
  *error = tee->failureError();
  setTeeForCancel(nullptr);
  tee->abortAndCleanup();
  tee.reset();
  return true;
}

void FetchWorker::storeCompletedSessionEntry(
    SessionCache& session_cache, const SessionKey& session_key, const std::string& group_name,
    const std::string& server_uri, const std::vector<std::string>& topic_names,
    const std::unordered_map<std::uint32_t, std::string>& name_by_id,
    const std::unordered_map<std::uint32_t, std::uint64_t>& counts, const std::string& tee_identity,
    TeeOutcome tee_outcome, bool refetch_after_disk_miss) {
  CachedSession entry;
  entry.display_name = group_name;
  entry.server_uri = server_uri;
  // name -> decoded count (session topic names are unique), instead of a
  // per-topic reverse scan of name_by_id (quality review, nit 5).
  std::unordered_map<std::string, std::uint64_t> count_by_name;
  count_by_name.reserve(name_by_id.size());
  for (const auto& [tid, name] : name_by_id) {
    const auto it = counts.find(tid);
    count_by_name[name] = (it != counts.end()) ? it->second : 0;
  }
  for (const auto& t : topic_names) {
    const auto it = count_by_name.find(t);
    const std::uint64_t count = (it != count_by_name.end()) ? it->second : 0;
    entry.counts_by_topic[t] = count;
    entry.total_messages += count;
  }
  // Stage-4 (D7) state: the STABLE dataset id this pull created (the
  // existence key), the durable cache identity when the tee published a
  // valid file, the tee outcome (promotion suppression, §9.6), and which
  // §6.1 memory-hit rule led here.
  {
    std::lock_guard<std::mutex> lock(fetch_dataset_mu_);
    entry.dataset_id = fetch_dataset_.has_value() ? fetch_dataset_->id : 0;
  }
  entry.cache_identity = (tee_outcome == TeeOutcome::kFinalized) ? tee_identity : std::string{};
  entry.tee_outcome = tee_outcome;
  entry.last_hit_case =
      refetch_after_disk_miss ? MemoryHitCase::kRefetchedDiskMiss : MemoryHitCase::kNone;
  session_cache.store(session_key, std::move(entry));
}

bool FetchWorker::datasetExistsInHost(const CachedSession& entry) const {
  // Presence-unknown -> false (-> cache MISS), never a false HIT. No host
  // provider bound, or the host lacks acquire_catalog_snapshot, both yield false.
  if (!host_provider_) {
    return false;
  }
  PJ::sdk::ToolboxHostView host = host_provider_();
  auto snap = host.catalogSnapshot();
  if (!snap) {
    return false;  // "toolbox host does not support acquire_catalog_snapshot"
  }
  for (const auto& ds : snap->dataSources()) {
    const std::string_view name(ds.name.data, ds.name.size);
    if (entry.dataset_id != 0) {
      // D7: the STABLE dataset id is the existence key (display names collide
      // and mutate); the recorded name doubles as a recycle tiebreak — an id
      // match wearing a different name means the host reused the id for some
      // OTHER dataset, which is proven-gone for this entry. KEPT under
      // adversarial F11: SDK 0.20.0 documents NO non-recycling guarantee for
      // PJ_data_source_handle_t ids (verified — no such contract text in
      // plugin_data_api.h), so dropping the tiebreak would trade a rare
      // rename false-miss for a wrong-dataset false-HIT. Rename-induced
      // misses only cost a refetch.
      if (ds.handle.id == entry.dataset_id) {
        return name == entry.display_name;
      }
    } else if (name == entry.display_name) {
      return true;  // legacy entry (no id recorded): name-only fallback
    }
  }
  return false;
}

bool FetchWorker::serveFromMemoryCache(
    ImportRuntime* rt, SessionCache& session_cache, const SessionKey& session_key,
    const std::string& tee_identity, const std::string& descriptor_json,
    const std::string& group_name, const std::vector<std::string>& topic_names,
    const std::string& save_directory, const SessionCache::ExistencePredicate& exists,
    const std::function<void(const std::filesystem::path&)>& export_by_copy,
    TeeOutcome* tee_outcome, bool* refetch_after_disk_miss) {
  auto cached = session_cache.lookup(session_key, exists);
  if (!cached.has_value()) {
    return false;  // plain miss
  }
  // Hold a SHARED read lease across the disk validity check AND the export
  // copy below: a concurrent cross-process eviction (which needs the
  // exclusive lock) cannot delete the file mid-serve. Best-effort — a live
  // exclusive holder (a re-materialization) makes the lease unavailable, and
  // the lookup/copy failure paths below stay reported, never silent.
  std::optional<FileLock> read_lease;
  if (rt != nullptr) {
    read_lease = rt->fileCache().acquireReadLease(tee_identity, nullptr);
  }
  std::filesystem::path disk_file;
  const bool disk_valid = (rt != nullptr) && rt->fileCache().lookup(tee_identity, &disk_file);
  // Belt-and-braces identity binding (adversarial F1, independent of the
  // include_latched key fix): the entry must have been stored for EXACTLY the
  // requested durable identity — a mismatched (or empty, tee-failed)
  // cache_identity means the in-memory counts and the on-disk artifact
  // describe different requests, so serving the memory entry (or later
  // promoting the disk file over its dataset) would mix sessions.
  const bool identity_ok = (rt == nullptr) || cached->cache_identity == tee_identity;
  if (rt != nullptr && (!disk_valid || !identity_ok)) {
    // §6.1: the durable file is gone/invalid (or provably not THIS request's)
    // — the memory entry cannot stand in for it. Evict and refetch.
    session_cache.evict(session_key);
    *refetch_after_disk_miss = true;
    return false;
  }
  // HIT: re-emit the per-topic pullFinished ledger from cached counts
  // with NO BackendConnection construction. Each requested topic reports
  // ok with a final progress sample from its cached count (count ==
  // "messages", a reasonable progress proxy since the bytes already live
  // in the store).
  if (rt != nullptr) {
    session_cache.recordHitCase(session_key, MemoryHitCase::kServedValidDisk);
    *tee_outcome = TeeOutcome::kExistingValid;
    // §6.1: a memory hit whose disk file exists RE-promotes it (kExistingValid
    // is the PR-1 signal), keyed on the CACHED stable dataset id — except an
    // entry already kPromoted (re-promoting an already-file-backed dataset
    // would only make the host reload the same file; the promoted state IS
    // the end state a re-promotion seeks). dataset_id==0 (legacy entry)
    // cannot be promoted — promotion requires a live dataset to replace.
    if (cached->dataset_id != 0 && cached->promotion_state != PromotionState::kPromoted) {
      (void)rt->promoteToFileSource(
          ImportRuntime::makePromotionRequest(cached->dataset_id, tee_identity, disk_file,
                                              descriptor_json),
          &session_key);
    }
  }
  if (pullServedFromCache) {
    pullServedFromCache(group_name);
  }
  for (const auto& t : topic_names) {
    auto cit = cached->counts_by_topic.find(t);
    const std::uint64_t count = (cit != cached->counts_by_topic.end()) ? cit->second : 0;
    if (pullProgress) {
      pullProgress(t, static_cast<std::int64_t>(count));
    }
    if (pullFinished) {
      pullFinished(group_name, t, /*ok=*/true, {});
    }
  }
  if (rt != nullptr && !save_directory.empty()) {
    export_by_copy(disk_file);  // export = byte copy of the valid cache file
  }
  // Adversarial F2: the served dataset keeps depending on the disk file
  // (lazy re-opens) — RETAIN the read lease for the runtime lifetime instead
  // of dropping it at scope exit (no-op when an identity lease is already
  // retained, e.g. from the original finalize).
  if (rt != nullptr && read_lease.has_value()) {
    // The lease was acquired BEFORE the disk validation above and held
    // across it (invariant 2), so this only records it.
    rt->adoptLeaseForLifetime(tee_identity, std::move(*read_lease));
  }
  return true;
}

PJ::Expected<PJ::sdk::DataSourceHandle> FetchWorker::datasetForFetch(
    const PJ::sdk::ToolboxHostView& host, const std::string& sequence_name) {
  std::lock_guard<std::mutex> lock(fetch_dataset_mu_);
  if (!fetch_dataset_.has_value()) {
    auto ds = host.createDataSource(sequence_name);
    if (!ds) {
      return PJ::unexpected(std::move(ds).error());
    }
    fetch_dataset_ = *ds;
  }
  return *fetch_dataset_;
}

void FetchWorker::connectAsync(std::string uri, std::string cert_path, std::string api_key, bool allow_insecure) {
  // Construct the WS+Protobuf backend and run the Hello handshake. On failure
  // the verbatim server error (AUTH_FAILED / PROTOCOL_VERSION / transport) is
  // surfaced through connectFinished so the dialog can route it to the
  // notification bell. The try/catch guards against any exception escaping the
  // worker thread.
  try {
    backend_ = std::make_unique<BackendConnection>(uri, cert_path, api_key, allow_insecure);
    // A NEW connection (even one that goes on to fail) may talk to a
    // different/rebuilt server: the cached vocabulary and the last gate
    // selection belong to the OLD backend_ and must not silently carry over
    // (stale ids, or a customer/site pair the new server doesn't have).
    vocab_.reset();
    last_gate_customer_.clear();
    last_gate_site_.clear();
    last_gate_robot_.clear();
    last_gate_request_id_ = 0;
    std::string error;
    if (!backend_->connect(&error)) {
      backend_.reset();
      if (connectFinished) {
        connectFinished(false, uri, {}, error.empty() ? std::string("connection failed") : error);
      }
      return;
    }
    // Remember the credentials so each pull can open its own session connection.
    conn_uri_ = uri;
    conn_cert_path_ = cert_path;
    conn_api_key_ = api_key;
    conn_allow_insecure_ = allow_insecure;
    connection_lost_notified_ = false;  // fresh socket -> arm the lost-notify again
    std::string version_text;
    if (auto v = backend_->version()) {
      version_text = v->version;
    }
    // D8: surface the BackendCapabilities BEFORE connectFinished so the dialog
    // has the hierarchy flag + vocabulary in hand when it renders the next tick.
    // An omitted field (has_backend()==false) -> defaults (hierarchy off, empty
    // vocab), which the dialog treats as the flat-corpus path.
    if (capabilitiesReady) {
      capabilitiesReady(backend_->backendCapabilities().value_or(BackendCaps{}));
    }
    // D2: same ordering rationale as capabilitiesReady above — surface the
    // Capabilities (resume_supported/tag_edit_supported) BEFORE connectFinished
    // so the dialog can gate the tag-edit button off it in time for the next
    // tick. An omitted field -> ServerCaps{} defaults (tag_edit_supported
    // false), the conservative "don't offer a control that might fail" choice.
    if (serverCapabilitiesReady) {
      serverCapabilitiesReady(backend_->serverCapabilities().value_or(ServerCaps{}));
    }
    if (connectFinished) {
      connectFinished(true, uri, "Connected — server " + version_text, {});
    }
  } catch (const std::exception& e) {
    backend_.reset();
    if (connectFinished) {
      connectFinished(false, uri, {}, e.what());
    }
  } catch (...) {
    backend_.reset();
    if (connectFinished) {
      connectFinished(false, uri, {}, "Unknown error");
    }
  }
}

void FetchWorker::fetchVocabularyAsync(std::uint64_t request_id) {
  if (latest_gate_request_.load() != request_id) {
    // Superseded before starting: unlike listSequencesFilteredAsync's gated
    // list, a vocabulary fetch has no terminal-signal contract to honor (no
    // callback here is documented as "exactly one per call") — a plain no-op
    // is correct and the dialog's own id check would have dropped a stale
    // answer anyway.
    return;
  }
  if (!backend_) {
    if (vocabularyFailed) {
      vocabularyFailed(request_id);
    }
    return;
  }
  // The browse picker maps only the customer->site->robot tree + customer
  // counts, so ask the server to compute only that.
  auto vocab = backend_->getVocabulary(BackendConnection::kVocabularyTimeout,
                                       BackendConnection::VocabScope::kPickerOnly);
  if (backend_->isClosed()) {
    // A dead browse socket during the fetch: route through the same
    // once-per-connection connectionLost signal as every other RPC here,
    // rather than a bespoke vocabularyFailed (the dialog's connected-state
    // gating already reacts to connectionLost).
    notifyConnectionLostOnce();
    return;
  }
  if (!vocab) {
    if (vocabularyFailed) {
      vocabularyFailed(request_id);
    }
    return;
  }
  vocab_ = std::move(*vocab);
  if (vocabularyReady) {
    vocabularyReady(request_id, *vocab_, /*recovery=*/false);
  }
}

void FetchWorker::listSequencesFilteredAsync(std::uint64_t request_id, std::string customer,
                                             std::string site, std::string robot) {
  // The ONLY terminal signal for a gated list: exactly one finish() call on
  // every path out of this function (including the pre-start supersession
  // no-op below) — see F5/F4 in the task's design rationale.
  auto finish = [this, request_id](GateListResult::Error error, std::vector<SequenceInfo> sequences,
                                   std::string message = {}) {
    if (gateListFinished) {
      gateListFinished(GateListResult{request_id, error, std::move(sequences), std::move(message)});
    }
  };
  if (latest_gate_request_.load() != request_id) {
    // A newer gate request was already issued (supersedeGateRequests) before
    // this queued command started running: NO-OP rather than sweep the
    // now-irrelevant selection (F4 — commands are FIFO on one worker thread,
    // so a stale request can sit behind a full sweep without this check).
    return finish(GateListResult::Error::kSuperseded, {});
  }
  if (!backend_) {
    return finish(GateListResult::Error::kConnectionLost, {});
  }
  // Remembered even if this attempt ultimately fails/is superseded: it is
  // "the last gate selection the user asked for", which the tag-edit re-list
  // path below reuses to stay scoped to the same site after a commit. The id
  // is stored ALONGSIDE the names (not re-read from latest_gate_request_ at
  // use time) so the pair can never drift apart — see the tag-edit call site.
  last_gate_customer_ = customer;
  last_gate_site_ = site;
  last_gate_robot_ = robot;
  last_gate_request_id_ = request_id;

  // Two resolution attempts: the cached vocabulary, then ONE refresh when the
  // generation died mid-request (builder rebuild raced the sweep).
  for (int attempt = 0; attempt < 2; ++attempt) {
    if (!vocab_) {
      auto fresh = backend_->getVocabulary(BackendConnection::kVocabularyTimeout,
                                           BackendConnection::VocabScope::kPickerOnly);
      if (backend_->isClosed()) {
        // A dead browse socket during the recovery refresh: mirror
        // fetchVocabularyAsync's and the sweep-below's handling exactly —
        // without this check a dead socket read as an empty optional and
        // fell through to the generic kRebuildStorm below, so the dialog
        // never learned the connection was gone (connectionLost never
        // fired) and the misdiagnosis read as "catalog rebuilding" instead
        // of "reconnect".
        notifyConnectionLostOnce();
        return finish(GateListResult::Error::kConnectionLost, {});
      }
      if (!fresh) {
        break;  // vocabulary RPC failed on a live socket -> kRebuildStorm below
      }
      vocab_ = std::move(*fresh);
      // recovery=true: combos refresh, but the dialog must NOT start a second
      // sweep — THIS loop owns the retry (F3, the duplicate-sweep finding).
      if (vocabularyReady) {
        vocabularyReady(request_id, *vocab_, /*recovery=*/true);
      }
    }
    const auto filter = resolveGateFilter(*vocab_, customer, site, robot);
    if (!filter) {
      // Names no longer resolve against the (possibly just-refreshed)
      // vocabulary: a rebuild renamed/removed the site, or this is a stale
      // persisted selection typed before any vocabulary existed.
      return finish(GateListResult::Error::kSelectionGone, {});
    }
    bool complete = false;
    bool stale = false;
    auto abort = [this, request_id] { return latest_gate_request_.load() != request_id; };
    BackendConnection::PageCallback on_page;
    if (gatePageReady) {
      on_page = [this, request_id](const std::vector<SequenceInfo>& page, bool reset) {
        gatePageReady(request_id, page, reset);
      };
    }
    std::string list_error;
    auto sequences = backend_->listSequences(&complete, on_page, &*filter, &stale, abort, &list_error);
    if (backend_->isClosed()) {
      notifyConnectionLostOnce();
      return finish(GateListResult::Error::kConnectionLost, std::move(sequences));
    }
    if (latest_gate_request_.load() != request_id) {
      // Superseded mid-sweep (the abort predicate stopped listSequences()
      // early): the partial `sequences` it returned belong to a request the
      // dialog no longer cares about, so they are dropped rather than passed
      // to finish().
      return finish(GateListResult::Error::kSuperseded, {});
    }
    if (stale) {
      // The filter's dimension ids died with the old generation (a builder
      // rebuild raced the sweep): refresh the vocabulary and retry ONCE, per
      // BackendConnection::listSequences()'s filtered-abort contract.
      vocab_.reset();
      continue;
    }
    return finish(complete ? GateListResult::Error::kNone : GateListResult::Error::kPartial,
                  std::move(sequences), std::move(list_error));
  }
  // Both attempts were exhausted without a usable result (a vocabulary
  // refresh failed, or the generation kept dying out from under us): the
  // catalog is rebuilding faster than this loop can keep up.
  finish(GateListResult::Error::kRebuildStorm, {});
}

void FetchWorker::notifyConnectionLostOnce() {
  if (connection_lost_notified_) {
    return;
  }
  connection_lost_notified_ = true;
  if (connectionLost) {
    connectionLost();
  }
}

void FetchWorker::listTopicsAsync(std::string sequence_name) {
  if (!backend_) {
    if (topicsFailed) {
      topicsFailed(std::move(sequence_name), "not connected");
    }
    return;
  }

  TopicsResult result = backend_->listTopicsChecked(sequence_name);
  if (!result.ok) {
    const bool closed = backend_->isClosed();
    if (topicsFailed) {
      topicsFailed(std::move(sequence_name), std::move(result.error));
    }
    if (closed) {
      notifyConnectionLostOnce();
    }
    return;
  }
  std::vector<TopicInfo> infos = std::move(result.topics);

  std::vector<std::string> names;
  names.reserve(infos.size());
  for (const auto& info : infos) {
    names.push_back(info.topic_name);
  }

  // topicInfosReady carries the full TopicInfo list (size/schema/message-count);
  // topicsReady carries the name-only view. The dialog keeps the two aligned.
  if (topicInfosReady) {
    topicInfosReady(sequence_name, std::move(infos));
  }
  if (topicsReady) {
    topicsReady(std::move(sequence_name), std::move(names));
  }
}

void FetchWorker::fetchTopicMetadataAsync(std::string sequence_name, std::string topic_name) {
  if (!backend_) {
    return;  // no backend → emit nothing (the dialog leaves the panel as-is)
  }
  if (auto info = backend_->getTopicMetadata(sequence_name, topic_name)) {
    if (topicMetadataReady) {
      topicMetadataReady(std::move(sequence_name), std::move(topic_name), std::move(*info));
    }
  }
}

void FetchWorker::updateTagsAsync(std::string sequence_name,
                                  std::vector<std::pair<std::string, std::string>> set_tags,
                                  std::vector<std::string> unset_keys) {
  if (!backend_) {
    if (tagsUpdated) {
      tagsUpdated(std::move(sequence_name), false, "not connected");
    }
    return;
  }

  std::string error;
  const bool ok = backend_->updateTags(sequence_name, set_tags, unset_keys, /*effective_out=*/nullptr, &error);

  // Fire the per-commit result first so the dialog can surface a failure
  // verbatim. On success, RE-LIST so the flat user_metadata + per-tag override
  // view refresh and the Lua filter re-evaluates against the new tags — emitted
  // through the SAME sequencesReady path the catalog browse uses (the dialog's
  // onSequencesReady invalidates its seq view cache).
  if (tagsUpdated) {
    tagsUpdated(sequence_name, ok, ok ? std::string{} : error);
  }
  if (!ok) {
    return;
  }

  // Re-list to refresh the flat metadata + Lua filter view. If the dialog has
  // an active gate selection (the last customer/site the user browsed to —
  // and a tag edit implies a listed, hence gated, file, so the pair is set in
  // practice), stay scoped to it via the SAME gated path the browse gate
  // uses, rather than re-sweeping the WHOLE catalog. No gate selection on
  // record (e.g. the browse gate machinery hasn't landed on the dialog side
  // yet, or the ungated fallback path is in use) falls back to the legacy
  // unfiltered re-list below — just a guard, no assert.
  //
  // WHY last_gate_request_id_ (not latest_gate_request_.load()) here: this
  // command can sit queued behind a NEWER gate transition. If the GUI has
  // already bumped latest_gate_request_ to a new id and enqueued the real
  // sweep for the NEW site, reading latest_gate_request_ here would pass the
  // pre-start supersession check (id matches "latest") and sweep the OLD
  // site UNDER the new id — self-healing once the queued sweep runs, but
  // transiently wrong and indistinguishable from a real answer by the
  // dialog's id-equality check. Passing last_gate_request_id_ (the id these
  // NAMES were actually requested under) makes a stale pairing correctly
  // no-op as kSuperseded instead.
  if (!last_gate_customer_.empty() && !last_gate_site_.empty() && !last_gate_robot_.empty()) {
    listSequencesFilteredAsync(last_gate_request_id_, last_gate_customer_, last_gate_site_, last_gate_robot_);
    return;
  }

  // Guard on the `complete` flag exactly like every other browse path
  // (listSequencesFilteredAsync above): a PARTIAL re-list (a page dropped, or a
  // rebuild racing the pagination) must NOT replace the dialog's authoritative
  // catalog with a truncated snapshot — surface it as an error and keep the
  // existing view instead.
  bool complete = false;
  std::string list_error;
  std::vector<SequenceInfo> sequences = backend_->listSequences(&complete, {}, nullptr, nullptr, {}, &list_error);
  if (!complete) {
    if (errorOccurred) {
      errorOccurred("Tag saved, but the recording-list refresh failed"
                    + (list_error.empty() ? std::string(" (server paging error)") : ": " + list_error)
                    + " — the list may be stale; retry to refresh.");
    }
    return;
  }
  if (sequencesReady) {
    sequencesReady(std::move(sequences));
  }
}

void FetchWorker::pullTopicsAsync(std::vector<std::string> sequence_names, std::string group_name,
                                  std::vector<std::string> topic_names, std::int64_t start_ns, std::int64_t end_ns,
                                  std::string save_directory) {
  // The group/display name groups all topics of a (possibly stitched) selection
  // into one catalog dataset + addresses the per-topic ledger callbacks. For
  // N==1 it equals the single sequence name (byte-identical to the pre-Slice-7
  // behavior).
  if (group_name.empty() && !sequence_names.empty()) {
    group_name = sequence_names.front();
  }
  // Export bookkeeping, declared BEFORE finish_all so every exit path can
  // flush the exactly-one-McapSaveResult contract. The export is strictly
  // SECONDARY to the download: no export failure may abort the pull or the
  // host import (the symmetric rule to "parser rejection must not make the
  // reconstructed MCAP lossy").
  std::optional<McapOutputPaths> save_paths;
  bool save_result_emitted = false;
  auto emit_save_result = [this, &save_result_emitted](McapSaveResult result) {
    if (save_result_emitted) {
      return;
    }
    save_result_emitted = true;
    if (mcapSaveFinished) {
      mcapSaveFinished(std::move(result));
    }
  };
  // Remove the reserved/no-longer-wanted partial. Every exit that does not
  // retain a READABLE partial must release its reservation (see
  // prepareMcapOutputPaths — the partial exists from allocation time).
  auto discard_partial = [&save_paths]() {
    if (save_paths.has_value()) {
      std::error_code ec;
      std::filesystem::remove(save_paths->partial_path, ec);
    }
  };
  // ---- cache-tee bookkeeping (RUNTIME mode, stage-4 PR-1) ------------------
  // rt != nullptr selects the single-encoder shape: the session tees ONCE
  // into the SessionFileCache and export destinations receive byte copies.
  // Declared before finish_all so the exactly-one teeFinished report rides
  // the same terminal flush as the export contract.
  ImportRuntime* const rt = import_runtime_;
  TeeOutcome tee_outcome = TeeOutcome::kNone;
  std::string tee_identity;
  std::string tee_error;
  bool tee_report_emitted = false;
  auto emit_tee_report = [&]() {
    if (rt == nullptr || tee_report_emitted) {
      return;
    }
    tee_report_emitted = true;
    if (teeFinished) {
      teeFinished(tee_outcome, tee_identity, tee_error);
    }
  };
  // Always emit allFetchesComplete on every exit path so the dialog clears
  // fetch_active (and re-enables Close). Doubles as the terminal flush for the
  // export contract: an export that never started reports Skipped (the pull's
  // own error reporting already covers the cause) and releases its
  // reservation. The tee report flushes here too (before allFetchesComplete).
  auto finish_all = [&]() {
    if (!save_directory.empty() && !save_result_emitted) {
      discard_partial();
      emit_save_result(
          McapSaveResult{McapSaveStatus::Skipped, {}, "download ended before the export could start"});
    }
    emit_tee_report();
    if (allFetchesComplete) {
      allFetchesComplete(group_name);
    }
  };
  // The ONE copy step both export shapes share (the single-encoder rule):
  // copy `source` into the reserved `.mcap.partial` (ours by exclusive
  // reservation). What happens NEXT deliberately diverges: the Complete path
  // publishes it (atomic rename to the final name), the cancel path RETAINS
  // it as the readable export partial — keep that divergence explicit at the
  // call sites, never inside this helper.
  auto copy_to_export_partial = [&](const std::filesystem::path& source, std::string* error) {
    std::error_code ec;
    std::filesystem::copy_file(source, save_paths->partial_path,
                               std::filesystem::copy_options::overwrite_existing, ec);
    if (ec) {
      if (error != nullptr) {
        *error = "could not copy the cached session to '" + save_paths->partial_path.string() +
                 "': " + ec.message();
      }
      return false;
    }
    return true;
  };
  // Fulfill a requested export as a byte COPY of `source`, PUBLISHED under
  // the reserved final name (copy into the reserved partial, then atomic
  // rename). Emits exactly one Complete/Failed result.
  auto export_by_copy = [&](const std::filesystem::path& source) {
    if (!save_paths.has_value()) {
      return;  // reservation already failed and was reported
    }
    std::string copy_error;
    if (!copy_to_export_partial(source, &copy_error)) {
      discard_partial();
      emit_save_result(McapSaveResult{McapSaveStatus::Failed, {}, copy_error});
      return;
    }
    std::error_code ec;
    std::filesystem::rename(save_paths->partial_path, save_paths->final_path, ec);
    if (ec) {
      discard_partial();
      emit_save_result(McapSaveResult{
          McapSaveStatus::Failed, {},
          "could not finalize MCAP '" + save_paths->final_path.string() + "': " + ec.message()});
      return;
    }
    emit_save_result(McapSaveResult{McapSaveStatus::Complete, save_paths->final_path.string(), {}});
  };
  // Emit pullFinished for EVERY requested topic with the same outcome (used for
  // early bail-outs: not connected / open failed / no topics could bind).
  auto finish_all_topics = [this, &group_name, &topic_names](bool ok, const std::string& error) {
    for (const auto& t : topic_names) {
      if (pullFinished) {
        pullFinished(group_name, t, ok, error);
      }
    }
  };

  if (topic_names.empty() || sequence_names.empty()) {
    finish_all();
    return;
  }

  // Allocate + RESERVE both names before touching the network. A non-empty
  // save directory deliberately bypasses the count-only SessionCache below:
  // the cache holds no raw payloads from which a new MCAP could be
  // reconstructed. A bad export destination costs the user the EXPORT, never
  // the download — report and pull without a tee.
  if (!save_directory.empty()) {
    std::string path_error;
    save_paths = prepareMcapOutputPaths(
        std::filesystem::path(save_directory), sequence_names, utcTimestampForFilename(), &path_error);
    if (!save_paths.has_value()) {
      emit_save_result(McapSaveResult{McapSaveStatus::Failed, {}, path_error});
    }
  }

  if (!host_provider_) {
    finish_all_topics(false, "toolbox host not bound");
    finish_all();
    return;
  }

  if (!backend_ || conn_uri_.empty()) {
    finish_all_topics(false, "not connected");
    finish_all();
    return;
  }

  // ---- descriptor identity (RUNTIME mode) -----------------------------------
  // The canonical SourceDescriptor over the EXACT fetch tuple this pull sends
  // to OpenFresh below: `sequence_names` ARE the wire s3_keys (key-addressed
  // wire v2 — see the OpenSessionParams construction), `topic_names` the wire
  // topic list, the raw ns window ("0"/"0" = whole range) and the
  // include_latched flag the request carries. display_name is excluded from
  // identity by construction (spec §4).
  constexpr bool kIncludeLatched = true;  // mirrored into params.include_latched below
  std::string canonical_descriptor_json;
  std::string display_descriptor_json;  // toSourceDescriptorJson — the D6 promotion payload
  if (rt != nullptr) {
    SourceDescriptor descriptor;
    descriptor.version = 1;
    descriptor.kind = "mcap-cloud-session";
    descriptor.server_uri = conn_uri_;
    descriptor.s3_keys = sequence_names;
    descriptor.topics = topic_names;
    descriptor.start_ns = start_ns;
    descriptor.end_ns = end_ns;
    descriptor.include_latched = kIncludeLatched;
    descriptor.display_name = group_name;
    canonical_descriptor_json = canonicalSourceDescriptorJson(descriptor);
    display_descriptor_json = toSourceDescriptorJson(descriptor);
    tee_identity = descriptorIdentity(descriptor);
  }

  // ---- SessionCache HIT path (Slice 8): ZERO transport ----------------------
  // Compute the SessionKey over the EXACT logical selection (server_uri,
  // sequence_names[], topics[], time_range). Keyed on `sequence_names` directly
  // (the stable s3 keys) rather than any wire-resolved file_id: post-M6 the
  // catalog is rebuilt out of process and every rebuild RENUMBERS rowids, so a
  // file_id captured now could be silently reassigned to a DIFFERENT file by
  // the time this cache entry is looked up again — a hash/equality HIT on a
  // stale numeric id would replay the WRONG file's cached counts (see
  // session_key.hpp's header comment). `sequence_names` is always available
  // here (no resolve, no MISS fallthrough needed). A HIT requires the cached
  // dataset to STILL exist in the host.
  //
  // LEGACY mode: a requested export bypasses the count-only HIT (no raw bytes
  // to reconstruct a file from). RUNTIME mode applies the spec §6.1 rules
  // instead: memory hit + VALID disk cache -> serve from memory and satisfy
  // any export by COPY; memory hit + missing/invalid disk -> EVICT the memory
  // entry and fall through to a normal network refetch (never re-tee from
  // memory — the in-memory cache stores counts, not bytes).
  const PJ::cloud::SessionKey session_key = PJ::cloud::computeSessionKey(
      conn_uri_, sequence_names, topic_names, {start_ns, end_ns}, kIncludeLatched);
  SessionCache& session_cache = (rt != nullptr) ? rt->sessionCache() : session_cache_;
  bool refetch_after_disk_miss = false;
  if (rt != nullptr || !save_paths.has_value()) {
    const auto& exists = dataset_exists_
                             ? dataset_exists_
                             : SessionCache::ExistencePredicate(
                                   [this](const CachedSession& entry) { return datasetExistsInHost(entry); });
    if (serveFromMemoryCache(rt, session_cache, session_key, tee_identity, display_descriptor_json,
                             group_name, topic_names, save_directory, exists, export_by_copy,
                             &tee_outcome, &refetch_after_disk_miss)) {
      finish_all();
      return;
    }
  }

  // Reset the per-download dataset handle so this pull creates exactly one.
  {
    std::lock_guard<std::mutex> lock(fetch_dataset_mu_);
    fetch_dataset_ = std::nullopt;
  }

  // ---- cache-tee phase 1 (RUNTIME mode): locks BEFORE any network -----------
  // Registry ticket + cache cleanup + exclusive MaterializeLock + under-lock
  // corruption deletion. Acquired here so the CacheTee's RAII cleanup covers
  // EVERY exit below (connect failure, open-session failure, empty plan,
  // exceptions): the cache partial never survives and the lock is always
  // released. A begin failure (lock busy, unusable root) drops the tee — the
  // ingest continues (§9.6) — and, because the cache is the sole encoder,
  // fails a requested export with the actionable cause.
  std::unique_ptr<CacheTee> tee;
  // Publish/retire the tee for requestCancel() (frees a backpressure-blocked
  // producer). Same discipline as backend_session_for_cancel_: always under
  // cancel_mu_ (setTeeForCancel), always retired BEFORE the owning pointer
  // resets; the scope guard (declared AFTER `tee`, so it unwinds first)
  // covers every exit.
  auto set_tee_cancel_hook = [this](CacheTee* hook) { setTeeForCancel(hook); };
  struct TeeCancelHookGuard {
    FetchWorker* worker;
    ~TeeCancelHookGuard() { worker->setTeeForCancel(nullptr); }
  } tee_cancel_hook_guard{this};
  if (rt != nullptr) {
    tee = std::make_unique<CacheTee>(*rt);
    std::string begin_error;
    if (!tee->begin(tee_identity, &begin_error)) {
      tee.reset();
      tee_outcome = TeeOutcome::kFailed;
      tee_error = begin_error;
      if (save_paths.has_value()) {
        discard_partial();
        emit_save_result(McapSaveResult{McapSaveStatus::Failed, {}, begin_error});
      }
    } else {
      set_tee_cancel_hook(tee.get());
    }
  }

  // FAIR admission (adversarial F10): ONE pull at a time, FIFO, acquired
  // BEFORE any session/connection exists so a queued pull holds no remote
  // resources while waiting; held (RAII) for the whole pull, including the
  // on_dataset host-lock release window. Runtime mode only — legacy mode has
  // a single worker by construction.
  std::optional<ImportRuntime::AdmissionTicket> admission;
  if (rt != nullptr) {
    admission = rt->acquireAdmission(
        [this]() { return cancel_flag_.load(std::memory_order_relaxed); },
        std::chrono::milliseconds(50));
    if (!admission.has_value()) {
      finish_all_topics(false, "cancelled");
      finish_all();
      return;
    }
  }

  // Open a FRESH session connection (NOT the browse socket): a cancelled or
  // failed session never poisons the next download, and the browse path stays
  // responsive. Mirrors pj_cloud_source's per-session connect. On connect
  // failure every topic fails.
  std::unique_ptr<BackendConnection> session_owned;
  try {
    session_owned =
        std::make_unique<BackendConnection>(conn_uri_, conn_cert_path_, conn_api_key_, conn_allow_insecure_);
  } catch (...) {
    finish_all_topics(false, "failed to create session connection");
    finish_all();
    return;
  }
  BackendConnection* session_backend = session_owned.get();

  // Expose the session backend for requestCancel() BEFORE the blocking
  // connect()/openSessionFresh() calls below — registering it only ahead of the
  // download loop (the original placement) left the OpenSession wait
  // uncancellable from the GUI, reintroducing the exact 120 s stall the
  // sendAndWait wake predicate exists to fix (review-caught, both reviewers).
  // Scope guard: cleared under cancel_mu_ on EVERY exit path, and declared
  // after session_owned so it unwinds first — requestCancel() can never
  // dereference the destroyed connection.
  struct CancelHookGuard {
    FetchWorker* worker;
    ~CancelHookGuard() { worker->setSessionForCancel(nullptr); }
  } cancel_hook_guard{this};
  setSessionForCancel(session_backend);
  // A cancel latched BEFORE the hook was published could not reach the wire;
  // honor it cooperatively before any blocking call. (After publication a
  // cancel reaches cancelSession() directly, and BackendConnection latches it
  // for the connection's lifetime — openSessionFresh deliberately does NOT
  // reset the flag, so no arming-order race can swallow it.)
  if (cancel_flag_.load(std::memory_order_relaxed)) {
    finish_all_topics(false, "cancelled");
    finish_all();
    return;
  }

  std::string connect_err;
  if (!session_owned->connect(&connect_err)) {
    // A cancel is the CAUSE, not a symptom: report it as such instead of the
    // transport's view of the aborted handshake ("no response…"/socket text).
    const bool was_cancelled = cancel_flag_.load(std::memory_order_relaxed);
    finish_all_topics(false, was_cancelled
                                 ? std::string("cancelled")
                                 : (connect_err.empty() ? std::string("session connect failed")
                                                        : connect_err));
    finish_all();
    return;
  }
  // connect()'s handshake wait does not wake on cancel — a cancel during it
  // latched the backend flag but had nothing to interrupt; honor it now
  // instead of opening a session that would only fail as "cancelled" later.
  if (cancel_flag_.load(std::memory_order_relaxed)) {
    finish_all_topics(false, "cancelled");
    finish_all();
    return;
  }

  // Key-addressed OpenFresh (wire v2): the sequence names ARE the durable
  // s3_keys, so they go into the request verbatim — no list -> file_id
  // resolution round trip, no staleness window. For a stitched selection ALL
  // keys go into ONE OpenFresh (the server stitches them into one continuous
  // logical stream) and the SERVER is authoritative for unknown keys
  // (ERROR_NOT_FOUND naming the key, surfaced verbatim below).
  OpenSessionParams params;
  params.s3_keys = sequence_names;
  params.topic_names = topic_names;
  if (start_ns != 0 || end_ns != 0) {
    params.start_ns = start_ns;
    params.end_ns = end_ns;
  }
  // Latched/transient-local replay: deliver map/costmap/static-pose topics'
  // last value even when the time window opens after they were published once at
  // the start (otherwise they vanish with "no messages in the selected time
  // range"). GUI default-on; harmless without a window (server ignores it).
  // ONE constant feeds this flag AND the descriptor identity above — the
  // identity must name the request actually sent.
  params.include_latched = kIncludeLatched;

  // Phase feedback: opening a session does real server-side storage work
  // (chunk-index loads scale with the stitched file count and the WAN), and
  // until the response lands NO byte flows — without this the GUI's status
  // froze at "0.00 MiB/s" for up to minutes with zero explanation.
  if (pullPhase) {
    pullPhase(
        "Opening session: " + std::to_string(params.s3_keys.size()) + " file(s), " +
        std::to_string(topic_names.size()) + " topic(s) - waiting for the server's plan");
  }

  SessionInfo session_info;
  std::string open_err;
  if (!session_backend->openSessionFresh(params, &session_info, &open_err)) {
    finish_all_topics(false, open_err.empty() ? std::string("failed to open session") : open_err);
    finish_all();
    return;
  }

  if (pullPhase) {
    const double est_mib = static_cast<double>(session_info.estimated_chunk_bytes) / (1024.0 * 1024.0);
    char est[64];
    std::snprintf(est, sizeof(est), "~%.1f MiB", est_mib);
    pullPhase(
        "Session opened: " + std::to_string(session_info.topics.size()) + " topic(s), " + est +
        " estimated - downloading");
  }

  // Hand the numeric pre-flight estimate to the dialog for the byte-based
  // progress percentage (pullPhase above only carries it as prose).
  if (pullEstimate) {
    pullEstimate(session_info.estimated_chunk_bytes);
  }

  // Map session topic_id -> requested topic name so per-topic pullFinished /
  // pullProgress address the user-facing names.
  std::unordered_map<std::uint32_t, std::string> name_by_id;
  for (const auto& t : session_info.topics) {
    name_by_id.emplace(t.topic_id, t.topic_name);
  }

  // EMPTY-PLAN contract (spec slice 2): the server answers OpenSession with an
  // EMPTY topic dictionary when no selected topic has any message in the
  // selected files/time range — common when zero-message catalog topics are
  // selected (75 of 171 on the S3-use-case staging bags) or the slider window
  // misses the data. That is "nothing to download", NOT a decoder problem:
  // report it as such (the old path fell through to a bogus per-topic
  // "no decoder for topic").
  if (session_info.topics.empty()) {
    if (errorOccurred) {
      std::string window = "whole range";
      if (params.start_ns.has_value() && params.end_ns.has_value()) {
        window = "time range " + std::to_string(*params.start_ns) + ".." + std::to_string(*params.end_ns) + " ns";
      }
      errorOccurred(
          "MCAP Cloud: the server returned an EMPTY plan (no data matches the selection): " +
          std::to_string(params.s3_keys.size()) + " file(s), " + std::to_string(topic_names.size()) +
          " topic(s) requested, " + window +
          ". The selected topics have no recorded messages in that selection.");
    }
    finish_all_topics(false, "no messages in the selected time range");
    finish_all();
    return;
  }

  // ---- cache-tee phase 2 (RUNTIME mode): the SINGLE encoder opens ----------
  // BEFORE the hasDecodable early-exit below (§9.0 raw-tee parser
  // independence: a no-decoder session must still materialize the cache
  // file) and BEFORE the first message: free-space check against the
  // server's pre-flight estimate, exclusive 0600 sink at the per-process
  // partial, the embedded canonical-descriptor provenance record, and the
  // bounded writer thread. An open failure drops the tee (ingest continues,
  // §9.6) and fails a requested export — the cache is the sole encoder.
  if (tee != nullptr) {
    std::string open_error;
    if (!tee->openWriter(session_info, canonical_descriptor_json,
                         session_info.estimated_chunk_bytes, &open_error)) {
      set_tee_cancel_hook(nullptr);
      tee.reset();
      tee_outcome = TeeOutcome::kFailed;
      tee_error = open_error;
      if (save_paths.has_value()) {
        discard_partial();
        emit_save_result(McapSaveResult{McapSaveStatus::Failed, {}, open_error});
      }
    }
  }

  ParserIngestDriver driver;
  PJ::sdk::ToolboxHostView host = host_provider_();

  if (!runtime_host_provider_) {
    finish_all_topics(false, "no runtime host provider");
    finish_all();
    return;
  }
  PJ::ToolboxRuntimeHostView runtime = runtime_host_provider_();

  // Hold the host-write lock across the WHOLE bind + decode loop: the toolbox
  // DataWriter has no internal mutex and this worker runs host writes on its
  // own thread. RUNTIME mode locks the ImportRuntime's SHARED mutex — the
  // one that also serializes future provider jobs against this worker.
  std::unique_lock<std::mutex> write_lock(rt != nullptr ? rt->hostWriteMutex() : host_write_mu_);

  auto ds = datasetForFetch(host, group_name);
  if (!ds) {
    write_lock.unlock();
    finish_all_topics(false, ds.error());
    finish_all();
    return;
  }

  IngestBindResult bind = driver.bindSession(runtime, *ds, session_info);

  // Per-topic decodability maps are built AFTER the download (see below):
  // bindings are now created lazily on each topic's first message, so a
  // bind failure only becomes known mid-stream.
  std::unordered_map<std::uint32_t, bool> decodable_by_id;
  std::unordered_map<std::uint32_t, std::string> skip_reason_by_id;

  // Debug surface for REAL parser-bind failures: one compact notification with
  // "topic (type): host reason" lines (capped), so a missing/incompatible
  // parser is diagnosable from the bell instead of an opaque per-topic tally.
  if (errorOccurred && !bind.errors.empty()) {
    constexpr std::size_t kMaxBindErrorLines = 8;
    std::string detail = "MCAP Cloud: " + std::to_string(bind.errors.size()) + " of " +
                         std::to_string(session_info.topics.size()) + " session topics failed to bind a parser:";
    for (std::size_t i = 0; i < bind.errors.size() && i < kMaxBindErrorLines; ++i) {
      detail += "\n  " + bind.errors[i];
    }
    if (bind.errors.size() > kMaxBindErrorLines) {
      detail += "\n  ... and " + std::to_string(bind.errors.size() - kMaxBindErrorLines) + " more";
    }
    errorOccurred(std::move(detail));
  }

  // No-decoder early exit — ONLY when no cache tee is running. With a live
  // tee the download proceeds regardless (§9.0: a raw cache file must not
  // require a bound parser); the post-download per-topic classification still
  // reports every topic's no-parser reason.
  if (!driver.hasDecodable() && tee == nullptr) {
    write_lock.unlock();
    // Surface each requested topic's failure with its own reason where known.
    // A topic missing from the session dictionary means the server's plan had
    // no data for it (NOT a decoder problem); a topic present but unbound
    // carries the bind error from the host.
    for (const auto& t : topic_names) {
      std::string reason = "no messages in the selected time range";
      for (const auto& [tid, name] : name_by_id) {
        if (name == t) {
          reason = skip_reason_by_id.count(tid) ? skip_reason_by_id.at(tid) : "no parser bound for topic";
          break;
        }
      }
      if (pullFinished) {
        pullFinished(group_name, t, false, reason);
      }
    }
    finish_all();
    return;
  }

  // LEGACY-mode direct export writer only: in RUNTIME mode the cache is the
  // sole encoder and the export is fulfilled by COPY at the terminal below.
  std::unique_ptr<SessionMcapWriter> mcap_writer;
  std::string mcap_write_error;
  if (rt == nullptr && save_paths.has_value()) {
    mcap_writer = std::make_unique<SessionMcapWriter>();
    if (!mcap_writer->open(save_paths->partial_path, session_info, &mcap_write_error)) {
      // Export is SECONDARY: report + drop the tee, keep downloading. No path
      // in the result: a failed open leaves no file behind (the writer removes
      // its own debris; discard covers the stream-open-failure reservation).
      mcap_writer.reset();
      discard_partial();
      emit_save_result(McapSaveResult{McapSaveStatus::Failed, {}, mcap_write_error});
      mcap_write_error.clear();
    }
  }

  // #470 progress surface (spec §13 stage 2): make the progressive import
  // HOST-visible — title-bar strip, per-dataset ingest lifecycle, cooperative
  // stop — through the dataset-ingest view over the already-bound context
  // (SDK 0.20.0's wider facade on the same create_parser_ingest fat pointer).
  // Best-effort by design: a host without the progress slots REFUSES
  // progressStart, and on such a host progressUpdate's false return means
  // "unsupported", never "user cancelled" — so the cancel interpretation in
  // the pull loop below is gated on host_progress_active. Stream-thread
  // tagged calls, driven from this worker thread like every other host call.
  bool host_progress_active = false;
  if (driver.hasDecodable()) {
    host_progress_active =
        driver.datasetIngest()
            .progressStart("MCAP Cloud: " + group_name, session_info.approximate_messages,
                           /*cancellable=*/true)
            .has_value();
  }
  std::uint64_t transport_messages_seen = 0;

  // Host-stop WATCHDOG (extracted to HostStopWatchdog above — shared with the
  // direct pull): scoped strictly to the download; stop()ed before the
  // terminal classification and finalize(), so the dataset-ingest view
  // outlives it; the destructor is the exception-safe join.
  HostStopWatchdog host_stop_watchdog(driver, host_progress_active, [this]() { requestCancel(); },
                                      watchdog_thread_factory_for_test_);

  // Progress throttle: emit pullProgress at most ~10 Hz per topic.
  std::unordered_map<std::uint32_t, std::int64_t> bytes_by_id;
  std::unordered_map<std::uint32_t, std::chrono::steady_clock::time_point> last_emit;
  // pullProgress is throttled PER TOPIC (last_emit is keyed by topic id). The
  // wire-byte figure is session-wide, so it gets its OWN single throttle stamp —
  // otherwise it would re-emit the same value once per topic that trips its own
  // window (N GUI events/window instead of one).
  std::chrono::steady_clock::time_point last_wire_emit{};
  std::chrono::steady_clock::time_point last_host_progress_emit{};
  const auto kProgressInterval = std::chrono::milliseconds(100);

  // (backend_session_for_cancel_ was published before connect/open above;
  // CancelHookGuard clears it on every exit path.)

  // Surface "Resuming (attempt N/max)…" through the worker->dialog path on each
  // reconnect attempt during a mid-pull transport drop.
  session_backend->setResumeHint([this, group_name](unsigned attempt, unsigned max) {
    if (pullResuming) {
      pullResuming(group_name, attempt, max);
    }
  });

  // downloadSessionResumable() survives a mid-stream transport drop: it
  // reconnects + OpenResume{last_received_seq} and CONTINUES the SAME handler
  // below (ParserIngestDriver keeps appending; no dupes by the seq contract).
  SessionStats stats = session_backend->downloadSessionResumable(
      session_info, [&](const DecodedMessage& m) -> bool {
        if (cancel_flag_.load(std::memory_order_relaxed)) {
          return false;  // downloadSession sends the wire Cancel + returns
        }
        // Save the transport record before host parsing. Parser rejection is an
        // import concern and must not make the reconstructed MCAP lossy.
        if (mcap_writer && !mcap_writer->write(m, &mcap_write_error)) {
          // The symmetric rule: a disk failure on the export tee must not
          // abort the download or truncate the host import. Without a
          // finalized footer the partial is unreadable garbage — remove it
          // rather than advertise it; the import continues tee-less.
          std::string ignored;
          (void)mcap_writer->close(&ignored);
          mcap_writer.reset();
          discard_partial();
          emit_save_result(McapSaveResult{McapSaveStatus::Failed, {}, mcap_write_error});
          mcap_write_error.clear();
        }
        // RUNTIME mode: hand the record to the cache tee's bounded queue
        // (owned copy; BLOCKS briefly under backpressure when the disk lags —
        // the wait is cancellable via requestCancel -> CacheTee::requestAbort).
        // The §9.6 failure/cancel discipline lives in teeEnqueueFailed (one
        // place for both pulls); on a genuine tee failure the export also
        // fails with the raw cause — the cache is the sole encoder, there is
        // no independent export stream to fall back to.
        if (std::string enqueue_error; teeEnqueueFailed(tee, m, &enqueue_error)) {
          tee_outcome = TeeOutcome::kFailed;
          tee_error = std::move(enqueue_error);
          if (save_paths.has_value()) {
            discard_partial();
            emit_save_result(McapSaveResult{McapSaveStatus::Failed, {}, tee_error});
          }
        }
        (void)driver.decode(m);  // best-effort; drops + counts on failure

        // Throttled per-topic progress.
        auto& acc = bytes_by_id[m.topic_id];
        acc += static_cast<std::int64_t>(m.payload.size());
        const auto now = std::chrono::steady_clock::now();
        auto& last = last_emit[m.topic_id];
        if (now - last >= kProgressInterval) {
          last = now;
          if (pullProgress) {
            if (auto it = name_by_id.find(m.topic_id); it != name_by_id.end()) {
              pullProgress(it->second, acc);
            }
          }
        }
        // Live network figure (compressed wire bytes) — session-wide, so it rides
        // its own throttle stamp rather than the per-topic one above.
        if (pullWireBytes && now - last_wire_emit >= kProgressInterval) {
          last_wire_emit = now;
          pullWireBytes(static_cast<std::int64_t>(session_backend->sessionWireBytesReceived()));
        }
        // Host progress tick (#470) — session-wide, own throttle stamp. The
        // step unit is TRANSPORT messages (matches approximate_messages, the
        // total announced to progressStart). A false progressUpdate return or
        // a host stop request cancels cooperatively via the same return-false
        // -> wire-Cancel path the dialog cancel uses; both signals are only
        // honored when the host accepted progressStart (see above).
        ++transport_messages_seen;
        if (host_progress_active && now - last_host_progress_emit >= kProgressInterval) {
          last_host_progress_emit = now;
          if (!driver.datasetIngest().progressUpdate(transport_messages_seen) ||
              driver.datasetIngest().isStopRequested()) {
            return false;
          }
        }
        return true;
      });

  host_stop_watchdog.stop();

  // ONE atomic terminal-outcome boundary (review-caught): sample cancel_flag_
  // exactly ONCE, here, and derive every terminal decision from it — the
  // export promotion gate, the progressFinish pairing, and the per-topic
  // cancelled classification below. Three independent loads let a cancel
  // racing in during (potentially long) MCAP finalization classify the SAME
  // pull as completed on one surface and cancelled on another. A cancel
  // arriving after this boundary is consistently ignored everywhere.
  const bool cancelled_after_download = cancel_flag_.load(std::memory_order_relaxed);

  // ---- cache-tee terminal (RUNTIME mode; mutually exclusive with the legacy
  // mcap_writer block below) — the retention rules live in finishCacheTee.
  if (tee != nullptr) {
    const TeeTerminalOutcome terminal = finishCacheTee(
        *tee, stats, cancelled_after_download, save_paths, emit_save_result, discard_partial,
        export_by_copy, copy_to_export_partial);
    tee_outcome = terminal.outcome;
    tee_error = terminal.error;
    set_tee_cancel_hook(nullptr);
    tee.reset();
  }

  if (mcap_writer) {
    // The import outcome (stats/topic ledger) is NEVER touched here: any
    // export/finalize failure is a purely local problem — the mcap_save_failed
    // latch keeps the panel open so the notification stays visible, and only
    // the export is reported failed.
    std::string close_error;
    const bool close_ok = mcap_writer->close(&close_error);
    McapSaveResult result{McapSaveStatus::Failed, save_paths->partial_path.string(), close_error};
    if (!close_ok) {
      // A finalize failure means no footer/summary — the file is NOT a
      // readable MCAP. Remove it; report no path.
      discard_partial();
      result.path.clear();
    } else if (stats.eos == SessionEos::Complete && !cancelled_after_download) {
      std::error_code rename_error;
      std::filesystem::rename(save_paths->partial_path, save_paths->final_path, rename_error);
      if (rename_error) {
        result.error =
            "could not finalize MCAP '" + save_paths->final_path.string() + "': " + rename_error.message();
      } else {
        result = McapSaveResult{McapSaveStatus::Complete, save_paths->final_path.string(), {}};
      }
    } else {
      // DELIBERATE retention: a user-requested export KEEPS the readable
      // partial after a cancellation/transport drop (close() above finalized
      // footer + summary). The cache tee (the RUNTIME-mode terminal above)
      // does the OPPOSITE — its partials never survive
      // (docs/canonical-layout-import.md §10); in that mode the export
      // partial that survives is a byte COPY of the cache partial, taken
      // BEFORE the cache partial is deleted. Do not "align" the two
      // retentions — they are opposite by design.
      result.status = McapSaveStatus::Partial;
      // Wording derives from the SAME terminal snapshot as every other
      // surface (re-verify-caught): a Complete-but-cancelled-at-the-boundary
      // pull must say "cancelled" here too, matching the progress/dialog
      // classification.
      result.error =
          !stats.error.empty()
              ? stats.error
              : ((stats.eos == SessionEos::Cancelled || cancelled_after_download)
                     ? "download cancelled"
                     : "download ended before completion");
    }
    emit_save_result(std::move(result));
  }

  // Pair the host progress sequence on the completed path: one final
  // authoritative sample + progressFinish, on this same worker thread.
  // Cancel/failure paths deliberately skip progressFinish — the
  // releaseDatasetIngest inside driver.finalize() pairs the host's
  // ingest-finished callback either way (#470 pairs release-without-finish),
  // without painting an aborted pull as a completed progress sequence.
  if (host_progress_active && stats.eos == SessionEos::Complete && !cancelled_after_download) {
    (void)driver.datasetIngest().progressUpdate(transport_messages_seen);
    driver.datasetIngest().progressFinish();
  }

  // Seal the host-side parser writes (releaseDatasetIngest -> flushAll) while
  // still inside the host-write critical section, so the GUI-thread
  // notifyDataChanged -> catalog rebuild that follows sees every row and
  // every object topic.
  driver.finalize();

  // Final per-topic progress flush + per-topic completion.
  const auto counts = driver.decodedCounts();
  const auto errors = driver.errorCounts();
  const bool cancelled = (stats.eos == SessionEos::Cancelled) || cancelled_after_download;
  const bool session_failed = (stats.eos == SessionEos::Error || stats.eos == SessionEos::Unset);

  write_lock.unlock();  // release before invoking the GUI-thread callbacks

  // Final authoritative wire-byte total (the whole pull, all resume legs).
  if (pullWireBytes) {
    pullWireBytes(static_cast<std::int64_t>(stats.wire_bytes_received));
  }

  // Snapshot the POST-download per-topic outcomes: lazy binding means a topic's
  // decodable flag can flip to false at its first message (bind failure).
  for (const auto& [tid, dec] : driver.decoders()) {
    decodable_by_id[tid] = dec.decodable;
    if (!dec.decodable) {
      skip_reason_by_id[tid] = dec.skip_reason;
    }
  }

  for (const auto& t : topic_names) {
    // Resolve the topic_id for this requested name.
    std::optional<std::uint32_t> tid_opt;
    for (const auto& [tid, name] : name_by_id) {
      if (name == t) {
        tid_opt = tid;
        break;
      }
    }
    // Final progress sample.
    if (pullProgress && tid_opt) {
      if (auto bit = bytes_by_id.find(*tid_opt); bit != bytes_by_id.end()) {
        pullProgress(t, bit->second);
      }
    }
    bool ok = false;
    std::string error;
    if (cancelled) {
      // The BATCH outcome wins (re-verify-caught): checked before the
      // per-topic missing/undecodable reasons so a cancelled batch reports
      // every topic "cancelled" — otherwise an all-bindings-failed batch
      // that was then cancelled reported no "cancelled" topic at all, the
      // dialog never latched, and it summarized "Fetch failed…" while the
      // progress/export surfaces said cancelled. Per-topic bind-failure
      // detail is not lost: the bell notification surfaced it at bind time.
      // Treated as not-ok but the dialog tags it "Cancelled" (kept OUT of
      // the failure tally).
      error = "cancelled";
    } else if (!tid_opt) {
      // Requested topic absent from the session dictionary: the server's plan
      // found no message for it in the selected files/time range (zero-message
      // catalog topics and windows that miss the data both land here).
      error = "no messages in the selected time range";
    } else if (decodable_by_id.count(*tid_opt) && !decodable_by_id.at(*tid_opt)) {
      error = skip_reason_by_id.count(*tid_opt) ? skip_reason_by_id.at(*tid_opt) : "no parser bound for topic";
    } else if (session_failed) {
      error = stats.error.empty() ? "session ended without terminal Eos" : stats.error;
    } else {
      const std::uint64_t decoded = counts.count(*tid_opt) ? counts.at(*tid_opt) : 0;
      const std::uint64_t errc = errors.count(*tid_opt) ? errors.at(*tid_opt) : 0;
      if (errc > 0) {
        // Decode errors (parser push/bind failures) mean the host holds FEWER
        // rows than the server sent — logical equality is violated. Report the
        // topic as PARTIAL (not-ok) so the ledger surfaces it and the user is
        // not told a lossy import "Done". The count of dropped messages is in
        // the message; the cache store below is suppressed for the whole
        // session so a repeat fetch does not become a false zero-transport HIT.
        error = std::to_string(errc) + " message(s) failed to decode (partial import of " +
                std::to_string(decoded) + " ok)";
      } else {
        ok = true;
      }
    }
    if (pullFinished) {
      pullFinished(group_name, t, ok, std::move(error));
    }
  }

  // Any per-topic decode error means the host holds FEWER rows than the server
  // sent, so a cached counts entry would undercount and a future fetch would be
  // a false zero-transport HIT that never re-pulls the missing rows. Suppress
  // the cache store for the WHOLE session in that case (mirrors the cancel /
  // error suppression below).
  std::uint64_t total_decode_errors = 0;
  for (const auto& [tid, ec] : errors) {
    (void)tid;
    total_decode_errors += ec;
  }

  // ---- SessionCache store: COMPLETE-only (Slice 8) --------------------------
  // Store the per-topic counts ONLY on a clean COMPLETE download (no cancel, no
  // error/Unset, no decode errors, and at least one decodable topic — a
  // tee-only download-through holds no rows the datastore could serve a HIT
  // from). cancel / error / no-terminal-Eos / partial decode -> NO entry (no
  // half-cached state). The key is the SAME tuple the HIT path computes —
  // `sequence_names` directly, never a resolved file_id (post-M6 rowids
  // renumber across catalog rebuilds; see session_key.hpp). The datastore now
  // owns the decoded rows under group_name; the cache records only counts
  // metadata (plus the stage-4 dataset/tee state) so a repeat fetch is a
  // zero-transport HIT.
  if (stats.eos == SessionEos::Complete && !cancelled && !session_failed && total_decode_errors == 0 &&
      driver.hasDecodable()) {
    storeCompletedSessionEntry(session_cache, session_key, group_name, conn_uri_, topic_names,
                               name_by_id, counts, tee_identity, tee_outcome,
                               refetch_after_disk_miss);
  }

  // ---- promotion at completion (stage-4 PR-3, D6 — the authoring dual
  // path, spec §6.1/§9.8): a finalized cache file + a live eager dataset =
  // hand the artifact to the host to become a stock file-backed source.
  // AFTER the store above so a re-entrant on_result finds the entry.
  // Fire-and-forget on this interactive path: the shared result state
  // (captured by the callback) updates the entry's promotion_state whenever
  // the host settles; the worker never blocks on it. kFinalized implies a
  // clean COMPLETE (finishCacheTee), so no extra terminal gating is needed.
  if (rt != nullptr && tee_outcome == TeeOutcome::kFinalized) {
    PJ::DatasetId promoted_dataset = 0;
    {
      std::lock_guard<std::mutex> lock(fetch_dataset_mu_);
      promoted_dataset = fetch_dataset_.has_value() ? fetch_dataset_->id : 0;
    }
    if (promoted_dataset != 0) {
      (void)rt->promoteToFileSource(
          ImportRuntime::makePromotionRequest(promoted_dataset, tee_identity,
                                              rt->fileCache().pathFor(tee_identity),
                                              display_descriptor_json),
          &session_key);
    }
  }

  finish_all();
}

PullResult FetchWorker::pull(PullRequest request) {
  PullResult result;  // defaults: kFailed + empty error (filled on every path)

  // ---- self-contained input validation (no dialog, no connectAsync state) --
  ImportRuntime* const rt = request.runtime;
  if (rt == nullptr) {
    // Nothing to restore through: with no runtime there is no registry the
    // caller could have dropped a lease in.
    result.error = "direct pull requires an ImportRuntime";
    return result;
  }

  // Round-5 F1: no guard needed here anymore — request.lease_drop is the
  // move-only RAII token, declared AFTER request.ticket, so any exit or
  // exception before the tee captures it restores under the ticket by
  // member-destruction order alone.

  if (request.sequence_names.empty()) {
    result.error = "empty selection (no s3 keys)";
    return result;
  }
  if (!host_provider_ || !runtime_host_provider_) {
    result.error = "toolbox host not bound";
    return result;
  }
  if (request.group_name.empty()) {
    request.group_name = request.sequence_names.front();
  }
  auto cancelled_now = [this]() { return cancel_flag_.load(std::memory_order_relaxed); };
  auto finish_cancelled = [&result]() {
    result.terminal = PullTerminal::kCancelled;
    result.error = "cancelled";
  };

  // Reset the per-download dataset handle so this pull creates exactly one.
  {
    std::lock_guard<std::mutex> lock(fetch_dataset_mu_);
    fetch_dataset_ = std::nullopt;
  }

  // ---- cache-tee phase 1 (ALWAYS-ON for direct pulls): locks BEFORE any
  // network. Same discipline as the interactive path: the CacheTee's RAII
  // cleanup covers every exit below, a begin failure drops the tee and the
  // ingest continues (§9.6). Guard declared AFTER `tee` so it unwinds first.
  std::unique_ptr<CacheTee> tee = std::make_unique<CacheTee>(*rt);
  struct TeeCancelHookGuard {
    FetchWorker* worker;
    ~TeeCancelHookGuard() { worker->setTeeForCancel(nullptr); }
  } tee_cancel_hook_guard{this};
  {
    std::string begin_error;
    // The tee's NO-THROW prologue captures ticket, lock AND token before
    // anything that can throw — including on a FAILED begin, whose abort
    // still restores.
    if (!tee->begin(request.identity, &begin_error, std::move(request.ticket),
                    std::move(request.lock), std::move(request.lease_drop))) {
      tee.reset();
      result.tee_outcome = TeeOutcome::kFailed;
      result.tee_error = begin_error;
    } else {
      setTeeForCancel(tee.get());
    }
  }

  // FAIR admission (adversarial F10): same gate as the interactive pull —
  // ONE pull at a time, FIFO, acquired BEFORE any session/connection
  // resources exist (a queued job holds no remote session or inbox), held
  // (RAII) for the whole pull including the on_dataset host-lock release
  // window so the other producer cannot steal the turn.
  std::optional<ImportRuntime::AdmissionTicket> admission = rt->acquireAdmission(
      [this]() { return cancel_flag_.load(std::memory_order_relaxed); },
      std::chrono::milliseconds(50));
  if (!admission.has_value()) {
    finish_cancelled();
    return result;  // the tee's RAII cleanup deletes the partial
  }

  // ---- ONE session connection owned by the pull, published for cancel
  // BEFORE the blocking connect (D4: a cancel during the socket-open, Hello
  // or OpenSession wait must wake promptly — the open wait joins the cancel
  // predicate in buildAndOpenSocket, the RPC waits ride wake_on_cancel).
  std::unique_ptr<BackendConnection> session_owned;
  try {
    session_owned = std::make_unique<BackendConnection>(
        request.connection.uri, request.connection.credentials.cert_path,
        request.connection.credentials.api_key, request.connection.credentials.allow_insecure);
  } catch (...) {
    result.error = "failed to create session connection";
    return result;
  }
  BackendConnection* session_backend = session_owned.get();
  struct CancelHookGuard {
    FetchWorker* worker;
    ~CancelHookGuard() { worker->setSessionForCancel(nullptr); }
  } cancel_hook_guard{this};
  setSessionForCancel(session_backend);
  // A cancel latched BEFORE the hook was published could not reach the wire;
  // honor it cooperatively before any blocking call.
  if (cancelled_now()) {
    finish_cancelled();
    return result;
  }

  std::string connect_err;
  if (!session_owned->connect(&connect_err)) {
    if (cancelled_now()) {
      finish_cancelled();  // the cancel is the CAUSE, not a symptom
    } else {
      result.error = connect_err.empty() ? "session connect failed" : connect_err;
      // The §10 remediation hint, MACHINE-gated (never error-text sniffing):
      // the server rejected the Hello as AUTH_FAILED *and* the resolved
      // snapshot carried NO credential — missing-credential, not
      // wrong-credential (a presented-and-rejected key gets no hint: the
      // stored key is simply wrong, and "no stored credential" would lie).
      // F15: gate on PROVENANCE, not token bytes — a stored EMPTY
      // dev-anonymous token is a PRESENT credential (kStored), and telling
      // that user "no stored credential" would lie; only a genuinely absent
      // credential (kNone) earns the hint.
      if (session_backend->lastHelloErrorCode() == HelloErrorCode::kAuthFailed &&
          request.connection.credentials.api_key_source == TokenSource::kNone) {
        result.error +=
            "; no stored credential for this server — connect once in the MCAP Cloud toolbox "
            "or set MCAP_CLOUD_API_KEY (with MCAP_CLOUD_URL matching this origin)";
      }
    }
    return result;
  }
  if (cancelled_now()) {
    finish_cancelled();
    return result;
  }

  // Key-addressed OpenFresh (wire v2). include_latched comes FROM THE
  // DESCRIPTOR: the same field feeds the wire request and the identity the
  // caller computed, so the identity always names the request actually sent.
  OpenSessionParams params;
  params.s3_keys = request.sequence_names;
  params.topic_names = request.topic_names;
  if (request.start_ns != 0 || request.end_ns != 0) {
    params.start_ns = request.start_ns;
    params.end_ns = request.end_ns;
  }
  params.include_latched = request.include_latched;

  SessionInfo session_info;
  std::string open_err;
  if (!session_backend->openSessionFresh(params, &session_info, &open_err)) {
    if (cancelled_now()) {
      finish_cancelled();
    } else {
      result.error = open_err.empty() ? "failed to open session" : open_err;
    }
    return result;
  }
  if (session_info.topics.empty()) {
    // EMPTY-PLAN: for an import there is nothing to materialize — a clean,
    // actionable failure (the descriptor names data the server cannot serve).
    result.error = "the server returned an EMPTY plan: no data matches the descriptor selection";
    return result;
  }

  // Re-verify R2(a): the server's own pre-flight estimate is compared with
  // the effective byte ceiling BEFORE any byte streams — an import that is
  // predictably over budget is refused up front rather than billed for a
  // partial transfer it must then discard.
  if (request.max_transfer_bytes > 0 &&
      session_info.estimated_chunk_bytes > request.max_transfer_bytes) {
    result.terminal = PullTerminal::kFailed;
    result.byte_ceiling_exceeded = true;
    result.error = "server pre-flight estimate ~" +
                   std::to_string(session_info.estimated_chunk_bytes) +
                   " bytes exceeds the " + std::to_string(request.max_transfer_bytes) +
                   "-byte transfer ceiling — refused before streaming";
    return result;  // the tee's RAII cleanup releases locks; nothing was opened
  }

  std::unordered_map<std::uint32_t, std::string> name_by_id;
  for (const auto& t : session_info.topics) {
    name_by_id.emplace(t.topic_id, t.topic_name);
  }

  // ---- cache-tee phase 2: the SINGLE encoder opens BEFORE the first
  // message (and regardless of decodability — §9.0 raw-tee parser
  // independence). An open failure drops the tee; the ingest continues.
  if (tee != nullptr) {
    std::string open_error;
    if (!tee->openWriter(session_info, request.canonical_descriptor_json,
                         session_info.estimated_chunk_bytes, &open_error)) {
      setTeeForCancel(nullptr);
      tee.reset();
      result.tee_outcome = TeeOutcome::kFailed;
      result.tee_error = open_error;
    }
  }

  ParserIngestDriver driver;
  PJ::sdk::ToolboxHostView host = host_provider_();
  PJ::ToolboxRuntimeHostView runtime = runtime_host_provider_();

  // CANCEL-AWARE acquisition of the SHARED host-write mutex (quality review
  // IMPORTANT-1): the interactive runtime-mode pull holds this mutex across
  // its ENTIRE download, so a blocking lock here would wedge a provider job
  // — and its cancel/join/destroy — for minutes behind an unrelated fetch.
  // Mirror the provider's materialize-gate discipline: poll try_lock at a
  // short cadence and honor the cancel flag between attempts.
  constexpr auto kHostWriteLockPoll = std::chrono::milliseconds(50);
  auto lock_host_write_cancellable = [&](std::unique_lock<std::mutex>& lock) {
    while (!lock.try_lock()) {
      if (cancel_flag_.load(std::memory_order_relaxed)) {
        return false;
      }
      std::this_thread::sleep_for(kHostWriteLockPoll);
    }
    return true;
  };

  // Create the dataset under the SHARED host-write lock (the runtime mutex
  // serializes this pull against the interactive worker), then surface it
  // through datasetCreated OUTSIDE the lock: the PR-3 job forwards it as the
  // ABI on_dataset, which must precede any binding/publication/progress —
  // and host callback code must never run under our write lock.
  std::unique_lock<std::mutex> write_lock(rt->hostWriteMutex(), std::defer_lock);
  if (!lock_host_write_cancellable(write_lock)) {
    finish_cancelled();
    return result;  // the tee's RAII cleanup deletes the partial
  }
  auto ds = datasetForFetch(host, request.group_name);
  if (!ds) {
    write_lock.unlock();
    result.error = ds.error();
    return result;
  }
  result.dataset = *ds;
  write_lock.unlock();
  if (request.datasetCreated) {
    request.datasetCreated(*ds);
  }
  if (!lock_host_write_cancellable(write_lock)) {
    finish_cancelled();
    return result;
  }

  const IngestBindResult bind = driver.bindSession(runtime, *ds, session_info);

  if (!driver.hasDecodable() && tee == nullptr) {
    write_lock.unlock();
    std::string detail = "no parser bound for any selected topic and the cache tee is unavailable";
    if (!bind.errors.empty()) {
      detail += ": " + bind.errors.front();
    }
    result.error = std::move(detail);
    return result;
  }

  // #470 progress surface + host-stop watchdog, exactly as the interactive
  // pull (shared HostStopWatchdog; stop()ed before the terminal boundary).
  bool host_progress_active = false;
  if (driver.hasDecodable()) {
    host_progress_active =
        driver.datasetIngest()
            .progressStart("MCAP Cloud import: " + request.group_name, session_info.approximate_messages,
                           /*cancellable=*/true)
            .has_value();
  }
  std::uint64_t transport_messages_seen = 0;
  HostStopWatchdog host_stop_watchdog(driver, host_progress_active, [this]() { requestCancel(); },
                                      watchdog_thread_factory_for_test_);

  std::chrono::steady_clock::time_point last_host_progress_emit{};
  const auto kProgressInterval = std::chrono::milliseconds(100);
  bool byte_ceiling_exceeded = false;
  bool duration_ceiling_exceeded = false;
  const auto download_start = std::chrono::steady_clock::now();
  // R2(d): arm the hard session deadline so a SILENT/control-only stream
  // still stops at the duration ceiling (the frame wait is capped to it and
  // resume never retries past it); the duration latch below classifies.
  session_backend->setSessionDeadline(
      request.max_transfer_duration.count() > 0
          ? std::optional<std::chrono::steady_clock::time_point>(download_start +
                                                                 request.max_transfer_duration)
          : std::nullopt);

  SessionStats stats = session_backend->downloadSessionResumable(
      session_info, [&](const DecodedMessage& m) -> bool {
        if (cancel_flag_.load(std::memory_order_relaxed)) {
          return false;  // downloadSession sends the wire Cancel + returns
        }
        // §7 guard 3: the caller-imposed wire-byte ceiling. Latch the
        // DISTINCT cause BEFORE aborting — the sink-false return classifies
        // as SessionEos::Cancelled on the wire, and the terminal mapping
        // below must not report a resource-limit abort as a cancel.
        if (request.max_transfer_bytes > 0 &&
            session_backend->sessionWireBytesReceived() > request.max_transfer_bytes) {
          byte_ceiling_exceeded = true;
          return false;
        }
        // F12: the transfer-duration ceiling, same distinct-FAILED shape.
        if (request.max_transfer_duration.count() > 0 &&
            std::chrono::steady_clock::now() - download_start > request.max_transfer_duration) {
          duration_ceiling_exceeded = true;
          return false;
        }
        // Cache tee: the shared §9.6 discipline (teeEnqueueFailed) — a tee
        // FAILURE drops the tee and the ingest continues; a cancel-freed
        // enqueue keeps the tee alive for the single terminal boundary.
        if (std::string enqueue_error; teeEnqueueFailed(tee, m, &enqueue_error)) {
          result.tee_outcome = TeeOutcome::kFailed;
          result.tee_error = std::move(enqueue_error);
        }
        (void)driver.decode(m);  // best-effort; drops + counts on failure

        ++transport_messages_seen;
        if (request.onProgress) {
          request.onProgress(transport_messages_seen);
        }
        const auto now = std::chrono::steady_clock::now();
        if (host_progress_active && now - last_host_progress_emit >= kProgressInterval) {
          last_host_progress_emit = now;
          if (!driver.datasetIngest().progressUpdate(transport_messages_seen) ||
              driver.datasetIngest().isStopRequested()) {
            return false;
          }
        }
        return true;
      });

  host_stop_watchdog.stop();

  // ONE atomic terminal-outcome boundary (mirrors the interactive pull):
  // sample cancel_flag_ exactly once and derive every decision from it.
  const bool cancelled_after_download = cancel_flag_.load(std::memory_order_relaxed);

  // FINAL cumulative ceiling check (adversarial F6): every session frame —
  // batches, Progress, the terminal Eos, resume-leg control traffic —
  // counts toward wire bytes, but the per-message check above can only run
  // while messages flow; bytes arriving AFTER the last message previously
  // finalized/stored/promoted above budget. stats.wire_bytes_received is
  // the whole-pull total (spanning resume legs), so one comparison here
  // closes every control-frame gap; the overage aborts the tee below
  // (not-complete), suppresses the store/promotion (terminal != kComplete)
  // and terminals FAILED with the byte cause.
  if (request.max_transfer_bytes > 0 && !byte_ceiling_exceeded &&
      stats.wire_bytes_received > request.max_transfer_bytes) {
    byte_ceiling_exceeded = true;
  }
  if (request.max_transfer_duration.count() > 0 && !duration_ceiling_exceeded &&
      std::chrono::steady_clock::now() - download_start >= request.max_transfer_duration) {
    duration_ceiling_exceeded = true;  // F12/R2(d): same final-boundary rule as bytes
  }

  if (tee != nullptr) {
    // No export on the direct pull: nullopt save_paths, inert helpers. A
    // final ceiling overage must never finalize (F6) — it rides the same
    // not-complete input as a cancel.
    const TeeTerminalOutcome terminal = finishCacheTee(
        *tee, stats, cancelled_after_download || byte_ceiling_exceeded || duration_ceiling_exceeded,
        std::nullopt,
        [](McapSaveResult) {}, []() {},
        [](const std::filesystem::path&) {},
        [](const std::filesystem::path&, std::string*) { return true; });
    result.tee_outcome = terminal.outcome;
    result.tee_error = terminal.error;
    setTeeForCancel(nullptr);
    tee.reset();
  }

  if (host_progress_active && stats.eos == SessionEos::Complete && !cancelled_after_download &&
      !byte_ceiling_exceeded && !duration_ceiling_exceeded) {
    (void)driver.datasetIngest().progressUpdate(transport_messages_seen);
    driver.datasetIngest().progressFinish();
  }

  // Seal the host-side parser writes while still inside the critical section.
  driver.finalize();

  const auto counts = driver.decodedCounts();
  const auto errors = driver.errorCounts();
  std::uint64_t total_decode_errors = 0;
  for (const auto& [tid, ec] : errors) {
    (void)tid;
    total_decode_errors += ec;
  }
  write_lock.unlock();

  result.stats = stats;
  // Usability predicate (quality review IMPORTANT-3): "some topic COULD
  // decode" is not enough — an all-decode-errors session has decodable
  // bindings and ZERO rows, and must not be described as a usable eager
  // dataset. Usable = a decodable binding exists AND (at least one row
  // actually decoded OR the stream was genuinely empty — an empty time
  // window imports an empty-but-correct dataset).
  std::uint64_t decoded_rows = 0;
  for (const auto& [tid, rows] : counts) {
    (void)tid;
    decoded_rows += rows;
  }
  result.any_decodable =
      driver.hasDecodable() && (decoded_rows > 0 || stats.messages_received == 0);
  result.decode_errors = total_decode_errors;
  result.byte_ceiling_exceeded = byte_ceiling_exceeded;
  result.duration_ceiling_exceeded = duration_ceiling_exceeded;

  // Terminal precedence (documented, pinned by the ABI adversarial tests):
  // CANCEL WINS over the byte ceiling when both latched — an explicit caller
  // cancel outranks the resource classification (the partial is deleted
  // either way, and a retried import reports the ceiling uncontaminated);
  // the ceiling then wins over every other failure cause.
  if (cancelled_after_download ||
      (stats.eos == SessionEos::Cancelled && !byte_ceiling_exceeded && !duration_ceiling_exceeded)) {
    finish_cancelled();
    result.error = "download cancelled";
  } else if (byte_ceiling_exceeded) {
    result.terminal = PullTerminal::kFailed;
    result.error = "transfer byte ceiling exceeded: " + std::to_string(stats.wire_bytes_received) +
                   " wire bytes received > " + std::to_string(request.max_transfer_bytes) +
                   " byte limit";
  } else if (duration_ceiling_exceeded) {
    result.terminal = PullTerminal::kFailed;
    result.error = "transfer duration ceiling exceeded (limit " +
                   std::to_string(request.max_transfer_duration.count()) + " ms)";
  } else if (stats.eos == SessionEos::Complete) {
    result.terminal = PullTerminal::kComplete;
    result.error.clear();
  } else {
    result.terminal = PullTerminal::kFailed;
    result.error = stats.error.empty() ? "session ended without terminal Eos" : stats.error;
  }

  // COMPLETE-only shared SessionCache store (D7) — same suppression rules as
  // the interactive path (decode errors / nothing decodable -> no entry).
  const PJ::cloud::SessionKey session_key = PJ::cloud::computeSessionKey(
      request.connection.uri, request.sequence_names, request.topic_names,
      {request.start_ns, request.end_ns}, request.include_latched);  // F1: the descriptor field keys

  if (result.terminal == PullTerminal::kComplete && total_decode_errors == 0 && driver.hasDecodable()) {
    storeCompletedSessionEntry(rt->sessionCache(), session_key, request.group_name,
                               request.connection.uri, request.topic_names, name_by_id, counts,
                               request.identity, result.tee_outcome,
                               /*refetch_after_disk_miss=*/false);
  }

  // ---- promotion at completion (D6 — the SAME ImportRuntime hook the
  // interactive fetch uses). AFTER the store so a re-entrant on_result finds
  // the entry. The pull does NOT wait: the shared result rides PullResult so
  // the PR-3 job can wait cancel-aware (or detach), and the absent-service
  // case comes back already settled EAGER_ONLY-equivalent.
  if (result.terminal == PullTerminal::kComplete && result.tee_outcome == TeeOutcome::kFinalized &&
      result.dataset.has_value()) {
    result.promotion = rt->promoteToFileSource(
        ImportRuntime::makePromotionRequest(result.dataset->id, request.identity,
                                            rt->fileCache().pathFor(request.identity),
                                            request.descriptor_json),
        &session_key);
  }

  return result;
}

}  // namespace mcap_cloud
