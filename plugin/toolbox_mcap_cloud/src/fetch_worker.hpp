// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <pj_base/sdk/plugin_data_api.hpp>
#include <pj_base/sdk/toolbox_plugin_base.hpp>
#include <string>
#include <utility>
#include <vector>

#include "backend_connection.hpp"
#include "backend_types.hpp"
#include "session_cache.hpp"

namespace mcap_cloud {

class CacheTee;
class ImportRuntime;

enum class McapSaveStatus {
  Complete,
  Partial,
  Failed,
  /// The download aborted before the export could produce anything (connect /
  /// open-session failure, empty plan, nothing decodable). Distinct from
  /// Failed: no writer ran, no file exists, and the pull's own error reporting
  /// already covers the cause — the dialog treats this as informational.
  Skipped,
};

struct McapSaveResult {
  McapSaveStatus status = McapSaveStatus::Failed;
  std::string path;
  std::string error;
};

/// Thin background adapter for the cloud backend, running on the dialog's worker
/// thread. It owns the concrete ixwebsocket+Protobuf BackendConnection(s) (the
/// catalog-browse socket plus one fresh session connection per pull); callers
/// serialize commands onto this thread and route callbacks back to the GUI thread.
///
/// TOOLBOX shape (Slice 5/16): the worker owns BOTH the catalog browse path
/// (connect / listSequences / listTopics / getTopicMetadata) AND the in-dialog
/// session download+ingest (pullTopicsAsync -> openSessionFresh + downloadSession
/// delegating to ParserIngestDriver — ensureParserBinding + pushMessage through
/// the toolbox runtime host, so the host's registered parsers write all scalars
/// and classify/store object topics). The host writes are serialized by
/// host_write_mu_ (the toolbox DataWriter has no internal mutex); a single
/// DataSourceHandle per download groups all topics into one catalog entry.
class FetchWorker {
 public:
  FetchWorker();
  ~FetchWorker();

  /// Provide a callback that returns the toolbox host. Set once on the GUI
  /// thread; reads happen on the worker thread, so the callback must be
  /// thread-safe or always return a stable view.
  void setHostProvider(std::function<PJ::sdk::ToolboxHostView()> provider) {
    host_provider_ = std::move(provider);
  }

  /// Provide a callback that returns the toolbox runtime host. REQUIRED: a pull
  /// fails with "no runtime host provider" if this is unset. The worker uses it
  /// for notifyDataChanged (post-import catalog refresh) and for
  /// createParserIngest / releaseParserIngest (host-delegated parsing via
  /// ParserIngestDriver).
  void setRuntimeHostProvider(std::function<PJ::ToolboxRuntimeHostView()> provider) {
    runtime_host_provider_ = std::move(provider);
  }

  /// Bind the per-toolbox-instance ImportRuntime (stage-4 PR-1). Set once on
  /// the GUI thread BEFORE any pull, like the providers above; non-owning
  /// (the toolbox owns the runtime, which outlives every worker). When bound:
  /// the pull serializes host writes on the runtime's SHARED mutex, uses the
  /// runtime's thread-safe SessionCache, and ALWAYS tees the session once
  /// into the SessionFileCache (the single-encoder rule — exports become
  /// byte copies of the cache file; see pullTopicsAsync). Unset (legacy /
  /// CLI-shaped tests): the pre-stage-4 behavior is preserved verbatim.
  void setImportRuntime(ImportRuntime* runtime) { import_runtime_ = runtime; }

  /// Cross-thread cancel. Signals the in-flight wire session (so
  /// downloadSession() returns) AND a cache tee blocked in its backpressure
  /// wait (so the pull thread cannot stay wedged behind a stalled disk).
  /// Both hooks are published/retired under cancel_mu_ — see the .cpp.
  void requestCancel();
  void resetCancel() {
    cancel_flag_.store(false, std::memory_order_relaxed);
  }
  [[nodiscard]] bool isCancelled() const {
    return cancel_flag_.load(std::memory_order_relaxed);
  }

  /// Connect (or reconnect) to the given URI. Calls connectFinished on completion.
  void connectAsync(std::string uri, std::string cert_path, std::string api_key, bool allow_insecure);

  /// List topics for a given sequence (partial metadata).
  void listTopicsAsync(std::string sequence_name);

  /// Fetch full per-topic metadata (schema, ontology tag, user
  /// metadata) on demand for the Info panel. Calls topicMetadataReady.
  void fetchTopicMetadataAsync(std::string sequence_name, std::string topic_name);

  /// Tag-editor commit (Slice 6, Plan D Task 9): apply staged tag edits to the
  /// file backing `sequence_name` (set_tags upsert overrides; unset_keys remove
  /// an override / NULL-mask an embedded tag), then RE-LIST sequences so the
  /// flat user_metadata + per-tag override view refresh and the Lua filter sees
  /// the edits. Fires tagsUpdated(sequence, ok, error) exactly once, then — on
  /// success — emits the same sequencesReady the catalog browse path uses (so
  /// the dialog reuses its onSequencesReady refresh, invalidating the seq view
  /// cache + Lua re-eval). On failure no re-list happens; the dialog surfaces
  /// the verbatim error. NOTE (key-addressing): the underlying UpdateTags RPC
  /// addresses the file by s3_key (sequence_name verbatim), so it no longer
  /// depends on the browse name->file_id index being fresh — that index can be
  /// minutes stale (rebuilt only by the last listSequences()) without this call
  /// failing. The post-success re-list above still MUST happen regardless: it
  /// is what refreshes the flat metadata + Lua filter view, which key-addressing
  /// does not provide for free.
  void updateTagsAsync(std::string sequence_name, std::vector<std::pair<std::string, std::string>> set_tags,
                       std::vector<std::string> unset_keys);

  /// In-dialog bounded-horizon download + decode of the selected topics of one
  /// OR MORE sequences (the Mosaico Fetch shape; Slice 7 stitched multi-file
  /// selection). `sequence_names` is the deterministically-ordered list (sorted
  /// by start time so a reordered UI selection yields the same request); they
  /// go key-addressed into EXACTLY ONE OpenFresh session (wire v2 — the names
  /// ARE the durable s3_keys; no file_id resolution) and the server
  /// stitches them into one continuous logical stream. `group_name` is the
  /// display/group handle used for the dataset label + the per-topic ledger
  /// callbacks ("first (+N-1 more)" for N>1; the single name for N==1). Opens a
  /// FRESH session over its own BackendConnection (the browse socket is
  /// single-threaded blocking and owned by the catalog path), delegates parsing
  /// to the host via ParserIngestDriver (ensureParserBinding + pushMessage
  /// through ToolboxRuntimeHostView), drives downloadSession() forwarding each
  /// raw CDR record to the host, and emits pullProgress (throttled ~10 Hz),
  /// pullFinished per topic (exactly once), then allFetchesComplete once.
  /// Honors requestCancel(). The whole host-write critical section is serialized
  /// by host_write_mu_. driver.finalize() seals host parser writes (flushAll)
  /// while still inside the critical section, before notifyDataChanged.
  /// A non-empty `save_directory` additionally requests a local MCAP export.
  /// The export is strictly SECONDARY: any export failure is reported via
  /// mcapSaveFinished but never aborts the download or the host import.
  /// LEGACY mode (no ImportRuntime bound): the worker writes the export
  /// directly and a non-empty save_directory bypasses the count-only
  /// SessionCache HIT (the in-memory cache holds no raw payloads).
  /// RUNTIME mode (setImportRuntime): the CACHE IS THE SOLE ENCODER (spec
  /// docs/canonical-layout-import.md §9) — every pull tees the raw records
  /// once into the SessionFileCache partial (CacheTee: provenance record,
  /// bounded async queue, validated finalize; a tee failure never aborts the
  /// ingest, §9.6) and the export receives a byte COPY (Complete: of the
  /// finalized cache file, atomic temp+rename; cancel: of the readable cache
  /// partial into the export .partial, after which the cache partial is
  /// DELETED — cache partials never survive, spec §10). A memory hit with a
  /// valid disk cache file serves the export by copy with zero transport; a
  /// memory hit whose disk file is gone evicts the entry and refetches.
  void pullTopicsAsync(std::vector<std::string> sequence_names, std::string group_name,
                       std::vector<std::string> topic_names, std::int64_t start_ns, std::int64_t end_ns,
                       std::string save_directory = {});

  // `uri` echoes the EXACT uri this connectAsync() call was invoked with (not
  // re-read from any mutable dialog state), so a caller that may have edited
  // its own editable URI field since issuing the connect can still learn which
  // server this result is actually about.
  std::function<void(bool ok, std::string uri, std::string status, std::string error)> connectFinished;
  /// D8: the BackendCapabilities (HelloResponse.backend) the server advertised,
  /// emitted once on a successful connect BEFORE connectFinished so the dialog
  /// can drive additive UI (the '/'-prefix hierarchy combo + the query-assist
  /// vocabulary) off it. Carries supports_file_hierarchy + metadata_key_vocabulary;
  /// empty/false when the server omitted the field.
  std::function<void(BackendCaps caps)> capabilitiesReady;
  /// The Capabilities (HelloResponse.capabilities) the server advertised,
  /// emitted at the SAME point as capabilitiesReady above (before
  /// connectFinished) so the dialog can gate the tag-edit button off it before
  /// its next tick. Carries resume_supported + tag_edit_supported; defaults
  /// (both false) when the server omitted the field — see ServerCaps's comment
  /// in backend_types.hpp for why an absent field is NOT the same as a
  /// deliberate false at the BackendConnection::updateTags() gate, but the
  /// dialog's button-disable UI collapses "unknown" and "known-false" to the
  /// same disabled state (never offering a control that might fail is the
  /// conservative default for an ancient/odd server too).
  std::function<void(ServerCaps caps)> serverCapabilitiesReady;
  /// Full SequenceInfo entries, including user_metadata (used by the Lua
  /// metadata filter).
  std::function<void(std::vector<SequenceInfo> sequences)> sequencesReady;
  std::function<void(std::string sequence_name, std::vector<std::string> topic_names)> topicsReady;
  /// Full TopicInfo list from listTopics (name + size + timestamp range +
  /// created/locked/chunks). Schema/ontology/user_metadata are NOT populated
  /// here — those arrive via topicMetadataReady after fetchTopicMetadataAsync.
  std::function<void(std::string sequence_name, std::vector<TopicInfo> topics)> topicInfosReady;
  /// Fired INSTEAD of topicsReady/topicInfosReady when a topics request FAILS
  /// (not connected, timeout, dead socket, server error, unknown name). The
  /// dialog must NOT cache a failure as "zero topics" — that was the sticky
  /// empty-Topics-panel bug; an uncached sequence retries on the next
  /// selection change.
  std::function<void(std::string sequence_name, std::string error)> topicsFailed;
  /// Fires once per connection when a worker call discovers the browse socket
  /// is dead (ix never auto-reconnects it). The dialog flips its connected
  /// state so the UI stops pretending the link is up.
  std::function<void()> connectionLost;
  /// Full per-topic metadata (incl. schema fields) for the Info panel.
  std::function<void(std::string sequence_name, std::string topic_name, TopicInfo info)> topicMetadataReady;
  /// Coarse pull phase ("Opening session: ...", "Session opened: ... -
  /// downloading"). Fired at phase boundaries so the dialog can show what the
  /// worker is waiting on BEFORE any byte flows (session opening can take real
  /// time over WAN); the dialog appends a live elapsed counter.
  std::function<void(std::string phase)> pullPhase;
  /// Cumulative bytes ingested for a topic during a pull (decoded RAW payload
  /// bytes). Throttled to ~10 Hz on the worker side.
  std::function<void(std::string topic_name, std::int64_t bytes)> pullProgress;
  /// Cumulative WS payload bytes RECEIVED off the wire for the whole pull
  /// (compressed batch bodies — the network figure, vs pullProgress's decoded
  /// figure). Throttled with pullProgress; a final sample fires at pull end.
  std::function<void(std::int64_t wire_bytes)> pullWireBytes;
  /// Total estimated download size in bytes (the server pre-flight budget, an
  /// UPPER BOUND). Fired once, right after the session opens, so the dialog can
  /// render a byte-based progress percentage. 0 = server gave no estimate.
  std::function<void(std::uint64_t estimated_total_bytes)> pullEstimate;
  /// Per-topic completion. Fires exactly once per requested topic. ok=false on
  /// undecodable schema (no parser) or a transport/session failure.
  std::function<void(std::string sequence_name, std::string topic_name, bool ok, std::string error)> pullFinished;
  /// Fires once after every topic of one pull has reported pullFinished.
  std::function<void(std::string sequence_name)> allFetchesComplete;
  /// Reconnect-resume hint (Slice 8): fires just before each reconnect attempt
  /// during a mid-pull transport drop. `attempt` is 1-based, `max` is the cap.
  /// Routed to the dialog's "Resuming (attempt N/max)…" status + notify.
  std::function<void(std::string group, unsigned attempt, unsigned max)> pullResuming;
  /// Cache HIT (Slice 8): fires once when a pull is served entirely from the
  /// in-memory SessionCache (zero transport). Routed to a "served from cache"
  /// notify on the dialog.
  std::function<void(std::string group)> pullServedFromCache;
  /// Local MCAP export result — EXACTLY ONE per pull that requested an export
  /// (every exit path emits: Complete names the final `.mcap`; Partial names
  /// the READABLE `.mcap.partial` retained after a cancellation/transport
  /// drop; Failed carries the RAW prepare/open/write/rename cause, no "MCAP
  /// export failed" prefix — the dialog owns the user-facing wording; Skipped
  /// = the download aborted before the export started, no file exists).
  /// Fires before allFetchesComplete so the dialog's close policy sees it.
  std::function<void(McapSaveResult result)> mcapSaveFinished;
  /// Cache-tee outcome for one RUNTIME-mode pull (fires exactly once per
  /// pull when an ImportRuntime is bound, before allFetchesComplete; never
  /// fires in legacy mode). `identity` is the canonical descriptor identity
  /// the session tees under — but it is EMPTY (with outcome kNone) for the
  /// exits that abort BEFORE the descriptor is computed (empty selection,
  /// no host provider, browse never connected), so a consumer (PR-3) must
  /// never key a map on the identity without checking for "". `error` is
  /// non-empty for kFailed/kAborted. The same outcome is recorded on the
  /// stored SessionCache entry — PR-3's promotion hook keys off it (kFailed
  /// suppresses promotion, §9.6).
  std::function<void(TeeOutcome outcome, std::string identity, std::string error)> teeFinished;
  /// Tag-edit commit result. ok=false carries the verbatim server/transport
  /// error. On ok=true a sequencesReady follows so the dialog refreshes the
  /// catalog metadata (the Lua filter re-evaluates against the new tags).
  std::function<void(std::string sequence_name, bool ok, std::string error)> tagsUpdated;
  /// Surfaced for non-fatal catalog-RPC failures that don't map to a specific
  /// callback. Routed to the dialog's status line.
  std::function<void(std::string message)> errorOccurred;

 private:
  /// Return the single DataSourceHandle for the current download, creating it on
  /// first use under fetch_dataset_mu_. All topics of one sequence share it so
  /// they land in ONE catalog group. pullTopicsAsync resets it at pull start.
  [[nodiscard]] PJ::Expected<PJ::sdk::DataSourceHandle> datasetForFetch(
      const PJ::sdk::ToolboxHostView& host, const std::string& sequence_name);

  /// Fire connectionLost exactly once per browse connection when backend_
  /// reports a closed socket. Reset by a successful connectAsync. Worker-thread
  /// only (commands run serially), so a plain bool suffices.
  void notifyConnectionLostOnce();
  bool connection_lost_notified_ = false;

 public:
  /// Terminal result of ONE gated (filtered) list request. Exactly one is
  /// emitted per listSequencesFilteredAsync call that is not superseded before
  /// starting. `error != kNone` explains why the result isn't authoritative
  /// (the dialog must NOT render "no recordings" for anything but a genuinely
  /// complete, error==kNone, result).
  struct GateListResult {
    std::uint64_t request_id = 0;
    enum class Error { kNone, kPartial, kConnectionLost, kSelectionGone, kRebuildStorm, kSuperseded };
    Error error = Error::kNone;
    std::vector<SequenceInfo> sequences;
    // kPartial only: the backend's failure reason (listSequences error_out —
    // e.g. a degraded-start server's "catalog not yet available — first build
    // in progress"). May be empty; the dialog then falls back to its generic
    // paging-error wording.
    std::string message;
  };

  /// Vocabulary result. request_id is echoed (uniformly with vocabularyFailed/
  /// gatePageReady/gateListFinished) so the dialog can drop a stale answer by
  /// simple id comparison, with no special case for this callback.
  /// recovery=true when this refresh was triggered from INSIDE a
  /// filtered-list stale recovery — the dialog must then only refresh combos,
  /// never auto-start another sweep (the recovery owns the retry).
  std::function<void(std::uint64_t request_id, VocabularyInfo vocab, bool recovery)> vocabularyReady;
  /// GetVocabulary failed on a live connection (the kVocabularyError phase).
  std::function<void(std::uint64_t request_id)> vocabularyFailed;
  /// One page of a gated sweep; reset semantics as in BackendConnection.
  std::function<void(std::uint64_t request_id, std::vector<SequenceInfo> page, bool reset)>
      gatePageReady;
  std::function<void(GateListResult result)> gateListFinished;

  /// Fetch the vocabulary (the gate's data source). request_id is echoed to
  /// vocabularyReady/vocabularyFailed so the dialog can ignore stale answers.
  void fetchVocabularyAsync(std::uint64_t request_id);
  /// List ONE site server-filtered, resolving (customer, site) NAMES against
  /// the worker's latest vocabulary; supersedable by a later gate request id.
  void listSequencesFilteredAsync(std::uint64_t request_id, std::string customer, std::string site);
  /// Called from the GUI thread when a NEW gate request id is issued, so an
  /// in-flight sweep aborts promptly (atomic; safe cross-thread).
  void supersedeGateRequests(std::uint64_t latest) { latest_gate_request_.store(latest); }

 private:
  // ---- gate (customer/site filtered browse) state, worker-thread only, plus
  // the one cross-thread atomic (see supersedeGateRequests above) ------------
  std::optional<VocabularyInfo> vocab_;
  std::atomic<std::uint64_t> latest_gate_request_{0};
  std::string last_gate_customer_;
  std::string last_gate_site_;
  std::uint64_t last_gate_request_id_ = 0;  // worker-thread only: the id last_gate_customer_/site_ were requested under

  std::unique_ptr<BackendConnection> backend_;  // catalog-browse socket
  // Credentials remembered from the last successful connectAsync, so a pull can
  // open its OWN fresh session connection (isolated from the browse socket; a
  // cancelled session never poisons the next download).
  std::string conn_uri_;
  std::string conn_cert_path_;
  std::string conn_api_key_;
  bool conn_allow_insecure_ = false;
  std::function<PJ::sdk::ToolboxHostView()> host_provider_;
  std::function<PJ::ToolboxRuntimeHostView()> runtime_host_provider_;
  // Non-owning; set once on the GUI thread before any pull (see
  // setImportRuntime). nullptr = legacy mode.
  ImportRuntime* import_runtime_ = nullptr;
  std::atomic<bool> cancel_flag_{false};

  std::optional<PJ::sdk::DataSourceHandle> fetch_dataset_;
  std::mutex fetch_dataset_mu_;
  // Serializes the host-write critical section (createDataSource + bindSession +
  // decode loop) — the toolbox DataWriter has no internal mutex. Lock order is
  // always host_write_mu_ -> fetch_dataset_mu_, never the reverse. RUNTIME
  // mode locks the ImportRuntime's SHARED host-write mutex instead: a private
  // per-worker mutex cannot serialize a future provider job against the
  // interactive worker (this member remains the legacy-mode fallback).
  std::mutex host_write_mu_;

  // The session BackendConnection in flight during a pull, exposed for
  // requestCancel() to signal a wire CancelSession. Guarded by cancel_mu_: the
  // worker publishes it BEFORE the blocking connect()/openSessionFresh() calls
  // (so a cancel during session establishment wakes the OpenSession wait, not
  // just the download loop) and clears it via an RAII guard (under the lock) on
  // every exit path before the owning unique_ptr is destroyed, so a concurrent
  // requestCancel() from the GUI thread can never dereference a freed object.
  std::mutex cancel_mu_;
  BackendConnection* backend_session_for_cancel_{nullptr};
  // The pull's live CacheTee, exposed for requestCancel() to free a producer
  // blocked in the tee's backpressure wait (quality review IMPORTANT-2).
  // Same lifetime discipline as backend_session_for_cancel_ above: published
  // after a successful CacheTee::begin, retired UNDER cancel_mu_ before the
  // owning unique_ptr resets/destroys the tee on every path.
  CacheTee* tee_for_cancel_{nullptr};

  // ---- in-memory SessionCache (Slice 8) ------------------------------------
  // LEGACY-mode instance, owned by the worker. A HIT re-emits the per-topic
  // pullFinished ledger from cached counts with ZERO transport; entries are
  // stored only on a COMPLETE download. RUNTIME mode uses the ImportRuntime's
  // SHARED (thread-safe) SessionCache instead so provider jobs and the
  // interactive worker observe one cache (D7).
  SessionCache session_cache_;
  // Existence predicate seam: answers "is this dataset still in the host?".
  // Defaults to a catalogSnapshot()-backed check when a host provider is bound;
  // tests inject a fake. Presence-unknown MUST return false (-> MISS).
  SessionCache::ExistencePredicate dataset_exists_;

 public:
  // Test seam: inject a custom dataset-existence predicate (overrides the
  // default catalogSnapshot()-backed one). Used by the hermetic cache test to
  // drive HIT / present-but-gone / presence-unknown without a real host.
  void setDatasetExistsForTest(SessionCache::ExistencePredicate pred) { dataset_exists_ = std::move(pred); }
  // Test seam: read-only access to the cache (size / inspection in tests).
  [[nodiscard]] const SessionCache& sessionCacheForTest() const { return session_cache_; }

 private:
  // The §6.1 memory-hit block of pullTopicsAsync (pure extraction — quality
  // review IMPORTANT-3). Returns true when the pull was SERVED from the
  // in-memory cache (caller: finish_all + return); false = miss, continue
  // with a network fetch. RUNTIME mode applies the disk-validity rules
  // (valid disk -> serve + export by copy under a shared read lease;
  // missing/invalid disk -> evict + report refetch); LEGACY mode preserves
  // the pre-stage-4 count-only HIT.
  [[nodiscard]] bool serveFromMemoryCache(
      ImportRuntime* rt, SessionCache& session_cache, const SessionKey& session_key,
      const std::string& tee_identity, const std::string& group_name,
      const std::vector<std::string>& topic_names, const std::string& save_directory,
      const SessionCache::ExistencePredicate& exists,
      const std::function<void(const std::filesystem::path&)>& export_by_copy,
      TeeOutcome* tee_outcome, bool* refetch_after_disk_miss);

  // Default existence predicate (D7): keyed on the entry's stable dataset_id
  // when recorded (recorded display name as the id-recycle tiebreak), with a
  // name-only fallback for legacy id-less entries. Returns false when no host
  // is bound or the host lacks acquire_catalog_snapshot (presence-unknown ->
  // MISS).
  [[nodiscard]] bool datasetExistsInHost(const CachedSession& entry) const;
};

}  // namespace mcap_cloud
