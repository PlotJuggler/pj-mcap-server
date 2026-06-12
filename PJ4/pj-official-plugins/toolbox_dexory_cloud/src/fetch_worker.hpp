// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <pj_base/sdk/plugin_data_api.hpp>
#include <pj_base/sdk/toolbox_plugin_base.hpp>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "backend_connection.hpp"
#include "backend_types.hpp"
#include "session_cache.hpp"

namespace dexory_cloud {

/// Thin background adapter for the cloud backend, running on the dialog's worker
/// thread. The worker itself is transport-agnostic: callers serialize commands
/// and route callbacks to the GUI thread.
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

  void requestCancel() {
    cancel_flag_.store(true, std::memory_order_relaxed);
    // Also signal any in-flight wire session so downloadSession() returns.
    if (auto* b = backend_session_for_cancel_.load(std::memory_order_acquire)) {
      b->cancelSession();
    }
  }
  void resetCancel() {
    cancel_flag_.store(false, std::memory_order_relaxed);
  }
  [[nodiscard]] bool isCancelled() const {
    return cancel_flag_.load(std::memory_order_relaxed);
  }

  /// Cache topic infos from the most recent listTopicsAsync so per-topic
  /// metadata can be looked up later without another round trip. Keyed by
  /// topic_name.
  void setTopicInfoCache(std::unordered_map<std::string, TopicInfo> by_name) {
    topic_info_by_name_ = std::move(by_name);
  }

  /// Connect (or reconnect) to the given URI. Calls connectFinished on completion.
  void connectAsync(std::string uri, std::string cert_path, std::string api_key, bool allow_insecure);

  /// List all sequences from the connected server.
  void listSequencesAsync();

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
  /// the verbatim error.
  void updateTagsAsync(std::string sequence_name, std::vector<std::pair<std::string, std::string>> set_tags,
                       std::vector<std::string> unset_keys);

  /// In-dialog bounded-horizon download + decode of the selected topics of one
  /// OR MORE sequences (the Mosaico Fetch shape; Slice 7 stitched multi-file
  /// selection). `sequence_names` is the deterministically-ordered list (sorted
  /// by start time so a reordered UI selection yields the same request); they
  /// resolve to file_ids[] and open EXACTLY ONE OpenFresh session (the server
  /// stitches them into one continuous logical stream). `group_name` is the
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
  void pullTopicsAsync(std::vector<std::string> sequence_names, std::string group_name,
                       std::vector<std::string> topic_names, std::int64_t start_ns, std::int64_t end_ns);

  std::function<void(bool ok, std::string status, std::string error)> connectFinished;
  /// D8: the BackendCapabilities (HelloResponse.backend) the server advertised,
  /// emitted once on a successful connect BEFORE connectFinished so the dialog
  /// can drive additive UI (the '/'-prefix hierarchy combo + the query-assist
  /// vocabulary) off it. Carries supports_file_hierarchy + metadata_key_vocabulary;
  /// empty/false when the server omitted the field.
  std::function<void(BackendCaps caps)> capabilitiesReady;
  /// Full SequenceInfo entries, including user_metadata (used by the Lua
  /// metadata filter). The name-only callback is kept
  /// for code paths that only want names.
  std::function<void(std::vector<SequenceInfo> sequences)> sequencesReady;
  std::function<void(std::vector<std::string> names)> sequenceNamesReady;
  // Progressive discovery (PJ3 parity): the initial list, so the table can
  // populate before per-sequence detail finishes streaming in...
  std::function<void(std::vector<SequenceInfo> sequences)> sequenceListStarted;
  // ...and one callback per sequence as the server fills in its detail
  // (min/max timestamp, size, metadata), so Date/Size columns populate
  // incrementally rather than snapping in all at once.
  std::function<void(SequenceInfo sequence)> sequenceInfoReady;
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
  std::atomic<bool> cancel_flag_{false};
  std::unordered_map<std::string, TopicInfo> topic_info_by_name_;

  std::optional<PJ::sdk::DataSourceHandle> fetch_dataset_;
  std::mutex fetch_dataset_mu_;
  // Serializes the host-write critical section (createDataSource + bindSession +
  // decode loop) — the toolbox DataWriter has no internal mutex. Lock order is
  // always host_write_mu_ -> fetch_dataset_mu_, never the reverse.
  std::mutex host_write_mu_;

  // The session BackendConnection in flight during a pull, exposed for
  // requestCancel() to signal a wire CancelSession. Set under host_write_mu_
  // around the download; cleared when the download returns.
  std::atomic<BackendConnection*> backend_session_for_cancel_{nullptr};

  // ---- in-memory SessionCache (Slice 8) ------------------------------------
  // Owned by the worker (per-plugin-instance lifetime; single worker thread, no
  // extra locking). A HIT re-emits the per-topic pullFinished ledger from cached
  // counts with ZERO transport; entries are stored only on a COMPLETE download.
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
  // Default existence predicate: true iff `display_name` appears in the toolbox
  // host's catalog snapshot dataSources(). Returns false when no host is bound or
  // the host lacks acquire_catalog_snapshot (presence-unknown -> MISS).
  [[nodiscard]] bool datasetExistsInHost(const std::string& display_name) const;
};

}  // namespace dexory_cloud
