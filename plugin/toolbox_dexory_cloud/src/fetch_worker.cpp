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

#include <chrono>
#include <cstdio>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "parser_ingest_driver.hpp"
#include "session_cache.hpp"
#include "session_decode.hpp"
#include "session_key.hpp"

namespace dexory_cloud {

FetchWorker::FetchWorker() = default;
FetchWorker::~FetchWorker() = default;

bool FetchWorker::datasetExistsInHost(const std::string& display_name) const {
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
    if (name == display_name) {
      return true;
    }
  }
  return false;
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
    std::string error;
    if (!backend_->connect(&error)) {
      backend_.reset();
      if (connectFinished) {
        connectFinished(false, {}, error.empty() ? std::string("connection failed") : error);
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
      connectFinished(true, "Connected — server " + version_text, {});
    }
  } catch (const std::exception& e) {
    backend_.reset();
    if (connectFinished) {
      connectFinished(false, {}, e.what());
    }
  } catch (...) {
    backend_.reset();
    if (connectFinished) {
      connectFinished(false, {}, "Unknown error");
    }
  }
}

void FetchWorker::listSequencesAsync() {
  // Not connected → empty result (no error spam): the sequence table stays
  // empty, matching the dialog's connected-state gating.
  if (!backend_) {
    if (sequencesReady) {
      sequencesReady({});
    }
    if (sequenceNamesReady) {
      sequenceNamesReady({});
    }
    return;
  }

  bool complete = false;
  std::vector<SequenceInfo> sequences = backend_->listSequences(&complete);
  if (backend_->isClosed()) {
    // A dead browse socket reads as an empty/short list; tell the dialog the
    // connection is gone instead of letting it render a silently empty table.
    notifyConnectionLostOnce();
  } else if (!complete) {
    // Pagination broke mid-way but the socket is still up: the list is PARTIAL.
    // Surface it as an error rather than presenting a silently-truncated catalog
    // as authoritative (the browse name->file_id index kept its last COMPLETE
    // snapshot inside listSequences()).
    if (errorOccurred) {
      errorOccurred("Recording list is incomplete (server paging error); showing a partial catalog — retry to refresh.");
    }
  }

  // Name-only convenience callback (used by code paths that only want names),
  // emitted before the full record callback so both views land in one tick.
  std::vector<std::string> names;
  names.reserve(sequences.size());
  for (const auto& s : sequences) {
    names.push_back(s.name);
  }
  if (sequenceNamesReady) {
    sequenceNamesReady(names);
  }
  if (sequencesReady) {
    sequencesReady(std::move(sequences));
  }
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

  // Cache the full per-topic records by name (the dialog uses this for later
  // metadata lookups without another round trip).
  std::unordered_map<std::string, TopicInfo> by_name;
  by_name.reserve(infos.size());
  std::vector<std::string> names;
  names.reserve(infos.size());
  for (const auto& info : infos) {
    names.push_back(info.topic_name);
    by_name.emplace(info.topic_name, info);
  }
  setTopicInfoCache(std::move(by_name));

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

  std::vector<SequenceInfo> sequences = backend_->listSequences();
  std::vector<std::string> names;
  names.reserve(sequences.size());
  for (const auto& s : sequences) {
    names.push_back(s.name);
  }
  if (sequenceNamesReady) {
    sequenceNamesReady(std::move(names));
  }
  if (sequencesReady) {
    sequencesReady(std::move(sequences));
  }
}

void FetchWorker::pullTopicsAsync(std::vector<std::string> sequence_names, std::string group_name,
                                  std::vector<std::string> topic_names, std::int64_t start_ns, std::int64_t end_ns) {
  // The group/display name groups all topics of a (possibly stitched) selection
  // into one catalog dataset + addresses the per-topic ledger callbacks. For
  // N==1 it equals the single sequence name (byte-identical to the pre-Slice-7
  // behavior).
  if (group_name.empty() && !sequence_names.empty()) {
    group_name = sequence_names.front();
  }
  // Always emit allFetchesComplete on every exit path so the dialog clears
  // fetch_active (and re-enables Close).
  auto finish_all = [this, &group_name]() {
    if (allFetchesComplete) {
      allFetchesComplete(group_name);
    }
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
  {
    const PJ::cloud::SessionKey key =
        PJ::cloud::computeSessionKey(conn_uri_, sequence_names, topic_names, {start_ns, end_ns});
    const auto& exists = dataset_exists_
                             ? dataset_exists_
                             : SessionCache::ExistencePredicate(
                                   [this](const std::string& name) { return datasetExistsInHost(name); });
    if (auto cached = session_cache_.lookup(key, exists)) {
      // HIT: re-emit the per-topic pullFinished ledger from cached counts with
      // NO BackendConnection construction. Each requested topic reports ok with
      // a final progress sample from its cached count (count == "messages", a
      // reasonable progress proxy since the bytes already live in the store).
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
      finish_all();
      return;
    }
  }

  // Reset the per-download dataset handle so this pull creates exactly one.
  {
    std::lock_guard<std::mutex> lock(fetch_dataset_mu_);
    fetch_dataset_ = std::nullopt;
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
  std::string connect_err;
  if (!session_owned->connect(&connect_err)) {
    finish_all_topics(false, connect_err.empty() ? std::string("session connect failed") : connect_err);
    finish_all();
    return;
  }
  BackendConnection* session_backend = session_owned.get();

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
  params.include_latched = true;

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
          "Dexory Cloud: the server returned an EMPTY plan (no data matches the selection): " +
          std::to_string(params.s3_keys.size()) + " file(s), " + std::to_string(topic_names.size()) +
          " topic(s) requested, " + window +
          ". The selected topics have no recorded messages in that selection.");
    }
    finish_all_topics(false, "no messages in the selected time range");
    finish_all();
    return;
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
  // own thread.
  std::unique_lock<std::mutex> write_lock(host_write_mu_);

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
    std::string detail = "Dexory Cloud: " + std::to_string(bind.errors.size()) + " of " +
                         std::to_string(session_info.topics.size()) + " session topics failed to bind a parser:";
    for (std::size_t i = 0; i < bind.errors.size() && i < kMaxBindErrorLines; ++i) {
      detail += "\n  " + bind.errors[i];
    }
    if (bind.errors.size() > kMaxBindErrorLines) {
      detail += "\n  ... and " + std::to_string(bind.errors.size() - kMaxBindErrorLines) + " more";
    }
    errorOccurred(std::move(detail));
  }

  if (!driver.hasDecodable()) {
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

  // Progress throttle: emit pullProgress at most ~10 Hz per topic.
  std::unordered_map<std::uint32_t, std::int64_t> bytes_by_id;
  std::unordered_map<std::uint32_t, std::chrono::steady_clock::time_point> last_emit;
  // pullProgress is throttled PER TOPIC (last_emit is keyed by topic id). The
  // wire-byte figure is session-wide, so it gets its OWN single throttle stamp —
  // otherwise it would re-emit the same value once per topic that trips its own
  // window (N GUI events/window instead of one).
  std::chrono::steady_clock::time_point last_wire_emit{};
  const auto kProgressInterval = std::chrono::milliseconds(100);

  // Expose the session backend so requestCancel() can fire a wire Cancel
  // (guarded by cancel_mu_ — the pointer is cleared under the same lock before
  // session_owned is destroyed, closing the UAF window).
  {
    std::lock_guard<std::mutex> lock(cancel_mu_);
    backend_session_for_cancel_ = session_backend;
  }

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
        return true;
      });

  {
    std::lock_guard<std::mutex> lock(cancel_mu_);
    backend_session_for_cancel_ = nullptr;
  }

  // Seal the host-side parser writes (releaseParserIngest -> flushAll) while
  // still inside the host-write critical section, so the GUI-thread
  // notifyDataChanged -> catalog rebuild that follows sees every row and
  // every object topic.
  driver.finalize();

  // Final per-topic progress flush + per-topic completion.
  const auto counts = driver.decodedCounts();
  const auto errors = driver.errorCounts();
  const bool cancelled = (stats.eos == SessionEos::Cancelled) || cancel_flag_.load(std::memory_order_relaxed);
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
    if (!tid_opt) {
      // Requested topic absent from the session dictionary: the server's plan
      // found no message for it in the selected files/time range (zero-message
      // catalog topics and windows that miss the data both land here).
      error = "no messages in the selected time range";
    } else if (decodable_by_id.count(*tid_opt) && !decodable_by_id.at(*tid_opt)) {
      error = skip_reason_by_id.count(*tid_opt) ? skip_reason_by_id.at(*tid_opt) : "no parser bound for topic";
    } else if (cancelled) {
      // Treated as not-ok but the dialog tags it "Cancelled" (kept OUT of the
      // failure tally) when state_.cancelling is set.
      error = "cancelled";
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
  // error/Unset, no decode errors). cancel / error / no-terminal-Eos / partial
  // decode -> NO entry (no half-cached state). The key is the SAME tuple the HIT
  // path computes — `sequence_names` directly, never a resolved file_id (post-M6
  // rowids renumber across catalog rebuilds; see session_key.hpp). The datastore
  // now owns the decoded rows under group_name; the cache records only counts
  // metadata so a repeat fetch is a zero-transport HIT.
  if (stats.eos == SessionEos::Complete && !cancelled && !session_failed && total_decode_errors == 0) {
    const PJ::cloud::SessionKey key =
        PJ::cloud::computeSessionKey(conn_uri_, sequence_names, topic_names, {start_ns, end_ns});
    CachedSession entry;
    entry.display_name = group_name;
    entry.server_uri = conn_uri_;
    for (const auto& t : topic_names) {
      std::uint64_t count = 0;
      for (const auto& [tid, name] : name_by_id) {
        if (name == t) {
          count = counts.count(tid) ? counts.at(tid) : 0;
          break;
        }
      }
      entry.counts_by_topic[t] = count;
      entry.total_messages += count;
    }
    session_cache_.store(key, std::move(entry));
  }

  finish_all();
}

}  // namespace dexory_cloud
