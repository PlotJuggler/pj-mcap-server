// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// ImportRuntime (stage-4 PR-1, D2/D5/D7-amended) — the ONE per-toolbox-instance
// runtime shared by the interactive dialog path today and the
// pj.descriptor_import.v1 provider in PR-3. It owns everything both paths must
// share:
//   - the durable SessionFileCache (spec §5) — the SINGLE MCAP encoder's
//     destination ("cache sole encoder; exports byte copies", spec §9);
//   - the thread-safe in-memory SessionCache (D7: DatasetId-keyed existence);
//   - the host-write mutex serializing EVERY worker's host-write critical
//     section (each FetchWorker's private mutex cannot serialize a provider
//     job against the interactive worker);
//   - an IN-MEMORY trust set preloaded from the TrustedOrigins ledger
//     (bounded isTrusted — §6.3 forbids per-query file I/O; write-through on
//     recordSuccessfulHello);
//   - the keyed active-materialization registry (in-process contention
//     detection; the cross-process half is the SessionFileCache file lock).
//
// CacheTee is the single-encoder materialization pipeline both paths drive:
// registry ticket -> cache cleanup -> exclusive MaterializeLock -> under-lock
// corruption deletion -> exclusive 0600 sink + SessionMcapWriter + embedded
// canonical-descriptor provenance -> bounded owned-payload write queue drained
// by a writer thread (backpressure when full; a disk stall must bound memory,
// never grow it) -> validated finalize (Complete) or partial deletion (any
// other exit — cache partials NEVER survive, spec §10). A tee failure never
// aborts the ingest (§9.6): the owner drops the tee and continues.
//
// No dialog/Qt dependencies — a future headless caller drives this directly.
#pragma once

#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>

#include "backend_types.hpp"
#include "decoded_message.hpp"
#include "session_cache.hpp"
#include "session_file_cache.hpp"
#include "session_mcap_writer.hpp"
#include "trusted_origins.hpp"

namespace mcap_cloud {

class ImportRuntime {
 public:
  /// Construct over an injected cache + ledger (tests) or the standard roots
  /// (the toolbox: SessionFileCache::standard + TrustedOrigins::standard).
  /// Preloads the in-memory trust set from the ledger — the ONLY unprompted
  /// file read; every later isTrusted is pure in-memory.
  ImportRuntime(SessionFileCache file_cache, TrustedOrigins trusted);

  ImportRuntime(const ImportRuntime&) = delete;
  ImportRuntime& operator=(const ImportRuntime&) = delete;

  [[nodiscard]] SessionFileCache& fileCache() { return file_cache_; }
  [[nodiscard]] const SessionFileCache::Config& cacheConfig() const { return cache_config_; }
  [[nodiscard]] SessionCache& sessionCache() { return session_cache_; }
  /// Serializes the host-write critical section across ALL workers of this
  /// toolbox instance (the toolbox DataWriter has no internal mutex).
  [[nodiscard]] std::mutex& hostWriteMutex() { return host_write_mu_; }

  /// Write-through: records into the TrustedOrigins ledger AND the in-memory
  /// set. Call on a successful interactive Hello only (spec §7 guard 1).
  void recordSuccessfulHello(std::string_view uri);
  /// Bounded in-memory query (no file I/O — §6.3). False for unparsable URIs.
  [[nodiscard]] bool isTrusted(std::string_view uri) const;

  /// RAII in-process materialization marker for one identity. Released on
  /// destruction; move-only.
  class MaterializeTicket {
   public:
    MaterializeTicket(MaterializeTicket&& other) noexcept;
    MaterializeTicket& operator=(MaterializeTicket&& other) noexcept;
    MaterializeTicket(const MaterializeTicket&) = delete;
    MaterializeTicket& operator=(const MaterializeTicket&) = delete;
    ~MaterializeTicket();

   private:
    friend class ImportRuntime;
    MaterializeTicket(ImportRuntime* runtime, std::string identity);
    void release();
    ImportRuntime* runtime_ = nullptr;
    std::string identity_;
  };

  /// In-process contention detection: nullopt while another materialization of
  /// the SAME identity is registered in THIS process (the cross-process case
  /// is the SessionFileCache MaterializeLock).
  [[nodiscard]] std::optional<MaterializeTicket> tryBeginMaterialize(std::string_view identity);

 private:
  void endMaterialize(const std::string& identity);

  SessionFileCache file_cache_;
  SessionFileCache::Config cache_config_{};
  TrustedOrigins trusted_;
  SessionCache session_cache_;
  std::mutex host_write_mu_;

  mutable std::mutex trust_mu_;
  std::unordered_set<std::string> trusted_keys_;  // trustedOriginKey shape

  std::mutex active_mu_;
  std::unordered_set<std::string> active_identities_;
};

/// One cache-materialization attempt (see the file header). Drive:
///   begin(identity) -> openWriter(info, canonical_json, estimate)
///   -> enqueue(msg)... -> drainAndClose() -> finalize() on Complete,
///   or abortAndCleanup() on any other exit (the destructor also aborts).
/// Threading: begin/openWriter/enqueue/drainAndClose/finalize/abort run on the
/// ONE owning (pull) thread; the internal writer thread is the only other
/// toucher and is joined by drainAndClose/abort.
class CacheTee {
 public:
  explicit CacheTee(ImportRuntime& runtime);
  ~CacheTee();  // abortAndCleanup() unless finalized

  CacheTee(const CacheTee&) = delete;
  CacheTee& operator=(const CacheTee&) = delete;

  /// Phase 1 (before any network): registry ticket -> cache cleanup (orphan
  /// partials + LRU; runs BEFORE our own lock so it never self-contends) ->
  /// exclusive MaterializeLock -> delete a lookup()-failing existing file
  /// under the lock (spec §5 corruption policy).
  [[nodiscard]] bool begin(const std::string& identity, std::string* error);

  /// Phase 2 (after OpenSession, when SessionInfo is known): free-space check
  /// (estimated_bytes > available at the cache root refuses; 0 skips),
  /// exclusive 0600 sink at the per-process partial path, writer open, and
  /// the embedded canonical-descriptor provenance record (BEFORE the first
  /// message write — spec §5). Starts the bounded writer thread. On failure
  /// nothing is left behind (partial removed) and the lock stays held until
  /// destruction.
  [[nodiscard]] bool openWriter(const SessionInfo& info, const std::string& canonical_descriptor_json,
                                std::uint64_t estimated_bytes, std::string* error);

  /// Copy `message` into the bounded queue. BLOCKS while the queue is full
  /// (backpressure — a slow disk bounds memory, it never grows it). Returns
  /// false once the tee has failed (writer error) — the caller drops the tee
  /// and the ingest continues (§9.6).
  [[nodiscard]] bool enqueue(const DecodedMessage& message);

  [[nodiscard]] bool failed() const;
  [[nodiscard]] std::string failureError() const;

  /// Terminal, on the owning thread: close the queue, join the writer thread,
  /// close the MCAP writer (footer+summary) and the sink. After a true
  /// return, partialPath() is a READABLE MCAP (the export cancel path copies
  /// it before the partial is deleted).
  [[nodiscard]] bool drainAndClose(std::string* error);

  /// Validate + atomically publish the closed partial (Complete path only).
  /// ExpectedContent defaults to the producer-side counts this tee tracked
  /// (messages enqueued for known session topics / distinct such topics) —
  /// the only check that catches a cleanly-closed PREFIX. `expected_override`
  /// exists for tests. On success the partial is gone and finalPath() exists.
  [[nodiscard]] bool finalize(const std::optional<SessionFileCache::ExpectedContent>& expected_override,
                              std::string* error);

  /// Any-exit cleanup: stop + join the writer thread, close writer/sink,
  /// DELETE the partial (cache partials never survive — spec §10), release
  /// the lock and the registry ticket. Idempotent; no-op after finalize().
  void abortAndCleanup();

  [[nodiscard]] const std::filesystem::path& partialPath() const { return partial_path_; }
  [[nodiscard]] std::filesystem::path finalPath() const;

 private:
  void writerLoop();
  void failLocked(std::string reason);  // caller holds queue_mu_

  ImportRuntime& runtime_;
  std::string identity_;
  std::optional<ImportRuntime::MaterializeTicket> ticket_;
  std::optional<SessionFileCache::MaterializeLock> lock_;
  std::filesystem::path partial_path_;

  ExclusiveFileSink sink_;
  SessionMcapWriter writer_;
  bool writer_open_ = false;
  bool finalized_ = false;
  bool cleaned_up_ = false;

  // Producer-side ExpectedContent tracking (owning thread only).
  std::unordered_set<std::uint32_t> known_topic_ids_;      // session dictionary
  std::unordered_set<std::uint32_t> enqueued_known_topics_;  // distinct known ids seen
  std::uint64_t enqueued_known_messages_ = 0;

  // Bounded owned-payload queue (queue_mu_ guards everything below).
  static constexpr std::size_t kMaxQueuedMessages = 4096;
  static constexpr std::size_t kMaxQueuedBytes = 32ull * 1024 * 1024;
  mutable std::mutex queue_mu_;
  std::condition_variable queue_not_full_;
  std::condition_variable queue_not_empty_;
  std::deque<DecodedMessage> queue_;
  std::size_t queued_bytes_ = 0;
  bool queue_closed_ = false;
  bool failed_ = false;
  std::string fail_error_;
  std::thread writer_thread_;
};

}  // namespace mcap_cloud
