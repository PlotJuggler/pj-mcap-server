// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "import_runtime.hpp"

#include <system_error>
#include <utility>

namespace mcap_cloud {

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// ImportRuntime
// ---------------------------------------------------------------------------

ImportRuntime::ImportRuntime(SessionFileCache file_cache, TrustedOrigins trusted)
    : file_cache_(std::move(file_cache)), trusted_(std::move(trusted)) {
  // Construction-time preload — the only unprompted ledger read. Later
  // queries are pure in-memory (§6.3 bounded-query rule).
  for (auto& key : trusted_.allOrigins()) {
    trusted_keys_.insert(std::move(key));
  }
}

void ImportRuntime::recordSuccessfulHello(std::string_view uri) {
  const auto key = trustedOriginKey(uri);
  if (!key.has_value()) {
    return;  // unparsable uri: fail closed, record nothing (ledger does the same)
  }
  trusted_.recordSuccessfulHello(uri);  // write-through to the ledger
  std::lock_guard<std::mutex> lock(trust_mu_);
  trusted_keys_.insert(*key);
}

bool ImportRuntime::isTrusted(std::string_view uri) const {
  const auto key = trustedOriginKey(uri);
  if (!key.has_value()) {
    return false;  // rejected shapes never match, not even themselves
  }
  std::lock_guard<std::mutex> lock(trust_mu_);
  return trusted_keys_.count(*key) > 0;
}

std::optional<ImportRuntime::MaterializeTicket> ImportRuntime::tryBeginMaterialize(
    std::string_view identity) {
  std::string key(identity);
  std::lock_guard<std::mutex> lock(active_mu_);
  if (!active_identities_.insert(key).second) {
    return std::nullopt;  // this process is already materializing that identity
  }
  return MaterializeTicket(this, std::move(key));
}

void ImportRuntime::endMaterialize(const std::string& identity) {
  std::lock_guard<std::mutex> lock(active_mu_);
  active_identities_.erase(identity);
}

ImportRuntime::MaterializeTicket::MaterializeTicket(ImportRuntime* runtime, std::string identity)
    : runtime_(runtime), identity_(std::move(identity)) {}

ImportRuntime::MaterializeTicket::MaterializeTicket(MaterializeTicket&& other) noexcept
    : runtime_(std::exchange(other.runtime_, nullptr)), identity_(std::move(other.identity_)) {}

ImportRuntime::MaterializeTicket& ImportRuntime::MaterializeTicket::operator=(
    MaterializeTicket&& other) noexcept {
  if (this != &other) {
    release();
    runtime_ = std::exchange(other.runtime_, nullptr);
    identity_ = std::move(other.identity_);
  }
  return *this;
}

ImportRuntime::MaterializeTicket::~MaterializeTicket() {
  release();
}

void ImportRuntime::MaterializeTicket::release() {
  if (runtime_ != nullptr) {
    runtime_->endMaterialize(identity_);
    runtime_ = nullptr;
  }
}

// ---------------------------------------------------------------------------
// CacheTee
// ---------------------------------------------------------------------------

CacheTee::CacheTee(ImportRuntime& runtime) : runtime_(runtime) {}

CacheTee::~CacheTee() {
  abortAndCleanup();
}

bool CacheTee::begin(const std::string& identity, std::string* error) {
  auto fail = [error](std::string message) {
    if (error != nullptr) {
      *error = std::move(message);
    }
    return false;
  };
  identity_ = identity;

  // In-process contention first (cheap, exact), then maintenance, then the
  // cross-process lock. cleanup() runs BEFORE our own lock so its per-victim
  // exclusive try never self-contends with this materialization.
  ticket_ = runtime_.tryBeginMaterialize(identity);
  if (!ticket_.has_value()) {
    return fail("a materialization of this session is already in progress in this process");
  }
  runtime_.fileCache().cleanup(runtime_.cacheConfig());

  std::string lock_error;
  lock_ = runtime_.fileCache().tryLockForMaterialize(identity, &lock_error);
  if (!lock_.has_value()) {
    ticket_.reset();
    return fail(lock_error);
  }
  partial_path_ = runtime_.fileCache().partialPathFor(*lock_);

  // Under-lock corruption policy (spec §5): an existing file that fails the
  // validated lookup is deleted before re-materializing. (A VALID existing
  // file is simply re-materialized over by the atomic finalize rename.)
  const fs::path existing = runtime_.fileCache().pathFor(identity);
  std::error_code ec;
  if (!existing.empty() && fs::exists(existing, ec) && !ec) {
    fs::path unused;
    if (!runtime_.fileCache().lookup(identity, &unused)) {
      fs::remove(existing, ec);
    }
  }
  return true;
}

bool CacheTee::openWriter(const SessionInfo& info, const std::string& canonical_descriptor_json,
                          std::uint64_t estimated_bytes, std::string* error) {
  auto fail = [this, error](std::string message) {
    // Leave no debris: a failed open never publishes anything. The lock and
    // ticket stay held until destruction/abort so a retry in another worker
    // does not race this one's teardown.
    std::error_code ec;
    fs::remove(partial_path_, ec);
    if (error != nullptr) {
      *error = std::move(message);
    }
    return false;
  };
  if (!lock_.has_value()) {
    return fail("cache tee not begun");
  }

  // Free-space reserve (spec §5): refuse when the server's pre-flight
  // estimate exceeds the space available at the cache root. 0 = no estimate
  // -> skip (never block on an unknown).
  if (estimated_bytes > 0) {
    std::error_code space_ec;
    const fs::space_info space = fs::space(partial_path_.parent_path(), space_ec);
    if (!space_ec && space.available < estimated_bytes) {
      return fail("insufficient free space for the session cache: need ~" +
                  std::to_string(estimated_bytes) + " bytes, " +
                  std::to_string(space.available) + " available at " +
                  partial_path_.parent_path().string());
    }
  }

  // A stale same-pid partial (crash leftover) would fail the exclusive
  // create; it is ours by construction (we hold the identity lock), remove it.
  std::error_code ec;
  fs::remove(partial_path_, ec);

  std::string sink_error;
  if (!sink_.open(partial_path_, &sink_error)) {
    return fail(sink_error);
  }
  std::string writer_error;
  if (!writer_.open(sink_.writable(), info, &writer_error)) {
    sink_.closeFile();
    return fail(writer_error);
  }
  writer_open_ = true;
  // Provenance BEFORE the first message write (spec §5: a cache file
  // self-describes which request produced it; finalize/lookup verify it).
  if (!writer_.writeMetadata("mcap_cloud/source_descriptor", canonical_descriptor_json,
                             &writer_error)) {
    std::string ignored;
    (void)writer_.close(&ignored);
    sink_.closeFile();
    writer_open_ = false;
    return fail(writer_error);
  }

  known_topic_ids_.clear();
  for (const auto& topic : info.topics) {
    known_topic_ids_.insert(topic.topic_id);
  }

  writer_thread_ = std::thread([this] { writerLoop(); });
  return true;
}

bool CacheTee::enqueue(const DecodedMessage& message) {
  // Owned copy taken OUTSIDE the lock — the payload copy is the expensive
  // part and must not extend the critical section.
  DecodedMessage owned = message;
  std::unique_lock<std::mutex> lock(queue_mu_);
  if (!writer_thread_.joinable()) {
    return false;  // never opened (or already drained/aborted)
  }
  queue_not_full_.wait(lock, [this] {
    return failed_ || queue_closed_ || abort_requested_.load(std::memory_order_relaxed) ||
           (queue_.size() < max_queued_messages_ && queued_bytes_ < kMaxQueuedBytes);
  });
  if (failed_ || queue_closed_ || abort_requested_.load(std::memory_order_relaxed)) {
    return false;
  }
  queued_bytes_ += owned.payload.size();
  if (known_topic_ids_.count(owned.topic_id) > 0) {
    ++enqueued_known_messages_;
    enqueued_known_topics_.insert(owned.topic_id);
  }
  queue_.push_back(std::move(owned));
  queue_not_empty_.notify_one();
  return true;
}

void CacheTee::requestAbort() {
  abort_requested_.store(true, std::memory_order_relaxed);
  // Take the mutex before notifying so a waiter that just evaluated its
  // predicate cannot sleep through the wakeup.
  std::lock_guard<std::mutex> lock(queue_mu_);
  queue_not_full_.notify_all();
  queue_not_empty_.notify_all();
}

bool CacheTee::abortRequested() const {
  return abort_requested_.load(std::memory_order_relaxed);
}

bool CacheTee::failed() const {
  std::lock_guard<std::mutex> lock(queue_mu_);
  return failed_;
}

std::string CacheTee::failureError() const {
  std::lock_guard<std::mutex> lock(queue_mu_);
  return fail_error_;
}

void CacheTee::writerLoop() {
  for (;;) {
    DecodedMessage message;
    {
      std::unique_lock<std::mutex> lock(queue_mu_);
      queue_not_empty_.wait(lock, [this] {
        return !queue_.empty() || queue_closed_ || failed_ ||
               abort_requested_.load(std::memory_order_relaxed);
      });
      if (failed_ || abort_requested_.load(std::memory_order_relaxed)) {
        return;  // failure/abort: leave the queue; cleanup discards it
      }
      if (queue_.empty()) {
        return;  // closed + drained
      }
      message = std::move(queue_.front());
      queue_.pop_front();
      queued_bytes_ -= message.payload.size();
      queue_not_full_.notify_one();
    }
    if (writer_hook_for_test_) {
      writer_hook_for_test_(message);  // test seam: deterministic drain stall
    }
    std::string write_error;
    if (!writer_.write(message, &write_error)) {
      std::lock_guard<std::mutex> lock(queue_mu_);
      failLocked(std::move(write_error));
      return;
    }
    // The mcap writer swallows sink failures (the CheckedFileWriter lesson);
    // observe the latched sink error explicitly after every write.
    if (!sink_.error().empty()) {
      std::lock_guard<std::mutex> lock(queue_mu_);
      failLocked(sink_.error());
      return;
    }
  }
}

void CacheTee::failLocked(std::string reason) {
  failed_ = true;
  if (fail_error_.empty()) {
    fail_error_ = std::move(reason);
  }
  queue_not_full_.notify_all();
  queue_not_empty_.notify_all();
}

bool CacheTee::drainAndClose(std::string* error) {
  auto fail = [error](std::string message) {
    if (error != nullptr) {
      *error = std::move(message);
    }
    return false;
  };
  {
    std::lock_guard<std::mutex> lock(queue_mu_);
    queue_closed_ = true;
    queue_not_empty_.notify_all();
    queue_not_full_.notify_all();
  }
  if (writer_thread_.joinable()) {
    writer_thread_.join();
  }
  bool was_failed;
  std::string why;
  {
    std::lock_guard<std::mutex> lock(queue_mu_);
    was_failed = failed_;
    why = fail_error_;
  }
  if (!writer_open_) {
    return fail(was_failed ? why : std::string("cache tee writer was never opened"));
  }
  std::string close_error;
  const bool close_ok = writer_.close(&close_error);
  writer_open_ = false;
  sink_.closeFile();
  if (was_failed) {
    return fail(why);
  }
  if (!close_ok) {
    return fail(close_error);
  }
  if (!sink_.error().empty()) {
    return fail(sink_.error());
  }
  return true;
}

bool CacheTee::finalize(const std::optional<SessionFileCache::ExpectedContent>& expected_override,
                        std::string* error) {
  if (!lock_.has_value()) {
    if (error != nullptr) {
      *error = "cache tee not begun";
    }
    return false;
  }
  // Producer-side counts (the tee KNOWS what the finished session must
  // contain): messages enqueued for topics in the session dictionary and the
  // distinct set of such topics — unknown-topic records are skipped by the
  // writer and never enter Statistics. A writer thread that lost messages
  // produces a cleanly-closed PREFIX only this comparison can reject.
  SessionFileCache::ExpectedContent expected{
      .message_count = enqueued_known_messages_,
      .channel_count = enqueued_known_topics_.size(),
  };
  if (expected_override.has_value()) {
    expected = *expected_override;
  }
  if (!runtime_.fileCache().finalize(*lock_, expected, error)) {
    return false;
  }
  finalized_ = true;
  return true;
}

fs::path CacheTee::finalPath() const {
  return runtime_.fileCache().pathFor(identity_);
}

void CacheTee::abortAndCleanup() {
  if (cleaned_up_) {
    return;
  }
  cleaned_up_ = true;
  {
    std::lock_guard<std::mutex> lock(queue_mu_);
    queue_closed_ = true;
    if (!finalized_) {
      failLocked("cache materialization aborted");
    } else {
      queue_not_empty_.notify_all();
      queue_not_full_.notify_all();
    }
  }
  if (writer_thread_.joinable()) {
    writer_thread_.join();
  }
  if (writer_open_) {
    std::string ignored;
    (void)writer_.close(&ignored);
    writer_open_ = false;
  }
  sink_.closeFile();
  if (!finalized_ && !partial_path_.empty()) {
    // Cache partials never survive (spec §10) — the inverse of the export
    // path's deliberate readable-partial retention.
    std::error_code ec;
    fs::remove(partial_path_, ec);
  }
  lock_.reset();    // release the cross-process materialize lock
  ticket_.reset();  // release the in-process registry slot
}

}  // namespace mcap_cloud
