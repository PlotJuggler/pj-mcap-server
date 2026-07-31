// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "import_runtime.hpp"

#include <limits>
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

bool ImportRuntime::recordSuccessfulHello(std::string_view uri) {
  const auto key = trustedOriginKey(uri);
  if (!key.has_value()) {
    return false;  // unparsable uri: fail closed, record nothing (ledger does the same)
  }
  if (!trusted_.recordSuccessfulHello(uri)) {
    // F14: the durable write failed — do NOT hold transient in-memory trust
    // (queries keep answering needs-confirmation); the caller diagnoses.
    return false;
  }
  std::lock_guard<std::mutex> lock(trust_mu_);
  trusted_keys_.insert(*key);
  return true;
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

ImportRuntime::AdmissionTicket::AdmissionTicket(AdmissionTicket&& other) noexcept
    : runtime_(std::exchange(other.runtime_, nullptr)) {}

ImportRuntime::AdmissionTicket& ImportRuntime::AdmissionTicket::operator=(
    AdmissionTicket&& other) noexcept {
  if (this != &other) {
    if (runtime_ != nullptr) {
      runtime_->releaseAdmission();
    }
    runtime_ = std::exchange(other.runtime_, nullptr);
  }
  return *this;
}

ImportRuntime::AdmissionTicket::~AdmissionTicket() {
  if (runtime_ != nullptr) {
    runtime_->releaseAdmission();
  }
}

std::optional<ImportRuntime::AdmissionTicket> ImportRuntime::acquireAdmission(
    const std::function<bool()>& cancelled, std::chrono::milliseconds poll) {
  std::unique_lock<std::mutex> lock(adm_mu_);
  if (!adm_held_ && adm_queue_.empty()) {
    adm_held_ = true;
    return AdmissionTicket(this);
  }
  AdmissionWaiter waiter;
  adm_queue_.push_back(&waiter);
  for (;;) {
    waiter.cv.wait_for(lock, poll);
    if (waiter.granted) {
      return AdmissionTicket(this);  // the releaser transferred the held gate
    }
    if (cancelled && cancelled()) {
      // Remove self; a grant racing this cancel is handled by re-checking
      // granted AFTER unlinking (still under the mutex).
      for (auto it = adm_queue_.begin(); it != adm_queue_.end(); ++it) {
        if (*it == &waiter) {
          adm_queue_.erase(it);
          break;
        }
      }
      if (waiter.granted) {
        // The gate was handed to us in the same race window: pass it on.
        lock.unlock();
        releaseAdmission();
        return std::nullopt;
      }
      return std::nullopt;
    }
  }
}

void ImportRuntime::releaseAdmission() {
  std::unique_lock<std::mutex> lock(adm_mu_);
  if (!adm_queue_.empty()) {
    AdmissionWaiter* next = adm_queue_.front();
    adm_queue_.pop_front();
    next->granted = true;  // the gate stays held — ownership transfers
    next->cv.notify_one();
    return;
  }
  adm_held_ = false;
}

// Insert (or refcount into) the registry. Caller-provided `lease` is dropped
// when an entry already exists — the existing flock stays authoritative.
// Returns true when THIS call created the entry.
bool ImportRuntime::insertLeaseLocked(const std::string& key, FileLock lease, unsigned refs) {
  auto it = retained_leases_.find(key);
  if (it != retained_leases_.end()) {
    it->second.refs += refs;  // repeat retains stack; the passed lease releases on return
    return false;
  }
  retained_leases_.emplace(key, LeaseEntry{std::move(lease), refs});
  return true;
}

// LEASE-THEN-VALIDATE (invariant 2), performed OUTSIDE lease_mu_: acquire the
// shared sidecar lease first, then validate the artifact while still holding
// it, so no evictor can slip between the two. nullopt = no lease (contended
// or invalid); `diagnostic` distinguishes.
std::optional<FileLock> ImportRuntime::acquireValidatedLease(std::string_view identity,
                                                             std::string* diagnostic) {
  std::string lease_error;
  auto lease = file_cache_.acquireReadLease(identity, &lease_error);
  if (!lease.has_value()) {
    if (diagnostic != nullptr) {
      *diagnostic = "read lease unavailable (" + lease_error + ")";
    }
    return std::nullopt;
  }
  std::filesystem::path validated;
  if (!file_cache_.lookup(identity, &validated)) {
    if (diagnostic != nullptr) {
      *diagnostic = "cache artifact does not validate under the lease";
    }
    return std::nullopt;  // lease releases here: never pin a missing/invalid file
  }
  return lease;
}

bool ImportRuntime::retainLeaseForLifetime(std::string_view identity, std::string* diagnostic) {
  const std::string key(identity);
  {
    // Idempotent fast path: an existing entry just takes another reference —
    // repeat queries never stack flocks (invariant 1: one atomic step).
    std::lock_guard<std::mutex> lock(lease_mu_);
    auto it = retained_leases_.find(key);
    if (it != retained_leases_.end()) {
      ++it->second.refs;
      return true;
    }
  }
  std::string local_diagnostic;
  auto lease = acquireValidatedLease(identity, &local_diagnostic);
  if (!lease.has_value()) {
    if (diagnostic != nullptr) {
      *diagnostic = local_diagnostic;
    }
    std::lock_guard<std::mutex> lock(lease_diag_mu_);
    last_lease_diagnostic_ = local_diagnostic;
    return false;
  }
  std::lock_guard<std::mutex> lock(lease_mu_);
  (void)insertLeaseLocked(key, std::move(*lease), 1);
  return true;
}

void ImportRuntime::adoptLeaseForLifetime(std::string_view identity, FileLock lease,
                                          unsigned refs) {
  std::lock_guard<std::mutex> lock(lease_mu_);
  (void)insertLeaseLocked(std::string(identity), std::move(lease), refs == 0 ? 1u : refs);
}

ImportRuntime::LeaseDrop ImportRuntime::dropLeaseForMaterialize(std::string_view identity) {
  std::lock_guard<std::mutex> lock(lease_mu_);
  auto it = retained_leases_.find(std::string(identity));
  if (it == retained_leases_.end()) {
    return LeaseDrop{};
  }
  const LeaseDrop drop{true, it->second.refs};
  retained_leases_.erase(it);  // the flock releases here, atomically with the report
  return drop;
}

void ImportRuntime::restoreLease(std::string_view identity, const LeaseDrop& drop) {
  if (!drop.had_lease) {
    return;
  }
  // Bounded short retry: a cross-process EXCLUSIVE holder means the artifact
  // is being rematerialized right now — after the window we give up without a
  // lease and record why (a stale-generation pin would be worse).
  std::string diagnostic;
  for (unsigned attempt = 0; attempt <= lease_retry_attempts_; ++attempt) {
    if (attempt > 0) {
      std::this_thread::sleep_for(lease_retry_poll_);
    }
    auto lease = acquireValidatedLease(identity, &diagnostic);
    if (lease.has_value()) {
      std::lock_guard<std::mutex> lock(lease_mu_);
      (void)insertLeaseLocked(std::string(identity), std::move(*lease), drop.refs);
      return;
    }
  }
  std::lock_guard<std::mutex> lock(lease_diag_mu_);
  last_lease_diagnostic_ = "could not restore the dataset-lifetime cache lease for " +
                           std::string(identity) + ": " + diagnostic;
}

std::string ImportRuntime::lastLeaseDiagnostic() const {
  std::lock_guard<std::mutex> lock(lease_diag_mu_);
  return last_lease_diagnostic_;
}

bool ImportRuntime::hasRetainedLease(std::string_view identity) const {
  std::lock_guard<std::mutex> lock(lease_mu_);
  return retained_leases_.count(std::string(identity)) > 0;
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

bool CacheTee::begin(const std::string& identity, std::string* error,
                     std::optional<SessionFileCache::MaterializeLock> adopted_lock) {
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
  // A retained dataset-lifetime lease on THIS identity would block our own
  // exclusive materialize lock below — drop it. ONE atomic operation that
  // both releases and reports (invariant 1): a racing memory-hit retain can
  // no longer be erased without being recorded, which the previous
  // has-then-release two-step allowed. A FAILED/cancelled rematerialization
  // restores exactly this token in abortAndCleanup, BEFORE the ticket goes
  // (invariant 3), or the previous valid artifact backing a live dataset is
  // left permanently unleased.
  lease_drop_ = runtime_.dropLeaseForMaterialize(identity);

  if (adopted_lock.has_value()) {
    // F9: a caller (the provider job) already performed its bounded
    // cancelable wait on the cross-process lock and hands it in — never
    // re-acquire (self-contention on the sidecar).
    lock_ = std::move(adopted_lock);
  } else {
    std::string lock_error;
    if (lock_fail_for_test_) {
      lock_error = "injected exclusive-lock failure (test seam)";
    } else {
      lock_ = runtime_.fileCache().tryLockForMaterialize(identity, &lock_error,
                                                         &begin_lock_contended_);
    }
    if (!lock_.has_value()) {
      // Round-4 R1(a): do NOT release the ticket here. begin() has already
      // dropped this identity's lifetime lease, and releasing the ticket
      // before that lease is restored reopens exactly the ticket-less
      // restoration window invariant 3 exists to close. The failure returns
      // with the ticket STILL held; the owner destroys/aborts the tee, and
      // abortAndCleanup restores the lease and only then releases the ticket.
      return fail(lock_error);
    }
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

  // Free-space reserve + cache budget (spec §5, per adversarial F12 and
  // re-verify R2): with a nonzero estimate,
  //   - an estimate larger than the WHOLE cache budget can never fit (R2b);
  //   - eviction targets max_total_bytes - estimate, so
  //     current_total + estimate <= max_total_bytes after the pass (R2b;
  //     locked/leased stragglers may keep it best-effort, matching
  //     cleanup()'s existing semantics);
  //   - the reserve check is SATURATING (R2c): an absurd estimate refuses
  //     instead of overflowing past available >= estimate + min_free_bytes.
  // 0 = no estimate -> skip (never block on an unknown; the byte/duration
  // ceilings bound the actual transfer instead).
  if (estimated_bytes > 0) {
    const std::uintmax_t est = static_cast<std::uintmax_t>(estimated_bytes);
    SessionFileCache::Config budget = runtime_.cacheConfig();
    if (est > budget.max_total_bytes) {
      return fail("session estimate ~" + std::to_string(estimated_bytes) +
                  " bytes exceeds the whole cache budget (" +
                  std::to_string(budget.max_total_bytes) + " bytes)");
    }
    budget.max_total_bytes -= est;  // evict toward current_total + est <= budget
    runtime_.fileCache().cleanup(budget);
    const std::uintmax_t reserve = runtime_.cacheConfig().min_free_bytes;
    const std::uintmax_t needed =
        (est > std::numeric_limits<std::uintmax_t>::max() - reserve)
            ? std::numeric_limits<std::uintmax_t>::max()
            : est + reserve;
    std::error_code space_ec;
    const fs::space_info space = fs::space(partial_path_.parent_path(), space_ec);
    if (!space_ec && space.available < needed) {
      return fail("insufficient free space for the session cache: need ~" +
                  std::to_string(estimated_bytes) + " bytes plus the " +
                  std::to_string(reserve) + "-byte reserve, " +
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

  // Thread-spawn failure is a resource-exhaustion reality (adversarial F8):
  // it must degrade to the documented NONFATAL tee failure — close + remove
  // the partial, report the cause — never let std::system_error escape into
  // the pull (the interactive command pump would std::terminate).
  try {
    writer_thread_ = thread_factory_for_test_
                         ? thread_factory_for_test_([this] { writerLoop(); })
                         : std::thread([this] { writerLoop(); });
  } catch (...) {
    std::string ignored;
    (void)writer_.close(&ignored);
    sink_.closeFile();
    writer_open_ = false;
    return fail("could not start the cache writer thread (resource exhaustion)");
  }
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
  // Round-4 R2: `finalized_` is NOT set here. It gates abortAndCleanup's
  // lease restoration, so setting it at the rename meant a later
  // handoff failure returned false while the abort skipped restoring
  // lease_drop_ — leaving the PRIOR live dataset unpinned during a
  // rematerialization of an already-leased identity. It is set only after
  // the ENTIRE sequence (publish + handoff + adoption) has succeeded.
  //
  // PUBLISHED-BUT-NOT-FINALIZED state (reachable between here and the end):
  //   * the renamed final file STAYS — abortAndCleanup deletes only
  //     partial_path_, which the rename already moved away, so the remove is
  //     a no-op (verified at that site);
  //   * lease_drop_ IS restored, re-pinning the identity for the prior live
  //     dataset — and since the identity's path now holds the freshly
  //     published, content-equivalent file, that pin is exactly what a lazy
  //     re-open needs (rename-over replacement is safe by construction);
  //   * the caller records FAILURE (no kFinalized, no stored identity, no
  //     promotion), so nothing points at the new artifact;
  //   * that leaves an orphaned published file, which is plain evictable
  //     cache content for a later cleanup — the conservative outcome.
  //
  // Adversarial F2 + re-verify R1(a): hand the exclusive materialize lock
  // over as a SHARED dataset-lifetime lease retained by the runtime. The
  // handoff is LOAD-BEARING, not best-effort: if the platform downgrade
  // loses its (non-atomic) conversion window — or the post-downgrade
  // revalidation finds an evictor removed the file — finalize() FAILS, so
  // callers never record kFinalized, never store the cache identity, and
  // never promote a missing/unleased path. A downgrade failure with the
  // file still present leaves a valid-but-unleased artifact behind (a
  // future lookup may hit it); reporting failure here is the conservative
  // truth about THIS materialization.
  std::optional<FileLock> lease;
  std::string lease_error;
  if (lease_handoff_fail_for_test_) {
    lease_error = "injected lease-handoff failure (test seam)";
    lock_.reset();  // mirror toSharedLease's released-on-failure contract
  } else {
    lease = SessionFileCache::toSharedLease(std::move(*lock_), &lease_error);
    lock_.reset();
  }
  if (!lease.has_value()) {
    if (error != nullptr) {
      *error = "cache file published but the read-lease handoff failed (" + lease_error +
               ") — treating this materialization as failed";
    }
    return false;
  }
  std::filesystem::path revalidated;
  if (!runtime_.fileCache().lookup(identity_, &revalidated)) {
    if (error != nullptr) {
      *error = "cache file vanished during the lease handoff (evicted in the conversion window)";
    }
    return false;
  }
  // Already lease-then-validated: the downgraded lease is held across the
  // revalidation above, so this only records it (invariant 2 satisfied).
  // Round-4 R3: adopt the FULL count begin() dropped — the documented
  // semantics are "a materialization drops every reference and restores the
  // same count", so the prior holders' references must survive a successful
  // rematerialization exactly as they survive an aborted one. No prior lease
  // means this materialization is the first holder: 1.
  runtime_.adoptLeaseForLifetime(identity_, std::move(*lease),
                                 lease_drop_.had_lease ? lease_drop_.refs : 1u);
  lease_drop_ = ImportRuntime::LeaseDrop{};  // consumed: the abort must not re-restore
  finalized_ = true;                         // the WHOLE sequence succeeded (R2)
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
  // ORDERING (invariant 3): release the OS exclusive lock FIRST (our own
  // exclusive would block the shared re-acquisition), then restore the lease
  // WHILE THE TICKET IS STILL HELD — the ticket is what stops a same-process
  // materializer from taking the identity mid-restore and leaving nobody to
  // put the lease back — and only then release the ticket.
  lock_.reset();
  if (pre_restore_hook_for_test_) {
    pre_restore_hook_for_test_();  // F1 seam: the ticket is still held here
  }
  if (!finalized_) {
    // begin() dropped a live dataset's lifetime lease to take the exclusive
    // lock and this materialization did NOT publish: put it back
    // (lease-then-validate, bounded retry, no-op when nothing was dropped).
    runtime_.restoreLease(identity_, lease_drop_);
    lease_drop_ = ImportRuntime::LeaseDrop{};
  }
  ticket_.reset();  // release the in-process registry slot LAST
}

}  // namespace mcap_cloud
