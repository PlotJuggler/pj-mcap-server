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

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include <pj_base/sdk/descriptor_import.hpp>  // PJ::SourcePromotionHostView / Request

#include "backend_types.hpp"
#include "decoded_message.hpp"
#include "session_cache.hpp"
#include "session_file_cache.hpp"
#include "session_mcap_writer.hpp"
#include "trusted_origins.hpp"

namespace mcap_cloud {

// The stock MCAP loader's STABLE MANIFEST ID + its locked minimal preset
// (D6, consult-locked values; grounded in pj-official-plugins
// data_load_mcap/manifest.json:2). use_log_time is REQUIRED — the eager
// ingest pushes log_time_ns while the loader defaults to publish time; a
// promoted dataset must be identical to what a later import produces. NO
// filepath in the preset — the host rewrites it (spec §6.2 step 6). Pinned
// byte-identical by fetch_worker_promotion_test.
inline constexpr const char* kMcapLoaderPluginId = "mcap-loader";
inline constexpr const char* kMcapLoaderPresetJson =
    "{\"clamp_large_arrays\":true,\"max_array_size\":500,\"selected_topics\":[],"
    "\"use_header_timestamp\":false,\"use_log_time\":true}";

/// Settled-exactly-once promotion outcome, shared (shared_ptr) between the
/// host's on_result callback — which may fire re-entrantly, on any host
/// thread, or long after the initiating pull returned — and any waiter (the
/// PR-3 import job). The callback closure captures ONLY this shared state
/// (plus a weak SessionCache handle), never plugin/GUI state, so a late
/// result can never touch freed memory.
class PromotionResult {
 public:
  /// First call wins; later calls are no-ops (defensive — the ABI promises
  /// exactly-once, but a settle-once state costs nothing).
  void settle(bool ok, std::string message);
  /// nullopt while the accepted promotion is still outstanding.
  [[nodiscard]] std::optional<bool> ok() const;
  [[nodiscard]] std::string message() const;
  /// Block until settled OR `cancelled()` returns true (polled every `poll`
  /// — the job's cancel() cannot notify this cv, so the wait is a bounded
  /// poll). Returns true when settled; false = the caller cancelled while
  /// the promotion was still outstanding (DETACH: this shared state simply
  /// outlives the caller and settles whenever on_result fires).
  [[nodiscard]] bool waitSettled(const std::function<bool()>& cancelled,
                                 std::chrono::milliseconds poll) const;

 private:
  mutable std::mutex mu_;
  mutable std::condition_variable cv_;
  bool done_ = false;
  bool ok_ = false;
  std::string message_;
};

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
  [[nodiscard]] SessionCache& sessionCache() { return *session_cache_; }
  /// Serializes the host-write critical section across ALL workers of this
  /// toolbox instance (the toolbox DataWriter has no internal mutex).
  [[nodiscard]] std::mutex& hostWriteMutex() { return host_write_mu_; }

  /// Bind the OPTIONAL pj.source_promotion.v1 host view (per toolbox
  /// instance — never share a bound view across plugin instances, see
  /// SourcePromotionHostView's doc). Absence means every completed
  /// materialize is EAGER_ONLY-equivalent. Set at bind() on the main
  /// thread; read from pull/job threads (mutex-guarded). The caller (the
  /// toolbox) must keep the underlying binding alive while any promotion is
  /// outstanding — it owns both this runtime and the bound registry scope.
  void setPromotionHost(std::optional<PJ::SourcePromotionHostView> view);
  [[nodiscard]] bool hasPromotionHost() const;

  /// The D6 promotion request over a validated cache artifact. `dataset` is
  /// the pull's DataSourceHandle id (the provisional dataset announced via
  /// on_dataset); `descriptor_json` is toSourceDescriptorJson (canonical
  /// fields + display_name), never the canonical identity serialization.
  [[nodiscard]] static PJ::SourcePromotionRequest makePromotionRequest(
      PJ::DatasetId dataset, const std::string& source_identity,
      const std::filesystem::path& local_path, const std::string& descriptor_json);

  /// The SHARED promotion-at-completion hook (D6 — the interactive fetch and
  /// the PR-3 import job both terminate through this). ALWAYS returns a
  /// non-null result:
  ///   - service absent/invalid or synchronously rejected -> settled
  ///     ok=false immediately (EAGER_ONLY-equivalent, message says why);
  ///   - accepted -> settles when on_result fires (possibly re-entrantly,
  ///     before this returns; possibly much later, after the caller moved
  ///     on — the DETACH shape).
  /// When `cache_key` is non-null the matching SessionCache entry's
  /// promotion_state tracks the transaction (kPending -> kPromoted /
  /// kEagerOnly), updated from the callback through SHARED ownership of the
  /// thread-safe cache (weak_ptr) — safe even against runtime teardown.
  std::shared_ptr<PromotionResult> promoteToFileSource(const PJ::SourcePromotionRequest& request,
                                                       const SessionKey* cache_key);

  /// Diagnostics-only count of ACCEPTED promotions whose on_result has not
  /// settled yet (adversarial F7's plugin-side accounting; the
  /// teardown-deadlock half is already covered by the job DETACH — the
  /// promotion-shutdown reorder is the PJ4-side fix). The counter is
  /// shared-owned so a late settle after runtime teardown decrements safely.
  [[nodiscard]] int outstandingPromotions() const { return promotions_outstanding_->load(); }

  /// Write-through: records into the TrustedOrigins ledger AND — only after
  /// the DURABLE ledger write succeeded (adversarial F14) — the in-memory
  /// set. Call on a successful interactive Hello only (spec §7 guard 1).
  /// False = nothing recorded anywhere (transient trust is never held
  /// silently); the caller surfaces a diagnostic.
  [[nodiscard]] bool recordSuccessfulHello(std::string_view uri);
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

  // ---- FAIR pull-admission gate (adversarial F10) --------------------------
  // ONE pull (interactive fetch or provider job) is admitted at a time,
  // FIFO — acquired BEFORE OpenSession so a queued pull holds no remote
  // session/inbox resources while waiting, and held across the pull's
  // on_dataset host-lock release window so the other producer cannot steal
  // the turn. RAII ticket; cancelable wait (bounded poll against the
  // caller's cancel predicate).
  class AdmissionTicket {
   public:
    AdmissionTicket(AdmissionTicket&& other) noexcept;
    AdmissionTicket& operator=(AdmissionTicket&& other) noexcept;
    AdmissionTicket(const AdmissionTicket&) = delete;
    AdmissionTicket& operator=(const AdmissionTicket&) = delete;
    ~AdmissionTicket();

   private:
    friend class ImportRuntime;
    explicit AdmissionTicket(ImportRuntime* runtime) : runtime_(runtime) {}
    ImportRuntime* runtime_ = nullptr;
  };
  /// Blocks FIFO until admitted or `cancelled()` returns true (polled every
  /// `poll`); nullopt = cancelled while waiting.
  [[nodiscard]] std::optional<AdmissionTicket> acquireAdmission(
      const std::function<bool()>& cancelled, std::chrono::milliseconds poll);

  // ---- dataset-lifetime lease registry (round-3 THE RULE) ------------------
  // The runtime retains ONE shared read lease per finalized/served/queried
  // identity for the TOOLBOX-INSTANCE lifetime, so another process's
  // cleanup/eviction (exclusive on the same sidecar) can never unlink a cache
  // file that live datasets may lazily re-open (a promoted source's loader
  // cold-reads chunks long after finalize).
  //
  // THREE INVARIANTS, applied uniformly — the individual races the reviews
  // found were all symptoms of violating one of them:
  //
  //  1. ATOMIC BOOKKEEPING. Every step below is ONE operation on this one
  //     mutex-guarded, identity-keyed, REFCOUNTED registry. There is no
  //     has-then-release (or check-then-act) two-step in production code, so
  //     a racing retain can never be erased unnoticed.
  //  2. LEASE-THEN-VALIDATE. Retention ALWAYS acquires the shared sidecar
  //     lease FIRST and validates the artifact UNDER it. An evictor needs
  //     the EXCLUSIVE lock on that same sidecar, so while our shared lease is
  //     held the file cannot go away: the outcome is either
  //     leased-and-present or no-lease — never a lease pinning a missing
  //     file, never a validated file that is evicted before we lease it.
  //  3. RESTORE BEFORE THE TICKET. A materialization drops the lease to take
  //     the exclusive lock; the restore runs while the in-process
  //     MaterializeTicket is STILL held (the ticket is what stops a
  //     same-process materializer from stealing the window). The OS lock may
  //     be released first — shared acquisition contends correctly.
  //
  // REFCOUNT semantics: retains stack (repeat queries take another reference
  // instead of a second flock); the ONLY thing that drops references is a
  // materialization, which drops them ALL (it needs the exclusive lock) and
  // restores the same count afterwards. There is deliberately no per-holder
  // release yet: releasing when a DATASET dies needs a host dataset-deletion
  // callback the SDK does not expose (recorded SDK follow-up), so until then
  // leases die with the runtime (destructor). The refcount is maintained now
  // so that follow-up is a local change.
  struct LeaseDrop {
    bool had_lease = false;
    unsigned refs = 0;
  };

  /// The distinct, actionable REFUSAL-WHILE-REFERENCED error (round-5 F2).
  /// Matched verbatim by the provider terminal and the tests.
  static constexpr const char* kArtifactInUseError =
      "cache artifact is in use by loaded data — close or reload it, then retry";

  /// Live reference count for `identity` in this runtime's lease registry
  /// (0 = unreferenced). One atomic registry read.
  [[nodiscard]] unsigned leaseRefs(std::string_view identity) const;

  /// Round-5 F1: a MOVE-ONLY RAII owner of a lease-drop token. From the
  /// moment a drop happens, exactly ONE ScopedLeaseDrop owns the obligation
  /// to restore it; every handoff (provider -> PullRequest -> CacheTee) is a
  /// noexcept move, and an unwind through ANY owner restores via the
  /// destructor — stranding is impossible by construction, not by ordering
  /// care. dismiss() marks the token consumed (a successful finalize);
  /// restoreNow() restores eagerly at the ordered point (abortAndCleanup,
  /// before the ticket releases). With round-5 F2's refusal-while-referenced
  /// in place, production drops are always EMPTY tokens (a referenced
  /// identity is never materialized); the mechanism stays load-bearing for
  /// the adopted-token paths and as the structural exception-safety net.
  class ScopedLeaseDrop {
   public:
    ScopedLeaseDrop() = default;
    ScopedLeaseDrop(ImportRuntime* runtime, std::string identity, LeaseDrop drop) noexcept
        : runtime_(runtime), identity_(std::move(identity)), drop_(drop) {}
    ScopedLeaseDrop(ScopedLeaseDrop&& other) noexcept
        : runtime_(other.runtime_), identity_(std::move(other.identity_)), drop_(other.drop_) {
      other.runtime_ = nullptr;
      other.drop_ = LeaseDrop{};
    }
    ScopedLeaseDrop& operator=(ScopedLeaseDrop&& other) noexcept {
      if (this != &other) {
        restoreNow();
        runtime_ = other.runtime_;
        identity_ = std::move(other.identity_);
        drop_ = other.drop_;
        other.runtime_ = nullptr;
        other.drop_ = LeaseDrop{};
      }
      return *this;
    }
    ScopedLeaseDrop(const ScopedLeaseDrop&) = delete;
    ScopedLeaseDrop& operator=(const ScopedLeaseDrop&) = delete;
    ~ScopedLeaseDrop() { restoreNow(); }

    /// Absorb another token's obligation (ADDITION — independent drops
    /// removed independent reference sets; round-4 R3). noexcept: no
    /// allocation, just bookkeeping + emptying `other`.
    void merge(ScopedLeaseDrop&& other) noexcept {
      if (!other.drop_.had_lease) {
        other.runtime_ = nullptr;
        return;
      }
      if (!drop_.had_lease) {
        runtime_ = other.runtime_;
        identity_ = std::move(other.identity_);
        drop_ = other.drop_;
      } else {
        drop_.refs += other.drop_.refs;  // identities equal by contract
      }
      other.runtime_ = nullptr;
      other.drop_ = LeaseDrop{};
    }

    /// Restore immediately (idempotent; the token empties).
    void restoreNow() noexcept {
      if (runtime_ != nullptr && drop_.had_lease) {
        runtime_->restoreLease(identity_, drop_);
      }
      runtime_ = nullptr;
      drop_ = LeaseDrop{};
    }
    /// Mark consumed (the finalize handoff superseded it) — no restore.
    void dismiss() noexcept {
      runtime_ = nullptr;
      drop_ = LeaseDrop{};
    }
    /// Guarantee the eventual restore re-pins at least one reference even if
    /// nothing was dropped (the revalidate-race arm pins a fresh artifact).
    void ensureRestoresAtLeastOne(ImportRuntime* runtime, std::string identity) noexcept {
      if (!drop_.had_lease) {
        runtime_ = runtime;
        identity_ = std::move(identity);
        drop_ = LeaseDrop{true, 1};
      }
    }
    [[nodiscard]] bool hasLease() const noexcept { return drop_.had_lease; }
    [[nodiscard]] unsigned refs() const noexcept { return drop_.refs; }

   private:
    ImportRuntime* runtime_ = nullptr;
    std::string identity_;
    LeaseDrop drop_;
  };

  /// The RAII-owning form of dropLeaseForMaterialize (round-5 F1). The
  /// identity string is built BEFORE the atomic drop, so an allocation
  /// failure aborts with the lease still retained — after construction every
  /// step is noexcept.
  [[nodiscard]] ScopedLeaseDrop dropLeaseForMaterializeScoped(std::string_view identity);

  /// Acquire (shared, NON-BLOCKING) + validate + retain for `identity`.
  /// Idempotent/refcounted: an already-retained identity just takes another
  /// reference. False when the lease is unavailable (a cross-process
  /// exclusive holder is rematerializing) or the artifact does not validate
  /// UNDER the lease; `diagnostic` (optional) says which. Bounded: one flock
  /// on the existing sidecar + the same bounded validation lookup() does —
  /// safe on the §6.3 query path.
  [[nodiscard]] bool retainLeaseForLifetime(std::string_view identity, std::string* diagnostic);

  /// Retain an ALREADY-ACQUIRED, already-validated lease (the finalize
  /// handoff and the memory-hit serve both hold one). Same idempotent
  /// refcount; a duplicate simply releases the passed lease. `refs` is the
  /// reference count to record — a successful rematerialization passes the
  /// count its drop removed, so the documented "drops them all and restores
  /// the same count" holds on the SUCCESS path too (round-4 R3).
  void adoptLeaseForLifetime(std::string_view identity, FileLock lease, unsigned refs = 1);

  /// ATOMIC drop of every reference for `identity` — one operation that both
  /// releases the lease and reports what was released, so the restore can
  /// never disagree with a racing retain (invariant 1). A materialization
  /// must do this before taking the exclusive sidecar lock.
  [[nodiscard]] LeaseDrop dropLeaseForMaterialize(std::string_view identity);

  /// Restore what `drop` recorded (invariant 2 + 3): lease-then-validate,
  /// with a BOUNDED short retry while a cross-process exclusive holder still
  /// owns the sidecar. After that window we give up WITHOUT a lease and
  /// record a diagnostic — the artifact is being rematerialized externally,
  /// and pinning a stale generation would be worse than not pinning. No-op
  /// when the token holds no lease. CALL BEFORE RELEASING THE TICKET.
  void restoreLease(std::string_view identity, const LeaseDrop& drop);

  /// The last restore/retain failure diagnostic (empty when none). Surfaces
  /// the deliberately-quiet give-up above; read by the owning path and the
  /// tests.
  [[nodiscard]] std::string lastLeaseDiagnostic() const;

  /// Diagnostics/tests only — NEVER the first half of a check-then-act (see
  /// invariant 1; production paths use the atomic operations above).
  [[nodiscard]] bool hasRetainedLease(std::string_view identity) const;
  /// Test seam: shrink the bounded restore retry window.
  void setLeaseRestoreRetryForTest(unsigned attempts, std::chrono::milliseconds poll) {
    lease_retry_attempts_ = attempts;
    lease_retry_poll_ = poll;
  }

 private:
  void endMaterialize(const std::string& identity);

  SessionFileCache file_cache_;
  SessionFileCache::Config cache_config_{};
  TrustedOrigins trusted_;
  // shared_ptr so the promotion callback can hold a WEAK handle: a result
  // settling after this runtime's teardown locks either a live cache
  // (shared ownership keeps the object alive through the update) or
  // nothing — never a dangling reference.
  std::shared_ptr<SessionCache> session_cache_ = std::make_shared<SessionCache>();
  std::mutex host_write_mu_;

  mutable std::mutex promo_mu_;
  std::optional<PJ::SourcePromotionHostView> promotion_host_;
  std::shared_ptr<std::atomic<int>> promotions_outstanding_ = std::make_shared<std::atomic<int>>(0);

  mutable std::mutex trust_mu_;
  std::unordered_set<std::string> trusted_keys_;  // trustedOriginKey shape

  std::mutex active_mu_;
  std::unordered_set<std::string> active_identities_;

  // The lease registry (see THE RULE above). `lease_mu_` is a LEAF lock: the
  // flock + validation happen OUTSIDE it (the lease is held continuously
  // across both, which is what makes lease-then-validate sound), and only the
  // map mutation is guarded.
  struct LeaseEntry {
    FileLock lock;
    unsigned refs = 0;
  };
  bool insertLeaseLocked(const std::string& key, FileLock lease, unsigned refs);
  [[nodiscard]] std::optional<FileLock> acquireValidatedLease(std::string_view identity,
                                                              std::string* diagnostic);
  mutable std::mutex lease_mu_;
  std::unordered_map<std::string, LeaseEntry> retained_leases_;
  mutable std::mutex lease_diag_mu_;
  std::string last_lease_diagnostic_;
  unsigned lease_retry_attempts_ = 5;                        // bounded restore window
  std::chrono::milliseconds lease_retry_poll_{20};           // ~100 ms total by default

  // F10 admission gate: a FIFO queue of waiter records; the releaser hands
  // the (held) gate to the front waiter directly, so admission order is
  // arrival order — never mutex-wakeup luck.
  void releaseAdmission();
  struct AdmissionWaiter {
    std::condition_variable cv;
    bool granted = false;
  };
  std::mutex adm_mu_;
  std::deque<AdmissionWaiter*> adm_queue_;
  bool adm_held_ = false;
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
  /// under the lock (spec §5 corruption policy). `adopted_ticket` (PR-3): a
  /// caller that already holds the in-process registry slot for this SAME
  /// identity (the provider job's bounded lock-wait) hands it over instead
  /// of self-contending on a second tryBeginMaterialize. `adopted_lock`
  /// (adversarial F9): same handoff for the CROSS-PROCESS materialize lock
  /// after the job's bounded cancelable wait — begin() then never
  /// re-acquires either (it would self-contend on the sidecar).
  ///
  /// Round-5 F1: `adopted_lease_drop` is the MOVE-ONLY RAII token, captured
  /// into this tee (with the ticket and lock) in a NO-THROW prologue before
  /// anything that can throw — no exception path can strand any of them.
  /// Round-5 F2 — REFUSAL-WHILE-REFERENCED: begin() refuses an identity the
  /// lease registry still references (kArtifactInUseError). Loaded data
  /// lazily re-opens the artifact by generation-specific chunk offsets, and
  /// a rename-over with a logically-equal but byte-different re-encoding
  /// would silently corrupt those reads; refusing converts that into an
  /// honest failure. Consequences: the normal flow is unaffected (a
  /// valid+referenced artifact classifies as a HIT long before
  /// materialization); the §6.1 vanished-while-referenced refetch still
  /// serves eagerly but does NOT republish the cache until the references
  /// die (runtime teardown); the under-lock corruption deletion is never
  /// reached for a referenced identity. With the refusal in place begin()
  /// performs NO lease drop of its own — the token member only carries
  /// adopted tokens, which dissolves the round-4 handoff re-pin controversy
  /// structurally (the restore path only ever handles a zero-or-adopted
  /// count).
  [[nodiscard]] bool begin(const std::string& identity, std::string* error,
                           std::optional<ImportRuntime::MaterializeTicket> adopted_ticket = std::nullopt,
                           std::optional<SessionFileCache::MaterializeLock> adopted_lock = std::nullopt,
                           ImportRuntime::ScopedLeaseDrop adopted_lease_drop = {});

  /// True iff the last begin() failure was the F2 refusal (identity
  /// referenced by loaded data). Owning thread only.
  [[nodiscard]] bool beginRefusedInUse() const { return begin_refused_in_use_; }

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
  /// false once the tee has failed (writer error) OR an abort was requested
  /// (requestAbort — the wait is cancellable cross-thread) — the caller
  /// drops the tee and the ingest continues (§9.6).
  [[nodiscard]] bool enqueue(const DecodedMessage& message);

  /// Cross-thread abort signal (any thread; e.g. a GUI cancel while the pull
  /// thread is blocked in enqueue backpressure behind a stalled disk). Both
  /// CV waits treat it like a failure: the blocked producer returns false
  /// promptly and the writer thread exits at its next wakeup. NOTE: a write
  /// already INSIDE a hung disk syscall is not interruptible — the eventual
  /// abortAndCleanup join still waits that one write out (kernel reality,
  /// same as the pre-tee inline export write).
  void requestAbort();
  /// True once requestAbort was called. Distinguishes a cancel-driven
  /// enqueue==false (classify kAborted) from a disk failure (kFailed).
  [[nodiscard]] bool abortRequested() const;

  /// True once the writer thread latched a disk/mcap failure. Used with
  /// abortRequested() to classify a false enqueue(); also the PR-3 provider
  /// job's terminal-mapping query surface.
  [[nodiscard]] bool failed() const;
  [[nodiscard]] std::string failureError() const;

  /// Test seams — set BEFORE openWriter (which starts the writer thread):
  /// shrink the bounded queue / stall the writer deterministically so a test
  /// can pin a producer genuinely blocked on the backpressure wait.
  void setQueueCapacityForTest(std::size_t max_messages) { max_queued_messages_ = max_messages; }
  void setWriterHookForTest(std::function<void(const DecodedMessage&)> hook) {
    writer_hook_for_test_ = std::move(hook);
  }
  /// Round-3 F1 seam: runs inside abortAndCleanup at the EXACT point between
  /// releasing the OS lock and restoring the lease — i.e. inside the window
  /// invariant 3 protects with the still-held ticket. A test drives a rival
  /// same-process materializer here to prove it cannot steal that window.
  void setPreRestoreHookForTest(std::function<void()> hook) {
    pre_restore_hook_for_test_ = std::move(hook);
  }
  /// Round-5 F1 seam: make begin() throw AFTER its no-throw capture
  /// prologue — pins that an exception inside begin() cannot strand an
  /// adopted token (the tee's teardown restores it under the ticket).
  void setBeginThrowForTest(bool fail) { begin_throw_for_test_ = fail; }
  /// Round-4 R1(a) seam: force begin()'s exclusive-lock acquisition to fail.
  /// The real trigger (an external process winning the microscopic window
  /// between our lease drop and our lock try) is not deterministically
  /// constructible — and a test cannot hold that external lock while our own
  /// shared lease is retained, since the two are mutually exclusive by
  /// design. This drives the same failure ARM.
  void setLockFailForTest(bool fail) { lock_fail_for_test_ = fail; }
  /// R1(a) seam: force the finalize-time lease handoff to fail (the platform
  /// downgrade race is not deterministically constructible from outside).
  void setLeaseHandoffFailForTest(bool fail) { lease_handoff_fail_for_test_ = fail; }
  /// Injectable writer-thread factory (adversarial F8): a test injects a
  /// throwing factory to pin that spawn failure degrades to the documented
  /// NONFATAL tee failure (partial removed, ingest continues) instead of an
  /// exception escaping into the caller.
  void setThreadFactoryForTest(std::function<std::thread(std::function<void()>)> factory) {
    thread_factory_for_test_ = std::move(factory);
  }

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
  bool begin_lock_contended_ = false;  // F9 (see beginFailedOnLockContention)
  bool lease_handoff_fail_for_test_ = false;  // R1(a) seam (see below)
  bool lock_fail_for_test_ = false;           // round-4 R1(a) seam
  bool begin_throw_for_test_ = false;         // round-5 F1 seam (see below)
  bool begin_refused_in_use_ = false;         // round-5 F2 (see beginRefusedInUse)
  std::optional<ImportRuntime::MaterializeTicket> ticket_;
  std::optional<SessionFileCache::MaterializeLock> lock_;
  // Declared AFTER ticket_/lock_ (round-5 F1): member destruction is reverse
  // declaration order, so even a destructor-only teardown restores the token
  // BEFORE the ticket releases — invariant 3 by construction. With the F2
  // refusal this token is EMPTY in production begin() (a referenced identity
  // never reaches a drop); it exists for adopted tokens (the PR-3 layer) and
  // as the structural exception-safety net.
  ImportRuntime::ScopedLeaseDrop lease_drop_;
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

  // Bounded owned-payload queue (queue_mu_ guards everything below except
  // the atomic abort flag, which both CV predicates read so requestAbort can
  // wake them from ANY thread).
  static constexpr std::size_t kMaxQueuedMessages = 4096;
  static constexpr std::size_t kMaxQueuedBytes = 32ull * 1024 * 1024;
  std::size_t max_queued_messages_ = kMaxQueuedMessages;  // test seam
  std::function<void(const DecodedMessage&)> writer_hook_for_test_;
  std::function<void()> pre_restore_hook_for_test_;
  std::function<std::thread(std::function<void()>)> thread_factory_for_test_;
  std::atomic<bool> abort_requested_{false};
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
