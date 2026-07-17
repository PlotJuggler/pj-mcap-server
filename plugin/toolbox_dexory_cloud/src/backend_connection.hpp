// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// BackendConnection — the real WS+Protobuf transport for the cloud connector
// plugin. It speaks the canonical pj_cloud.v1 wire protocol over a single
// ixwebsocket connection: a Hello/HelloResponse handshake, then synchronous
// blocking ListFiles / GetFile request/response RPCs correlated by request_id.
//
// THREADING: every public method here is BLOCKING and is only ever called from
// the FetchWorker's single worker thread (see fetch_worker.cpp), so blocking is
// correct — the GUI thread is never touched. The ixwebsocket library delivers
// inbound frames on its own background thread; we hand each decoded
// ServerMessage to a request_id-keyed waiter via a condition_variable.
//
// This replaces the inert stub (which always failed "backend not implemented").
// The FetchWorker public surface and the dialog wiring are UNCHANGED: only the
// method bodies behind this class do real network work now.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "backend_types.hpp"

namespace ix {
class WebSocket;
}  // namespace ix

namespace pj_cloud::v1 {
class ServerMessage;
class ClientMessage;
class Hello;
}  // namespace pj_cloud::v1

namespace dexory_cloud {

// Defined in session_decode.hpp; only a reference to it appears in this header
// (the MessageHandler typedef), so a forward declaration suffices.
struct DecodedMessage;

class BackendConnection {
 public:
  BackendConnection(std::string uri, std::string cert_path, std::string api_key, bool allow_insecure);
  ~BackendConnection();

  BackendConnection(const BackendConnection&) = delete;
  BackendConnection& operator=(const BackendConnection&) = delete;

  // Open the socket and perform the Hello handshake. On success, returns true
  // and version() is populated. On failure returns false and *error_out (if
  // non-null) carries a human-readable reason — including the verbatim message
  // of any Error{AUTH_FAILED / PROTOCOL_VERSION} the server returns.
  bool connect(std::string* error_out);

  // The server_version from the HelloResponse, or nullopt before a successful
  // connect().
  [[nodiscard]] std::optional<ServerVersion> version() const;

  // The BackendCapabilities (HelloResponse.backend) the server advertised at
  // connect(). nullopt before a successful connect() or when the server omitted
  // the field (has_backend()==false). The dialog/CLI use it for additive UI
  // hints (file hierarchy, query-assist vocabulary) without learning the storage
  // backend.
  [[nodiscard]] std::optional<BackendCaps> backendCapabilities() const;

  // The Capabilities (HelloResponse.capabilities) the server advertised at
  // connect(): resume_supported + tag_edit_supported. nullopt before a
  // successful connect() or when the server omitted the field
  // (has_capabilities()==false) — callers must treat an absent value as
  // "unknown", NOT as false (an older/odd server that omits the field is not
  // asserting read-only; see updateTags()'s gate, which only short-circuits
  // when the value is PRESENT and false).
  [[nodiscard]] std::optional<ServerCaps> serverCapabilities() const;

  // The opaque catalog generation of the last COMPLETE listSequences() (empty
  // before one succeeds). Equality-only; echo it with any dimension-id filter.
  [[nodiscard]] const std::string& catalogGeneration() const { return catalog_generation_; }

  // ListFiles RPC, paging through next_page_token, mapped to SequenceInfo. The
  // internal name->file_id index is rebuilt from the result so listTopics /
  // getTopicMetadata can resolve a sequence name back to its file id.
  //
  // complete (optional out): set false when pagination broke early (timeout,
  // dead socket, or an unexpected reply) — the returned vector is then PARTIAL.
  // On an incomplete listing the internal name->file_id index is NOT replaced
  // with the partial snapshot (the last COMPLETE index is retained), so a
  // dropped page can't silently shrink the browse index; the caller decides
  // whether to surface the partial vector or discard it.
  [[nodiscard]] std::vector<SequenceInfo> listSequences(bool* complete = nullptr);

  // GetFile RPC for the file backing `sequence_name`, addressed by s3_key
  // (sequence_name is sent verbatim as s3_key — see the key-addressing note
  // above the implementation: catalog file ids renumber across
  // external-builder rebuilds, so this RPC no longer depends on the browse
  // name->file_id index being populated or fresh). Mapped to one TopicInfo per
  // topic, with failure DISTINGUISHED from genuinely-zero topics: result.ok is
  // false (with a human-readable result.error) on timeout, dead socket, or a
  // server Error reply (including an unknown key, which the server reports as
  // ERROR_NOT_FOUND). Callers that cache per-sequence topics must use this and
  // only cache ok results.
  [[nodiscard]] TopicsResult listTopicsChecked(const std::string& sequence_name);

  // Convenience wrapper over listTopicsChecked for callers that don't need the
  // status (an error maps to an empty list).
  [[nodiscard]] std::vector<TopicInfo> listTopics(const std::string& sequence_name);

  // True once the underlying socket reported Close/Error. The browse socket
  // never auto-reconnects, so a true here means every later RPC will fail until
  // the owner builds a fresh connection.
  [[nodiscard]] bool isClosed();

  // Full per-topic metadata for the Info panel: a GetFile RPC, returning the
  // matching topic's TopicInfo. nullopt if the sequence/topic is unknown or the
  // RPC fails.
  [[nodiscard]] std::optional<TopicInfo> getTopicMetadata(
      const std::string& sequence_name, const std::string& topic_name);

  // UpdateTags RPC (envelope arm 13) for the file backing `sequence_name`.
  // `set_tags` upserts override (key,value) pairs; `unset_keys` removes an
  // override (or NULL-masks an embedded tag of that key). Addressed by s3_key
  // (sequence_name sent verbatim as s3_key — same key-addressing rationale as
  // listTopicsChecked() above): catalog file ids renumber across
  // external-builder rebuilds, so this RPC is immune to a stale/absent browse
  // name->file_id index — the server resolves the key in its CURRENT
  // generation. On success returns true and fills *effective_out (if non-null)
  // with the server's post-update effective-tags view. On failure returns
  // false and sets *error to the verbatim server Error message (or a transport
  // reason). Blocking, worker-thread only (same as the other RPCs).
  [[nodiscard]] bool updateTags(const std::string& sequence_name,
                                const std::vector<std::pair<std::string, std::string>>& set_tags,
                                const std::vector<std::string>& unset_keys, std::vector<TagRow>* effective_out,
                                std::string* error);

  // -------------------------------------------------------------------------
  // Session / streaming (Slice 2). The GUI-facing catalog surface above is
  // unchanged; these add the bounded-horizon download path of design spec §6.
  // -------------------------------------------------------------------------


  // OpenSessionRequest{fresh}: open a streaming session over the given file_ids
  // (time-ordered), an optional topic subset (empty = all union topics), and an
  // optional time window. On success fills *info with the subscription handle +
  // topic/schema dictionaries + estimates and returns true. On failure returns
  // false and sets *error (the verbatim server Error message, or a transport
  // reason). An EMPTY plan (no surviving topic / empty range) is NOT a failure:
  // *info comes back with zeroed estimates + empty maps and the immediately
  // following Eos{COMPLETE} is consumed by the next downloadSession() call.
  [[nodiscard]] bool openSessionFresh(const OpenSessionParams& params, SessionInfo* info, std::string* error);

  // The callback the download loop invokes for each decoded message, in stream
  // order. The payload is the final RAW bytes (any per-message ZSTD/LZ4 already
  // undone) and is valid only for the duration of the call. Return false to
  // abort the download (the loop then sends a Cancel and returns).
  using MessageHandler = std::function<bool(const DecodedMessage&)>;

  // Drive the MessageBatch stream for the session just opened by
  // openSessionFresh(). Receives each batch, decodes it (ZSTD body / NONE
  // singleton / per-message RAW-ZSTD-LZ4; unknown body_encoding -> clean error),
  // invokes `on_message` per message, and SessionAcks every 64 batches or 500 ms
  // so the server's retain buffer drains. Terminates on Eos (COMPLETE/CANCELLED/
  // ERROR) or a transport drop. Returns the running + final counters; on any
  // failure stats.error is non-empty and stats.eos reflects the cause.
  [[nodiscard]] SessionStats downloadSession(const SessionInfo& info, const MessageHandler& on_message);

  // RECONNECT-RESUME wrapper (Plan D Task 10). Same contract as downloadSession()
  // for COMPLETE / CANCELLED / ERROR / sink-abort, but additionally survives a
  // mid-stream TRANSPORT DROP (Eos never seen): it reconnects (fresh socket +
  // Hello with the same credentials), sends OpenResume{subscription_id,
  // resume_after_seq = last fully-delivered batch seq}, and CONTINUES the SAME
  // download loop + the SAME on_message handler — gap-free AND dupe-free by the
  // server's seq>resume_after_seq replay contract (spec §6.6). Retry policy:
  // spec §10 — up to 3 reconnect attempts with backoff [1s,4s,16s]. A server
  // Error{RESUME_NOT_POSSIBLE} (session evicted/never existed) fails the pull
  // CLEANLY with the verbatim message (stats.eos=Error) — NO silent OpenFresh
  // fallback (that would duplicate already-ingested rows). A cancel honored
  // before/between attempts returns Cancelled. Worker-thread only.
  [[nodiscard]] SessionStats downloadSessionResumable(const SessionInfo& info, const MessageHandler& on_message);

  // Live WS payload bytes received for the active session so far (compressed
  // batch bodies + control frames). Reset at each fresh open; monotonic within a
  // download (incl. resume legs). Thread-safe. Lets the worker emit a live
  // "received" figure next to the "decoded" total during a fetch.
  [[nodiscard]] std::uint64_t sessionWireBytesReceived() const {
    return session_wire_bytes_.load(std::memory_order_relaxed);
  }

  // Optional hint invoked just before each reconnect attempt (attempt is 1-based,
  // max is the attempt cap). Wired by the worker to surface "Resuming (attempt
  // N/3)…" through the dialog. Set once on the worker thread before a download.
  void setResumeHint(std::function<void(unsigned attempt, unsigned max)> hint) { resume_hint_ = std::move(hint); }

  // TEST-ONLY hook: force a socket drop after delivering `n` batches on the FIRST
  // download attempt only (0 = disabled, default). Drives the live resume test
  // against a real server. Reset after firing once so subsequent (resumed)
  // attempts complete normally. NOT used in production paths.
  void testForceDropAfterBatches(unsigned n) { test_drop_after_ = n; }

  // TEST-ONLY: drive openSessionResume() directly (e.g. with a bogus
  // subscription_id) to assert the verbatim RESUME_NOT_POSSIBLE handling without
  // a full download. Returns the same (ok, *error_out, *rejected) contract.
  bool testOpenSessionResume(std::uint64_t subscription_id, std::uint64_t resume_after_seq, std::string* error_out,
                             bool* rejected) {
    return openSessionResume(subscription_id, resume_after_seq, error_out, rejected);
  }

  // Request cancellation of the active session (CancelSession on the priority
  // path). The server responds with Eos{CANCELLED}; downloadSession() observes
  // it and returns. Safe to call from any thread.
  void cancelSession();

 private:
  // Result of one frame-loop pass over a single (re)attached consumer. resumable
  // is true iff the loop exited on a transport drop (no terminal Eos seen) that
  // is eligible for a reconnect-resume. last_seq carries the last fully-delivered
  // batch seq forward across attempts (the resume cursor).
  struct FrameLoopResult {
    SessionStats stats;
    bool resumable = false;
    std::uint64_t last_seq = 0;
  };

  // The inner MessageBatch frame loop, factored out of downloadSession() so the
  // resume wrapper can re-enter it after a reconnect. `seed_last_seq` seeds the
  // resume cursor so it keeps advancing across attempts. `first_attempt` gates
  // the test-drop injection to the first pass only.
  FrameLoopResult runFrameLoop(const SessionInfo& info, const MessageHandler& on_message,
                               std::uint64_t seed_last_seq, bool first_attempt);

  // Build + open a fresh ix::WebSocket exactly as connect() does (same
  // buildWsUrl, TLS opts, callbacks, the socket_open_ vs ReadyState::Closed race
  // fix) and wait for Open. Shared by connect() and reconnectAndHello(). On
  // failure returns false and sets *error_out. Resets socket_/socket_closed_/
  // socket_open_/socket_error_/pending_/session_inbox_ for a clean attempt.
  bool buildAndOpenSocket(std::string* error_out);

  // Reconnect a fresh socket and re-run the Hello handshake (same credentials).
  // Used by the resume loop. On failure returns false + sets *error_out.
  bool reconnectAndHello(std::string* error_out);

  // Send OpenResume{subscription_id, resume_after_seq} and await the
  // OpenSessionResponse, re-arming the session inbox. On success returns true and
  // the frame loop can re-enter. On Error{RESUME_NOT_POSSIBLE} (or any other
  // wire/transport failure) returns false and sets *error_out to the verbatim
  // server message (or a transport reason); *rejected is set true ONLY when the
  // server explicitly rejected the resume (so the caller knows to fail cleanly
  // without further retries).
  bool openSessionResume(std::uint64_t subscription_id, std::uint64_t resume_after_seq, std::string* error_out,
                         bool* rejected);
  // Send `request` (assigning it a fresh request_id) and block until the
  // matching ServerMessage arrives or the timeout elapses. Returns false on
  // send failure / timeout / socket closed. The default timeout fits catalog
  // RPCs (SQLite-backed, fast); OpenSession/OpenResume pass
  // kOpenSessionTimeout because the server builds the per-file chunk plan over
  // object storage BEFORE replying (over WAN that is real seconds, scaling
  // with the stitched file count).
  bool sendAndWait(pj_cloud::v1::ClientMessage& request, pj_cloud::v1::ServerMessage* response_out,
                   std::chrono::seconds timeout = kRequestTimeout);

  // Serialize + send a ClientMessage as-is (no request_id assignment, no wait).
  // Used for fire-and-forget session frames (SessionAck, CancelSession). Returns
  // false on serialization / send failure / closed socket.
  bool sendFrame(const pj_cloud::v1::ClientMessage& request);

  // Block until the next SESSION frame (batch/progress/eos routed by
  // subscription_id, request_id==0) is available, or the socket closes, or the
  // timeout elapses. Returns false on close/timeout; otherwise fills *out.
  bool waitSessionFrame(std::chrono::milliseconds timeout, pj_cloud::v1::ServerMessage* out);

  // Called from the ixwebsocket receive thread for each inbound binary frame.
  void onBinaryFrame(const std::string& bytes);

  // Fills a Hello with the protocol version, auth token, and the response
  // encodings this client can decode (the compressed-envelope opt-in). One
  // builder shared by initial connect() and reconnectAndHello() so the two paths
  // can't drift (Codex review).
  void fillHello(pj_cloud::v1::Hello* hello) const;

  std::uint64_t nextRequestId();

  std::string uri_;        // raw user URI (ws:// or wss://); /api/ws is appended on connect
  std::string cert_path_;  // custom CA bundle for wss (empty = system)
  std::string api_key_;    // bearer token (empty = dev anonymous, allowed)
  bool allow_insecure_;    // wss: skip peer/hostname verification

  std::unique_ptr<ix::WebSocket> socket_;
  std::optional<ServerVersion> version_;
  // HelloResponse.backend, parsed at connect()/reconnectAndHello(). nullopt when
  // the server omits the field (has_backend()==false).
  std::optional<BackendCaps> backend_caps_;
  // HelloResponse.capabilities, parsed at connect() only (reconnectAndHello(),
  // the resume-path re-handshake, does not refresh EITHER of version_/
  // backend_caps_ today — a mid-download reconnect keeps the ORIGINAL
  // connect()'s capabilities rather than re-deriving them, matching that
  // existing convention). nullopt when the server omits the field
  // (has_capabilities()==false).
  std::optional<ServerCaps> server_caps_;

  static constexpr std::chrono::seconds kRequestTimeout{10};
  // OpenSession/OpenResume only: the server answers AFTER loading every
  // selected file's chunk index from object storage (parallelized server-side,
  // but still WAN round trips that scale with the stitched/aggregated file
  // count). 120s gives a wide margin over the measured ~1s/file post-fix cost
  // without making a genuinely dead server hang the worker forever.
  static constexpr std::chrono::seconds kOpenSessionTimeout{120};

  std::mutex mu_;
  std::condition_variable cv_;
  std::uint64_t next_request_id_ = 1;
  bool socket_closed_ = false;
  // Set by the ix Open callback. connect() waits on (socket_open_ ||
  // socket_closed_) — NEVER on ix::WebSocket::getReadyState(): a fresh socket
  // rests in ReadyState::Closed until its background thread starts connecting,
  // so polling the state right after the asynchronous start() misreads that
  // INITIAL Closed as a terminal failure (the "could not open WebSocket" bug).
  bool socket_open_ = false;
  // The ix errorInfo.reason of the last Error event, surfaced to the user.
  std::string socket_error_;
  // request_id -> slot. A waiter inserts an empty slot before sending; the
  // receive thread fills it and signals. ready==false means "still pending".
  struct Pending {
    bool ready = false;
    std::shared_ptr<pj_cloud::v1::ServerMessage> message;
  };
  std::unordered_map<std::uint64_t, Pending> pending_;

  // name->file_id index built from the last listSequences(). std::less<> would
  // need transparent lookup; plain unordered_map with std::string keys is fine.
  std::unordered_map<std::string, std::uint64_t> file_id_by_name_;

  // Opaque catalog generation the last COMPLETE listSequences() was served from
  // (ListFilesResponse.catalog_generation). Dimension ids from GetVocabulary are
  // only meaningful together with this token (echoed via
  // ListFilesRequest.expected_catalog_generation) — kept for the facet-UI
  // follow-up; equality-only, never parsed or logged as text.
  std::string catalog_generation_;

  // ---- session routing -----------------------------------------------------
  // The receive thread fans inbound frames into two paths: request_id-keyed RPC
  // waiters (pending_, above) and the SESSION inbox below. A frame is a session
  // frame when its request_id is 0 AND it is a batch/progress/eos (or an Error
  // carrying a subscription_id). OpenSessionResponse echoes request_id, so it
  // travels the normal pending_ RPC path, NOT the inbox.
  //
  // session_active_ gates inbox accumulation: outside a download the inbox stays
  // empty (stray late frames are dropped). downloadSession() drains it in order.
  bool session_active_ = false;
  std::uint64_t session_subscription_id_ = 0;
  std::atomic<bool> cancel_requested_{false};
  std::deque<std::shared_ptr<pj_cloud::v1::ServerMessage>> session_inbox_;
  // Total WS payload bytes of the SESSION frames received off the wire since the
  // active download began (compressed batch bodies + tiny control frames). Reset
  // at the start of each download entry point and reported as
  // SessionStats.wire_bytes_received. Written on the receive thread (under mu_),
  // read on the caller thread at download end — hence atomic. NOTE: this is the WS
  // payload size, not true network bytes (no TLS/TCP/IP framing overhead).
  std::atomic<std::uint64_t> session_wire_bytes_{0};

  // ---- reconnect-resume (Slice 8) ------------------------------------------
  // Surface "Resuming (attempt N/max)…" through the worker->dialog path.
  std::function<void(unsigned attempt, unsigned max)> resume_hint_;
  // TEST-ONLY forced drop: fire after this many batches on the first attempt.
  unsigned test_drop_after_ = 0;
  // Backoff schedule (spec §10): 3 attempts at 1s / 4s / 16s.
  static constexpr unsigned kMaxResumeAttempts = 3;
  static constexpr int kResumeBackoffMs[kMaxResumeAttempts] = {1000, 4000, 16000};
};

}  // namespace dexory_cloud
