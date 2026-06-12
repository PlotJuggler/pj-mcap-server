// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT

#include "backend_connection.hpp"

#include <ixwebsocket/IXWebSocket.h>

#include <algorithm>
#include <chrono>
#include <thread>
#include <utility>

#include "pj_cloud.pb.h"
#include "session_decode.hpp"
#include "wire_mapping.hpp"

namespace dexory_cloud {

namespace {

constexpr std::uint32_t kProtocolVersion = 1;

// The canonical wire mounts the WebSocket at /api/ws on top of the user's host
// URI. Accept ws:// and wss://; append the path (avoiding a double slash if the
// user already typed a trailing one).
std::string buildWsUrl(const std::string& uri) {
  std::string url = uri;
  while (!url.empty() && url.back() == '/') {
    url.pop_back();
  }
  url += "/api/ws";
  return url;
}

bool isWssScheme(const std::string& uri) {
  return uri.rfind("wss://", 0) == 0;
}

// Human-readable name for an ErrorCode, used when the server's message field is
// empty so the user still gets a meaningful failure reason.
std::string errorCodeName(pj_cloud::v1::ErrorCode code) {
  switch (code) {
    case pj_cloud::v1::ERROR_AUTH_FAILED:
      return "authentication failed";
    case pj_cloud::v1::ERROR_PROTOCOL_VERSION:
      return "protocol version mismatch";
    case pj_cloud::v1::ERROR_NOT_FOUND:
      return "not found";
    case pj_cloud::v1::ERROR_INVALID_REQUEST:
      return "invalid request";
    case pj_cloud::v1::ERROR_RESOURCE_LIMIT:
      return "resource limit";
    case pj_cloud::v1::ERROR_S3_UNAVAILABLE:
      return "storage unavailable";
    case pj_cloud::v1::ERROR_INTERNAL:
      return "internal server error";
    case pj_cloud::v1::ERROR_RESUME_NOT_POSSIBLE:
      return "resume not possible";
    default:
      return "server error";
  }
}

// Surface a wire Error verbatim (message preferred, code name as fallback).
std::string formatWireError(const pj_cloud::v1::Error& err) {
  if (!err.message().empty()) {
    return err.message();
  }
  return errorCodeName(err.code());
}

}  // namespace

BackendConnection::BackendConnection(std::string uri, std::string cert_path, std::string api_key, bool allow_insecure)
    : uri_(std::move(uri)),
      cert_path_(std::move(cert_path)),
      api_key_(std::move(api_key)),
      allow_insecure_(allow_insecure) {}

BackendConnection::~BackendConnection() {
  if (socket_) {
    socket_->stop();
  }
}

std::uint64_t BackendConnection::nextRequestId() {
  std::lock_guard<std::mutex> lock(mu_);
  return next_request_id_++;
}

void BackendConnection::onBinaryFrame(const std::string& bytes) {
  auto message = std::make_shared<pj_cloud::v1::ServerMessage>();
  if (!message->ParseFromString(bytes)) {
    return;  // undecodable frame — drop it; the waiting RPC will time out
  }
  const std::uint64_t request_id = message->request_id();
  std::lock_guard<std::mutex> lock(mu_);

  if (request_id != 0) {
    // request_id-keyed RPC path (Hello / ListFiles / GetFile / OpenSession
    // response — OpenSessionResponse echoes the request_id, so it lands here).
    auto it = pending_.find(request_id);
    if (it == pending_.end()) {
      return;  // no waiter (late/duplicate frame) — drop
    }
    it->second.ready = true;
    it->second.message = std::move(message);
    cv_.notify_all();
    return;
  }

  // request_id == 0: a SESSION frame (MessageBatch / Progress / Eos, or a
  // stream-fatal Error carrying subscription_id). Only accumulate while a
  // download is active; stray late frames after Eos are dropped.
  if (session_active_ &&
      (message->has_batch() || message->has_progress() || message->has_eos() || message->has_error())) {
    session_inbox_.push_back(std::move(message));
    cv_.notify_all();
  }
}

bool BackendConnection::sendAndWait(pj_cloud::v1::ClientMessage& request, pj_cloud::v1::ServerMessage* response_out,
                                    std::chrono::seconds timeout) {
  if (!socket_) {
    return false;
  }
  const std::uint64_t request_id = nextRequestId();
  request.set_request_id(request_id);

  std::string payload;
  if (!request.SerializeToString(&payload)) {
    return false;
  }

  {
    std::lock_guard<std::mutex> lock(mu_);
    if (socket_closed_) {
      return false;
    }
    pending_[request_id] = Pending{};
  }

  const auto send_info = socket_->sendBinary(payload);
  if (!send_info.success) {
    std::lock_guard<std::mutex> lock(mu_);
    pending_.erase(request_id);
    return false;
  }

  std::shared_ptr<pj_cloud::v1::ServerMessage> reply;
  {
    std::unique_lock<std::mutex> lock(mu_);
    const bool got = cv_.wait_for(lock, timeout, [&] {
      auto it = pending_.find(request_id);
      return socket_closed_ || (it != pending_.end() && it->second.ready);
    });
    auto it = pending_.find(request_id);
    if (got && it != pending_.end() && it->second.ready) {
      reply = std::move(it->second.message);
    }
    pending_.erase(request_id);
  }

  if (!reply) {
    return false;  // timeout or socket closed before the reply arrived
  }
  if (response_out != nullptr) {
    *response_out = std::move(*reply);
  }
  return true;
}

bool BackendConnection::buildAndOpenSocket(std::string* error_out) {
  auto set_error = [error_out](const std::string& msg) {
    if (error_out != nullptr) {
      *error_out = msg;
    }
  };

  socket_ = std::make_unique<ix::WebSocket>();
  socket_->setUrl(buildWsUrl(uri_));
  socket_->disableAutomaticReconnection();
  socket_->disablePerMessageDeflate();
  socket_->setHandshakeTimeout(static_cast<int>(kRequestTimeout.count()));

  if (isWssScheme(uri_)) {
    ix::SocketTLSOptions tls;
    tls.tls = true;
    if (!cert_path_.empty()) {
      tls.caFile = cert_path_;
    }
    if (allow_insecure_) {
      // Skip peer verification AND hostname validation (dev / self-signed).
      tls.caFile = "NONE";
      tls.disable_hostname_validation = true;
    }
    socket_->setTLSOptions(tls);
  }

  socket_->setOnMessageCallback([this](const ix::WebSocketMessagePtr& msg) {
    switch (msg->type) {
      case ix::WebSocketMessageType::Message:
        if (msg->binary) {
          onBinaryFrame(msg->str);
        }
        break;
      case ix::WebSocketMessageType::Open: {
        std::lock_guard<std::mutex> lock(mu_);
        socket_open_ = true;
        cv_.notify_all();
        break;
      }
      case ix::WebSocketMessageType::Close: {
        std::lock_guard<std::mutex> lock(mu_);
        socket_closed_ = true;
        cv_.notify_all();
        break;
      }
      case ix::WebSocketMessageType::Error: {
        std::lock_guard<std::mutex> lock(mu_);
        socket_closed_ = true;
        socket_error_ = msg->errorInfo.reason;
        cv_.notify_all();
        break;
      }
      default:
        break;
    }
  });

  // Reset connection state BEFORE start(): start() is asynchronous (it spawns
  // the connect thread and returns immediately), and a fresh ix::WebSocket
  // rests in ReadyState::Closed until that thread begins connecting — so the
  // ready state must NOT be polled here. We instead wait for the Open / Error /
  // Close events the callback above translates into socket_open_ /
  // socket_closed_ (with the underlying reason captured in socket_error_).
  // A reconnect MUST also clear stale pending_ RPC waiters + the session inbox
  // and reset cancel_requested_ so a prior attempt's frames/false-closed flags
  // can't corrupt this one.
  {
    std::lock_guard<std::mutex> lock(mu_);
    socket_open_ = false;
    socket_closed_ = false;
    socket_error_.clear();
    pending_.clear();
    session_inbox_.clear();
  }

  socket_->start();

  {
    std::unique_lock<std::mutex> lock(mu_);
    const bool signalled =
        cv_.wait_for(lock, kRequestTimeout, [&] { return socket_open_ || socket_closed_; });
    if (!signalled) {
      lock.unlock();
      socket_->stop();
      socket_.reset();
      set_error("timed out connecting to " + buildWsUrl(uri_));
      return false;
    }
    if (!socket_open_) {
      std::string reason = socket_error_;
      lock.unlock();
      socket_->stop();
      socket_.reset();
      set_error("could not open WebSocket to " + buildWsUrl(uri_) +
                (reason.empty() ? "" : ": " + reason));
      return false;
    }
  }
  return true;
}

bool BackendConnection::connect(std::string* error_out) {
  auto set_error = [error_out](const std::string& msg) {
    if (error_out != nullptr) {
      *error_out = msg;
    }
  };

  if (!buildAndOpenSocket(error_out)) {
    return false;
  }

  // Hello handshake. Empty api_key is allowed (dev anonymous).
  pj_cloud::v1::ClientMessage request;
  auto* hello = request.mutable_hello();
  hello->set_protocol_version(kProtocolVersion);
  hello->set_auth_token(api_key_);

  pj_cloud::v1::ServerMessage response;
  if (!sendAndWait(request, &response)) {
    socket_->stop();
    socket_.reset();
    set_error("no response to handshake from " + buildWsUrl(uri_));
    return false;
  }

  if (response.has_error()) {
    set_error(formatWireError(response.error()));
    socket_->stop();
    socket_.reset();
    return false;
  }
  if (!response.has_hello_response()) {
    set_error("unexpected handshake reply from server");
    socket_->stop();
    socket_.reset();
    return false;
  }

  version_ = ServerVersion{response.hello_response().server_version()};
  // Parse the optional BackendCapabilities (HelloResponse.backend). Absent when
  // the server omits it (has_backend()==false): leave backend_caps_ at nullopt.
  if (response.hello_response().has_backend()) {
    const auto& be = response.hello_response().backend();
    BackendCaps caps;
    caps.supports_file_hierarchy = be.supports_file_hierarchy();
    caps.metadata_key_vocabulary.assign(be.metadata_key_vocabulary().begin(),
                                        be.metadata_key_vocabulary().end());
    backend_caps_ = std::move(caps);
  }
  return true;
}

std::optional<ServerVersion> BackendConnection::version() const {
  return version_;
}

std::optional<BackendCaps> BackendConnection::backendCapabilities() const {
  return backend_caps_;
}

std::vector<SequenceInfo> BackendConnection::listSequences() {
  std::vector<SequenceInfo> sequences;
  std::unordered_map<std::string, std::uint64_t> file_ids;
  if (!socket_) {
    return sequences;
  }

  std::string page_token;
  for (;;) {
    pj_cloud::v1::ClientMessage request;
    auto* list = request.mutable_list_files();
    list->set_page_token(page_token);

    pj_cloud::v1::ServerMessage response;
    if (!sendAndWait(request, &response)) {
      break;  // timeout / closed — return whatever we collected
    }
    if (!response.has_list_files()) {
      break;  // error or unexpected payload
    }

    const auto& list_response = response.list_files();
    for (auto& mapped : mapListFilesResponse(list_response)) {
      file_ids[mapped.info.name] = mapped.file_id;
      sequences.push_back(std::move(mapped.info));
    }

    page_token = list_response.next_page_token();
    if (page_token.empty()) {
      break;  // exhausted
    }
  }

  // Publish the freshly-built index atomically (listTopics reads it next).
  file_id_by_name_ = std::move(file_ids);
  return sequences;
}

TopicsResult BackendConnection::listTopicsChecked(const std::string& sequence_name) {
  TopicsResult result;
  if (!socket_) {
    result.error = "not connected";
    return result;
  }
  auto it = file_id_by_name_.find(sequence_name);
  if (it == file_id_by_name_.end()) {
    result.error = "unknown sequence \"" + sequence_name + "\" (not in the catalog index; refresh the list)";
    return result;
  }

  pj_cloud::v1::ClientMessage request;
  request.mutable_get_file()->set_file_id(it->second);

  pj_cloud::v1::ServerMessage response;
  if (!sendAndWait(request, &response)) {
    std::lock_guard<std::mutex> lock(mu_);
    result.error = socket_closed_ ? "connection lost (socket closed)" : "no response to topics request (timeout)";
    return result;
  }
  if (response.has_error()) {
    result.error = formatWireError(response.error());
    return result;
  }
  if (!response.has_get_file()) {
    result.error = "unexpected reply to GetFile";
    return result;
  }
  result.ok = true;
  result.topics = mapGetFileResponseTopics(response.get_file());
  return result;
}

std::vector<TopicInfo> BackendConnection::listTopics(const std::string& sequence_name) {
  return listTopicsChecked(sequence_name).topics;
}

bool BackendConnection::isClosed() {
  std::lock_guard<std::mutex> lock(mu_);
  return socket_closed_;
}

std::optional<TopicInfo> BackendConnection::getTopicMetadata(
    const std::string& sequence_name, const std::string& topic_name) {
  // GetFile carries every topic; pick the one we were asked about. Reusing
  // listTopics keeps the single GetFile path authoritative.
  const std::vector<TopicInfo> topics = listTopics(sequence_name);
  for (const auto& topic : topics) {
    if (topic.topic_name == topic_name) {
      return topic;
    }
  }
  return std::nullopt;
}

bool BackendConnection::updateTags(const std::string& sequence_name,
                                   const std::vector<std::pair<std::string, std::string>>& set_tags,
                                   const std::vector<std::string>& unset_keys, std::vector<TagRow>* effective_out,
                                   std::string* error) {
  auto set_error = [error](const std::string& msg) {
    if (error != nullptr) {
      *error = msg;
    }
  };
  if (!socket_) {
    set_error("not connected");
    return false;
  }
  auto it = file_id_by_name_.find(sequence_name);
  if (it == file_id_by_name_.end()) {
    set_error("unknown sequence '" + sequence_name + "'");
    return false;
  }

  pj_cloud::v1::ClientMessage request;
  auto* update = request.mutable_update_tags();
  update->set_file_id(it->second);
  for (const auto& [key, value] : set_tags) {
    auto* tag = update->add_set_tags();
    tag->set_key(key);
    tag->set_value(value);
  }
  for (const auto& key : unset_keys) {
    update->add_unset_keys(key);
  }

  pj_cloud::v1::ServerMessage response;
  if (!sendAndWait(request, &response)) {
    set_error("no response to UpdateTags (timeout or socket closed)");
    return false;
  }
  if (response.has_error()) {
    set_error(formatWireError(response.error()));
    return false;
  }
  if (!response.has_update_tags()) {
    set_error("unexpected reply to UpdateTags");
    return false;
  }
  if (effective_out != nullptr) {
    *effective_out = mapTags(response.update_tags().effective_tags());
  }
  return true;
}

// ---------------------------------------------------------------------------
// Session / streaming
// ---------------------------------------------------------------------------

bool BackendConnection::sendFrame(const pj_cloud::v1::ClientMessage& request) {
  if (!socket_) {
    return false;
  }
  std::string payload;
  if (!request.SerializeToString(&payload)) {
    return false;
  }
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (socket_closed_) {
      return false;
    }
  }
  return socket_->sendBinary(payload).success;
}

bool BackendConnection::waitSessionFrame(std::chrono::milliseconds timeout, pj_cloud::v1::ServerMessage* out) {
  std::unique_lock<std::mutex> lock(mu_);
  const bool got = cv_.wait_for(lock, timeout, [&] { return socket_closed_ || !session_inbox_.empty(); });
  if (!got) {
    return false;  // timeout with nothing queued
  }
  if (session_inbox_.empty()) {
    return false;  // socket closed before any frame arrived
  }
  auto front = std::move(session_inbox_.front());
  session_inbox_.pop_front();
  *out = std::move(*front);
  return true;
}

std::vector<std::uint64_t> BackendConnection::resolveFileIds(const std::vector<std::string>& sequence_names,
                                                             std::vector<std::string>* missing) const {
  std::vector<std::uint64_t> ids;
  ids.reserve(sequence_names.size());
  for (const auto& name : sequence_names) {
    auto it = file_id_by_name_.find(name);
    if (it == file_id_by_name_.end()) {
      if (missing != nullptr) {
        missing->push_back(name);
      }
      continue;
    }
    ids.push_back(it->second);
  }
  return ids;
}

bool BackendConnection::openSessionFresh(const OpenSessionParams& params, SessionInfo* info, std::string* error) {
  auto set_error = [error](const std::string& msg) {
    if (error != nullptr) {
      *error = msg;
    }
  };
  if (!socket_) {
    set_error("not connected");
    return false;
  }
  if (params.file_ids.empty()) {
    set_error("no file ids selected for session");
    return false;
  }

  pj_cloud::v1::ClientMessage request;
  auto* open = request.mutable_open_session();
  auto* fresh = open->mutable_fresh();
  for (const std::uint64_t id : params.file_ids) {
    fresh->add_file_ids(id);
  }
  for (const auto& topic : params.topic_names) {
    fresh->add_topic_names(topic);
  }
  if (params.start_ns.has_value() && params.end_ns.has_value()) {
    auto* tr = fresh->mutable_time_range();
    tr->set_start_ns(*params.start_ns);
    tr->set_end_ns(*params.end_ns);
  }
  fresh->set_include_latched(params.include_latched);

  // Arm the session inbox BEFORE sending: the server may begin pumping batches
  // (request_id==0) immediately after the OpenSessionResponse, and that response
  // races with the first batch on two different code paths (pending_ vs inbox).
  // Arming first guarantees no early batch is dropped.
  {
    std::lock_guard<std::mutex> lock(mu_);
    session_active_ = true;
    session_subscription_id_ = 0;
    session_inbox_.clear();
    cancel_requested_.store(false);
  }

  pj_cloud::v1::ServerMessage response;
  if (!sendAndWait(request, &response, kOpenSessionTimeout)) {
    std::lock_guard<std::mutex> lock(mu_);
    session_active_ = false;
    session_inbox_.clear();
    set_error("no response to OpenSession (timeout or socket closed)");
    return false;
  }

  if (response.has_error()) {
    std::lock_guard<std::mutex> lock(mu_);
    session_active_ = false;
    session_inbox_.clear();
    set_error(formatWireError(response.error()));
    return false;
  }
  if (!response.has_open_session()) {
    std::lock_guard<std::mutex> lock(mu_);
    session_active_ = false;
    session_inbox_.clear();
    set_error("unexpected reply to OpenSession");
    return false;
  }

  const auto& or_resp = response.open_session();
  SessionInfo out;
  out.subscription_id = or_resp.subscription_id();
  if (or_resp.has_merged_time_range()) {
    out.merged_start_ns = or_resp.merged_time_range().start_ns();
    out.merged_end_ns = or_resp.merged_time_range().end_ns();
  }
  out.estimated_chunk_bytes = or_resp.estimated_chunk_bytes();
  out.approximate_messages = or_resp.approximate_messages();
  out.topics.reserve(static_cast<std::size_t>(or_resp.topic_id_map_size()));
  for (const auto& b : or_resp.topic_id_map()) {
    SessionTopic t;
    t.topic_id = b.topic_id();
    t.topic_name = b.topic_name();
    t.schema_id = b.schema_id();
    t.message_encoding = b.message_encoding();
    out.topics.push_back(std::move(t));
  }
  out.schemas.reserve(static_cast<std::size_t>(or_resp.schemas_size()));
  for (const auto& s : or_resp.schemas()) {
    SessionSchema sc;
    sc.schema_id = s.schema_id();
    sc.name = s.name();
    sc.encoding = s.encoding();
    sc.data = s.data();
    out.schemas.push_back(std::move(sc));
  }

  {
    std::lock_guard<std::mutex> lock(mu_);
    session_subscription_id_ = out.subscription_id;
  }
  *info = std::move(out);
  return true;
}

BackendConnection::FrameLoopResult BackendConnection::runFrameLoop(const SessionInfo& info,
                                                                   const MessageHandler& on_message,
                                                                   std::uint64_t seed_last_seq, bool first_attempt) {
  FrameLoopResult result;
  SessionStats& stats = result.stats;
  result.last_seq = seed_last_seq;

  auto send_ack = [&](std::uint64_t through_seq) {
    pj_cloud::v1::ClientMessage msg;
    auto* ack = msg.mutable_ack();
    ack->set_subscription_id(info.subscription_id);
    ack->set_through_seq(through_seq);
    sendFrame(msg);
  };
  auto send_cancel = [&]() {
    pj_cloud::v1::ClientMessage cancel;
    cancel.mutable_cancel()->set_subscription_id(info.subscription_id);
    sendFrame(cancel);
  };
  auto finish = [&]() {
    std::lock_guard<std::mutex> lock(mu_);
    session_active_ = false;
    session_inbox_.clear();
  };

  constexpr int kAckEveryBatches = 64;
  constexpr auto kAckEveryDur = std::chrono::milliseconds(500);
  constexpr auto kFrameTimeout = std::chrono::seconds(60);

  int batches_since_ack = 0;
  auto last_ack = std::chrono::steady_clock::now();
  std::uint64_t last_seq = seed_last_seq;  // resume cursor: last fully-delivered seq
  std::uint64_t batches_this_pass = 0;     // for the test-drop hook
  std::vector<DecodedMessage> decoded;

  for (;;) {
    // Honor a cancel request before blocking on the next frame.
    if (cancel_requested_.exchange(false)) {
      send_cancel();
      // Keep reading: the server replies with Eos{CANCELLED}, terminating below.
    }

    pj_cloud::v1::ServerMessage msg;
    if (!waitSessionFrame(kFrameTimeout, &msg)) {
      // Transport drop / timeout with no terminal Eos -> RESUMABLE.
      stats.error = "session frame timeout or socket closed mid-stream";
      stats.eos = SessionEos::Unset;
      result.resumable = true;
      result.last_seq = last_seq;
      // Do NOT finish() here: the resume loop may re-arm and continue. The
      // session_inbox_ is reset by buildAndOpenSocket() on the next attempt.
      return result;
    }

    if (msg.has_batch()) {
      const auto& batch = msg.batch();
      std::string derr;
      if (!decodeBatch(batch, &decoded, &derr)) {
        // Defensive decode failure (e.g. unknown body_encoding) — abort and
        // cancel so the server can tear the session down.
        send_cancel();
        stats.error = derr;
        stats.eos = SessionEos::Error;
        finish();
        result.last_seq = last_seq;
        return result;
      }
      bool aborted = false;
      for (const auto& m : decoded) {
        if (!on_message(m)) {
          aborted = true;
          break;
        }
        stats.messages_received++;
        stats.bytes_received += m.payload.size();
      }
      stats.batches_received++;
      last_seq = batch.seq();  // last FULLY-DELIVERED seq (resume cursor)
      if (aborted) {
        send_cancel();
        stats.error = "download aborted by sink";
        stats.eos = SessionEos::Cancelled;
        finish();
        result.last_seq = last_seq;
        return result;
      }
      batches_since_ack++;
      if (batches_since_ack >= kAckEveryBatches || (std::chrono::steady_clock::now() - last_ack) >= kAckEveryDur) {
        send_ack(last_seq);
        batches_since_ack = 0;
        last_ack = std::chrono::steady_clock::now();
      }
      // TEST-ONLY: force a socket drop after N delivered batches on the FIRST
      // attempt only, exercising the real reconnect-resume path against a live
      // server. Reset after firing so the resumed attempts complete normally.
      batches_this_pass++;
      if (first_attempt && test_drop_after_ > 0 && batches_this_pass == test_drop_after_) {
        test_drop_after_ = 0;
        if (socket_) {
          socket_->stop();  // -> Close/Error -> socket_closed_ -> waitSessionFrame fails
        }
      }
    } else if (msg.has_progress()) {
      // Observed only; nothing to write. (A future sink could surface it.)
    } else if (msg.has_eos()) {
      const auto& eos = msg.eos();
      stats.eos_total_messages_sent = eos.total_messages_sent();
      stats.eos_total_bytes_sent = eos.total_bytes_sent();
      switch (eos.reason()) {
        case pj_cloud::v1::EOS_REASON_COMPLETE:
          stats.eos = SessionEos::Complete;
          break;
        case pj_cloud::v1::EOS_REASON_CANCELLED:
          stats.eos = SessionEos::Cancelled;
          break;
        case pj_cloud::v1::EOS_REASON_ERROR:
          stats.eos = SessionEos::Error;
          stats.error = "server reported stream error (Eos ERROR)";
          break;
        default:
          stats.eos = SessionEos::Unset;
          stats.error = "Eos with unspecified reason";
          break;
      }
      // Final ack so the server can prune the retain buffer fully.
      send_ack(last_seq);
      finish();
      result.last_seq = last_seq;
      return result;
    } else if (msg.has_error()) {
      // Stream-fatal Error carrying subscription_id; Eos{ERROR} normally
      // follows but we terminate on the Error itself.
      stats.error = formatWireError(msg.error());
      stats.eos = SessionEos::Error;
      finish();
      result.last_seq = last_seq;
      return result;
    }
  }
}

SessionStats BackendConnection::downloadSession(const SessionInfo& info, const MessageHandler& on_message) {
  if (!socket_) {
    SessionStats stats;
    stats.error = "not connected";
    return stats;
  }
  // Single-pass (no reconnect): preserve the original downloadSession() contract.
  // A transport drop returns stats.eos==Unset, exactly as before.
  FrameLoopResult result = runFrameLoop(info, on_message, /*seed_last_seq=*/0, /*first_attempt=*/true);
  if (result.resumable) {
    // No reconnect requested here; classify the drop as before (eos==Unset, error
    // set, session torn down).
    std::lock_guard<std::mutex> lock(mu_);
    session_active_ = false;
    session_inbox_.clear();
  }
  return result.stats;
}

bool BackendConnection::reconnectAndHello(std::string* error_out) {
  // Tear the dead socket down (if any) before rebuilding.
  if (socket_) {
    socket_->stop();
    socket_.reset();
  }
  if (!buildAndOpenSocket(error_out)) {
    return false;
  }
  pj_cloud::v1::ClientMessage request;
  auto* hello = request.mutable_hello();
  hello->set_protocol_version(kProtocolVersion);
  hello->set_auth_token(api_key_);

  pj_cloud::v1::ServerMessage response;
  if (!sendAndWait(request, &response)) {
    if (error_out != nullptr) {
      *error_out = "no response to handshake on reconnect";
    }
    return false;
  }
  if (response.has_error()) {
    if (error_out != nullptr) {
      *error_out = formatWireError(response.error());
    }
    return false;
  }
  if (!response.has_hello_response()) {
    if (error_out != nullptr) {
      *error_out = "unexpected handshake reply on reconnect";
    }
    return false;
  }
  return true;
}

bool BackendConnection::openSessionResume(std::uint64_t subscription_id, std::uint64_t resume_after_seq,
                                          std::string* error_out, bool* rejected) {
  if (rejected != nullptr) {
    *rejected = false;
  }
  auto set_error = [error_out](const std::string& msg) {
    if (error_out != nullptr) {
      *error_out = msg;
    }
  };
  if (!socket_) {
    set_error("not connected");
    return false;
  }

  pj_cloud::v1::ClientMessage request;
  auto* open = request.mutable_open_session();
  auto* resume = open->mutable_resume();
  resume->set_subscription_id(subscription_id);
  resume->set_resume_after_seq(resume_after_seq);

  // Arm the inbox BEFORE sending (same race fix as openSessionFresh): the server
  // may begin replaying batches immediately after the OpenSessionResponse.
  {
    std::lock_guard<std::mutex> lock(mu_);
    session_active_ = true;
    session_subscription_id_ = subscription_id;
    session_inbox_.clear();
    cancel_requested_.store(false);
  }

  pj_cloud::v1::ServerMessage response;
  if (!sendAndWait(request, &response, kOpenSessionTimeout)) {
    std::lock_guard<std::mutex> lock(mu_);
    session_active_ = false;
    session_inbox_.clear();
    set_error("no response to OpenResume (timeout or socket closed)");
    return false;
  }
  if (response.has_error()) {
    // RESUME_NOT_POSSIBLE (or any wire error) -> verbatim message, flagged as a
    // server rejection so the caller fails CLEANLY (no OpenFresh fallback).
    std::lock_guard<std::mutex> lock(mu_);
    session_active_ = false;
    session_inbox_.clear();
    set_error(formatWireError(response.error()));
    if (rejected != nullptr) {
      *rejected = true;
    }
    return false;
  }
  if (!response.has_open_session()) {
    std::lock_guard<std::mutex> lock(mu_);
    session_active_ = false;
    session_inbox_.clear();
    set_error("unexpected reply to OpenResume");
    return false;
  }
  // Re-armed: the SAME subscription_id/topic_id_map/schemas come back; we only
  // need session_active_ + the inbox primed, which they are.
  return true;
}

SessionStats BackendConnection::downloadSessionResumable(const SessionInfo& info, const MessageHandler& on_message) {
  if (!socket_) {
    SessionStats stats;
    stats.error = "not connected";
    return stats;
  }

  // Cumulative client-side counters across ALL legs (the resume cursor advances
  // per leg; the per-leg SessionStats are merged here so the returned counts are
  // the WHOLE download, gap-free + dupe-free). The terminal leg's Eos fields and
  // eos reason become the cumulative result.
  SessionStats cumulative;
  std::uint64_t resume_cursor = 0;  // last fully-delivered batch seq, persists across attempts
  unsigned attempts_used = 0;       // reconnect attempts consumed (max kMaxResumeAttempts)
  std::string last_reason;

  // Tear the session down + return `cumulative` with the given eos/error.
  auto finalize = [&](SessionEos eos, std::string error) -> SessionStats {
    cumulative.eos = eos;
    cumulative.error = std::move(error);
    std::lock_guard<std::mutex> lock(mu_);
    session_active_ = false;
    session_inbox_.clear();
    return cumulative;
  };

  for (;;) {
    const bool first_attempt = (attempts_used == 0);
    FrameLoopResult result = runFrameLoop(info, on_message, resume_cursor, first_attempt);
    resume_cursor = result.last_seq;

    // Fold this leg's running counters into the cumulative total.
    cumulative.messages_received += result.stats.messages_received;
    cumulative.bytes_received += result.stats.bytes_received;
    cumulative.batches_received += result.stats.batches_received;

    if (!result.resumable) {
      // Terminal leg: carry its Eos totals + reason as the cumulative result.
      cumulative.eos = result.stats.eos;
      cumulative.eos_total_messages_sent = result.stats.eos_total_messages_sent;
      cumulative.eos_total_bytes_sent = result.stats.eos_total_bytes_sent;
      cumulative.error = result.stats.error;
      return cumulative;
    }

    // Resumable transport drop. A cancel observed (or requested) before/around
    // the drop means the user wants to stop — do NOT reconnect.
    if (cancel_requested_.load() || result.stats.eos == SessionEos::Cancelled) {
      return finalize(SessionEos::Cancelled, {});
    }

    last_reason = result.stats.error;

    if (attempts_used >= kMaxResumeAttempts) {
      // Out of attempts: fail cleanly, partial rows kept (eos stays Unset).
      return finalize(SessionEos::Unset, "reconnect failed after " + std::to_string(kMaxResumeAttempts) +
                                             " attempts: " +
                                             (last_reason.empty() ? "transport drop" : last_reason));
    }

    // Backoff before the attempt (spec §10), interruptible by cancel.
    const int backoff_ms = kResumeBackoffMs[attempts_used];
    {
      std::unique_lock<std::mutex> lock(mu_);
      cv_.wait_for(lock, std::chrono::milliseconds(backoff_ms), [&] { return cancel_requested_.load(); });
    }
    if (cancel_requested_.load()) {
      return finalize(SessionEos::Cancelled, {});
    }

    attempts_used++;  // this reconnect attempt is now consumed
    if (resume_hint_) {
      resume_hint_(attempts_used, kMaxResumeAttempts);
    }
    // A cancel requested DURING the hint (or any time before reconnecting) wins:
    // abort the retry loop without a reconnect.
    if (cancel_requested_.load()) {
      return finalize(SessionEos::Cancelled, {});
    }

    // Reconnect + Hello with the same credentials.
    std::string reconnect_err;
    if (!reconnectAndHello(&reconnect_err)) {
      last_reason = reconnect_err.empty() ? std::string("reconnect failed") : reconnect_err;
      continue;  // next attempt (or out-of-attempts on the next loop)
    }

    // OpenResume with the last fully-delivered seq.
    std::string resume_err;
    bool rejected = false;
    if (!openSessionResume(info.subscription_id, resume_cursor, &resume_err, &rejected)) {
      if (rejected) {
        // RESUME_NOT_POSSIBLE: fail CLEANLY with the verbatim message — NO
        // OpenFresh fallback (that would duplicate already-ingested rows).
        return finalize(SessionEos::Error, resume_err);
      }
      last_reason = resume_err.empty() ? std::string("resume failed") : resume_err;
      continue;  // transport failure on resume -> next attempt
    }
    // Re-armed: loop back into runFrameLoop seeded with resume_cursor.
  }
}

void BackendConnection::cancelSession() {
  cancel_requested_.store(true);
  // Nudge any thread blocked in waitSessionFrame so it re-checks promptly.
  std::lock_guard<std::mutex> lock(mu_);
  cv_.notify_all();
}

}  // namespace dexory_cloud
