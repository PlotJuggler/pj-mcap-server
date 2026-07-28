// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include "backend_types.hpp"
#include "decoded_message.hpp"

namespace mcap_cloud {

/// Transport-independent reconstruction of a SessionInfo + DecodedMessage
/// stream into one MCAP. The implementation owns the only writer-specific id
/// mapping and is shared by every MCAP-reconstruction call site (currently the
/// GUI worker's export tee and the headless CLI).
///
/// Error contract: each call returns false with the reason in `*error`;
/// `*error` is only meaningful after a false return.
///
/// Threading: single-threaded — owned and driven by exactly one thread (the
/// dialog worker / the CLI main thread); no internal locking.
///
/// Finalize contract: close() writes footer + summary regardless of how the
/// record stream ended, so a partially-streamed file stays a READABLE MCAP.
/// The destructor closes best-effort only (finalize failures are unobservable
/// there) — call close() to observe them.
class SessionMcapWriter {
 public:
  SessionMcapWriter();
  ~SessionMcapWriter();

  SessionMcapWriter(const SessionMcapWriter&) = delete;
  SessionMcapWriter& operator=(const SessionMcapWriter&) = delete;

  [[nodiscard]] bool open(const std::filesystem::path& path, const SessionInfo& info, std::string* error);
  [[nodiscard]] bool write(const DecodedMessage& message, std::string* error);
  [[nodiscard]] bool close(std::string* error);

  /// Messages skipped because their topic_id was absent from the session
  /// dictionary (a violated server invariant — skipped defensively so one
  /// stray record never aborts a multi-GiB stream; see write()).
  [[nodiscard]] std::uint64_t skippedUnknownTopics() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace mcap_cloud
