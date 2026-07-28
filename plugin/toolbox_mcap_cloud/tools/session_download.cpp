// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
#include "session_download.hpp"

#include "decoded_message.hpp"
#include "session_mcap_writer.hpp"

namespace mcap_cloud {

SessionStats downloadToMcap(BackendConnection& conn, const OpenSessionParams& params, const std::string& out_path,
                            SessionInfo* info_out, std::string* error_out) {
  SessionStats stats;
  auto set_error = [&](const std::string& msg) {
    stats.error = msg;
    if (error_out != nullptr) {
      *error_out = msg;
    }
  };

  SessionInfo info;
  std::string open_err;
  if (!conn.openSessionFresh(params, &info, &open_err)) {
    set_error(open_err.empty() ? "openSessionFresh failed" : open_err);
    stats.eos = SessionEos::Error;
    return stats;
  }
  if (info_out != nullptr) {
    *info_out = info;
  }

  SessionMcapWriter writer;
  std::string writer_error;
  if (!writer.open(out_path, info, &writer_error)) {
    set_error(writer_error);
    stats.eos = SessionEos::Error;
    return stats;
  }

  auto on_message = [&](const DecodedMessage& m) -> bool {
    return writer.write(m, &writer_error);
  };

  stats = conn.downloadSession(info, on_message);

  // Always close the writer (flush footer + index) so even a partial file is a
  // readable MCAP. close() is safe regardless of how the stream ended.
  std::string close_error;
  if (!writer.close(&close_error) && writer_error.empty()) {
    writer_error = std::move(close_error);
  }
  if (!writer_error.empty()) {
    set_error(writer_error);
    stats.eos = SessionEos::Error;
  }

  if (!stats.error.empty() && error_out != nullptr) {
    *error_out = stats.error;
  }
  return stats;
}

}  // namespace mcap_cloud
