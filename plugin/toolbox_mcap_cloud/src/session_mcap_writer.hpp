// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#pragma once

#include <memory>
#include <string>

#include "backend_types.hpp"
#include "decoded_message.hpp"

namespace mcap_cloud {

/// Transport-independent reconstruction of a SessionInfo + DecodedMessage
/// stream into one MCAP. The implementation owns the only writer-specific id
/// mapping and is shared by the GUI worker and the headless CLI.
class SessionMcapWriter {
 public:
  SessionMcapWriter();
  ~SessionMcapWriter();

  SessionMcapWriter(const SessionMcapWriter&) = delete;
  SessionMcapWriter& operator=(const SessionMcapWriter&) = delete;

  [[nodiscard]] bool open(const std::string& path, const SessionInfo& info, std::string* error);
  [[nodiscard]] bool write(const DecodedMessage& message, std::string* error);
  [[nodiscard]] bool close(std::string* error);
  [[nodiscard]] bool isOpen() const;

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace mcap_cloud
