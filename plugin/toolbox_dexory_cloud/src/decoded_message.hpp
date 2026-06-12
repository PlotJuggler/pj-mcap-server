// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// DecodedMessage — one message lifted out of a session MessageBatch, the unit
// the session decoder hands to consumers (the CLI MCAP writer, the toolbox
// ParserIngestDriver). Split into this tiny protobuf-free header so consumers
// that only need the struct don't pull in pj_cloud.pb.h / libprotobuf.
// session_decode.hpp includes this and adds the batch decoder over the generated
// protobuf type.
#pragma once

#include <cstdint>
#include <string>

namespace dexory_cloud {

// One decoded message lifted out of a batch. The payload is the final RAW bytes
// (any per-message ZSTD/LZ4 already undone); it is owned so it outlives the
// batch / decompression scratch.
struct DecodedMessage {
  std::uint32_t topic_id = 0;
  std::uint32_t schema_id = 0;
  std::int64_t log_time_ns = 0;
  std::int64_t publish_time_ns = 0;
  std::string payload;  // RAW bytes, owned
};

}  // namespace dexory_cloud
