// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// session_decode — the pure, socket-free batch decoder for the session/streaming
// path of the Dexory Cloud WS+Protobuf client.
//
// It turns one decoded pj_cloud.v1 MessageBatch into a flat list of decoded
// messages (topic_id / schema_id / times / RAW payload), applying exactly the
// wire semantics of design spec §6.4:
//
//   * BODY_ENCODING_ZSTD (the default path): `body` is ONE self-contained ZSTD
//     frame holding a marshaled MessageBatchBody. Decompress once (one-shot,
//     NO cross-batch decoder context — the resume invariant), VERIFY the
//     decompressed length equals body_uncompressed_size, parse the
//     MessageBatchBody, take the inner RAW payloads verbatim.
//   * BODY_ENCODING_NONE / UNSPECIFIED (singleton / legacy fallback): the
//     messages ride in the legacy `messages` field with per-message
//     payload_encoding — RAW verbatim, ZSTD one-shot decode, or LZ4 frame decode
//     (inbound only; the v1 server never emits LZ4 but the client must decode it).
//   * Any other body_encoding is REJECTED with a clean error (defensive parsing):
//     a future encoding must never be silently misinterpreted.
//
// This is deliberately host-free and socket-free so it can be unit-tested
// against hand-built protobuf MessageBatch messages (see
// tests/session_decode_test.cpp). The BackendConnection (which owns the
// WebSocket) calls decodeBatch() for each inbound batch.
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "decoded_message.hpp"  // DecodedMessage (protobuf-free)
#include "pj_cloud.pb.h"

namespace dexory_cloud {

// Decode one MessageBatch into its messages. Returns true on success (out filled,
// in batch order). On failure returns false and sets *error to a clean,
// human-readable reason; *out is left in an unspecified-but-safe state and must
// not be consumed. Stateless: no decoder context is carried across calls (the
// one-shot-per-batch resume invariant, spec §6.4).
[[nodiscard]] bool decodeBatch(const pj_cloud::v1::MessageBatch& batch, std::vector<DecodedMessage>* out,
                               std::string* error);

}  // namespace dexory_cloud
