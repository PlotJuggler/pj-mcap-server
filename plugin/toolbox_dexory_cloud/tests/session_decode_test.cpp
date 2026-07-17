// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC unit test for the session batch decoder (src/session_decode.*). No
// server, no socket, no env gate — it builds MessageBatch protos IN MEMORY,
// compresses bodies with the linked zstd, and asserts the decoder reproduces the
// messages and rejects malformed / unknown inputs per design spec §6.4:
//
//   * BODY_ENCODING_ZSTD: marshal a MessageBatchBody, ZSTD one-shot, set
//     body_uncompressed_size -> decode must reproduce the messages byte-for-byte.
//   * body_uncompressed_size mismatch -> rejected.
//   * BODY_ENCODING_NONE singleton with per-message RAW and per-message ZSTD ->
//     both decode (RAW verbatim, ZSTD decompressed).
//   * Unknown body_encoding -> REJECTED with a clean error (never misinterpreted).
//
// Registered as DexoryCloudSessionDecodeTest; runs in the hermetic CI suite.

#include <gtest/gtest.h>

#include <lz4frame.h>
#include <zstd.h>

#include <string>
#include <vector>

#include "pj_cloud.pb.h"
#include "session_decode.hpp"

namespace {

// One-shot ZSTD compress (test helper — the server side of the wire).
std::string zstdCompress(const std::string& in, int level = 3) {
  const size_t bound = ZSTD_compressBound(in.size());
  std::string out;
  out.resize(bound);
  const size_t n = ZSTD_compress(out.data(), out.size(), in.data(), in.size(), level);
  EXPECT_FALSE(ZSTD_isError(n)) << ZSTD_getErrorName(n);
  out.resize(n);
  return out;
}

pj_cloud::v1::Message makeMessage(std::uint32_t topic_id, std::uint32_t schema_id, std::int64_t log_ns,
                                  std::int64_t pub_ns, const std::string& payload,
                                  pj_cloud::v1::PayloadEncoding enc = pj_cloud::v1::PAYLOAD_ENCODING_RAW) {
  pj_cloud::v1::Message m;
  m.set_topic_id(topic_id);
  m.set_schema_id(schema_id);
  m.set_log_time_ns(log_ns);
  m.set_publish_time_ns(pub_ns);
  m.set_payload_encoding(enc);
  m.set_payload(payload);
  return m;
}

// One-shot LZ4 frame compress (test helper — the legacy-singleton wire side).
std::string lz4Compress(const std::string& in) {
  const size_t bound = LZ4F_compressFrameBound(in.size(), nullptr);
  std::string out;
  out.resize(bound);
  const size_t n = LZ4F_compressFrame(out.data(), out.size(), in.data(), in.size(), nullptr);
  EXPECT_FALSE(LZ4F_isError(n)) << LZ4F_getErrorName(n);
  out.resize(n);
  return out;
}

}  // namespace

// LZ4 singleton round-trip: the inbound-only LZ4 leg must recover the payload
// verbatim (previously untested — the known "LZ4/frame-loop test gap").
TEST(DexoryCloudSessionDecode, Lz4SingletonRoundTrip) {
  const std::string payload = "lz4-payload-0123456789-abcdefghij";
  pj_cloud::v1::MessageBatch batch;
  batch.set_body_encoding(pj_cloud::v1::BODY_ENCODING_NONE);
  *batch.add_messages() =
      makeMessage(1, 1, 100, 100, lz4Compress(payload), pj_cloud::v1::PAYLOAD_ENCODING_LZ4);

  std::vector<dexory_cloud::DecodedMessage> out;
  std::string error;
  ASSERT_TRUE(dexory_cloud::decodeBatch(batch, &out, &error)) << error;
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].payload, payload);
}

// Trailing bytes after a complete LZ4 frame are a protocol violation, exactly
// like the envelope decoder's exactly-one-frame rule — silently ignoring them
// would accept a corrupt/concatenated payload as clean.
TEST(DexoryCloudSessionDecode, Lz4TrailingGarbageRejected) {
  std::string wire = lz4Compress("payload-that-decodes-fine");
  wire += "TRAILING-GARBAGE";
  pj_cloud::v1::MessageBatch batch;
  batch.set_body_encoding(pj_cloud::v1::BODY_ENCODING_NONE);
  *batch.add_messages() = makeMessage(1, 1, 100, 100, wire, pj_cloud::v1::PAYLOAD_ENCODING_LZ4);

  std::vector<dexory_cloud::DecodedMessage> out;
  std::string error;
  EXPECT_FALSE(dexory_cloud::decodeBatch(batch, &out, &error))
      << "trailing bytes after the LZ4 frame must be rejected";
}

// A frame truncated mid-stream must fail with a clean error, never silently
// deliver a short payload.
TEST(DexoryCloudSessionDecode, Lz4TruncatedFrameRejected) {
  std::string wire = lz4Compress(std::string(4096, 'x'));
  wire.resize(wire.size() / 2);  // truncate mid-frame
  pj_cloud::v1::MessageBatch batch;
  batch.set_body_encoding(pj_cloud::v1::BODY_ENCODING_NONE);
  *batch.add_messages() = makeMessage(1, 1, 100, 100, wire, pj_cloud::v1::PAYLOAD_ENCODING_LZ4);

  std::vector<dexory_cloud::DecodedMessage> out;
  std::string error;
  EXPECT_FALSE(dexory_cloud::decodeBatch(batch, &out, &error))
      << "truncated LZ4 frame must be rejected";
}

// Default path: a ZSTD-compressed MessageBatchBody round-trips to the exact
// messages, with the inner RAW payloads recovered verbatim.
TEST(DexoryCloudSessionDecode, ZstdBodyRoundTrip) {
  pj_cloud::v1::MessageBatchBody body;
  *body.add_messages() = makeMessage(1, 10, 1000, 1000, "hello world");
  *body.add_messages() = makeMessage(2, 11, 2000, 2001, std::string(4096, 'x'));  // > 4KB
  *body.add_messages() = makeMessage(1, 10, 3000, 3000, "");                      // empty payload

  std::string marshaled;
  ASSERT_TRUE(body.SerializeToString(&marshaled));

  pj_cloud::v1::MessageBatch batch;
  batch.set_seq(7);
  batch.set_source_file_id(42);
  batch.set_body_encoding(pj_cloud::v1::BODY_ENCODING_ZSTD);
  batch.set_body_uncompressed_size(marshaled.size());
  batch.set_body(zstdCompress(marshaled));

  std::vector<dexory_cloud::DecodedMessage> out;
  std::string err;
  ASSERT_TRUE(dexory_cloud::decodeBatch(batch, &out, &err)) << err;
  ASSERT_EQ(out.size(), 3u);

  EXPECT_EQ(out[0].topic_id, 1u);
  EXPECT_EQ(out[0].schema_id, 10u);
  EXPECT_EQ(out[0].log_time_ns, 1000);
  EXPECT_EQ(out[0].publish_time_ns, 1000);
  EXPECT_EQ(out[0].payload, "hello world");

  EXPECT_EQ(out[1].topic_id, 2u);
  EXPECT_EQ(out[1].payload, std::string(4096, 'x'));

  EXPECT_EQ(out[2].payload, "");
}

// A body_uncompressed_size that disagrees with the decompressed length is a
// corrupt/forged frame and must be rejected (the size is a hard check).
TEST(DexoryCloudSessionDecode, ZstdBodySizeMismatchRejected) {
  pj_cloud::v1::MessageBatchBody body;
  *body.add_messages() = makeMessage(1, 0, 1, 1, "payload");
  std::string marshaled;
  ASSERT_TRUE(body.SerializeToString(&marshaled));

  pj_cloud::v1::MessageBatch batch;
  batch.set_body_encoding(pj_cloud::v1::BODY_ENCODING_ZSTD);
  batch.set_body_uncompressed_size(marshaled.size() + 1);  // wrong on purpose
  batch.set_body(zstdCompress(marshaled));

  std::vector<dexory_cloud::DecodedMessage> out;
  std::string err;
  EXPECT_FALSE(dexory_cloud::decodeBatch(batch, &out, &err));
  EXPECT_FALSE(err.empty());
}

// A corrupt ZSTD body (not a valid frame) is rejected, not misread.
TEST(DexoryCloudSessionDecode, ZstdBodyCorruptRejected) {
  pj_cloud::v1::MessageBatch batch;
  batch.set_body_encoding(pj_cloud::v1::BODY_ENCODING_ZSTD);
  batch.set_body_uncompressed_size(10);
  batch.set_body("this is not a zstd frame");

  std::vector<dexory_cloud::DecodedMessage> out;
  std::string err;
  EXPECT_FALSE(dexory_cloud::decodeBatch(batch, &out, &err));
  EXPECT_FALSE(err.empty());
}

// Singleton / legacy fallback: messages ride in `messages` with per-message
// encoding. RAW is taken verbatim; ZSTD is one-shot decompressed.
TEST(DexoryCloudSessionDecode, NoneSingletonRawAndZstd) {
  const std::string raw_payload(8192, 'R');
  const std::string zstd_plain(8192, 'Z');
  const std::string zstd_payload = zstdCompress(zstd_plain);

  pj_cloud::v1::MessageBatch batch;
  batch.set_seq(1);
  batch.set_body_encoding(pj_cloud::v1::BODY_ENCODING_NONE);
  *batch.add_messages() = makeMessage(5, 1, 100, 100, raw_payload, pj_cloud::v1::PAYLOAD_ENCODING_RAW);
  *batch.add_messages() = makeMessage(6, 2, 200, 200, zstd_payload, pj_cloud::v1::PAYLOAD_ENCODING_ZSTD);

  std::vector<dexory_cloud::DecodedMessage> out;
  std::string err;
  ASSERT_TRUE(dexory_cloud::decodeBatch(batch, &out, &err)) << err;
  ASSERT_EQ(out.size(), 2u);
  EXPECT_EQ(out[0].topic_id, 5u);
  EXPECT_EQ(out[0].payload, raw_payload);  // verbatim
  EXPECT_EQ(out[1].topic_id, 6u);
  EXPECT_EQ(out[1].payload, zstd_plain);  // decompressed
}

// UNSPECIFIED body_encoding is treated as NONE (back-compat) — messages in the
// legacy field decode normally.
TEST(DexoryCloudSessionDecode, UnspecifiedBodyTreatedAsNone) {
  pj_cloud::v1::MessageBatch batch;
  batch.set_body_encoding(pj_cloud::v1::BODY_ENCODING_UNSPECIFIED);
  *batch.add_messages() = makeMessage(1, 0, 1, 1, "verbatim", pj_cloud::v1::PAYLOAD_ENCODING_RAW);

  std::vector<dexory_cloud::DecodedMessage> out;
  std::string err;
  ASSERT_TRUE(dexory_cloud::decodeBatch(batch, &out, &err)) << err;
  ASSERT_EQ(out.size(), 1u);
  EXPECT_EQ(out[0].payload, "verbatim");
}

// Defensive parsing: an unknown body_encoding value must be REJECTED with a
// clean error, never silently misinterpreted. We force an out-of-range value
// through the enum so a future encoding cannot be misread.
TEST(DexoryCloudSessionDecode, UnknownBodyEncodingRejected) {
  pj_cloud::v1::MessageBatch batch;
  batch.set_body_encoding(static_cast<pj_cloud::v1::BodyEncoding>(99));
  *batch.add_messages() = makeMessage(1, 0, 1, 1, "ignored");

  std::vector<dexory_cloud::DecodedMessage> out;
  std::string err;
  EXPECT_FALSE(dexory_cloud::decodeBatch(batch, &out, &err));
  EXPECT_NE(err.find("unknown body_encoding"), std::string::npos) << err;
}

// ---------------------------------------------------------------------------
// decodeEncodedEnvelope — the compressed-envelope RPC path (hardened decoder).
// The tests mirror the server: zstdCompress() is the server-side encoder, and
// ZSTD_compress writes the frame content size into the header (as Go's EncodeAll
// does), which the decoder requires.
// ---------------------------------------------------------------------------

// A well-formed frame with the correct announced size round-trips exactly.
TEST(DecodeEncodedEnvelope, RoundTripsExact) {
  const std::string payload = "the quick brown fox jumps over the lazy dog, repeatedly. ";
  std::string big;
  for (int i = 0; i < 200; ++i) {
    big += payload;
  }
  const std::string frame = zstdCompress(big);
  std::string out;
  ASSERT_TRUE(dexory_cloud::decodeEncodedEnvelope(frame, big.size(), &out));
  EXPECT_EQ(out, big);
}

// The announced size MUST match the frame's true content size.
TEST(DecodeEncodedEnvelope, RejectsAnnouncedSizeMismatch) {
  const std::string body = "some compressible content some compressible content";
  const std::string frame = zstdCompress(body);
  std::string out;
  EXPECT_FALSE(dexory_cloud::decodeEncodedEnvelope(frame, body.size() + 1, &out));
  EXPECT_FALSE(dexory_cloud::decodeEncodedEnvelope(frame, body.size() - 1, &out));
}

// Zero and over-ceiling announced sizes are rejected before any allocation.
TEST(DecodeEncodedEnvelope, RejectsOutOfBoundsAnnounced) {
  const std::string frame = zstdCompress("hello hello hello");
  std::string out;
  EXPECT_FALSE(dexory_cloud::decodeEncodedEnvelope(frame, 0, &out));
  EXPECT_FALSE(dexory_cloud::decodeEncodedEnvelope(frame, dexory_cloud::kMaxDecodedEnvelopeBytes + 1, &out));
}

// Trailing / concatenated data after a valid frame is a protocol violation.
TEST(DecodeEncodedEnvelope, RejectsTrailingData) {
  const std::string body = "payload payload payload payload";
  std::string frame = zstdCompress(body);
  frame += "GARBAGE";  // a second, junk "frame" appended
  std::string out;
  EXPECT_FALSE(dexory_cloud::decodeEncodedEnvelope(frame, body.size(), &out));
}

// Garbage and empty bodies are rejected cleanly (no crash, no OOM).
TEST(DecodeEncodedEnvelope, RejectsGarbageAndEmpty) {
  std::string out;
  EXPECT_FALSE(dexory_cloud::decodeEncodedEnvelope("not a zstd frame at all", 32, &out));
  EXPECT_FALSE(dexory_cloud::decodeEncodedEnvelope(std::string{}, 32, &out));
}
