// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT

#include "session_decode.hpp"

// LZ4 frame decode covers ONLY the legacy-singleton inbound path (the v1 server
// never emits LZ4). Under wasm (__EMSCRIPTEN__) the decode core links the
// decoder-only zstd amalgamation but not lz4, so the LZ4 branch is compiled out
// there (it is unreachable against a v1 server anyway). This guard is additive:
// on every NATIVE build __EMSCRIPTEN__ is undefined, so lz4frame.h is included
// and lz4DecodeAll is compiled exactly as before — native behavior is
// byte-for-byte unchanged. See wasm/README.md (LZ4 constraint).
#ifndef __EMSCRIPTEN__
#include <lz4frame.h>
#endif
#include <zstd.h>

#include <cstring>
#include <limits>

namespace dexory_cloud {

namespace {

// One-shot ZSTD frame decode (NO persistent context — spec §6.4 one-shot-per-batch
// invariant). On the default path the caller separately checks the decompressed
// size against body_uncompressed_size; here we just produce the bytes.
// max_bytes caps the allocation: a forged header declaring multi-GiB must fail
// cleanly BEFORE the resize, not OOM the host.
bool zstdDecodeAll(const std::string& in, std::string* out, std::string* error, std::uint64_t max_bytes) {
  const unsigned long long content = ZSTD_getFrameContentSize(in.data(), in.size());
  if (content == ZSTD_CONTENTSIZE_ERROR) {
    *error = "invalid ZSTD frame";
    return false;
  }
  std::size_t cap = 0;
  if (content == ZSTD_CONTENTSIZE_UNKNOWN) {
    // The v1 server always writes content size in its frames; this is only a
    // safety net for an unframed input. Grow generously, within the ceiling.
    cap = in.size() * 8 + 1024;
    if (cap > max_bytes) {
      cap = static_cast<std::size_t>(max_bytes);
    }
  } else {
    if (content > max_bytes) {
      *error = "ZSTD frame declares " + std::to_string(content) + " bytes (over the decode ceiling)";
      return false;
    }
    cap = static_cast<std::size_t>(content);
  }
  out->resize(cap);
  const std::size_t n = ZSTD_decompress(out->data(), out->size(), in.data(), in.size());
  if (ZSTD_isError(n)) {
    *error = std::string("ZSTD decode failed: ") + ZSTD_getErrorName(n);
    return false;
  }
  out->resize(n);
  return true;
}

#ifndef __EMSCRIPTEN__
// LZ4 frame decode (inbound only — the v1 server never emits LZ4; the client
// must still decode it, spec §6.4). Uses a transient frame decompression context
// per call so there is no carried state. NATIVE-ONLY: lz4 is not linked into the
// wasm decode core (the wasm build never reaches this path against a v1 server);
// the guard is additive and leaves the native build unchanged.
bool lz4DecodeAll(const std::string& in, std::string* out, std::string* error, std::uint64_t max_bytes) {
  LZ4F_dctx* dctx = nullptr;
  const LZ4F_errorCode_t cr = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
  if (LZ4F_isError(cr)) {
    *error = std::string("LZ4 ctx create failed: ") + LZ4F_getErrorName(cr);
    return false;
  }
  out->clear();
  std::string scratch;
  scratch.resize(64 * 1024);

  const char* src = in.data();
  std::size_t src_remaining = in.size();
  bool ok = true;
  // Loop until LZ4F_decompress signals frame end (returns 0) or the source is
  // exhausted. It may consume the input in several passes.
  for (;;) {
    std::size_t dst_size = scratch.size();
    std::size_t this_src = src_remaining;
    const std::size_t hint = LZ4F_decompress(dctx, scratch.data(), &dst_size, src, &this_src, nullptr);
    if (LZ4F_isError(hint)) {
      *error = std::string("LZ4 decode failed: ") + LZ4F_getErrorName(hint);
      ok = false;
      break;
    }
    out->append(scratch.data(), dst_size);
    if (out->size() > max_bytes) {
      *error = "LZ4 decode exceeds the decode ceiling";
      ok = false;
      break;
    }
    src += this_src;
    src_remaining -= this_src;
    if (hint == 0) {
      // Frame complete. Trailing bytes after the frame are a protocol
      // violation (the payload is EXACTLY one frame — mirror the envelope
      // decoder's exactly-one-frame rule): silently ignoring them would
      // accept a corrupt/concatenated payload as clean.
      if (src_remaining != 0) {
        *error = "LZ4 decode failed: trailing data after frame end";
        ok = false;
      }
      break;
    }
    if (this_src == 0 && dst_size == 0) {
      // No progress and not complete — truncated/garbage input.
      *error = "LZ4 decode failed: truncated frame";
      ok = false;
      break;
    }
  }
  LZ4F_freeDecompressionContext(dctx);
  return ok;
}
#endif  // !__EMSCRIPTEN__

}  // namespace

bool decodeEncodedEnvelope(const std::string& body, std::uint64_t announced_size, std::string* out) {
  if (announced_size == 0 || announced_size > kMaxDecodedEnvelopeBytes) {
    return false;  // announced size absent or over the ceiling
  }
  if (body.empty() || body.size() > kMaxDecodedEnvelopeBytes) {
    return false;  // compressed body absent or implausibly large
  }
  // Reject trailing/concatenated data: the body must be EXACTLY one frame.
  const std::size_t frame_size = ZSTD_findFrameCompressedSize(body.data(), body.size());
  if (ZSTD_isError(frame_size) || frame_size != body.size()) {
    return false;
  }
  // The frame must declare its content size, and it must equal the announced size
  // (Go's EncodeAll always writes the content size, so a missing one is invalid).
  const unsigned long long content = ZSTD_getFrameContentSize(body.data(), body.size());
  if (content == ZSTD_CONTENTSIZE_ERROR || content == ZSTD_CONTENTSIZE_UNKNOWN) {
    return false;
  }
  if (content != announced_size || content > kMaxDecodedEnvelopeBytes) {
    return false;
  }
  out->resize(static_cast<std::size_t>(content));
  const std::size_t n = ZSTD_decompress(out->data(), out->size(), body.data(), body.size());
  if (ZSTD_isError(n) || n != content) {
    return false;
  }
  return true;
}

bool decodeBatch(const pj_cloud::v1::MessageBatch& batch, std::vector<DecodedMessage>* out, std::string* error,
                 std::uint64_t max_decoded_bytes) {
  out->clear();

  switch (batch.body_encoding()) {
    case pj_cloud::v1::BODY_ENCODING_ZSTD: {
      // Default path: one self-contained ZSTD frame -> marshaled MessageBatchBody.
      std::string raw;
      if (!zstdDecodeAll(batch.body(), &raw, error, max_decoded_bytes)) {
        return false;
      }
      // Hard check: the server announced the uncompressed size; a mismatch means
      // a corrupt/forged frame (spec: verify body_uncompressed_size).
      if (raw.size() != batch.body_uncompressed_size()) {
        *error = "batch body_uncompressed_size mismatch";
        return false;
      }
      // ParseFromArray takes an int; raw.size() is bounded by max_decoded_bytes
      // (<= 512 MiB < INT_MAX above), but guard explicitly so a future ceiling
      // bump can't silently truncate the size into a negative int.
      if (raw.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        *error = "batch body too large to parse";
        return false;
      }
      pj_cloud::v1::MessageBatchBody body;
      // ParseFromArray copies each payload, so the returned DecodedMessage owns
      // its bytes and nothing aliases the decode scratch.
      if (!body.ParseFromArray(raw.data(), static_cast<int>(raw.size()))) {
        *error = "corrupt MessageBatchBody";
        return false;
      }
      out->reserve(static_cast<std::size_t>(body.messages_size()));
      for (const auto& m : body.messages()) {
        // Inner payloads are RAW on this path (the frame did the compressing).
        DecodedMessage dm;
        dm.topic_id = m.topic_id();
        dm.schema_id = m.schema_id();
        dm.log_time_ns = m.log_time_ns();
        dm.publish_time_ns = m.publish_time_ns();
        dm.payload = m.payload();
        out->push_back(std::move(dm));
      }
      return true;
    }

    case pj_cloud::v1::BODY_ENCODING_NONE:
    case pj_cloud::v1::BODY_ENCODING_UNSPECIFIED: {
      // Singleton / legacy fallback: messages ride in `messages`, per-message
      // payload_encoding (RAW / ZSTD / LZ4-inbound).
      out->reserve(static_cast<std::size_t>(batch.messages_size()));
      for (const auto& m : batch.messages()) {
        DecodedMessage dm;
        dm.topic_id = m.topic_id();
        dm.schema_id = m.schema_id();
        dm.log_time_ns = m.log_time_ns();
        dm.publish_time_ns = m.publish_time_ns();
        switch (m.payload_encoding()) {
          case pj_cloud::v1::PAYLOAD_ENCODING_ZSTD: {
            std::string dec;
            if (!zstdDecodeAll(m.payload(), &dec, error, max_decoded_bytes)) {
              return false;
            }
            dm.payload = std::move(dec);
            break;
          }
          case pj_cloud::v1::PAYLOAD_ENCODING_LZ4: {
#ifdef __EMSCRIPTEN__
            // The wasm decode core does not link lz4; the v1 server never emits
            // LZ4, so this path is unreachable in a browser against a v1 server.
            // Reject cleanly rather than silently mis-decode (defensive parsing).
            *error = "LZ4 payload decode not available in wasm build";
            return false;
#else
            std::string dec;
            if (!lz4DecodeAll(m.payload(), &dec, error, max_decoded_bytes)) {
              return false;
            }
            dm.payload = std::move(dec);
            break;
#endif
          }
          case pj_cloud::v1::PAYLOAD_ENCODING_RAW:
          case pj_cloud::v1::PAYLOAD_ENCODING_UNSPECIFIED:
          default:
            // RAW (and the UNSPECIFIED default, which the server only uses as
            // "no compression") — take the bytes verbatim.
            dm.payload = m.payload();
            break;
        }
        out->push_back(std::move(dm));
      }
      return true;
    }

    default:
      // Defensive parsing: never silently misinterpret an unknown body_encoding.
      *error = "unknown body_encoding " + std::to_string(static_cast<int>(batch.body_encoding())) + " (client rejects)";
      return false;
  }
}

}  // namespace dexory_cloud
