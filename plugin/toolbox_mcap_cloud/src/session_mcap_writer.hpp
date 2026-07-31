// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <string>

#include "backend_types.hpp"
#include "decoded_message.hpp"

namespace mcap {
class IWritable;
}  // namespace mcap

namespace mcap_cloud {

/// Pre-opened exclusive 0600 file sink for the cache tee (spec §5/§9.0): the
/// file is created O_CREAT|O_EXCL, owner-only (CREATE-NEW on Windows), so a
/// symlink or pre-existing file at the partial path fails the open instead of
/// being followed/truncated. Error latching mirrors CheckedFileWriter — the
/// vendored mcap FileWriter ignores short-write/flush/close failures, so the
/// sink records them for the caller to observe (error()). Buffered stdio;
/// single-threaded like SessionMcapWriter (hand-off between threads must be
/// sequenced by the caller).
class ExclusiveFileSink {
 public:
  ExclusiveFileSink();
  ~ExclusiveFileSink();  // closes best-effort; call closeFile() to observe errors

  ExclusiveFileSink(const ExclusiveFileSink&) = delete;
  ExclusiveFileSink& operator=(const ExclusiveFileSink&) = delete;

  /// Exclusive-create `path` 0600. False (with *error) when the path already
  /// exists or the open fails; no file is left behind on failure.
  [[nodiscard]] bool open(const std::filesystem::path& path, std::string* error);
  /// The mcap sink view. Valid only after a successful open(), until closeFile().
  [[nodiscard]] mcap::IWritable& writable();
  /// Latched write/flush/close error ("" = healthy so far).
  [[nodiscard]] const std::string& error() const;
  /// Flush + close, latching any failure into error(). Idempotent.
  void closeFile();

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
};

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
  /// Open over a caller-owned sink (the caller owns file creation policy —
  /// exclusive create, permissions, fsync — and MUST keep `sink` alive until
  /// after close()). Used by the future cache tee; the path overload remains
  /// the convenience for export/CLI (it delegates here — one init path for
  /// the schema/channel dictionaries).
  [[nodiscard]] bool open(mcap::IWritable& sink, const SessionInfo& info, std::string* error);
  [[nodiscard]] bool write(const DecodedMessage& message, std::string* error);
  /// Embed a named metadata record (e.g. the canonical source descriptor,
  /// name "mcap_cloud/source_descriptor"). Call between open() and the first
  /// write() — the record participates in the summary's MetadataIndex; after
  /// the first write() it fails (a mid-stream Metadata record would split the
  /// open chunk).
  [[nodiscard]] bool writeMetadata(const std::string& name,
                                   const std::string& value_json, std::string* error);
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
