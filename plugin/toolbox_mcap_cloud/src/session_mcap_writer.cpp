// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#include "session_mcap_writer.hpp"

#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <unordered_map>
#include <utility>

#include <mcap/writer.hpp>

#if defined(_WIN32)
#include <fcntl.h>
#include <io.h>
#include <share.h>
#include <sys/stat.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace mcap_cloud {
namespace {

class CheckedFileWriter final : public mcap::IWritable {
 public:
  bool open(const std::filesystem::path& path) {
    errno = 0;  // stale errno would otherwise decorate the failure message
    // The path overload — never path.string(): on Windows that narrows through
    // the active code page and mangles non-ACP profile/directory names.
    stream_.open(path, std::ios::binary | std::ios::trunc);
    if (!stream_.is_open()) {
      error_ = "could not open output MCAP '" + path.string() + "'";
      if (errno != 0) {
        error_ += ": ";
        error_ += std::strerror(errno);
      }
      return false;
    }
    return true;
  }

  void handleWrite(const std::byte* data, std::uint64_t size) override {
    if (!error_.empty()) {
      // Keep size_ advancing after a latched error: mcap::McapWriter reads
      // IWritable::size() for its offset bookkeeping, which must stay
      // self-consistent while the stream is drained to close().
      size_ += size;
      return;
    }
    stream_.write(reinterpret_cast<const char*>(data), static_cast<std::streamsize>(size));
    size_ += size;
    if (!stream_) {
      error_ = "filesystem write failed";
    }
  }

  void end() override {
    if (!stream_.is_open()) {
      return;
    }
    stream_.flush();
    if (!stream_ && error_.empty()) {
      error_ = "filesystem flush failed";
    }
    stream_.close();
    if (stream_.fail() && error_.empty()) {
      error_ = "filesystem close failed";
    }
  }

  void flush() override {
    if (!stream_.is_open() || !error_.empty()) {
      return;
    }
    stream_.flush();
    if (!stream_) {
      error_ = "filesystem flush failed";
    }
  }

  std::uint64_t size() const override {
    return size_;
  }

  [[nodiscard]] const std::string& error() const {
    return error_;
  }

 private:
  std::ofstream stream_;
  std::uint64_t size_ = 0;
  std::string error_;
};

// Stdio-buffered mcap sink over a caller-policy fd (exclusive create, 0600).
// Latches short-write/flush/close failures exactly like CheckedFileWriter —
// the vendored mcap FileWriter swallows them outside debug asserts.
class ExclusiveStdioSink final : public mcap::IWritable {
 public:
  explicit ExclusiveStdioSink(std::FILE* stream) : stream_(stream) {}

  void handleWrite(const std::byte* data, std::uint64_t size) override {
    // Keep size_ advancing after a latched error: mcap::McapWriter reads
    // IWritable::size() for its offset bookkeeping, which must stay
    // self-consistent while the stream is drained to close().
    if (error_.empty() && stream_ != nullptr) {
      const std::size_t written =
          std::fwrite(data, 1, static_cast<std::size_t>(size), stream_);
      if (written != static_cast<std::size_t>(size)) {
        error_ = "filesystem write failed";
      }
    }
    size_ += size;
  }

  void end() override { closeStream(); }

  void flush() override {
    if (stream_ == nullptr || !error_.empty()) {
      return;
    }
    if (std::fflush(stream_) != 0) {
      error_ = "filesystem flush failed";
    }
  }

  std::uint64_t size() const override { return size_; }

  void closeStream() {
    if (stream_ == nullptr) {
      return;
    }
    if (std::fflush(stream_) != 0 && error_.empty()) {
      error_ = "filesystem flush failed";
    }
    if (std::fclose(stream_) != 0 && error_.empty()) {
      error_ = "filesystem close failed";
    }
    stream_ = nullptr;
  }

  [[nodiscard]] const std::string& error() const { return error_; }

 private:
  std::FILE* stream_ = nullptr;
  std::uint64_t size_ = 0;
  std::string error_;
};

}  // namespace

struct ExclusiveFileSink::Impl {
  std::optional<ExclusiveStdioSink> sink;
  std::string open_error;
};

ExclusiveFileSink::ExclusiveFileSink() : impl_(std::make_unique<Impl>()) {}

ExclusiveFileSink::~ExclusiveFileSink() {
  closeFile();
}

bool ExclusiveFileSink::open(const std::filesystem::path& path, std::string* error) {
  auto fail = [error](std::string message) {
    if (error != nullptr) {
      *error = std::move(message);
    }
    return false;
  };
  if (impl_->sink.has_value()) {
    return fail("exclusive sink is already open");
  }
  errno = 0;
#if defined(_WIN32)
  int fd = -1;
  const errno_t open_err = ::_wsopen_s(&fd, path.c_str(),
                                       _O_WRONLY | _O_CREAT | _O_EXCL | _O_BINARY,
                                       _SH_DENYWR, _S_IREAD | _S_IWRITE);
  if (open_err != 0 || fd < 0) {
    return fail("could not exclusively create '" + path.string() + "': " +
                std::strerror(open_err != 0 ? open_err : errno));
  }
  std::FILE* stream = ::_fdopen(fd, "wb");
  if (stream == nullptr) {
    ::_close(fd);
    // Honor the no-file-left-behind promise: the exclusive create above
    // already made the file (POSIX branch does the same removal).
    std::error_code remove_ec;
    std::filesystem::remove(path, remove_ec);
    return fail("could not open stream over '" + path.string() + "'");
  }
#else
  const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    return fail("could not exclusively create '" + path.string() + "': " + std::strerror(errno));
  }
  std::FILE* stream = ::fdopen(fd, "wb");
  if (stream == nullptr) {
    ::close(fd);
    std::error_code remove_ec;
    std::filesystem::remove(path, remove_ec);
    return fail("could not open stream over '" + path.string() + "'");
  }
#endif
  impl_->sink.emplace(stream);
  return true;
}

mcap::IWritable& ExclusiveFileSink::writable() {
  return *impl_->sink;
}

const std::string& ExclusiveFileSink::error() const {
  if (impl_->sink.has_value()) {
    return impl_->sink->error();
  }
  return impl_->open_error;  // empty — never opened / already closed cleanly
}

void ExclusiveFileSink::closeFile() {
  if (impl_->sink.has_value()) {
    impl_->sink->closeStream();
    // Preserve the latched error across the sink's lifetime for error().
    impl_->open_error = impl_->sink->error();
  }
}

struct SessionMcapWriter::Impl {
  // `output` must outlive `writer`: ~McapWriter() calls close(), which writes
  // the footer through the IWritable it was opened with.
  CheckedFileWriter output;
  mcap::McapWriter writer;
  struct ChannelState {
    mcap::ChannelId id = 0;
    std::uint32_t next_sequence = 0;
  };
  std::unordered_map<std::uint32_t, ChannelState> channels_by_topic;
  std::uint64_t skipped_unknown_topics = 0;
  bool open = false;
  // Latched by the first write() call: gates writeMetadata (a Metadata record
  // mid-stream would close the open chunk — see mcap::McapWriter::write).
  bool wrote_message = false;
};

SessionMcapWriter::SessionMcapWriter() : impl_(std::make_unique<Impl>()) {}

SessionMcapWriter::~SessionMcapWriter() {
  if (impl_->open) {
    std::string ignored;
    (void)close(&ignored);
  }
}

bool SessionMcapWriter::open(const std::filesystem::path& path, const SessionInfo& info, std::string* error) {
  auto fail = [error](std::string message) {
    if (error != nullptr) {
      *error = std::move(message);
    }
    return false;
  };
  if (impl_->open) {
    return fail("MCAP writer is already open");
  }
  if (!impl_->output.open(path)) {
    return fail(impl_->output.error());
  }
  // Delegate to the sink overload — the ONE init path for the writer options
  // and the schema/channel dictionaries.
  if (!open(static_cast<mcap::IWritable&>(impl_->output), info, error)) {
    impl_->output.end();
    std::error_code remove_ec;
    std::filesystem::remove(path, remove_ec);
    return false;
  }
  if (!impl_->output.error().empty()) {
    const std::string message = impl_->output.error();
    impl_->writer.terminate();
    impl_->output.end();
    impl_->open = false;
    impl_->channels_by_topic.clear();
    // Nothing valuable was written yet — don't leave a garbage partial behind.
    std::error_code remove_ec;
    std::filesystem::remove(path, remove_ec);
    if (remove_ec) {
      // The caller reports "no file left behind" on open failure — when the
      // remove itself fails (read-only remount, permission flip) that would be
      // a lie, so name the stray file in the error instead.
      return fail(message + " (a stray partial remains at '" + path.string() +
                  "': " + remove_ec.message() + ")");
    }
    return fail(message);
  }
  return true;
}

bool SessionMcapWriter::open(mcap::IWritable& sink, const SessionInfo& info, std::string* error) {
  auto fail = [error](std::string message) {
    if (error != nullptr) {
      *error = std::move(message);
    }
    return false;
  };
  if (impl_->open) {
    return fail("MCAP writer is already open");
  }

  mcap::McapWriterOptions options("");
  options.compression = mcap::Compression::Zstd;
  options.compressionLevel = mcap::CompressionLevel::Default;
  options.chunkSize = 4 * 1024 * 1024;
  // Sink write failures are the caller's to observe (the sink owns the file
  // policy); the path overload layers its CheckedFileWriter check on top.
  impl_->writer.open(sink, options);
  impl_->open = true;
  impl_->wrote_message = false;

  std::unordered_map<std::uint32_t, mcap::SchemaId> schema_id_map;
  for (const auto& session_schema : info.schemas) {
    if (session_schema.schema_id == 0) {
      continue;
    }
    mcap::Schema schema(session_schema.name, session_schema.encoding, session_schema.data);
    impl_->writer.addSchema(schema);
    schema_id_map[session_schema.schema_id] = schema.id;
  }
  for (const auto& session_topic : info.topics) {
    mcap::SchemaId schema_id = 0;
    if (auto it = schema_id_map.find(session_topic.schema_id); it != schema_id_map.end()) {
      schema_id = it->second;
    }
    mcap::Channel channel(session_topic.topic_name, session_topic.message_encoding, schema_id);
    impl_->writer.addChannel(channel);
    impl_->channels_by_topic[session_topic.topic_id] = {channel.id, 0};
  }
  return true;
}

bool SessionMcapWriter::write(const DecodedMessage& message, std::string* error) {
  auto fail = [error](std::string text) {
    if (error != nullptr) {
      *error = std::move(text);
    }
    return false;
  };
  if (!impl_->open) {
    return fail("MCAP writer is not open");
  }
  impl_->wrote_message = true;  // closes the writeMetadata window (see below)
  const auto channel_it = impl_->channels_by_topic.find(message.topic_id);
  if (channel_it == impl_->channels_by_topic.end()) {
    // A topic_id outside the session dictionary is a violated server
    // invariant. Skip defensively with a count rather than fail: one stray
    // record must never abort (or un-save) a multi-GiB stream.
    ++impl_->skipped_unknown_topics;
    return true;
  }

  Impl::ChannelState& channel = channel_it->second;
  mcap::Message record;
  record.channelId = channel.id;
  record.sequence = channel.next_sequence++;
  record.logTime = static_cast<mcap::Timestamp>(message.log_time_ns);
  record.publishTime = static_cast<mcap::Timestamp>(message.publish_time_ns);
  record.dataSize = message.payload.size();
  record.data = reinterpret_cast<const std::byte*>(message.payload.data());
  const mcap::Status status = impl_->writer.write(record);
  if (!status.ok()) {
    return fail("MCAP write failed: " + status.message);
  }
  if (!impl_->output.error().empty()) {
    return fail("MCAP write failed: " + impl_->output.error());
  }
  return true;
}

bool SessionMcapWriter::writeMetadata(const std::string& name, const std::string& value_json,
                                      std::string* error) {
  auto fail = [error](std::string text) {
    if (error != nullptr) {
      *error = std::move(text);
    }
    return false;
  };
  if (!impl_->open) {
    return fail("MCAP writer is not open");
  }
  if (impl_->wrote_message) {
    // mcap::McapWriter::write(const Metadata&) closes the current chunk, so a
    // mid-stream metadata record would split chunks — the provenance hook is
    // an open()-time affair by contract.
    return fail("writeMetadata must be called before the first write()");
  }
  mcap::Metadata metadata;
  metadata.name = name;
  metadata.metadata = {{"json", value_json}};
  const mcap::Status status = impl_->writer.write(metadata);
  if (!status.ok()) {
    return fail("MCAP metadata write failed: " + status.message);
  }
  if (!impl_->output.error().empty()) {
    return fail("MCAP metadata write failed: " + impl_->output.error());
  }
  return true;
}

std::uint64_t SessionMcapWriter::skippedUnknownTopics() const {
  return impl_->skipped_unknown_topics;
}

bool SessionMcapWriter::close(std::string* error) {
  if (!impl_->open) {
    return true;
  }
  impl_->writer.close();
  impl_->open = false;
  if (!impl_->output.error().empty()) {
    if (error != nullptr) {
      *error = "MCAP finalize failed: " + impl_->output.error();
    }
    return false;
  }
  return true;
}

}  // namespace mcap_cloud
