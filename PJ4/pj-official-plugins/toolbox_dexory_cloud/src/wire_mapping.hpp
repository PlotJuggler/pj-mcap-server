// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Pure wire->local mapping functions for the Dexory Cloud WS+Protobuf client.
//
// These are deliberately socket-free and host-free so they can be unit-tested
// against hand-built protobuf messages (see tests/wire_mapping_test.cpp). The
// BackendConnection (which owns the WebSocket) calls these to turn decoded
// pj_cloud.v1 messages into the dialog's backend_types.hpp structs.
//
// CLIENT-INGEST CONTRACT (unified-plan §3.1): for the list view, one
// FileSummary == one SequenceInfo. The flat per-file metadata used by the Lua
// query filter comes from ListFilesResponse.metadata[file_id], keyed by the
// file id rendered as a DECIMAL STRING, copied verbatim (NO transform) into
// SequenceInfo.user_metadata. SequenceInfo.name is the human-readable s3_key
// (the MCAP filename) so the table shows filenames, not numeric ids; the caller
// keeps the name->file_id map for later GetFile / OpenSession calls.
#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "backend_types.hpp"
#include "pj_cloud.pb.h"

namespace dexory_cloud {

// One mapped sequence plus the file id it came from, so the caller can build
// its name->id index in lock-step with the SequenceInfo list it hands the UI.
struct MappedSequence {
  std::uint64_t file_id = 0;
  SequenceInfo info;
};

// Map a single FileSummary (+ its flat metadata entries, possibly empty) onto a
// SequenceInfo. recorded.start_ns/end_ns -> min/max; size_bytes -> total size;
// the flat metadata map is copied verbatim into user_metadata; FileSummary.tags
// (key/value/is_override) is copied into SequenceInfo.tags so the tag editor can
// tint override rows (the flat map cannot express the override bit).
[[nodiscard]] MappedSequence mapFileSummary(
    const pj_cloud::v1::FileSummary& summary, const pj_cloud::v1::FlatMetadata* metadata);

// Map a repeated pj_cloud.v1.Tag field (FileSummary.tags or
// UpdateTagsResponse.effective_tags) onto the local TagRow vector, verbatim.
[[nodiscard]] std::vector<TagRow> mapTags(
    const ::google::protobuf::RepeatedPtrField<pj_cloud::v1::Tag>& tags);

// Map a whole ListFilesResponse page into MappedSequence entries, pairing each
// FileSummary with its metadata[file_id] (decimal-string key) entry when
// present. Order is preserved (the server already returns a stable order).
[[nodiscard]] std::vector<MappedSequence> mapListFilesResponse(const pj_cloud::v1::ListFilesResponse& response);

// Map a single wire TopicInfo onto the dialog's local TopicInfo. The schema
// name/encoding ride into schema_fields as pseudo-rows so the Arrow-free
// "Fields (N):" block renders them; message_count is also stored first-class.
[[nodiscard]] TopicInfo mapTopicInfo(const pj_cloud::v1::TopicInfo& topic);

// Map the topics of a GetFileResponse into a TopicInfo vector (preserving order).
[[nodiscard]] std::vector<TopicInfo> mapGetFileResponseTopics(const pj_cloud::v1::GetFileResponse& response);

}  // namespace dexory_cloud
