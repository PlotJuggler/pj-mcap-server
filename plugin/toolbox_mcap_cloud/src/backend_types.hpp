// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Local replacements for the mosaico_sdk flight/types.hpp structs. The cloud
// connector plugin is a visual copy of toolbox_mosaico with the Apache Arrow /
// Arrow Flight / gRPC / mosaico_sdk transport removed and replaced by an inert
// stub (backend_connection.hpp). These structs carry only the fields the
// dialog + worker actually read; the arrow::Schema field of the SDK's TopicInfo
// is flattened into a plain (name,type) string vector so the Info panel can
// still render a "Fields (N):" block without Arrow.
//
// This header is deliberately self-contained — it must NOT transitively include
// any flight/* / arrow/* header. When the real WS+Protobuf client-core lands,
// these types stay (or are repopulated) behind the same names.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace mcap_cloud {

// Replaces mosaico::SequenceInfo. The dialog reads: name, min_ts_ns, max_ts_ns,
// total_size_bytes, user_metadata (→ SequenceRecord.metadata via the Lua
// filter). created_at_ns / resource_locator / sessions were "future use" and
// unread by the dialog → dropped. user_metadata kept as a flat string map
// (matches the SDK shape: std::unordered_map<string,string>).
// One effective tag plus the layer it came from. The flat user_metadata map
// carries key->value for the Lua filter, but it cannot express WHICH layer a
// tag came from (embedded vs override). The tag editor needs that distinction
// to tint override rows, so SequenceInfo carries a parallel `tags` vector built
// from FileSummary.tags (which has is_override). The flat map and this vector
// describe the SAME effective set; `tags` simply adds the is_override bit.
struct TagRow {
  std::string key;
  std::string value;
  bool is_override = false;
};

struct SequenceInfo {
  std::string name;
  std::int64_t min_ts_ns = 0;
  std::int64_t max_ts_ns = 0;
  std::int64_t total_size_bytes = 0;
  std::unordered_map<std::string, std::string> user_metadata;
  // Effective tags WITH the per-tag override bit (FileSummary.tags). Same
  // effective set as user_metadata; used by the tag editor to tint overrides.
  std::vector<TagRow> tags;
};

// Replaces mosaico::TopicInfo. The dialog/worker read: topic_name, ontology_tag,
// user_metadata, min_ts_ns, max_ts_ns, total_size_bytes, chunks_number,
// created_at_ns, completed_at_ns (optional), locked, resource_locator. The
// std::shared_ptr<arrow::Schema> `schema` field is replaced by a flat
// schema_fields vector of (name,type) string pairs so buildTopicInfoText can
// still render a "Fields (N):" block without Arrow. ticket_bytes (the Flight
// ticket) is dropped — it was transport-internal and unread by the dialog.
struct TopicInfo {
  std::string topic_name;
  std::string ontology_tag;
  std::vector<std::pair<std::string, std::string>> schema_fields;  // (field_name, type_string)
  std::unordered_map<std::string, std::string> user_metadata;
  std::int64_t min_ts_ns = 0;
  std::int64_t max_ts_ns = 0;
  std::int64_t total_size_bytes = 0;
  std::int64_t chunks_number = 0;
  std::int64_t created_at_ns = 0;
  std::optional<std::int64_t> completed_at_ns;
  bool locked = false;
  std::string resource_locator;
  // ADDITIVE (WS+Protobuf client-core): the MCAP wire TopicInfo carries the
  // per-topic message count and its schema name/encoding. message_count is a
  // first-class field so the Info panel / future per-topic estimates can read
  // it directly; schema_name/schema_encoding are ALSO folded into schema_fields
  // (as a "schema"/"encoding" pseudo-row pair) so the existing Arrow-free
  // "Fields (N):" renderer keeps working unchanged. Defaults keep every prior
  // construction site (sortTopicsLocked, the inert path) compiling untouched.
  std::int64_t message_count = 0;
};

// Status-carrying result of a topics request. A FAILED request (timeout, dead
// socket, server error, unknown sequence) is NOT the same as a sequence that
// genuinely has zero topics: callers that cache topics per sequence must only
// cache when ok is true, or a transient failure becomes a sticky empty Topics
// panel (the "topics only show after reconnect" bug).
struct TopicsResult {
  bool ok = false;
  std::string error;  // human-readable reason when !ok
  std::vector<TopicInfo> topics;
};

// Replaces mosaico::TimeRange (used to pass [start,end] to the backend).
struct TimeRange {
  std::optional<std::int64_t> start_ns;
  std::optional<std::int64_t> end_ns;
};

// Replaces mosaico::ServerVersion (BackendConnection::version()/connect()).
struct ServerVersion {
  std::string version;  // canonical string, e.g. "0.1.0"
};

// HelloResponse.backend (proto type BackendCapabilities): storage-backend-shaped
// client hints the server advertises at handshake time. The plugin learns these
// WITHOUT learning which storage backend (S3 vs GCS) sits behind the server —
// they are abstract capability flags (see proto/pj_cloud.proto BackendCapabilities).
//   supports_file_hierarchy → render a tree/breadcrumb browser (false ⇒ flat table).
//   metadata_key_vocabulary → keys the server knows are searchable; populates the
//                             Lua query-assist dropdowns.
// Parsed once at connect() into BackendConnection::backendCapabilities(); the CLI
// `hello` verb surfaces it. NOTE: the wire field is `backend` (its TYPE is
// BackendCapabilities) — the C++ accessor is hello_response().backend().
struct BackendCaps {
  bool supports_file_hierarchy = false;
  std::vector<std::string> metadata_key_vocabulary;
};

// HelloResponse.capabilities (proto type Capabilities): protocol-level feature
// flags, orthogonal to BackendCaps' storage-backend-shaped UI hints.
//   resume_supported    → true in v1 (reconnect-and-resume is always offered).
//   tag_edit_supported   → false means the catalog is READ-ONLY *and* no
//                          tag-edit IPC forwarder is configured on the server
//                          side (post-M6: the Python builder is the sole
//                          catalog writer; the Go server only forwards
//                          UpdateTags over a unix-socket IPC when one is wired
//                          up) — every UpdateTags the client sent would be
//                          rejected. The client MUST NOT offer a tag-edit UI
//                          it knows will fail (D2 contract). NOTE: this is a
//                          HANDSHAKE-TIME snapshot; daemon liveness is still
//                          re-checked at use time on the server, so
//                          tag_edit_supported==true does not itself guarantee
//                          a later UpdateTags succeeds.
// Parsed once at connect() into BackendConnection::serverCapabilities(); the
// CLI `hello` verb surfaces it and `tag`/updateTags() gate on it.
struct ServerCaps {
  bool resume_supported = false;
  bool tag_edit_supported = false;
};

// ---------------------------------------------------------------------------
// Session / streaming (Slice 2). These mirror the wire OpenSessionResponse
// bindings (names + schemas cross the wire ONCE; everything afterwards is
// uint32-keyed). The session client hands these to consumers (the CLI's MCAP
// writer, a future PJ4 DataSource) so they can register schemas/channels before
// the MessageBatch stream arrives.
// ---------------------------------------------------------------------------

// One entry of OpenSessionResponse.topic_id_map: a small uint32 topic_id bound
// to a topic name + the schema it references + its message encoding.
struct SessionTopic {
  std::uint32_t topic_id = 0;
  std::string topic_name;
  std::uint32_t schema_id = 0;       // references SessionSchema; 0 = no schema
  std::string message_encoding;      // e.g. "cdr", "protobuf"
};

// One entry of OpenSessionResponse.schemas: a small uint32 schema_id bound to a
// schema name + its definition language (encoding) + its raw bytes.
struct SessionSchema {
  std::uint32_t schema_id = 0;
  std::string name;
  std::string encoding;              // schema definition language
  std::string data;                  // schema bytes (e.g. .proto, .msg), owned
};

// The pre-flight result of OpenSessionRequest{fresh}: the subscription handle the
// rest of the session is routed by, the stitched horizon, the server's estimates,
// and the topic/schema dictionaries.
struct SessionInfo {
  std::uint64_t subscription_id = 0;
  std::int64_t merged_start_ns = 0;
  std::int64_t merged_end_ns = 0;
  std::uint64_t estimated_chunk_bytes = 0;  // server fetch budget (upper bound)
  std::uint64_t approximate_messages = 0;   // exact w/ MessageIndex, else upper bound
  std::vector<SessionTopic> topics;
  std::vector<SessionSchema> schemas;
};

// How a session terminated, mapped from the wire Eos.reason. UNSET means the
// stream ended without a terminal Eos (e.g. socket dropped mid-stream).
enum class SessionEos {
  Unset,
  Complete,
  Cancelled,
  Error,
};

// Running + final session counters, returned by the download loop. These are the
// client-side ground truth the live test asserts against (received message COUNT).
struct SessionStats {
  std::uint64_t messages_received = 0;   // messages handed to the sink
  std::uint64_t bytes_received = 0;      // sum of RAW (decompressed) payload bytes handed to the sink
  std::uint64_t wire_bytes_received = 0; // WS payload bytes received off the wire (compressed batch bodies + control frames)
  std::uint64_t batches_received = 0;
  std::uint64_t eos_total_messages_sent = 0;  // server's Eos.total_messages_sent
  std::uint64_t eos_total_bytes_sent = 0;
  SessionEos eos = SessionEos::Unset;
  std::string error;  // non-empty iff the session failed (eos==Error or transport)
};

// Parameters for OpenSessionRequest{fresh}. topics empty = all union topics.
// start_ns/end_ns set together select an optional time window (default = the
// stitched union of the selected files).
struct OpenSessionParams {
  // KEY-ADDRESSED (wire v2): the selection is the durable s3_keys (sequence
  // names verbatim), never catalog rowids — rowids renumber across builder
  // rebuilds, so an id captured at browse time could name a DIFFERENT object
  // by open time. The server resolves the keys in its CURRENT generation.
  std::vector<std::string> s3_keys;
  std::vector<std::string> topic_names;
  std::optional<std::int64_t> start_ns;
  std::optional<std::int64_t> end_ns;
  // Latched / transient-local replay (OpenFresh.include_latched): when true AND a
  // time window is set, the server also delivers, per requested topic absent from
  // the window, its last message before start_ns -- so map/costmap/static-pose
  // topics published once at the start survive time-windowing. Default OFF (keeps
  // CLI round-trip + smoke exact); the GUI opts in.
  bool include_latched = false;
};

}  // namespace mcap_cloud
