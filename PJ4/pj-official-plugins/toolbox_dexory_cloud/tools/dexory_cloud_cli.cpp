// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// dexory-cloud-cli — a headless command-line driver over the EXACT
// BackendConnection class the GUI plugin uses (no parallel transport). It exists
// so connectivity + catalog correctness against the server/Minio MCAPs can be
// exercised without launching the PlotJuggler GUI: it is the human-facing twin
// of the gtest harness (tests/backend_connection_*_test.cpp).
//
// Commands:
//   hello                          connect + print server_version
//   list [--json]                  sequences: name, time range, size, msg count, metadata
//   topics <sequence-name> [--json]  per-topic name, schema, encoding, message_count
//   download <seq1> [<seq2> ...] --output out.mcap [--topics a,b] [--time-range s,e] [--json]
//                                  open a fresh session and reconstruct a local MCAP.
//                                  Multiple sequences are STITCHED into one
//                                  continuous logical stream via a single OpenFresh
//                                  (file_ids[]); they are time-ordered + must not
//                                  overlap (the server rejects overlapping ranges).
//   tag <sequence-name> [--set k=v]... [--unset k]... [--json]
//                                  edit a file's override tags, then print the
//                                  refreshed effective metadata
//   debug <seq1> [<seq2> ...] [--topics a,b] [--time-range s,e] [--limit N] [--json]
//                                  open a fresh session and print the first N
//                                  decoded messages (topic, log_time, payload
//                                  size) WITHOUT writing a file.
//
// Flags / env:
//   --url   / DEXORY_CLOUD_URL      WS base URI (default ws://localhost:8080)
//   --token / DEXORY_CLOUD_API_KEY  bearer token (may be empty = dev anonymous)
//   --output FILE                   (download) destination MCAP path (required)
//   --topics a,b,c                  (download / debug) topic subset (default: all)
//   --time-range startNs,endNs      (download / debug) optional time window (nanoseconds)
//   --limit N                       (debug) number of messages to print (default 10; 0 = all)
//   --set k=v                       (tag) upsert one override tag (repeatable)
//   --unset k                       (tag) remove an override / mask an embedded tag (repeatable)
//
// Exit codes: 0 success · 1 connection/RPC failure · 2 usage error.

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "backend_connection.hpp"
#include "backend_types.hpp"
#include "cli_url_resolve.hpp"  // resolveCliUrl / resolveCliToken (unit-tested precedence)
#include "decoded_message.hpp"  // DecodedMessage (forward-declared in backend_connection.hpp)
#include "format_utils.h"
#include "session_download.hpp"
#include "stitch_select.h"  // buildStitchedSelection / validateNonOverlapping
#include "time_format.h"  // src/core is on the include path

namespace {

constexpr int kExitOk = 0;
constexpr int kExitFailure = 1;
constexpr int kExitUsage = 2;

// The default WS URL lives in cli_url_resolve.hpp (kDefaultCliUrl) so the
// resolution rule and its unit test share one source of truth.

void printUsage(std::ostream& os) {
  os << "Usage: dexory-cloud-cli [--url URL] [--token TOKEN] <command> [args]\n"
        "\n"
        "Commands:\n"
        "  hello                            connect and print the server version\n"
        "  list [--json]                    list sequences (name, time range, size, count, metadata)\n"
        "  topics <sequence-name> [--json]  list a sequence's topics (name, schema, encoding, count)\n"
        "  download <seq1> [<seq2> ...] --output FILE [--topics a,b] [--time-range s,e] [--latched] [--json]\n"
        "                                   open a session and reconstruct a local MCAP\n"
        "                                   (multiple sequences are stitched, time-ordered)\n"
        "  tag <sequence-name> [--set k=v]... [--unset k]... [--json]\n"
        "                                   edit override tags, then print refreshed metadata\n"
        "  debug <seq1> [<seq2> ...] [--topics a,b] [--time-range s,e] [--limit N] [--json]\n"
        "                                   print the first N decoded messages (no file written)\n"
        "\n"
        "Flags:\n"
        "  --url URL                WS base URI (env DEXORY_CLOUD_URL; default ws://localhost:8080)\n"
        "  --token TOKEN            bearer token (env DEXORY_CLOUD_API_KEY; may be empty)\n"
        "  --insecure               wss://: skip TLS certificate verification (self-signed dev certs)\n"
        "  --output FILE            (download) destination MCAP path (required)\n"
        "  --topics a,b,c           (download/debug) comma-separated topic subset (default: all)\n"
        "  --time-range startNs,endNs (download/debug) optional time window in nanoseconds\n"
        "  --latched                (download) also deliver each topic's last message before the\n"
        "                           window (latched/transient-local replay: map, costmaps, static poses)\n"
        "  --limit N                (debug) number of messages to print (default 10; 0 = all)\n"
        "  --set k=v                (tag) upsert one override tag (repeatable)\n"
        "  --unset k                (tag) remove an override / mask an embedded tag (repeatable)\n"
        "\n"
        "Exit codes: 0 success, 1 connection/RPC failure, 2 usage error.\n";
}

// Split a comma-separated list, trimming surrounding whitespace and dropping
// empty entries (so "a, b ,, c" -> {a, b, c}).
std::vector<std::string> splitCsv(const std::string& csv) {
  std::vector<std::string> out;
  std::stringstream ss(csv);
  std::string item;
  while (std::getline(ss, item, ',')) {
    const auto first = item.find_first_not_of(" \t");
    if (first == std::string::npos) {
      continue;
    }
    const auto last = item.find_last_not_of(" \t");
    out.push_back(item.substr(first, last - first + 1));
  }
  return out;
}

// ISO-8601 UTC, or "-" when the timestamp is unset (0).
std::string isoOrDash(std::int64_t ns) {
  if (ns <= 0) {
    return "-";
  }
  return formatIso8601Utc(ns);
}

// Render a flat metadata map as a stable, JSON-friendly object (nlohmann's
// ordered handling keeps emission deterministic for a given input).
nlohmann::json metadataToJson(const std::unordered_map<std::string, std::string>& meta) {
  nlohmann::json obj = nlohmann::json::object();
  for (const auto& [key, value] : meta) {
    obj[key] = value;
  }
  return obj;
}

// ---- hello ----------------------------------------------------------------

int runHello(dexory_cloud::BackendConnection& conn, const std::string& error) {
  // `error` already carries the connect() failure verbatim when conn failed.
  const auto version = conn.version();
  if (!version.has_value()) {
    std::cerr << "hello: no server version (handshake incomplete): " << error << '\n';
    return kExitFailure;
  }
  std::cout << "server_version: " << version->version << '\n';

  // BackendCapabilities (HelloResponse.backend): storage-backend-shaped client
  // hints. Printed when the server advertised them (has_backend()).
  if (const auto caps = conn.backendCapabilities(); caps.has_value()) {
    std::cout << "supports_file_hierarchy: " << (caps->supports_file_hierarchy ? "true" : "false") << '\n'
              << "metadata_key_vocabulary (" << caps->metadata_key_vocabulary.size() << "):";
    for (const auto& key : caps->metadata_key_vocabulary) {
      std::cout << ' ' << key;
    }
    std::cout << '\n';
  }
  return kExitOk;
}

// ---- list -----------------------------------------------------------------

int runList(dexory_cloud::BackendConnection& conn, bool as_json) {
  const std::vector<dexory_cloud::SequenceInfo> sequences = conn.listSequences();

  if (as_json) {
    nlohmann::json arr = nlohmann::json::array();
    for (const auto& seq : sequences) {
      nlohmann::json item;
      item["name"] = seq.name;
      item["start"] = isoOrDash(seq.min_ts_ns);
      item["end"] = isoOrDash(seq.max_ts_ns);
      item["start_ns"] = seq.min_ts_ns;
      item["end_ns"] = seq.max_ts_ns;
      item["size_bytes"] = seq.total_size_bytes;
      // message_count lives in the flat metadata map (the server publishes it
      // there); surface it as a top-level convenience field too when present.
      if (auto it = seq.user_metadata.find("message_count"); it != seq.user_metadata.end()) {
        item["message_count"] = it->second;
      }
      item["metadata"] = metadataToJson(seq.user_metadata);
      arr.push_back(std::move(item));
    }
    std::cout << arr.dump(2) << '\n';
    return kExitOk;
  }

  std::cout << sequences.size() << " sequence(s)\n";
  for (const auto& seq : sequences) {
    std::cout << "\n- " << seq.name << '\n'
              << "    time:  " << isoOrDash(seq.min_ts_ns) << "  ->  " << isoOrDash(seq.max_ts_ns) << '\n'
              << "    size:  " << dexory_cloud::formatBytes(seq.total_size_bytes) << " (" << seq.total_size_bytes
              << " bytes)\n";
    if (auto it = seq.user_metadata.find("message_count"); it != seq.user_metadata.end()) {
      std::cout << "    messages: " << it->second << '\n';
    }
    std::cout << "    metadata (" << seq.user_metadata.size() << "):\n";
    for (const auto& [key, value] : seq.user_metadata) {
      std::cout << "      " << key << " = " << value << '\n';
    }
  }
  return kExitOk;
}

// ---- topics ---------------------------------------------------------------

int runTopics(dexory_cloud::BackendConnection& conn, const std::string& sequence_name, bool as_json) {
  // listSequences() must run first: it builds the name->file_id index that
  // listTopics() resolves the sequence name against.
  const std::vector<dexory_cloud::SequenceInfo> sequences = conn.listSequences();
  bool known = false;
  for (const auto& seq : sequences) {
    if (seq.name == sequence_name) {
      known = true;
      break;
    }
  }
  if (!known) {
    std::cerr << "topics: unknown sequence '" << sequence_name << "'\n";
    return kExitFailure;
  }

  const std::vector<dexory_cloud::TopicInfo> topics = conn.listTopics(sequence_name);
  if (topics.empty()) {
    std::cerr << "topics: no topics returned for '" << sequence_name << "'\n";
    return kExitFailure;
  }

  // The wire TopicInfo carries schema name/encoding; the local TopicInfo folds
  // them into schema_fields as ("schema",..)/("encoding",..) pseudo-rows.
  auto schemaField = [](const dexory_cloud::TopicInfo& t, const std::string& key) -> std::string {
    for (const auto& [name, value] : t.schema_fields) {
      if (name == key) {
        return value;
      }
    }
    return {};
  };

  if (as_json) {
    nlohmann::json arr = nlohmann::json::array();
    for (const auto& topic : topics) {
      nlohmann::json item;
      item["name"] = topic.topic_name;
      item["schema"] = schemaField(topic, "schema");
      item["encoding"] = schemaField(topic, "encoding");
      item["message_count"] = topic.message_count;
      arr.push_back(std::move(item));
    }
    std::cout << arr.dump(2) << '\n';
    return kExitOk;
  }

  std::cout << topics.size() << " topic(s) in " << sequence_name << '\n';
  for (const auto& topic : topics) {
    std::cout << "\n- " << topic.topic_name << '\n'
              << "    schema:   " << schemaField(topic, "schema") << '\n'
              << "    encoding: " << schemaField(topic, "encoding") << '\n'
              << "    messages: " << topic.message_count << '\n';
  }
  return kExitOk;
}

// ---- download --------------------------------------------------------------

const char* eosName(dexory_cloud::SessionEos eos) {
  switch (eos) {
    case dexory_cloud::SessionEos::Complete:
      return "COMPLETE";
    case dexory_cloud::SessionEos::Cancelled:
      return "CANCELLED";
    case dexory_cloud::SessionEos::Error:
      return "ERROR";
    case dexory_cloud::SessionEos::Unset:
    default:
      return "UNSET";
  }
}

int runDownload(dexory_cloud::BackendConnection& conn, const std::vector<std::string>& sequence_names,
                const std::string& output, const std::vector<std::string>& topics,
                const std::optional<std::int64_t>& start_ns, const std::optional<std::int64_t>& end_ns, bool as_json,
                bool include_latched) {
  if (output.empty()) {
    std::cerr << "download: --output FILE is required\n";
    return kExitUsage;
  }
  if (sequence_names.empty()) {
    std::cerr << "download: at least one <sequence-name> is required\n";
    return kExitUsage;
  }

  // Same-sequence-twice is a usage error (an accidental self-overlap the server
  // would reject with INVALID_REQUEST; we catch it cleanly client-side).
  {
    std::vector<std::string> seen = sequence_names;
    std::sort(seen.begin(), seen.end());
    for (std::size_t i = 1; i < seen.size(); ++i) {
      if (seen[i] == seen[i - 1]) {
        std::cerr << "download: duplicate sequence '" << seen[i] << "'\n";
        return kExitUsage;
      }
    }
  }

  // listSequences() builds the name->file_id index that resolveFileIds() reads.
  const std::vector<dexory_cloud::SequenceInfo> sequences = conn.listSequences();

  // Build a name -> SequenceInfo lookup for the time-ordering + overlap guard.
  std::unordered_map<std::string, const dexory_cloud::SequenceInfo*> by_name;
  for (const auto& seq : sequences) {
    by_name.emplace(seq.name, &seq);
  }

  // Deterministically order the selection by (min_ts_ns, name) — the SAME order
  // the GUI merge uses — so the CLI and GUI produce identical OpenFresh requests
  // regardless of the order names were typed on the command line.
  std::vector<dexory_cloud::SelInput> sel_inputs;
  sel_inputs.reserve(sequence_names.size());
  for (const auto& name : sequence_names) {
    dexory_cloud::SelInput in;
    in.name = name;
    if (auto it = by_name.find(name); it != by_name.end()) {
      in.min_ts_ns = it->second->min_ts_ns;
      in.max_ts_ns = it->second->max_ts_ns;
      in.size_bytes = it->second->total_size_bytes;
    }
    sel_inputs.push_back(std::move(in));
  }

  // Client-side overlap guard (mirrors the GUI; the server stays authoritative).
  if (const std::string overlap = dexory_cloud::validateNonOverlapping(sel_inputs); !overlap.empty()) {
    std::cerr << "download: cannot stitch (" << overlap << ")\n";
    return kExitFailure;
  }

  const dexory_cloud::StitchedSelection stitched = dexory_cloud::buildStitchedSelection(sel_inputs);

  // Resolve the time-ordered names to file_ids (resolveFileIds preserves input
  // order). ALL ids go into ONE OpenFresh — the server stitches them.
  std::vector<std::string> missing;
  const auto file_ids = conn.resolveFileIds(stitched.ordered_names, &missing);
  if (!missing.empty() || file_ids.size() != stitched.ordered_names.size() || file_ids.empty()) {
    std::cerr << "download: unknown sequence";
    if (!missing.empty()) {
      std::cerr << "(s):";
      for (const auto& m : missing) {
        std::cerr << " '" << m << "'";
      }
    }
    std::cerr << '\n';
    return kExitFailure;
  }

  dexory_cloud::OpenSessionParams params;
  params.file_ids = file_ids;
  params.topic_names = topics;
  params.start_ns = start_ns;
  params.end_ns = end_ns;
  params.include_latched = include_latched;

  const std::string display = stitched.display_name;

  dexory_cloud::SessionInfo info;
  std::string error;
  const dexory_cloud::SessionStats stats = dexory_cloud::downloadToMcap(conn, params, output, &info, &error);

  const bool ok = stats.eos == dexory_cloud::SessionEos::Complete && stats.error.empty();

  if (as_json) {
    nlohmann::json obj;
    obj["sequence"] = display;
    obj["output"] = output;
    obj["subscription_id"] = info.subscription_id;
    obj["topic_count"] = info.topics.size();
    obj["schema_count"] = info.schemas.size();
    obj["estimated_chunk_bytes"] = info.estimated_chunk_bytes;
    obj["approximate_messages"] = info.approximate_messages;
    obj["messages_received"] = stats.messages_received;
    obj["bytes_received"] = stats.bytes_received;
    obj["batches_received"] = stats.batches_received;
    obj["eos_reason"] = eosName(stats.eos);
    obj["eos_total_messages_sent"] = stats.eos_total_messages_sent;
    obj["eos_total_bytes_sent"] = stats.eos_total_bytes_sent;
    if (!stats.error.empty()) {
      obj["error"] = stats.error;
    }
    std::cout << obj.dump(2) << '\n';
  } else {
    std::cout << "download " << display << " -> " << output << '\n'
              << "  subscription_id: " << info.subscription_id << '\n'
              << "  topics: " << info.topics.size() << "  schemas: " << info.schemas.size() << '\n'
              << "  estimate: ~" << info.approximate_messages << " messages, "
              << dexory_cloud::formatBytes(static_cast<std::int64_t>(info.estimated_chunk_bytes)) << '\n'
              << "  received: " << stats.messages_received << " messages in " << stats.batches_received
              << " batch(es), " << dexory_cloud::formatBytes(static_cast<std::int64_t>(stats.bytes_received)) << '\n'
              << "  eos: " << eosName(stats.eos) << " (server total_messages_sent=" << stats.eos_total_messages_sent
              << ")\n";
    if (!stats.error.empty()) {
      std::cout << "  error: " << stats.error << '\n';
    }
  }

  return ok ? kExitOk : kExitFailure;
}

// ---- debug ------------------------------------------------------------------

// Resolve one-or-more sequence names to a time-ordered, non-overlapping,
// stitched OpenSessionParams (the SAME ordering/overlap logic runDownload uses,
// minus --output). On a usage/resolution error prints to stderr and returns the
// exit code via *exit_code; returns false then. On success returns true and
// fills *params + *display.
bool resolveStitchedParams(dexory_cloud::BackendConnection& conn, const std::vector<std::string>& sequence_names,
                           const std::vector<std::string>& topics, const std::optional<std::int64_t>& start_ns,
                           const std::optional<std::int64_t>& end_ns, dexory_cloud::OpenSessionParams* params,
                           std::string* display, int* exit_code) {
  if (sequence_names.empty()) {
    std::cerr << "debug: at least one <sequence-name> is required\n";
    *exit_code = kExitUsage;
    return false;
  }
  {
    std::vector<std::string> seen = sequence_names;
    std::sort(seen.begin(), seen.end());
    for (std::size_t i = 1; i < seen.size(); ++i) {
      if (seen[i] == seen[i - 1]) {
        std::cerr << "debug: duplicate sequence '" << seen[i] << "'\n";
        *exit_code = kExitUsage;
        return false;
      }
    }
  }

  const std::vector<dexory_cloud::SequenceInfo> sequences = conn.listSequences();
  std::unordered_map<std::string, const dexory_cloud::SequenceInfo*> by_name;
  for (const auto& seq : sequences) {
    by_name.emplace(seq.name, &seq);
  }

  std::vector<dexory_cloud::SelInput> sel_inputs;
  sel_inputs.reserve(sequence_names.size());
  for (const auto& name : sequence_names) {
    dexory_cloud::SelInput in;
    in.name = name;
    if (auto it = by_name.find(name); it != by_name.end()) {
      in.min_ts_ns = it->second->min_ts_ns;
      in.max_ts_ns = it->second->max_ts_ns;
      in.size_bytes = it->second->total_size_bytes;
    }
    sel_inputs.push_back(std::move(in));
  }

  if (const std::string overlap = dexory_cloud::validateNonOverlapping(sel_inputs); !overlap.empty()) {
    std::cerr << "debug: cannot stitch (" << overlap << ")\n";
    *exit_code = kExitFailure;
    return false;
  }

  const dexory_cloud::StitchedSelection stitched = dexory_cloud::buildStitchedSelection(sel_inputs);
  std::vector<std::string> missing;
  const auto file_ids = conn.resolveFileIds(stitched.ordered_names, &missing);
  if (!missing.empty() || file_ids.size() != stitched.ordered_names.size() || file_ids.empty()) {
    std::cerr << "debug: unknown sequence";
    if (!missing.empty()) {
      std::cerr << "(s):";
      for (const auto& m : missing) {
        std::cerr << " '" << m << "'";
      }
    }
    std::cerr << '\n';
    *exit_code = kExitFailure;
    return false;
  }

  params->file_ids = file_ids;
  params->topic_names = topics;
  params->start_ns = start_ns;
  params->end_ns = end_ns;
  *display = stitched.display_name;
  return true;
}

// `debug`: open a fresh session and print the first `limit` decoded messages
// (topic, log_time, payload size) WITHOUT writing any MCAP file. Returns false
// from the message handler once `limit` messages are printed so the loop sends a
// clean Cancel and returns. Useful for spot-checking what a download will yield.
int runDebug(dexory_cloud::BackendConnection& conn, const std::vector<std::string>& sequence_names,
             const std::vector<std::string>& topics, const std::optional<std::int64_t>& start_ns,
             const std::optional<std::int64_t>& end_ns, std::uint64_t limit, bool as_json) {
  dexory_cloud::OpenSessionParams params;
  std::string display;
  int exit_code = kExitOk;
  if (!resolveStitchedParams(conn, sequence_names, topics, start_ns, end_ns, &params, &display, &exit_code)) {
    return exit_code;
  }

  dexory_cloud::SessionInfo info;
  std::string error;
  if (!conn.openSessionFresh(params, &info, &error)) {
    std::cerr << "debug: openSessionFresh failed: " << (error.empty() ? "unknown error" : error) << '\n';
    return kExitFailure;
  }

  // topic_id -> topic_name, from the session's topic dictionary.
  std::unordered_map<std::uint32_t, std::string> name_by_topic_id;
  for (const auto& t : info.topics) {
    name_by_topic_id.emplace(t.topic_id, t.topic_name);
  }

  struct Row {
    std::string topic;
    std::int64_t log_time_ns = 0;
    std::size_t payload_size = 0;
  };
  std::vector<Row> rows;
  std::uint64_t printed = 0;
  auto on_message = [&](const dexory_cloud::DecodedMessage& msg) -> bool {
    std::string topic_name;
    if (auto it = name_by_topic_id.find(msg.topic_id); it != name_by_topic_id.end()) {
      topic_name = it->second;
    } else {
      topic_name = "topic_id=" + std::to_string(msg.topic_id);
    }
    rows.push_back(Row{topic_name, msg.log_time_ns, msg.payload.size()});
    ++printed;
    // Returning false aborts the download cleanly (sink-abort -> Cancel). When
    // limit==0, take everything (drive to natural EOS).
    return limit == 0 || printed < limit;
  };

  const dexory_cloud::SessionStats stats = conn.downloadSession(info, on_message);

  // A sink-abort after `limit` rows ends as CANCELLED with the documented
  // "download aborted by sink" marker — that is the EXPECTED success path here
  // (we deliberately stop early). A natural COMPLETE (limit==0, or fewer than
  // `limit` messages exist) with no error is also success. Anything else (a
  // transport drop, a server Eos ERROR) is a real failure.
  const bool clean_abort = stats.eos == dexory_cloud::SessionEos::Cancelled && stats.error == "download aborted by sink";
  const bool ok = clean_abort || stats.error.empty();

  if (as_json) {
    nlohmann::json obj;
    obj["sequence"] = display;
    obj["subscription_id"] = info.subscription_id;
    obj["limit"] = limit;
    obj["printed"] = rows.size();
    nlohmann::json arr = nlohmann::json::array();
    for (const auto& r : rows) {
      arr.push_back({{"topic", r.topic}, {"log_time_ns", r.log_time_ns}, {"payload_size", r.payload_size}});
    }
    obj["messages"] = std::move(arr);
    obj["eos_reason"] = eosName(stats.eos);
    // Only a REAL failure carries an error; the expected sink-abort marker is not
    // surfaced as an error (it is the normal early-stop).
    if (!ok && !stats.error.empty()) {
      obj["error"] = stats.error;
    }
    std::cout << obj.dump(2) << '\n';
  } else {
    std::cout << "debug " << display << " (first " << (limit == 0 ? rows.size() : limit) << " message(s))\n"
              << "  subscription_id: " << info.subscription_id << "  topics: " << info.topics.size() << '\n';
    for (const auto& r : rows) {
      std::cout << "  - topic=" << r.topic << "  log_time_ns=" << r.log_time_ns << "  payload_bytes=" << r.payload_size
                << '\n';
    }
    std::cout << "  printed " << rows.size() << " message(s); eos=" << eosName(stats.eos)
              << (clean_abort ? " (stopped early at --limit)" : "") << '\n';
    if (!ok && !stats.error.empty()) {
      std::cout << "  error: " << stats.error << '\n';
    }
  }

  return ok ? kExitOk : kExitFailure;
}

// ---- tag --------------------------------------------------------------------

int runTag(dexory_cloud::BackendConnection& conn, const std::string& sequence_name,
           const std::vector<std::pair<std::string, std::string>>& set_tags,
           const std::vector<std::string>& unset_keys, bool as_json) {
  if (set_tags.empty() && unset_keys.empty()) {
    std::cerr << "tag: nothing to do (give at least one --set k=v or --unset k)\n";
    return kExitUsage;
  }

  // listSequences() builds the name->file_id index that updateTags() resolves
  // the sequence name against (the result itself is unused here).
  (void)conn.listSequences();

  std::vector<dexory_cloud::TagRow> effective;
  std::string error;
  if (!conn.updateTags(sequence_name, set_tags, unset_keys, &effective, &error)) {
    std::cerr << "tag: " << (error.empty() ? "update failed" : error) << '\n';
    return kExitFailure;
  }

  // Re-list so the printed metadata reflects the post-update flat view (the
  // exact path the GUI relies on for its Lua filter). Find the refreshed entry.
  const std::vector<dexory_cloud::SequenceInfo> sequences = conn.listSequences();
  const dexory_cloud::SequenceInfo* refreshed = nullptr;
  for (const auto& seq : sequences) {
    if (seq.name == sequence_name) {
      refreshed = &seq;
      break;
    }
  }

  if (as_json) {
    nlohmann::json obj;
    obj["sequence"] = sequence_name;
    nlohmann::json eff = nlohmann::json::array();
    for (const auto& t : effective) {
      eff.push_back({{"key", t.key}, {"value", t.value}, {"is_override", t.is_override}});
    }
    obj["effective_tags"] = std::move(eff);
    if (refreshed != nullptr) {
      obj["metadata"] = metadataToJson(refreshed->user_metadata);
    }
    std::cout << obj.dump(2) << '\n';
    return kExitOk;
  }

  std::cout << "tag " << sequence_name << ": OK\n"
            << "  effective tags (" << effective.size() << "):\n";
  for (const auto& t : effective) {
    std::cout << "    " << t.key << " = " << t.value << (t.is_override ? "  [override]" : "") << '\n';
  }
  if (refreshed != nullptr) {
    std::cout << "  metadata (" << refreshed->user_metadata.size() << "):\n";
    for (const auto& [key, value] : refreshed->user_metadata) {
      std::cout << "    " << key << " = " << value << '\n';
    }
  }
  return kExitOk;
}

// std::getenv as an optional: nullopt when the variable is unset, else its value
// (which may be empty). Fed to the pure resolveCliUrl/resolveCliToken helpers.
std::optional<std::string> envOpt(const char* name) {
  const char* value = std::getenv(name);
  if (value == nullptr) {
    return std::nullopt;
  }
  return std::string(value);
}

}  // namespace

int main(int argc, char** argv) {
  // Explicit --url / --token (nullopt until the flag is seen); resolved against
  // the environment + the built-in default AFTER argv parsing so precedence is a
  // single, unit-tested rule (tools/cli_url_resolve.hpp).
  std::optional<std::string> url_flag;
  std::optional<std::string> token_flag;

  std::string command;
  std::vector<std::string> positionals;
  bool as_json = false;
  bool include_latched = false;  // download --latched (latched/transient-local replay)
  std::string output;          // download --output
  std::string topics_csv;      // download/debug --topics
  std::string time_range_csv;  // download/debug --time-range
  std::uint64_t debug_limit = 10;  // debug --limit (0 = all)
  std::vector<std::pair<std::string, std::string>> set_tags;  // tag --set k=v (repeatable)
  std::vector<std::string> unset_keys;                        // tag --unset k (repeatable)
  // --insecure: for wss:// against a self-signed dev cert, skip TLS certificate
  // verification (wires the BackendConnection's existing allow_insecure / the
  // ixwebsocket TLS option the GUI plugin exposes). Ignored for ws://.
  bool insecure = false;

  auto needValue = [&](const std::string& flag, int& i) -> const char* {
    if (i + 1 >= argc) {
      std::cerr << "error: " << flag << " requires a value\n";
      return nullptr;
    }
    return argv[++i];
  };

  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "-h" || arg == "--help") {
      printUsage(std::cout);
      return kExitOk;
    }
    if (arg == "--json") {
      as_json = true;
    } else if (arg == "--latched") {
      include_latched = true;
    } else if (arg == "--insecure") {
      insecure = true;
    } else if (arg == "--url") {
      const char* v = needValue(arg, i);
      if (v == nullptr) {
        return kExitUsage;
      }
      url_flag = std::string(v);
    } else if (arg == "--token") {
      const char* v = needValue(arg, i);
      if (v == nullptr) {
        return kExitUsage;
      }
      token_flag = std::string(v);
    } else if (arg == "--output") {
      const char* v = needValue(arg, i);
      if (v == nullptr) {
        return kExitUsage;
      }
      output = v;
    } else if (arg == "--topics") {
      const char* v = needValue(arg, i);
      if (v == nullptr) {
        return kExitUsage;
      }
      topics_csv = v;
    } else if (arg == "--time-range") {
      const char* v = needValue(arg, i);
      if (v == nullptr) {
        return kExitUsage;
      }
      time_range_csv = v;
    } else if (arg == "--limit") {
      const char* v = needValue(arg, i);
      if (v == nullptr) {
        return kExitUsage;
      }
      try {
        const long long parsed = std::stoll(v);
        if (parsed < 0) {
          std::cerr << "error: --limit must be >= 0\n";
          return kExitUsage;
        }
        debug_limit = static_cast<std::uint64_t>(parsed);
      } catch (const std::exception&) {
        std::cerr << "error: --limit must be a non-negative integer\n";
        return kExitUsage;
      }
    } else if (arg == "--set") {
      const char* v = needValue(arg, i);
      if (v == nullptr) {
        return kExitUsage;
      }
      // Split on the FIRST '='; the value may itself contain '='. A bare key
      // (no '=') is rejected — use --unset to remove instead.
      const std::string kv = v;
      const auto eq = kv.find('=');
      if (eq == std::string::npos || eq == 0) {
        std::cerr << "error: --set expects k=v (got '" << kv << "')\n";
        return kExitUsage;
      }
      set_tags.emplace_back(kv.substr(0, eq), kv.substr(eq + 1));
    } else if (arg == "--unset") {
      const char* v = needValue(arg, i);
      if (v == nullptr) {
        return kExitUsage;
      }
      if (*v == '\0') {
        std::cerr << "error: --unset expects a non-empty key\n";
        return kExitUsage;
      }
      unset_keys.emplace_back(v);
    } else if (arg.rfind("--", 0) == 0) {
      std::cerr << "error: unknown flag '" << arg << "'\n";
      printUsage(std::cerr);
      return kExitUsage;
    } else if (command.empty()) {
      command = arg;
    } else {
      positionals.push_back(arg);
    }
  }

  if (command.empty()) {
    std::cerr << "error: missing command\n";
    printUsage(std::cerr);
    return kExitUsage;
  }
  if (command != "hello" && command != "list" && command != "topics" && command != "download" && command != "tag" &&
      command != "debug") {
    std::cerr << "error: unknown command '" << command << "'\n";
    printUsage(std::cerr);
    return kExitUsage;
  }
  if (command == "topics" && positionals.empty()) {
    std::cerr << "error: 'topics' requires a <sequence-name>\n";
    printUsage(std::cerr);
    return kExitUsage;
  }
  if (command == "tag") {
    if (positionals.empty()) {
      std::cerr << "error: 'tag' requires a <sequence-name>\n";
      printUsage(std::cerr);
      return kExitUsage;
    }
    if (set_tags.empty() && unset_keys.empty()) {
      std::cerr << "error: 'tag' requires at least one --set k=v or --unset k\n";
      printUsage(std::cerr);
      return kExitUsage;
    }
  }

  std::optional<std::int64_t> start_ns;
  std::optional<std::int64_t> end_ns;
  if (command == "download") {
    if (positionals.empty()) {
      std::cerr << "error: 'download' requires a <sequence-name>\n";
      printUsage(std::cerr);
      return kExitUsage;
    }
    if (output.empty()) {
      std::cerr << "error: 'download' requires --output FILE\n";
      printUsage(std::cerr);
      return kExitUsage;
    }
  }
  if (command == "debug" && positionals.empty()) {
    std::cerr << "error: 'debug' requires a <sequence-name>\n";
    printUsage(std::cerr);
    return kExitUsage;
  }
  if ((command == "download" || command == "debug") && !time_range_csv.empty()) {
    const auto parts = splitCsv(time_range_csv);
    if (parts.size() != 2) {
      std::cerr << "error: --time-range must be 'startNs,endNs'\n";
      return kExitUsage;
    }
    try {
      start_ns = static_cast<std::int64_t>(std::stoll(parts[0]));
      end_ns = static_cast<std::int64_t>(std::stoll(parts[1]));
    } catch (const std::exception&) {
      std::cerr << "error: --time-range values must be integers (nanoseconds)\n";
      return kExitUsage;
    }
  }

  // Resolve URL/token precedence: explicit flag > environment > default. An
  // absent --url falls back to DEXORY_CLOUD_URL (then ws://localhost:8080); an
  // absent --token falls back to DEXORY_CLOUD_API_KEY (then empty = dev anonymous).
  const std::string url = dexory_cloud::resolveCliUrl(url_flag, envOpt("DEXORY_CLOUD_URL"));
  const std::string token = dexory_cloud::resolveCliToken(token_flag, envOpt("DEXORY_CLOUD_API_KEY"));

  // All commands need a live connection. Use the exact class the GUI uses. The
  // harness normally targets ws://; --insecure wires the same allow_insecure TLS
  // skip-verify the GUI plugin exposes so a wss:// dev-cert leg works end-to-end.
  dexory_cloud::BackendConnection conn(url, /*cert_path=*/"", /*api_key=*/token,
                                       /*allow_insecure=*/insecure);
  std::string error;
  if (!conn.connect(&error)) {
    // Surface the verbatim connection/handshake error.
    std::cerr << "error: " << (error.empty() ? "connection failed" : error) << '\n';
    return kExitFailure;
  }

  if (command == "hello") {
    return runHello(conn, error);
  }
  if (command == "list") {
    return runList(conn, as_json);
  }
  if (command == "download") {
    // All positionals are sequence names (one or more); they stitch into one
    // OpenFresh. A single positional is byte-identical to the pre-Slice-7 path.
    return runDownload(conn, positionals, output, splitCsv(topics_csv), start_ns, end_ns, as_json, include_latched);
  }
  if (command == "tag") {
    return runTag(conn, positionals.front(), set_tags, unset_keys, as_json);
  }
  if (command == "debug") {
    // All positionals are sequence names (one or more); they stitch like download.
    return runDebug(conn, positionals, splitCsv(topics_csv), start_ns, end_ns, debug_limit, as_json);
  }
  // command == "topics"
  return runTopics(conn, positionals.front(), as_json);
}
