// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT

#include "dexory_cloud_dialog.hpp"

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <nlohmann/json.hpp>
#include <pj_base/sdk/platform.hpp>
#include <set>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "cert_dialog_ui.hpp"
#include "tag_dialog_ui.hpp"
#include "core/time_format.h"
#include "aggregate_sessions.h"
#include "date_filter.h"
#include "elide_name.h"
#include "s3_key_fields.h"
#include "seq_display.h"
#include "fetch_summary.h"
#include "fetch_worker.hpp"
#include "format_utils.h"
#include "dexory_cloud_panel_manifest.hpp"
#include "dexory_cloud_panel_ui.hpp"
#include "name_filter.h"
#include "query/edit.h"
#include "query/engine.h"
#include "query/query.h"
#include "query_filter.h"
#include "server_history.h"
#include "settings_store.hpp"
#include "slider_window.h"
#include "stitch_select.h"
#include "table_sort.h"
#include "tls_utils.h"

namespace dexory_cloud {

namespace {

struct ServerCredentials {
  std::string cert_path;
  std::string api_key;
  bool allow_insecure = false;
};

std::string credentialsSettingsPrefix(const std::string& uri) {
  return "dexory_cloud/server_cache/" + normalizeServerKey(uri) + "/";
}

// D6: the bearer token (the SECRET) now lives in the CredentialStore (0600
// file, libsecret-ready seam), NOT in plaintext SettingsView. The non-secret
// prefs (cert_path, allow_insecure) stay in SettingsView keyed by the same
// normalized-URI prefix (Plan D Task 6 note 4). `view` carries the non-secret
// prefs; `creds` carries the secret token.
ServerCredentials loadCredentialsForUri(PJ::sdk::SettingsView view, CredentialStore& store, const std::string& uri) {
  SettingsStore settings(view);
  const std::string prefix = credentialsSettingsPrefix(uri);
  ServerCredentials creds;
  creds.cert_path = settings.getString(prefix + "cert_path");
  creds.allow_insecure = settings.getBool(prefix + "allow_insecure", false);
  // The token comes from the secret store; an absent entry leaves api_key empty.
  if (auto tok = store.get(uri)) {
    creds.api_key = *tok;
  }
  return creds;
}

void saveCredentialsForUri(PJ::sdk::SettingsView view, CredentialStore& store, const std::string& uri,
                           const ServerCredentials& creds) {
  SettingsStore settings(view);
  const std::string prefix = credentialsSettingsPrefix(uri);
  settings.setString(prefix + "cert_path", creds.cert_path);
  settings.setBool(prefix + "allow_insecure", creds.allow_insecure);
  // The token is the only secret — store it in the CredentialStore.
  store.set(uri, creds.api_key);
}

// Load per-server credentials, resolving the token with precedence
// explicit(env) > stored > none: the DEXORY_CLOUD_API_KEY env var wins over the
// stored token (headless / live-test parity unchanged), then the stored token,
// then dev-anonymous empty. Mirrors cli_url_resolve's resolveCliToken chain
// (extended with the STORED tier via resolveStoredToken).
ServerCredentials resolveCredentials(PJ::sdk::SettingsView view, CredentialStore& store, const std::string& uri) {
  ServerCredentials creds = loadCredentialsForUri(view, store, uri);
  creds.api_key = resolveStoredToken(PJ::sdk::getEnv("DEXORY_CLOUD_API_KEY"), store.get(uri));
  return creds;
}

// ---------------------------------------------------------------------------
// Info-panel / table formatting helpers (ported from PJ3 data_view_panel.cpp
// and format_utils.h). The Info panel is rendered as monospaced plain text.
//
// formatBytes (1024-based, PJ3 parity) lives in src/format_utils.h so it can be
// unit-tested without the Arrow/Flight link.
// ---------------------------------------------------------------------------

std::string isoFromNs(std::int64_t ts_ns) {
  if (ts_ns <= 0) {
    return {};
  }
  return formatIso8601Utc(ts_ns);
}

std::string dateOnly(std::int64_t ts_ns) {
  if (ts_ns <= 0) {
    return "--/--/----";
  }
  return formatDateDDMMYYYY(ts_ns);
}

std::string dateTimeUtc(std::int64_t ts_ns) {
  return formatDateTimeUtc(ts_ns);
}

// --- Query-assist helpers (Key/Op/Value dropdowns) ---

// Clamp a (possibly stale or -1) caret offset into [0, len].
int clampQueryCursor(int cursor, const std::string& text) {
  const int n = static_cast<int>(text.size());
  return cursor < 0 ? n : (cursor > n ? n : cursor);
}

// Distinct metadata keys — the Schema map is already key-sorted.
std::vector<std::string> schemaKeys(const Schema& schema) {
  std::vector<std::string> keys;
  keys.reserve(schema.size());
  for (const auto& kv : schema) {
    keys.push_back(kv.first);
  }
  return keys;
}

// D8: the Key dropdown's item list = the union of the keys present in the
// loaded sequences' metadata (schema) AND the server-advertised
// metadata_key_vocabulary (HelloResponse.backend.metadata_key_vocabulary),
// sorted + de-duplicated. This surfaces server-known searchable keys even
// before any sequence metadata streams in (Plan D Task 8: vocabulary feeds the
// query-assist dropdowns). Both widget_data() and onIndexChanged() resolve the
// picked index against THIS list so they stay in lockstep.
std::vector<std::string> queryAssistKeys(const Schema& schema, const std::vector<std::string>& vocabulary) {
  std::set<std::string> merged;
  for (const auto& kv : schema) {
    merged.insert(kv.first);
  }
  for (const auto& key : vocabulary) {
    if (!key.empty()) {
      merged.insert(key);
    }
  }
  return std::vector<std::string>(merged.begin(), merged.end());
}

// Distinct, sorted values recorded for `key` across the dataset.
std::vector<std::string> schemaValues(const Schema& schema, const std::string& key) {
  auto it = schema.find(key);
  if (it == schema.end()) {
    return {};
  }
  std::vector<std::string> values(it->second.begin(), it->second.end());
  std::sort(values.begin(), values.end());
  values.erase(std::unique(values.begin(), values.end()), values.end());
  return values;
}

template <typename MapType>
std::string formatMetadata(const MapType& metadata, std::string_view indent = {}) {
  if (metadata.empty()) {
    return {};
  }
  // Deterministic ordering — unordered_map iteration order is unspecified.
  std::vector<std::pair<std::string, std::string>> sorted(metadata.begin(), metadata.end());
  std::sort(sorted.begin(), sorted.end(), [](const auto& a, const auto& b) { return a.first < b.first; });
  std::string text;
  for (const auto& [key, value] : sorted) {
    // Full value, un-elided: the Info panel is an opt-in details view (toggled
    // via checkShowInfo) and must contain ALL metadata verbatim, including the
    // long Hive s3_key. The panel word-wraps (lineWrapMode=WidgetWidth) so long
    // values stay readable without horizontal scrolling.
    text += fmt::format("{}{}:\n{}  {}\n", indent, key, indent, value);
  }
  return text;
}

// Render the flat (name,type) schema fields as a "Fields (N):" block. The
// Arrow-typed formatFieldType/formatSchemaFields helpers of toolbox_mosaico are
// gone with the Arrow dependency; TopicInfo now carries plain string pairs. The
// inert backend never populates schema_fields, so this block is simply omitted
// for the Dexory Cloud plugin until the real client-core lands.
std::string formatSchemaFields(const std::vector<std::pair<std::string, std::string>>& fields) {
  std::string text;
  if (fields.empty()) {
    return text;
  }
  text += fmt::format("Fields ({}):\n", fields.size());
  for (const auto& [name, type] : fields) {
    text += fmt::format("  {} : {}\n", name, type);
  }
  return text;
}

// Aggregation gap threshold: chunks chain into one session while the gap to the
// previous file's end stays under this; a larger gap starts a new run. 5 min is
// far above any rolling-recorder seam (ms-scale) and far below a real
// redeploy/restart gap. Tunable; see aggregate_sessions.h.
constexpr std::int64_t kSessionGapThresholdNs = 300LL * 1'000'000'000LL;

// Readable, unique column-0 label for an aggregated session row, e.g.
// "arri-182 - ros-bags - 2026-05-19 16:43:49 (34 files)". Robot/source come from
// the Hive partition; the start datetime keeps it unique across same-partition
// runs. ASCII only (it rides the WidgetData JSON + table).
std::string sessionLabel(const dexory_cloud::AggSession& s) {
  std::string robot;
  std::string source;
  std::string_view p = s.partition;
  std::size_t pos = 0;
  while (pos <= p.size()) {
    std::size_t slash = p.find('/', pos);
    std::string_view seg = slash == std::string_view::npos ? p.substr(pos) : p.substr(pos, slash - pos);
    if (seg.substr(0, 6) == "robot=") {
      robot = std::string(seg.substr(6));
    } else if (seg.substr(0, 7) == "source=") {
      source = std::string(seg.substr(7));
    }
    if (slash == std::string_view::npos) {
      break;
    }
    pos = slash + 1;
  }
  std::string base;
  if (!robot.empty() && !source.empty()) {
    base = robot + " - " + source;
  } else if (!robot.empty()) {
    base = robot;
  } else if (!source.empty()) {
    base = source;
  } else if (!s.partition.empty()) {
    base = s.partition;
  } else if (!s.keys.empty()) {
    base = baseName(s.keys.front());  // flat corpus: first file's leaf
  }
  return fmt::format("{} - {} ({} files)", base, dateTimeUtc(s.min_ts_ns), s.keys.size());
}

// The S3-key field keys offered as Basic-tab dropdowns, in display order.
const std::array<std::pair<const char*, const char*>, 4> kBasicFilterKeys = {{
    {"customer", "Customer"},
    {"customer_site", "Site"},
    {"robot", "Robot"},
    {"source", "Source"},
}};

// Topics every download must include: without /tf + /tf_static a
// transform-consuming import (3D scene, TF-resolved point clouds) receives data
// it cannot place. Appended IMPLICITLY at fetch time (never force-SELECTED in
// the table: a plugin that mutates the user's selection per event fights the
// host's selection applier — every gesture got a programmatic clear+reselect a
// tick later, scrambling clicks and drags; 2026-07-12).
constexpr std::array<std::string_view, 2> kForcedTopics = {"/tf", "/tf_static"};

// Distinct sorted values of one parsed S3-key field across the sequences — the
// dropdown options for that Basic-tab key.
std::vector<std::string> distinctFieldValues(const std::vector<SequenceRecord>& seqs, std::string_view key) {
  std::set<std::string, std::less<>> vals;
  for (const auto& rec : seqs) {
    const Metadata fields = parseS3KeyFields(rec.name);
    if (auto it = fields.find(key); it != fields.end()) {
      vals.insert(it->second);
    }
  }
  return {vals.begin(), vals.end()};
}

// Does a sequence pass the Basic-tab equality filters (AND across non-empty
// selections)? Empty selections impose no constraint.
bool matchesBasicFilter(const SequenceRecord& rec, const std::map<std::string, std::string, std::less<>>& filters) {
  const Metadata fields = parseS3KeyFields(rec.name);
  for (const auto& [key, value] : filters) {
    if (value.empty()) {
      continue;
    }
    auto it = fields.find(key);
    if (it == fields.end() || it->second != value) {
      return false;
    }
  }
  return true;
}

// The metadata a sequence may be FILTERED by — the SAME 4 canonical S3-key fields
// the Basic tab uses (customer/customer_site/robot/source), parsed from the
// object key. This is the ONLY filter dimension: the Advanced (Lua) tab evaluates
// against this map too, NOT against rec.metadata (which carries MCAP-content
// stats like chunk_count/message_count/duration for DISPLAY + stitching only). A
// query referencing any other key resolves to absent, so MCAP contents can never
// be a filter — only the dedicated server-derived key fields.
Metadata canonicalFilterFields(const SequenceRecord& rec) {
  const Metadata all = parseS3KeyFields(rec.name);
  Metadata out;
  for (const auto& kv : kBasicFilterKeys) {
    if (auto it = all.find(kv.first); it != all.end()) {
      out.emplace(std::string(kv.first), it->second);
    }
  }
  return out;
}

// The Advanced-tab query-assist vocabulary: exactly the 4 canonical keys, a
// constant independent of the server. The server's metadata_key_vocabulary is
// MCAP-content-derived and is intentionally NOT offered as a filter dimension.
std::vector<std::string> canonicalVocabularyKeys() {
  std::vector<std::string> keys;
  keys.reserve(kBasicFilterKeys.size());
  for (const auto& kv : kBasicFilterKeys) {
    keys.emplace_back(kv.first);
  }
  return keys;
}

std::string buildSequenceInfoText(const SequenceRecord& rec) {
  std::string text;
  // Full key — the Info panel is the opt-in details view and shows everything
  // verbatim (the panel word-wraps).
  text += fmt::format("Sequence : {}\n", rec.name);
  if (rec.max_ts_ns > 0) {
    text += fmt::format("Date     : {}\n", dateOnly(rec.max_ts_ns));
  }
  if (rec.total_size_bytes > 0) {
    text += fmt::format("Size     : {}\n", formatBytes(rec.total_size_bytes));
  }
  if (!rec.metadata.empty()) {
    text += "\nMetadata:\n";
    text += formatMetadata(rec.metadata);
  }
  return text;
}

// Read the "message_count" metadata key as an int64 (the server publishes it in
// the flat metadata map); 0 when absent or unparseable.
std::int64_t messageCountOf(const SequenceRecord& rec) {
  auto it = rec.metadata.find("message_count");
  if (it == rec.metadata.end()) {
    return 0;
  }
  try {
    return static_cast<std::int64_t>(std::stoll(it->second));
  } catch (const std::exception&) {
    return 0;
  }
}

// Slice 7: stitched-selection Info-panel header (N>1). Mirrors the single-
// sequence text shape but summarizes the union: file count, union time span,
// summed size, summed message count, and the ordered file list.
std::string buildStitchedInfoText(const std::vector<const SequenceRecord*>& recs) {
  // Order by (min_ts, name) so the Files list reads in stitched order, matching
  // buildStitchedSelection / the resolved OpenFresh order.
  std::vector<const SequenceRecord*> ordered(recs.begin(), recs.end());
  std::sort(ordered.begin(), ordered.end(), [](const SequenceRecord* a, const SequenceRecord* b) {
    if (a->min_ts_ns != b->min_ts_ns) {
      return a->min_ts_ns < b->min_ts_ns;
    }
    return a->name < b->name;
  });

  std::int64_t union_min = 0;
  std::int64_t union_max = 0;
  std::int64_t total_size = 0;
  std::int64_t total_msgs = 0;
  bool have_min = false;
  for (const SequenceRecord* r : ordered) {
    if (r->min_ts_ns > 0 && (!have_min || r->min_ts_ns < union_min)) {
      union_min = r->min_ts_ns;
      have_min = true;
    }
    if (r->max_ts_ns > union_max) {
      union_max = r->max_ts_ns;
    }
    total_size += r->total_size_bytes;
    total_msgs += messageCountOf(*r);
  }

  std::string text;
  text += fmt::format("Stitched : {} files\n", ordered.size());
  if (union_min > 0 || union_max > 0) {
    text += fmt::format("Time     : {} -> {}\n", dateOnly(union_min), dateOnly(union_max));
  }
  if (total_size > 0) {
    text += fmt::format("Size     : {}\n", formatBytes(total_size));
  }
  if (total_msgs > 0) {
    text += fmt::format("Messages : {}\n", total_msgs);
  }
  text += "Files:\n";
  for (const SequenceRecord* r : ordered) {
    text += fmt::format("  - {}\n", r->name);
  }
  return text;
}

std::string buildTopicInfoText(const TopicInfo& info) {
  std::string text;
  text += fmt::format("Topic    : {}\n", info.topic_name);
  if (!info.ontology_tag.empty()) {
    text += fmt::format("Tag      : {}\n", info.ontology_tag);
  }
  if (info.created_at_ns > 0) {
    text += fmt::format("Created  : {}\n", dateTimeUtc(info.created_at_ns));
  }
  if (info.locked) {
    if (info.completed_at_ns.has_value() && *info.completed_at_ns > 0) {
      text += fmt::format("Status   : sealed ({})\n", dateTimeUtc(*info.completed_at_ns));
    } else {
      text += "Status   : sealed\n";
    }
  } else {
    text += "Status   : live\n";
  }
  if (info.chunks_number > 0) {
    text += fmt::format("Chunks   : {}\n", info.chunks_number);
  }
  if (info.total_size_bytes > 0) {
    text += fmt::format("Size     : {}\n", formatBytes(info.total_size_bytes));
  }
  if (!info.resource_locator.empty()) {
    text += fmt::format("Resource : {}\n", info.resource_locator);
  }
  if (!info.schema_fields.empty()) {
    text += formatSchemaFields(info.schema_fields);
  }
  if (!info.user_metadata.empty()) {
    text += "\nMetadata:\n";
    text += formatMetadata(info.user_metadata);
  }
  return text;
}

}  // namespace

DexoryCloudDialog::DexoryCloudDialog() : worker_(std::make_unique<FetchWorker>()) {
  worker_->connectFinished = [this](bool ok, std::string status, std::string err) {
    postEvent([this, ok, status = std::move(status), err = std::move(err)]() mutable {
      onConnectFinished(ok, std::move(status), std::move(err));
    });
  };
  // D8: caps arrive on a successful connect (before connectFinished). Latch the
  // hierarchy flag + vocabulary into state_ on the GUI thread so the next tick's
  // widget_data() can show/populate the prefix combo + query-assist vocabulary.
  worker_->capabilitiesReady = [this](BackendCaps caps) {
    postEvent([this, caps = std::move(caps)]() mutable { onCapabilitiesReady(std::move(caps)); });
  };
  // D2: same GUI-thread event-drain pattern as capabilitiesReady above, for the
  // tag-edit-supported gate.
  worker_->serverCapabilitiesReady = [this](ServerCaps caps) {
    postEvent([this, caps]() { onServerCapabilitiesReady(caps); });
  };
  worker_->sequencesReady = [this](std::vector<SequenceInfo> sequences) {
    postEvent([this, sequences = std::move(sequences)]() mutable { onSequencesReady(std::move(sequences)); });
  };
  worker_->sequenceListStarted = [this](std::vector<SequenceInfo> sequences) {
    postEvent([this, sequences = std::move(sequences)]() mutable { onSequenceListStarted(std::move(sequences)); });
  };
  worker_->sequenceInfoReady = [this](SequenceInfo sequence) {
    postEvent([this, sequence = std::move(sequence)]() mutable { onSequenceInfoReady(std::move(sequence)); });
  };
  worker_->topicsReady = [this](std::string sequence_name, std::vector<std::string> topic_names) {
    postEvent([this, sequence_name = std::move(sequence_name), topic_names = std::move(topic_names)]() mutable {
      onTopicsReady(std::move(sequence_name), std::move(topic_names));
    });
  };
  worker_->topicInfosReady = [this](std::string sequence_name, std::vector<TopicInfo> topics) {
    postEvent([this, sequence_name = std::move(sequence_name), topics = std::move(topics)]() mutable {
      onTopicInfosReady(std::move(sequence_name), std::move(topics));
    });
  };
  worker_->topicsFailed = [this](std::string sequence_name, std::string error) {
    postEvent([this, sequence_name = std::move(sequence_name), error = std::move(error)]() mutable {
      onTopicsFailed(std::move(sequence_name), std::move(error));
    });
  };
  worker_->connectionLost = [this] {
    postEvent([this] { onConnectionLost(); });
  };
  worker_->topicMetadataReady = [this](std::string sequence_name, std::string topic_name, TopicInfo info) {
    postEvent([this, sequence_name = std::move(sequence_name), topic_name = std::move(topic_name),
               info = std::move(info)]() mutable {
      onTopicMetadataReady(std::move(sequence_name), std::move(topic_name), std::move(info));
    });
  };
  // In-dialog download/ingest callbacks (Mosaico parity): pullProgress /
  // pullFinished / allFetchesComplete drive the per-topic ledger + the
  // close/import/notifyDataChanged policy. All route through the GUI-thread
  // event queue.
  worker_->pullProgress = [this](std::string topic_name, std::int64_t bytes) {
    postEvent(
        [this, topic_name = std::move(topic_name), bytes]() mutable { onPullProgress(std::move(topic_name), bytes); });
  };
  worker_->pullWireBytes = [this](std::int64_t wire_bytes) {
    postEvent([this, wire_bytes]() {
      std::lock_guard<std::mutex> lock(state_.mu);
      state_.wire_bytes_total = wire_bytes;
    });
  };
  worker_->pullPhase = [this](std::string phase) {
    postEvent([this, phase = std::move(phase)]() mutable { onPullPhase(std::move(phase)); });
  };
  worker_->pullFinished = [this](std::string sequence_name, std::string topic_name, bool ok, std::string error) {
    postEvent([this, sequence_name = std::move(sequence_name), topic_name = std::move(topic_name), ok,
               error = std::move(error)]() mutable {
      onPullFinished(std::move(sequence_name), std::move(topic_name), ok, std::move(error));
    });
  };
  worker_->allFetchesComplete = [this](std::string sequence_name) {
    postEvent(
        [this, sequence_name = std::move(sequence_name)]() mutable { onAllFetchesComplete(std::move(sequence_name)); });
  };
  // Reconnect-resume hint (Slice 8): surface "Resuming (attempt N/max)…" through
  // the same GUI-thread event queue + notification bell.
  worker_->pullResuming = [this](std::string group, unsigned attempt, unsigned max) {
    postEvent([this, group = std::move(group), attempt, max]() mutable {
      onPullResuming(std::move(group), attempt, max);
    });
  };
  // Cache HIT (Slice 8): one-shot "served from cache" notify.
  worker_->pullServedFromCache = [this](std::string group) {
    postEvent([this, group = std::move(group)]() mutable { onPullServedFromCache(std::move(group)); });
  };
  worker_->errorOccurred = [this](std::string message) {
    postEvent([this, message = std::move(message)]() mutable { notify(PJ::ToolboxMessageLevel::kError, message); });
  };
  // Tag-edit commit result (Slice 6). On success the worker emits sequencesReady
  // right after, so onTagsUpdated only needs to surface a failure / a "saved"
  // notice; the catalog refresh (and the Lua filter re-eval) rides the existing
  // onSequencesReady path.
  worker_->tagsUpdated = [this](std::string sequence_name, bool ok, std::string error) {
    postEvent([this, sequence_name = std::move(sequence_name), ok, error = std::move(error)]() mutable {
      onTagsUpdated(std::move(sequence_name), ok, std::move(error));
    });
  };

  worker_thread_ = std::thread([this] { workerLoop(); });
  // Persisted-state restore + auto-connect happen in initFromSettings(), once
  // the host binds the settings view via setSettings() (during plugin bind()).
}

CredentialStore& DexoryCloudDialog::credentialStore() {
  // Lazily construct the default file-backed store (libsecret drop-in later).
  // Constructed on first credential access so a Qt-host unit-load without
  // setSettings() still resolves a backend.
  if (!credentials_) {
    credentials_ = std::make_unique<FileCredentialStore>(defaultConfigRoot());
  }
  return *credentials_;
}

void DexoryCloudDialog::setSettings(PJ::sdk::SettingsView settings) {
  settings_ = settings;
  initFromSettings();
}

void DexoryCloudDialog::setHostProvider(std::function<PJ::sdk::ToolboxHostView()> provider) {
  worker_->setHostProvider(std::move(provider));
}

void DexoryCloudDialog::setRuntimeHostProvider(std::function<PJ::ToolboxRuntimeHostView()> provider) {
  worker_->setRuntimeHostProvider(provider);
  runtime_host_provider_ = std::move(provider);
}

void DexoryCloudDialog::initFromSettings() {
  // Restore persisted UI state and auto-connect to the last server (PJ3
  // parity). Runs at bind time, before the tick loop or any worker result can
  // touch state_, so the unlocked access here is safe.
  SettingsStore settings(settings_);
  const std::vector<std::string> history = settings.getStringList("dexory_cloud/server_history");
  if (!history.empty()) {
    state_.uri = history.front();
  }
  state_.query_text = settings.getString("dexory_cloud/metadata_query");
  state_.range_lower = std::clamp(settings.getInt("dexory_cloud/range_lower", 0), 0, DialogState::kSliderSteps);
  state_.range_upper =
      std::clamp(settings.getInt("dexory_cloud/range_upper", DialogState::kSliderSteps), 0, DialogState::kSliderSteps);
  // Selection is deliberately NOT restored across runs (2026-07-12): the
  // toolbox always opens with nothing selected. (The restore_* staging slots
  // remain for the in-session flows but start empty.)
  // View-mode + filter-tab + Basic-tab constraints from the previous run. The
  // Basic values are provisional: onSequencesReady prunes any that no longer
  // match the server's data (the combo falls back to "(any)").
  state_.aggregate = settings.getBool("dexory_cloud/aggregate", false);
  state_.topics_all = settings.getBool("dexory_cloud/topics_all", false);
  state_.filter_tab = std::clamp(settings.getInt("dexory_cloud/filter_tab", 0), 0, 1);
  for (const auto& [key, label] : kBasicFilterKeys) {
    (void)label;
    const std::string value = settings.getString(std::string("dexory_cloud/basic_filter/") + key);
    if (!value.empty()) {
      state_.basic_filter[key] = value;
    }
  }

  if (!history.empty()) {
    const std::string uri = state_.uri;
    const ServerCredentials creds = resolveCredentials(settings_, credentialStore(), uri);
    // Same printable-ASCII gate as the explicit Connect handler (PJ3
    // connectToServer validates both paths). On auto-connect PJ3 shows no
    // popup, so a malformed persisted value silently aborts the auto-connect
    // rather than handing control bytes to gRPC.
    if (isPrintableAscii(uri) && isPrintableAscii(creds.cert_path) && isPrintableAscii(creds.api_key)) {
      state_.connecting = true;
      state_.suppress_connect_error = true;  // PJ3 AutoConnect: no error notification on failure.
      postCommand(
          [w = worker_.get(), uri, cert_path = creds.cert_path, api_key = creds.api_key,
           allow_insecure = creds.allow_insecure] { w->connectAsync(uri, cert_path, api_key, allow_insecure); });
    }
  }
}

DexoryCloudDialog::~DexoryCloudDialog() {
  // Persist the Lua query + slider proportions for next open (PJ3 parity).
  persistState();
  // Signal the SDK's mid-stream cancel flag before joining; otherwise the
  // worker could block indefinitely while pullTopics is in flight.
  if (worker_) {
    worker_->requestCancel();
  }
  {
    std::lock_guard<std::mutex> lock(cmd_mu_);
    worker_stop_ = true;
  }
  cmd_cv_.notify_all();
  if (worker_thread_.joinable()) {
    worker_thread_.join();
  }
  // onTick and this destructor both run on the single GUI thread, so no
  // this-capturing event closure can run during or after destruction; closures
  // left in evt_queue_ are destroyed un-run with the dialog.
}

void DexoryCloudDialog::workerLoop() {
  for (;;) {
    std::function<void()> task;
    {
      std::unique_lock<std::mutex> lock(cmd_mu_);
      cmd_cv_.wait(lock, [this] { return worker_stop_ || !cmd_queue_.empty(); });
      if (worker_stop_ && cmd_queue_.empty()) {
        return;
      }
      task = std::move(cmd_queue_.front());
      cmd_queue_.pop_front();
    }
    task();
  }
}

void DexoryCloudDialog::postCommand(std::function<void()> fn) {
  {
    std::lock_guard<std::mutex> lock(cmd_mu_);
    cmd_queue_.push_back(std::move(fn));
  }
  cmd_cv_.notify_one();
}

void DexoryCloudDialog::postEvent(std::function<void()> fn) {
  std::lock_guard<std::mutex> lock(evt_mu_);
  evt_queue_.push_back(std::move(fn));
}

bool DexoryCloudDialog::onTick() {
  std::deque<std::function<void()>> batch;
  {
    std::lock_guard<std::mutex> lock(evt_mu_);
    batch.swap(evt_queue_);
  }
  for (auto& c : batch) {
    try {
      c();
    } catch (...) {
      // Keep one bad callback from escaping the plugin tick ABI.
    }
  }
  return !batch.empty();
}

std::string DexoryCloudDialog::manifest() const {
  return kDexoryCloudPanelManifest;
}

std::string DexoryCloudDialog::ui_content() const {
  return kDexoryCloudPanelUi;
}

std::string DexoryCloudDialog::saveConfig() const {
  // Toolbox shape: the panel imports IN-DIALOG, so no session selection is
  // carried. saveConfig() persists only the browse-phase UI prefs (server URI +
  // Lua query + slider proportions) so the panel re-opens with the same state.
  std::lock_guard<std::mutex> lock(state_.mu);
  nlohmann::json cfg;
  cfg["server_uri"] = state_.uri;
  cfg["metadata_query"] = state_.query_text;
  cfg["range_lower"] = state_.range_lower;
  cfg["range_upper"] = state_.range_upper;
  return cfg.dump();
}

bool DexoryCloudDialog::loadConfig(std::string_view config_json) {
  auto cfg = nlohmann::json::parse(config_json, nullptr, false);
  if (cfg.is_discarded() || !cfg.is_object()) {
    return false;
  }
  std::lock_guard<std::mutex> lock(state_.mu);
  if (auto it = cfg.find("server_uri"); it != cfg.end() && it->is_string()) {
    state_.uri = it->get<std::string>();
  }
  if (auto it = cfg.find("metadata_query"); it != cfg.end() && it->is_string()) {
    state_.query_text = it->get<std::string>();
  }
  if (auto it = cfg.find("range_lower"); it != cfg.end() && it->is_number_integer()) {
    state_.range_lower = std::clamp(it->get<int>(), 0, DialogState::kSliderSteps);
  }
  if (auto it = cfg.find("range_upper"); it != cfg.end() && it->is_number_integer()) {
    state_.range_upper = std::clamp(it->get<int>(), 0, DialogState::kSliderSteps);
  }
  return true;
}

std::string DexoryCloudDialog::widget_data() {
  std::lock_guard<std::mutex> lock(state_.mu);
  PJ::WidgetData wd;
  // No in-panel status strip — connection / import / error events go to the
  // app's top notification bell via notify(); live download progress shows in
  // the Info panel during a fetch.
  wd.setText("comboUri", state_.uri);

  // PJ3 parity: combo always lists the MRU history + the default server pin.
  {
    static const std::string kDefaultServer = "ws://localhost:8080";
    std::vector<std::string> items;
    const std::vector<std::string> history = SettingsStore(settings_).getStringList("dexory_cloud/server_history");
    bool has_default = false;
    for (const std::string& s : history) {
      items.push_back(s);
      if (s == kDefaultServer) {
        has_default = true;
      }
    }
    if (!has_default) {
      items.push_back(kDefaultServer);
    }
    wd.setItems("comboUri", items);
  }

  // Query editor (plain QPlainTextEdit, code-edit mode). The Lua query language
  // lives entirely in the plugin: we push the persisted text ONCE to restore it,
  // then validate edits via the plugin's engine and surface the result through
  // the generic field-validity indicator.
  {
    // One-shot text restore. Pushing query_text every tick would clobber the
    // editor's own edits the instant the user types (the stale-echo race that
    // broke the previous split design). After the first push the editor owns the
    // text; edits return via onCodeChanged.
    if (!state_.query_text_pushed || state_.query_push_pending) {
      wd.setCodeContent("lua_queryBar", state_.query_text);
      wd.setCodeCursor("lua_queryBar", state_.query_cursor);
      state_.query_text_pushed = true;
      state_.query_push_pending = false;
    }
    wd.setCodeLanguage("lua_queryBar", "lua");  // Lua syntax highlighting + code-editor wiring
    // Opt into caret tracking: the Key/Op/Value dropdowns re-analyze at the
    // caret, so we need cursor moves (not just edits) reported via
    // onCodeChangedWithCursor. Other code editors don't opt in and so aren't
    // re-run on every cursor move.
    wd.setCodeCaretTracking("lua_queryBar");

    // Validity feedback via the plugin's Lua engine. An empty query is valid (no
    // filter); otherwise the tick + tooltip reflect Engine::validate.
    if (state_.query_text.empty()) {
      wd.setFieldValid("lua_queryBar", true, "");
    } else {
      const bool valid = Engine::validate(state_.query_text).valid;
      wd.setFieldValid("lua_queryBar", valid, valid ? "" : "invalid syntax");
    }

    // Cursor-aware Key/Op/Value assist dropdowns. analyze() the query at the
    // caret to decide what each dropdown would insert/replace and whether it is
    // actionable; the value list is the distinct values of the key in context.
    // Each is reset to no-selection so picking the same item again re-fires.
    ensureQuerySchemaLocked();
    const int cursor = clampQueryCursor(state_.query_cursor, state_.query_text);
    const CursorContext ctx = analyze(state_.query_text, cursor, state_.query_schema);

    wd.setItems("keyCombo", queryAssistKeys(state_.query_schema, state_.metadata_key_vocabulary));
    wd.setEnabled("keyCombo", ctx.can_pick_key());
    wd.setCurrentIndex("keyCombo", -1);

    wd.setItems("opCombo", operators());
    wd.setEnabled("opCombo", ctx.can_pick_op());
    wd.setCurrentIndex("opCombo", -1);

    wd.setItems("valCombo", schemaValues(state_.query_schema, ctx.context_key));
    wd.setEnabled("valCombo", ctx.can_pick_value());
    wd.setCurrentIndex("valCombo", -1);
  }

  // RangeSlider: bounds + handle values. Once a sequence with a known time
  // span is selected, enable it and turn on the duration floating labels
  // (handle = offset from start, center = selected duration) — PJ3 parity.
  wd.setRangeSliderBounds("rangeSlider", 0, DialogState::kSliderSteps);
  wd.setRangeSliderValues("rangeSlider", state_.range_lower, state_.range_upper);
  {
    // Slice 7: the slider spans the UNION [min(min_ts), max(max_ts)] across the
    // selected sequences (single-select keeps the one sequence's span).
    std::int64_t union_min = 0;
    std::int64_t union_max = 0;
    bool have_min = false;
    for (const auto& s : state_.sequences) {
      if (std::find(state_.selected_sequences.begin(), state_.selected_sequences.end(), s.name) ==
          state_.selected_sequences.end()) {
        continue;
      }
      if (s.min_ts_ns > 0 && (!have_min || s.min_ts_ns < union_min)) {
        union_min = s.min_ts_ns;
        have_min = true;
      }
      if (s.max_ts_ns > union_max) {
        union_max = s.max_ts_ns;
      }
    }
    if (union_max > union_min) {
      wd.setEnabled("rangeSlider", true);
      wd.setRangeSliderTimeSpan("rangeSlider", union_min, union_max);
      // Chunk-boundary markers: one box per selected file at its TRUE [start,
      // end] extent (mapped to slider units), labeled with its 1-based index in
      // time order, when the selection is a multi-file session. Disjoint
      // selections therefore render as separate boxes with blank slider space in
      // the gaps. Single-file or disabled selections clear the markers.
      std::vector<PJ::RangeSliderMarker> markers;
      if (state_.selected_sequences.size() > 1) {
        std::vector<std::pair<std::int64_t, std::int64_t>> spans;  // (start, end)
        spans.reserve(state_.selected_sequences.size());
        for (const auto& s : state_.sequences) {
          if (std::find(state_.selected_sequences.begin(), state_.selected_sequences.end(), s.name) !=
              state_.selected_sequences.end()) {
            spans.emplace_back(s.min_ts_ns, s.max_ts_ns);
          }
        }
        std::sort(spans.begin(), spans.end());
        const double span = static_cast<double>(union_max - union_min);
        auto toUnits = [&](std::int64_t ns) {
          return static_cast<int>(static_cast<double>(ns - union_min) / span * DialogState::kSliderSteps);
        };
        for (std::size_t i = 0; i < spans.size(); ++i) {
          markers.push_back({toUnits(spans[i].first), toUnits(spans[i].second), std::to_string(i + 1)});
        }
      }
      wd.setRangeSliderMarkers("rangeSlider", markers);
    } else {
      wd.setEnabled("rangeSlider", false);
      wd.setRangeSliderMarkers("rangeSlider", {});
    }
  }

  // DateRangePicker: hint the dataset's available span as the from/to placeholders.
  if (state_.global_min_ts_ns > 0) {
    wd.setDateRangePlaceholder(
        "datePicker", formatDateOnlyIso(state_.global_min_ts_ns),
        state_.global_max_ts_ns > 0 ? formatDateOnlyIso(state_.global_max_ts_ns) : "");
  }

  // Button enable/disable (PJ3 parity): Connect is inert while a connect or a
  // fetch is in flight; Download needs a sequence + topic(s) and an idle
  // worker; Cancel is live only during a fetch.
  wd.setEnabled("buttonConnect", !state_.connecting && !state_.fetch_active);
  // Connection-state visibility: auto-connect at bind is silent by design, so
  // announce its outcome here instead — a stateful tooltip on the (re)connect
  // button, and the host's standard invalid tint on the URI field while it is
  // not connected (cleared the moment the connection lands). An EMPTY URI is
  // not tinted: a fresh install opening on a red field would read as an error.
  {
    std::string connect_tip;
    bool uri_ok = true;
    std::string uri_tip;
    if (state_.connecting) {
      connect_tip = fmt::format("Connecting to {}...", state_.uri);
      uri_tip = connect_tip;
    } else if (state_.connected) {
      connect_tip = fmt::format("Connected to {} - click to reconnect", state_.uri);
      uri_tip = fmt::format("Connected to {}", state_.uri);
    } else if (state_.uri.empty()) {
      connect_tip = "Enter a server URI (e.g. ws://localhost:8080) and connect";
      uri_tip = connect_tip;
    } else {
      connect_tip = fmt::format("Not connected - click to connect to {}", state_.uri);
      uri_ok = false;
      uri_tip = connect_tip;
    }
    wd.setFieldValid("buttonConnect", true, connect_tip);
    wd.setFieldValid("comboUri", uri_ok, uri_tip);
  }
  // Slice 7: Fetch is enabled with >=1 selected sequence + a topic selection
  // (All mode always qualifies; Custom needs >=1 selected row). A stitched
  // multi-file selection downloads via ONE OpenFresh.
  wd.setEnabled(
      "buttonFetch", state_.connected && !state_.selected_sequences.empty() &&
                         (state_.topics_all || !state_.topic_selected_rows.empty()) && !state_.fetch_active);
  // Tooltip = the FIRST unmet requirement (priority-ordered), so a disabled
  // Download always says why. Pushed via setFieldValid with ok=true: the host's
  // generic field-validity branch then applies only the tooltip (no invalid
  // red-background cue — the disabled look already covers the visual state).
  {
    const char* fetch_tip =
        "Download the selected topics of the selected sequence(s); /tf and /tf_static are always included";
    if (state_.fetch_active) {
      fetch_tip = "Disabled: a download is in progress (use Cancel to stop it)";
    } else if (!state_.connected) {
      fetch_tip = "Disabled: connect to a server first";
    } else if (state_.selected_sequences.empty()) {
      fetch_tip = "Disabled: select at least one sequence";
    } else if (!state_.topics_all && state_.topic_selected_rows.empty()) {
      fetch_tip = "Disabled: select at least one topic (or switch topics to All)";
    }
    wd.setFieldValid("buttonFetch", true, fetch_tip);
  }
  // Refresh re-lists sequences without a disconnect/reconnect (PJ3
  // main_window.cpp:933-945): live only while connected and idle.
  wd.setEnabled("buttonRefresh", state_.connected && !state_.connecting && !state_.fetch_active);
  wd.setEnabled("buttonCancel", state_.fetch_active);
  // Closing mid-fetch tears the worker down before allFetchesComplete runs,
  // stranding the topics that already wrote into the shared store. Force the
  // user to Cancel first (Cancel flushes/cleans up the batch deterministically).
  wd.setEnabled("buttonClose", !state_.fetch_active);
  // Edit Tags (Slice 6): live only when connected, EXACTLY ONE sequence is
  // selected, and no fetch is in flight (a tag edit re-lists sequences, which
  // would race a download's catalog reads). Slice 7: a tag edit is unambiguous
  // only for a single selection, so it is disabled when multiple are selected.
  // D2: ALSO gated on tag_edit_supported (HelloResponse.capabilities) — a
  // read-only catalog with no tag-edit IPC forwarder configured (post-M6) must
  // never offer a control BackendConnection::updateTags() is guaranteed to
  // reject. AND on !connecting (Codex review): during a reconnect handshake
  // `connected`/`tag_edit_supported` still describe the PREVIOUS server, so
  // the editor must not open on that stale window. This whole expression is
  // recomputed every host tick, so a selection change can never re-enable the
  // button past these gates.
  wd.setEnabled("buttonEditTags", state_.connected && !state_.connecting &&
                                      state_.seq_selected_rows.size() == 1 &&
                                      !state_.primary_sequence.empty() && !state_.fetch_active &&
                                      state_.tag_edit_supported);

  // Icon-only Connect / Cert buttons (the .ui clears their text + sets the
  // tooltips). Resolved by the host from its themed icon set; unknown ids fall
  // back to no icon.
  wd.setButtonIconNamed("buttonConnect", "plug_connect");
  wd.setButtonIconNamed("buttonCert", "contract");
  // Refresh uses the host's themed named-icon path (Material "Refresh"), which
  // rasterizes via LoadSvg to a high-res master — crisp on HiDPI, unlike an
  // inline SVG rendered to a logical-size pixmap.
  wd.setButtonIconNamed("buttonRefresh", "refresh");

  // Sequence table — Lua predicate filter + name-substring filter combine
  // to produce the visible-row set. Empty query + empty filter ⇒ all rows
  // visible (clearVisibleRows). The Lua engine is built lazily and reused
  // across getWidgetData calls; per-sequence metadata is injected before
  // each eval and cleared after.
  {
    // Recompute rows + visible only when an input changed (see SeqViewCache);
    // widget_data() runs every host tick, and the per-sequence filter + metadata
    // copies are too heavy to redo at 20Hz on the GUI thread.
    auto& cache = state_.seq_view_cache;
    const bool cache_hit = cache.valid && cache.aggregate == state_.aggregate && cache.filter_tab == state_.filter_tab &&
                     cache.basic_filter == state_.basic_filter && cache.seq_epoch == state_.seq_epoch &&
                     cache.seq_filter == state_.seq_filter && cache.seq_filter_regex == state_.seq_filter_regex &&
                     cache.query_text == state_.query_text && cache.date_from_ns == state_.date_from_ns &&
                     cache.date_to_ns == state_.date_to_ns;
    if (!cache_hit) {
      // Build a schema from the union of every sequence's CANONICAL filter fields
      // (the 4 S3-key fields) — the PJ3 query engine uses it for shorthand
      // expansion. NOT rec.metadata: the Advanced query filters only by the
      // canonical fields, never by MCAP-content stats. Only needed when a query
      // is present (Advanced tab).
      Schema schema;
      if (!state_.query_text.empty()) {
        for (const auto& rec : state_.sequences) {
          for (const auto& kv : canonicalFilterFields(rec)) {
            schema[kv.first].push_back(kv.second);
          }
        }
      }
      std::vector<FilterSequence> filter_seqs;
      filter_seqs.reserve(state_.sequences.size());
      for (const auto& rec : state_.sequences) {
        filter_seqs.push_back({rec.name, rec.min_ts_ns, rec.max_ts_ns, canonicalFilterFields(rec)});
      }
      FilterParams params;
      params.name_filter = state_.seq_filter;
      params.name_regex = state_.seq_filter_regex;
      // The metadata query applies ONLY on the Advanced tab; on Basic the Lua
      // query is ignored. Name + date filters always apply in both modes.
      params.query_text = state_.filter_tab == 1 ? state_.query_text : std::string{};
      params.date_from_ns = state_.date_from_ns;
      params.date_to_ns = state_.date_to_ns;
      // Per-file visible set (indices into state_.sequences): name + date + the
      // active tab's metadata filter. Advanced -> the Lua query (computed by the
      // shared helper). Basic -> the dropdown equality filters on the S3 fields.
      std::vector<int> file_visible = computeVisibleSequences(filter_seqs, params, schema);
      if (state_.filter_tab == 0 && !state_.basic_filter.empty()) {
        std::vector<int> narrowed;
        narrowed.reserve(file_visible.size());
        for (int idx : file_visible) {
          if (matchesBasicFilter(state_.sequences[static_cast<std::size_t>(idx)], state_.basic_filter)) {
            narrowed.push_back(idx);
          }
        }
        file_visible = std::move(narrowed);
      }

      std::vector<std::vector<std::string>> rows;
      std::vector<std::vector<std::string>> row_keys;
      std::vector<int> visible;
      std::unordered_map<std::string, std::vector<std::string>> row_to_keys;

      if (state_.aggregate) {
        // One row per SESSION. A session is visible iff ANY of its files passes
        // the filters (so filtering by robot/tag/date surfaces matching runs).
        std::unordered_set<std::string> visible_keys;
        for (int idx : file_visible) {
          visible_keys.insert(state_.sequences[static_cast<std::size_t>(idx)].name);
        }
        std::vector<dexory_cloud::AggInput> inputs;
        inputs.reserve(state_.sequences.size());
        for (const auto& rec : state_.sequences) {
          inputs.push_back({rec.name, rec.min_ts_ns, rec.max_ts_ns});
        }
        const auto sessions = dexory_cloud::aggregateSessions(inputs, kSessionGapThresholdNs);
        // Sum size per session by key lookup.
        std::unordered_map<std::string, std::int64_t> size_by_key;
        for (const auto& rec : state_.sequences) {
          size_by_key[rec.name] = rec.total_size_bytes;
        }
        rows.reserve(sessions.size());
        row_keys.reserve(sessions.size());
        for (const auto& s : sessions) {
          std::int64_t total = 0;
          bool any_visible = false;
          for (const auto& k : s.keys) {
            total += size_by_key.count(k) ? size_by_key[k] : 0;
            if (visible_keys.count(k)) {
              any_visible = true;
            }
          }
          // Column-0 text is the PanelEngine selection identity, so it MUST be
          // unique: on a (degenerate) label collision, suffix " #N" instead of
          // first-wins-emplace silently routing the click to the wrong session.
          std::string label = sessionLabel(s);
          for (int dup = 2; row_to_keys.count(label) > 0; ++dup) {
            label = fmt::format("{} #{}", sessionLabel(s), dup);
          }
          rows.push_back({label, dateOnly(s.max_ts_ns), formatBytes(total)});
          row_keys.push_back(s.keys);
          row_to_keys.emplace(std::move(label), s.keys);
          if (any_visible) {
            visible.push_back(static_cast<int>(rows.size()) - 1);
          }
        }
      } else {
        // One row per file (the Name column shows the date= stripped display).
        rows.reserve(state_.sequences.size());
        row_keys.reserve(state_.sequences.size());
        for (std::size_t i = 0; i < state_.sequences.size(); ++i) {
          const auto& rec = state_.sequences[i];
          const std::string& display = i < state_.seq_display_names.size() ? state_.seq_display_names[i] : rec.name;
          rows.push_back({display, dateOnly(rec.max_ts_ns), formatBytes(rec.total_size_bytes)});
          row_keys.push_back({rec.name});
          row_to_keys.emplace(display, std::vector<std::string>{rec.name});
        }
        visible = file_visible;  // 1:1 row==file in this mode
      }

      cache.rows = std::move(rows);
      cache.row_keys = std::move(row_keys);
      // Keep ONE prior generation of the label->keys map: clicks are harvested
      // against on-screen text that can be a refresh older than this rebuild
      // (see prev_row_to_keys in the header).
      cache.prev_row_to_keys = std::move(cache.row_to_keys);
      cache.row_to_keys = std::move(row_to_keys);
      cache.visible = std::move(visible);
      cache.valid = true;
      cache.aggregate = state_.aggregate;
      cache.filter_tab = state_.filter_tab;
      cache.basic_filter = state_.basic_filter;
      cache.seq_epoch = state_.seq_epoch;
      cache.seq_filter = state_.seq_filter;
      cache.seq_filter_regex = state_.seq_filter_regex;
      cache.query_text = state_.query_text;
      cache.date_from_ns = state_.date_from_ns;
      cache.date_to_ns = state_.date_to_ns;
    }

    // Heavy-key gating: rows/headers only when the cache was rebuilt this tick
    // (every content mutation funnels through seq_epoch / the cache-hit fields).
    if (!cache_hit || !state_.seq_rows_pushed) {
      wd.setTableHeaders("seqTable", {"Name", "Date", "Size"});
      wd.setTableRows("seqTable", cache.rows);
      state_.seq_rows_pushed = true;
    }

    // Info/metadata panel: hidden by default, shown when the checkShowInfo
    // checkbox is ticked. The PanelEngine/QUiLoader does NOT honor the .ui
    // `visible=false` property — so visibility must be pushed via the
    // widget-data API every tick. setChecked keeps the box in sync (QSignalBlocker
    // in the host's apply pass prevents a feedback toggle).
    wd.setVisible("dataViewContainer", state_.show_info);
    wd.setChecked("checkShowInfo", state_.show_info);
    // Info toggle RETIRED from the chrome for now (2026-07-12) — hidden, not
    // deleted: all the Info-panel plumbing (show_info, dataViewContainer, the
    // metadata renderer) stays live so one flipped literal brings it back.
    wd.setVisible("checkShowInfo", false);
    wd.setChecked("checkAggregate", state_.aggregate);
    // Basic|Advanced filter mode (the radio pair the host renders as a
    // DualOptionsWidget) + the Basic-tab dropdowns. Each dropdown lists
    // "(any)" + the distinct values of its S3-key field; index 0 = no
    // constraint. Exactly one pane is visible per mode.
    wd.setChecked("radioFilterBasic", state_.filter_tab == 0);
    wd.setChecked("radioFilterAdvanced", state_.filter_tab == 1);
    wd.setVisible("basicPane", state_.filter_tab == 0);
    wd.setVisible("advancedPane", state_.filter_tab == 1);
    for (const auto& [key, label] : kBasicFilterKeys) {
      const std::vector<std::string> values = distinctFieldValues(state_.sequences, key);
      std::vector<std::string> items;
      items.reserve(values.size() + 1);
      items.emplace_back("(any)");
      for (const auto& v : values) {
        items.push_back(v);
      }
      const std::string combo = std::string("filter_") + key;
      wd.setItems(combo, items);
      int idx = 0;
      if (auto it = state_.basic_filter.find(key); it != state_.basic_filter.end() && !it->second.empty()) {
        if (auto vit = std::find(values.begin(), values.end(), it->second); vit != values.end()) {
          idx = static_cast<int>(vit - values.begin()) + 1;
        }
      }
      wd.setCurrentIndex(combo, idx);
    }

    // (The D8 "Folder:" prefix combo was removed 2026-07-12: with Hive keys the
    // top-level prefix IS the customer dimension, which the Basic metadata
    // filter covers properly — vocabulary, persistence, stale-value pruning.)
    std::vector<int> seq_visible = cache.visible;
    if (seq_visible != state_.seq_visible_pushed) {
      wd.setVisibleRows("seqTable", seq_visible);
      state_.seq_visible_pushed = seq_visible;
    }

    // Selection highlight, recomputed fresh from selected_sequences (the real-key
    // source of truth) ∩ each row's key list. Robust across both view modes and
    // any re-sort — a row is selected iff all its files are in the selection.
    std::vector<int> selected_rows;
    if (!state_.selected_sequences.empty()) {
      std::unordered_set<std::string> sel(state_.selected_sequences.begin(), state_.selected_sequences.end());
      for (std::size_t i = 0; i < cache.row_keys.size(); ++i) {
        const auto& ks = cache.row_keys[i];
        if (!ks.empty() && std::all_of(ks.begin(), ks.end(), [&](const std::string& k) { return sel.count(k) > 0; })) {
          selected_rows.push_back(static_cast<int>(i));
        }
      }
    }
    state_.seq_selected_rows = selected_rows;  // keep other consumers consistent
    // Hidden-selection policy (2026-07-12): if the current filters/view hide
    // EVERY selected row, drop the selection entirely — topics + slider reset.
    // Otherwise the panel holds a selection the user cannot see (a 20-minute
    // slider span with no highlighted row). Skipped mid-fetch: the download
    // snapshot took the selection at click time and keeps its bookkeeping.
    if (!state_.selected_sequences.empty() && !state_.fetch_active && !state_.sequences.empty()) {
      const std::set<int> visible_set(seq_visible.begin(), seq_visible.end());
      const bool any_visible = std::any_of(selected_rows.begin(), selected_rows.end(),
                                           [&](int r) { return visible_set.count(r) > 0; });
      if (!any_visible) {
        clearSelectionStateLocked();
        selected_rows.clear();
      }
    }
    if (!selected_rows.empty()) {
      wd.setSelectedRows("seqTable", selected_rows);
    }
    // Header doubles as the connection-state line while there is nothing to
    // count — a bare "Sequences (0/0)" after a silent failed auto-connect gave
    // no hint that connecting (not filtering) is the missing step.
    if (state_.connecting) {
      wd.setLabel("seqHeader", "Sequences - connecting...");
    } else if (!state_.connected) {
      wd.setLabel("seqHeader", "Sequences - not connected");
    } else {
      wd.setLabel("seqHeader", fmt::format("Sequences ({}/{})", seq_visible.size(), cache.rows.size()));
    }
  }

  // Topic table — name-substring filter via visible_rows. Multi-select
  // semantics handled by the .ui (selectionMode=MultiSelection). Two selection
  // modes (the All|Custom radio pair the host renders as a DualOptionsWidget):
  // All = every listed topic downloads, the table is inert. Custom = the
  // selected rows download (+ kForcedTopics appended implicitly at fetch time);
  // zero-count rows are disabled (nothing to fetch).
  {
    std::vector<std::vector<std::string>> rows;
    std::vector<int> visible;
    std::vector<int> disabled;
    rows.reserve(state_.topic_names.size());
    for (size_t i = 0; i < state_.topic_names.size(); ++i) {
      const auto& name = state_.topic_names[i];
      // Per-topic MESSAGE COUNT ("Count"): the wire TopicInfo carries no
      // per-topic byte size (deriving one needs the deferred file_metrics body
      // pass), so the second column shows the count we genuinely have. "0" is
      // deliberately visible — a declared-but-empty channel is worth noticing.
      std::string count_text;
      std::int64_t count = -1;  // unknown until topic_infos arrives
      if (i < state_.topic_infos.size()) {
        count = state_.topic_infos[i].message_count;
        count_text = fmt::format("{}", count);
      }
      // Custom mode: a zero-count row is not selectable (nothing to download).
      if (!state_.topics_all && count == 0) {
        disabled.push_back(static_cast<int>(i));
      }
      rows.push_back({name, count_text});
      if (nameMatches(name, state_.topic_filter, state_.topic_filter_regex)) {
        visible.push_back(static_cast<int>(i));
      }
    }
    wd.setChecked("radioTopicsAll", state_.topics_all);
    wd.setChecked("radioTopicsCustom", !state_.topics_all);
    // In All mode the table is inert — the mode already selects everything.
    wd.setEnabled("topicTable", !state_.topics_all);
    wd.setTableHeaders("topicTable", {"Name", "Count"});
    wd.setTableRows("topicTable", rows);
    wd.setVisibleRows("topicTable", visible);
    wd.setDisabledRows("topicTable", state_.topics_all ? std::vector<int>{} : disabled);
    if (!state_.topic_selected_rows.empty()) {
      wd.setSelectedRows("topicTable", state_.topic_selected_rows);
    }
    if (state_.fetch_active && !state_.fetch_status.empty()) {
      wd.setLabel("topicHeader", fetchStatusLineLocked());
    } else if (state_.topics_loading) {
      wd.setLabel("topicHeader", "Topics — loading…");
    } else if (!state_.topics_failed.empty()) {
      // At least one selected sequence's topics request failed (timeout / dead
      // socket / server error). Failures are not cached, so reselecting the
      // row (or reconnecting) retries.
      wd.setLabel("topicHeader", fmt::format("Topics — request failed ({} of {}); reselect to retry",
                                             state_.topics_failed.size(), state_.selected_sequences.size()));
    } else if (state_.topic_names.empty()) {
      wd.setLabel("topicHeader", "Topics");
    } else if (state_.topics_all) {
      wd.setLabel("topicHeader", fmt::format("Topics (all {})", state_.topic_names.size()));
    } else {
      // Selection feedback: how many topics the Download will actually carry.
      wd.setLabel(
          "topicHeader",
          fmt::format("Topics ({}/{})", state_.topic_selected_rows.size(), state_.topic_names.size()));
    }
  }

  // Info / metadata panel — sequence block on top, then each selected topic.
  // Topic blocks use the full metadata (incl. Arrow schema) cached from
  // getTopicMetadata when available, otherwise the partial listTopics record.
  {
    std::string info_text;
    std::string header = "Info";
    // During a fetch the Info panel doubles as the per-topic progress view
    // (PJ3 DownloadStatsDialog content), since the panel model has no separate
    // progress window. Shows each selected topic's bytes + status.
    if (state_.fetch_active) {
      header = "Download progress";
      info_text += fetchStatusLineLocked() + "\n\n";
      for (int row : state_.topic_selected_rows) {
        if (row < 0 || row >= static_cast<int>(state_.topic_names.size())) {
          continue;
        }
        const std::string& tname = state_.topic_names[static_cast<size_t>(row)];
        std::int64_t bytes = 0;
        if (auto it = state_.bytes_by_topic.find(tname); it != state_.bytes_by_topic.end()) {
          bytes = it->second;
        }
        std::string status = "downloading…";
        if (auto it = state_.topic_fetch_status.find(tname);
            it != state_.topic_fetch_status.end() && !it->second.empty()) {
          status = it->second;
        }
        // Per-topic rolling speed over the same 5 s window the aggregate uses,
        // surfacing PJ3 DownloadStatsDialog's per-topic Speed column. Only shown
        // while the topic is still transferring (Done/Failed/Cancelled are static).
        double topic_bps = 0.0;
        if (auto sit = state_.speed_samples.find(tname); sit != state_.speed_samples.end() && sit->second.size() >= 2) {
          const auto& s = sit->second;
          const std::int64_t dt_ms = s.back().ms - s.front().ms;
          if (dt_ms > 0) {
            topic_bps = static_cast<double>(s.back().bytes - s.front().bytes) * 1000.0 / static_cast<double>(dt_ms);
          }
        }
        const double mib = static_cast<double>(bytes) / (1024.0 * 1024.0);
        const bool transferring = (status == "downloading…" || status == "Cancelling…");
        if (transferring) {
          info_text += fmt::format(
              "{}\n    {:.2f} MiB · {:.2f} MiB/s · {}\n", tname, mib, topic_bps / (1024.0 * 1024.0), status);
        } else {
          info_text += fmt::format("{}\n    {:.2f} MiB · {}\n", tname, mib, status);
        }
      }
      wd.setPlainText("dataView", info_text);
      wd.setLabel("dataViewHeader", header);
    } else if (state_.selected_sequences.size() > 1) {
      // Slice 7: stitched summary header for a multi-file selection (union
      // span/size/messages + the ordered file list).
      std::vector<const SequenceRecord*> recs;
      for (const auto& s : state_.sequences) {
        if (std::find(state_.selected_sequences.begin(), state_.selected_sequences.end(), s.name) !=
            state_.selected_sequences.end()) {
          recs.push_back(&s);
        }
      }
      if (!recs.empty()) {
        info_text += buildStitchedInfoText(recs);
        header = fmt::format("Info — {} files", recs.size());
      }
      for (int row : state_.topic_selected_rows) {
        if (row < 0 || row >= static_cast<int>(state_.topic_names.size())) {
          continue;
        }
        const std::string& tname = state_.topic_names[static_cast<size_t>(row)];
        info_text += "\n";
        info_text += std::string(40, '-');
        info_text += "\n\n";
        if (auto it = state_.topic_meta.find(tname); it != state_.topic_meta.end()) {
          info_text += buildTopicInfoText(it->second);
        } else if (row < static_cast<int>(state_.topic_infos.size())) {
          info_text += buildTopicInfoText(state_.topic_infos[static_cast<size_t>(row)]);
          info_text += "  (loading schema…)\n";
        }
      }
      wd.setPlainText("dataView", info_text);
      wd.setLabel("dataViewHeader", header);
    } else {
      const SequenceRecord* seq_rec = nullptr;
      if (!state_.primary_sequence.empty()) {
        for (const auto& s : state_.sequences) {
          if (s.name == state_.primary_sequence) {
            seq_rec = &s;
            break;
          }
        }
      }
      if (seq_rec != nullptr) {
        info_text += buildSequenceInfoText(*seq_rec);
        // The header is a QLabel that sizes to its text — a full ~125-char Hive
        // key stretches the whole panel. Show just the filename (the full key is
        // still in the Sequence/s3_key rows of the body).
        header = fmt::format("Info — {}", baseName(seq_rec->name));
      }
      for (int row : state_.topic_selected_rows) {
        if (row < 0 || row >= static_cast<int>(state_.topic_names.size())) {
          continue;
        }
        const std::string& tname = state_.topic_names[static_cast<size_t>(row)];
        info_text += "\n";
        info_text += std::string(40, '-');
        info_text += "\n\n";
        if (auto it = state_.topic_meta.find(tname); it != state_.topic_meta.end()) {
          info_text += buildTopicInfoText(it->second);
        } else if (row < static_cast<int>(state_.topic_infos.size())) {
          info_text += buildTopicInfoText(state_.topic_infos[static_cast<size_t>(row)]);
          info_text += "  (loading schema…)\n";
        }
      }
      wd.setPlainText("dataView", info_text);
      wd.setLabel("dataViewHeader", header);
    }
  }

  if (state_.open_cert_pending) {
    state_.open_cert_pending = false;
    // Pre-fill the cert sub-dialog with the saved credentials for this server
    // (PJ3 CertDialog parity). PanelEngine applies these to the sub-dialog's
    // certPath / apiKey / allowInsecure inputs before showing it. We surface
    // the *saved* values, not the DEXORY_CLOUD_API_KEY env
    // fallback, so the dialog reflects what's actually persisted.
    const ServerCredentials saved = loadCredentialsForUri(settings_, credentialStore(), state_.uri);
    wd.setText("certPath", saved.cert_path);
    wd.setText("apiKey", saved.api_key);
    wd.setChecked("allowInsecure", saved.allow_insecure);
    // Open the embedded cert_dialog.ui as a read-only modal popup. The
    // existing requestSubDialog mechanism in dialog_protocol only surfaces
    // the UI — there's no roundtrip of user edits back to the plugin yet.
    // PJ3-parity persistence reads cert path + api key from the settings store
    // (loadCredentialsForUri) on next Connect; the Cert dialog surface here
    // gives the user a way to inspect/initiate that flow.
    state_.active_sub_dialog = DialogState::ActiveSubDialog::kCert;
    wd.requestSubDialog(kCertDialogUi);
  }

  // Tag editor (Slice 6, Plan D Task 9). Mirrors the cert sub-dialog idiom: this
  // is the SAME widget_data() snapshot the host reads to pre-fill the modal
  // sub-dialog (panel_engine applyWidgetData(sub_dialog, view) right before
  // exec()), so the header label + the read-only effective-tags table MUST be
  // set in the very call that issues requestSubDialog. The sub-dialog then runs
  // a nested modal loop with our tick paused — push buttons inside it are inert
  // and no further widget_data() runs until it closes — so this is a one-shot
  // pre-fill, not a per-tick refresh. The post-commit table refresh rides the
  // next reopen against the freshly re-listed tags.
  if (state_.open_tag_pending) {
    state_.open_tag_pending = false;
    wd.setLabel("tagSequenceLabel", fmt::format("Tags — {}", baseName(state_.tag_edit_sequence)));
    wd.setTableHeaders("tagTable", {"Key", "Value", "Source"});
    const auto rows = buildTagEditorRowsLocked();
    std::vector<std::vector<std::string>> table_rows;
    table_rows.reserve(rows.size());
    for (const auto& r : rows) {
      table_rows.push_back({r.key, r.value, r.source});
    }
    wd.setTableRows("tagTable", table_rows);
    for (std::size_t i = 0; i < rows.size(); ++i) {
      // Tint override rows so the user can tell them apart from embedded tags at
      // a glance. "" clears any prior tint (embedded rows).
      wd.setRowColor("tagTable", static_cast<int>(i), rows[i].is_override ? "#fff3cd" : "");
    }
    state_.tag_dialog_open = true;
    state_.active_sub_dialog = DialogState::ActiveSubDialog::kTag;
    wd.requestSubDialog(kTagDialogUi);
  }

  if (state_.close_pending) {
    wd.requestClose("user_back");
    state_.close_pending = false;
  }

  return wd.toJson();
}

bool DexoryCloudDialog::onTextChanged(std::string_view widget_name, std::string_view text) {
  std::lock_guard<std::mutex> lock(state_.mu);
  if (widget_name == "comboUri") {
    state_.uri = std::string(text);
    return true;
  }
  if (widget_name == "seqFilter") {
    state_.seq_filter = std::string(text);
    return true;
  }
  if (widget_name == "topicFilter") {
    state_.topic_filter = std::string(text);
    return true;
  }
  if (widget_name == "lua_queryBar") {
    state_.query_text = std::string(text);
    return true;
  }
  // Cert sub-dialog input widgets: panel_engine fires onTextChanged for
  // each text/checkable child after the user clicks OK,
  // followed by an onClicked("subDialogAccepted") to commit. We just
  // stage the value; commit happens in onClicked below.
  if (widget_name == "certPath") {
    state_.pending_cert_path = std::string(text);
    state_.has_pending_cert_edit = true;
    return true;
  }
  if (widget_name == "apiKey") {
    state_.pending_api_key = std::string(text);
    state_.has_pending_api_key_edit = true;
    return true;
  }
  // Tag-editor sub-dialog inputs (Slice 6). Same idiom as the cert dialog: the
  // host harvests every QLineEdit value on OK (right before subDialogAccepted),
  // so we just stage key/value here; the commit happens in subDialogAccepted.
  // (Push buttons inside a PanelEngine sub-dialog are inert — only line-edits /
  // checkboxes / combos are harvested — so the staging lives in these inputs,
  // not in tagSetButton/tagUnsetButton clicks.)
  if (widget_name == "tagKey") {
    state_.pending_tag_key = std::string(text);
    return true;
  }
  if (widget_name == "tagValue") {
    state_.pending_tag_value = std::string(text);
    return true;
  }
  if (widget_name == "allowInsecure") {
    // Checkable values arrive through the typed `checked` channel as
    // booleans (onToggled), not through onTextChanged. The legacy
    // string-encoded "true"/"false" path is kept for cases where the
    // event arrives as text — defensive only.
    state_.pending_allow_insecure = (text == "true");
    state_.has_pending_allow_insecure_edit = true;
    return true;
  }
  return false;
}

bool DexoryCloudDialog::onClicked(std::string_view widget_name) {
  if (widget_name == "buttonClose") {
    std::lock_guard<std::mutex> lock(state_.mu);
    state_.close_pending = true;
    return true;
  }
  if (widget_name == "buttonConnect") {
    std::string uri;
    std::string cert_path;
    std::string api_key;
    bool allow_insecure = false;
    {
      std::lock_guard<std::mutex> lock(state_.mu);
      uri = state_.uri;
    }
    // Pull saved credentials keyed by the normalized server URI (PJ3 parity:
    // dedupes "grpc+tls://X:6726", "GRPC+TLS://X:6726", and "x:6726" to the
    // same cache entry), with DEXORY_CLOUD_API_KEY env fallback.
    auto creds = resolveCredentials(settings_, credentialStore(), uri);
    cert_path = creds.cert_path;
    api_key = creds.api_key;
    allow_insecure = creds.allow_insecure;

    // Printable-ASCII gate (PJ3 main_window.cpp:947-1013). The host/cert/api-key
    // are headers/URIs handed straight into gRPC, which asserts-and-aborts on
    // control bytes (CR/LF/NUL). Reject here — before the worker is dispatched —
    // and abort the connect so the worst outcome is a visible notification.
    if (!isPrintableAscii(uri)) {
      notify(PJ::ToolboxMessageLevel::kError, "Server URI contains invalid characters (control or non-ASCII bytes).");
      return true;
    }
    if (!isPrintableAscii(cert_path)) {
      notify(PJ::ToolboxMessageLevel::kError, "TLS certificate path contains invalid characters.");
      return true;
    }
    if (!isPrintableAscii(api_key)) {
      notify(PJ::ToolboxMessageLevel::kError, "API key contains invalid characters (control or non-ASCII bytes).");
      return true;
    }

    {
      std::lock_guard<std::mutex> lock(state_.mu);
      state_.connecting = true;
      state_.suppress_connect_error = false;  // explicit Connect reports failures
      // Capability lifecycle (Codex review): the URI may now point at a
      // DIFFERENT server, so the previous connection's latch must not leak
      // into the new handshake window — drop it now and let
      // onServerCapabilitiesReady re-latch from the NEW server's Hello.
      state_.tag_edit_supported = false;
    }
    notify(PJ::ToolboxMessageLevel::kInfo, fmt::format("Connecting to {}…", uri));
    postCommand([w = worker_.get(), uri, cert_path, api_key, allow_insecure] {
      w->connectAsync(uri, cert_path, api_key, allow_insecure);
    });
    return true;
  }
  if (widget_name == "buttonRefresh") {
    // Re-list sequences without a disconnect/reconnect (PJ3 onRefreshClicked,
    // main_window.cpp:933-945). No-op unless connected and idle.
    {
      std::lock_guard<std::mutex> lock(state_.mu);
      if (!state_.connected || state_.connecting || state_.fetch_active) {
        return true;
      }
    }
    notify(PJ::ToolboxMessageLevel::kInfo, "Refreshing sequences…");
    postCommand([w = worker_.get()] { w->listSequencesAsync(); });
    return true;
  }
  if (widget_name == "buttonCert") {
    std::lock_guard<std::mutex> lock(state_.mu);
    state_.open_cert_pending = true;
    return true;
  }
  // Tag editor (Slice 6, Plan D Task 9): open the modal tag editor for the
  // selected sequence. Resets the staged-edit buffers so each open starts clean.
  if (widget_name == "buttonEditTags") {
    std::lock_guard<std::mutex> lock(state_.mu);
    // Slice 7: tag editing is single-selection only; a multi-select disables the
    // button. D2: also re-check tag_edit_supported (read-only catalog / no
    // tag-edit IPC configured) — same defense-in-depth as the other conditions
    // here. Guard against a stray click in any state where the button should be
    // disabled.
    if (!state_.connected || state_.connecting || state_.seq_selected_rows.size() != 1 ||
        state_.primary_sequence.empty() || state_.fetch_active || !state_.tag_edit_supported) {
      return true;  // button is disabled in this state; ignore a stray click
    }
    state_.tag_edit_sequence = state_.primary_sequence;
    state_.staged_set_tags.clear();
    state_.staged_unset_keys.clear();
    state_.pending_tag_key.clear();
    state_.pending_tag_value.clear();
    state_.open_tag_pending = true;
    return true;
  }
  // Stage an upsert of (tagKey, tagValue). Last-wins on the same key; a key that
  // was previously staged for unset is un-staged from unset.
  if (widget_name == "tagSetButton") {
    std::lock_guard<std::mutex> lock(state_.mu);
    const std::string key = state_.pending_tag_key;
    const std::string value = state_.pending_tag_value;
    if (key.empty()) {
      return true;  // nothing to stage; ignore
    }
    // Drop any prior staged set of this key (last-wins) and any staged unset.
    state_.staged_set_tags.erase(
        std::remove_if(state_.staged_set_tags.begin(), state_.staged_set_tags.end(),
                       [&key](const std::pair<std::string, std::string>& kv) { return kv.first == key; }),
        state_.staged_set_tags.end());
    state_.staged_unset_keys.erase(
        std::remove(state_.staged_unset_keys.begin(), state_.staged_unset_keys.end(), key),
        state_.staged_unset_keys.end());
    state_.staged_set_tags.emplace_back(key, value);
    return true;
  }
  // Stage an unset of the keyed tag. Removes any prior staged set of that key.
  if (widget_name == "tagUnsetButton") {
    std::lock_guard<std::mutex> lock(state_.mu);
    const std::string key = state_.pending_tag_key;
    if (key.empty()) {
      return true;  // nothing to stage; ignore
    }
    state_.staged_set_tags.erase(
        std::remove_if(state_.staged_set_tags.begin(), state_.staged_set_tags.end(),
                       [&key](const std::pair<std::string, std::string>& kv) { return kv.first == key; }),
        state_.staged_set_tags.end());
    if (std::find(state_.staged_unset_keys.begin(), state_.staged_unset_keys.end(), key) ==
        state_.staged_unset_keys.end()) {
      state_.staged_unset_keys.push_back(key);
    }
    return true;
  }
  // Regex-mode toggles (checkable PushButtons): one click = one toggle, so the
  // plugin flips its own flag in lock-step with the button's visual state.
  if (widget_name == "seqRegexToggle") {
    std::lock_guard<std::mutex> lock(state_.mu);
    state_.seq_filter_regex = !state_.seq_filter_regex;
    return true;
  }
  if (widget_name == "topicRegexToggle") {
    std::lock_guard<std::mutex> lock(state_.mu);
    state_.topic_filter_regex = !state_.topic_filter_regex;
    return true;
  }
  if (widget_name == "subDialogAccepted") {
    // Both the cert and tag editors emit this synthetic click on OK, so the
    // handler disambiguates on which sub-dialog was last opened.
    DialogState::ActiveSubDialog which = DialogState::ActiveSubDialog::kNone;
    {
      std::lock_guard<std::mutex> lock(state_.mu);
      which = state_.active_sub_dialog;
      state_.active_sub_dialog = DialogState::ActiveSubDialog::kNone;
      state_.tag_dialog_open = false;
    }

    if (which == DialogState::ActiveSubDialog::kTag) {
      // Tag editor committed (Plan D Task 9). The host harvested the tagKey /
      // tagValue inputs into pending_tag_* just before this click. Derive ONE
      // operation from them — non-empty value upserts an override; an empty
      // value with a key removes the override / NULL-masks an embedded tag —
      // merged with anything previously staged this session (last-wins). Then
      // post the worker round-trip, which re-lists so the Lua filter sees it.
      std::string seq;
      std::vector<std::pair<std::string, std::string>> set_tags;
      std::vector<std::string> unset_keys;
      {
        std::lock_guard<std::mutex> lock(state_.mu);
        const std::string key = state_.pending_tag_key;
        const std::string value = state_.pending_tag_value;
        if (!key.empty()) {
          // Fold the just-harvested input into the staged lists (last-wins).
          state_.staged_set_tags.erase(
              std::remove_if(state_.staged_set_tags.begin(), state_.staged_set_tags.end(),
                             [&key](const std::pair<std::string, std::string>& kv) { return kv.first == key; }),
              state_.staged_set_tags.end());
          state_.staged_unset_keys.erase(
              std::remove(state_.staged_unset_keys.begin(), state_.staged_unset_keys.end(), key),
              state_.staged_unset_keys.end());
          if (value.empty()) {
            state_.staged_unset_keys.push_back(key);
          } else {
            state_.staged_set_tags.emplace_back(key, value);
          }
        }
        seq = state_.tag_edit_sequence;
        set_tags = state_.staged_set_tags;
        unset_keys = state_.staged_unset_keys;
        // Clear staging now that we've snapshotted it for the commit.
        state_.staged_set_tags.clear();
        state_.staged_unset_keys.clear();
        state_.pending_tag_key.clear();
        state_.pending_tag_value.clear();
        state_.tag_edit_sequence.clear();
      }
      if (seq.empty() || (set_tags.empty() && unset_keys.empty())) {
        notify(PJ::ToolboxMessageLevel::kInfo, "No tag changes to apply");
        return true;
      }
      notify(PJ::ToolboxMessageLevel::kInfo, fmt::format("Updating tags for {}…", seq));
      postCommand([w = worker_.get(), seq, set_tags, unset_keys] {
        w->updateTagsAsync(seq, set_tags, unset_keys);
      });
      return true;
    }

    // Cert sub-dialog committed. Merge any staged edits over the
    // currently-cached credentials and write back to the settings store keyed
    // by the current URI. The next buttonConnect click will re-read
    // these via the credential cache and hand them to the backend.
    std::string uri;
    {
      std::lock_guard<std::mutex> lock(state_.mu);
      uri = state_.uri;
    }
    ServerCredentials updated = loadCredentialsForUri(settings_, credentialStore(), uri);
    {
      std::lock_guard<std::mutex> lock(state_.mu);
      if (state_.has_pending_cert_edit) {
        updated.cert_path = state_.pending_cert_path;
      }
      if (state_.has_pending_api_key_edit) {
        updated.api_key = state_.pending_api_key;
      }
      if (state_.has_pending_allow_insecure_edit) {
        updated.allow_insecure = state_.pending_allow_insecure;
      }
      state_.has_pending_cert_edit = false;
      state_.has_pending_api_key_edit = false;
      state_.has_pending_allow_insecure_edit = false;
      state_.pending_cert_path.clear();
      state_.pending_api_key.clear();
      state_.pending_allow_insecure = false;
    }
    saveCredentialsForUri(settings_, credentialStore(), uri, updated);
    notify(PJ::ToolboxMessageLevel::kInfo, "Credentials saved");
    return true;
  }
  if (widget_name == "buttonFetch") {
    // IN-DIALOG IMPORT (toolbox shape): the worker opens a fresh session and
    // delegates message parsing to the host's MessageParser plugins via
    // ParserIngestDriver. Per-topic progress + the close/import policy ride the
    // pull ledger (onPullProgress/onPullFinished/onAllFetchesComplete).
    std::vector<std::string> ordered_names;
    std::string display_name;
    std::vector<std::string> topics;
    std::int64_t start = 0;
    std::int64_t end = 0;
    std::string overlap_error;
    {
      std::lock_guard<std::mutex> lock(state_.mu);
      if (!state_.connected || state_.selected_sequences.empty()) {
        notify(PJ::ToolboxMessageLevel::kWarning, "Select a sequence + topic(s) first");
        return true;
      }
      int empty_topics = 0;
      if (state_.topics_all) {
        // All mode: request every listed topic EXPLICITLY (not the wire's
        // "empty = all" shorthand) so the per-topic pull ledger, progress and
        // estimates see the same list the user saw. Zero-count topics are
        // expected here — no pre-flight warning.
        topics = state_.topic_names;
      } else {
        for (int row : state_.topic_selected_rows) {
          if (row >= 0 && row < static_cast<int>(state_.topic_names.size())) {
            topics.push_back(state_.topic_names[row]);
            // Catalog-declared topics with ZERO recorded messages are selectable
            // (the recorder writes the channel even when nothing published — 75
            // of 171 on the Dexory staging bags). Count them for the pre-flight
            // hint below; the server will return no data for them.
            if (row < static_cast<int>(state_.topic_infos.size()) && state_.topic_infos[row].message_count == 0) {
              ++empty_topics;
            }
          }
        }
      }
      if (topics.empty()) {
        notify(PJ::ToolboxMessageLevel::kWarning, "Select a sequence + topic(s) first");
        return true;
      }
      if (!state_.topics_all) {
        // Implicit TF inclusion: append kForcedTopics that the selection lacks
        // (only those actually listed for this sequence set). All-mode already
        // carries the full list.
        for (std::string_view forced : kForcedTopics) {
          if (std::find(topics.begin(), topics.end(), forced) != topics.end()) {
            continue;
          }
          if (std::find(state_.topic_names.begin(), state_.topic_names.end(), forced) !=
              state_.topic_names.end()) {
            topics.emplace_back(forced);
          }
        }
      }
      if (empty_topics > 0) {
        // Non-blocking heads-up: these will come back "no messages in the
        // selected time range" rather than failing to decode.
        notify(PJ::ToolboxMessageLevel::kWarning,
               fmt::format("{} of {} selected topic(s) have no recorded messages in the selected sequence(s)",
                           empty_topics, topics.size()));
      }
      // Slice 7: build the synthetic stitched selection over the selected
      // sequences. The ordered name list is deterministic (min_ts, name) — a
      // reordered selection yields the same request. The union [min, max] drives
      // the proportional time-range mapping.
      std::vector<SelInput> sel_inputs;
      for (const auto& s : state_.sequences) {
        if (std::find(state_.selected_sequences.begin(), state_.selected_sequences.end(), s.name) ==
            state_.selected_sequences.end()) {
          continue;
        }
        sel_inputs.push_back({s.name, s.min_ts_ns, s.max_ts_ns, s.total_size_bytes, messageCountOf(s)});
      }
      // Client-side overlap pre-validation (design spec §6.3: the multi-recording
      // UI rejects overlap selections client-side). The server check stays
      // authoritative; this is the fast UX guard — bail BEFORE any transport call.
      if (sel_inputs.size() > 1) {
        overlap_error = validateNonOverlapping(sel_inputs);
      }
      if (overlap_error.empty()) {
        const StitchedSelection stitched = buildStitchedSelection(sel_inputs);
        ordered_names = stitched.ordered_names;
        // Elide for the host dataset label / data tree + the status line (display
        // only; the fetch request below uses the full ordered_names). The tail-
        // biased elision preserves any " (+N more)" suffix.
        display_name = elideMiddle(stitched.display_name, 56);
        // Convert proportional 0..kSliderSteps into absolute nanoseconds against
        // the UNION [min_ts, max_ts]. (0,0) means "whole stitched range". The
        // mapping is OVERFLOW-SAFE (sliderToWindow computes the offset in
        // 128-bit): a multi-hour aggregate span * a slider position used to
        // overflow int64 and wrap the window negative, so every topic came back
        // "no messages in the selected time range" — see slider_window.h.
        const SliderWindow win = sliderToWindow(stitched.union_min_ts_ns, stitched.union_max_ts_ns,
                                                state_.range_lower, state_.range_upper, DialogState::kSliderSteps);
        if (win.has_window) {
          start = win.start_ns;
          end = win.end_ns;
        }
        // Reset the per-batch fetch ledger (PJ3 parity: a batch completes via
        // allFetchesComplete, not per-topic).
        state_.fetch_active = true;
        state_.cancelling = false;
        // Immediate feedback: the worker's first pullPhase can be seconds away
        // (the per-download connection itself must connect over the WAN).
        state_.fetch_status = "Starting download - connecting session";
        state_.fetch_phase_static = true;
        state_.fetch_phase_started = std::chrono::steady_clock::now();
        state_.fetch_total = static_cast<int>(topics.size());
        state_.fetch_done = 0;
        state_.fetch_failed = 0;
        state_.imported_any = false;
        state_.error_counts.clear();
        state_.bytes_by_topic.clear();
        state_.wire_bytes_total = 0;
        state_.speed_samples.clear();
        state_.topic_fetch_status.clear();
      }
    }
    if (!overlap_error.empty()) {
      // Reject client-side; no transport call, no fetch_active set.
      notify(PJ::ToolboxMessageLevel::kError, fmt::format("Cannot stitch: {}", overlap_error));
      return true;
    }
    worker_->resetCancel();
    notify(PJ::ToolboxMessageLevel::kInfo,
           fmt::format("Fetching {} topic(s) from {}…", topics.size(), display_name));
    postCommand(
        [w = worker_.get(), names = std::move(ordered_names), display_name, topics = std::move(topics), start,
         end]() mutable { w->pullTopicsAsync(std::move(names), display_name, std::move(topics), start, end); });
    persistState();  // crash-resilient: remember query + range at fetch time
    return true;
  }
  if (widget_name == "buttonCancel") {
    worker_->requestCancel();
    {
      std::lock_guard<std::mutex> lock(state_.mu);
      state_.cancelling = true;
      // Reflect the cancel in the Info-panel progress header immediately, and
      // mark every not-yet-finished topic "Cancelling…" so the per-topic view
      // updates without waiting for allFetchesComplete (PJ3 parity).
      state_.fetch_status = "Cancelling…";
      for (int row : state_.topic_selected_rows) {
        if (row < 0 || row >= static_cast<int>(state_.topic_names.size())) {
          continue;
        }
        const std::string& tname = state_.topic_names[static_cast<size_t>(row)];
        auto it = state_.topic_fetch_status.find(tname);
        if (it == state_.topic_fetch_status.end() || it->second.empty() || it->second == "downloading…") {
          state_.topic_fetch_status[tname] = "Cancelling…";
        }
      }
    }
    notify(PJ::ToolboxMessageLevel::kInfo, "Cancelling…");
    return true;
  }
  return false;
}

bool DexoryCloudDialog::onToggled(std::string_view widget_name, bool checked) {
  if (widget_name == "allowInsecure") {
    std::lock_guard<std::mutex> lock(state_.mu);
    state_.pending_allow_insecure = checked;
    state_.has_pending_allow_insecure_edit = true;
    return true;
  }
  if (widget_name == "checkShowInfo") {
    std::lock_guard<std::mutex> lock(state_.mu);
    state_.show_info = checked;  // widget_data() applies the visibility next tick
    return true;
  }
  if (widget_name == "checkAggregate") {
    std::lock_guard<std::mutex> lock(state_.mu);
    state_.aggregate = checked;  // seq_view_cache misses on the mode change and rebuilds
    // Row identities change between the file and session views (one session row
    // = N file keys), so carrying a selection across the toggle produced
    // half-mapped highlights. Start clean instead.
    clearSelectionStateLocked();
    return true;
  }
  if (widget_name == "radioTopicsAll" || widget_name == "radioTopicsCustom") {
    // The All|Custom topic-selection mode (radio pair -> DualOptionsWidget).
    // Only the newly-CHECKED radio moves the mode; the render tick re-applies
    // table enablement / disabled rows / forced topics for the new mode.
    if (checked) {
      std::lock_guard<std::mutex> lock(state_.mu);
      state_.topics_all = (widget_name == "radioTopicsAll");
    }
    return true;
  }
  if (widget_name == "radioFilterBasic" || widget_name == "radioFilterAdvanced") {
    // The Basic|Advanced mode selector (radio pair -> DualOptionsWidget). Each
    // radio reports its own toggle; only the newly-CHECKED one moves the mode
    // (the unchecked partner's `false` arrives too and is ignored).
    if (checked) {
      std::lock_guard<std::mutex> lock(state_.mu);
      // 0 Basic / 1 Advanced — seq_view_cache rebuilds on the change.
      state_.filter_tab = (widget_name == "radioFilterAdvanced") ? 1 : 0;
    }
    return true;
  }
  return false;
}

bool DexoryCloudDialog::onTabChanged(std::string_view /*widget_name*/, int /*index*/) {
  return false;  // no tab widgets left; the filter mode is the radio pair above
}

bool DexoryCloudDialog::onValueChanged(std::string_view /*widget_name*/, int /*value*/) {
  return false;
}

bool DexoryCloudDialog::onRangeChanged(std::string_view widget_name, int lower, int upper) {
  if (widget_name == "rangeSlider") {
    std::lock_guard<std::mutex> lock(state_.mu);
    state_.range_lower = std::clamp(lower, 0, DialogState::kSliderSteps);
    state_.range_upper = std::clamp(upper, 0, DialogState::kSliderSteps);
    return true;
  }
  return false;
}

bool DexoryCloudDialog::onDateRangeChanged(
    std::string_view widget_name, std::string_view from_iso, std::string_view to_iso) {
  if (widget_name != "datePicker") {
    return false;
  }
  // The DateRangePicker emits UTC ISO datetimes (date + time-of-day, empty =
  // unbounded). Convert to epoch-ns (ms precision) for the interval filter.
  constexpr std::int64_t kNsPerMs = 1'000'000LL;
  auto floorDiv = [](std::int64_t value, std::int64_t divisor) {
    const std::int64_t quotient = value / divisor;
    const std::int64_t remainder = value % divisor;
    return remainder < 0 ? quotient - 1 : quotient;
  };
  auto toEpochNsAtMsPrecision = [&](std::int64_t ns) { return floorDiv(ns, kNsPerMs) * kNsPerMs; };
  const auto from_ns = parseIso8601Utc(from_iso);
  const auto to_ns = parseIso8601Utc(to_iso);
  std::lock_guard<std::mutex> lock(state_.mu);
  state_.date_from_ns = from_ns.has_value() ? toEpochNsAtMsPrecision(*from_ns) : 0;
  state_.date_to_ns = to_ns.has_value() ? toEpochNsAtMsPrecision(*to_ns) : 0;
  return true;
}

// The live fetch-status line: the byte-driven progress text as-is, or — while
// a coarse phase ("Opening session…") is showing — the phase plus a live
// elapsed counter, so a long server-side plan-build visibly ticks instead of
// freezing at 0.00 MiB/s. Caller holds state_.mu.
std::string DexoryCloudDialog::fetchStatusLineLocked() const {
  if (!state_.fetch_phase_static) {
    return state_.fetch_status;
  }
  const auto secs =
      std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() - state_.fetch_phase_started)
          .count();
  return fmt::format("{} ({}s)", state_.fetch_status, secs);
}

void DexoryCloudDialog::recomputeTopicUnionLocked() {
  // Slice 7: topic_names / topic_infos = union of every selected sequence's
  // cached topics. Per-name size/message_count are summed across the sequences
  // that carry the topic; names dedup + sort. topics_loading clears only once
  // every selected sequence has a cache entry.
  std::map<std::string, TopicInfo, std::less<>> merged;
  for (const std::string& name : state_.selected_sequences) {
    auto it = state_.topic_infos_by_seq.find(name);
    if (it == state_.topic_infos_by_seq.end()) {
      continue;  // not yet listed
    }
    for (const TopicInfo& info : it->second) {
      auto mit = merged.find(info.topic_name);
      if (mit == merged.end()) {
        merged.emplace(info.topic_name, info);
      } else {
        // Sum the per-file size + message count for the same topic name.
        mit->second.total_size_bytes += info.total_size_bytes;
        mit->second.message_count += info.message_count;
      }
    }
  }

  // All selected sequences reported? If not, keep the loading hint up. A
  // FAILED sequence counts as reported (it has no cache entry by design so the
  // next selection change retries) — otherwise a single failure would pin the
  // "loading…" hint forever.
  bool all_loaded = true;
  for (const std::string& name : state_.selected_sequences) {
    if (state_.topic_infos_by_seq.find(name) == state_.topic_infos_by_seq.end() &&
        state_.topics_failed.find(name) == state_.topics_failed.end()) {
      all_loaded = false;
      break;
    }
  }

  // Capture the prior topic selection by name so it survives the rebuild.
  std::set<std::string> previously_selected;
  for (int r : state_.topic_selected_rows) {
    if (r >= 0 && r < static_cast<int>(state_.topic_names.size())) {
      previously_selected.insert(state_.topic_names[static_cast<std::size_t>(r)]);
    }
  }

  state_.topic_names.clear();
  state_.topic_infos.clear();
  state_.topic_names.reserve(merged.size());
  state_.topic_infos.reserve(merged.size());
  for (auto& [tname, info] : merged) {
    state_.topic_names.push_back(tname);
    state_.topic_infos.push_back(info);
  }
  // Re-apply the current sort + re-map selection by name.
  state_.topic_selected_rows.clear();
  for (std::size_t i = 0; i < state_.topic_names.size(); ++i) {
    if (previously_selected.count(state_.topic_names[i]) > 0) {
      state_.topic_selected_rows.push_back(static_cast<int>(i));
    }
  }
  sortTopicsLocked();
  // schema cache is per (sequence, topic); a changed union invalidates it.
  state_.topic_meta.clear();
  state_.topics_loading = !all_loaded;
}

void DexoryCloudDialog::ensureQuerySchemaLocked() {
  if (state_.query_schema_epoch == state_.seq_epoch) {
    return;
  }
  state_.query_schema.clear();
  for (const auto& rec : state_.sequences) {
    // Canonical filter fields only — the query-assist key/value dropdowns offer
    // the 4 S3-key fields, never MCAP-content stats (matches the filter eval).
    for (const auto& kv : canonicalFilterFields(rec)) {
      state_.query_schema[kv.first].push_back(kv.second);
    }
  }
  state_.query_schema_epoch = state_.seq_epoch;
}

bool DexoryCloudDialog::onCodeChangedWithCursor(std::string_view widget_name, std::string_view code, int cursor) {
  if (widget_name != "lua_queryBar") {
    return false;
  }
  std::lock_guard<std::mutex> lock(state_.mu);
  state_.query_text = std::string(code);
  state_.query_cursor = cursor >= 0 ? cursor : static_cast<int>(state_.query_text.size());
  return true;
}

bool DexoryCloudDialog::onIndexChanged(std::string_view widget_name, int index) {
  if (index < 0) {
    return false;
  }
  // Basic-tab metadata dropdowns (filter_<field>). Item 0 is "(any)" (no
  // constraint); items 1..N are the distinct values, resolved against the same
  // distinctFieldValues() the widget_data populated.
  for (const auto& [key, label] : kBasicFilterKeys) {
    if (widget_name == std::string("filter_") + key) {
      std::lock_guard<std::mutex> lock(state_.mu);
      if (index == 0) {
        state_.basic_filter.erase(std::string(key));
      } else {
        const auto values = distinctFieldValues(state_.sequences, key);
        if (index - 1 < static_cast<int>(values.size())) {
          state_.basic_filter[std::string(key)] = values[static_cast<std::size_t>(index - 1)];
        }
      }
      return true;
    }
  }
  enum class Which { kKey, kOp, kVal };
  Which which;
  if (widget_name == "keyCombo") {
    which = Which::kKey;
  } else if (widget_name == "opCombo") {
    which = Which::kOp;
  } else if (widget_name == "valCombo") {
    which = Which::kVal;
  } else {
    return false;
  }

  std::lock_guard<std::mutex> lock(state_.mu);
  ensureQuerySchemaLocked();
  const int cursor = clampQueryCursor(state_.query_cursor, state_.query_text);
  const CursorContext ctx = analyze(state_.query_text, cursor, state_.query_schema);

  // Resolve the picked item and the action for this dropdown. The item lists
  // mirror what widget_data() populated from the same (text, cursor, schema).
  std::string item;
  Action action = Action::Disabled;
  if (which == Which::kKey) {
    const auto keys = queryAssistKeys(state_.query_schema, state_.metadata_key_vocabulary);
    if (index >= static_cast<int>(keys.size())) {
      return true;
    }
    item = keys[static_cast<std::size_t>(index)];
    action = ctx.key_action;
  } else if (which == Which::kOp) {
    const auto& ops = operators();
    if (index >= static_cast<int>(ops.size())) {
      return true;
    }
    item = ops[static_cast<std::size_t>(index)];
    action = ctx.op_action;
  } else {
    const auto values = schemaValues(state_.query_schema, ctx.context_key);
    if (index >= static_cast<int>(values.size())) {
      return true;
    }
    item = values[static_cast<std::size_t>(index)];
    action = ctx.val_action;
  }
  if (action == Action::Disabled) {
    return true;  // dropdown wasn't actionable at this cursor position
  }

  // A value is a Lua literal: string values must be quoted so they lex as a
  // Value, not a bare Key (keys/operators are inserted verbatim).
  const std::string insert_text = (which == Which::kVal) ? quoteValueForQuery(item) : item;
  const EditResult result = applyCompletion(state_.query_text, ctx, action, insert_text);

  state_.query_text = result.text;
  state_.query_cursor = result.cursor;
  state_.query_push_pending = true;  // push the rewritten text + caret back to the editor
  return true;
}

void DexoryCloudDialog::clearSelectionStateLocked() {
  state_.restore_selected_sequence.clear();
  state_.restore_selected_topics.clear();
  state_.seq_selected_rows.clear();
  state_.selected_sequences.clear();
  state_.primary_sequence.clear();
  state_.topic_names.clear();
  state_.topic_infos.clear();
  state_.topic_selected_rows.clear();
  state_.topics_loading = false;
  state_.topics_failed.clear();
  state_.topics_failure_notified = false;
}

bool DexoryCloudDialog::onSelectionChanged(std::string_view widget_name, const std::vector<std::string>& selected) {
  if (widget_name == "seqTable") {
    if (selected.empty()) {
      std::lock_guard<std::mutex> lock(state_.mu);
      state_.seq_selected_rows.clear();
      state_.selected_sequences.clear();
      state_.primary_sequence.clear();
      state_.topic_names.clear();
      state_.topic_infos.clear();
      state_.topic_selected_rows.clear();
      state_.topics_loading = false;
      return true;
    }
    // Slice 7: the seqTable is ExtendedSelection. Resolve every selected name to
    // a row, build the plural selection, and kick a listTopics PER selected
    // sequence that isn't already cached (union flow). The topic panel then
    // shows the UNION (recomputeTopicUnionLocked) as each result arrives.
    std::vector<std::string> need_topics;
    {
      std::lock_guard<std::mutex> lock(state_.mu);
      // A manual pick supersedes any pending restore.
      state_.restore_selected_sequence.clear();
      state_.restore_selected_topics.clear();

      state_.selected_sequences.clear();
      state_.seq_selected_rows.clear();  // recomputed in widget_data() from selected_sequences
      // The PanelEngine harvests column-0 (display) text. Map each selected row
      // back to the real S3 file key(s) it represents via row_to_keys: one key
      // in file mode, ALL of a session's files in aggregate mode. These keys are
      // the backend identity for every downstream call (topics/stitch/download),
      // so selecting one aggregated session == selecting all its chunks.
      const auto& row_to_keys = state_.seq_view_cache.row_to_keys;
      const auto& prev_row_to_keys = state_.seq_view_cache.prev_row_to_keys;
      for (const std::string& display : selected) {
        auto it = row_to_keys.find(display);
        if (it == row_to_keys.end()) {
          // The click was harvested against a label from the PREVIOUS rebuild
          // (aggregate labels move while the indexer fills a partition). Fall
          // back one generation rather than resolving to zero keys (which
          // would silently empty the Topics panel).
          it = prev_row_to_keys.find(display);
          if (it == prev_row_to_keys.end()) {
            continue;
          }
        }
        for (const std::string& key : it->second) {
          state_.selected_sequences.push_back(key);
        }
      }
      // Deterministic order for the persisted/primary handle; dedup (sessions are
      // disjoint so this only guards against a degenerate double-map).
      std::sort(state_.selected_sequences.begin(), state_.selected_sequences.end());
      state_.selected_sequences.erase(
          std::unique(state_.selected_sequences.begin(), state_.selected_sequences.end()),
          state_.selected_sequences.end());
      state_.primary_sequence = state_.selected_sequences.empty() ? std::string{} : state_.selected_sequences.front();

      // Drop cache entries for sequences no longer selected so a later union
      // recompute doesn't include stale topics.
      for (auto it = state_.topic_infos_by_seq.begin(); it != state_.topic_infos_by_seq.end();) {
        if (std::find(state_.selected_sequences.begin(), state_.selected_sequences.end(), it->first) ==
            state_.selected_sequences.end()) {
          it = state_.topic_infos_by_seq.erase(it);
        } else {
          ++it;
        }
      }
      state_.topic_selected_rows.clear();
      state_.topics_loading = true;  // header shows "loading…" until every seq reports
      // New selection epoch: prior failures are retried (they were never
      // cached) and the one-notification-per-epoch latch re-arms.
      state_.topics_failed.clear();
      state_.topics_failure_notified = false;

      for (const std::string& name : state_.selected_sequences) {
        if (state_.topic_infos_by_seq.find(name) == state_.topic_infos_by_seq.end()) {
          need_topics.push_back(name);
        }
      }
      // If every selected sequence is already cached, recompute the union now.
      recomputeTopicUnionLocked();
    }
    for (const std::string& name : need_topics) {
      postCommand([w = worker_.get(), name] { w->listTopicsAsync(name); });
    }
    return true;
  }
  if (widget_name == "topicTable") {
    // Slice 7: a selected topic may belong to several sequences in a stitched
    // selection. Fetch its metadata against the FIRST selected sequence that
    // carries it (per-sequence topic_infos_by_seq cache); the Info panel only
    // needs one representative record for the schema/tag block.
    std::vector<std::pair<std::string, std::string>> need_metadata;  // (sequence, topic)
    {
      std::lock_guard<std::mutex> lock(state_.mu);
      state_.topic_selected_rows.clear();
      for (const auto& sel : selected) {
        for (size_t i = 0; i < state_.topic_names.size(); ++i) {
          if (state_.topic_names[i] == sel) {
            state_.topic_selected_rows.push_back(static_cast<int>(i));
            // Kick off a one-shot metadata fetch (schema + tag) for the Info
            // panel if we don't already have the full record cached.
            if (state_.topic_meta.find(sel) == state_.topic_meta.end()) {
              for (const std::string& seq_name : state_.selected_sequences) {
                auto cit = state_.topic_infos_by_seq.find(seq_name);
                if (cit == state_.topic_infos_by_seq.end()) {
                  continue;
                }
                const bool carries = std::any_of(cit->second.begin(), cit->second.end(),
                                                 [&sel](const TopicInfo& t) { return t.topic_name == sel; });
                if (carries) {
                  need_metadata.emplace_back(seq_name, sel);
                  break;
                }
              }
            }
            break;
          }
        }
      }
    }
    for (const auto& [seq, topic] : need_metadata) {
      postCommand([w = worker_.get(), seq, topic] { w->fetchTopicMetadataAsync(seq, topic); });
    }
    return true;
  }
  return false;
}

void DexoryCloudDialog::onCapabilitiesReady(BackendCaps caps) {
  std::lock_guard<std::mutex> lock(state_.mu);
  state_.supports_file_hierarchy = caps.supports_file_hierarchy;
  // The Advanced query-assist vocabulary is the 4 canonical S3-key fields, NOT
  // the server's metadata_key_vocabulary (which is MCAP-content-derived). We
  // deliberately ignore caps.metadata_key_vocabulary so MCAP stats never appear
  // as a filterable key.
  (void)caps.metadata_key_vocabulary;
  state_.metadata_key_vocabulary = canonicalVocabularyKeys();
}

void DexoryCloudDialog::onServerCapabilitiesReady(ServerCaps caps) {
  std::lock_guard<std::mutex> lock(state_.mu);
  // resume_supported is read at use time (the reconnect-resume path), not
  // latched into state_ — the dialog has no resume-specific UI to gate today.
  state_.tag_edit_supported = caps.tag_edit_supported;
}

void DexoryCloudDialog::onConnectFinished(bool ok, std::string status, std::string error) {
  std::string uri;
  bool plaintext_retry_needed = false;
  std::string plaintext_uri;
  bool suppress_error = false;
  {
    std::lock_guard<std::mutex> lock(state_.mu);
    state_.connected = ok;
    uri = state_.uri;

    // PJ3 plaintext-fallback: TLS connect failed, user allowed insecure
    // fallback, no custom cert in use, and we haven't tried plaintext
    // yet for this URI. Switch grpc+tls:// → grpc:// and retry once.
    if (!ok) {
      auto creds = loadCredentialsForUri(settings_, credentialStore(), uri);
      const bool no_custom_cert = creds.cert_path.empty();
      if (creds.allow_insecure && no_custom_cert && !state_.attempted_plaintext_fallback) {
        plaintext_uri = uri;
        if (const auto pos = plaintext_uri.find("grpc+tls://"); pos != std::string::npos) {
          plaintext_uri.replace(pos, std::string("grpc+tls://").size(), "grpc://");
        }
        if (plaintext_uri != uri) {
          plaintext_retry_needed = true;
          state_.attempted_plaintext_fallback = true;
        }
      }
    } else {
      state_.attempted_plaintext_fallback = false;
    }
    // Stay "connecting" only while a plaintext retry is about to fire.
    state_.connecting = plaintext_retry_needed;
    suppress_error = state_.suppress_connect_error;
  }

  if (ok) {
    // PJ3 parity: promote a successful URI to the head of the MRU history
    // (cap 20) and persist via the settings store ("dexory_cloud/server_history").
    SettingsStore settings(settings_);
    std::vector<std::string> history = settings.getStringList("dexory_cloud/server_history");
    history = promoteToHead(history, uri, /*cap=*/20);
    settings.setStringList("dexory_cloud/server_history", history);

    notify(PJ::ToolboxMessageLevel::kInfo, status);
    postCommand([w = worker_.get()] { w->listSequencesAsync(); });
    // A fresh socket invalidates the failure ledger; re-request topics for any
    // still-selected sequence that never got a successful answer, so a
    // reconnect deterministically heals the Topics panel (no reselect needed).
    std::vector<std::string> need_topics;
    {
      std::lock_guard<std::mutex> lock(state_.mu);
      state_.topics_failed.clear();
      state_.topics_failure_notified = false;
      for (const std::string& name : state_.selected_sequences) {
        if (state_.topic_infos_by_seq.find(name) == state_.topic_infos_by_seq.end()) {
          need_topics.push_back(name);
        }
      }
      if (!need_topics.empty()) {
        state_.topics_loading = true;
      }
    }
    for (const std::string& name : need_topics) {
      postCommand([w = worker_.get(), name] { w->listTopicsAsync(name); });
    }
    return;
  }

  if (plaintext_retry_needed) {
    postCommand([w = worker_.get(), plaintext_uri] { w->connectAsync(plaintext_uri, {}, {}, true); });
  } else if (!suppress_error) {
    // PJ3 AutoConnect context shows no popup; explicit connects do.
    notify(PJ::ToolboxMessageLevel::kError, fmt::format("Dexory Cloud connection failed: {}", error));
  }
}

void DexoryCloudDialog::onPullPhase(std::string phase) {
  std::lock_guard<std::mutex> lock(state_.mu);
  if (!state_.fetch_active || state_.cancelling) {
    return;
  }
  state_.fetch_status = std::move(phase);
  state_.fetch_phase_static = true;
  state_.fetch_phase_started = std::chrono::steady_clock::now();
}

void DexoryCloudDialog::onPullProgress(std::string topic_name, std::int64_t bytes) {
  std::lock_guard<std::mutex> lock(state_.mu);
  // Bytes are flowing: the byte-driven MiB/s status takes over from the
  // coarse phase line.
  state_.fetch_phase_static = false;
  // Per-topic byte ledger + rolling 5 s speed sample (PJ3 DownloadStatsDialog
  // parity). "done" is the count of topics that have fully completed
  // (pullFinished), not those merely streaming.
  state_.bytes_by_topic[topic_name] = bytes;
  const std::int64_t now_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
          .count();
  auto& samples = state_.speed_samples[topic_name];
  samples.push_back({now_ms, bytes});
  // Trim to the trailing 5 s window.
  const std::int64_t window_start = now_ms - 5000;
  while (samples.size() > 2 && samples.front().ms < window_start) {
    samples.erase(samples.begin());
  }
  std::int64_t total_bytes = 0;
  for (const auto& kv : state_.bytes_by_topic) {
    total_bytes += kv.second;
  }
  double bytes_per_sec = 0.0;
  for (const auto& [topic, s] : state_.speed_samples) {
    if (s.size() >= 2) {
      const std::int64_t dt_ms = s.back().ms - s.front().ms;
      if (dt_ms > 0) {
        bytes_per_sec += static_cast<double>(s.back().bytes - s.front().bytes) * 1000.0 / static_cast<double>(dt_ms);
      }
    }
  }
  const double mib = static_cast<double>(total_bytes) / (1024.0 * 1024.0);
  const double mibps = bytes_per_sec / (1024.0 * 1024.0);
  // High-frequency progress: shown in the Info panel during the fetch, NOT
  // pushed to the notification bell (it would flood the diagnostics log). Once
  // the user has hit Cancel, don't overwrite the "Cancelling…" header.
  if (!state_.cancelling) {
    // Two figures, deliberately distinct:
    //  - "received": compressed bytes off the wire (state_.wire_bytes_total) —
    //    the network cost, which the zstd batch/envelope compression shrinks.
    //  - "decoded": DECOMPRESSED message payloads (post chunk/batch decode), so
    //    it legitimately exceeds both the wire figure and the on-disk file sizes.
    // Showing both makes the compression ratio visible. The rate is decode
    // throughput, not network. The "received" fragment is shown once known (>0).
    std::string recv_prefix;
    if (state_.wire_bytes_total > 0) {
      recv_prefix = fmt::format("{:.2f} MiB received / ",
                                static_cast<double>(state_.wire_bytes_total) / (1024.0 * 1024.0));
    }
    state_.fetch_status =
        fmt::format("Fetching: {}/{} topics, {}{:.2f} MiB decoded ({:.2f} MiB/s)", state_.fetch_done,
                    state_.fetch_total, recv_prefix, mib, mibps);
  }
}

void DexoryCloudDialog::onPullFinished(
    std::string /*sequence_name*/, std::string topic_name, bool ok, std::string error) {
  std::lock_guard<std::mutex> lock(state_.mu);
  // PJ3 parity: tally per-topic results into the batch ledger. The panel does
  // NOT close here — that happens once in onAllFetchesComplete after the whole
  // batch lands.
  ++state_.fetch_done;
  if (ok) {
    state_.imported_any = true;
    state_.topic_fetch_status[topic_name] = "Done";
  } else if (state_.cancelling || error == "cancelled") {
    // Interrupted by the user's Cancel, not a real failure: label it
    // "Cancelled" and keep it OUT of the error tally so a cancel doesn't
    // raise spurious fetch-error notifications.
    state_.topic_fetch_status[topic_name] = "Cancelled";
  } else {
    ++state_.fetch_failed;
    state_.topic_fetch_status[topic_name] = "Failed";
    // Collapse identical messages so "[3x] no data" reads once, not thrice.
    ++state_.error_counts[std::move(error)];
  }
}

void DexoryCloudDialog::onAllFetchesComplete(std::string sequence_name) {
  FetchSummary summary;
  int total = 0;
  int failed = 0;
  bool was_cancelled = false;
  bool imported_any = false;
  {
    std::lock_guard<std::mutex> lock(state_.mu);
    state_.fetch_active = false;
    total = state_.fetch_total;
    failed = state_.fetch_failed;
    was_cancelled = state_.cancelling;
    imported_any = state_.imported_any;
    summary = buildFetchSummary(
        state_.fetch_total, state_.fetch_done, state_.fetch_failed, state_.imported_any, state_.cancelling,
        state_.error_counts);
    state_.cancelling = false;
    if (summary.should_close) {
      state_.close_pending = true;  // PJ3 parity: panel closes after a successful batch.
    }
  }

  if (!summary.error_summary.empty()) {
    notify(PJ::ToolboxMessageLevel::kError, fmt::format("Dexory Cloud fetch errors:\n{}", summary.error_summary));
  }

  // Flush buffered writer chunks into the engine and rebuild the catalog once
  // for the whole batch. appendRecord only buffers into the shared DataWriter —
  // without notifyDataChanged the imported topics never appear in the dataset
  // tree. CANCEL keeps the topics that finished first (each topic's data is
  // written DIRECTLY into the shared store as it decodes; the toolbox host
  // exposes no rollback), so surfacing real already-landed data is safer than a
  // fake discard.
  if (imported_any && runtime_host_provider_) {
    auto runtime = runtime_host_provider_();
    if (runtime.valid()) {
      runtime.notifyDataChanged();
    }
  }

  if (was_cancelled) {
    notify(
        PJ::ToolboxMessageLevel::kInfo,
        imported_any ? "Download cancelled — kept the topics that finished first" : "Download cancelled");
  } else if (summary.should_import) {
    const int imported_count = total - failed;
    notify(
        PJ::ToolboxMessageLevel::kInfo,
        fmt::format("Imported {}/{} topics from {}", imported_count, total, sequence_name));
  }
}

void DexoryCloudDialog::onPullResuming(std::string /*group*/, unsigned attempt, unsigned max) {
  // Reconnect-resume hint: reflect it in the Info-panel header AND ring the
  // notification bell (reusing the existing notify path). Don't clobber a
  // user-initiated "Cancelling…" header.
  {
    std::lock_guard<std::mutex> lock(state_.mu);
    if (!state_.cancelling) {
      state_.fetch_status = fmt::format("Resuming (attempt {}/{})…", attempt, max);
    }
  }
  notify(PJ::ToolboxMessageLevel::kInfo,
         fmt::format("Reconnecting to cloud — resuming download (attempt {}/{})…", attempt, max));
}

void DexoryCloudDialog::onPullServedFromCache(std::string group) {
  notify(PJ::ToolboxMessageLevel::kInfo, fmt::format("Served from cache: {}", group));
}

void DexoryCloudDialog::persistState() {
  std::string query;
  int lower = 0;
  int upper = DialogState::kSliderSteps;
  std::string selected_sequence;
  std::vector<std::string> selected_topics;
  bool aggregate = true;
  bool topics_all = false;
  int filter_tab = 0;
  std::map<std::string, std::string, std::less<>> basic_filter;
  {
    std::lock_guard<std::mutex> lock(state_.mu);
    query = state_.query_text;
    lower = state_.range_lower;
    upper = state_.range_upper;
    aggregate = state_.aggregate;
    topics_all = state_.topics_all;
    filter_tab = state_.filter_tab;
    basic_filter = state_.basic_filter;
    // Selection is deliberately not persisted (2026-07-12): the toolbox opens
    // with nothing selected. The empty strings below CLEAR any value a previous
    // version persisted, so an old install cannot resurrect a stale restore.
  }
  SettingsStore settings(settings_);
  settings.setString("dexory_cloud/metadata_query", query);
  settings.setInt("dexory_cloud/range_lower", lower);
  settings.setInt("dexory_cloud/range_upper", upper);
  settings.setString("dexory_cloud/selected_sequence", selected_sequence);
  settings.setStringList("dexory_cloud/selected_topics", selected_topics);
  settings.setBool("dexory_cloud/aggregate", aggregate);
  settings.setBool("dexory_cloud/topics_all", topics_all);
  settings.setInt("dexory_cloud/filter_tab", filter_tab);
  // One key per Basic-tab field; an unset filter writes "" so a previously
  // persisted constraint is cleared, not resurrected.
  for (const auto& [key, label] : kBasicFilterKeys) {
    (void)label;
    const auto it = basic_filter.find(key);
    settings.setString(
        std::string("dexory_cloud/basic_filter/") + key, it != basic_filter.end() ? it->second : std::string{});
  }
}

void DexoryCloudDialog::notify(PJ::ToolboxMessageLevel level, const std::string& message) {
  // Route one-shot status/error messages to the toolbox runtime host's
  // notification bell (reportMessage). When the runtime host provider is unset
  // (e.g. a dialog-only smoke load with no toolbox host) this is a no-op.
  if (!runtime_host_provider_) {
    return;
  }
  auto runtime = runtime_host_provider_();
  if (runtime.valid()) {
    runtime.reportMessage(level, message);
  }
}

void DexoryCloudDialog::rebuildSeqDisplayLocked() {
  // Pass 1: candidate display = the key shortened to its Hive values (k= prefixes
  // stripped, date= segment dropped).
  std::vector<std::string> candidates;
  candidates.reserve(state_.sequence_names.size());
  std::unordered_map<std::string, int> counts;
  for (const std::string& key : state_.sequence_names) {
    std::string disp = shortenSequenceName(key);
    ++counts[disp];
    candidates.push_back(std::move(disp));
  }
  // Pass 2: keep the stripped display only where it is unique; otherwise fall
  // back to the full key so the column-0 text stays a 1:1 identity (the
  // PanelEngine harvests column-0 text on selection).
  state_.seq_display_names.clear();
  state_.seq_display_names.reserve(state_.sequence_names.size());
  state_.display_to_key.clear();
  state_.display_to_key.reserve(state_.sequence_names.size());
  for (std::size_t i = 0; i < state_.sequence_names.size(); ++i) {
    const std::string& key = state_.sequence_names[i];
    std::string display = counts[candidates[i]] == 1 ? candidates[i] : key;
    state_.display_to_key.emplace(display, key);
    state_.seq_display_names.push_back(std::move(display));
  }
}

void DexoryCloudDialog::populateSequencesLocked(std::vector<SequenceInfo>& seqs, bool seed_dates) {
  state_.sequences.clear();
  state_.sequence_names.clear();
  state_.sequences.reserve(seqs.size());
  state_.sequence_names.reserve(seqs.size());
  for (auto& s : seqs) {
    SequenceRecord rec;
    rec.name = s.name;
    rec.min_ts_ns = s.min_ts_ns;
    rec.max_ts_ns = s.max_ts_ns;
    rec.total_size_bytes = s.total_size_bytes;
    // The backend's user_metadata is std::unordered_map<string, string>;
    // convert into the std::map<string, string, std::less<>> used by the
    // ported PJ3 Lua engine.
    for (const auto& kv : s.user_metadata) {
      rec.metadata.emplace(kv.first, kv.second);
    }
    // Effective tags with the override bit (tag editor reads this).
    rec.tags = s.tags;
    state_.sequence_names.push_back(rec.name);
    state_.sequences.push_back(std::move(rec));
  }
  rebuildSeqDisplayLocked();

  // Compute the global [min, max] timestamp span and (final result only) seed
  // the date-range picker with it (PJ3 shows the data's full range, e.g.
  // 29/04/2016 → 08/04/2020). "All" filter ⇒ the span passes every sequence.
  std::int64_t gmin = 0;
  std::int64_t gmax = 0;
  for (const auto& s : state_.sequences) {
    if (s.min_ts_ns > 0 && (gmin == 0 || s.min_ts_ns < gmin)) {
      gmin = s.min_ts_ns;
    }
    if (s.max_ts_ns > gmax) {
      gmax = s.max_ts_ns;
    }
  }
  state_.global_min_ts_ns = gmin;
  state_.global_max_ts_ns = gmax;
  if (seed_dates && gmin > 0 && gmax > 0) {
    state_.date_from_ns = gmin;
    state_.date_to_ns = gmax;
    state_.date_from_iso = isoFromNs(gmin);
    state_.date_to_iso = isoFromNs(gmax);
  }
  ++state_.seq_epoch;  // invalidate the seqTable view cache
}

void DexoryCloudDialog::sortSequencesLocked() {
  if (state_.seq_sort_col < 0 || state_.sequences.empty()) {
    return;
  }
  const int col = state_.seq_sort_col;
  const bool asc = state_.seq_sort_asc;
  // Build the column's comparable keys, get a stable permutation, apply it.
  std::vector<std::size_t> perm;
  if (col == 1) {  // Date — numeric on max_ts_ns (the displayed date)
    std::vector<std::int64_t> keys;
    keys.reserve(state_.sequences.size());
    for (const auto& s : state_.sequences) {
      keys.push_back(s.max_ts_ns);
    }
    perm = sortedPermutation(keys, asc);
  } else if (col == 2) {  // Size — numeric on total_size_bytes
    std::vector<std::int64_t> keys;
    keys.reserve(state_.sequences.size());
    for (const auto& s : state_.sequences) {
      keys.push_back(s.total_size_bytes);
    }
    perm = sortedPermutation(keys, asc);
  } else {  // Name (col 0)
    std::vector<std::string> keys;
    keys.reserve(state_.sequences.size());
    for (const auto& s : state_.sequences) {
      keys.push_back(s.name);
    }
    perm = sortedPermutation(keys, asc);
  }

  std::vector<SequenceRecord> reordered;
  reordered.reserve(state_.sequences.size());
  for (std::size_t p : perm) {
    reordered.push_back(std::move(state_.sequences[p]));
  }
  state_.sequences = std::move(reordered);

  state_.sequence_names.clear();
  state_.sequence_names.reserve(state_.sequences.size());
  for (const auto& s : state_.sequences) {
    state_.sequence_names.push_back(s.name);
  }
  rebuildSeqDisplayLocked();
  // Re-map ALL selected rows to the selected sequences' new positions (Slice 7).
  state_.seq_selected_rows.clear();
  for (std::size_t i = 0; i < state_.sequence_names.size(); ++i) {
    if (std::find(state_.selected_sequences.begin(), state_.selected_sequences.end(), state_.sequence_names[i]) !=
        state_.selected_sequences.end()) {
      state_.seq_selected_rows.push_back(static_cast<int>(i));
    }
  }
  std::sort(state_.seq_selected_rows.begin(), state_.seq_selected_rows.end());
  ++state_.seq_epoch;  // row order changed → invalidate the seqTable view cache
}

void DexoryCloudDialog::sortTopicsLocked() {
  if (state_.topic_sort_col < 0 || state_.topic_names.empty()) {
    return;
  }
  const int col = state_.topic_sort_col;
  const bool asc = state_.topic_sort_asc;
  const bool have_infos = state_.topic_infos.size() == state_.topic_names.size();

  // Capture the selected topics by name so selection survives the reorder.
  std::set<std::string> selected;
  for (int r : state_.topic_selected_rows) {
    if (r >= 0 && r < static_cast<int>(state_.topic_names.size())) {
      selected.insert(state_.topic_names[static_cast<std::size_t>(r)]);
    }
  }

  // Sort an index permutation, then apply it to the parallel name/info vectors.
  std::vector<std::size_t> perm;
  if (col == 1 && have_infos) {  // Messages — numeric
    std::vector<std::int64_t> keys;
    keys.reserve(state_.topic_infos.size());
    for (const auto& t : state_.topic_infos) {
      keys.push_back(t.message_count);
    }
    perm = sortedPermutation(keys, asc);
  } else {  // Name (col 0) or fallback when counts are unavailable
    perm = sortedPermutation(state_.topic_names, asc);
  }

  std::vector<std::string> new_names;
  new_names.reserve(state_.topic_names.size());
  std::vector<TopicInfo> new_infos;
  if (have_infos) {
    new_infos.reserve(state_.topic_infos.size());
  }
  for (std::size_t p : perm) {
    new_names.push_back(state_.topic_names[p]);
    if (have_infos) {
      new_infos.push_back(state_.topic_infos[p]);
    }
  }
  state_.topic_names = std::move(new_names);
  if (have_infos) {
    state_.topic_infos = std::move(new_infos);
  }

  // Re-map index-based selection from the captured names.
  state_.topic_selected_rows.clear();
  for (std::size_t i = 0; i < state_.topic_names.size(); ++i) {
    if (selected.count(state_.topic_names[i]) > 0) {
      state_.topic_selected_rows.push_back(static_cast<int>(i));
    }
  }
}

std::vector<std::string> DexoryCloudDialog::restoreSelectedTopicsLocked() {
  std::vector<std::string> need_metadata;
  if (state_.restore_selected_topics.empty()) {
    return need_metadata;
  }
  // One-shot: take the staged names and clear the slot so a later re-fetch or
  // manual selection isn't overridden.
  const std::vector<std::string> wanted = std::move(state_.restore_selected_topics);
  state_.restore_selected_topics.clear();

  state_.topic_selected_rows.clear();
  for (std::size_t i = 0; i < state_.topic_names.size(); ++i) {
    for (const std::string& name : wanted) {
      if (state_.topic_names[i] == name) {
        state_.topic_selected_rows.push_back(static_cast<int>(i));
        // Fetch full metadata (Arrow schema + tag) for the Info panel, same as
        // the manual topic-selection path, when not already cached.
        if (state_.topic_meta.find(name) == state_.topic_meta.end()) {
          need_metadata.push_back(name);
        }
        break;
      }
    }
  }
  return need_metadata;
}

bool DexoryCloudDialog::onHeaderClicked(std::string_view widget_name, int section) {
  std::lock_guard<std::mutex> lock(state_.mu);
  if (widget_name == "seqTable") {
    if (state_.seq_sort_col == section) {
      state_.seq_sort_asc = !state_.seq_sort_asc;
    } else {
      state_.seq_sort_col = section;
      state_.seq_sort_asc = true;
    }
    sortSequencesLocked();
    return true;
  }
  if (widget_name == "topicTable") {
    if (state_.topic_sort_col == section) {
      state_.topic_sort_asc = !state_.topic_sort_asc;
    } else {
      state_.topic_sort_col = section;
      state_.topic_sort_asc = true;
    }
    sortTopicsLocked();
    return true;
  }
  return false;
}

void DexoryCloudDialog::onSequenceListStarted(std::vector<SequenceInfo> seqs) {
  std::lock_guard<std::mutex> lock(state_.mu);
  // Early populate so the table shows up immediately; leave the date picker
  // untouched (the final sequencesReady seeds it from the complete span).
  populateSequencesLocked(seqs, /*seed_dates=*/false);
  sortSequencesLocked();
}

void DexoryCloudDialog::onSequenceInfoReady(SequenceInfo seq) {
  std::lock_guard<std::mutex> lock(state_.mu);
  // Fill in this one sequence's detail in place (min/max ts → Date, size →
  // Size, metadata → query schema) so the columns populate incrementally as
  // the server streams detail, instead of snapping in all at once at the final
  // sequencesReady (PJ3 parity). Keyed by name; positions are left as-is
  // (re-sorting per row would make the list jump during load — the final
  // sequencesReady re-sorts once).
  for (auto& rec : state_.sequences) {
    if (rec.name != seq.name) {
      continue;
    }
    rec.min_ts_ns = seq.min_ts_ns;
    rec.max_ts_ns = seq.max_ts_ns;
    rec.total_size_bytes = seq.total_size_bytes;
    rec.metadata.clear();
    for (const auto& kv : seq.user_metadata) {
      rec.metadata.emplace(kv.first, kv.second);
    }
    rec.tags = seq.tags;
    ++state_.seq_epoch;  // this row's Date/Size changed → invalidate the view cache so it streams in live
    break;
  }
}

void DexoryCloudDialog::onSequencesReady(std::vector<SequenceInfo> seqs) {
  std::size_t count = 0;
  std::string reselect_sequence;
  {
    std::lock_guard<std::mutex> lock(state_.mu);
    populateSequencesLocked(seqs, /*seed_dates=*/true);
    sortSequencesLocked();
    count = state_.sequences.size();

    // Graceful fallback for restored/stale Basic-tab constraints: a value that
    // no longer exists in the fresh data would render as "(any)" in the combo
    // yet still hide every row through matchesBasicFilter — prune it so the
    // filter state and the visible combo agree (default = no constraint).
    for (auto it = state_.basic_filter.begin(); it != state_.basic_filter.end();) {
      const std::vector<std::string> values = distinctFieldValues(state_.sequences, it->first);
      if (std::find(values.begin(), values.end(), it->second) == values.end()) {
        it = state_.basic_filter.erase(it);
        ++state_.seq_epoch;  // constraint changed → recompute the visible set
      } else {
        ++it;
      }
    }

    // PJ3 parity: re-select the persisted sequence if it's present in the
    // freshly-listed sequences and the user hasn't already picked one this
    // session. One-shot — clear the restore slot once consumed so a later
    // manual selection / refresh doesn't snap back.
    if (!state_.restore_selected_sequence.empty() && state_.selected_sequences.empty()) {
      for (std::size_t i = 0; i < state_.sequence_names.size(); ++i) {
        if (state_.sequence_names[i] == state_.restore_selected_sequence) {
          // Slice 7: persisted restore is single — seed the plural state with
          // exactly the one restored sequence.
          state_.selected_sequences = {state_.restore_selected_sequence};
          state_.primary_sequence = state_.restore_selected_sequence;
          state_.seq_selected_rows = {static_cast<int>(i)};
          state_.topics_loading = true;  // header shows "loading…" until topicsReady
          reselect_sequence = state_.restore_selected_sequence;
          break;
        }
      }
      state_.restore_selected_sequence.clear();
      if (reselect_sequence.empty()) {
        // The saved sequence is gone from the server — drop any staged topic
        // restore too, since topics are scoped to a sequence.
        state_.restore_selected_topics.clear();
      }
    }
  }
  // Surface the "data arrived" outcome in the app's notification bell.
  notify(PJ::ToolboxMessageLevel::kInfo, fmt::format("{} sequences", count));
  // Kick off the topic list for the re-selected sequence (mirrors the manual
  // onSelectionChanged path); restored topic names are re-applied in onTopicsReady.
  if (!reselect_sequence.empty()) {
    postCommand([w = worker_.get(), reselect_sequence] { w->listTopicsAsync(reselect_sequence); });
  }
}

std::vector<DexoryCloudDialog::TagEditorRow> DexoryCloudDialog::buildTagEditorRowsLocked() const {
  // Effective tags of the edited sequence (the cached FileSummary.tags view),
  // overlaid by the staged set/unset edits so the table previews pending
  // changes. Override-layer tags AND any staged edit tint distinctly from
  // plain embedded tags. Caller holds state_.mu.
  const SequenceRecord* rec = nullptr;
  for (const auto& s : state_.sequences) {
    if (s.name == state_.tag_edit_sequence) {
      rec = &s;
      break;
    }
  }

  std::vector<TagEditorRow> rows;
  std::unordered_map<std::string, std::size_t> index_by_key;
  if (rec != nullptr) {
    for (const auto& t : rec->tags) {
      index_by_key[t.key] = rows.size();
      rows.push_back(TagEditorRow{t.key, t.value, t.is_override ? "override" : "embedded", t.is_override});
    }
  }

  // Apply staged unsets: mark the key as a pending removal (or add a synthetic
  // row if the key is not currently present — e.g. masking an embedded tag the
  // server hasn't echoed locally).
  for (const auto& key : state_.staged_unset_keys) {
    auto it = index_by_key.find(key);
    if (it != index_by_key.end()) {
      rows[it->second].value.clear();
      rows[it->second].source = "staged unset";
      rows[it->second].is_override = true;
    } else {
      index_by_key[key] = rows.size();
      rows.push_back(TagEditorRow{key, "", "staged unset", true});
    }
  }

  // Apply staged sets last (last-wins over an unset of the same key staged
  // earlier — though onClicked already keeps the two lists mutually exclusive).
  for (const auto& [key, value] : state_.staged_set_tags) {
    auto it = index_by_key.find(key);
    if (it != index_by_key.end()) {
      rows[it->second].value = value;
      rows[it->second].source = "staged set";
      rows[it->second].is_override = true;
    } else {
      index_by_key[key] = rows.size();
      rows.push_back(TagEditorRow{key, value, "staged set", true});
    }
  }
  return rows;
}

void DexoryCloudDialog::onTagsUpdated(std::string sequence_name, bool ok, std::string error) {
  // The worker fires this exactly once per commit. On success it emits
  // sequencesReady right after, so onSequencesReady refreshes the catalog
  // metadata + bumps seq_epoch (the Lua filter re-evaluates against the new
  // tags); here we only surface the outcome in the notification bell.
  if (ok) {
    notify(PJ::ToolboxMessageLevel::kInfo, fmt::format("Tags updated for {}", sequence_name));
    return;
  }
  notify(PJ::ToolboxMessageLevel::kError,
         fmt::format("Tag update failed for {}: {}", sequence_name, error.empty() ? "unknown error" : error));
}

void DexoryCloudDialog::onTopicsReady(std::string sequence_name, std::vector<std::string> /*topic_names*/) {
  // Slice 7: topicInfosReady (below) carries the full per-topic records and is
  // what feeds the per-sequence cache + union. topicsReady is the name-only
  // companion signal; the union path drives entirely off topic_infos_by_seq, so
  // this only refreshes the persisted-topic restore once the union has settled.
  std::vector<std::string> need_metadata;
  {
    std::lock_guard<std::mutex> lock(state_.mu);
    // Membership test: ignore a stale result for a sequence no longer selected.
    if (std::find(state_.selected_sequences.begin(), state_.selected_sequences.end(), sequence_name) ==
        state_.selected_sequences.end()) {
      return;  // user moved on
    }
    if (!state_.topics_loading) {
      need_metadata = restoreSelectedTopicsLocked();
    }
  }
  for (const std::string& topic : need_metadata) {
    postCommand([w = worker_.get(), sequence_name, topic] { w->fetchTopicMetadataAsync(sequence_name, topic); });
  }
}

void DexoryCloudDialog::onTopicInfosReady(std::string sequence_name, std::vector<TopicInfo> topics) {
  std::vector<std::string> need_metadata;
  {
    std::lock_guard<std::mutex> lock(state_.mu);
    // Membership test (Slice 7): store into the per-sequence cache when the
    // sequence is still part of the selection, then recompute the topic UNION.
    if (std::find(state_.selected_sequences.begin(), state_.selected_sequences.end(), sequence_name) ==
        state_.selected_sequences.end()) {
      return;  // user moved on
    }
    state_.topic_infos_by_seq[sequence_name] = std::move(topics);
    recomputeTopicUnionLocked();
    if (!state_.topics_loading) {
      need_metadata = restoreSelectedTopicsLocked();
    }
  }
  for (const std::string& topic : need_metadata) {
    postCommand([w = worker_.get(), sequence_name, topic] { w->fetchTopicMetadataAsync(sequence_name, topic); });
  }
}

void DexoryCloudDialog::onTopicsFailed(std::string sequence_name, std::string error) {
  bool notify_user = false;
  std::string message;
  {
    std::lock_guard<std::mutex> lock(state_.mu);
    if (std::find(state_.selected_sequences.begin(), state_.selected_sequences.end(), sequence_name) ==
        state_.selected_sequences.end()) {
      return;  // user moved on
    }
    // Record the failure WITHOUT touching topic_infos_by_seq: an uncached
    // sequence is re-requested on the next selection change, so retry is free.
    state_.topics_failed[sequence_name] = error;
    notify_user = !state_.topics_failure_notified && state_.connected;
    state_.topics_failure_notified = true;
    message = fmt::format("Dexory Cloud: topics request failed: {}", error);
    recomputeTopicUnionLocked();
  }
  if (notify_user) {
    notify(PJ::ToolboxMessageLevel::kWarning, message);
  }
}

void DexoryCloudDialog::onConnectionLost() {
  {
    std::lock_guard<std::mutex> lock(state_.mu);
    if (!state_.connected) {
      return;  // already known (explicit disconnect or an earlier notice)
    }
    state_.connected = false;
  }
  notify(PJ::ToolboxMessageLevel::kWarning, "Dexory Cloud: connection to the server was lost — press Connect to reconnect");
}

void DexoryCloudDialog::onTopicMetadataReady(std::string sequence_name, std::string topic_name, TopicInfo info) {
  std::lock_guard<std::mutex> lock(state_.mu);
  // Membership test (Slice 7): accept metadata for any still-selected sequence.
  if (std::find(state_.selected_sequences.begin(), state_.selected_sequences.end(), sequence_name) ==
      state_.selected_sequences.end()) {
    return;  // user moved on
  }
  state_.topic_meta[std::move(topic_name)] = std::move(info);
}

}  // namespace dexory_cloud
