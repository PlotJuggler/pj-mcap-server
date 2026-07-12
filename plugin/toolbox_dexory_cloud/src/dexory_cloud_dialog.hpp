// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
#pragma once

#include <condition_variable>
#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>

#include <pj_base/sdk/plugin_data_api.hpp>
#include <pj_base/sdk/toolbox_plugin_base.hpp>
#include <pj_plugins/sdk/dialog_plugin_typed.hpp>
#include <pj_plugins/sdk/widget_data.hpp>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "backend_types.hpp"  // dexory_cloud::SequenceInfo / TopicInfo / TimeRange
#include "core/types.h"
#include "credential_store.hpp"  // dexory_cloud::CredentialStore (D6 token store)

namespace dexory_cloud {

class FetchWorker;
class LuaQueryEngine;

struct SequenceRecord {
  std::string name;
  std::int64_t min_ts_ns = 0;
  std::int64_t max_ts_ns = 0;
  std::int64_t total_size_bytes = 0;
  Metadata metadata;
  // Effective tags WITH the per-tag override bit (from FileSummary.tags). The
  // flat `metadata` map drives the Lua filter; this parallel vector lets the tag
  // editor tint override rows (the flat map cannot express which layer a tag
  // came from). Same effective set as `metadata`.
  std::vector<TagRow> tags;
};

// DialogState — pure data the dialog state machine drives. Mutated on the
// GUI thread (from widget events + worker-result callbacks drained by onTick),
// serialized into WidgetData on every getWidgetData().
struct DialogState {
  // mutable: saveConfig() is const (DataSource ABI) but must lock to read the
  // accepted-selection snapshot consistently with the GUI thread.
  mutable std::mutex mu;
  std::string uri = "ws://localhost:8080";
  bool connected = false;
  // True between a Connect click and the connectFinished result — drives
  // the Connect button's disabled state (PJ3 parity).
  bool connecting = false;
  // Set after a plaintext fallback has been attempted for the current
  // URI; prevents infinite TLS-fail → plaintext-fail → TLS-retry loop.
  bool attempted_plaintext_fallback = false;

  // D8: BackendCapabilities the server advertised at connect (HelloResponse.
  // backend). supports_file_hierarchy gates the additive '/'-prefix combo over
  // the seqTable (the as-built adaptation of Plan D's unrenderable QTreeWidget);
  // metadata_key_vocabulary seeds the keyCombo query-assist. For the flat Dexory
  // corpus the server reports supports_file_hierarchy=false, so the prefix combo
  // stays hidden and the dialog behaves exactly as before (default off).
  bool supports_file_hierarchy = false;
  std::vector<std::string> metadata_key_vocabulary;

  // D2: HelloResponse.capabilities.tag_edit_supported, latched from
  // serverCapabilitiesReady (see onServerCapabilitiesReady). false (the safe default,
  // matching ServerCaps{}) until a successful connect proves the server
  // supports tag editing; getWidgetData() ANDs this into buttonEditTags's
  // enabled expression so the dialog never offers a tag-edit control the
  // server is guaranteed to reject (post-M6: a read-only catalog with no
  // tag-edit IPC forwarder configured). The BackendConnection::updateTags()
  // gate is the authoritative enforcement point; this is UI-only.
  bool tag_edit_supported = false;

  // Discovery
  std::vector<SequenceRecord> sequences;
  std::vector<std::string> sequence_names;  // mirrors sequences[i].name (the real S3 key) for fast scan
  // DISPLAY-ONLY shortening for the seqTable Name column: the Hive `date=` path
  // segment is dropped (the table has a dedicated Date column). The real S3 key
  // in sequence_names stays the identity for every backend call; the PanelEngine
  // harvests column-0 (display) text on selection, so display_to_key translates
  // it back at the single re-entry point (onSelectionChanged). Collision-safe:
  // any display that two keys share falls back to the full key (see
  // rebuildSeqDisplayLocked). Parallel to sequence_names by index.
  std::vector<std::string> seq_display_names;
  std::unordered_map<std::string, std::string> display_to_key;
  // Bumped on every content/order change to `sequences` (populate + sort) so the
  // seqTable view cache below can detect staleness with a cheap counter compare.
  std::size_t seq_epoch = 0;
  // Delivery gating for the HEAVY seqTable keys (2026-07-12): rows/headers are
  // re-sent only when the view cache was rebuilt, and the visible set only when
  // it changed. At 24k catalog files the unconditional per-tick push serialized
  // megabytes of JSON per frame and saturated the GUI thread (the 1-2 Hz
  // calendar hover preview + unresponsive topic clicks).
  bool seq_rows_pushed = false;
  std::vector<int> seq_visible_pushed = {-1};  // sentinel: never delivered
  std::vector<std::string> topic_names;
  std::vector<TopicInfo> topic_infos;  // partial info from listTopics (size/ts/created)
  // Slice 7: per-sequence topic cache, keyed by sequence name. When N sequences
  // are selected the topic panel shows the UNION of their topics; this caches
  // each sequence's listTopics result so re-selecting an already-listed sequence
  // needs no round trip. topic_names / topic_infos above hold the recomputed
  // union (deterministic: union of names, per-name size = SUM across the
  // sequences that carry that topic).
  std::map<std::string, std::vector<TopicInfo>, std::less<>> topic_infos_by_seq;
  // Failure ledger for the CURRENT selection: sequence -> error. Failures are
  // deliberately NOT stored in topic_infos_by_seq (a cached failure would be
  // indistinguishable from "zero topics" and would never retry — the sticky
  // empty-Topics bug). Cleared on every selection change and on reconnect, so
  // re-selecting a failed sequence re-requests it.
  std::map<std::string, std::string, std::less<>> topics_failed;
  // One notification per selection epoch (an aggregated row can fail N times).
  bool topics_failure_notified = false;
  // True between a sequence selection and its topicsReady — drives the
  // "Topics — loading…" header hint (the only in-panel feedback now that the
  // bottom status strip is gone; the topic-list RPC is a network round trip).
  bool topics_loading = false;
  // Full per-topic metadata (incl. Arrow schema) fetched on demand for the
  // Info panel, keyed by topic name. Survives across selection changes.
  std::map<std::string, TopicInfo, std::less<>> topic_meta;
  std::string seq_filter;
  std::string topic_filter;
  // Regex-mode toggles for the name filters (PJ3 ".*" buttons). Off =
  // case-insensitive substring; on = standard regex match.
  bool seq_filter_regex = false;
  bool topic_filter_regex = false;
  // Info/metadata panel visibility — hidden by default; toggled by the
  // checkShowInfo checkbox. When shown, the Info panel renders the full
  // (un-elided) metadata for the selection.
  bool show_info = false;
  // Time-based aggregation — ON by default (checkAggregate). When on, the
  // seqTable shows one row per SESSION (a partition's time-contiguous run of
  // chunk files, grouped by aggregateSessions) instead of one row per file.
  // Selecting a session row selects ALL its constituent files -> the existing
  // multi-file stitch path (union topics, slider span, stitched download). The
  // real file keys stay the backend identity; the row->keys mapping lives in
  // the seq_view_cache below.
  bool aggregate = false;  // OFF by default (2026-07-12): one row per file
  // Column sort state — the plugin owns row ordering (built-in table widget
  // sorting would desync the index-based selection/visibility). -1 = unsorted
  // (server/load order). seqTable cols: 0=Name 1=Date 2=Size; topicTable: 0=Name 1=Size.
  // PJ3 parity: both tables default to Name ascending (column 0) because the
  // server's iteration order is unstable — without a deterministic sort, rows
  // would visibly reshuffle on every re-fetch (sequence_panel.cpp:102,
  // topic_panel.cpp:101-104).
  int seq_sort_col = 0;
  bool seq_sort_asc = true;
  int topic_sort_col = 0;
  bool topic_sort_asc = true;
  // Slice 7 (stitched multi-file selection): the seqTable is ExtendedSelection,
  // so selection is plural. seq_selected_rows is sorted ascending (highlight);
  // selected_sequences is the selected names. primary_sequence is the
  // single-selection-scoped handle (selected_sequences.front() when size>=1) for
  // the paths that are intentionally single-sequence: Edit Tags, the
  // single-sequence Info header, and persisted restore.
  std::vector<int> seq_selected_rows;
  std::vector<int> topic_selected_rows;
  std::vector<std::string> selected_sequences;
  std::string primary_sequence;

  // PJ3 parity (main_window.cpp:1051-1052,1064-1065): the last sequence + topic
  // selection is persisted and re-selected on the next connect. These hold the
  // restored values until the matching sequence/topic list arrives; the
  // restore-on-arrival is one-shot (cleared once consumed) so a later manual
  // selection or re-fetch doesn't keep snapping back to the saved one.
  std::string restore_selected_sequence;
  std::vector<std::string> restore_selected_topics;

  // Global timestamp span across all sequences, used to seed the date-range
  // edits and the "All" preset. Computed when sequences load.
  std::int64_t global_min_ts_ns = 0;
  std::int64_t global_max_ts_ns = 0;

  // Time range — RangeSlider handle positions in slider units [0, kSliderSteps],
  // applied proportionally to the selected sequence's [min_ts_ns, max_ts_ns].
  // Drives the int64 start/end passed to pullTopic. PJ3 parity: kSliderSteps.
  static constexpr int kSliderSteps = 1'000'000;
  int range_lower = 0;
  int range_upper = kSliderSteps;

  // Sequence-level date filter: ISO-8601 strings driven by the
  // date picker pair. Empty = no filter on that side.
  std::string date_from_iso;
  std::string date_to_iso;
  // Latched epoch-ns of the picked range; recomputed on every change.
  std::int64_t date_from_ns = 0;
  std::int64_t date_to_ns = 0;

  // Suppress the error *notification* for an auto-connect (PJ3 AutoConnect
  // context shows no popup); explicit Connect clicks still report failures.
  bool suppress_connect_error = false;

  // Lua query editor. The query language lives entirely in the plugin; the host
  // shows a plain QPlainTextEdit (code-edit mode). The plugin pushes the
  // persisted query back ONCE on first widget_data (query_text_pushed) to
  // restore it, then never again — pushing every tick would clobber the editor's
  // own edits (the bug this design fixes). Edits flow back via onCodeChanged.
  std::string query_text;
  bool query_text_pushed = false;
  // Caret offset (bytes) in the query editor, delivered by onCodeChangedWithCursor;
  // drives the cursor-aware Key/Op/Value assist dropdowns and completion inserts.
  int query_cursor = 0;
  // Set when the plugin programmatically rewrites query_text (a dropdown insert)
  // and the new text+caret must be pushed back to the editor on the next tick.
  // User keystrokes do NOT set this, so the editor keeps owning its own text.
  bool query_push_pending = false;
  // Metadata schema (key → distinct values) for the query assist, cached by
  // seq_epoch so it rebuilds only when the sequence set changes.
  Schema query_schema;
  std::size_t query_schema_epoch = static_cast<std::size_t>(-1);
  // Topic-selection mode, driven by the All|Custom radio pair in the Topics
  // header (radioTopicsAll / radioTopicsCustom -> DualOptionsWidget). true =
  // All: every listed topic downloads, the table is inert. false = Custom:
  // the selected rows download (+ /tf, /tf_static appended implicitly at fetch
  // time — kForcedTopics); zero-count rows are disabled.
  bool topics_all = false;
  // Metadata-filter mode, driven by the Basic|Advanced radio pair (rendered as
  // a DualOptionsWidget by the host; radioFilterBasic / radioFilterAdvanced).
  // 0 = Basic: dropdown equality filters on the S3-key fields below; the Lua
  // query is ignored. 1 = Advanced: the Lua query applies; the Basic dropdowns
  // are ignored. Switching modes switches which filter is active (state for the
  // other mode is preserved, not cleared).
  int filter_tab = 0;
  // Basic-tab selections: S3-key field name -> chosen value. Absent/empty = "any"
  // (no constraint). Keys: customer, customer_site, robot, source.
  std::map<std::string, std::string, std::less<>> basic_filter;

  // Memoized seqTable view (rows + visible-row set). widget_data() runs on the
  // GUI thread every host tick (~20Hz); recomputing the per-sequence display
  // strings, the metadata-map copies, and the name/date/Lua filter on every
  // tick burned the GUI thread and made the sequence list feel laggy. The
  // result is a pure function of the inputs captured here, so it is recomputed
  // only when one of them changes (PJ3 was event-driven, not timer-driven).
  struct SeqViewCache {
    bool valid = false;
    bool aggregate = false;  // which view mode the cache was built for
    int filter_tab = 0;      // which filter tab the visible set was computed for
    std::map<std::string, std::string, std::less<>> basic_filter;  // Basic-tab selections it was computed for
    std::size_t seq_epoch = 0;
    std::string seq_filter;
    bool seq_filter_regex = false;
    std::string query_text;
    std::int64_t date_from_ns = 0;
    std::int64_t date_to_ns = 0;
    std::vector<std::vector<std::string>> rows;
    std::vector<int> visible;
    // Per displayed row -> the real S3 file keys it represents (1 key in file
    // mode, N in aggregate mode). row_to_keys maps the column-0 text (the
    // PanelEngine selection identity) to the same key list, for onSelectionChanged.
    std::vector<std::vector<std::string>> row_keys;
    std::unordered_map<std::string, std::vector<std::string>> row_to_keys;
    // The PREVIOUS rebuild's row_to_keys, kept one generation as a lookup
    // fallback: a click is harvested against the on-screen label, which can be
    // one refresh older than the cache (aggregate labels embed min_ts + file
    // count, both of which move while the indexer is still filling a
    // partition). Without the fallback such a click resolves to ZERO keys and
    // the Topics panel goes silently empty.
    std::unordered_map<std::string, std::vector<std::string>> prev_row_to_keys;
  };
  SeqViewCache seq_view_cache;

  // Fetch progress — per-topic byte counters, refreshed from
  // pullProgress signals on the GUI thread.
  std::map<std::string, std::int64_t, std::less<>> bytes_by_topic;
  std::string fetch_status;
  // True while fetch_status is a coarse PHASE line from pullPhase ("Opening
  // session…") rather than the byte-driven progress line; widget_data appends
  // a live elapsed counter so the user sees movement before any byte flows.
  bool fetch_phase_static = false;
  std::chrono::steady_clock::time_point fetch_phase_started{};

  // Fetch lifecycle (PJ3 parity: a batch of topics completes via
  // allFetchesComplete, not per-topic). The panel only closes after the
  // whole batch lands, and only when not cancelling.
  bool fetch_active = false;
  bool cancelling = false;
  int fetch_total = 0;   // topics requested this batch
  int fetch_done = 0;    // topics that reported pullFinished (ok or fail)
  int fetch_failed = 0;  // subset of fetch_done that failed
  bool imported_any = false;
  // Per-message error tally so identical failures collapse into "[Nx] msg"
  // (PJ3 showCopyableWarning dedup).
  std::map<std::string, int, std::less<>> error_counts;
  // Per-topic rolling speed samples: (epoch_ms, cumulative_bytes), trimmed to
  // a 5 s window — mirrors PJ3 DownloadStatsDialog speed calc.
  struct SpeedSample {
    std::int64_t ms;
    std::int64_t bytes;
  };
  std::map<std::string, std::vector<SpeedSample>, std::less<>> speed_samples;
  std::map<std::string, std::string, std::less<>> topic_fetch_status;  // name → "" / "Done" / "Failed"

  // Sub-dialog request flags (read+cleared in getWidgetData).
  bool open_cert_pending = false;

  // Which modal sub-dialog is currently open. Both the cert and tag editors
  // emit the synthetic `subDialogAccepted` click on OK, so the handler must
  // disambiguate. Set when a sub-dialog is requested, cleared on accept/cancel.
  enum class ActiveSubDialog { kNone, kCert, kTag };
  ActiveSubDialog active_sub_dialog = ActiveSubDialog::kNone;

  // ---- Tag editor (Slice 6, Plan D Task 9) --------------------------------
  // Open request (read+cleared in getWidgetData), then the staged edits the
  // sub-dialog accumulates. Mirrors the cert staging idiom exactly: onTextChanged
  // stages the key/value inputs; tagSetButton/tagUnsetButton append to the staged
  // lists; subDialogAccepted commits them via updateTagsAsync.
  bool open_tag_pending = false;
  // The sequence the editor targets (snapshot of selected_sequence at open).
  std::string tag_edit_sequence;
  // Live values of the tagKey / tagValue inputs (staged via onTextChanged).
  std::string pending_tag_key;
  std::string pending_tag_value;
  // Staged set (upsert) and unset edits, committed on OK. set_tags is keyed
  // last-wins; unset_keys removes an override / masks an embedded tag.
  std::vector<std::pair<std::string, std::string>> staged_set_tags;
  std::vector<std::string> staged_unset_keys;
  // True between an Edit-Tags open and the commit's listSequences refresh —
  // keeps the table reflecting staged edits across ticks while open.
  bool tag_dialog_open = false;

  // Staged credential edits from the cert sub-dialog. PanelEngine fires
  // onTextChanged for each text/checkable child after the user
  // clicks OK; we capture the values here until the synthetic
  // `subDialogAccepted` click commits them to persisted settings under the
  // current URI.
  std::string pending_cert_path;
  std::string pending_api_key;
  bool pending_allow_insecure = false;
  bool has_pending_cert_edit = false;
  bool has_pending_api_key_edit = false;
  bool has_pending_allow_insecure_edit = false;

  // Close request (read+cleared in getWidgetData).
  bool close_pending = false;
};

class DexoryCloudDialog : public PJ::DialogPluginTyped {
 public:
  DexoryCloudDialog();
  ~DexoryCloudDialog() override;

  // DialogPluginTyped overrides
  std::string manifest() const override;
  std::string ui_content() const override;
  std::string widget_data() override;
  bool onTextChanged(std::string_view widget_name, std::string_view text) override;
  bool onClicked(std::string_view widget_name) override;
  bool onToggled(std::string_view widget_name, bool checked) override;
  bool onSelectionChanged(std::string_view widget_name, const std::vector<std::string>& selected) override;
  bool onValueChanged(std::string_view widget_name, int value) override;
  bool onRangeChanged(std::string_view widget_name, int lower, int upper) override;
  bool onDateRangeChanged(std::string_view widget_name, std::string_view from_iso, std::string_view to_iso) override;
  bool onCodeChangedWithCursor(std::string_view widget_name, std::string_view code, int cursor) override;
  bool onIndexChanged(std::string_view widget_name, int index) override;
  bool onTabChanged(std::string_view widget_name, int index) override;
  bool onHeaderClicked(std::string_view widget_name, int section) override;
  bool onTick() override;

  // ---- Toolbox config / host seam (Slice 5 restore) ----------------------
  // The dialog browses the catalog AND downloads in-place: on Fetch it drives
  // worker_->pullTopicsAsync, which opens a fresh session and delegates message
  // parsing to the host's MessageParser plugins via ParserIngestDriver. The
  // toolbox plugin hands the dialog the write/runtime host via
  // setHostProvider/setRuntimeHostProvider during bind().
  //
  // saveConfig()/loadConfig() persist the browse-phase UI prefs (query/range/
  // server) so the panel re-opens with the same state — harmless, no session
  // selection is carried (the toolbox imports in-dialog, not on restart).
  [[nodiscard]] std::string saveConfig() const;
  [[nodiscard]] bool loadConfig(std::string_view config_json);

  // Provider seams wired by the toolbox plugin (toolboxHost / runtimeHost).
  void setHostProvider(std::function<PJ::sdk::ToolboxHostView()> provider);
  void setRuntimeHostProvider(std::function<PJ::ToolboxRuntimeHostView()> provider);

  // Binds the host's `pj.settings.v1` store and restores persisted UI state
  // (+ auto-connect). Called by the toolbox during bind(). An unbound view
  // (host omits the optional service) yields defaults gracefully.
  void setSettings(PJ::sdk::SettingsView settings);

 private:
  // Restore persisted query/range/server + auto-connect. Runs once when the
  // settings view is bound, before the tick loop.
  void initFromSettings();

  void onConnectFinished(bool ok, std::string status, std::string error);
  // D8: latch the server's BackendCapabilities (hierarchy flag + query-assist
  // vocabulary) into state_. Runs on the GUI thread (event-drained).
  void onCapabilitiesReady(BackendCaps caps);
  // D2: latch the server's Capabilities (resume_supported/tag_edit_supported)
  // into state_ so getWidgetData()'s buttonEditTags gate can see it. Runs on
  // the GUI thread (event-drained), same as onCapabilitiesReady above.
  void onServerCapabilitiesReady(ServerCaps caps);
  void onSequencesReady(std::vector<SequenceInfo> sequences);
  // Progressive discovery (PJ3 parity): populate the table from the initial
  // list as soon as it arrives, then fill each row's Date/Size as the server
  // streams per-sequence detail (onSequenceInfoReady), before the final list.
  void onSequenceListStarted(std::vector<SequenceInfo> sequences);
  void onSequenceInfoReady(SequenceInfo sequence);
  void onTopicsReady(std::string sequence_name, std::vector<std::string> topic_names);
  void onTopicInfosReady(std::string sequence_name, std::vector<TopicInfo> topics);
  // Failure twin of onTopicInfosReady: records the error WITHOUT caching (so
  // the next selection change retries) and surfaces one notification per
  // selection epoch instead of one per sequence.
  void onTopicsFailed(std::string sequence_name, std::string error);
  // The worker discovered the browse socket is dead (server reap / network
  // drop). Flips connected state so the UI stops pretending the link is up.
  void onConnectionLost();
  void onTopicMetadataReady(std::string sequence_name, std::string topic_name, TopicInfo info);

  // In-dialog download ledger (Mosaico parity). pullProgress updates per-topic
  // bytes + rolling speed; pullFinished tallies done/failed with dedup'd error
  // counts; allFetchesComplete closes the panel on success and calls
  // runtimeHost().notifyDataChanged() so the catalog tree rebuilds.
  void onPullProgress(std::string topic_name, std::int64_t bytes);
  // Coarse pull-phase line from the worker ("Opening session…"); rendered with
  // a live elapsed suffix until the first byte-driven progress sample.
  void onPullPhase(std::string phase);
  // fetch_status, with a live elapsed suffix while a coarse phase is showing.
  // Caller holds state_.mu.
  [[nodiscard]] std::string fetchStatusLineLocked() const;
  void onPullFinished(std::string sequence_name, std::string topic_name, bool ok, std::string error);
  void onAllFetchesComplete(std::string sequence_name);

  // Reconnect-resume UX (Slice 8). Fires per reconnect attempt during a mid-pull
  // transport drop: sets the Info-panel "Resuming (attempt N/max)…" header and
  // rings the notification bell, reusing the existing worker->dialog event path.
  void onPullResuming(std::string group, unsigned attempt, unsigned max);
  // Cache HIT (Slice 8): surface a one-shot "served from cache" notify.
  void onPullServedFromCache(std::string group);

  // Tag-edit commit result (Slice 6). On failure surfaces the verbatim error via
  // notify(); on success the worker emits sequencesReady right after, so
  // onSequencesReady refreshes the catalog metadata + invalidates the seq view
  // cache (the Lua filter re-evaluates against the new tags).
  void onTagsUpdated(std::string sequence_name, bool ok, std::string error);

  // One display row of the tag-editor table: effective tags of the edited
  // sequence overlaid by the staged set/unset edits. is_override is true for
  // override-layer tags AND any staged edit (so they tint distinctly from
  // embedded tags). Caller (getWidgetData) builds the QTableWidget from these.
  struct TagEditorRow {
    std::string key;
    std::string value;
    std::string source;  // "embedded" / "override" / "staged set" / "staged unset"
    bool is_override = false;
  };
  // Build the tag-editor table rows for state_.tag_edit_sequence, applying the
  // staged edits over the current effective tags. Caller MUST hold state_.mu.
  [[nodiscard]] std::vector<TagEditorRow> buildTagEditorRowsLocked() const;

  void workerLoop();
  void postCommand(std::function<void()> fn);
  void postEvent(std::function<void()> fn);

  // Surface a one-shot status/error message via the toolbox runtime host's
  // notification bell (reportMessage). Falls back to a no-op when the runtime
  // host provider is unset (e.g. dialog-only smoke load). High-frequency
  // progress is NOT routed here — it shows in the Info panel during a fetch.
  void notify(PJ::ToolboxMessageLevel level, const std::string& message);

  // Rebuild state_.sequences / sequence_names from a fresh listSequences
  // result. Caller MUST hold state_.mu. When seed_dates is true the date-range
  // picker is reseeded to the dataset's full [min,max] span (final result
  // only — the progressive early populate leaves the picker untouched).
  void populateSequencesLocked(std::vector<SequenceInfo>& sequences, bool seed_dates);
  // Recompute seq_display_names + display_to_key from sequence_names. Call after
  // any rebuild of sequence_names (populate + sort). Collision-safe: a display
  // shared by two distinct keys falls back to the full key for those rows.
  void rebuildSeqDisplayLocked();

  // Re-order the sequence / topic row models per the current sort column+order
  // and re-map index-based selection by name. Caller MUST hold state_.mu;
  // both are no-ops when the table's sort column is -1 (load order).
  void sortSequencesLocked();
  // Drop the whole sequence selection + the dependent topic view (topics list,
  // topic selection, slider span source). Caller holds state_.mu.
  void clearSelectionStateLocked();
  void sortTopicsLocked();

  // Rebuild the cached query-assist metadata schema when the sequence set
  // changed (keyed by seq_epoch). Caller must hold state_.mu.
  void ensureQuerySchemaLocked();

  // Slice 7: recompute topic_names / topic_infos as the UNION of every selected
  // sequence's cached topics (topic_infos_by_seq). Per-name size/message_count
  // are summed across the sequences that carry the topic; names are
  // deduplicated + sorted. topics_loading is cleared only once every selected
  // sequence has a cache entry. Caller MUST hold state_.mu.
  void recomputeTopicUnionLocked();

  // Re-apply the persisted topic selection (restore_selected_topics) onto the
  // freshly-listed topic rows of the restored sequence. One-shot: clears the
  // restore slot once consumed. Returns the restored topic names that still need
  // an on-demand metadata fetch (Info panel) — the caller posts those after
  // releasing the lock. Caller MUST hold state_.mu.
  std::vector<std::string> restoreSelectedTopicsLocked();

  // Persist the Lua query + slider proportions to settings (PJ3 parity:
  // restored next time the panel opens). Caller must NOT hold state_.mu.
  void persistState();

  DialogState state_;
  std::thread worker_thread_;
  std::unique_ptr<FetchWorker> worker_;
  std::mutex cmd_mu_;
  std::condition_variable cmd_cv_;
  std::deque<std::function<void()>> cmd_queue_;
  bool worker_stop_ = false;
  std::mutex evt_mu_;
  std::deque<std::function<void()>> evt_queue_;
  // Host-backed QSettings-like store (pj.settings.v1). Default-constructed
  // unbound until setSettings(); an unbound view reads defaults / drops writes.
  PJ::sdk::SettingsView settings_;
  // D6: per-server bearer-token store (the SECRET). The default backend is a
  // 0600 file under $XDG_CONFIG_HOME/dexory_cloud; a libsecret backend is a
  // later drop-in behind the CredentialStore interface. cert_path /
  // allow_insecure (non-secret) stay in settings_ above. Lazily constructed on
  // first credential access so a unit-load without setSettings() still works.
  std::unique_ptr<CredentialStore> credentials_;
  CredentialStore& credentialStore();
  // Toolbox runtime host provider (notifyDataChanged after import + the
  // reportMessage notification bell). Set by the toolbox during bind(); unset
  // for a dialog-only smoke load (notify() then no-ops).
  std::function<PJ::ToolboxRuntimeHostView()> runtime_host_provider_;
};

}  // namespace dexory_cloud
