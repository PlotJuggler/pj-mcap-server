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
#include <optional>
#include <unordered_map>

#include <pj_base/sdk/plugin_data_api.hpp>
#include <pj_base/sdk/toolbox_plugin_base.hpp>
#include <pj_plugins/sdk/dialog_plugin_typed.hpp>
#include <pj_plugins/sdk/widget_data.hpp>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "backend_types.hpp"  // mcap_cloud::SequenceInfo / TopicInfo / TimeRange
#include "core/types.h"
#include "credential_store.hpp"  // mcap_cloud::CredentialStore (D6 token store)
#include "trusted_origins.hpp"   // mcap_cloud::TrustedOrigins (spec §7 guard 1 ledger)
#include "fetch_worker.hpp"      // mcap_cloud::FetchWorker::GateListResult (onGateListFinished's param type)
#include "vocab_select.hpp"      // mcap_cloud::GatePhase / resolveGateFilter / autoSelectCustomer / siteNamesFor

namespace mcap_cloud {

class ImportRuntime;
class LuaQueryEngine;
struct McapSaveResult;

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
  // mutable: saveConfig() is const (the DialogPluginTyped saveConfig()/
  // loadConfig() contract) but must lock to read the accepted-selection
  // snapshot consistently with the GUI thread.
  mutable std::mutex mu;
  std::string uri = "ws://localhost:8080";
  bool connected = false;
  // True between a Connect click and the connectFinished result — drives
  // the Connect button's disabled state (PJ3 parity).
  bool connecting = false;

  // D2: HelloResponse.capabilities.tag_edit_supported, latched from
  // serverCapabilitiesReady (see onServerCapabilitiesReady). false (the safe default,
  // matching ServerCaps{}) until a successful connect proves the server
  // supports tag editing; getWidgetData() ANDs this into buttonEditTags's
  // enabled expression so the dialog never offers a tag-edit control the
  // server is guaranteed to reject (post-M6: a read-only catalog with no
  // tag-edit IPC forwarder configured). The BackendConnection::updateTags()
  // gate is the authoritative enforcement point; this is UI-only.
  bool tag_edit_supported = false;

  // Browse gate: the catalog is NEVER fetched unfiltered. One monotonic id per
  // gate transition; every worker callback echoes it and stale echoes are
  // dropped. Phase drives the pill.
  std::optional<VocabularyInfo> vocabulary;
  std::string gate_customer;  // selected NAMES (durable identity, persisted)
  std::string gate_site;
  // Third gate level (2026-08-09). A file listing is the most expensive thing
  // the browse path transfers, so nothing is listed until all three are chosen
  // and the narrowing happens SERVER-side (ListFilter.robot_id).
  std::string gate_robot;
  std::uint64_t gate_request_seq = 0;   // last issued id
  GatePhase gate_phase = GatePhase::kDisconnected;
  std::string active_server_key;        // canonical key of the CONNECTED server

  // Cached gate-combo item lists (see widget_data()'s gate-combo block): rebuilt
  // ONLY in onVocabularyReady and the filter_customer/filter_customer_site
  // onIndexChanged handlers -- never on a per-tick basis. widget_data() runs
  // every host tick (~20-60Hz); at 50 customers x 20 sites, rebuilding these by
  // rescanning the vocabulary (+ siteNamesFor's own scan) every tick was ~1000
  // string copies + JSON encoding on 59 of every 60 ticks for nothing.
  std::vector<std::string> gate_customer_items;
  std::vector<std::string> gate_site_items;
  std::vector<std::string> gate_robot_items;

  // Cached pill-hint text (see widget_data()): gateHintText's kNeedsSelection
  // branch does two std::to_string() calls + concatenation, and kNeedsSelection
  // is the mandatory gate state the user often SITS in — recompute only when
  // (phase, total_files, total_sites) actually changes, not on every tick.
  struct GateHintCache {
    bool valid = false;
    GatePhase phase = GatePhase::kDisconnected;
    std::uint64_t total_files = 0;
    std::size_t total_sites = 0;
    std::string text;
  };
  GateHintCache gate_hint_cache;

  // Discovery
  std::vector<SequenceRecord> sequences;
  std::vector<std::string> sequence_names;  // mirrors sequences[i].name (the real S3 key) for fast scan
  // DISPLAY-ONLY shortening for the seqTable Name column: the Hive `date=` path
  // segment is dropped (the table has a dedicated Date column). The real S3 key
  // in sequence_names stays the identity for every backend call; the PanelEngine
  // harvests column-0 (display) text on selection, which onSelectionChanged
  // resolves back to real S3 key(s) via seq_view_cache.row_to_keys (see below).
  // Collision-safe: any display that two keys share falls back to the full key
  // (see rebuildSeqDisplayLocked). Parallel to sequence_names by index.
  std::vector<std::string> seq_display_names;
  // How many LEADING rows of `sequences` are guaranteed unchanged since the
  // last seq_epoch bump. A progressive listing sweep only ever APPENDS, so the
  // view cache can extend itself instead of rebuilding all N rows per render
  // window (which made a sweep O(N x windows): 43k rows x ~15 windows). 0 means
  // "assume nothing" — every non-append mutation (full populate, sort, erase)
  // resets it, so the cache falls back to a full rebuild.
  std::size_t seq_stable_prefix = 0;
  // Occurrence count per candidate display name, maintained alongside
  // seq_display_names so an append can detect a NEW collision in O(new) instead
  // of recounting all names. A collision forces one full rebuildSeqDisplayLocked
  // (rare: candidates keep the unique leaf filename).
  std::unordered_map<std::string, int> seq_display_counts;
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
  // Same gating for the topicTable rows/headers: false = re-send on the next
  // widget_data() (set at every topic_names/topic_infos mutation — union
  // recompute, sort, selection clears). The light per-call keys (visible/
  // disabled/selected/labels) are NOT gated — they depend on filter text and
  // selection, which change independently and serialize to a few ints.
  bool topic_rows_pushed = false;
  // MRU server history ("mcap_cloud/server_history"): seeded by
  // initFromSettings (first getDialog) and refreshed on every history write
  // (onConnectFinished) — the previous per-call read crossed the plugin->host
  // settings ABI on every widget_data().
  std::vector<std::string> server_history;
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
  // the paths that are intentionally single-sequence: Edit Tags and the
  // single-sequence Info header.
  std::vector<int> seq_selected_rows;
  std::vector<int> topic_selected_rows;
  std::vector<std::string> selected_sequences;
  std::string primary_sequence;

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

  // Sequence-level date filter: epoch-ns of the picked range (from the date
  // picker pair), recomputed on every change. 0/0 = unbounded ("All").
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
  // Cumulative WS payload bytes RECEIVED off the wire for the active pull (the
  // compressed network figure, vs bytes_by_topic's decoded total). Refreshed from
  // pullWireBytes on the GUI thread; shown as "X MiB received / Y MiB decoded".
  std::int64_t wire_bytes_total = 0;
  // Server pre-flight download-size estimate (upper bound; 0 = unknown). Drives
  // the byte-based progress percentage on the status line. Set from pullEstimate.
  std::uint64_t estimated_total_bytes = 0;
  std::string fetch_status;
  // Full breakdown (topics + received/decoded + compression ratio) shown as the
  // status line's hover tooltip, so the visible line can stay compact. Empty
  // outside an active byte-flowing fetch.
  std::string fetch_tooltip;
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
  // Optional raw-session EXPORT (persisted as mcap_cloud/export_*). Default
  // OFF: an involuntary full-session file per fetch is not a sane default,
  // and the future replay cache (docs/canonical-layout-import.md) is a
  // separate, capped store — this toggle governs only the user-owned export
  // copy. The path is a directory because the plugin stays Qt-free and
  // generates collision-safe filenames itself.
  bool save_mcap = false;
  std::string save_directory;
  // Latched before allFetchesComplete so a non-Complete export result (Failed
  // or Partial — Skipped excluded) suppresses the otherwise automatic close
  // after a successful host import, keeping the notification visible.
  bool mcap_save_failed = false;
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

class McapCloudDialog : public PJ::DialogPluginTyped {
  // The progressive-sweep path (appendSequencesLocked + the view cache's
  // incremental extend) is private but is exactly where a wrong change ships
  // silently: the end-of-sweep repopulate hides mid-sweep corruption from every
  // other test. This friend lets sweep_incremental_test.cpp assert the invariant
  // that append-in-chunks == one full populate.
  friend struct McapCloudDialogSweepAccess;

 public:
  McapCloudDialog();
  ~McapCloudDialog() override;

  // DialogPluginTyped overrides
  std::string manifest() const override;
  std::string ui_content() const override;
  std::string widget_data() override;
  bool onTextChanged(std::string_view widget_name, std::string_view text) override;
  bool onClicked(std::string_view widget_name) override;
  bool onFolderSelected(std::string_view widget_name, std::string_view path) override;
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
  // The per-toolbox-instance ImportRuntime (stage-4 PR-1). Non-owning; wired
  // by the toolbox constructor BEFORE bind()/setSettings. Forwarded to the
  // worker (cache tee + shared host-write mutex + shared SessionCache); the
  // dialog itself routes successful-Hello trust recording through it so the
  // in-memory trust set and the ledger stay in lockstep.
  void setImportRuntime(ImportRuntime* runtime);

  // Binds the host's `pj.settings.v1` store. STORE-ONLY (stage-4 PR-2, spec
  // docs/canonical-layout-import.md §6.3): called by the toolbox during
  // bind(), which must stay network-free for a headless provider bind — the
  // persisted-UI restore + auto-connect run at the first getDialog() via
  // ensureInitFromSettings() below. A re-bind after initialization swaps the
  // stored view (subsequent reads/writes go to the new host store) but never
  // implicitly reconnects. An unbound view (host omits the optional service)
  // yields defaults gracefully.
  void setSettings(PJ::sdk::SettingsView settings);

  // One-shot interactive initialization: restore persisted UI state +
  // auto-connect to the most recent server. Called by the toolbox's
  // getDialog() — the interactive-only entry point (PJ4 shows the panel only
  // through it), so a headless provider session never triggers it. Latched
  // ONCE PER PLUGIN LIFETIME: repeated getDialog() calls and re-binds are
  // plain borrows/view swaps, never a re-init or reconnect. Residual noted
  // for §6.3: the dialog CONSTRUCTOR starts the command-pump worker thread,
  // which idles until a command is queued — no network, credential, or
  // settings access happens pre-init, so the early thread is not a §6.3
  // violation.
  void ensureInitFromSettings();

 private:
  // Restore persisted query/range/server + auto-connect. Runs once, at the
  // first getDialog() (see ensureInitFromSettings), before any worker
  // command exists.
  void initFromSettings();

  // `uri` is the URI ACTUALLY CONNECTED (captured at the connect() call site,
  // not re-read from state_.uri — the editable field may have changed since
  // the connect was issued; see beginGateRequestLocked's caller).
  void onConnectFinished(bool ok, std::string uri, std::string status, std::string error);
  // D8: the server's BackendCapabilities arrive here (GUI thread, event-
  // drained). Neither field (supports_file_hierarchy, metadata_key_vocabulary)
  // is latched into state_ today — see the .cpp definition for why.
  void onCapabilitiesReady(BackendCaps caps);
  // D2: latch the server's Capabilities (resume_supported/tag_edit_supported)
  // into state_ so getWidgetData()'s buttonEditTags gate can see it. Runs on
  // the GUI thread (event-drained), same as onCapabilitiesReady above.
  void onServerCapabilitiesReady(ServerCaps caps);
  void onSequencesReady(std::vector<SequenceInfo> sequences);

  // ---- Browse gate (customer/site) — GatePhase state machine -------------
  // EVERY gate transition funnels through this: it drops all site-scoped
  // state (rows, selection, topics, date filter) so nothing from the
  // previous site can leak into (or mask) the next one, then issues a fresh
  // monotonic request id and supersedes any in-flight worker sweep. Caller
  // MUST hold state_.mu.
  [[nodiscard]] std::uint64_t beginGateRequestLocked(GatePhase next_phase);
  // True when `request_id` is not the CURRENT gate transition (a newer
  // beginGateRequestLocked()/supersedeGateRequests() call already moved on), so
  // any worker answer carrying this id is stale and must be dropped. Single
  // source of truth for the drop check + comment repeated across the 4 gate-
  // result handlers below. Caller MUST hold state_.mu.
  [[nodiscard]] bool gateRequestStaleLocked(std::uint64_t request_id) const;
  // Rebuild gate_customer_items / gate_site_items from the current
  // state_.vocabulary + state_.gate_customer. Called only where those inputs
  // actually change (onVocabularyReady; the filter_customer/filter_customer_site
  // onIndexChanged handlers) — widget_data() reads the cached result instead of
  // rebuilding it every tick. Caller MUST hold state_.mu.
  void refreshGateComboItemsLocked();
  // Vocabulary arrived (the gate's data source). Drops a stale id. When
  // `recovery` is true this came from INSIDE a filtered-list stale-recovery
  // retry (the worker owns that retry) — only combos refresh, never a new
  // sweep. Otherwise resolves/auto-selects customer+site and either starts a
  // gated sweep or lands on kNeedsSelection/kEmptyCatalog.
  void onVocabularyReady(std::uint64_t request_id, VocabularyInfo vocab, bool recovery);
  // GetVocabulary failed on a live connection: phase -> kVocabularyError.
  void onVocabularyFailed(std::uint64_t request_id);
  // One ListFiles page, mid-sweep: append into progressive_seqs_ (or clear it
  // first when reset — first page, or a stale-catalog restart) and repopulate
  // the table so the browse renders in ~150 ms instead of after the full ~8 s
  // sweep, gated on the request id instead of always accepted. The final
  // onGateListFinished stays authoritative (dates, reselect).
  void onGatePageReady(std::uint64_t request_id, std::vector<SequenceInfo> page, bool reset);
  // Terminal result of one gated list sweep. May arrive WITHOUT a preceding
  // dialog-initiated transition (the tag-edit re-list reuses the stored gate
  // id) — gating is by request_id alone, never by an assumed prior phase.
  void onGateListFinished(FetchWorker::GateListResult result);
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
  void onMcapSaveFinished(McapSaveResult result);
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
  // Append-only fast path for a progressive sweep: adds seqs[from..) to the
  // existing state instead of rebuilding it. Returns false when it cannot be
  // used (a display-name collision needs a global re-derive), in which case the
  // caller must fall back to populateSequencesLocked.
  [[nodiscard]] bool appendSequencesLocked(const std::vector<SequenceInfo>& seqs, std::size_t from);
  // Recompute seq_display_names from sequence_names. Call after any rebuild of
  // sequence_names (populate + sort). Collision-safe: a display shared by two
  // distinct keys falls back to the full key for those rows.
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

  // Persist the Lua query + slider proportions to settings (PJ3 parity:
  // restored next time the panel opens). Caller must NOT hold state_.mu.
  void persistState();

  DialogState state_;

  // Pages accumulated across one in-flight catalog sweep (onGatePageReady).
  // GUI-thread only: written exclusively inside postEvent lambdas drained by
  // onTick, so no lock; cleared on reset and again when the authoritative
  // onSequencesReady lands (which re-populates from its own complete vector).
  std::vector<SequenceInfo> progressive_seqs_;
  // Throttle stopgap for onGatePageReady (2026-07-26): populateSequencesLocked
  // + sortSequencesLocked() together are O(n^2 log n) over one sweep (a full
  // rebuild of `sequences` from `progressive_seqs_`, deep-copying every
  // SequenceRecord, then a full re-sort) — at 500 rows/page and a 14,480-row
  // site (29 pages) that is ~217,500 deep row copies instead of ~14,500 (~15x)
  // and ~2.6M sort ops instead of ~200k (~13x), all on the GUI thread, and it
  // gets WORSE as the sweep progresses (more accumulated rows each page). Pages
  // still ALWAYS accumulate into progressive_seqs_; only the populate+sort is
  // throttled to at most once per 150 ms (see onGatePageReady) — the rows
  // aren't lost, they render on the next qualifying page, and the final
  // authoritative render always happens via onGateListFinished/onSequencesReady
  // regardless of whether the last progressive page rendered. GUI-thread only,
  // same as progressive_seqs_ above (no lock needed).
  std::chrono::steady_clock::time_point last_progressive_render_{};
  // Duration of the last seqTable view-cache rebuild (widget_data). The
  // progressive-render throttle scales its window off this: at 43k rows a
  // rebuild costs ~150 ms, so the old FIXED 150 ms window let a large sweep
  // trigger rebuilds back-to-back and saturate the GUI thread for its whole
  // duration — the panel read as frozen. GUI-thread only, no lock (same as
  // last_progressive_render_).
  double last_rebuild_ms_ = 0.0;

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
  // 0600 file under $XDG_CONFIG_HOME/mcap_cloud; a libsecret backend is a
  // later drop-in behind the CredentialStore interface. cert_path /
  // allow_insecure (non-secret) stay in settings_ above. Lazily constructed on
  // first credential access so a unit-load without setSettings() still works.
  std::unique_ptr<CredentialStore> credentials_;
  CredentialStore& credentialStore();
  // Trusted-origin ledger (spec §7 guard 1): origins that completed a
  // successful interactive Hello — the future auto-replay trust source.
  // Deliberately separate from credentials_ (a stored token must never imply
  // replay trust). Lazily constructed like credentials_ above. When an
  // ImportRuntime is bound, trust recording goes THROUGH it instead (write-
  // through keeps the same ledger file while updating the bounded in-memory
  // set) — this member is the runtime-less (unit-load) fallback only.
  std::unique_ptr<TrustedOrigins> trusted_origins_;
  TrustedOrigins& trustedOrigins();
  // Non-owning; see setImportRuntime. nullptr for a dialog-only unit load.
  ImportRuntime* import_runtime_ = nullptr;
  // ensureInitFromSettings' once-per-plugin-lifetime latch. GUI-thread only
  // (set inside the first getDialog() call).
  bool init_from_settings_done_ = false;
  // Toolbox runtime host provider (notifyDataChanged after import + the
  // reportMessage notification bell). Set by the toolbox during bind(); unset
  // for a dialog-only smoke load (notify() then no-ops).
  std::function<PJ::ToolboxRuntimeHostView()> runtime_host_provider_;
};

}  // namespace mcap_cloud
