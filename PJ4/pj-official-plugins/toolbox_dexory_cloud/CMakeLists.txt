find_package(nlohmann_json REQUIRED)
find_package(fmt REQUIRED)
find_package(lua REQUIRED)
find_package(sol2 REQUIRED)
# WS+Protobuf client-core transport. protobuf brings the protobuf_generate()
# helper + the protobuf::protoc IMPORTED target (Conan's protoc, NOT the system
# protoc) so the generated C++ matches the linked libprotobuf runtime.
find_package(protobuf REQUIRED)
find_package(ixwebsocket REQUIRED)
# Session/streaming (Slice 2): ZSTD batch-body + per-message decode, LZ4
# inbound-decode. Same Conan targets data_load_mcap links
# (zstd::libzstd_static + LZ4::lz4_static).
find_package(zstd REQUIRED)
find_package(lz4 REQUIRED)

include(${CMAKE_CURRENT_LIST_DIR}/../cmake/EmbedUi.cmake)
include(${CMAKE_CURRENT_LIST_DIR}/../cmake/EmbedManifest.cmake)

# ---------------------------------------------------------------------------
# Canonical wire protocol codegen.
#
# The single source of truth for the wire schema is the repo-root proto
# (pj-mcap-server/proto/pj_cloud.proto), shared with the Go server. Mirror
# parser_protobuf's pattern: generate C++ bindings with the CONAN protoc into
# the build tree, then compile the .pb.cc into the plugin. The generated header
# lands in generated/ (already on the plugin include path below).
# ---------------------------------------------------------------------------
set(DEXORY_CLOUD_PROTO ${CMAKE_CURRENT_SOURCE_DIR}/../../../proto/pj_cloud.proto)
set(DEXORY_CLOUD_PROTO_OUT_DIR ${CMAKE_CURRENT_BINARY_DIR}/generated)
file(MAKE_DIRECTORY ${DEXORY_CLOUD_PROTO_OUT_DIR})

protobuf_generate(
  LANGUAGE        cpp
  OUT_VAR         DEXORY_CLOUD_PROTO_SRCS
  PROTOS          ${DEXORY_CLOUD_PROTO}
  IMPORT_DIRS     ${CMAKE_CURRENT_SOURCE_DIR}/../../../proto
  PROTOC_OUT_DIR  ${DEXORY_CLOUD_PROTO_OUT_DIR}
)

# Generated protobuf code is not held to the plugin's -Werror bar.
set_source_files_properties(${DEXORY_CLOUD_PROTO_SRCS} PROPERTIES
  COMPILE_OPTIONS "-w"
  SKIP_LINTING ON
)

# The plugin is a cloud TOOLBOX (DexoryCloudToolbox : PJ::ToolboxPluginBase) +
# the catalog/Fetch dialog, both vtables in this one .so.
# src/dexory_cloud_toolbox.cpp is the entry; the in-dialog Fetch delegates
# parsing to the HOST's MessageParser plugins via the toolbox parser-ingest tail
# slots (ParserIngestDriver, src/parser_ingest_driver.cpp). The plugin ships
# ZERO message decoders. The transport is the REAL WS+Protobuf client
# (src/backend_connection.* over ixwebsocket) speaking pj_cloud.v1: catalog RPCs
# (Hello/ListFiles/GetFile) drive the browse dialog; the session path
# (OpenSession/MessageBatch/Cancel/Ack/Eos) drives the in-dialog download.
set(DEXORY_CLOUD_SOURCES
  src/dexory_cloud_toolbox.cpp
  src/parser_ingest_driver.cpp
  src/dexory_cloud_dialog.cpp
  src/fetch_worker.cpp
  src/backend_connection.cpp
  src/wire_mapping.cpp
  src/session_decode.cpp
  src/settings_store.cpp
  src/credential_store.cpp
  src/server_history.cpp
  src/core/time_format.cpp
)

add_library(toolbox_dexory_cloud_plugin SHARED ${DEXORY_CLOUD_SOURCES} ${DEXORY_CLOUD_PROTO_SRCS})

target_compile_features(toolbox_dexory_cloud_plugin PRIVATE cxx_std_20)
# Don't apply ${PJ_WARNING_FLAGS} here — the vendored PJ3 query/* headers
# and sol2 trigger -Wconversion and -Wunused-parameter under -Werror. The
# core dialog/state-machine code is reviewed manually; this matches the
# upstream toolbox build flags for the query/ subdir.
target_compile_options(toolbox_dexory_cloud_plugin PRIVATE
  -Wall -Wextra -Werror
  -Wno-conversion -Wno-unused-parameter -Wno-unused-function
)

target_include_directories(toolbox_dexory_cloud_plugin PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/src/core
  ${CMAKE_CURRENT_SOURCE_DIR}/src/query
  ${CMAKE_CURRENT_BINARY_DIR}/generated
)

target_link_libraries(toolbox_dexory_cloud_plugin PRIVATE
  plotjuggler_sdk::plugin_sdk
  fmt::fmt
  nlohmann_json::nlohmann_json
  lua::lua
  sol2::sol2
  protobuf::libprotobuf
  ixwebsocket::ixwebsocket
  zstd::libzstd_static
  LZ4::lz4_static
)

pj_embed_ui(toolbox_dexory_cloud_plugin
  UI_FILE  ${CMAKE_CURRENT_SOURCE_DIR}/ui/dexory_cloud_panel.ui
  HEADER   ${CMAKE_CURRENT_BINARY_DIR}/generated/dexory_cloud_panel_ui.hpp
  VAR_NAME kDexoryCloudPanelUi
)

pj_embed_ui(toolbox_dexory_cloud_plugin
  UI_FILE  ${CMAKE_CURRENT_SOURCE_DIR}/ui/cert_dialog.ui
  HEADER   ${CMAKE_CURRENT_BINARY_DIR}/generated/cert_dialog_ui.hpp
  VAR_NAME kCertDialogUi
)

pj_embed_ui(toolbox_dexory_cloud_plugin
  UI_FILE  ${CMAKE_CURRENT_SOURCE_DIR}/ui/tag_dialog.ui
  HEADER   ${CMAKE_CURRENT_BINARY_DIR}/generated/tag_dialog_ui.hpp
  VAR_NAME kTagDialogUi
)

pj_embed_manifest(toolbox_dexory_cloud_plugin
  HEADER   ${CMAKE_CURRENT_BINARY_DIR}/generated/dexory_cloud_panel_manifest.hpp
  VAR_NAME kDexoryCloudPanelManifest
)

pj_emit_plugin_manifest(toolbox_dexory_cloud_plugin
  FAMILY        toolbox
  MANIFEST_FILE ${CMAKE_CURRENT_SOURCE_DIR}/manifest.json
)

# ----------------------------------------------------------------------------
# Headless SDK harness CLI (ALWAYS built — NOT gated on BUILD_TESTING).
#
# dexory-cloud-cli drives the EXACT BackendConnection class the GUI uses (no
# parallel transport) so connectivity + catalog correctness against the server
# is testable without the Qt GUI. It compiles the transport units + generated
# proto directly (the plugin .so does not export them). Plain C++ — no Qt.
# ----------------------------------------------------------------------------
# The vendored Foxglove MCAP writer (data_load_mcap/contrib/mcap/writer.{hpp,inl})
# reconstructs the streamed session as a local MCAP. Header-only; the single
# MCAP_IMPLEMENTATION lives in tools/session_download.cpp. (Chosen over the conan
# mcap/2.1.1 package: it is already vendored + proven in data_load_mcap and links
# the same LZ4/zstd static targets, so no new conan dependency is introduced.)
set(DEXORY_CLOUD_VENDORED_MCAP ${CMAKE_CURRENT_SOURCE_DIR}/../data_load_mcap/contrib)

add_executable(dexory-cloud-cli
  tools/dexory_cloud_cli.cpp
  tools/session_download.cpp
  src/backend_connection.cpp
  src/wire_mapping.cpp
  src/session_decode.cpp
  src/core/time_format.cpp
  ${DEXORY_CLOUD_PROTO_SRCS}
)
target_compile_features(dexory-cloud-cli PRIVATE cxx_std_20)
target_compile_options(dexory-cloud-cli PRIVATE
  -Wall -Wextra -Werror -Wshadow -Wold-style-cast -Wcast-qual -Wconversion
)
target_include_directories(dexory-cloud-cli PRIVATE
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/src/core
  ${DEXORY_CLOUD_PROTO_OUT_DIR}
)
# The vendored MCAP headers are not held to our lint bar (SYSTEM include).
target_include_directories(dexory-cloud-cli SYSTEM PRIVATE ${DEXORY_CLOUD_VENDORED_MCAP})
# The transport units transitively include the generated pj_cloud.pb.h, whose
# inline accessors trip -Wconversion (size_type -> int). That header is codegen,
# not held to our lint bar (the plugin target relaxes the same flag for the same
# reason). session_download.cpp pulls in the vendored MCAP writer.inl, which also
# trips -Wconversion / -Wold-style-cast. The CLI's own translation unit keeps the
# full strict flags.
set_source_files_properties(
  src/backend_connection.cpp
  src/wire_mapping.cpp
  src/session_decode.cpp
  tools/session_download.cpp
  PROPERTIES COMPILE_OPTIONS "-Wno-conversion;-Wno-old-style-cast;-Wno-cast-qual;-Wno-shadow"
)
target_link_libraries(dexory-cloud-cli PRIVATE
  protobuf::libprotobuf
  ixwebsocket::ixwebsocket
  nlohmann_json::nlohmann_json
  zstd::libzstd_static
  LZ4::lz4_static
)

# ----------------------------------------------------------------------------
# Tests — std / lua / SDK only. The plugin ships ZERO message decoders;
# host-delegated parsing correctness is owned by pj_runtime:
#   - parser_ingest_driver_test (hermetic contract): ParserIngestDriver ABI,
#     per-topic skip paths, push fidelity, counters, idempotent release.
#   - Live ingest correctness (a later task): FetchWorker pullTopicsAsync against
#     a running server via ParserIngestDriver; per-topic counts from host.
#   - Host-side decode correctness: ToolboxRuntimeHostTest,
#     ToolboxParserIngestRealRosTest (owned by pj_runtime / pj_official_plugins).
# The transport / session / catalog tests below are family-agnostic and
# unchanged (DexoryCloudBackendLive is the smoke.sh live gate).
# ----------------------------------------------------------------------------
if(BUILD_TESTING)
  find_package(GTest REQUIRED)

  # Fetch-completion policy (close/import/error-dedup). Header-only, std only.
  add_executable(toolbox_dexory_cloud_fetch_summary_test
    tests/fetch_summary_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_fetch_summary_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_fetch_summary_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudFetchSummaryTest COMMAND toolbox_dexory_cloud_fetch_summary_test)

  # Stitched-selection merge (Slice 7): reorder-identical-request + overlap
  # validation + ordering + display name. Header-only, std only.
  add_executable(toolbox_dexory_cloud_stitch_select_test
    tests/stitch_select_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_stitch_select_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_stitch_select_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudStitchSelectTest COMMAND toolbox_dexory_cloud_stitch_select_test)

  # Sequence Name-column display shortening (strip the Hive date= segment).
  # Header-only, std only.
  add_executable(toolbox_dexory_cloud_seq_display_test
    tests/seq_display_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_seq_display_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_seq_display_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudSeqDisplayTest COMMAND toolbox_dexory_cloud_seq_display_test)

  # Slider->nanoseconds window mapping (overflow-safe; pins the aggregate
  # int64-overflow regression). Header-only, std only.
  add_executable(toolbox_dexory_cloud_slider_window_test
    tests/slider_window_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_slider_window_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_slider_window_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudSliderWindowTest COMMAND toolbox_dexory_cloud_slider_window_test)

  # Time-based session aggregation (partition + gap-threshold grouping).
  # Header-only, std only.
  add_executable(toolbox_dexory_cloud_aggregate_sessions_test
    tests/aggregate_sessions_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_aggregate_sessions_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_aggregate_sessions_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudAggregateSessionsTest COMMAND toolbox_dexory_cloud_aggregate_sessions_test)

  # S3-key field parser (non-advanced metadata filter: 6 Hive + filename fields).
  # Header-only, std only.
  add_executable(toolbox_dexory_cloud_s3_key_fields_test
    tests/s3_key_fields_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_s3_key_fields_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_s3_key_fields_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudS3KeyFieldsTest COMMAND toolbox_dexory_cloud_s3_key_fields_test)

  # SequencePicker date/time filter incl. "Every day". Header-only, std only.
  add_executable(toolbox_dexory_cloud_date_filter_test
    tests/date_filter_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_date_filter_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_date_filter_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudDateFilterTest COMMAND toolbox_dexory_cloud_date_filter_test)

  # Display elision for long Hive-partitioned object keys (HERMETIC): bounded
  # output, head+tail preserved, ASCII-only ellipsis, floor clamp. Header-only.
  add_executable(toolbox_dexory_cloud_elide_name_test
    tests/elide_name_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_elide_name_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_elide_name_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudElideNameTest COMMAND toolbox_dexory_cloud_elide_name_test)

  # D8 client-side '/'-prefix hierarchy derivation (HERMETIC): distinct sorted
  # prefixes, flat-corpus-empty, combo "All" sentinel, starts-with semantics,
  # and the narrows-existing-visible-rows composition. Header-only
  # (src/hierarchy_prefix.h), std only — no Qt, no host.
  add_executable(toolbox_dexory_cloud_hierarchy_prefix_test
    tests/hierarchy_prefix_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_hierarchy_prefix_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_hierarchy_prefix_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudHierarchyPrefixTest COMMAND toolbox_dexory_cloud_hierarchy_prefix_test)

  # Name filter (substring + ".*" regex).
  add_executable(toolbox_dexory_cloud_name_filter_test
    tests/name_filter_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_name_filter_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_name_filter_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudNameFilterTest COMMAND toolbox_dexory_cloud_name_filter_test)

  # Query engine (Lexer/Parser/serialize/Query/complete/analyze/Engine). The
  # query/* headers are byte-for-byte the PJ3 engine; these tests pin their
  # behavior. Lua-backed eval/validate needs lua + sol2 only.
  add_executable(toolbox_dexory_cloud_query_engine_test
    tests/query_engine_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_query_engine_test PRIVATE
    lua::lua sol2::sol2 GTest::gtest_main
  )
  target_compile_features(toolbox_dexory_cloud_query_engine_test PRIVATE cxx_std_20)
  # Match the plugin's relaxed flags: the vendored PJ3 query/* headers and sol2
  # trip -Wconversion / -Wunused-parameter under -Werror.
  target_compile_options(toolbox_dexory_cloud_query_engine_test PRIVATE
    -Wall -Wextra -Werror
    -Wno-conversion -Wno-unused-parameter -Wno-unused-function
  )
  target_include_directories(toolbox_dexory_cloud_query_engine_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src/core
  )
  add_test(NAME DexoryCloudQueryEngineTest COMMAND toolbox_dexory_cloud_query_engine_test)

  # Query-filter / visible-row helper: validity-gating + name/date combine.
  # Header-only logic plus the Lua engine; needs lua + sol2 but no Arrow/Flight.
  add_executable(toolbox_dexory_cloud_query_filter_test
    tests/query_filter_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_query_filter_test PRIVATE
    lua::lua sol2::sol2 GTest::gtest_main
  )
  target_compile_features(toolbox_dexory_cloud_query_filter_test PRIVATE cxx_std_20)
  target_compile_options(toolbox_dexory_cloud_query_filter_test PRIVATE
    -Wall -Wextra -Werror
    -Wno-conversion -Wno-unused-parameter -Wno-unused-function
  )
  target_include_directories(toolbox_dexory_cloud_query_filter_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src/core
  )
  add_test(NAME DexoryCloudQueryFilterTest COMMAND toolbox_dexory_cloud_query_filter_test)

  # Column-sort permutation (numeric vs lexicographic, stability). Header-only.
  add_executable(toolbox_dexory_cloud_table_sort_test
    tests/table_sort_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_table_sort_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_table_sort_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudTableSortTest COMMAND toolbox_dexory_cloud_table_sort_test)

  # Human-readable byte formatting (1024-based, PJ3 parity). Header-only.
  add_executable(toolbox_dexory_cloud_format_utils_test
    tests/format_utils_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_format_utils_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_format_utils_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudFormatUtilsTest COMMAND toolbox_dexory_cloud_format_utils_test)

  # SessionKey normalization + FNV-1a hash (Slice 8, Plan B Task 8a). The exact
  # four plan cases. Header-only (src/session_key.hpp), std only, hermetic.
  add_executable(toolbox_dexory_cloud_session_key_test
    tests/session_key_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_session_key_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_session_key_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudSessionKeyTest COMMAND toolbox_dexory_cloud_session_key_test)

  # In-memory SessionCache (Slice 8, Plan D Task 5 adapted): HIT/MISS, key
  # exactness, reorder-collide, LRU-by-entry-count, COMPLETE-only store, fresh-
  # instance-empty, existence-predicate (gone -> evict+miss; presence-unknown ->
  # miss without evict). Header-only (src/session_cache.hpp), std only, hermetic.
  add_executable(toolbox_dexory_cloud_session_cache_test
    tests/session_cache_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_session_cache_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_session_cache_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudSessionCacheTest COMMAND toolbox_dexory_cloud_session_cache_test)

  # PJ3-ported leaf utility tests. Standard library only — no Arrow Flight,
  # no toolbox host.
  add_executable(toolbox_dexory_cloud_server_history_test
    tests/server_history_test.cpp
    src/server_history.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_server_history_test PRIVATE
    GTest::gtest_main
  )
  target_include_directories(toolbox_dexory_cloud_server_history_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudServerHistoryTest COMMAND toolbox_dexory_cloud_server_history_test)

  add_executable(toolbox_dexory_cloud_tls_utils_test
    tests/tls_utils_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_tls_utils_test PRIVATE
    GTest::gtest_main
  )
  target_include_directories(toolbox_dexory_cloud_tls_utils_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudTlsUtilsTest COMMAND toolbox_dexory_cloud_tls_utils_test)

  add_executable(toolbox_dexory_cloud_time_format_test
    tests/time_format_test.cpp
    src/core/time_format.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_time_format_test PRIVATE
    GTest::gtest_main
  )
  target_include_directories(toolbox_dexory_cloud_time_format_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  add_test(NAME DexoryCloudTimeFormatTest COMMAND toolbox_dexory_cloud_time_format_test)

  add_executable(toolbox_dexory_cloud_settings_store_test
    tests/settings_store_test.cpp
    src/settings_store.cpp
  )
  target_include_directories(toolbox_dexory_cloud_settings_store_test PRIVATE src)
  target_link_libraries(toolbox_dexory_cloud_settings_store_test PRIVATE
    plotjuggler_sdk::plugin_sdk nlohmann_json::nlohmann_json GTest::gtest_main
  )
  add_test(NAME DexoryCloudSettingsStoreTest COMMAND toolbox_dexory_cloud_settings_store_test)

  # D6 CredentialStore seam + file-backed fallback (HERMETIC): per-server token
  # round-trip, key normalization/collision, isolation, overwrite, erase,
  # empty-vs-absent, 0600 file / 0700 dir perms, corrupt-store tolerance, and
  # the explicit>stored>none precedence helper. Rooted at a private temp dir so
  # it never touches the real $XDG_CONFIG_HOME. Links nlohmann_json + plugin_sdk
  # (for pj_base/sdk/platform.hpp getEnv) — std + json only, no socket, no Qt.
  add_executable(toolbox_dexory_cloud_credential_store_test
    tests/credential_store_test.cpp
    src/credential_store.cpp
    src/server_history.cpp
  )
  target_include_directories(toolbox_dexory_cloud_credential_store_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  target_link_libraries(toolbox_dexory_cloud_credential_store_test PRIVATE
    plotjuggler_sdk::plugin_sdk nlohmann_json::nlohmann_json GTest::gtest_main
  )
  add_test(NAME DexoryCloudCredentialStoreTest COMMAND toolbox_dexory_cloud_credential_store_test)

  # Wire mapping: protobuf round-trip of ListFilesResponse / GetFileResponse ->
  # the dialog's SequenceInfo / TopicInfo. Socket-free (no live server): it
  # builds protobuf messages, serializes + parses them, then asserts the pure
  # mapping. Compiles the generated .pb.cc + the mapping unit; links libprotobuf.
  add_executable(toolbox_dexory_cloud_wire_mapping_test
    tests/wire_mapping_test.cpp
    src/wire_mapping.cpp
    ${DEXORY_CLOUD_PROTO_SRCS}
  )
  target_include_directories(toolbox_dexory_cloud_wire_mapping_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${DEXORY_CLOUD_PROTO_OUT_DIR}
  )
  target_link_libraries(toolbox_dexory_cloud_wire_mapping_test PRIVATE
    protobuf::libprotobuf GTest::gtest_main
  )
  add_test(NAME DexoryCloudWireMappingTest COMMAND toolbox_dexory_cloud_wire_mapping_test)

  # Session batch decode (HERMETIC): builds MessageBatch protos in memory,
  # compresses bodies with the linked zstd, and asserts the decoder reproduces
  # the messages (ZSTD body / NONE singleton RAW+ZSTD) and REJECTS a size
  # mismatch / corrupt frame / unknown body_encoding. No socket, no server.
  add_executable(toolbox_dexory_cloud_session_decode_test
    tests/session_decode_test.cpp
    src/session_decode.cpp
    ${DEXORY_CLOUD_PROTO_SRCS}
  )
  target_include_directories(toolbox_dexory_cloud_session_decode_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${DEXORY_CLOUD_PROTO_OUT_DIR}
  )
  # session_decode.cpp + generated proto trip -Wconversion under -Werror; the
  # test TU keeps strict flags (gtest's defaults), the unit/codegen TUs relax it.
  set_source_files_properties(
    src/session_decode.cpp
    PROPERTIES COMPILE_OPTIONS "-Wno-conversion"
  )
  target_link_libraries(toolbox_dexory_cloud_session_decode_test PRIVATE
    protobuf::libprotobuf zstd::libzstd_static LZ4::lz4_static GTest::gtest_main
  )
  add_test(NAME DexoryCloudSessionDecodeTest COMMAND toolbox_dexory_cloud_session_decode_test)

  # Parser-ingest driver (Slice 16, HERMETIC): ParserIngestDriver bindSession/
  # decode/finalize against a fake recorder host implementing the toolbox
  # parser-ingest tail slots (create/release_parser_ingest) + the data-source
  # runtime slots (ensure_parser_binding/push_message). Pins the verbatim ABI
  # fields (incl. the load-bearing non-empty parser_config_json "{}"), the
  # per-topic skip paths, push fidelity, counters, idempotent release, and the
  # older-host per-topic reason. SDK headers + GTest only — no rosx, no socket.
  add_executable(toolbox_dexory_cloud_parser_ingest_test
    tests/parser_ingest_driver_test.cpp
    src/parser_ingest_driver.cpp
  )
  target_compile_features(toolbox_dexory_cloud_parser_ingest_test PRIVATE cxx_std_20)
  target_compile_options(toolbox_dexory_cloud_parser_ingest_test PRIVATE
    -Wall -Wextra -Werror
  )
  target_include_directories(toolbox_dexory_cloud_parser_ingest_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
  )
  target_link_libraries(toolbox_dexory_cloud_parser_ingest_test PRIVATE
    plotjuggler_sdk::plugin_sdk GTest::gtest_main
  )
  add_test(NAME DexoryCloudParserIngestTest COMMAND toolbox_dexory_cloud_parser_ingest_test)

  # Reconnect-resume + cache (Slice 8, Plan D Task 10 + Task 5). LIVE-gated on
  # DEXORY_CLOUD_LIVE_URL (plus one hermetic backoff-contract test that always
  # runs): forces a mid-pull socket drop and asserts the resume loop drives the
  # stream to COMPLETE with the exact zeg_1 count (33670) and NO duplicates;
  # RESUME_NOT_POSSIBLE verbatim on a bogus subscription_id; cancel-during-resume
  # -> Cancelled; and a repeat worker pull is a cache HIT with zero transport.
  # Compiles the worker + transport units so pullTopicsAsync runs end-to-end
  # (no rosx — parsing is host-delegated via ParserIngestDriver).
  add_executable(toolbox_dexory_cloud_session_resume_live_test
    tests/session_resume_live_test.cpp
    src/parser_ingest_driver.cpp
    src/fetch_worker.cpp
    src/backend_connection.cpp
    src/wire_mapping.cpp
    src/session_decode.cpp
    ${DEXORY_CLOUD_PROTO_SRCS}
  )
  target_compile_features(toolbox_dexory_cloud_session_resume_live_test PRIVATE cxx_std_20)
  target_include_directories(toolbox_dexory_cloud_session_resume_live_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src/core
    ${DEXORY_CLOUD_PROTO_OUT_DIR}
  )
  set_source_files_properties(
    src/fetch_worker.cpp
    src/backend_connection.cpp
    src/wire_mapping.cpp
    src/session_decode.cpp
    PROPERTIES COMPILE_OPTIONS "-Wno-conversion"
  )
  target_link_libraries(toolbox_dexory_cloud_session_resume_live_test PRIVATE
    plotjuggler_sdk::plugin_sdk
    protobuf::libprotobuf ixwebsocket::ixwebsocket nlohmann_json::nlohmann_json
    zstd::libzstd_static LZ4::lz4_static GTest::gtest_main
  )
  add_test(NAME DexoryCloudSessionResumeLiveTest COMMAND toolbox_dexory_cloud_session_resume_live_test)

  # Live worker-level HOST-DELEGATED ingest test (Slice 16). LIVE-gated on
  # DEXORY_CLOUD_LIVE_URL: drives the REAL FetchWorker::pullTopicsAsync over the
  # REAL BackendConnection with BOTH providers wired (FakeToolboxHost datasets +
  # FakeIngestHost parser-ingest recorder) and asserts the pinned zeg_1 ground
  # truth at the ingest ABI: 6 verbatim bindings (imu = sensor_msgs/msg/Imu,
  # ros2msg, non-empty schema, config "{}"), ONE created+released ingest
  # context, 33670 total pushes / 14904 on the imu handle, non-empty push bytes,
  # and a clean cancel-mid-pull (finalize still releases the context). Same
  # worker/transport sources as the resume live test (no rosx — the plugin
  # ships no decoders).
  add_executable(toolbox_dexory_cloud_parser_ingest_live_test
    tests/parser_ingest_live_test.cpp
    src/parser_ingest_driver.cpp
    src/fetch_worker.cpp
    src/backend_connection.cpp
    src/wire_mapping.cpp
    src/session_decode.cpp
    ${DEXORY_CLOUD_PROTO_SRCS}
  )
  target_compile_features(toolbox_dexory_cloud_parser_ingest_live_test PRIVATE cxx_std_20)
  target_include_directories(toolbox_dexory_cloud_parser_ingest_live_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src/core
    ${DEXORY_CLOUD_PROTO_OUT_DIR}
  )
  target_link_libraries(toolbox_dexory_cloud_parser_ingest_live_test PRIVATE
    plotjuggler_sdk::plugin_sdk
    protobuf::libprotobuf ixwebsocket::ixwebsocket nlohmann_json::nlohmann_json
    zstd::libzstd_static LZ4::lz4_static GTest::gtest_main
  )
  add_test(NAME DexoryCloudParserIngestLiveTest COMMAND toolbox_dexory_cloud_parser_ingest_live_test)

  # Live session download test: drives the REAL BackendConnection session path
  # (openSessionFresh + downloadSession) against a running server and asserts the
  # exact received message counts (all-topics 33670, /nissan/vehicle_speed 4513).
  # Self-skips unless DEXORY_CLOUD_LIVE_URL is set.
  add_executable(toolbox_dexory_cloud_session_download_live_test
    tests/session_download_live_test.cpp
    src/backend_connection.cpp
    src/wire_mapping.cpp
    src/session_decode.cpp
    ${DEXORY_CLOUD_PROTO_SRCS}
  )
  target_include_directories(toolbox_dexory_cloud_session_download_live_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${DEXORY_CLOUD_PROTO_OUT_DIR}
  )
  set_source_files_properties(
    src/backend_connection.cpp
    src/wire_mapping.cpp
    src/session_decode.cpp
    PROPERTIES COMPILE_OPTIONS "-Wno-conversion"
  )
  target_link_libraries(toolbox_dexory_cloud_session_download_live_test PRIVATE
    protobuf::libprotobuf ixwebsocket::ixwebsocket zstd::libzstd_static LZ4::lz4_static GTest::gtest_main
  )
  add_test(NAME DexoryCloudSessionDownloadLiveTest COMMAND toolbox_dexory_cloud_session_download_live_test)

  # Live transport test: exercises the REAL BackendConnection connect/list path
  # against a running pj-cloud server. Self-skips unless DEXORY_CLOUD_LIVE_URL
  # is set (e.g. ws://localhost:8080), so the hermetic suite stays green in CI.
  add_executable(toolbox_dexory_cloud_backend_live_test
    tests/backend_connection_live_test.cpp
    src/backend_connection.cpp
    src/wire_mapping.cpp
    src/session_decode.cpp
    ${DEXORY_CLOUD_PROTO_SRCS}
  )
  target_include_directories(toolbox_dexory_cloud_backend_live_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${DEXORY_CLOUD_PROTO_OUT_DIR}
  )
  target_link_libraries(toolbox_dexory_cloud_backend_live_test PRIVATE
    protobuf::libprotobuf ixwebsocket::ixwebsocket zstd::libzstd_static LZ4::lz4_static GTest::gtest_main
  )
  add_test(NAME DexoryCloudBackendLiveTest COMMAND toolbox_dexory_cloud_backend_live_test)

  # Hermetic error-path test: NO env gate, NO server. Pins the closed-port
  # connect failure (non-empty error naming the URL) and the never-connected
  # listSequences()/listTopics() empty-result contract. Links like the live test.
  add_executable(toolbox_dexory_cloud_backend_error_test
    tests/backend_connection_error_test.cpp
    src/backend_connection.cpp
    src/wire_mapping.cpp
    src/session_decode.cpp
    ${DEXORY_CLOUD_PROTO_SRCS}
  )
  target_include_directories(toolbox_dexory_cloud_backend_error_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${DEXORY_CLOUD_PROTO_OUT_DIR}
  )
  target_link_libraries(toolbox_dexory_cloud_backend_error_test PRIVATE
    protobuf::libprotobuf ixwebsocket::ixwebsocket zstd::libzstd_static LZ4::lz4_static GTest::gtest_main
  )
  add_test(NAME DexoryCloudBackendErrorTest COMMAND toolbox_dexory_cloud_backend_error_test)

  # D12 env-var fallback (HERMETIC): the CLI URL/token precedence rule
  # (explicit --url/--token > env DEXORY_CLOUD_URL/DEXORY_CLOUD_API_KEY >
  # default). Pure header (tools/cli_url_resolve.hpp), std only — no socket.
  add_executable(toolbox_dexory_cloud_cli_url_resolve_test
    tests/cli_url_resolve_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_cli_url_resolve_test PRIVATE GTest::gtest_main)
  target_include_directories(toolbox_dexory_cloud_cli_url_resolve_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/tools
  )
  add_test(NAME DexoryCloudCliUrlResolveTest COMMAND toolbox_dexory_cloud_cli_url_resolve_test)

  # D11 plugin-load smoke (HERMETIC): dlopen the built toolbox .so the way the
  # host does and assert BOTH entry vtables (PJ_get_toolbox_vtable +
  # PJ_get_dialog_vtable) resolve non-null, plus the boot ABI symbol. Resolves
  # symbols only (RTLD_NOW|RTLD_LOCAL) — never constructs the Qt dialog without a
  # QApplication. The .so path is passed via a compile definition from the target
  # file (mirrors how the ROS parity test passes PJ_ROS_PARSER_PLUGIN_PATH).
  add_executable(toolbox_dexory_cloud_plugin_load_smoke_test
    tests/plugin_load_smoke_test.cpp
  )
  target_link_libraries(toolbox_dexory_cloud_plugin_load_smoke_test PRIVATE
    GTest::gtest_main ${CMAKE_DL_LIBS}
  )
  target_compile_definitions(toolbox_dexory_cloud_plugin_load_smoke_test PRIVATE
    DEXORY_PLUGIN_SO_PATH="$<TARGET_FILE:toolbox_dexory_cloud_plugin>"
  )
  # Build the .so before this test runs (it dlopens the artifact).
  add_dependencies(toolbox_dexory_cloud_plugin_load_smoke_test toolbox_dexory_cloud_plugin)
  add_test(NAME DexoryCloudPluginLoadSmokeTest COMMAND toolbox_dexory_cloud_plugin_load_smoke_test)

  # B14 no-Qt guard (HERMETIC, build-only): the as-built dexory-cloud-cli links
  # ZERO Qt (ixwebsocket transport, not QtWebSockets). Enforce it: ldd the built
  # CLI and FAIL if any libQt line appears (case-insensitive: libQt6Core.so etc.).
  # This is the as-built adaptation of Plan B Task 14's CI guard — the plugin CI
  # that the plan envisioned does not exist, so the invariant is pinned as a
  # ctest instead. Only the CLI is asserted Qt-free; the plugin .so IS a Qt dialog
  # and is intentionally NOT guarded here.
  add_test(NAME DexoryCloudCliNoQtGuard
    COMMAND ${CMAKE_COMMAND}
      -DCLI_BIN=$<TARGET_FILE:dexory-cloud-cli>
      -P ${CMAKE_CURRENT_SOURCE_DIR}/cmake/check_no_qt.cmake
  )

  # The embedded .ui files are hex-dumped verbatim by pj_embed_ui (never parsed
  # at build time), so a malformed XML compiles fine and only explodes at
  # RUNTIME when the dialog host parses it (Designer: "Expected '>', but got
  # ' '"). Regression gate: every ui/*.ui must be well-formed XML and pure
  # ASCII (non-ASCII narrows in the constexpr char[] hex embed under -Werror).
  # Caught live: an em-dash "fixed" to `--` INSIDE an XML comment is illegal
  # XML and silently killed the tag sub-dialog.
  add_test(NAME DexoryCloudUiFilesWellFormed
    COMMAND python3 -c
"import sys, xml.etree.ElementTree as ET
for f in sys.argv[1:]:
    ET.parse(f)  # raises on malformed XML (incl. '--' inside comments)
    data = open(f, 'rb').read()
    bad = [b for b in data if b > 0x7e or (b < 0x20 and b not in (0x09, 0x0a, 0x0d))]
    assert not bad, f'{f}: non-ASCII/control bytes present'
    print(f, 'ok')
"
    ${CMAKE_CURRENT_SOURCE_DIR}/ui/dexory_cloud_panel.ui
    ${CMAKE_CURRENT_SOURCE_DIR}/ui/cert_dialog.ui
    ${CMAKE_CURRENT_SOURCE_DIR}/ui/tag_dialog.ui
  )
endif()
