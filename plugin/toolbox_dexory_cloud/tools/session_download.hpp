// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// session_download — the CLI's MCAP-reconstructing download driver. Given a
// connected BackendConnection and a resolved OpenSessionParams, it opens a fresh
// session, registers the session's schemas + channels into a local MCAP file
// (via the vendored Foxglove mcap::McapWriter), and writes one Message record
// per streamed message (log/publish times + payload verbatim). channel id ==
// wire topic_id and schema id == wire schema_id, so a reader counts the file
// back to the exact streamed total — the harness's round-trip ground-truth gate.
//
// MCAP WRITER CHOICE: the VENDORED header-only writer at
// data_load_mcap/contrib/mcap/writer.{hpp,inl} (with MCAP_IMPLEMENTATION) is
// used, NOT the conan mcap/2.1.1 package. Rationale documented in the report.
#pragma once

#include <cstdint>
#include <string>

#include "backend_connection.hpp"
#include "backend_types.hpp"

namespace dexory_cloud {

// Open a fresh session for `params`, reconstruct it into the MCAP at `out_path`,
// and return the final session stats. The output MCAP is CHUNKED + ZSTD-
// compressed with the session's schemas/channels. On any failure the returned
// SessionStats carries a non-empty .error (and .eos reflecting the cause); the
// partially-written file is still closed cleanly so a reader can inspect it.
[[nodiscard]] SessionStats downloadToMcap(BackendConnection& conn, const OpenSessionParams& params,
                                          const std::string& out_path, SessionInfo* info_out, std::string* error_out);

}  // namespace dexory_cloud
