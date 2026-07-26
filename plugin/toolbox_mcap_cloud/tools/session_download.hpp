// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// session_download — the CLI's MCAP-reconstructing download driver. Given a
// connected BackendConnection and a resolved OpenSessionParams, it opens a fresh
// session, registers the session's schemas + channels into a local MCAP file
// (via the shared SessionMcapWriter), and writes one Message record
// per streamed message (log/publish times + payload verbatim). The shared
// writer remaps wire topic/schema ids to the ids assigned by mcap::McapWriter;
// a reader still counts the file back to the exact streamed total — the
// harness's round-trip ground-truth gate.
//
// The implementation comes from the Conan mcap package already required by
// this target and shared with the GUI download path.
#pragma once

#include <cstdint>
#include <string>

#include "backend_connection.hpp"
#include "backend_types.hpp"

namespace mcap_cloud {

// Open a fresh session for `params`, reconstruct it into the MCAP at `out_path`,
// and return the final session stats. The output MCAP is CHUNKED + ZSTD-
// compressed with the session's schemas/channels. On any failure the returned
// SessionStats carries a non-empty .error (and .eos reflecting the cause); the
// partially-written file is still closed cleanly so a reader can inspect it.
[[nodiscard]] SessionStats downloadToMcap(BackendConnection& conn, const OpenSessionParams& params,
                                          const std::string& out_path, SessionInfo* info_out, std::string* error_out);

}  // namespace mcap_cloud
