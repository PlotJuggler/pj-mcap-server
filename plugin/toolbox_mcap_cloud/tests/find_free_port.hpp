// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Shared loopback-port probe for the hermetic fake-WS-server tests
// (server_caps_test.cpp, list_pagination_test.cpp).
//
// Two reasons this is a declaration-only header with the implementation in its
// own translation unit, rather than an inline function:
//
//  1. It was previously copy-pasted between those two test files and the copies
//     DIVERGED — the Windows port fixed one and left the other POSIX-only,
//     which broke the windows-x64 CI leg. One definition makes that impossible.
//
//  2. The implementation needs <winsock2.h> / IXNetSystem.h on Windows, and
//     BOTH drag in windows.h, whose winerror.h defines ERROR_* as numeric
//     macros that textually clobber the pj_cloud::v1::ERROR_* enumerators a
//     test body may reference (MSVC C2589) — the same trap documented in
//     backend_connection.cpp. Keeping every platform socket header inside
//     find_free_port.cpp means no test TU ever sees windows.h.
//
// So: include ONLY this header from a test; never a socket header directly.
#pragma once

namespace mcap_cloud_test {

// Discover a free loopback port by binding a throwaway socket to port 0 (the
// OS assigns one), reading it back via getsockname, then releasing it.
// ix::WebSocketServer's SocketServer::getPort() only echoes back its
// constructor argument (it never calls getsockname() itself after an
// OS-assigned bind), so this has to happen out-of-band on the client side.
// Small TOCTOU race (another process could grab the port in the microseconds
// before WebSocketServer binds it) — an accepted trade-off for a hermetic unit
// test; unlike backend_connection_error_test.cpp's reserved closed port
// (127.0.0.1:9), a real listener needs an actually-free port.
//
// Also performs the Windows WSAStartup (ix::initNetSystem) the raw socket call
// and the ix server/client both require; a no-op on POSIX.
int findFreePort();

}  // namespace mcap_cloud_test
