// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// The ONLY translation unit in tests/ that may include a platform socket
// header: on Windows both <winsock2.h> and IXNetSystem.h pull in windows.h,
// whose winerror.h ERROR_* macros textually clobber the pj_cloud::v1::ERROR_*
// enumerators used in test bodies (MSVC C2589). Isolating them here keeps every
// test TU clean — see find_free_port.hpp for the full rationale.

#include "find_free_port.hpp"

#include <ixwebsocket/IXNetSystem.h>

#ifdef _WIN32
#include <winsock2.h>  // sockaddr_in, htonl/ntohs, getsockname, closesocket
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace mcap_cloud_test {

int findFreePort() {
  // WSAStartup before the raw ::socket below AND the ix server/client the tests
  // stand up (ixwebsocket does not self-initialize on Windows); no-op on POSIX.
  // Windows/POSIX also disagree on the socket handle type, the getsockname
  // length type, and the close call — alias all three.
  ix::initNetSystem();
#ifdef _WIN32
  using probe_socket_t = SOCKET;
  using probe_socklen_t = int;
#else
  using probe_socket_t = int;
  using probe_socklen_t = socklen_t;
#endif
  const probe_socket_t probe = ::socket(AF_INET, SOCK_STREAM, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ::bind(probe, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  probe_socklen_t len = sizeof(addr);
  ::getsockname(probe, reinterpret_cast<sockaddr*>(&addr), &len);
  const int port = ntohs(addr.sin_port);
#ifdef _WIN32
  ::closesocket(probe);
#else
  ::close(probe);
#endif
  return port;
}

}  // namespace mcap_cloud_test
