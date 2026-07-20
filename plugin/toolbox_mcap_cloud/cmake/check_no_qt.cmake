# Copyright 2026 Davide Faconti
# SPDX-License-Identifier: MIT
#
# B14 no-Qt guard (build-only ctest): run `ldd ${CLI_BIN}` and FAIL if any Qt
# shared library appears in the dependency list. The as-built mcap-cloud-cli
# uses the ixwebsocket transport (NOT QtWebSockets) and is asserted to link zero
# Qt (CLAUDE.md "Reference codebases" / Plan B transport note). This is the
# as-built adaptation of Plan B Task 14's CI guard — the plugin CI it envisioned
# does not exist, so the invariant is enforced as a ctest.
#
# Invoked as: cmake -DCLI_BIN=<path> -P check_no_qt.cmake

if(NOT DEFINED CLI_BIN)
  message(FATAL_ERROR "check_no_qt.cmake: CLI_BIN not set")
endif()
if(NOT EXISTS "${CLI_BIN}")
  message(FATAL_ERROR "check_no_qt.cmake: CLI binary not found: ${CLI_BIN}")
endif()

find_program(LDD_EXE ldd)
if(NOT LDD_EXE)
  message(FATAL_ERROR "check_no_qt.cmake: ldd not found on PATH")
endif()

execute_process(
  COMMAND "${LDD_EXE}" "${CLI_BIN}"
  OUTPUT_VARIABLE ldd_out
  ERROR_VARIABLE ldd_err
  RESULT_VARIABLE ldd_rc
)
if(NOT ldd_rc EQUAL 0)
  message(FATAL_ERROR "check_no_qt.cmake: ldd failed (rc=${ldd_rc}): ${ldd_err}")
endif()

# Case-insensitive match for any libQt* dependency (libQt6Core.so, libQt5Gui, ...).
string(TOLOWER "${ldd_out}" ldd_lower)
if(ldd_lower MATCHES "libqt")
  message(FATAL_ERROR
    "check_no_qt.cmake: mcap-cloud-cli links Qt — it must be Qt-free.\nldd output:\n${ldd_out}")
endif()

message(STATUS "check_no_qt: ${CLI_BIN} links zero Qt (ok)")
