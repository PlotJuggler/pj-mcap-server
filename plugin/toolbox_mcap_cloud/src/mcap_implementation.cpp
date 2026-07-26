// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// The Conan MCAP C++ package is header-only. Keep its implementation macro in
// exactly one translation unit per target; reader symbols are included as well
// so hermetic round-trip tests can inspect files written by SessionMcapWriter.
#define MCAP_IMPLEMENTATION
#include <mcap/reader.hpp>
#include <mcap/writer.hpp>
