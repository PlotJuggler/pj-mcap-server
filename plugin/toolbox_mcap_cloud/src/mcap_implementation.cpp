// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// The Conan MCAP C++ package is header-only. Keep its implementation macro in
// exactly one translation unit per target. Product targets are WRITE-only —
// the reader implementation lives in tests/mcap_roundtrip_implementation.cpp
// so the shipped plugin does not compile or export mcap::McapReader symbols
// (mcap's MCAP_PUBLIC defeats -fvisibility=hidden).
#define MCAP_IMPLEMENTATION
#include <mcap/writer.hpp>
