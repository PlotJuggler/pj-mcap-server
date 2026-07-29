// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// The Conan MCAP C++ package is header-only. Keep its implementation macro in
// exactly one translation unit per target. This is the PRODUCT combined
// reader+writer TU: the session file cache (validated finalization + lookup,
// src/session_file_cache.cpp) needs mcap::McapReader, and reader.hpp and
// writer.hpp BOTH pull types.inl under MCAP_IMPLEMENTATION (non-inline
// definitions — MetadataIndex ctor, RecordOffset operators, CRC helpers), so
// a separate reader-only TU beside the old writer-only one would duplicate
// those shared symbols at link. One combined TU per target is the only
// collision-free shape (tests/mcap_roundtrip_implementation.cpp reached the
// same conclusion for the test targets). The former writer-only property —
// "the shipped plugin does not export mcap::McapReader symbols" — is
// deliberately given up: cache validation is product code now.
#define MCAP_IMPLEMENTATION
#include <mcap/reader.hpp>
#include <mcap/writer.hpp>
