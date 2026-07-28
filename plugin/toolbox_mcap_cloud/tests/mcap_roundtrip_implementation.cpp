// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Test-only MCAP implementation TU: reader + writer in ONE MCAP_IMPLEMENTATION
// (the round-trip test reads back what SessionMcapWriter wrote; splitting the
// two across TUs would duplicate the shared type/CRC definitions). Product
// targets use the writer-only src/mcap_implementation.cpp instead.
#define MCAP_IMPLEMENTATION
#include <mcap/reader.hpp>
#include <mcap/writer.hpp>
