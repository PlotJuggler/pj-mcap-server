// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Test-only MCAP implementation TU: reader + writer in ONE MCAP_IMPLEMENTATION
// (the round-trip test reads back what SessionMcapWriter wrote; splitting the
// two across TUs would duplicate the shared type/CRC definitions). The product
// TU (src/mcap_implementation.cpp) is combined too since the session file
// cache landed; this file stays so test target source lists remain
// independent of the product TU.
#define MCAP_IMPLEMENTATION
#include <mcap/reader.hpp>
#include <mcap/writer.hpp>
