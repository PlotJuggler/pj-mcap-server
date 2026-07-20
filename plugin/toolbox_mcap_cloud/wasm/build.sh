#!/usr/bin/env bash
# Copyright 2026 Davide Faconti
# SPDX-License-Identifier: MIT
#
# build.sh — the reproducible WASM compile job for the cloud connector decode-core.
#
# Unified plan M2c-DEX "WASM bonus" + risk 8: "a CI job that at minimum COMPILES
# client-core to WASM so the path cannot silently rot". This is a DEMO / NON-GATING
# deliverable — it never touches the native plugin build, the smoke/matrix/ctest
# gates, or any running server. See README.md for the transport/TLS/protobuf
# constraints this surfaces.
#
# It compiles the PURE protocol/decode core (src/session_key.hpp,
# src/session_cache.hpp, src/hierarchy_prefix.h, src/stitch_select.h,
# tools/cli_url_resolve.hpp) plus the official single-file zstd 1.5.7 DECODER
# amalgamation (third_party/zstd/) into one .wasm, then runs it under node and
# checks for the "WASM SMOKE PASS" line.
#
# Idempotent: re-running rebuilds into build/ (cleaned each run). Works from a
# clean checkout given an emsdk on PATH OR a discoverable user-local emsdk.
#
# Usage:
#   ./build.sh                 # build + (if node present) run the smoke
#   EMSDK=/path/to/emsdk ./build.sh
#   ./build.sh --no-run        # compile only (skip node execution)
#
# Last line: "WASM BUILD PASS" or "WASM BUILD FAIL: <reason>" (exit code matches).

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PLUGIN_DIR="$(cd "${HERE}/.." && pwd)"
SRC_DIR="${PLUGIN_DIR}/src"
TOOLS_DIR="${PLUGIN_DIR}/tools"
ZSTD_DIR="${HERE}/third_party/zstd"
BUILD_DIR="${HERE}/build"

RUN_NODE=1
for arg in "$@"; do
  case "$arg" in
    --no-run) RUN_NODE=0 ;;
    *) echo "unknown arg: $arg"; echo "WASM BUILD FAIL: bad-args"; exit 2 ;;
  esac
done

fail() {
  echo "WASM BUILD FAIL: $1"
  exit 1
}

# ── 1. Locate + activate emsdk ────────────────────────────────────────────────
# Preference order: emcc already on PATH > $EMSDK env > a few well-known
# user-local clones. Sourcing emsdk_env.sh puts emcc/em++/node on PATH.
if ! command -v em++ >/dev/null 2>&1; then
  CANDIDATES=()
  [ -n "${EMSDK:-}" ] && CANDIDATES+=("${EMSDK}")
  CANDIDATES+=("${HOME}/emsdk" "${HOME}/.local/emsdk" "${HERE}/.emsdk")
  for c in "${CANDIDATES[@]}"; do
    if [ -f "${c}/emsdk_env.sh" ]; then
      echo "Activating emsdk at ${c}"
      # shellcheck disable=SC1091
      source "${c}/emsdk_env.sh" >/dev/null 2>&1 || true
      break
    fi
  done
fi

command -v em++ >/dev/null 2>&1 || fail "emcc/em++ not found (install emsdk or set EMSDK=/path/to/emsdk)"
echo "Using $(em++ --version 2>/dev/null | head -1)"

# ── 2. Clean build dir ────────────────────────────────────────────────────────
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}"

# ── 3. Generate a real ZSTD frame with the NATIVE encoder (host g++) ──────────
# The wasm decoder then re-inflates it: cross-build (native encode -> wasm decode)
# functional evidence, not a self-fulfilling round-trip. We compile gen_test_frame
# against any available libzstd >= 1.5 (the conan 1.5.7 static lib if found, else
# the system zstd). Frame format (content-size header, one-shot) is encoder-version
# independent for the decoder, so either source produces a frame the 1.5.7 decoder
# accepts.
TEST_FRAME_HDR="${BUILD_DIR}/wasm_test_frame.h"
GEN_BIN="${BUILD_DIR}/gen_test_frame"

# Try conan zstd 1.5.7 first (matches the native pin), then system zstd.
ZSTD_INC=""
ZSTD_LIB=""
CONAN_ZSTD_H="$(find "${HOME}/.conan2/p" -path '*p/include/zstd.h' \
  -exec grep -l 'ZSTD_VERSION_RELEASE  *7' {} + 2>/dev/null | head -1)"
if [ -n "${CONAN_ZSTD_H}" ]; then
  CONAN_ZSTD_PKG="$(dirname "$(dirname "${CONAN_ZSTD_H}")")"
  if [ -f "${CONAN_ZSTD_PKG}/lib/libzstd.a" ]; then
    ZSTD_INC="-I${CONAN_ZSTD_PKG}/include"
    ZSTD_LIB="${CONAN_ZSTD_PKG}/lib/libzstd.a"
    echo "Native encoder: conan libzstd 1.5.7 (${CONAN_ZSTD_PKG})"
  fi
fi
if [ -z "${ZSTD_LIB}" ]; then
  # Fall back to the vendored 1.5.7 header + system libzstd for the *encode* side.
  if command -v zstd >/dev/null 2>&1 || ldconfig -p 2>/dev/null | grep -q libzstd; then
    ZSTD_INC="-I${ZSTD_DIR}"
    ZSTD_LIB="-lzstd"
    echo "Native encoder: system libzstd (vendored 1.5.7 header)"
  else
    fail "no native libzstd available to generate the test frame"
  fi
fi

HOST_CXX="${CXX:-c++}"
# shellcheck disable=SC2086
"${HOST_CXX}" -std=c++20 -O2 ${ZSTD_INC} "${HERE}/gen_test_frame.cpp" ${ZSTD_LIB} -o "${GEN_BIN}" \
  || fail "native gen_test_frame compile"
"${GEN_BIN}" "${TEST_FRAME_HDR}" || fail "gen_test_frame run"
[ -s "${TEST_FRAME_HDR}" ] || fail "test frame header empty"

# ── 4. Compile the zstd decoder amalgamation to a wasm object ─────────────────
# Decoder-only (no ZSTD_compress). 1.5.7, matching the native pin (README §zstd).
emcc -O2 -c "${ZSTD_DIR}/zstddeclib.c" -o "${BUILD_DIR}/zstddeclib.o" \
  || fail "zstddeclib.c -> wasm object"

# ── 5. Compile + link the pure decode-core smoke to .wasm ─────────────────────
# Full project warning bar (-Wall -Wextra -Werror) on OUR units — the pure headers
# must stay clean under it. -s ENVIRONMENT=node makes the artifact node-runnable.
WASM_OUT="${BUILD_DIR}/mcap_cloud_wasm_smoke.js"
em++ -std=c++20 -O2 -Wall -Wextra -Werror \
  -I"${SRC_DIR}" \
  -I"${TOOLS_DIR}" \
  -I"${ZSTD_DIR}" \
  -I"${BUILD_DIR}" \
  -s ENVIRONMENT=node \
  -s ALLOW_MEMORY_GROWTH=1 \
  "${HERE}/wasm_smoke_main.cpp" \
  "${BUILD_DIR}/zstddeclib.o" \
  -o "${WASM_OUT}" \
  || fail "decode-core smoke -> wasm"

[ -s "${BUILD_DIR}/mcap_cloud_wasm_smoke.wasm" ] || fail ".wasm artifact missing"
WASM_BYTES="$(stat -c%s "${BUILD_DIR}/mcap_cloud_wasm_smoke.wasm" 2>/dev/null || echo '?')"
echo "Produced ${BUILD_DIR}/mcap_cloud_wasm_smoke.wasm (${WASM_BYTES} bytes)"

# ── 6. (Non-gating) track that the protobuf-dependent TUs still COMPILE to wasm.
# They cannot LINK for wasm without cross-built libprotobuf+abseil (README
# §protobuf). Compiling them to objects catches source rot without claiming a
# browser-functional wire path. Skipped silently when the conan headers / generated
# proto are not present (e.g. a fresh checkout without a native build).
PB_GEN_DIR="$(find "${PLUGIN_DIR}/../build" -path '*toolbox_mcap_cloud/generated/pj_cloud.pb.h' 2>/dev/null \
  | head -1 | xargs -r dirname)"
PROTOBUF_INC="$(find "${HOME}/.conan2/p" -path '*p/include/google/protobuf/message.h' 2>/dev/null \
  | head -1 | sed 's#/google/protobuf/message.h##')"
ABSEIL_INC="$(find "${HOME}/.conan2/p" -path '*p/include/absl/base/config.h' 2>/dev/null \
  | head -1 | sed 's#/absl/base/config.h##')"
if [ -n "${PB_GEN_DIR}" ] && [ -n "${PROTOBUF_INC}" ] && [ -n "${ABSEIL_INC}" ]; then
  echo "Non-gating: protobuf-dependent TUs compile-to-object check"
  PB_OK=1
  # session_decode.cpp #includes <zstd.h> (vendored) — add ZSTD_DIR. The LZ4
  # inbound branch is __EMSCRIPTEN__-guarded out (no lz4 link needed for wasm).
  em++ -std=c++20 -O2 -c \
    -Wno-deprecated-pragma \
    -I"${SRC_DIR}" -I"${ZSTD_DIR}" -I"${PB_GEN_DIR}" -I"${PROTOBUF_INC}" -I"${ABSEIL_INC}" \
    "${SRC_DIR}/session_decode.cpp" -o "${BUILD_DIR}/session_decode.wasm.o" 2>/dev/null \
    || PB_OK=0
  em++ -std=c++20 -O2 -c \
    -Wno-deprecated-pragma \
    -I"${SRC_DIR}" -I"${PB_GEN_DIR}" -I"${PROTOBUF_INC}" -I"${ABSEIL_INC}" \
    "${SRC_DIR}/wire_mapping.cpp" -o "${BUILD_DIR}/wire_mapping.wasm.o" 2>/dev/null \
    || PB_OK=0
  if [ "${PB_OK}" = "1" ]; then
    echo "  session_decode.cpp + wire_mapping.cpp -> wasm objects OK (LINK still needs"
    echo "  cross-built libprotobuf+abseil — documented future work, NOT gating here)"
  else
    echo "  NOTE: pb-TU compile-to-object failed (non-gating; tracked, not fatal)"
  fi
else
  echo "Non-gating pb compile-to-object check: SKIPPED (no generated proto / conan headers found)"
fi

# ── 7. Run the smoke under node (functional evidence) ─────────────────────────
if [ "${RUN_NODE}" = "0" ]; then
  echo "Skipping node run (--no-run); compile-only."
  echo "WASM BUILD PASS"
  exit 0
fi

if ! command -v node >/dev/null 2>&1; then
  echo "node not on PATH (emsdk usually provides it); compiled .wasm only."
  echo "WASM BUILD PASS"
  exit 0
fi

echo "Running smoke under $(node --version)"
NODE_OUT="$(node "${WASM_OUT}" 2>&1)"
echo "${NODE_OUT}"
if echo "${NODE_OUT}" | grep -q "WASM SMOKE PASS"; then
  echo "WASM BUILD PASS"
  exit 0
fi
fail "smoke did not report WASM SMOKE PASS"
