#!/usr/bin/env bash
# build.sh — build the PJ Cloud Connector:
#   1. the Go server + dev tools  (fast, no protoc — the wire bindings are checked in)
#   2. the "Dexory Cloud" connector plugin (Conan + CMake)
#
# Prerequisite: the plotjuggler_sdk Conan package must already be in your cache.
# Install it from the PlotJuggler/plotjuggler_sdk-cloud repo (cloud branch):
#   cd /path/to/plotjuggler_sdk && conan create . --build=missing
#
# Idempotent: safe to re-run (incremental). Run ./run.sh afterwards to start the
# local backend (Minio + synthetic data + server).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The Go toolchain here lives at $HOME/.local/go and is NOT on the default PATH.
export PATH="$HOME/.local/go/bin:$HOME/go/bin:$PATH"
command -v go >/dev/null || { echo "ERROR: 'go' not on PATH (expected $HOME/.local/go/bin/go)"; exit 1; }

echo "==> [1/2] Building the Go server + dev tools (server/bin/)"
# Build directly with 'go build' (NOT 'make build', which regenerates the proto
# bindings and so needs protoc — the Go bindings are already checked in).
( cd "$ROOT/server" \
    && go build -o bin/pj-cloud-server  ./cmd/pj-cloud-server \
    && go build -o bin/seed             ./cmd/seed \
    && go build -o bin/gen-ci-fixtures  ./cmd/gen-ci-fixtures \
    && go build -o bin/gen-3d-fixture   ./cmd/gen-3d-fixture )
echo "    server -> server/bin/pj-cloud-server"

echo "==> [2/2] Building the Dexory Cloud connector plugin"
SDK_VER="$(cat "$ROOT/plugin/SDK_VERSION")"
if ! conan list "plotjuggler_sdk/$SDK_VER" 2>/dev/null | grep -q "plotjuggler_sdk/$SDK_VER"; then
  echo "ERROR: plotjuggler_sdk/$SDK_VER not found in Conan cache."
  echo "  Install it from the PlotJuggler/plotjuggler_sdk-cloud repo (cloud branch):"
  echo "    cd /path/to/plotjuggler_sdk && conan create . --build=missing"
  exit 1
fi
( cd "$ROOT/plugin/toolbox_dexory_cloud" \
    && conan install . --output-folder=build --build=missing -s compiler.cppstd=20 \
    && cmake -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake -DCMAKE_BUILD_TYPE=Release \
    && cmake --build build -j"$(nproc)" )
echo "    plugin -> plugin/toolbox_dexory_cloud/build/bin/libtoolbox_dexory_cloud_plugin.so"

echo
echo "==> build complete.  Next:  ./run.sh   (starts Minio + synthetic data + the server)"
