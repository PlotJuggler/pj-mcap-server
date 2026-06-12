#!/usr/bin/env bash
# build.sh — build the whole PJ Cloud Connector for a fresh checkout:
#   1. the Go server + dev tools   (fast, no protoc — the wire bindings are checked in)
#   2. the "Dexory Cloud" plugin   (Conan + CMake)
#   3. the PlotJuggler 4 app        (heavy; skipped with instructions if Qt is absent)
#
# Idempotent: safe to re-run (incremental). Run ./run.sh afterwards to start the
# local backend (Minio + synthetic data + server).
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# The Go toolchain here lives at $HOME/.local/go and is NOT on the default PATH.
export PATH="$HOME/.local/go/bin:$HOME/go/bin:$PATH"
command -v go >/dev/null || { echo "ERROR: 'go' not on PATH (expected $HOME/.local/go/bin/go)"; exit 1; }

echo "==> [1/3] Building the Go server + dev tools (server/bin/)"
# Build directly with 'go build' (NOT 'make build', which regenerates the proto
# bindings and so needs protoc — the Go bindings are already checked in).
( cd "$ROOT/server" \
    && go build -o bin/pj-cloud-server  ./cmd/pj-cloud-server \
    && go build -o bin/seed             ./cmd/seed \
    && go build -o bin/gen-ci-fixtures  ./cmd/gen-ci-fixtures \
    && go build -o bin/gen-3d-fixture   ./cmd/gen-3d-fixture )
echo "    server -> server/bin/pj-cloud-server"

echo "==> [2/3] Building the Dexory Cloud plugin"
( cd "$ROOT/PJ4/pj-official-plugins" && ./build.sh toolbox_dexory_cloud )
# Stage the freshly built plugin .so into run.sh's plugin dir (build/all/Release/bin).
PLUGIN_SO="$ROOT/PJ4/pj-official-plugins/build/toolbox_dexory_cloud/Release/bin/libtoolbox_dexory_cloud_plugin.so"
PLUGIN_DST="$ROOT/PJ4/pj-official-plugins/build/all/Release/bin"
if [ -f "$PLUGIN_SO" ] && [ -d "$PLUGIN_DST" ]; then
  cp "$PLUGIN_SO" "$PLUGIN_DST.tmp.so" 2>/dev/null && mv -f "$PLUGIN_DST.tmp.so" "$PLUGIN_DST/libtoolbox_dexory_cloud_plugin.so" || true
fi

echo "==> [3/3] Building the PlotJuggler 4 app"
if [ -d "$ROOT/PJ4/.qt/6.8.3/gcc_64" ]; then
  ( cd "$ROOT/PJ4" && ./build.sh )
else
  cat <<EOF
    SKIPPED — Qt 6.8.3 is not installed (the GUI is the only part that needs it).
    The server + plugin above are built and you can already test headless via the CLI.
    To build the GUI, do this one-time setup, then re-run ./build.sh:

      sudo apt-get install -y docker.io libva-dev libdrm-dev
      pip install aqtinstall
      aqt install-qt linux desktop 6.8.3 linux_gcc_64 \\
        --modules qtcharts qtwebsockets --outputdir "$ROOT/PJ4/.qt"
EOF
fi

echo
echo "==> build complete.  Next:  ./run.sh   (starts Minio + synthetic data + the server)"
