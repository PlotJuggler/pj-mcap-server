#!/usr/bin/env bash
# ensure-sdk.sh — make the plotjuggler_sdk Conan package available in the local
# Conan cache. Mirrors pj-official-plugins' scripts/ensure_core.sh, minus the
# JFrog remote and submodule tiers (this repo has neither):
#
#   1. Already in the local cache (dev box / warm CI cache) -> done.
#   2. Otherwise clone the PUBLIC PlotJuggler/plotjuggler_sdk repo at tag
#      v<SDK_VERSION> and `conan create` it (cold-CI path).
#
# Single source of truth for the version: plugin/SDK_VERSION (0.20.0 as of
# 2026-07-29). Upstream tag v0.20.0 is the squash merge of the descriptor-import
# v1 ABI branch (SDK PR #160, commit 4e7e14d) — the dev-box package is conan
# create'd from the same tag (plotjuggler_sdk-cloud worktree .worktrees/v0.20.0),
# so CI and the dev box build the same SDK.
#
# Env overrides:
#   PJ_SDK_GIT_URL — alternate SDK git URL (e.g. a fork).
#   BUILD_TYPE     — Conan build_type (default Release).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SDK_VERSION="$(tr -d '[:space:]' < "${REPO_ROOT}/plugin/SDK_VERSION")"
REF="plotjuggler_sdk/${SDK_VERSION}"
SDK_GIT_URL="${PJ_SDK_GIT_URL:-https://github.com/PlotJuggler/plotjuggler_sdk.git}"
# -s:b compiler.cppstd=20: build-context tools (e.g. protobuf's protoc in the
# plugin build that follows) need C++17+; MSVC's detected build profile
# defaults to cppstd=14 and fails validate() without it. Harmless on gcc/clang.
SETTINGS=(-s build_type="${BUILD_TYPE:-Release}" -s compiler.cppstd=20 -s:b compiler.cppstd=20)

# `conan cache path` errors when the recipe is truly absent; `conan list | grep`
# false-positives (it echoes the queried ref in its "not found" output).
if conan cache path "${REF}" >/dev/null 2>&1; then
  echo "ensure-sdk: ${REF} already present in the local Conan cache"
  exit 0
fi

workdir="$(mktemp -d)"
trap 'rm -rf "${workdir}"' EXIT

echo "ensure-sdk: cloning plotjuggler_sdk v${SDK_VERSION} from ${SDK_GIT_URL}"
git clone --branch "v${SDK_VERSION}" --depth 1 "${SDK_GIT_URL}" \
  "${workdir}/plotjuggler_sdk"

# Explicit `plotjuggler_sdk/*` build pattern builds the recipe we just cloned;
# --build=missing covers its (small, Qt-free) dependency closure.
conan create "${workdir}/plotjuggler_sdk" --version "${SDK_VERSION}" \
  "${SETTINGS[@]}" --build="plotjuggler_sdk/*" --build=missing

echo "ensure-sdk: built ${REF} from source"
