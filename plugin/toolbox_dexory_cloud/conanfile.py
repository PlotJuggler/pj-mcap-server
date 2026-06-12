import os

from conan import ConanFile

# Single source of truth for the plotjuggler_sdk version: the SDK_VERSION file at the
# repo root (read live), shared by the root recipe and every plugin recipe. Edit it with
# scripts/bump_core_version.py. The cursor-aware query-assist needs the caret dialog SDK
# (onCodeChangedWithCursor / codeChanged(code,cursor) / codeCursor) that landed in 0.5.1,
# which the repo-wide pin (now 0.6.0) comfortably satisfies.
_SDK_VERSION = (
    open(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir, "SDK_VERSION"))
    .read()
    .strip()
)


class ToolboxDexoryCloudConan(ConanFile):
    name = "toolbox_dexory_cloud"
    version = "0"
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps", "CMakeToolchain"
    requires = (
        f"plotjuggler_sdk/{_SDK_VERSION}",
        "gtest/1.17.0",
        "nlohmann_json/3.12.0",
        "lua/5.4.6",
        "sol2/3.5.0",
        # fmt required transitively by plotjuggler_sdk's pj_plugins
        "fmt/12.1.0",
        # WS+Protobuf client-core transport. libprotobuf must come from Conan
        # (NOT the system protoc 3.21) so the generated C++ matches the linked
        # runtime; versions pinned to match parser_protobuf / foxglove_bridge.
        "protobuf/6.33.5",
        "ixwebsocket/11.4.6",
        # Session/streaming (Slice 2): ZSTD batch-body + per-message decode, and
        # LZ4 inbound-decode (the v1 server never emits LZ4, but the client must
        # decode it per spec §6.4). Pinned to match data_load_mcap, which links
        # the same zstd::libzstd_static + LZ4::lz4_static targets.
        "zstd/1.5.7",
        "lz4/1.10.0",
    )
    # Build-context protobuf so the conan protoc (6.33.5) lands on the build
    # PATH and protobuf_generate()'s `find_program(protoc)` resolves to it,
    # NOT the system /usr/bin/protoc (3.21) — whose generated C++ is ABI-
    # incompatible with the libprotobuf 6.x headers we link against.
    tool_requires = ("protobuf/6.33.5",)
    default_options = {
        "*:shared": False,
        "lua/*:compile_as_cpp": True,
    }
