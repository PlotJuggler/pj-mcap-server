// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// D11 plugin-load smoke test (HERMETIC): dlopen the built toolbox .so the way the
// PlotJuggler host does and assert its two extern-"C" entry vtables resolve. The
// one Dexory Cloud .so exports BOTH a toolbox vtable and a borrowed dialog vtable
// (dexory_cloud_toolbox.cpp: PJ_TOOLBOX_PLUGIN + PJ_DIALOG_PLUGIN); the host's
// ToolboxLibrary resolves "PJ_get_toolbox_vtable" + "PJ_get_dialog_vtable" via
// dlsym (see plotjuggler_sdk pj_plugins/src/toolbox_library.cpp). We mirror that
// resolution with raw dlfcn — both symbols must exist and return a non-null
// vtable pointer.
//
// We deliberately do NOT call any factory create()/widget code: dlopen pulls in
// the dialog's Qt deps, and constructing a dialog without a QApplication would be
// unsafe. Resolving the entry symbols (the host's first contact) is the contract
// this test pins. The .so path arrives via the DEXORY_PLUGIN_SO_PATH compile
// definition (set from $<TARGET_FILE:...> in CMakeLists), exactly like the ROS
// parity test passes PJ_ROS_PARSER_PLUGIN_PATH.

#include <gtest/gtest.h>

#include <dlfcn.h>

namespace {

// The two extern-"C" entry symbols the host resolves. Their return types are
// opaque to this test (we only assert non-null), so plain function-pointer
// typedefs to `const void*` suffice — no SDK headers needed.
using VtableFn = const void* (*)();

}  // namespace

TEST(DexoryCloudPluginLoad, ResolvesToolboxAndDialogVtables) {
  // RTLD_NOW surfaces any unresolved symbol immediately (a broken .so fails here
  // rather than at first symbol use); RTLD_LOCAL keeps the dialog's Qt symbols
  // from leaking into the global namespace.
  void* handle = dlopen(DEXORY_PLUGIN_SO_PATH, RTLD_NOW | RTLD_LOCAL);
  ASSERT_NE(handle, nullptr) << "dlopen(" << DEXORY_PLUGIN_SO_PATH << ") failed: " << dlerror();

  // Boot-level ABI version symbol the host loader checks first.
  dlerror();  // clear
  void* abi = dlsym(handle, "pj_plugin_abi_version");
  EXPECT_NE(abi, nullptr) << "pj_plugin_abi_version not exported: " << dlerror();

  // Toolbox vtable entry.
  dlerror();
  auto get_toolbox = reinterpret_cast<VtableFn>(dlsym(handle, "PJ_get_toolbox_vtable"));
  ASSERT_NE(get_toolbox, nullptr) << "PJ_get_toolbox_vtable not exported: " << dlerror();
  EXPECT_NE(get_toolbox(), nullptr) << "PJ_get_toolbox_vtable() returned null";

  // Borrowed dialog vtable entry (the same .so also exports the dialog).
  dlerror();
  auto get_dialog = reinterpret_cast<VtableFn>(dlsym(handle, "PJ_get_dialog_vtable"));
  ASSERT_NE(get_dialog, nullptr) << "PJ_get_dialog_vtable not exported: " << dlerror();
  EXPECT_NE(get_dialog(), nullptr) << "PJ_get_dialog_vtable() returned null";

  dlclose(handle);
}
