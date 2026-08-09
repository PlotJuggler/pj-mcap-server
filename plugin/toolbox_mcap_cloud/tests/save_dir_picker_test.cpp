// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// HERMETIC pins for the "Browse..." directory chooser on the MCAP export path.
//
// The plugin links ZERO Qt, so the chooser is not opened here: widget_data()
// DECLARES buttonBrowseSaveDir as a folder picker and the HOST runs the actual
// dialog (PanelEngine -> PJ::FileDialog::getExistingDirectory), posting the
// chosen path back through onFolderSelected. Two failure modes are silent and
// therefore worth pinning:
//   - a missing/renamed declaration leaves the button INERT (PanelEngine
//     resolves the picker against the cached previous view, so the key must be
//     re-emitted every render tick, not once);
//   - a widget-name mismatch between the .ui, the declaration and the callback
//     makes the chooser open and then discard the user's choice.
#include <gtest/gtest.h>

#include <nlohmann/json.hpp>
#include <string>

#include "mcap_cloud_dialog.hpp"
#include "test_support_env.hpp"

namespace {

using mcap_cloud_test::HermeticEnv;

// The widget name must agree across three places: ui/mcap_cloud_panel.ui, the
// setFolderPicker() declaration in widget_data(), and the onFolderSelected
// branch. Spelling it once here makes a drift show up as a test failure.
constexpr const char* kBrowseButton = "buttonBrowseSaveDir";

TEST(McapCloudSaveDirPicker, RenderDeclaresTheFolderPicker) {
  HermeticEnv env("mcap-cloud-savedir-declare");
  mcap_cloud::McapCloudDialog dialog;

  const auto wd = nlohmann::json::parse(dialog.widget_data());
  ASSERT_TRUE(wd.contains(kBrowseButton)) << "the browse button is never declared -> inert in the host";
  const auto& entry = wd[kBrowseButton];
  // "folder_picker" (not "file_picker"/"save_file_picker"): the export target
  // is a DIRECTORY — the worker generates the collision-safe filename itself.
  EXPECT_EQ(entry.value("action", ""), "folder_picker");
  // PanelEngine applies button_text verbatim, so an empty string blanks it.
  EXPECT_FALSE(entry.value("button_text", "").empty());
}

TEST(McapCloudSaveDirPicker, SelectionUpdatesTheExportDirectory) {
  HermeticEnv env("mcap-cloud-savedir-select");
  mcap_cloud::McapCloudDialog dialog;

  ASSERT_TRUE(dialog.onFolderSelected(kBrowseButton, "/tmp/pj-cloud-export"));
  // Observable through the render tick: the line edit the user reads must carry
  // the chosen path (this is also what saveConfig() later persists).
  const auto wd = nlohmann::json::parse(dialog.widget_data());
  EXPECT_EQ(wd["saveDirectory"].value("text", ""), "/tmp/pj-cloud-export");
}

TEST(McapCloudSaveDirPicker, CancelledChooserLeavesTheDirectoryUntouched) {
  HermeticEnv env("mcap-cloud-savedir-cancel");
  mcap_cloud::McapCloudDialog dialog;

  ASSERT_TRUE(dialog.onFolderSelected(kBrowseButton, "/tmp/pj-cloud-keepme"));
  // An empty path is how a cancelled chooser reports back; it must NOT clear a
  // previously configured directory (that would close the Download gate).
  EXPECT_FALSE(dialog.onFolderSelected(kBrowseButton, ""));

  const auto wd = nlohmann::json::parse(dialog.widget_data());
  EXPECT_EQ(wd["saveDirectory"].value("text", ""), "/tmp/pj-cloud-keepme");
}

TEST(McapCloudSaveDirPicker, ForeignWidgetIsNotHandled) {
  HermeticEnv env("mcap-cloud-savedir-foreign");
  mcap_cloud::McapCloudDialog dialog;

  ASSERT_TRUE(dialog.onFolderSelected(kBrowseButton, "/tmp/pj-cloud-mine"));
  // Some other plugin widget's folder event must fall through (return false) so
  // the host can route it, and must not hijack the export directory.
  EXPECT_FALSE(dialog.onFolderSelected("someOtherButton", "/tmp/not-mine"));

  const auto wd = nlohmann::json::parse(dialog.widget_data());
  EXPECT_EQ(wd["saveDirectory"].value("text", ""), "/tmp/pj-cloud-mine");
}

}  // namespace
