// Copyright 2026 Davide Faconti
// SPDX-License-Identifier: MIT
//
// Renders the REAL dialog and asserts the topic table's emitted widget_data.
//
// This exists because of a shipped bug: "All" mode expressed inertness as
// wd.setEnabled("topicTable", false), and the host applies that key as a
// whole-widget QWidget::setEnabled (pj_dialog_host/src/widget_binding.cpp:1132),
// which disables the widget's SCROLLBARS and wheel handling too. A 174-topic
// list became unbrowsable: only the first screenful was reachable and the
// scrollbar was painted but dead.
//
// The correct mechanism was already in use two lines away: disabled_rows greys
// items per-row via setItemEnabled (widget_binding.cpp:481-486), clearing
// ItemIsEnabled|ItemIsSelectable while the widget itself stays live.
//
// No test could have caught the original bug: widget_data() is ~800 lines and,
// until 2026-08-09, nothing rendered it. These assertions are the beachhead for
// that layer, not a complete render contract.
#include <gtest/gtest.h>

#include <nlohmann/json.hpp>
#include <string>

#include "mcap_cloud_dialog.hpp"
#include "vocab_select.hpp"
#include "test_support_env.hpp"

namespace {

using mcap_cloud_test::HermeticEnv;

nlohmann::json renderDialog(mcap_cloud::McapCloudDialog& dialog) {
  return nlohmann::json::parse(dialog.widget_data());
}

// THE regression pin. `enabled=false` on a table is never an acceptable way to
// say "inert" in this codebase, because it takes scrolling with it.
TEST(TopicTableRender, TableIsNeverDisabledAsAWholeWidget) {
  HermeticEnv env("mcap-cloud-topic-render-enabled");
  mcap_cloud::McapCloudDialog dialog;

  // Select "All" explicitly: that is the mode the bug lived in, and the dialog
  // starts in Custom, so rendering the default state would pass even with the
  // original `setEnabled(topicTable, !topics_all)` restored (verified by
  // mutation — without this line this test does not catch the regression it is
  // named for).
  dialog.onToggled("radioTopicsAll", /*checked=*/true);

  const auto wd = renderDialog(dialog);
  ASSERT_TRUE(wd.contains("topicTable")) << "the topic table must be rendered every tick";
  const auto& tbl = wd["topicTable"];

  // Absent is fine (no change requested); present-and-false is the bug.
  if (tbl.contains("enabled")) {
    EXPECT_TRUE(tbl["enabled"].get<bool>())
        << "topicTable was disabled as a whole widget. That also disables its "
           "scrollbars and wheel handling (widget_binding.cpp:1132), which made a "
           "174-topic list unscrollable. Express inertness with disabled_rows instead.";
  }
}

// The mode toggle must not reach for setEnabled either: whichever mode the
// dialog starts in, the widget stays enabled and per-row state carries meaning.
TEST(TopicTableRender, ToggleDoesNotDisableTheWidget) {
  HermeticEnv env("mcap-cloud-topic-render-toggle");
  mcap_cloud::McapCloudDialog dialog;

  // radioTopicsAll / radioTopicsCustom are the All|Custom pair (a
  // DualOptionsWidget host-side); onToggled records the mode.
  for (const char* widget : {"radioTopicsAll", "radioTopicsCustom", "radioTopicsAll"}) {
    dialog.onToggled(widget, /*checked=*/true);
    const auto wd = renderDialog(dialog);
    ASSERT_TRUE(wd.contains("topicTable"));
    const auto& tbl = wd["topicTable"];
    if (tbl.contains("enabled")) {
      EXPECT_TRUE(tbl["enabled"].get<bool>())
          << "topicTable disabled after toggling to " << widget
          << " — inertness must come from disabled_rows, never the widget's enabled flag";
    }
  }
}

// The browse gate is what keeps a 43k-row site listing off the wire until the
// user has narrowed to one robot. A regression that lets the gate through on a
// partial selection would re-open exactly the download this feature removed.
// REVIEW DEFECT (Fable, verified): the first version of this test was VACUOUS.
// A fresh dialog is kDisconnected, so the hint reads "Connect to a server…",
// the `find("Select customer")` guard failed, and the EXPECT never ran — the
// exact double-if pattern that lets a test report success without asserting
// anything. Assert on the pure function directly instead: it is what produces
// the pill, and it cannot be short-circuited by dialog state.
TEST(TopicTableRender, GateHintAsksForAllThreeLevels) {
  const std::string hint =
      mcap_cloud::gateHintText(mcap_cloud::GatePhase::kNeedsSelection, /*total_files=*/8781595,
                               /*site_count=*/162);
  ASSERT_FALSE(hint.empty()) << "kNeedsSelection must produce a pill";
  EXPECT_NE(hint.find("customer"), std::string::npos) << hint;
  EXPECT_NE(hint.find("site"), std::string::npos) << hint;
  EXPECT_NE(hint.find("robot"), std::string::npos)
      << "the gate hint no longer asks for a robot: " << hint
      << " — if robot stopped being a required gate level, a whole site's file list "
         "crosses the wire again";

  // And the dialog must actually render that pill when it is in that phase, so
  // the widget name stays wired.
  HermeticEnv env("mcap-cloud-topic-render-gate");
  mcap_cloud::McapCloudDialog dialog;
  const auto wd = renderDialog(dialog);
  ASSERT_TRUE(wd.contains("gateHintLabel"))
      << "the gate pill widget is not rendered at all; the hint text above would never be seen";
}

}  // namespace
