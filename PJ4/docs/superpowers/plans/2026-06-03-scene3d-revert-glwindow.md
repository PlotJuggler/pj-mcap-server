# Revert pj_scene3D SceneViewWidget from QOpenGLWindow to QOpenGLWidget — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Revert `SceneViewWidget` from a native `QOpenGLWindow` back to an embedded `QOpenGLWidget`, and remove the glwindow-specific workarounds (right-click re-inject, `mouseReleaseEvent` grab-clear, the fixed-frame "bar"), restoring standard Qt patterns.

**Architecture:** Surgical hand-revert of the CURRENT files (not a `git revert` and not a wholesale file copy). Commit `2ad1b7d` is the pre-glwindow reference *shape*; we apply only the glwindow-specific reversions to the current files so that all post-glwindow work (occupancy-grid entity, TF panel `/tf` row, `render_time_` rename, `axes_visible_`, `reorderEntities`, the `ViewParams`/`FrameContext` paint refactor, the `clampTrackerTimeToRanges` fix) is preserved. The host (`pj_plotting/DockWidget`) installs a `QEvent::ContextMenu` event filter on the content widget **and every child** (`findChildren<QWidget*>()`), so once the view is a real `QOpenGLWidget` child, a right-click delivers a native `ContextMenu` event to it and the host shows the standard Split/Clear menu with zero view-side code — exactly the `2ad1b7d` behavior.

**Tech Stack:** C++20, Qt 6.8, OpenGL 4.5 core, CMake+Conan. Build via `./build.sh`, run via `./run.sh`.

**Reference vs preserve — read before editing:**
- The MPL license headers (`// Copyright 2026 Davide Faconti` + `// SPDX-License-Identifier: MPL-2.0`) at the top of every current file are **kept**. `2ad1b7d` predates them; do not delete them when reverting.
- Member/field names that changed AFTER the glwindow switch are **kept at their current names**: `render_time_` (not `tracker_time_`), plus `axes_visible_`, `setAxesVisible`, `reorderEntities`, occupancy entity includes, etc.
- We are NOT restoring the drag-vs-click discrimination (`press_pos_` / `dragged_since_press_`). That machinery existed only to decide when to *synthesize* a context-menu event under glwindow. With a real widget the host filter catches the native `ContextMenu` event directly, so it is dead weight. (Right-drag-while-zooming behavior is a manual-verification checkpoint in Task 3; if it regresses we add a targeted guard then, not pre-emptively.)

---

## File map

| File | Change |
|---|---|
| `pj_scene3D/widgets/include/pj_scene3d_widgets/scene_view_widget.h` | Base `QOpenGLWindow`→`QOpenGLWidget`; ctor `QWidget*`; drop `contextMenuRequested` signal, `mouseReleaseEvent`, `press_pos_`/`dragged_since_press_`; add `changeEvent`. |
| `pj_scene3D/widgets/src/scene_view_widget.cpp` | ctor `QOpenGLWidget(parent)`; `palette()` instead of `QGuiApplication::palette()`; simplify mouse handlers; add `changeEvent`; fix includes. |
| `pj_scene3D/widgets/include/pj_scene3d_widgets/Scene3DDockWidget.h` | Drop `view_container_` + `showViewContextMenu`; add `resizeEvent` + `layoutFrameOverlayCombo`. |
| `pj_scene3D/widgets/src/Scene3DDockWidget.cpp` | View as direct child; restore floating overlay combo + `layoutFrameOverlayCombo` + `resizeEvent`; remove the top bar, `createWindowContainer`, and `showViewContextMenu`; fix includes. |
| `pj_scene3D/CLAUDE.md` | Update `widgets/` description (no longer `QOpenGLWindow`/`createWindowContainer`/re-inject). |

`pj_app/` needs **no** changes (verified: no references to `contextMenuRequested` / `view_container_` / `createWindowContainer`). `pj_scene3D/docs/REQUIREMENTS.md:201` already says `QOpenGLWidget`.

---

## Task 1: Revert SceneViewWidget + Scene3DDockWidget to QOpenGLWidget

These four files must build together (the dock constructs `SceneViewWidget(this)` and drops the `contextMenuRequested` connect, both of which only compile after the view header changes). Edit all four, build once, commit once.

**Files:**
- Modify: `pj_scene3D/widgets/include/pj_scene3d_widgets/scene_view_widget.h`
- Modify: `pj_scene3D/widgets/src/scene_view_widget.cpp`
- Modify: `pj_scene3D/widgets/include/pj_scene3d_widgets/Scene3DDockWidget.h`
- Modify: `pj_scene3D/widgets/src/Scene3DDockWidget.cpp`

### scene_view_widget.h

- [ ] **Step 1: Swap the include.** Replace `#include <QOpenGLWindow>` with `#include <QOpenGLWidget>`.

- [ ] **Step 2: Add a `QEvent` forward declaration.** The current forward-decl block is:

```cpp
class QMouseEvent;
class QWheelEvent;
```

Make it:

```cpp
class QEvent;
class QMouseEvent;
class QWheelEvent;
```

- [ ] **Step 3: Replace the class-doc comment + base class.** The current block is:

```cpp
// Implemented as a QOpenGLWindow (native GL surface) rather than a
// QOpenGLWidget: the latter renders to an FBO that Qt composites into the
// window backingstore, which re-uploads the entire raster UI (~21.5 MB) to the
// GPU every frame the view repaints. A native surface is presented directly by
// the windowing-system compositor, so the UI is not re-textured on 3D updates.
// Embedded in the widget tree via QWidget::createWindowContainer (see
// Scene3DDockWidget).
class SceneViewWidget : public QOpenGLWindow {
```

Replace with:

```cpp
class SceneViewWidget : public QOpenGLWidget {
```

(The two preceding comment lines — "Entities are non-owning — Scene3DDockWidget owns them and is responsible for calling addEntity before they're allowed to render and removeEntity before destruction." — stay.)

- [ ] **Step 4: Constructor parameter type.** Change

```cpp
  explicit SceneViewWidget(QWindow* parent = nullptr);
```

to

```cpp
  explicit SceneViewWidget(QWidget* parent = nullptr);
```

- [ ] **Step 5: Remove the `contextMenuRequested` signal.** Delete this block from the `signals:` section (keep `framesChanged` above it):

```cpp

  // Emitted on a right-button *click* — a press and release with no intervening
  // drag (a right-*drag* still zooms the camera). `global_pos` is in screen
  // coordinates, ready to hand to QMenu::exec. The view deliberately owns no
  // menu: a QOpenGLWindow has no QWidget contextMenuEvent, and menu contents
  // (entities, camera) are Scene3DDockWidget's concern, so the dock builds it.
  void contextMenuRequested(const QPoint& global_pos);
```

- [ ] **Step 6: Fix the protected overrides.** The current block is:

```cpp
  void mousePressEvent(QMouseEvent* event) override;
  void mouseReleaseEvent(QMouseEvent* event) override;
  void mouseMoveEvent(QMouseEvent* event) override;
  void wheelEvent(QWheelEvent* event) override;
```

Replace with (drop `mouseReleaseEvent`, add `changeEvent`):

```cpp
  void mousePressEvent(QMouseEvent* event) override;
  void mouseMoveEvent(QMouseEvent* event) override;
  void wheelEvent(QWheelEvent* event) override;
  void changeEvent(QEvent* event) override;
```

- [ ] **Step 7: Remove the click/drag latch members.** The current block is:

```cpp
  QPoint last_mouse_pos_;
  // Press anchor + drag latch: distinguishes a right-*click* (opens the context
  // menu) from a right-*drag* (zooms). press_pos_ is set on every press; the
  // latch trips once the cursor leaves the platform drag threshold.
  QPoint press_pos_;
  bool dragged_since_press_ = false;
  Qt::MouseButton active_button_{Qt::NoButton};
```

Replace with:

```cpp
  QPoint last_mouse_pos_;
  Qt::MouseButton active_button_{Qt::NoButton};
```

### scene_view_widget.cpp

- [ ] **Step 8: Fix includes.** The current include block is:

```cpp
#include <QGuiApplication>
#include <QMouseEvent>
#include <QOpenGLContext>
#include <QOpenGLFunctions_4_5_Core>
#include <QOpenGLVersionFunctionsFactory>
#include <QPalette>
#include <QSize>
#include <QStyleHints>
#include <QSurfaceFormat>
#include <QWheelEvent>
#include <algorithm>
#include <utility>
```

Replace with (drop `QGuiApplication`, `QSize`, `QStyleHints`; add `QEvent`):

```cpp
#include <QEvent>
#include <QMouseEvent>
#include <QOpenGLContext>
#include <QOpenGLFunctions_4_5_Core>
#include <QOpenGLVersionFunctionsFactory>
#include <QPalette>
#include <QSurfaceFormat>
#include <QWheelEvent>
#include <algorithm>
#include <utility>
```

- [ ] **Step 9: Constructor.** Replace

```cpp
SceneViewWidget::SceneViewWidget(QWindow* parent) : QOpenGLWindow(QOpenGLWindow::NoPartialUpdate, parent) {
  setFormat(make_default_format());
  setMinimumSize(QSize(320, 240));
}
```

with

```cpp
SceneViewWidget::SceneViewWidget(QWidget* parent) : QOpenGLWidget(parent) {
  setFormat(make_default_format());
  setMinimumSize(320, 240);
  setMouseTracking(false);
}
```

- [ ] **Step 10: Theme palette read in `paintGL`.** Replace

```cpp
  // Theme-aware background + grid — see Phase 1 commit (theme-aware
  // background + high-contrast grid color) for the full rationale.
  // QWindow has no palette(); use the application palette for the light/dark
  // luminance fallback (the host normally drives this via setThemeHint).
  const QPalette pal = QGuiApplication::palette();
```

with

```cpp
  // Theme-aware background + grid — see Phase 1 commit (theme-aware
  // background + high-contrast grid color) for the full rationale.
  const QPalette pal = palette();
```

(Leave the `glColorMask` / "sub-1.0 alpha left in the QOpenGLWidget's FBO" comment block unchanged — it already says `QOpenGLWidget` and is correct again.)

- [ ] **Step 11: Simplify `mousePressEvent`.** Replace

```cpp
void SceneViewWidget::mousePressEvent(QMouseEvent* event) {
  last_mouse_pos_ = event->position().toPoint();
  press_pos_ = last_mouse_pos_;
  dragged_since_press_ = false;
  active_button_ = event->button();
}
```

with

```cpp
void SceneViewWidget::mousePressEvent(QMouseEvent* event) {
  last_mouse_pos_ = event->position().toPoint();
  active_button_ = event->button();
}
```

- [ ] **Step 12: Delete `mouseReleaseEvent` entirely.** Remove this whole function:

```cpp
void SceneViewWidget::mouseReleaseEvent(QMouseEvent* event) {
  // A right-button release with no intervening drag is a context-menu click;
  // a right-*drag* already zoomed the camera (mouseMoveEvent) and must not also
  // pop a menu. The dock builds the actual QMenu (see contextMenuRequested).
  if (event->button() == Qt::RightButton && !dragged_since_press_) {
    emit contextMenuRequested(event->globalPosition().toPoint());
  }
  // Clear the drag state on release. As a QOpenGLWidget this was masked (moves
  // only arrived while a button was held); as a native QOpenGLWindow the press
  // grabs the mouse and moves keep arriving, so without this the drag never
  // ends and the surface holds the grab — blocking clicks elsewhere.
  if (event->button() == active_button_) {
    active_button_ = Qt::NoButton;
  }
}
```

- [ ] **Step 13: Remove the drag-latch from `mouseMoveEvent`.** Delete these lines (the block between `last_mouse_pos_` assignment and the `dx/dy` computation):

```cpp
  const QPoint current = event->position().toPoint();
  // Latch a drag once the cursor leaves the platform drag threshold, so a
  // right release past this point is treated as a zoom, not a menu click.
  if (!dragged_since_press_ &&
      (current - press_pos_).manhattanLength() > QGuiApplication::styleHints()->startDragDistance()) {
    dragged_since_press_ = true;
  }
  const QPoint delta = current - last_mouse_pos_;
```

so the head of `mouseMoveEvent` becomes:

```cpp
  const QPoint current = event->position().toPoint();
  const QPoint delta = current - last_mouse_pos_;
```

- [ ] **Step 14: Add `changeEvent` at the end of the TU.** Immediately before the closing `}  // namespace pj::scene3d`, add:

```cpp
void SceneViewWidget::changeEvent(QEvent* event) {
  if (event->type() == QEvent::PaletteChange || event->type() == QEvent::ApplicationPaletteChange) {
    update();
  }
  QOpenGLWidget::changeEvent(event);
}

```

### Scene3DDockWidget.h

- [ ] **Step 15: Add a `protected` section with `resizeEvent`.** Insert immediately before `private slots:`:

```cpp
 protected:
  void resizeEvent(QResizeEvent* event) override;

```

(`class QResizeEvent;` is already forward-declared at the top of the header.)

- [ ] **Step 16: Declare `layoutFrameOverlayCombo`.** In the `private:` method group, directly above the `refreshFrameOverlayCombo();` declaration, add:

```cpp
  // Position the floating combo at the top-left of view_ with a small margin.
  void layoutFrameOverlayCombo();
```

- [ ] **Step 17: Remove the `showViewContextMenu` declaration.** Delete this block:

```cpp
  // Bridge SceneViewWidget::contextMenuRequested (a right-click on the native
  // QOpenGLWindow) into a QContextMenuEvent posted on this content widget, so
  // the host DockWidget's existing context-menu filter shows the standard
  // visualization menu (Split Horizontally / Split Vertically / Clear) — the
  // same menu timeseries and 2D widgets get. Keeps pj_scene3D free of any
  // pj_plotting dependency.
  void showViewContextMenu(const QPoint& global_pos);
```

- [ ] **Step 18: Remove the `view_container_` member.** Delete:

```cpp
  // The view is a native QOpenGLWindow; this is the QWidget that embeds it in
  // the layout (QWidget::createWindowContainer). Owns view_'s widget lifetime.
  QWidget* view_container_ = nullptr;
```

(The `frame_overlay_combo_` member and its "Floating fixed-frame combo painted on top of view_ … resizeEvent on the dock places it." comment are already correct — leave them.)

### Scene3DDockWidget.cpp

- [ ] **Step 19: Fix includes.** Remove these two (only `showViewContextMenu` used them):

```cpp
#include <QContextMenuEvent>
#include <QCoreApplication>
```

(`<QFontMetrics>` and `<QResizeEvent>` are already included and are needed by `layoutFrameOverlayCombo` / `resizeEvent`.)

- [ ] **Step 20: Rebuild the constructor body.** Replace the current block (from the "Fixed-frame selector" comment through the overlay-combo connect):

```cpp
  // Fixed-frame selector. It used to float over the GL view, but the view is
  // now a native QOpenGLWindow (see SceneViewWidget) embedded via
  // createWindowContainer — a native surface can't have sibling widgets
  // composited on top, so the combo lives in a thin bar above the view.
  auto* top_bar = new QWidget(this);
  auto* top_bar_layout = new QHBoxLayout(top_bar);
  top_bar_layout->setContentsMargins(4, 2, 4, 2);
  top_bar_layout->setSpacing(0);
  frame_overlay_combo_ = new QComboBox(top_bar);
  frame_overlay_combo_->setFocusPolicy(Qt::ClickFocus);
  top_bar_layout->addWidget(frame_overlay_combo_);
  top_bar_layout->addStretch();
  layout->addWidget(top_bar);

  // The 3D view: a native GL window presented directly by the compositor,
  // wrapped in a container widget so it sits in the layout. This avoids the
  // QOpenGLWidget backingstore path that re-uploaded the whole raster UI
  // (~21.5 MB) to the GPU every repaint.
  view_ = new pj::scene3d::SceneViewWidget();
  view_container_ = QWidget::createWindowContainer(view_, this);
  view_container_->setContentsMargins(0, 0, 0, 0);
  view_container_->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
  layout->addWidget(view_container_, 1);

  connect(view_, &pj::scene3d::SceneViewWidget::framesChanged, this, &Scene3DDockWidget::onAvailableFrames);
  connect(view_, &pj::scene3d::SceneViewWidget::contextMenuRequested, this, &Scene3DDockWidget::showViewContextMenu);
  refreshFrameOverlayCombo();
  connect(frame_overlay_combo_, &QComboBox::currentIndexChanged, this, &Scene3DDockWidget::onOverlayFramePicked);
```

with (view as a direct child; floating overlay combo as a sibling-child of `this`):

```cpp
  view_ = new pj::scene3d::SceneViewWidget(this);
  view_->setContentsMargins(0, 0, 0, 0);
  view_->setSizePolicy(QSizePolicy::Expanding, QSizePolicy::Expanding);
  layout->addWidget(view_);

  connect(view_, &pj::scene3d::SceneViewWidget::framesChanged, this, &Scene3DDockWidget::onAvailableFrames);

  // Floating fixed-frame combo overlaid on the top-left of view_. Created
  // as a sibling-child of `this` (not parented to view_) so Qt composites
  // it on top of the QOpenGLWidget without the well-known QOpenGLWidget
  // child-widget z-order glitches. Positioned manually in resizeEvent.
  //
  // Width is computed per-selection from the *currently selected* item's
  // text (see layoutFrameOverlayCombo) rather than sized to the longest
  // item — that keeps the overlay compact even when one frame name in the
  // dropdown is very long. Dropdown popup width is widened separately so
  // every row is fully readable when expanded.
  frame_overlay_combo_ = new QComboBox(this);
  frame_overlay_combo_->setFocusPolicy(Qt::ClickFocus);
  // Translucent panel so the 3D scene stays partially visible behind the
  // combo box. The popup list retains the platform-default opaque style
  // (QComboBox::QAbstractItemView, not the closed-button container).
  frame_overlay_combo_->setStyleSheet(QStringLiteral(
      "QComboBox { background-color: rgba(255, 255, 255, 200); border: 1px solid rgba(60, 60, 60, 180); "
      "padding: 2px 6px; border-radius: 3px; }"
      "QComboBox::drop-down { border: none; }"));
  frame_overlay_combo_->raise();
  refreshFrameOverlayCombo();

  connect(frame_overlay_combo_, &QComboBox::currentIndexChanged, this, &Scene3DDockWidget::onOverlayFramePicked);
```

- [ ] **Step 21: Re-layout the overlay after a combo refresh.** In `refreshFrameOverlayCombo()`, add a final call so the combo re-sizes to the freshly selected text. The current tail is:

```cpp
  const QString current = currentFixedFrame();
  const int idx = frame_overlay_combo_->findData(current);
  if (idx >= 0) {
    frame_overlay_combo_->setCurrentIndex(idx);
  }
}
```

Make it:

```cpp
  const QString current = currentFixedFrame();
  const int idx = frame_overlay_combo_->findData(current);
  if (idx >= 0) {
    frame_overlay_combo_->setCurrentIndex(idx);
  }
  layoutFrameOverlayCombo();
}
```

- [ ] **Step 22: Add `layoutFrameOverlayCombo` + `resizeEvent` definitions.** Immediately after the `onOverlayFramePicked` function definition, add:

```cpp
void Scene3DDockWidget::layoutFrameOverlayCombo() {
  if (frame_overlay_combo_ == nullptr || view_ == nullptr) {
    return;
  }
  constexpr int kMargin = 8;
  // Combo width tracks the selected item only. Measure the displayed text
  // (including leading indent), add slack for the dropdown chevron + the
  // padding configured in our stylesheet. Width capped to the dock width.
  const QFontMetrics fm(frame_overlay_combo_->font());
  const QString current_text = frame_overlay_combo_->currentText();
  constexpr int kChevronAndPaddingPx = 32;
  const int natural_w = fm.horizontalAdvance(current_text) + kChevronAndPaddingPx;
  const int w = std::min(natural_w, width() - 2 * kMargin);
  const int h = frame_overlay_combo_->sizeHint().height();
  const QPoint view_origin = view_->pos();
  frame_overlay_combo_->setGeometry(view_origin.x() + kMargin, view_origin.y() + kMargin, w, h);

  // Popup list keeps its own width so every row is fully readable when
  // expanded, even when the closed combo is narrow.
  if (auto* view = frame_overlay_combo_->view()) {
    int popup_w = natural_w;
    for (int i = 0; i < frame_overlay_combo_->count(); ++i) {
      popup_w = std::max(popup_w, fm.horizontalAdvance(frame_overlay_combo_->itemText(i)) + kChevronAndPaddingPx);
    }
    view->setMinimumWidth(popup_w);
  }
  frame_overlay_combo_->raise();
}

void Scene3DDockWidget::resizeEvent(QResizeEvent* event) {
  QWidget::resizeEvent(event);
  layoutFrameOverlayCombo();
}
```

- [ ] **Step 23: Delete `showViewContextMenu` definition.** Remove the whole function near the end of the file:

```cpp
void Scene3DDockWidget::showViewContextMenu(const QPoint& global_pos) {
  // The 3D view is a native QOpenGLWindow, so a right-click on it never reaches
  // the QWidget tree as a QContextMenuEvent — which is exactly what the host
  // DockWidget's event filter listens for to show the standard visualization
  // menu (Split Horizontally / Split Vertically / Clear). Re-inject that event
  // on ourselves (the IDataWidget content widget the host installs its filter
  // on), so the 3D scene gets the identical menu to timeseries/2D widgets with
  // no dependency on pj_plotting. If the widget is used un-hosted (demos), no
  // filter is installed and this is simply a no-op.
  QContextMenuEvent ev(QContextMenuEvent::Mouse, mapFromGlobal(global_pos), global_pos);
  QCoreApplication::sendEvent(this, &ev);
}
```

### Build + commit

- [ ] **Step 24: Build.**

Run: `./build.sh`
Expected: configures + compiles to completion, no errors. Watch specifically for: no `QOpenGLWindow` / `contextMenuRequested` / `view_container_` / `showViewContextMenu` unresolved-symbol or unused-variable errors.

- [ ] **Step 25: Run the module's core tests** (they don't exercise the widget, but confirm nothing else broke).

Run: `ctest --test-dir build -R "tf_buffer_test|tf_buffer_hierarchy_test|occupancy_grid_reconstructor_test|tracker_time_test" --output-on-failure`
Expected: all 4 PASS.

- [ ] **Step 26: Commit** (only after the user approves the diff — see repo commit policy; do not commit autonomously).

```bash
git add pj_scene3D/widgets/include/pj_scene3d_widgets/scene_view_widget.h \
        pj_scene3D/widgets/src/scene_view_widget.cpp \
        pj_scene3D/widgets/include/pj_scene3d_widgets/Scene3DDockWidget.h \
        pj_scene3D/widgets/src/Scene3DDockWidget.cpp
git commit -m "revert(pj_scene3d_widgets): render via embedded QOpenGLWidget instead of native QOpenGLWindow

The native QOpenGLWindow surface (dfe8012) removed the per-frame UI recomposite
cost but, by not being a real QWidget, caused integration issues that outweigh
that benefit. Revert SceneViewWidget to a QOpenGLWidget embedded as a direct
child, and unwind the glwindow-specific workarounds:

- Drop createWindowContainer + view_container_; the view is a layout child again.
- Drop the right-click contextMenuRequested re-inject (showViewContextMenu). A
  real widget delivers a native QContextMenuEvent that the host DockWidget's
  event filter already catches, so the standard Split/Clear menu just works.
- Drop the QWindow-grab mouseReleaseEvent and the click-vs-drag latch.
- Restore the floating top-left fixed-frame overlay combo (layoutFrameOverlayCombo
  + resizeEvent) in place of the thin frame bar.
- Restore QWidget palette()/changeEvent theme-luminance fallback.

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Update module docs

**Files:**
- Modify: `pj_scene3D/CLAUDE.md`

- [ ] **Step 1: Rewrite the `widgets/` bullet.** The current text is:

```
- `widgets/` — Qt viewer (`SceneViewWidget`, a native `QOpenGLWindow` embedded
  via `QWidget::createWindowContainer` — *not* a `QOpenGLWidget`; the FBO
  composite path re-uploaded the whole raster UI every repaint), render passes,
  entities, and `Scene3DDockWidget` (an `IDataWidget`). The view has no QWidget
  `contextMenuEvent`, so it detects a right-click and emits `contextMenuRequested`;
  the dock re-injects a `QContextMenuEvent` on itself so the host shows the same
  standard menu (Split Horizontally/Vertically, Clear) as other widgets.
  *Landing incrementally.*
```

Replace with:

```
- `widgets/` — Qt viewer (`SceneViewWidget`, a `QOpenGLWidget` embedded as a
  direct child of `Scene3DDockWidget`), render passes, entities, and
  `Scene3DDockWidget` (an `IDataWidget`). A right-click on the view delivers a
  native `QContextMenuEvent` that the host `DockWidget`'s event filter catches,
  so the 3D scene gets the same standard menu (Split Horizontally/Vertically,
  Clear) as other widgets with no view-side context-menu code. *Landing
  incrementally.*
```

- [ ] **Step 2: Verify no other CLAUDE.md / docs lines still describe the native window.**

Run: `grep -rn "QOpenGLWindow\|createWindowContainer\|contextMenuRequested" pj_scene3D/ pj_app/`
Expected: no matches (the only remaining `QOpenGLWidget` mentions are correct).

- [ ] **Step 3: Commit** (after user approval).

```bash
git add pj_scene3D/CLAUDE.md
git commit -m "docs(pj_scene3D): describe the view as an embedded QOpenGLWidget

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Manual verification in the running app

No automated widget harness exists for this module (the core tests are geometry-only), so the rendered behavior is verified by running the app. See the project memory note "Screenshotting Qt demos on this box" for the X11/`QT_IM_MODULE` setup if a screenshot is wanted.

**Files:** none.

- [ ] **Step 1: Launch.**

Run: `./run.sh` (optionally with `--plugin-dir` pointing at the locally-built official plugins, per `run.sh`'s default).
Expected: app starts, no crash.

- [ ] **Step 2: Load a 3D scene** containing TF + at least one PointCloud + one OccupancyGrid topic, and drop it into a 3D View dock.

- [ ] **Step 3: Verify each item** and record pass/fail:
  - Scene renders (grid, TF axis triads, pointcloud, occupancy grid all visible).
  - **Right-click on the 3D view** shows the standard dock menu (Split Horizontally / Split Vertically / Clear).
  - **Orbit/pan/zoom** with left / shift+left / right / wheel all work and **end cleanly** — after a drag, a normal left-click elsewhere is not swallowed (no stuck mouse grab).
  - **Float or split** the 3D dock (drag it out): the scene keeps rendering (GL resources survive reparenting via `AA_ShareOpenGLContexts`); it does not go blank.
  - **Fixed-frame overlay combo** appears at the top-left *over* the view, lists the frames indented by TF depth, and switching frames re-resolves the scene.
  - **Light/dark theme**: toggle the app theme; the 3D background and grid color track it.
  - **Right-drag-to-zoom checkpoint:** while zooming with a right-drag, confirm whether the context menu pops at drag end. If it does and that's annoying, note it — the fix is a small targeted guard (e.g. `setContextMenuPolicy` + emit on click only), added as a follow-up, not part of this revert.

- [ ] **Step 4: Confirm the original glwindow issue is resolved.** Re-exercise whatever symptom motivated the revert (e.g. the prior glwindow misbehavior under docking/interaction) and confirm it's gone.

- [ ] **Step 5: Report results** to the user (and, per `pj_scene3D/CLAUDE.md`'s "Lessons learned" note, if anything non-obvious surfaced during verification, capture it).

---

## Self-review notes

- **Spec coverage:** base-class swap (T1 s1–s4), right-click workaround removal (T1 s5,s12,s17,s23 + relying on host filter), `mouseReleaseEvent` removal (T1 s12), floating overlay restore (T1 s16,s20–22), `palette()` theme restore (T1 s10,s14), docs (T2), preserved part-4/TF work (explicit "preserve" callouts; no edits touch occupancy/TF/`render_time_`/`reorderEntities`), verification (T3). All spec sections mapped.
- **Type consistency:** field names kept current (`render_time_`, `axes_visible_`); removed names (`press_pos_`, `dragged_since_press_`, `view_container_`, `contextMenuRequested`, `showViewContextMenu`) are removed in *both* declaration and use within Task 1. `layoutFrameOverlayCombo` / `resizeEvent` declared (T1 s15–16) and defined (T1 s22) with matching signatures.
- **No placeholders:** every code step shows exact before/after text.
