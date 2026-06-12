# Design: Revert pj_scene3D SceneViewWidget from QOpenGLWindow back to QOpenGLWidget

**Date:** 2026-06-03
**Branch:** `gor/revert_glwidget`
**Status:** Approved design, pending spec review

## Problem

`pj_scene3D`'s `SceneViewWidget` is currently a native `QOpenGLWindow` embedded in
the dock via `QWidget::createWindowContainer`. It was switched from `QOpenGLWidget`
in commit `dfe8012` ("render via native QOpenGLWindow to kill per-frame UI
recomposite") purely as a performance optimization: a `QOpenGLWidget` renders to an
FBO that Qt composites into the window backingstore, which re-uploaded the entire
raster UI (~21.5 MB) to the GPU every frame the 3D view repainted.

The native-surface approach removed that cost but, because the view is no longer a
real `QWidget`, it caused a series of integration problems and forced workarounds.
**Decision: the performance benefit of the native window does not outweigh the
issues caused by it not being a native widget.** Revert to `QOpenGLWidget`, and in
the same change unwind the glwindow-specific workarounds, restoring standard Qt
patterns.

This reintroduces the per-frame UI recomposite cost; that tradeoff is accepted.

## Approach

**Manual surgical revert**, not `git revert`.

A blind `git revert dfe8012` is rejected: the occupancy-grid work (part 4) and the
TF-panel work landed *after* the glwindow switch and touch the same files, so the
revert would conflict heavily. Worse, the workaround-introducing commit `c9d32ba`
*mixes* glwindow workarounds (the right-click re-inject) with features we want to
**keep** (the permanent `/tf` panel row, the `tracker_time` clamp fix). A blind
revert would drop those.

Instead, use commit `2ad1b7d` (the last `QOpenGLWidget` state) as the **reference
shape** and hand-revert only the glwindow-specific lines, preserving everything
part-4 and TF added on top.

## Changes

### pj_scene3D/widgets — the core swap

`scene_view_widget.h` / `scene_view_widget.cpp`:
- Base class `QOpenGLWindow` → `QOpenGLWidget`; swap the include; constructor takes
  `QWidget* parent`.
- Restore the `palette()` / `changeEvent` light-dark luminance path; drop the
  `QGuiApplication::palette()` fallback that was needed because `QWindow` has no
  `palette()`/`changeEvent`.
- Add a standard `QWidget::contextMenuEvent` (replacing the re-inject hack on the
  dock side).
- Drop the `mouseReleaseEvent` that existed only to clear `active_button_` because a
  `QWindow` grabs the mouse on press and keeps delivering moves. Restore the
  pre-glwindow input handling. (Verify during impl that drag-end is correctly
  handled without it; if a release handler is genuinely cleaner, keep a minimal one
  — but the QWindow-grab rationale goes away.)
- Re-examine the sub-1.0-alpha FBO clear comment: a `QOpenGLWidget` again renders to
  an FBO, so restore whatever pre-glwindow clearing behavior is correct.

`Scene3DDockWidget.h` / `Scene3DDockWidget.cpp`:
- Construct `view_` as a **direct child `QOpenGLWidget`** in the dock layout again;
  delete the `createWindowContainer` call and the `view_container_` member.
- **Remove** the right-click → re-injected `QContextMenuEvent` workaround. A real
  widget's right-click reaches the ADS dock through normal event propagation, so the
  standard Split/Clear context menu works without intervention.
- Restore the **floating-overlay** fixed-frame combo: `frame_overlay_combo_` as a
  child positioned manually in `resizeEvent` via `layoutFrameOverlayCombo()`
  (top-left, over the view). Delete the thin "frame bar" that replaced it.

### pj_app

`Scene3DConfigPanel.*`:
- **Keep** the permanent `/tf` panel row and `appendTfRow()` from `c9d32ba`.
- Remove only the plumbing that fed the right-click workaround, if any lives here
  (verify during impl — the re-inject itself is in `Scene3DDockWidget`).

### Docs

- Fix `pj_scene3D/CLAUDE.md:28-29` — still describes the view as a native
  `QOpenGLWindow` embedded via `createWindowContainer`; change to `QOpenGLWidget`.
- `pj_scene3D/docs/REQUIREMENTS.md:201` already says `QOpenGLWidget` — no change.
- `pj_app/src/main.cpp` `AA_ShareOpenGLContexts` and its comment already target
  `QOpenGLWidget` (needed so GL resources survive ADS reparent/float). **Keep.**

## What is explicitly preserved

- All part-4 occupancy-grid rendering.
- The TF panel `/tf` row and visibility toggle (`setTfVisible`, `tfPresent`, etc.).
- The `clampTrackerTimeToRanges` zero-span-grid fix and its unit test.
- `AA_ShareOpenGLContexts` in `main.cpp`.

## Verification

Build, run (`./build.sh` then `./run.sh`), load a 3D scene containing pointcloud +
TF + occupancy grid, and confirm:
- Scene renders correctly.
- Right-click on the 3D view shows the standard ADS dock Split/Clear menu.
- Orbit/drag ends cleanly — no stuck mouse grab blocking clicks elsewhere.
- Floating or splitting the dock keeps the scene (context sharing intact, no blank
  view).
- Fixed-frame overlay combo appears top-left over the view and switches frames.
- Light/dark theme luminance fallback still works.

## Out of scope

- Re-addressing the per-frame UI recomposite cost by other means (partial update,
  damage regions). The cost is accepted; not pursued now.
- Any new "debug logs" investigation unless the symptom resurfaces after the revert.
