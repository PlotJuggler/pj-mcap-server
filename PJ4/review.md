# Review log — `feat/scene2d-multilayer-dock` (`e576bdb` + fix stack)

**Commit under review:** `e576bdb` "unified multi-layer 2D + 3D scene on the shared base" (parts 8+9+10), vs `main` (`d21d37c`).
**Fix stack applied during review:** `2558710` (clearLayers view re-point — teardown/reload UAF + GL leak), `1fd0f0e` (latched-layer tracker clamp restored, orphaned tracker_time.h removed), `9215752` (IObjectViewer on SceneDockWidget — catalog prune restored), `beab297` (SPDX/MPL headers), `20bee99` (dead entity* signals, routing comments, uint32 cast).

---

## Q1 — Is `is2dSceneObjectType` necessary? Does it create individualism against the other `is*ObjectType()` predicates / ad-hoc if-else?

### The duplication is real — five hand-maintained copies

| # | Site | 3D list | 2D list |
|---|---|---|---|
| 1 | `pj_app/scene_object_classification.h::is3dSceneObjectType` | PointCloud, FrameTransforms, OccupancyGrid | — |
| 2 | `pj_app/...::is2dSceneObjectType` | — | Image, AssetVideo, DepthImage, ImageAnnotations, SceneEntities |
| 3 | `Scene3DDockWidget.cpp` private `is3DSceneTopicType` (`:45`) | **exact duplicate of #1** | — |
| 4 | `Scene3DDockWidget::acceptsObjectType` (`:230`) | PointCloud, OccupancyGrid (narrower by design — TF is config) | — |
| 5 | `Scene2DDockWidget::acceptsObjectType` (`:198`) | — | **exact duplicate of #2** |

Only comments keep these in sync, and they had already drifted (the `kSceneEntities`
"3D wins" comment contradicted the code; fixed in `20bee99`).

### But the host-side predicate is necessary *in function*

At drop time no dock exists yet — the factory must pick the family to construct from
the seed's object type; you cannot virtual-dispatch `acceptsObjectType` into a dock
that doesn't exist. So an instance-free family classifier is legitimate shell policy.
What's wrong is that pj_app hand-copies the list instead of asking the family.

### Root cause of copy #3: a base-class gate bug

`SceneDockWidget::tryAcceptObjectTopic` pre-gates on `acceptsObjectType`
(`scene_dock_widget.cpp:62`), which would reject TF before `addLayer` can consume it
as a config topic (`handleSceneConfigTopic` runs first inside `addLayer`, `:73`). The
base conflates "family handles this type" with "type becomes a render layer", forcing
the 3D dock to override `tryAcceptObjectTopic` with its own wider list. The pre-gate
is redundant — `addLayer` already rejects unsupported types via
`createAndAttachLayer`'s `acceptsObjectType` check.

### Verdict: the if/switch form is fine; the replication and ownership are not

A registry/map would be machinery for two families; the closed compile-time switch is
the right form. The consolidation:

1. **One static classifier per family, owned by the family**:
   `static bool Scene3DDockWidget::handlesObjectType(type)` (3 types) and
   `static bool Scene2DDockWidget::handlesObjectType(type)` (5 types). Instance
   `acceptsObjectType` stays — it answers the narrower render-layer question.
2. **Fix the base gate**: `tryAcceptObjectTopic` just calls `addTopic` (config topics
   accepted via `handleSceneConfigTopic`; unsupported rejected by `addLayer`). The 3D
   `tryAcceptObjectTopic` override and its private `is3DSceneTopicType` both die.
3. **pj_app stops owning lists**: routing becomes
   `Scene3DDockWidget::handlesObjectType(t) ? scene3d : Scene2DDockWidget::handlesObjectType(t) ? scene2d : ""`;
   `CurveListPanel` iconography calls the 3D static; `scene_object_classification.h`
   shrinks to nothing or two one-line forwarders. Dependency direction is fine —
   pj_app already includes and constructs both dock types; the reverse coupling
   (docks consulting pj_app policy) would be wrong.

Net: 5 copies → 1 per family; routing, iconography, drop-accept, and the dock guard
all read the same definition, and the drift-guard test the review asked for becomes
unnecessary.

---

## Q2 — Is `TexturePathFormat` another `Rgba`: a fundamental struct that belongs in a canonical place?

**No — it is the counter-case.** The canonicality test that condemned `Rgba` was:
*does an equivalent canonical concept already exist, and is this semantically that
concept?* `Rgba` duplicated `sdk::ColorRGBA` byte-for-byte. `TexturePathFormat`
(`media_viewer_widget.h:113`, private section) duplicates nothing:

- The canonical pixel vocabulary already exists — `PJ::PixelFormat`
  (`pj_scene2d_core/decoded_frame.h`, 8 values). `TexturePathFormat` is deliberately
  a *different, narrower* concept: a 3-value **shader-path selector** ("which GLSL
  branch consumes the uploaded textures"), a many-to-one projection of PixelFormat
  (all RGB/BGR/mono layouts CPU-convert and land on `kRGBA`).
- Its numeric values are **ABI-pinned** by `yuv_to_rgb.frag:16`
  (`int pixelFormat; // 0 = YUV420P, 1 = NV12, 2 = RGBA`) — an enum-shaped contract
  with one shader, not an enumeration of domain concepts.
- It has exactly one consumer: `MediaViewerWidget` + its own shader. Promoting a
  shader-uniform ABI into `pj_base`/`pj_scene2d_core` would leak a GPU detail into
  the Qt-free, render-free decode layer (REQUIREMENTS §6; shaders are §4.7 widget
  territory) — the inverse of the `Rgba` mistake. If a second consumer of the shader
  ever appears, promote enum+shader together within `pj_scene2d_widgets`, never into
  core.

**Real smells found nearby (action items, not placement):**
1. The `PixelFormat → TexturePathFormat` mapping is implicit and **duplicated**
   between the per-layer upload path (`media_viewer_widget.cpp:585-696`) and the
   legacy single-frame path (`:1101-1205`). Centralize via a private
   `static TexturePathFormat texturePathFor(PixelFormat)` or by folding the two
   upload paths into one.
2. **`kNV12` is declared but never produced** — the shader has the `pixelFormat == 1`
   branch but no upload ever sets `layer.format = kNV12`. Dead, untestable shader
   path: wire NV12 planar upload or mark the value reserved.

---

## Q3 — Why do we need `has_tracker_time_`?

**It is a hand-rolled `std::optional<int64_t>` compensating for a base-class gap.**
`last_tracker_ns_` is a raw int64 defaulting to 0, and 0 is a valid ns timestamp
(0-based synthetic timelines, REQUIREMENTS §4.5) — so the flag disambiguates "no
tracker tick has ever arrived" from "tracker time == 0". Sole consumer: the
composite-rebuild seed path (`syncCompositeTimestamp` → `seedTimestampNs`,
`Scene2DDockWidget.cpp:303-305`): seed the rebuilt composite at the last tick if one
ever arrived, else fall back to the layers' `lastTrackerTimeNs()`, else seed nothing.
Without it, every composite rebuilt before the first tick would decode at a
fabricated `setTimestamp(0)`.

**Why it exists in this form — two stacked smells:**
1. A poor-man's optional split across two members updated in lockstep at two sites
   (`onTrackerTime:128-129`, live-samples lambda `:278-279`) — while
   `seedTimestampNs` already *returns* `std::optional<int64_t>`.
2. The base's clock state cannot answer the question (flagless 0-default int64), so
   the 2D dock shadowed it and bolted a flag onto its copy — the already-flagged
   duplicate-state finding. The base has the same disease masked:
   `registerLayer` (`scene_dock_widget.cpp:140`) unconditionally seeds new layers
   from `last_tracker_ns_` even when no tick ever arrived (hidden because the seed
   clamps into the layer's range).

**Principled fix (kills four findings in one move):** the base owns
`std::optional<int64_t> last_tracker_ns_`; accessor returns the optional. Then the
flag disappears (the optional is the flag), the 2D shadow pair + local
`clampedTrackerNs()` die, the live-reconnect routes through a base setter, the base
stops seeding layers with fabricated times, and `seedTimestampNs` collapses to
`lastTrackerNs().or_else(ask-the-layers)`.

---

## Q4 — Why did `kNanosecondsPerSecond` come back? It was fixed as a std::chrono conversion in a previous MR.

**Regression-by-porting, enabled by the base hoarding its conversion result.**
The fix is real and lives in `SceneDockWidget::onTrackerTime`
(`scene_dock_widget.cpp:246-256`): chrono-based conversion + NaN/inf guard + int64
saturation, with the comment "convert via chrono instead of a hand-rolled 1e9
factor … UB to cast". But the converted value was private — no accessor — so when
`e576bdb`'s new derived docks needed nanoseconds locally (2D composite seeding, 3D
view render time), the conversion was ported from the DELETED
`Media2DDockWidget::onTrackerTime` (`kNsPerSec = 1.0e9` + raw cast, old `:284-285`),
resurrecting the constant and the NaN-UB in both derived docks. Same root cause as
Q3: the base owns the clock but didn't expose it, so derived classes rebuilt private
clock pipelines from old code.

Status: 3D copy removed in `1fd0f0e` (delegates to base + `lastTrackerNs()`); 2D
copy (`Scene2DDockWidget.cpp:32/:128`) still present, dies with the Q3
consolidation. Bonus finding: the chrono fix was never repo-wide — `MainWindow.cpp`
carries TWO independent hand-rolled constants in one file (`kNanosecondsPerSecond`
`:136`, local `kNsPerSec` `:1448`), pre-existing on main.

**Directive adopted:** all ns-per-second conversions use one chrono-derived constant
in a shared constants header (implemented on the fix stack).

---

## Q5 — `using Scene3DEntity = Scene3DLayer;` (+Info/Context aliases): is this correct, and what value does it add?

**Mechanically correct, almost entirely valueless — and two of the three were dead on
arrival.** `using` aliases are perfect type equivalences (the `dynamic_cast` through
the alias in `entityFor` is identical to casting the real type); the one mechanical
trap is that aliases cannot be forward-declared (`class Scene3DEntity;` anywhere
would fail to compile).

Measured consumers (`scene3d_entity.h:101-103`):
- `Scene3DEntity = Scene3DLayer` — **4 uses**: `entityFor()` decl+impl
  (`Scene3DDockWidget.h:78`, `.cpp:377-378`) and one loop variable
  (`scene_view_widget.cpp:168`, which uses BOTH names for the same `entities_`
  vector in one file).
- `Scene3DEntityInfo = Scene3DLayerInfo` — **zero users**.
- `Scene3DEntityContext = Scene3DLayerContext` — **zero users**.
- Bonus: `Scene3DLayerInfo = PJ::SceneLayerInfo` (`:29`) — its only "user" is the
  dead `Scene3DEntityInfo` alias, i.e. a two-hop alias chain giving one struct three
  names, two of them unused; everything else already says `PJ::SceneLayerInfo`.

Unlike `using Media2DDockWidget = Scene2DDockWidget;` (a deliberate shim serving a
real external consumer, MainWindow), these serve nobody external — they freeze the
unfinished Entity→Layer rename into API surface and split the module vocabulary
(4 vs 42 uses). **Resolution: finish the rename — fix the 4 stragglers, delete all
four aliases** (done on the fix stack). The larger vocabulary question (concrete
classes still named `PointCloudEntity`/`OccupancyGridEntity`, file still
`scene3d_entity.h`) is left as an explicit author decision.

*(Q on `makePipelineFor` answered but not logged at user request; the
ns-per-second directive was implemented in `513a94b`: one chrono-derived
`kNanosecondsPerSecond` in `pj_runtime/constants.h`, five hand-rolled copies
removed.)*

---

## Q6 — Why a theme QString at all, and now a `pending_theme_hint_`?

**A stash-and-forward chain overriding a mechanism that already works.** The chain:
`MainWindow:942` pushes `theme_->currentTheme()` (a free-text theme *name*) into the
dock (+ re-push on `themeChanged`) → `Scene3DDockWidget::setThemeHint` stashes it in
`pending_theme_hint_` (the GL view is created lazily and may not exist yet;
`createSceneView` replays the stash) → `SceneViewWidget::setThemeHint`
substring-parses it (`contains("light")`→0, `contains("dark")`→1, else −1).

The punchline is `scene_view_widget.cpp:191`:
`dark_theme = theme_hint_ >= 0 ? (theme_hint_ == 1) : (window_bg.valueF() < 0.5f);`
— the view already derives dark/light from its own palette luminance (ground
truth). The whole chain exists only to override that fallback, and the override is
*less* correct: substring matching classifies a theme named "Twilight" as light;
names without light/dark fall back anyway; and since today's names are exactly
"dark"/"light", the hint always wins, leaving the palette fallback dead/untested —
two mechanisms, each half-alive. The `themeChanged` connect exists to trigger a
repaint Qt already delivers natively via `QEvent::PaletteChange`/`StyleChange`.

`pending_theme_hint_` is the third stash-and-forward middleman found in this MR
(Q3 `has_tracker_time_`, Q4 shadow clock): state parked in the dock because a lazy
child doesn't exist yet.

**Resolution (applied on the fix stack):** delete the entire chain — both
`setThemeHint`s, `pending_theme_hint_`, `theme_hint_`, the MainWindow push/connect —
make the view use palette luminance unconditionally, and repaint on
`PaletteChange`/`StyleChange` in `changeEvent()`. Any future explicit override
should be an enum on a theme service, never a substring-matched QString.

---

## Q7 — What is `frameNameLess`, why is it needed, and should it be a generic `LexLess` in a core module?

**What/why:** a case-insensitive ASCII less over TF frame names
(`Scene3DDockWidget.cpp:58-62`), used once to sort the fixed-frame combo's merged
clusters (TF hierarchy root-groups + entity fallback pseudo-roots) so the list is
deterministic and humanly ordered instead of arrival-ordered/ASCII-cased.

**Finding:** it is a **copy of a domain rule that already lives in core** — the
byte-identical lambda sits in `pj_scene3d_core/src/tf/tf_buffer.cpp:295-296`
(`getFrameHierarchy`'s sibling sort, pinned by `MixedCaseSiblingsSortCaseInsensitively`).
The widget copy exists to make the cluster-level order match the intra-cluster
order; nothing enforces the agreement (same disease as the Q1 type lists).

**On making it a generic `LexLess`:** rejected for two reasons. (1) No reachable
generic home: `pj_base` (the natural vocabulary spot) is the read-only SDK
submodule; `pj_widgets` is Qt-only and `pj_scene3d_core` cannot link it;
`pj_runtime`/`pj_datastore` are not linked by scene3d core. A "generic" LexLess
would in practice still live in `pj_scene3d_core` — generic name, domain-locked
location. (2) The contract that must hold is domain-shaped — "frame names sort the
same everywhere they are shown" — and a generic name loses it in both directions
(a maintainer "improving" LexLess for another caller silently changes frame
ordering; a reader of the widget sort cannot see it must match core's).

**Recommendation (adopted):** expose
`[[nodiscard]] bool frameNameLess(std::string_view, std::string_view) noexcept`
from `tf_buffer.h`, implemented once (a generic ci-less internally is fine), used
by both core's sibling sort and the dock's cluster sort; delete the widget copy.
The existing core test becomes the contract for both layers. If a third, unrelated
consumer of case-insensitive std::string ordering ever appears, lift a generic
comparator into `pj_base` via the SDK process and have `frameNameLess` delegate —
promotion with evidence, not in advance. Optional follow-up: numeric-aware natural
ordering (`link2 < link10`), implemented once in core so both levels inherit it.
