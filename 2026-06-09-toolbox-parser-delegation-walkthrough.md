# Why the Dexory Cloud toolbox cannot delegate parsing to parser_ros — a guided source walkthrough

**Date:** 2026-06-09
**Audience:** anyone who wants to *understand* (not just accept) why the toolbox plugin
currently decodes ROS2/CDR in-plugin, why tf/pointcloud topics come out as flat
timeseries instead of 3D-scene items, and why the fix must be host-side.

All paths cite the pristine tree `/home/gn/ws/PJ4`; the vendored copy under
`<repo>/PJ4` is line-identical. Every line number below was verified on 2026-06-09.

**How to read this document:** it is a reading order. Each "stop" names a file,
the functions to read in it, explains them step by step, and states the one fact
that stop proves. The stops chain into a proof in three acts:

```
ACT 1  The toolbox is never handed the parser-dispatch service ("pj.runtime.v1")
ACT 2  The escape hatch (push raw bytes as object topics) dies at render time
ACT 3  Running parser_ros inside the plugin yourself doesn't help either
```

---

## Two foundational ideas (read first)

### 1. Plugins talk to the host through C-ABI "fat pointers"

A plugin never holds a host C++ object. It holds a `{void* ctx, vtable*}` pair —
a struct of plain C function pointers plus an opaque context. The SDK wraps each
pair in a typed C++ "view" (`ToolboxHostView`, `DataSourceRuntimeHostView`, ...)
that just forwards to the function pointers:

```
   plugin (.so)                    │ C ABI │            host (PJ4 app)
                                   │       │
  ToolboxHostView ──────────────►  vtable* ─────────►  DatastoreToolboxHostState
   .appendRecord(...)              │  fn   │            (real implementation,
   .registerObjectTopic(...)       │ ptrs  │             owns DataEngine refs)
                                   │       │
```

Consequence: **what a plugin "can do" is exactly "which vtables the host handed
it"** — nothing more. The entire question of this document reduces to *what gets
registered for whom*.

### 2. Two stores: scalars vs objects

```
                       ┌────────────────────────────────────────────┐
                       │                 SESSION                    │
                       │                                            │
   numbers ──────────► │  DataEngine                                │
   (timeseries)        │   └── datasets → topics → numeric columns  │
                       │                                            │
   big binary blobs ─► │  ObjectStore                               │
   (tf, pointclouds,   │   └── object topics → timestamped BYTES    │
    images, grids)     │        (opaque! decoded only at RENDER     │
                       │         time, by a parser kept on the      │
                       │         side in SessionManager)            │
                       └────────────────────────────────────────────┘
```

Scalar curves live decoded in the `DataEngine`. 3D-capable things (tf,
pointclouds, occupancy grids) live in the `ObjectStore` as **raw bytes**; the 3D
widget re-decodes them on every render through a parser the host registered for
that topic. Keep this split in mind — it is the heart of Act 2.

---

# ACT 1 — what "delegated parsing" is, and why a toolbox never receives it

## Stop 1 — the delegated-ingest API (plugin side)

**File:** `plotjuggler_sdk/pj_base/include/pj_base/sdk/data_source_host_views.hpp`

This header defines the plugin-side view of delegated parsing. Three things to read.

### 1a. `ParserBindingRequest` (line ~90)

```cpp
struct ParserBindingRequest {
  std::string_view topic_name;          // "/tf"
  std::string_view parser_encoding;     // "ros2msg"
  std::string_view type_name;           // "tf2_msgs/TFMessage"
  Span<const uint8_t> schema;           // raw .msg text bytes
  std::string_view parser_config_json;  // optional
};
```

Five fields — everything the host needs to find and configure a parser. Note:
our Dexory plugin already holds all five at decode time (`SessionSchema`
carries name/encoding/data per topic; see `backend_types.hpp:120-147`).

### 1b. `DataSourceRuntimeHostView::ensureParserBinding` (line ~191)

```cpp
[[nodiscard]] Expected<ParserBindingHandle> ensureParserBinding(
    const ParserBindingRequest& request) const {
  if (!valid() || host_.vtable->ensure_parser_binding == nullptr) {
    return unexpected("runtime host is not bound");
  }
  PJ_parser_binding_request_t raw{ /* convert the 5 fields to C structs */ };
  ParserBindingHandle handle{};
  PJ_error_t err{};
  if (!host_.vtable->ensure_parser_binding(host_.ctx, &raw, &handle, &err)) {
    return unexpected(errorToString(err));
  }
  return handle;   // an opaque uint32_t, meaningful only inside THIS host object
}
```

Called **once per topic**. All the heavy lifting is host-side (Stop 6).

### 1c. `pushMessage` (line ~241) — the per-message call

Read this one slowly; the lifetime model is the cleverest part of the SDK.

```cpp
template <typename FetchMessageData>
[[nodiscard]] Status pushMessage(
    ParserBindingHandle handle, Timestamp host_timestamp_ns,
    FetchMessageData&& fetch_message_data) const;
```

1. The plugin does **not** pass bytes. It passes a **closure** that *produces*
   bytes when invoked. From the doc comment in the header:

   > The callable MUST be idempotent — the host may invoke it zero, one, or
   > many times depending on policy and consumer pulls. It MUST be
   > thread-safe: invocations may come from the ingest thread (kEager) or
   > from consumer threads (lazy pulls).

2. The template wraps your lambda into a C struct (`PJ_message_data_fetcher_t`):
   a heap-held copy of the lambda, a `fetchMessageData` trampoline, a `release`
   deleter. Exceptions are caught at the boundary and converted to `PJ_error_t`
   — nothing may throw across the C ABI.

3. The trampoline returns `{data, size, anchor}`:

```cpp
auto pv = fn();                                       // your closure runs
auto* held = new sdk::BufferAnchor(std::move(pv.anchor));  // heap-held shared_ptr
out->data = pv.bytes.data();                          // NO COPY of the bytes
out->size = pv.bytes.size();
out->anchor.ctx = held;
out->anchor.release = +[](void* h) noexcept { delete static_cast<sdk::BufferAnchor*>(h); };
```

   This is the **zero-copy contract**: bytes are never copied; a refcounted
   "anchor" keeps the original buffer (an mcap chunk, a network batch) alive
   for as long as anyone downstream still holds a view into it.

**What Stop 1 proves:** "delegating parsing" = one `ensureParserBinding` per
topic + one `pushMessage` per record. Both live on `DataSourceRuntimeHostView` —
a view a plugin can only have if someone hands it the matching fat pointer.

---

## Stop 2 — services are looked up by name

**File:** `plotjuggler_sdk/pj_base/include/pj_base/sdk/service_traits.hpp` (~79–182)

Host capabilities are distributed as named **services**. Each trait pairs a
string with view/vtable types:

```cpp
struct DataSourceRuntimeHostService {
  static constexpr const char* kName = "pj.runtime.v1";   // ← the magic string
  using View = ::PJ::DataSourceRuntimeHostView;
  // ...
};
```

A plugin's `bind()` receives a `ServiceRegistry` and calls
`services.require<SomeService>()`, which looks the string up in whatever the
host registered for *this plugin instance*. There is no other channel.

**What Stop 2 proves:** the question "can a toolbox delegate parsing?" becomes
literally: *does anyone put `"pj.runtime.v1"` into a toolbox's registry?*

---

## Stop 3 — the crux: what a toolbox is actually given

**File:** `pj_runtime/src/ToolboxRuntimeHost.cpp` (whole file, ~90 lines)

This is the host object created for every toolbox plugin.

### 3a. The constructor (lines 19–30)

```cpp
ToolboxRuntimeHost::ToolboxRuntimeHost(
    DataEngine& engine, ObjectStore& object_store,
    sdk::SettingsBackend& settings, Callbacks callbacks)
    : write_host_(engine, object_store),
      settings_host_(settings),
      callbacks_(std::move(callbacks)),
      runtime_vtable_{
          PJ_TOOLBOX_PLUGIN_PROTOCOL_VERSION,
          sizeof(PJ_toolbox_runtime_host_vtable_t),
          &ToolboxRuntimeHost::onReportMessage,     // ← the ENTIRE runtime
          &ToolboxRuntimeHost::onNotifyDataChanged, // ← vtable: two slots
      },
      runtime_{this, &runtime_vtable_} {}
```

Notice what it does **not** take: no parser catalog, no dataset id, no parser
registrar. The ingredients of parser dispatch are simply absent from this class.

### 3b. `registerServices` (lines 31–35) — the whole answer in four lines

```cpp
void ToolboxRuntimeHost::registerServices(ServiceRegistryBuilder& registry) {
  registry.registerService<sdk::ToolboxHostService>(write_host_.raw());        // pj.toolbox_write.v1
  registry.registerService<sdk::ToolboxRuntimeHostService>(runtime_);          // pj.toolbox_runtime.v1
  registry.registerService<sdk::SettingsStoreService>(settings_host_.view());  // pj.settings.v1
}
```

Three services. `"pj.runtime.v1"` is not among them. A toolbox calling
`services.require<DataSourceRuntimeHostService>()` gets *"service not
registered"*. This is GROUNDED FACT (1), in source.

```
        what a DATA SOURCE gets              what a TOOLBOX gets
   ┌───────────────────────────────┐   ┌───────────────────────────────┐
   │ pj.source_write.v1            │   │ pj.toolbox_write.v1           │
   │ pj.source_object_write.v1     │   │ pj.toolbox_runtime.v1         │
   │ pj.runtime.v1  ◄── PARSER     │   │ pj.settings.v1                │
   │                    DISPATCH   │   │                               │
   │                    LIVES HERE │   │   (pj.runtime.v1: ABSENT)     │
   └───────────────────────────────┘   └───────────────────────────────┘
```

### 3c. The threading idiom (lines 39–88)

`onReportMessage` / `onNotifyDataChanged` show the discipline any extension must
copy: the plugin may call from a worker thread, so the host marshals to the GUI
thread via `QMetaObject::invokeMethod(..., Qt::AutoConnection)`. And
`onNotifyDataChanged` seals buffered writes *before* the catalog rebuild:

```cpp
QMetaObject::invokeMethod(&self->marshaller_, [self]() {
  self->write_host_.flushPending();        // seal buffered rows/objects FIRST
  if (self->callbacks_.on_data_changed) {
    self->callbacks_.on_data_changed();    // THEN rebuild the catalog
  }
}, Qt::AutoConnection);
```

---

## Stop 4 — the plugin side of the same contract

**File:** `plotjuggler_sdk/pj_base/include/pj_base/sdk/toolbox_plugin_base.hpp` (~36–130)

### 4a. `ToolboxRuntimeHostView` (lines ~43–70)

The toolbox "runtime" surface is **two methods**:

```cpp
void reportMessage(ToolboxMessageLevel level, std::string_view message) const noexcept;
void notifyDataChanged() const;
```

Compare with the DataSource runtime view from Stop 1 (progress bars, stop
requests, parser binding, pushMessage). The asymmetry is by design: a toolbox is
a long-lived UI panel; a data source is a one-shot importer.

### 4b. `ToolboxPluginBase::bind` (lines ~104–129)

```cpp
virtual Status bind(sdk::ServiceRegistry services) {
  auto host = services.require<sdk::ToolboxHostService>();        // mandatory
  // ...
  auto runtime = services.require<sdk::ToolboxRuntimeHostService>(); // mandatory
  // ...
  if (auto cm = services.get<sdk::ColorMapRegistryService>())     // optional
    colormap_view_ = *cm;
  if (auto obj = services.get<sdk::ToolboxObjectReadHostService>()) // optional
    object_read_host_view_ = *obj;
  // ...
}
```

Even a rogue toolbox that overrode `bind()` and asked for `"pj.runtime.v1"`
would fail at the registry lookup — Stop 3 showed it was never registered. The
restriction is enforced at the source of truth, not by convention.

---

## Stop 5 — the full inventory of what a toolbox CAN do

**File:** `plotjuggler_sdk/pj_base/include/pj_base/sdk/plugin_data_api.hpp`,
class `ToolboxHostView` (~1012–1250)

Walk the method list:

| Method | What it does | Used today by |
|---|---|---|
| `createDataSource(name)` | makes a dataset; the handle's `.id` **is** a `DatasetId` | one per Dexory download |
| `ensureTopic(source, name)` | scalar topic | `RosDecodeDriver` |
| `appendRecord(topic, ts, fields)` | one row of numbers; fields auto-create | `RosDecodeDriver` |
| `appendArrowStream(...)` | bulk scalar ingest | Mosaico |
| `registerObjectTopic(source, name, metadata_json)` (~1190) | **raw-bytes object topic** | (the Act-2 escape hatch) |
| `pushOwnedObject(topic, ts, bytes)` (~1208) | push opaque bytes | (the Act-2 escape hatch) |

Note the **tail-slot guard** on the newer methods:

```cpp
if (!hasTailSlot(offsetof(PJ_toolbox_host_vtable_t, register_object_topic),
                 host_.vtable->register_object_topic)) {
  return unexpected("toolbox host does not support object topics (older host)");
}
```

The vtable carries a `struct_size`; newer methods live in "tail slots" checked
via `struct_size >= offset + sizeof(fn) && fn != nullptr`. **This is the
ABI-sanctioned way the SDK grows** — old hosts return a clean error instead of
crashing. (Any future fix would ride exactly this mechanism.)

**What Stop 5 proves:** what's *missing* from the list — anything that takes a
schema, returns a parser, or resembles `pushMessage`. The toolbox writes **final
values** (numbers or opaque bytes); it can never hand the host something to
*interpret*.

---

## Stop 6 — what the missing service actually does (host side)

**Files:**
`pj_runtime/include/pj_runtime/DataSourceRuntimeHost.h` (~195–245) — the state
`pj_runtime/src/DataSourceRuntimeHost.cpp:389–558` — `cbEnsureParserBinding`
`pj_runtime/src/DataSourceRuntimeHost.cpp:560–648` — `cbPushMessage`

### 6a. The state (the .h file first)

```cpp
DataEngine& engine_;                       // scalar store
ExtensionCatalogService& catalog_;         // ← THE PLUGIN CATALOG (finds parser_ros)
ObjectStore& object_store_;                // byte store
std::string source_id_;
ObjectTopicParserRegistrar object_topic_parser_registrar_;  // ← REMEMBER THIS CALLBACK
DatasetId dataset_id_;                     // the dataset this import writes into
std::unordered_map<uint32_t, ParserBinding> parser_bindings_;  // per-source handles
```

Every binding handle is only meaningful inside *this* instance. The service is
not a stateless API — it is a **per-import session object**.

### 6b. `cbEnsureParserBinding`, step by step

```
 plugin                          host (DataSourceRuntimeHost)
 ──────                          ───────────────────────────────────────────────
 ensureParserBinding({           1. catalog_.findParserByEncoding("ros2msg")
   topic:"/tf",                       → the LoadedMessageParser for parser_ros
   encoding:"ros2msg",           2. parser_entry->library.createHandle()
   type:"tf2_msgs/TFMessage",         → FRESH parser instance (one per topic)
   schema:<.msg text>})          3. engine_.createTopic(dataset_id_, "/tf")
                                 4. build DatastoreParserWriteHost(engine_, topic)
                                    registry.registerService<ParserWriteHostService>(...)
                                       → this is HOW a parser writes output!
                                 5. parser->bindSchema(type, schema)
                                 6. kind = parser->classifySchema(type, schema)
                                       kNone?            → scalar-only, done
                                       kFrameTransforms? → continue ↓
                                 7. object_store_.registerTopic({
                                      dataset_id_, "/tf",
                                      metadata_json:
                                        {"builtin_object_type":"frame_transforms"}})
                                 8. registry.registerService<ParserObjectWriteHostService>(...)
                                 9. SECOND parser instance, bindSchema again,
                                    object_topic_parser_registrar_(topic_id, parser2)
                                       → lands in SessionManager (Stop 10)
                                10. parser->bind(registry); stash ParserBinding;
       handle  ◄─────────────────   return handle
```

Key lines, verbatim:

```cpp
// (1) lookup — DataSourceRuntimeHost.cpp:398
const LoadedMessageParser* parser_entry =
    self->catalog_.findParserByEncoding(QString::fromUtf8(encoding...));

// (6) classification — :459
const sdk::BuiltinObjectType object_kind = parser->classifySchema(type_name, schema_span);

// (7) the metadata string the UI will read back (Stop 9) — :477
const std::string metadata_json =
    fmt::format(R"({{"builtin_object_type":"{}"}})", sdk::name(object_kind));

// (9) the render-time parser registration — :516-531
auto object_parser = std::make_unique<MessageParserHandle>(parser_entry->library.createHandle());
object_parser->bindSchema(type_name, schema_span);
self->object_topic_parser_registrar_(*object_topic_id, std::move(object_parser));
```

Why a *second* parser instance at step 9? Because the first one's lifetime ends
with the import; the second must outlive it to serve render-time re-decoding
(Stop 11). Step 4 is also worth internalizing: **a parser is itself a plugin
that gets services** — the host manufactures a single-topic write service and
hands it over. A parser without that service has nowhere to put results
(remember this for Act 3).

### 6c. `cbPushMessage`, step by step

```cpp
auto it = self->parser_bindings_.find(handle.id);           // 1. find the binding
// 2. resolve ObjectIngestPolicy — but ONLY for object topics:
const bool is_object_topic = binding.object_topic_id.has_value();
const auto policy = is_object_topic
    ? self->policy_resolver_.resolve(source_id_, topic_name, object_kind)
    : sdk::ObjectIngestPolicy::kEager;   // scalars always eager — a lazy default
                                         // must not silently drop normal curves
```

3. `kPureLazy`: don't even fetch — push a **lazy closure** into the ObjectStore
   that will invoke the plugin's fetcher on first read (this is why the fetcher
   must be idempotent + thread-safe).
4. Otherwise invoke the fetcher now (under `lazy_fetch_mutex_` if the source's
   reader is not safe to call concurrently) to materialize the bytes.
5. **The actual delegation** (line 629):

```cpp
if (auto status = binding.parser->parse(timestamp_ns,
        Span<const uint8_t>(payload.data, payload.size)); !status) { ... }
```

   parser_ros decodes the CDR and writes scalars through the write host built in
   step 6b-4 — including its **specialized handlers** (Imu single-stamp double,
   covariance upper-triangle, RPY...).
6. If it's an object topic, *additionally* park the same bytes in the
   ObjectStore as a lazy closure capturing the anchor ("always-lazy doctrine",
   lines 633–642) so the raw payload is replayable at render time, zero-copy.

**What Stop 6 proves:** one `pushMessage` produces *both* things we want —
flattened scalars AND raw bytes parked for the 3D scene. The toolbox is locked
out of both halves by Stop 3.

---

## Stop 7 — who constructs this machinery, and for whom

**File:** `pj_app/src/FileLoader.cpp`

### 7a. Lines 141–149: the extension gate

```cpp
const QString ext = normalizeExtension(path);
if (ext.isEmpty())   return fail(tr("File has no extension; cannot pick a plugin."));
const auto matches = extensions_.findSourcesForExtension(ext);
if (matches.empty()) return fail(tr("No DataSource plugin handles %1 files...").arg(ext));
```

Loading starts from a file extension. No extension → no plugin → fail. This is
GROUNDED FACT (2): a `FileSourceBase` without `file_extensions` is unreachable,
so "make the toolbox secretly also a file source" is a dead end.

### 7b. Lines ~221–250: the only construction site in the app

```cpp
auto dataset_or = target_engine.createDataset(
    DatasetDescriptor{.source_name = display_name_utf8, .time_domain_id = target_td_id});
const auto dataset_id = static_cast<DatasetId>(*dataset_or);
const PJ_data_source_handle_t source_handle{static_cast<uint32_t>(*dataset_or)};
//                                          ^^^^ handle.id IS the dataset id

// the registrar callback that feeds SessionManager (Stop 10):
object_parser_registrar = [this](ObjectTopicId id, std::unique_ptr<MessageParserHandle> parser) {
  session_.registerObjectTopicParser(id, std::move(parser));
};

DataSourceRuntimeHost ingest_session(
    engine, extensions_, dataset_id, source_handle,
    session_.objectStore(), source->id, /* registrar */ ...);
```

A stack object — created per load, destroyed after import. (The reload-in-place
variant collects parsers and re-registers them under remapped primary ids after
a dataset swap; see `SessionManager.cpp:73-84`.)

**What Stop 7 proves:** `"pj.runtime.v1"` is not a capability the host could
just "switch on" for toolboxes — it is a per-import session object wired with a
dataset, the plugin catalog, the ObjectStore, and a registrar into
`SessionManager`. The toolbox host (Stop 3) was never given any of those
ingredients. *That* is the structural reason. (It also reveals the fix: build
this same object for a toolbox-created dataset — every constructor argument is
already available in the `pj_runtime` layer where `ToolboxRuntimeHost` lives.)

---

# ACT 2 — the escape hatch, and where it dies

The toolbox CAN push raw bytes into the ObjectStore under any metadata it
likes. Let's follow that path until it hits the wall.

```
  toolbox                     ObjectStore                3D widget
  ───────                     ───────────                ─────────
  registerObjectTopic ──────► topic registered           attach():
   metadata_json =            (bytes + metadata,           parserForObjectTopic(id)
   {"builtin_object_type":     NO parser)                    │
    "frame_transforms"}                                      ▼
  pushOwnedObject ──────────► bytes stored               nullptr ──► FAIL
                                                          "no parser for topic_id"
       draggable? YES ✓            renderable? NO ✗
       (Stop 9: metadata           (Stops 10–11: only the DataSource
        drives drag routing)        ingest path registers parsers)
```

## Stop 8 — what the toolbox object API actually stores

**File:** `pj_datastore/src/plugin_data_host.cpp:1226–1265`
(`toolboxRegisterObjectTopic`) and `:1268–1290` (`toolboxPushOwnedObject`)

```cpp
// validate the source: confirms DataSourceHandle.id IS a dataset id
if (impl->core.engine_.getDataset(source.id) == nullptr) { ...fail... }

ObjectTopicDescriptor desc{};
desc.dataset_id    = source.id;
desc.topic_name    = std::string(toStringView(topic_name));
desc.metadata_json = std::string(toStringView(metadata_json));
auto result = impl->object_store.registerTopic(desc);
```

And the descriptor itself (`pj_datastore/include/pj_datastore/object_store.hpp:42`):

```cpp
/// Identity for an object topic: dataset scope + name (unique per dataset) plus
/// opaque metadata_json retained verbatim for callers that interpret bytes.
struct ObjectTopicDescriptor {
  DatasetId dataset_id = 0;
  std::string topic_name;
  std::string metadata_json;
};
```

Three fields. **No parser is created, looked up, or registered. The datastore
layer doesn't even know parsers exist** — parsers live a layer up, in
`pj_runtime`. `toolboxPushOwnedObject` then just copies your bytes into a vector
and `pushOwned`s them. So far so good.

## Stop 9 — draggability is decided by a string the toolbox controls

**File:** `pj_runtime/src/CatalogModel.cpp:75–100` (`objectTypeFromMetadata`)
and ~455–475 (the catalog rebuild loop)

```cpp
const auto metadata = nlohmann::json::parse(metadata_json);
const auto it = metadata.find("builtin_object_type");
// ...
const auto parsed = sdk::parseBuiltinObjectType(it->get<std::string>());
return *parsed;   // e.g. kFrameTransforms
```

The rebuild loop stuffs that into the catalog item:

```cpp
.payload = ObjectTopicPayload{
    .object_topic_id = object_topic_id,
    .object_type     = objectTypeFromMetadata(object_topic.metadata_json),
    ...
},
```

Then the routing chain:

- `pj_app/src/scene_object_classification.h:13` — `is3dSceneObjectType()`:
  `kPointCloud | kFrameTransforms | kOccupancyGrid → true`
- `pj_app/src/MainWindow.cpp:409` — routes a dragged object topic to a
  `scene3d` vs `scene2d` dock based on that predicate
- `pj_app/src/ui/CurveListPanel.cpp:51` — marks the list item 3D-draggable

**What Stop 9 proves:** push CDR bytes with
`{"builtin_object_type":"frame_transforms"}` and the tf topic *shows up
draggable into the 3D scene*. It looks like victory. It is not — keep reading.

## Stop 10 — the render-time parser registry, and its only writers

**Files:** `pj_runtime/include/pj_runtime/SessionManager.h:69–72`,
`pj_runtime/src/SessionManager.cpp:60–125`

```cpp
void registerObjectTopicParser(ObjectTopicId id, std::unique_ptr<MessageParserHandle> parser);
[[nodiscard]] MessageParserPluginBase* parserForObjectTopic(ObjectTopicId id) const;
```

The implementation is defensive in instructive ways:

```cpp
// an invalid REPLACEMENT is ignored (keeps the previous valid parser + warns):
qCWarning(lcSession) << "registerObjectTopicParser: ignoring invalid replacement...";

// each registration gets a FRESH mutex: consumers guarding the old parser
// must not race callers of the new one:
object_topic_parsers_[id.id] = ObjectParserSlot{std::move(parser), std::make_shared<std::mutex>()};
```

Now grep for callers of `registerObjectTopicParser`. There are exactly two:

1. the registrar lambda built in `FileLoader.cpp` (Stop 7), fed from
   `DataSourceRuntimeHost.cpp:516–531` (Stop 6, step 9);
2. `SessionManager.cpp:81` — the dataset-swap path re-registering the *same*
   collected parsers under remapped ids after a reload.

**There is no path from any plugin API into this map.**

## Stop 11 — the 3D consumers refuse parser-less topics

**File:** `pj_scene3D/widgets/src/entities/pointcloud_entity.cpp:360–380`

```cpp
bool PointCloudEntity::attach(const Scene3DEntityContext& ctx) {
  // ...
  parser_       = ctx_.session->parserForObjectTopic(topic_id_);
  parser_mutex_ = ctx_.session->parserMutexForObjectTopic(topic_id_);
  if (parser_ == nullptr) {
    qCWarning(lcPointCloudEntity) << "attach: no parser for topic_id=" << topic_id_.id;
    return false;                       // ← drag accepted, render REFUSED
  }
```

And at render time (~878–919) every frame does:
`store.latestAt(topic_id, t)` → `parser->parseObject(ts, payload)` →
`std::any_cast<PointCloud>` → GL. The widget **never** interprets bytes itself;
the ObjectStore is deliberately a dumb byte vault.

**File:** `pj_scene3D/widgets/src/transform_service.cpp:50–70` (the tf side)

```cpp
for (const auto& topic_id : object_store.listTopics(dataset_id)) {
  auto* parser = session_.parserForObjectTopic(topic_id);
  if (parser == nullptr) {
    continue;                           // ← parser-less topics SILENTLY SKIPPED
  }
  // probe first entry: if parseObject yields FrameTransforms → TF topic,
  // ingest every entry into the dataset's TF buffer
```

**What Stops 10–11 prove:** the escape hatch produces topics that are draggable
(Stop 9) but dead on arrival: pointcloud entities refuse to attach, tf topics
are skipped. The missing ten inches between the toolbox and the 3D scene is
**one entry in `object_topic_parsers_`** — writable only from host code.

---

# ACT 3 — "fine, the plugin will run parser_ros itself"

## Stop 12 — dlopen works; hosting doesn't

**Files:**
`<repo>/PJ4/pj-official-plugins/toolbox_dexory_cloud/tests/ros_decode_parity_test.cpp:119–157`
`pj-official-plugins/parser_ros/ros_parser.cpp` (~126–292)

Our own parity test dlopens the **real** parser_ros from inside our process:

```cpp
// ParserRosFixture::setUp()
lib_ = MessageParserLibrary::load(PJ_ROS_PARSER_PLUGIN_PATH);   // dlopen — works
handle_ = lib_->createHandle();                                  // works
// ... but FIRST it must build a FAKE HOST:
recorder_ = std::make_unique<ParserWriteRecorder>();             // ← fake write service
registry_builder_->registerService<sdk::ParserWriteHostService>(recorder_->raw());
handle_.bindSchema(type_name, schema);                           // works
handle_.parse(ts, payload);                                      // works → writes go
                                                                 //   into the recorder
```

That fake recorder is the tell. As Stop 6 (step 4) showed, **a parser is a
write-only component**: `parse()` returns no data — it pushes results into
whatever `ParserWriteHostService` / `ParserObjectWriteHostService` its host
registered. In `ros_parser.cpp` you can see the dispatch table: the generic
flatten plus the specialized handlers (`parsePointCloud`, `parseFrameTransforms`,
`parseImage`, ...) that produce `ObjectRecord{ts, std::any}` — but those objects
go to the *host's* object write service, and their 3D usefulness comes from the
*host's* SessionManager registration (Stop 10).

So in-plugin parser_ros buys, at best, exactly what rosx_introspection already
provides: decoded values the plugin must write through the toolbox API —
scalars. The two artifacts that make tf/pointclouds 3D-capable (an object topic
wired to the ObjectStore by the ingest path + a parser registered in
`SessionManager`) are both host-internal. dlopen'ing the parser changes nothing.

### Optional contrast reading

- `pj-official-plugins/data_load_mcap/mcap_source.cpp:181–318` — Act 1's API
  used for real by a legitimate DataSource. It fills `ParserBindingRequest`
  from MCAP channel/schema records (note the encoding mapping: channel
  `message_encoding` + schema `encoding` → `parser_encoding`) and pushes each
  message with a fetcher over the mcap chunk:

  ```cpp
  PJ::ParserBindingRequest request{
      .topic_name      = channel_ptr->topic,
      .parser_encoding = parser_encoding,    // "cdr" / "ros2msg" / ...
      .type_name       = schema->name,
      .schema          = schema_bytes,
      .parser_config_json = parser_config_str,
  };
  auto handle = runtimeHost().ensureParserBinding(request);
  // per message:
  runtimeHost().pushMessage(handle, timestamp_ns,
      [fetcher = byte_store_.makeFetcher(it, mv)]() {
        mcap::ByteView v = fetcher();
        return PJ::sdk::PayloadView{Span<const uint8_t>(...), v.anchor};
      });
  ```

  This is precisely the call pattern our `fetch_worker` would adopt the day a
  toolbox can reach the service — and our plugin already holds every request
  field (`SessionSchema.name/.encoding/.data`, topic, both timestamps, raw CDR
  payload).

- `toolbox_dexory_cloud/src/ros_decode_driver.hpp:7–12` — where this entire
  conclusion is recorded as the documented reason the plugin decodes in-plugin
  today.

---

# The proof, compressed

```
1. Delegated parsing = ensureParserBinding + pushMessage on "pj.runtime.v1"   (Stop 1)
2. Services are allocated BY NAME per plugin instance                          (Stop 2)
3. The toolbox host registers exactly 3 services — not that one               (Stops 3–4)
4. The toolbox write API holds only final-value writes (numbers/opaque bytes) (Stop 5)
5. "pj.runtime.v1" is backed by a per-import session object wired with the
   parser catalog, a dataset, the ObjectStore, and a SessionManager registrar  (Stop 6)
6. Only FileLoader ever constructs that object — for data sources only        (Stop 7)
7. Toolbox object topics store opaque bytes + metadata only                   (Stop 8)
8. Metadata makes topics DRAGGABLE...                                         (Stop 9)
9. ...but rendering requires a SessionManager-registered parser, whose only
   writer is the DataSource ingest path                                       (Stops 10–11)
10. In-plugin parser_ros is a write-only component with no host to write to;
    it cannot reach the ObjectStore wiring or the SessionManager registry     (Stop 12)
∴  No plugin-side solution exists. The gap is host-side — and small.
```

# Why the fix is small (sketch, not a plan)

The same reading shows the door is unlocked from the inside:

- Every ingredient `FileLoader.cpp` wires into a `DataSourceRuntimeHost`
  (engine, catalog, object store, registrar, dataset id) is available in the
  `pj_runtime` layer where `ToolboxRuntimeHost` lives (Stops 3, 7).
- A toolbox download is exactly the one-shot import lifecycle the machinery was
  designed for: ingest, destroy the session host, and the render-time parsers
  survive in `SessionManager` — just like file loads (Stops 6–7, 10).
- The vtable **tail-slot** mechanism (Stop 5) exists precisely for adding such
  a capability compatibly: new slots `create_parser_ingest(data_source_handle)`
  / `ingest_ensure_parser_binding` / `ingest_push_message` /
  `release_parser_ingest`, guarded by `struct_size`, degrade cleanly on old
  hosts.

```
            TODAY                                 WITH THE EXTENSION
  fetch_worker                          fetch_worker
      │ raw CDR                             │ raw CDR
      ▼                                     ▼
  RosDecodeDriver (rosx, in-plugin)     ensureParserBinding / pushMessage
      │ flattened scalars only              │ (per toolbox-created dataset)
      ▼                                     ▼
  ToolboxHostView.appendRecord          DataSourceRuntimeHost (host-side)
      │                                     ├── parser_ros scalars  → DataEngine
      ▼                                     │   (specialized Imu/Pose handlers —
  DataEngine only                           │    closes the parity gap)
  (tf = flat numbers,                       ├── raw bytes           → ObjectStore
   nothing in ObjectStore,                  └── parser registration → SessionManager
   nothing 3D-draggable)                          │
                                                  ▼
                                        tf & pointclouds: draggable AND renderable
                                        in the 3D scene; zero parsing in-plugin
```

Estimated scope (from the 2026-06-09 analysis): ~300–500 lines across
`plugin_data_api.h` / `plugin_data_api.hpp` (SDK), `ToolboxRuntimeHost.{h,cpp}`
(host), ABI sentinel test pins, an `SDK_VERSION` bump — plus the plugin-side
migration of `fetch_worker` from `RosDecodeDriver` to the binding calls, after
which rosx_introspection leaves the ingest path entirely.
