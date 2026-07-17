# S3 Use Case — AWS Staging Bucket — Test & Benchmark Notes

**Working notes (2026-06-22)** for using the real S3-use-case staging bucket to test and
benchmark the **catalog migration** (see [`auryn-catalog-migration-plan.md`](auryn-catalog-migration-plan.md)
+ [`catalog-vocabulary-rpc.md`](catalog-vocabulary-rpc.md)). Gathered by direct
exploration with the AWS CLI + `~/Apps/mcap-linux-amd64`. **All numbers are real,
measured.** Treat sizes/counts as point-in-time (the lake grows).

## 0. Connect

```bash
aws s3 ls s3://dexory-data-offload-staging-bucket/    # default profile already has read access
```
The repo already wires this up: `./run.sh --dexory_aws` → `config.dexory-staging.yaml`,
server on `:8084`, `AWS_PROFILE=dexory-staging`. The GCE/ADC deploy-smoke for the real
bucket is [`gce-deploy-smoke.md`](gce-deploy-smoke.md).
**Cost/safety:** never `aws s3 ls --recursive` the whole bucket (one robot alone is
762 files / 46 GiB); always scope to a `customer=…/…/source=…/date=…/` prefix. Never
download the `.rrd`/`.db`/`.zip` files or the ≥1 GB MCAPs in tests.

## 1. Bucket shape (Hive) + dimension cardinality

Strict Hive: `customer=<c>/customer_site=<site>/robot=<r>/source=<s>/date=<d>/<file>`
— **plus one non-Hive top-level prefix `_lakeview_index/`** (a single, *live-rewritten*
`.snap` ~19.8 MB; filename changes every few seconds → **skip by prefix, never catalog**).

The lake is **small in dimension cardinality** — every level is ≤2 digits, so the
`GetVocabulary` "≤99 per level, ship whole tree upfront" assumption holds with margin:

| Dimension | Count | Notes |
|---|---|---|
| customers | **8** | cj-logistics, dexory, dhl, idlogistics, maersk, odw, samsung, yusen |
| customer_sites | **16** | dexory 4 (nashville, **nashvillee**, vnv-u9, wallingford), samsung 4, dhl 2, maersk 2, rest 1 |
| robots | **~25 names / 27 dirs** | all `arri-<N>` except `test-robot-0`; cross-site reuse (arri-209 in nashville+nashvillee, arri-227 in vnv-u9+wallingford) |
| sources | ~7 real | only 2 hold MCAP (below) |

- **Largest fan-out:** `dexory/wallingford` = 9 robots; most sites have **1** robot.
- **Heavily skewed:** dexory dominates; 5/8 customers are 1-site/1-robot. Benchmarks
  must **not** assume a balanced distribution.
- **Vocab test gold:** pin exact expected sets (8 / 16 / 25). The **`nashville` vs
  `nashvillee` typo** is a built-in distinct-value/normalization test — surface both,
  never auto-merge. Cross-site robot reuse proves the catalog must key on the **full
  path**, not the bare robot name.

## 2. Where MCAP lives

**Only `source=ros-bags/` and `source=low-fat-bags/` hold `.mcap`.** The other sources
are non-MCAP and **must be skipped** (filter on the `.mcap` suffix, never download these):

| source | content | skip? |
|---|---|---|
| `ros-bags` | `.mcap` — **the primary corpus** | catalog |
| `low-fat-bags` | `.mcap` — reduced-topic, **present on only some robots** | catalog |
| `deployment-databases` | `.db` ~27–36 MB | skip |
| `rerun` | `.rrd` ~50–64 MB | skip |
| `risc-snapshots` | `.zip` ~42–54 MB | skip |
| `logs` | `.gz` | skip |

- **Source set varies per robot** — `dhl/lebanon/arri-110` and `samsung/coppell-a59`
  have **no** low-fat-bags; some robots have only `rerun` (zero MCAP). The builder must
  not assume a fixed source set.
- `full-fat-bags` appears on a couple of dexory robots — **content unconfirmed**; probe
  before assuming MCAP.

## 3. The `ros-bags` corpus (PRIMARY)

**⚠ The `ros-bags` directory mixes FOUR filename families** — do not key tests on one
name or size:

| Family | Shape | Health |
|---|---|---|
| `rosbox_<date>_<time>.mcap` | full continuous recording, **~21 MB** (older generation) | healthy, summarized |
| `<n>_continuous_<date>.mcap` | full continuous recording, **~65–90 MB**, 20-min split, 1.28M msgs, 150+ ch | healthy, summarized (the dominant healthy corpus) |
| `<n>_mini_snapshot_<date>.mcap` | **~30–160 KB**, usually **0 messages** | **mostly MALFORMED** → quarantine fixture (§7) |
| `<n>_full_snapshot_<date>.mcap` | tiny–**1.08 GB** | mixed: some empty, one reached **1.08 GB** |

> **For the "focus on rosbox_*" request:** `rosbox_*` and `*_continuous_*` are the same
> *kind* of file (full continuous recordings, different writer generations); together
> they are the healthy primary corpus. `rosbox_*` exists in real quantity (e.g. 76 in a
> single arri-182 date dir) but coexists with `continuous`/`snapshot` files in the same
> directory. Target the **continuous-recording corpus** (both names); exclude snapshots.

**Ground-truth of one real `*_continuous_*` file** (use to assert the builder's extracted
catalog row + as a round-trip target):
```
key: customer=dexory/customer_site=wallingford/robot=arri-156/source=ros-bags/
     date=2026-06-01/231_continuous_2026_06_01-00_06_43.mcap
size 90,361,220 B | 1,279,860 msgs | exactly 20m00s | profile ros2 | lib libmcap 2.1.3
zstd 467/467 chunks, 346.67 MiB → 65.38 MiB (81%), no chunk overlap, summarized (Statistics present)
152 channels | 50 distinct schemas | message encoding = cdr (all) | schema encoding = ros2msg (all)
2 metadata records named "rosbag2" (~238 KB YAML each, duplicated) | 0 attachments
```
- **Topic variety is high and includes custom vendor packages** — round-trip/parser
  tests must not assume standard ROS2 only: `custom_msgs/{Metric,TowerDriverState}`,
  `arri_nav_action/MissionParameters`, `arri_wire_guide/WireGuideStatus`,
  `roboteq_driver/MotorControllerStatus`, `monitor_msgs/MonitorState`,
  `debris_msgs/DebrisUpdate`, `annotations_consumers/Annotations`, plus `tf2_msgs/TFMessage`,
  `sensor_msgs/PointCloud2`, `nav_msgs/*`, `geometry_msgs/*`, camera `CompressedImage`/`CameraInfo`.
- **High-rate topics** (~100 Hz): joint_states, wire_guide tracks, motor encoders; `/tf` ~75 Hz.
- **Always zstd** in real recordings (no uncompressed/lz4 real samples) — but one ros-bag
  had **1/23 chunks uncompressed** (mixed compression *within* a file): the codec must not
  assume one compression per file.

**`low-fat-bags` (secondary):** reduced nav/segment topic set (13 channels, 10 schemas:
`/tf`, `/odom`, `/map_amcl`, `/arri_segment/*`, PointCloud2). **Size is NOT uniform** — a
2026-05-19 `BLOCK-B` is 5.4 MB / 7,928 msgs / 36.8 s, but a 2026-05-28 `_RISC_` generation
runs **88–132 MB**. Keys can contain **spaces and parentheses** (`…_BLOCK-A-(Slow)_0.mcap`)
→ **URL-encoding test** for the builder/reader/session path. Channel IDs are **non-contiguous**
(1–7 then 12–17); chunk **overlaps** present (max concurrent 2).

## 4. Smoke vs benchmark strategy

| Goal | Input | Why |
|---|---|---|
| **Correctness / round-trip / smoke** | **real** ros-bags + low-fat MCAPs | real Hive keys, real topics, custom schemas, real edge cases — fidelity |
| **Catalog-BUILD benchmark** (auryn builder / indexer) | **real**, large prefix | the builder reads only **summaries** (cheap ranged reads ~1 MB/file), so it *can* target whole robots/sites at lake scale |
| **Streaming THROUGHPUT benchmark** (the ~200 MB/s gate) | **synthetic `genmcap`** (primary) + a bounded real slice (complement) | downloads full bodies → want deterministic, local, reproducible inputs; real WAN data adds noise and cost |

**Bottom line:** real corpus = fidelity + cataloging-at-scale; synthetic = the headline
reproducible throughput number. A small `rosbox_*` count is fine — that's smoke's job;
benchmark scale comes from synthetic and/or the cheap summary-only build benchmark.

## 5. Recommended test slices (exact prefixes)

```
# SMOKE (a few hand-picked files, ~tens of MB — fast download + mcapdiff per CI):
#   one low-fat (clean, 13 topics, 5 MB) for fast logical round-trip:
s3://…/customer=dexory/customer_site=nashville/robot=arri-182/source=low-fat-bags/date=2026-05-19/2026-05-19_15-24-11_BLOCK-B_0.mcap
#   + one rosbox_/continuous full recording for multi-topic/custom-schema coverage (pick a ~21 MB rosbox_ if present).

# CATALOG-BUILD (one robot's ros-bags, one date — multi-file, pagination, warm-start=0):
s3://…/customer=dexory/customer_site=nashville/robot=arri-182/source=ros-bags/date=2026-06-01/   # 24 files

# MEDIUM (one robot, all dates — ~830 .mcap, ~56 GB; build benchmark via summaries, NOT downloads):
s3://…/customer=dexory/customer_site=nashville/robot=arri-182/

# LARGE build benchmark (one customer_site, ~9 robots; est. ~7k MCAP / ~450 GB — summary reads only):
s3://…/customer=dexory/customer_site=wallingford/
```
Whole-bucket order-of-magnitude **estimate** (label as estimate): ~40k–80k MCAP objects,
~3–5 TB — sizes the catalog DB row count and the full-scan benchmark.

## 6. Quarantine / edge-case fixtures (the real value — copy these keys)

Real-world breakage to harden the builder's quarantine/skip logic and the reader's resilience:

1. **Empty / malformed MCAP** (0 messages, `start=end=0`, channels in summary but **no data
   section**, `mcap doctor` FAILS):
   `…/robot=arri-182/source=ros-bags/date=2026-05-29/0_mini_snapshot_2026_05_29-00_42_04.mcap`
   (30,650 B, doctor 23 errors). Builder must catalog it (msg_count=0, guarded time range)
   **or** quarantine it — never crash on epoch-0 / `INT64_MAX` (9223372036854775807) sentinel
   `starting_time` in the rosbag2 metadata.
2. **"doctor errors" ≠ broken:** healthy `*_continuous_*` files report 72–73 doctor "errors"
   that are just **0-message channels declared in the summary**. **`mcap doctor` exit-0 is NOT
   a valid acceptance gate** for these real files.
3. **URL-encoding:** `…/source=low-fat-bags/…/2026-05-19_..._BLOCK-A-(Slow)_0.mcap` (spaces +
   parens). Un-encoded keys 404 / break the WS path.
4. **`_lakeview_index/` skip-by-prefix:** non-Hive, `.snap`, live-rewritten filename — skip by
   top-level prefix, never change-detect.
5. **Typo site:** `dexory/customer_site=nashvillee` (double-e) coexists with `nashville`,
   even shares robot arri-209 — distinct partitions; surface both.
6. **Source-filter:** assert `.db/.rrd/.zip/.gz` under the non-MCAP sources never enter the catalog.
7. **Extreme size spread in one date dir:** 30 KB → **1.08 GB** (`0_full_snapshot_2026_05_28-…mcap`).
   Fixed buffers/timeouts break at one end; the 1 GB file is the chunked-streaming max-size
   stress fixture (do NOT download in routine tests).
8. **Mixed compression within a file** (zstd + uncompressed chunks); **non-contiguous channel
   IDs**; **0-message channels** — codec & topic enumeration must tolerate all three.
9. **Varying source sets:** `dhl/lebanon/arri-110` has no low-fat-bags — tolerate missing sources.
10. **`test-robot-0`** (dexory/wallingford): junk robot with 11 nonstandard source names
    (`bags`, `on-demand`, `sensor-data`, `rerun-snapshots`, …) — builder must tolerate unknown
    sources and catalog only `.mcap`-bearing ones.

## 7. Corrections to earlier assumptions (don't reuse stale numbers)

- ros-bags are **NOT uniformly ~21 MB** — `*_continuous_*` are ~65–90 MB (median ~84 MB),
  `*_full_snapshot_*` up to **1.08 GB**, snapshots down to ~30 KB. `rosbox_*` (~21 MB) is an
  older generation, one of four naming families.
- low-fat-bags are **NOT uniformly 5–7 MB** — a `_RISC_` generation is 88–132 MB.
- **Size cannot infer flavor.** Use MCAP-internal time/stats, not filename or size.
- Two writer libraries appear (`libmcap 2.1.3`, `mcap go v1.8.0`) with differing
  summarization quality. `ros_distro=kilted`, serialization `cdr`, profile `ros2`.

## 8. Quick reference — assertion values to pin

- **Vocabulary:** 8 customers / 16 sites / ~25 robots (exact lists in §1); ≤99 all levels.
- **Continuous ros-bag:** 1,279,860 msgs · 152 channels · 50 schemas · 1200 s · zstd · cdr/ros2msg · summarized.
- **Low-fat BLOCK-B (5.4 MB):** 7,928 msgs · 13 channels · 10 schemas · 36.8 s · zstd · doctor-clean.
- **Empty mini_snapshot (30 KB):** 0 msgs · start=end=0 · doctor-fails → quarantine.
