# PlotJuggler Cloud Connector — Milestone 1 Live-Demo Runbook

| | |
|---|---|
| **Date** | 2026-06-05 |
| **Scope** | Dexory Milestone 1 PoC — repeatable, convincing live demo |
| **Audience** | Whoever runs the demo (author) + verifier reproducing it |
| **Companion docs** | `2026-06-05-dexory-m1-report.md` (the written M1 report), `2026-06-05-dexory-m1-closure-report.md` (the scale-validation deep dive) |

The proposal (§4) defines the PoC as "done" when *"this demo is repeatable and
convincing."* This runbook is that demo, written so a second person can paste every
command verbatim and see the same output. It covers the GUI path (the cloud Toolbox
inside PlotJuggler v4), the CLI path (`dexory-cloud-cli`, the exact `BackendConnection`
the GUI uses), the round-trip correctness proof (`mcapdiff`), and the ≥100-MCAP scale
demo on a separate, isolated bucket and port.

---

## 0. Conventions and prerequisites

All commands assume the repo root and the Go toolchain on `PATH`:

```bash
export REPO=/home/gn/ws/PJ4_Server_Template/pj-mcap-server
export PATH=$HOME/.local/go/bin:$HOME/go/bin:$PATH
cd "$REPO"
```

Binaries used by this demo (already built; the demo does **not** rebuild C++):

| Binary | Path | Built by |
|---|---|---|
| `pj-cloud-server` | `$REPO/server/bin/pj-cloud-server` | `make build` (Go) |
| `dexory-cloud-cli` | `$REPO/PJ4/pj-official-plugins/build/toolbox_dexory_cloud/Release/toolbox_dexory_cloud/dexory-cloud-cli` | `./build.sh toolbox_dexory_cloud` (C++/Qt) |
| `devprobe`, `mcapdiff` | `$REPO/server/bin/{devprobe,mcapdiff}` | `make build` (Go) |

A convenient shell alias for the CLI for the rest of the runbook:

```bash
export CLI="$REPO/PJ4/pj-official-plugins/build/toolbox_dexory_cloud/Release/toolbox_dexory_cloud/dexory-cloud-cli"
export MCAPDIFF="$REPO/server/bin/mcapdiff"
export DEVPROBE="$REPO/server/bin/devprobe"
```

On-disk ground-truth originals (the canonical bytes the Minio `recordings` bucket was
seeded from — `mcapdiff` compares reconstructions against these):

```
/home/gn/ws/jkk_dataset02/nissan_zala_50_*.mcap   # 8 ROS2 MCAPs
```

**Ports used by this demo:** `:8080` (interactive server for the GUI + CLI demo),
`:8083` + bucket `recordings-scale` (the scale demo only). The smoke harness's `:8081`
is never touched here.

---

## 1. Start Minio and confirm the corpus

```bash
docker compose -f "$REPO/infra/minio/docker-compose.yml" up -d
```

Expected: the `minio` container reaches healthy. Confirm the `recordings` bucket holds
the 8-file corpus (the minio/mc image ships no `grep`, so count on the host):

```bash
docker run --rm --network minio_default --entrypoint sh \
  minio/mc:RELEASE.2024-06-12T14-34-03Z \
  -c 'mc alias set local http://minio:9000 admin password123 >/dev/null 2>&1 && mc ls local/recordings' \
  | grep -c '\.mcap'
```

Expected output:

```
8
```

(Minio console is at `http://localhost:9001`, login `admin` / `password123`.)

---

## 2. Start the interactive server (`:8080`)

```bash
make server-start
```

Expected output:

```
starting pj-cloud-server on :8080 (log /tmp/pj-cloud-server.log)
started (pid <NNNN>)
```

Wait for the indexer's first (cold) scan and confirm health:

```bash
curl -fsS http://localhost:8080/health && echo
grep 'indexer: run complete' /tmp/pj-cloud-server.log | head -1
```

Expected: `/health` answers (e.g. `ok`), and the log shows the cold scan extracting
the whole corpus, for example:

```
... msg="indexer: run complete" scanned=8 new=8 reindexed=0 unchanged=0 failed=0 duration_ms=...
```

Stop it again at the end of the demo with `make server-stop`.

---

## 3. CLI path — browse, topic-filter, stitch, time-range, tags, round-trip

This is the headline M1 deliverable: the reference client that opens a session and
reconstructs the streamed data into a local MCAP. It is the same `BackendConnection`
the GUI Toolbox uses. Point it at the interactive server:

```bash
export DEXORY_CLOUD_URL=ws://localhost:8080
```

### 3.1 Hello / list / topics

```bash
"$CLI" hello
"$CLI" list
"$CLI" topics nissan_zala_50_zeg_1_0.mcap
```

Expected highlights:

- `hello` prints a line containing `server_version`.
- `list` shows 8 sequences, one row per `nissan_zala_50_*.mcap` with time range, size,
  message count, and metadata.
- `topics` for `zeg_1` shows the 6 topics, with `/nissan/gps/duro/imu` at message count
  **14904** and a total of **33670** across the 6 topics.

The 6 topics are:

```
/nissan/gps/duro/current_pose
/nissan/gps/duro/imu
/nissan/gps/duro/mag
/nissan/gps/duro/status_string
/nissan/vehicle_speed
/nissan/vehicle_steering
```

### 3.2 Single-file full download + round-trip proof

```bash
mkdir -p /tmp/m1-demo
"$CLI" download nissan_zala_50_zeg_1_0.mcap --output /tmp/m1-demo/zeg1_full.mcap --json
"$MCAPDIFF" /home/gn/ws/jkk_dataset02/nissan_zala_50_zeg_1_0.mcap /tmp/m1-demo/zeg1_full.mcap
```

Expected: the download reports `"messages_received": 33670` and an EOS COMPLETE; then:

```
OK: logically equal, no over-delivery
```

`mcapdiff` exits 0. Equality is on `(topic, log_time, payload, publish_time, schema
name/encoding/data)` — the design-spec §11 / unified-plan §6 logical-equality gate (see
§7 of the M1 report for the byte-for-byte-vs-logical wording note).

### 3.3 Topic-subset download (whole-file topic filtering — the Dexory M1 gate)

```bash
"$CLI" download nissan_zala_50_zeg_1_0.mcap \
  --topics /nissan/gps/duro/imu,/nissan/vehicle_speed \
  --output /tmp/m1-demo/zeg1_subset.mcap --json
"$MCAPDIFF" --topics /nissan/gps/duro/imu,/nissan/vehicle_speed \
  /home/gn/ws/jkk_dataset02/nissan_zala_50_zeg_1_0.mcap /tmp/m1-demo/zeg1_subset.mcap
```

Expected: `"messages_received": 19417` (14904 imu + 4513 vehicle_speed) and:

```
OK: logically equal, no over-delivery
```

`mcapdiff` filters the original to those two topics, then asserts both no
under-delivery **and** no over-delivery (no record outside the selection).

### 3.4 Stitched multi-file download (≥3 consecutive recordings)

The proposal's headline: select N consecutive recordings, get **one** continuous
logical session. Multiple sequence arguments are stitched, time-ordered, into a single
`OpenFresh`. Three consecutive nissan files:

```bash
"$CLI" download \
  nissan_zala_50_zeg_2_0.mcap nissan_zala_50_zeg_3_0.mcap nissan_zala_50_zeg_4_0.mcap \
  --output /tmp/m1-demo/stitch3.mcap --json
"$MCAPDIFF" \
  /home/gn/ws/jkk_dataset02/nissan_zala_50_zeg_2_0.mcap \
  /home/gn/ws/jkk_dataset02/nissan_zala_50_zeg_3_0.mcap \
  /home/gn/ws/jkk_dataset02/nissan_zala_50_zeg_4_0.mcap \
  /tmp/m1-demo/stitch3.mcap
```

Expected: `"messages_received": 87343` (43301 + 21731 + 22311), EOS COMPLETE; then
`mcapdiff` merges the three originals (concatenate + re-sort by topic,log_time) and
prints:

```
OK: logically equal, no over-delivery
```

A two-file stitch (`zeg_2` + `zeg_3` → 65032) is what `make smoke` step f5 checks
automatically.

### 3.5 Time-range (intra-file window) download

Time-range slicing is **M2 scope for Dexory** (proposal §4 lists intra-file time
windows as out of M1), but the engine is built and demonstrable. The window below is
the middle ~30% of `zeg_1`:

```bash
"$CLI" download nissan_zala_50_zeg_1_0.mcap \
  --time-range 1696577469299761084,1696577514415840735 \
  --output /tmp/m1-demo/zeg1_window.mcap --json
"$MCAPDIFF" --time-range 1696577469299761084,1696577514415840735 \
  /home/gn/ws/jkk_dataset02/nissan_zala_50_zeg_1_0.mcap /tmp/m1-demo/zeg1_window.mcap
```

Expected: `"messages_received": 10098`, EOS COMPLETE, `mcapdiff` exits 0
(`OK: logically equal, no over-delivery`).

### 3.6 Overlap rejection (the stitch safety gate)

Stitching two overlapping/identical spans must be rejected, not silently mis-ordered.
The CLI mirrors the server's authoritative check; a duplicate sequence is a clean usage
error (exit 2):

```bash
"$CLI" download nissan_zala_50_zeg_1_0.mcap nissan_zala_50_zeg_1_0.mcap \
  --output /tmp/m1-demo/dup.mcap ; echo "exit=$?"
```

Expected: an error message and `exit=2`; no output file is written.

### 3.7 Tag editing (M2 scope for Dexory; engine demonstrable)

Tag editing is M2 scope for the Dexory engagement (proposal §4), but the override-tag
engine works end-to-end and overrides persist across re-indexing. Set, observe, unset:

```bash
"$CLI" tag nissan_zala_50_zeg_1_0.mcap --set verified=yes --set demo_marker=m1 --json
# `list --json` is a top-level JSON ARRAY of sequence rows; each row has a
# top-level `name` and a `metadata` map (override tags land in `metadata`).
"$CLI" list --json | python3 -c 'import json,sys; d=json.load(sys.stdin); f=[x for x in d if x["name"]=="nissan_zala_50_zeg_1_0.mcap"][0]; print(f.get("metadata"))'
"$CLI" tag nissan_zala_50_zeg_1_0.mcap --unset verified --unset demo_marker --json
```

Expected: after `--set`, the metadata map contains `verified=yes` and `demo_marker=m1`;
after `--unset` they are gone. (`make smoke` step h additionally proves an override
**survives a forced re-index**.)

---

## 4. GUI path — the cloud Toolbox inside PlotJuggler v4

This is the visual demo. The connector ships as a **cloud Toolbox** (Mosaico-style
non-modal panel) — see the M1 report §2 for why it is a Toolbox and not a DataSource.
Launch PlotJuggler with the vendored plugin:

```bash
# The VENDORED app (carries the vendored host-side changes; its run.sh
# auto-loads the vendored plugins). Do NOT run the pristine /home/gn/ws/PJ4 app.
cd /home/gn/ws/PJ4_Server_Template/pj-mcap-server/PJ4 && ./run.sh
```

Then, with the `:8080` server from §2 running:

1. **Open the Toolbox.** In PlotJuggler, open the Toolbox menu/area and select
   **Dexory Cloud**. A non-modal panel appears (three-panel layout: sequences table /
   topics tree / info).
2. **Connect.** Enter `ws://localhost:8080` in the connection field and connect. The
   sequences table populates with the 8 nissan recordings (name, date, duration, size,
   message count, tags). The connection history dropdown remembers the URI.
3. **Browse + filter.** Two independent filters narrow the table. To match on the
   recording **name**, type a substring into the name **Filter…** box (e.g. `zeg`); the
   table narrows to the `zeg_*` recordings. To filter on **metadata**, use the metadata
   **query** editor with the Mosaico-style key/value DSL (lifted from `toolbox_mosaico`,
   evaluated via the embedded `sol2`/Lua engine). The DSL is metadata-key equality with
   `and`/`or`, e.g.:

   ```text
   topic_count == "6"
   ```

   The query evaluates against each sequence's **metadata** keys (`topic_count`,
   `chunk_count`, `message_count`, `size_bytes`, `start_ns`/`end_ns`, `duration_ns`, any
   override tags such as `verified`). Note the engine is **not** free-form Lua — a
   syntactically invalid query (e.g. a raw Lua expression like `name:find(...)`) is
   ignored and hides no rows (PJ3 validity-gating). Use the name **Filter…** box for name
   matching and the date slider for the time window; the metadata query is for tag/metadata
   predicates.
4. **Edit a tag.** Single-select one recording, click **Edit Tags…**, set
   `verified=yes`, apply. The tag appears in the table's tags column. (Edit Tags is
   single-selection only.)
5. **Multi-select stitched fetch.** Ctrl/Shift-select **three** consecutive recordings
   (`zeg_2`, `zeg_3`, `zeg_4`). The topics panel shows the **union** of their topics;
   the Info panel shows the stitched summary (one continuous time range); the time
   slider spans the union. Pick a subset of topics in the tree (e.g. `imu` +
   `vehicle_speed`), then click **Fetch**. A progress indicator advances; series appear
   in PlotJuggler's tree under the topic names as the data arrives. All 6 ROS2/CDR
   topic types decode in-plugin (see M1 report §2).
6. **Time-range fetch.** Drag the slider to narrow the window before fetching; only the
   in-window messages ingest.
7. **Cancel.** Start a fetch and hit **Cancel** mid-stream; the fetch stops cleanly and
   the panel returns to ready. A subsequent fetch works (a cancelled session's stale
   EOS does not poison the next pull — a live-test-caught fix; M1 report §6).

Once a fetch completes, the series behave like any locally-opened file — transforms,
scripting, and export all work unmodified.

---

## 5. Scale demo — ≥100 MCAPs on an isolated bucket and port (`:8083`)

This demonstrates acceptance criterion 1 (server indexes ≥100 MCAPs) without touching
the demo's `recordings` bucket or `:8080` server. It replicates the 8 real recordings
to 104 byte-identical objects via **server-side copy** (no re-upload) in a separate
bucket `recordings-scale`, and runs a **separate** server instance on `:8083`. The full
mechanism and consolidated numbers are in `2026-06-05-dexory-m1-closure-report.md`; this
section is the runnable script.

### 5.1 Create and populate the scale bucket (server-side copy)

```bash
mkdir -p /tmp/slice9-scale
# create bucket
docker run --rm --network minio_default --entrypoint sh \
  minio/mc:RELEASE.2024-06-12T14-34-03Z \
  -c 'mc alias set local http://minio:9000 admin password123 >/dev/null 2>&1 && mc mb -p local/recordings-scale'
# 8 stems x 13 server-side copies = 104 objects (no bytes leave the cluster)
docker run --rm --network minio_default --entrypoint sh \
  minio/mc:RELEASE.2024-06-12T14-34-03Z -c '
    mc alias set local http://minio:9000 admin password123 >/dev/null 2>&1
    for f in $(mc ls local/recordings | tr -s " " | cut -d" " -f6); do
      stem=${f%.mcap}
      for n in 01 02 03 04 05 06 07 08 09 10 11 12 13; do
        mc cp local/recordings/$f local/recordings-scale/${stem}_copy${n}.mcap
      done
    done'
# confirm 104 objects
docker run --rm --network minio_default --entrypoint sh \
  minio/mc:RELEASE.2024-06-12T14-34-03Z \
  -c 'mc alias set local http://minio:9000 admin password123 >/dev/null 2>&1 && mc ls local/recordings-scale' \
  | grep -c '\.mcap'
```

Expected final line:

```
104
```

### 5.2 Write the scale-server config (bucket is not env-overridable)

The bucket name lives only in `storage.s3.bucket`; nothing overrides it on the command
line. Write a temp YAML (only the bucket, listen, db, and storage endpoint differ from
defaults):

```bash
cat > /tmp/slice9-scale/server.yaml <<'YAML'
server:
  listen: ":8083"
catalog:
  db_path: "/tmp/slice9-scale/catalog-scale.db"
storage:
  s3:
    bucket: "recordings-scale"
    region: "us-east-1"
    endpoint: "http://localhost:9000"
    access_key: "admin"
    secret_key: "password123"
auth:
  bearer_token: ""
format: "mcap"
indexer:
  poll_interval: "30s"
  startup_scan: true
YAML
```

> The field names above match `server/internal/config/config.go` `Default()` exactly
> (S3 uses `access_key`/`secret_key`, indexer uses `poll_interval`). Only
> bucket/listen/db differ from the defaults; if the server rejects the YAML, diff
> against `config.go` `Default()` and adjust.

### 5.3 Cold scan of 104 footers

```bash
rm -f /tmp/slice9-scale/catalog-scale.db*
"$REPO/server/bin/pj-cloud-server" -config /tmp/slice9-scale/server.yaml -log-level info \
  > /tmp/slice9-scale/server-cold.log 2>&1 &
echo $! > /tmp/slice9-scale/server.pid
# wait for health, then read the cold-scan line
until curl -fsS -o /dev/null http://localhost:8083/health; do sleep 1; done
grep 'indexer: run complete' /tmp/slice9-scale/server-cold.log | head -1
```

Expected (a line like):

```
... msg="indexer: run complete" scanned=104 new=104 reindexed=0 unchanged=0 failed=0 duration_ms=1714   # point-in-time sample; expect ~1-2 s on localhost Minio
```

104 new, 0 failed, ~1.7 s for 104 footer-only Range reads (~16.5 ms/file). Confirm the
catalog serves 104 over the wire:

```bash
"$DEVPROBE" -url ws://localhost:8083/api/ws \
  | python3 -c 'import json,sys; print("file_count", json.load(sys.stdin)["file_count"])'
```

Expected:

```
file_count 104
```

### 5.4 A stitched 3-file round-trip at scale

```bash
DEXORY_CLOUD_URL=ws://localhost:8083 "$CLI" download \
  nissan_zala_50_zeg_2_0_copy01.mcap \
  nissan_zala_50_zeg_3_0_copy05.mcap \
  nissan_zala_50_zeg_4_0_copy09.mcap \
  --output /tmp/slice9-scale/stitch3.mcap --json
"$MCAPDIFF" \
  /home/gn/ws/jkk_dataset02/nissan_zala_50_zeg_2_0.mcap \
  /home/gn/ws/jkk_dataset02/nissan_zala_50_zeg_3_0.mcap \
  /home/gn/ws/jkk_dataset02/nissan_zala_50_zeg_4_0.mcap \
  /tmp/slice9-scale/stitch3.mcap
```

Expected: `"messages_received": 87343` and `OK: logically equal, no over-delivery`.
(The copies are byte-identical to their stems, so they diff clean against the on-disk
originals.)

### 5.5 Warm restart (durable catalog, zero re-extracts)

```bash
kill "$(cat /tmp/slice9-scale/server.pid)"; sleep 2
"$REPO/server/bin/pj-cloud-server" -config /tmp/slice9-scale/server.yaml -log-level info \
  > /tmp/slice9-scale/server-warm.log 2>&1 &
echo $! > /tmp/slice9-scale/server.pid
until curl -fsS -o /dev/null http://localhost:8083/health; do sleep 1; done
grep 'indexer: run complete' /tmp/slice9-scale/server-warm.log | head -1
```

Expected:

```
... msg="indexer: run complete" scanned=104 new=0 reindexed=0 unchanged=104 failed=0 duration_ms=6
```

0 re-extracts, 104 skip-unchanged, ~6 ms — the durable SQLite catalog warm-starts.

---

## 6. Reset / cleanup

Stop the interactive server:

```bash
make server-stop
```

Stop and remove the scale server + its DB (keeps `recordings` and `:8080` untouched):

```bash
kill "$(cat /tmp/slice9-scale/server.pid)" 2>/dev/null || true
rm -f /tmp/slice9-scale/catalog-scale.db*
```

Remove the scale bucket (optional; it is harmless to leave):

```bash
docker run --rm --network minio_default --entrypoint sh \
  minio/mc:RELEASE.2024-06-12T14-34-03Z \
  -c 'mc alias set local http://minio:9000 admin password123 >/dev/null 2>&1 && mc rb --force local/recordings-scale'
```

Remove demo artifacts:

```bash
rm -rf /tmp/m1-demo /tmp/slice9-scale
```

Leaving Minio up is fine; `docker compose -f "$REPO/infra/minio/docker-compose.yml" down`
stops it.

---

## 7. One-shot regression alternative

Everything in §1–§3 (minus the GUI) is also covered, hands-off, by the regression gate:

```bash
make smoke    # builds + starts its OWN server on :8081, runs steps a-h, prints SMOKE PASS
```

`make smoke` does not touch `:8080`/`:8083`; it stands up and reaps its own `:8081`
server. The final line is `SMOKE PASS` (exit 0) or `SMOKE FAIL: <step>`. Step f
(f1–f5) is the same round-trip family demonstrated manually in §3.2–§3.4. Use the
manual runbook for the *live demo*; use `make smoke` to prove nothing regressed.
