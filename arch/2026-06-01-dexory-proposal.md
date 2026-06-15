# Proposal — PlotJuggler Cloud Connector for Dexory

| | |
|---|---|
| **Date** | 2026-06-01 |
| **From** | Davide Faconti (author of [PlotJuggler](https://github.com/facontidavide/PlotJuggler)) |
| **To** | Dexory |
| **Engagement** | Two milestones × one month — Milestone 1 €10,000, Milestone 2 €6,000 (total €16,000) |
| **Gating** | Client approval of Milestone 1 required before Milestone 2 starts |

---

## 1. The problem

Dexory operates a fleet of autonomous robots that generate large volumes of sensor data on every shift. Recordings are stored as **MCAP** files in **Amazon S3** — typically several files per robot per run, with a single inspection session often spanning several **consecutive recordings** from the same robot (e.g. a long mission split across multiple files for size or rotation reasons).

For engineers investigating a specific event — a missed pallet detection, a localisation glitch, an unusual deceleration — the current workflow is friction-heavy:

- Browsing S3 with naming conventions or external scripts to find candidate recordings.
- Downloading each MCAP locally, even though a single inspection might only touch a handful of topics out of dozens stored in each file.
- Stitching consecutive recordings by hand when the event of interest straddles file boundaries.
- Re-doing this every time, because nothing is cached or indexed.

This is a recurring cost — multiplied across the engineering team, repeated weekly — that scales linearly with the fleet and the data retention horizon. The longer Dexory operates, the worse it gets.

## 2. Proposed solution at a glance

A **purpose-built server** sits in front of the S3 bucket and exposes a queryable, filterable catalog of all MCAP recordings. **PlotJuggler** (running on the engineer's laptop) speaks to this server through a custom connector plugin and streams **only the messages it actually needs** for a given investigation: selected files, selected topics, selected time range — nothing more.

```
                     ┌────────────────────────────────────────────┐
                     │  Dexory's network                          │
                     │                                            │
┌──────────────┐     │   ┌────────────────────┐                   │   ┌────────┐
│ PlotJuggler  │ ◄───┼───┤  PJ Cloud Server   │ ◄── HTTP Range ───┼──►│   S3   │
│ (engineer's  │     │   │                    │                   │   │  bucket│
│  laptop)     │     │   │  • Catalog (SQLite)│                   │   └────────┘
│              │     │   │  • Streaming       │                   │
│  cloud       │     │   │  • Indexer         │                   │
│  connector   │     │   │  • Web dashboard   │                   │
│  plugin      │     │   └────────────────────┘                   │
└──────────────┘     └────────────────────────────────────────────┘
```

**Key properties:**

- The server never downloads whole MCAP files to deliver a session. It reads each file's chunk index from the footer (~kilobytes) and pulls only the chunks the user asked about over **HTTP Range requests** against S3. For a typical "I want IMU + GPS from these 3 files for a 30-second window" query, this can mean shipping **<1 %** of the underlying bytes.
- The engineer sees **one continuous timeline** even when the underlying data spans multiple consecutive recordings. The server stitches them server-side; PlotJuggler treats it as if it were a single file.
- Filtering happens **before** the bytes start flowing: the engineer picks files (by recording date, by user-defined tags like `vehicle`, `route`, `run`), then picks topics, then optionally narrows the time range. Throughput cost is paid only for the data the engineer actually loads.
- A **web dashboard** on the same server gives operations a read-only view of the catalog: what's indexed, what's storage usage, what sessions are currently active.

## 3. Architectural details

This section is intentionally compact. A full design specification (937 lines) and a phased implementation plan (~13,000 lines across three sub-plans) already exist and will be shared as part of the engagement.

### 3.1 Two components, one wire contract

| Component | Language / runtime | Purpose |
|---|---|---|
| **`pj-cloud-server`** | Go (single static binary) | Catalog + indexer + streaming + admin dashboard, all in one process. Stateless except for an embedded SQLite catalog. Deploys via Docker. |
| **PlotJuggler connector** | C++ / Qt (a PlotJuggler v4 DataSource plugin) | Browses the catalog through a UI dialog, opens streaming sessions, decodes messages into PlotJuggler's data store using the existing MessageParser plugin family (ROS, Protobuf, JSON-Schema, …). |

A single **Protobuf schema** describes every message that crosses the wire, used by both sides at build time. No hand-rolled serialisation, no schema drift.

### 3.2 Transport

A **single WebSocket per client** carries everything: catalog browsing, tag editing, streaming session lifecycle, and the streamed messages themselves. Binary frames, Protobuf-encoded, with a small request/response correlation layer for catalog RPCs and a subscription-id layer for streams. One port, one TLS cert, one auth handshake.

### 3.3 Catalog & indexing

A background goroutine inside the server polls S3 every few minutes, reads the **MCAP footer** of each new object (one or two Range requests), and records what it found in SQLite:

- File-level metadata: time range, size, message count, S3 ETag for change detection.
- Topic-level metadata: name, schema encoding (`ros2msg`, `protobuf`, …), message count per topic.
- Tag metadata in two layers: **embedded tags** (read out of the MCAP file itself, set by Dexory's recording pipeline) plus **override tags** (added or corrected via the connector, sticky across re-indexing).

Catalog queries (filter by time, topic existence, tag predicates) take milliseconds even at the fleet scale Dexory is heading toward (thousands of recordings).

### 3.4 Streaming

When the engineer hits "Open session", the server:

1. Validates the selection (consecutive non-overlapping files, valid topics, sane time range).
2. Walks each selected file's chunk index to build the **minimal ordered list of chunks** that need to be fetched.
3. Returns a **pre-flight estimate** to the client — exact byte budget for the S3 pull, approximate message count — so the engineer sees a real progress bar rather than a spinner.
4. Spawns two cooperating goroutines: one fetches chunks from S3 and packs messages into batches; the other drains the batch buffer to the WebSocket.

The split between the two goroutines is what makes **reconnect-and-resume** work: a flaky network drops the connection, the producer keeps filling a bounded retain buffer, and when the client reconnects it asks to resume from the last acknowledged sequence number. No bytes refetched, no work redone.

### 3.5 PlotJuggler integration

The connector plugin slots into PlotJuggler v4 as a **DataSource** alongside the existing local-file sources (rosbag, MCAP, CSV, …). From the engineer's perspective the workflow is:

1. `File → Open Cloud Session…`
2. Browse / filter / select files in a dialog (table with sortable columns: name, date, duration, tags).
3. Pick topics (multi-select tree).
4. Optionally narrow time range with a slider.
5. Click `Open` — PlotJuggler shows a progress bar; the data lands in its time-series store as it arrives; the user can start plotting immediately.

Once the session completes, the data behaves identically to a file the engineer had opened locally — transforms, scripting, exports all work without modification. Closing the tab frees the memory; opening another cloud session reuses the same connection.

## 4. Milestone 1 — Proof of Concept (1 month, €10,000)

**Goal:** demonstrate the end-to-end data path against a representative subset of Dexory's S3 corpus, on a development machine, with the smallest credible feature surface.

### Deliverables

- **`pj-cloud-server` v0.1**: Go binary running the catalog + indexer + streaming session pipeline against a real S3 bucket (Dexory's, or a copy in a sandbox bucket). SQLite catalog populated by the background poller. WebSocket endpoint speaking the full v1 wire protocol.
- **Reference client**: a small Qt-based command-line tool (`pjcloud-cli`) that browses the catalog, opens a session, and **reconstructs the streamed data back into a local MCAP file**. This is the strongest correctness signal at PoC stage — the reconstructed file can be diff'd against the original to prove the wire path is lossless.
- **Round-trip integration test** running against a Minio-backed local stack via Docker Compose: the byte-equality of every message payload, schema, and timestamp is the pass/fail gate.
- **Live demo for Dexory**: I open a real session against the bucket, filter by tags + topics + time range, and we watch the reconstructed MCAP appear locally in real time. The PoC is "done" when this demo is repeatable and convincing.
- **Source code** on a private repo accessible to Dexory's team, with a brief README + run instructions.

### What's intentionally out of scope for M1

- The PlotJuggler GUI plugin itself. M1 proves the protocol; M2 puts the UI on top.
- Operator dashboard, metrics, alerting.
- Production deployment story (TLS, auth beyond a shared token, restart safety).
- Tag editing.
- Time-range partial selection (M1 supports whole-file topic filtering; M2 adds intra-file time windows).

This scope is sized so the PoC can be **judged on its merits**: if the demo works and the round-trip test passes for Dexory's real fixtures, the rest of M2's work is execution risk only, not technical risk.

### Acceptance criteria

- The server runs against Dexory's S3 bucket and indexes at least 100 real MCAPs.
- The CLI opens a session covering at least 3 consecutive recordings, downloads only the requested topics, and writes a valid MCAP whose every message matches the originals byte-for-byte.
- A short written report (8–12 pages) describes what was built, what worked, what didn't, and what M2 will change as a result.

## 5. Milestone 2 — Hardening + PlotJuggler integration (1 month, €6,000)

**Gated by Dexory's explicit approval of Milestone 1.** No work begins on M2 until M1 is accepted in writing.

**Goal:** turn the PoC into something Dexory's engineering team uses every day without thinking about it.

### Deliverables

- **PlotJuggler v4 connector plugin** (the `pjcloud://` DataSource plugin) replacing the reference CLI as the primary client. Includes the catalog browse dialog, topic + time-range selection UI, progress dialog, and secure token storage via the OS keyring.
- **Tag editing** from the connector: engineers can correct or annotate recordings (e.g. `verified=yes`, `incident=2026-05-12-A1`) and the overrides persist across re-indexing.
- **Reconnect-and-resume** wired through the plugin so a brief network blip doesn't lose the in-progress session.
- **Operator dashboard** on the server (single TLS-protected HTTP endpoint with basic auth): catalog summary, indexer status, active sessions, Prometheus `/metrics`, `/health` for orchestration.
- **Production deployment artifacts**: Dockerfile (already part of M1), `docker-compose.yml` for a representative deployment, sample `systemd` service file, configuration reference documentation, deployment runbook.
- **Hardening pass**: panic recovery and counter metrics on every goroutine boundary, graceful shutdown, S3 transient-failure retry with bounded backoff, write timeouts, slow-client backpressure verified under load.
- **Cross-language E2E test matrix** in CI: every push runs a fixture corpus covering compression, payload sizes, multi-file stitching, time-range edges, and an explicit `200 MB/s` throughput gate on a reference machine.
- **End-user documentation**: a short guide for Dexory engineers (how to install the plugin, how to filter, how to tag) and an operator guide (how to deploy, how to update config, what the dashboard means).

### Acceptance criteria

- The plugin installs into a stock PlotJuggler v4 build and opens a cloud session inside ~5 clicks from cold start.
- A Dexory engineer (not the author) can complete an end-to-end investigation against a real recording without intervention, working from the documentation alone.
- All E2E tests pass in CI for at least one full week before sign-off.
- The throughput gate (200 MB/s sustained on the reference machine) holds.
- Deployment runbook is reviewed by Dexory's ops contact.

## 6. Bonus — PlotJuggler in the browser (included with M2)

The wire protocol described in §3 (WebSocket + Protobuf + per-message records) was designed without native-only dependencies. That means it works **unmodified** against PlotJuggler's WebAssembly build — the same v4 UI running entirely inside a modern web browser.

This isn't a separate feature invented for this proposal; it's an existing ongoing work stream at PlotJuggler (the WASM port has already shipped M0 + M1). Bundling early access to it into this engagement means:

- **Zero-install viewing for casual users.** An ops engineer or product manager can open a session URL in Chrome and see exactly what the recording shows. No PlotJuggler install, no plugin to configure, no per-OS compatibility to manage.
- **Shareable sessions.** A senior engineer can send a colleague a URL pointing at a specific (files × topics × time range) selection; the colleague opens it and sees the same view.
- **Locked-down environments work.** Browsers run on managed laptops where installing a native desktop app is a procurement conversation. The connector working in a browser sidesteps that entirely.
- **No Dexory-side cost.** Dexory becomes an early consumer of the WASM build as a side effect of M2's protocol work, not as a separately scoped paid feature.

The browser path will be **demonstrated in the M2 demo session** as a credible preview, not promised as a polished production feature. Treat it as a signal of where the tooling is heading, included so Dexory can plan ahead.

## 7. What's out of scope for both milestones

I list these explicitly so there are no surprises:

- **Multi-tenancy / SaaS.** The server is designed for single-team self-hosting on Dexory's trusted network. Adding org-level isolation, OIDC, per-team quotas, etc. is a separate engagement.
- **Backward compatibility with PlotJuggler v3.** The plugin targets PlotJuggler v4, which is the current development direction.
- **Persistent retain across server restarts.** A server restart drops in-progress retain buffers; clients reopen sessions from scratch. Survives normal Kubernetes-style restarts; doesn't require a database for the streaming layer.
- **Real-time live streaming from running robots.** This connector is for **stored** recordings. Live data is a different problem with different tradeoffs (ROS bridges, MCAP-over-MQTT, etc.) and not covered here.
- **Long-form storage tiering.** S3 lifecycle policies (Glacier, etc.) interact with random-access reads in ways that need per-deployment thinking; out of scope for v1.
- **Custom Dexory-specific transforms or analyses.** The connector delivers messages; PlotJuggler's existing transform/scripting layer is what users compose to derive insights.

## 8. Terms

- **Price:** €10,000 + applicable VAT for Milestone 1, €6,000 + applicable VAT for Milestone 2.
- **Payment:** 50 % at start of each milestone, 50 % on acceptance.
- **Schedule:** Milestone 1 begins within two weeks of contract signature. Milestone 2 begins within two weeks of Milestone 1 acceptance (no fixed gap; Dexory can take longer to evaluate without penalty).
- **IP:** Dexory receives a perpetual, non-exclusive licence to use, modify, and redistribute the resulting code internally. The protocol design, the server, and the plugin can also be open-sourced upstream into the PlotJuggler organisation; happy to discuss either model.
- **Support:** 30 days of post-acceptance bugfix support after each milestone, included.

## 9. Next steps

If this looks right in shape:

1. A 30-minute call to walk through the architecture and answer technical questions from your team.
2. Sample access to a representative subset of recordings (or a description of their typical shape) so the M1 demo is convincing from day one.
3. A short statement of work derived from this document for legal sign-off.

I'm happy to revise scope, scale up or down, or split into smaller chunks if that fits Dexory's procurement model better.

Looking forward to working together.

— Davide Faconti
