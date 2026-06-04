# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this directory is

This is the **design / planning workspace for the "PJ Cloud Connector"** — a commercial
engagement (client: Dexory) to build a self-hosted server + client that serves MCAP
recordings from an S3 bucket to PlotJuggler-class clients on demand.

**There is no implementation code here yet.** This directory holds the design spec, three
implementation plans, and the commercial proposal. It is *not* a git repository (it lives
inside the larger, non-versioned `~/ws_plotjuggler/` workspace). When implementation
starts, the plans say the code goes in a **new, separate repo `pj-cloud/`** (a sibling of
`PJ4/`, not a subdirectory of this folder).

So: the build/test commands quoted below describe the *future* `pj-cloud/` repo. The only
thing runnable *in this directory today* is the proposal renderer.

## Documents (read in this order)

1. `2026-05-28-pj-cloud-connector-design.md` — **the canonical design spec (single source
   of truth).** 14 sections: architecture, repo layout, catalog/SQLite model, wire protocol,
   Go server design, Qt client design, failure/resume, testing strategy, phased build order.
   The three plans below all reference this spec and must not contradict it.
2. `2026-05-28-pj-cloud-server-v1.md` — **Plan A**: Go server, task-by-task with `- [ ]`
   checkboxes.
3. `2026-05-28-pj-cloud-client-cpp.md` — **Plan B**: Qt C++ test client (`client-core` lib +
   `client-cli` exe). Depends on Plan A's `proto/pj_cloud.proto`.
4. `2026-05-28-pj-cloud-integration.md` — **Plan C**: cross-language end-to-end correctness
   harness (Docker + Minio + round-trip MCAP byte-diff). Depends on the binaries from A & B.
5. `2026-06-01-dexory-proposal.{md,html,pdf}` — the commercial proposal. `*.md` is the
   source; `*.html` and `*.pdf` are generated artifacts (do not hand-edit them).

`proto/pj_cloud.proto` (defined in Plan A) is the **canonical wire schema** shared by all
three plans — treat it as the single source of truth for the protocol; generated bindings
are checked in so consumers don't need `protoc`.

## Commands

### In this directory (the only thing buildable here)

Render the proposal `*.md` → self-contained `*.html`:

```bash
python3 _render_proposal.py          # needs the `markdown` pip package
```

PDF is then produced from that HTML with headless Chrome
(`google-chrome --headless --print-to-pdf=2026-06-01-dexory-proposal.pdf 2026-06-01-dexory-proposal.html`).
Editing the proposal = edit the `.md`, re-run the script, regenerate the PDF.

### In the future `pj-cloud/` repo (per the plans — not present yet)

- Go server (Plan A, top-level `Makefile`): `make proto` (generate bindings) ·
  `make build` · `make test` (unit + race) · `make integration` (needs Docker for Minio) ·
  `make bench` (v1 benchmark gate). Single test: `cd server && go test ./internal/<pkg>/ -run <Name>`.
- Qt C++ client (Plan B): Conan 2 + CMake ≥ 3.21, C++20. Two targets `client-core`
  (Qt-aware static lib, **no `Qt6::Widgets`**) and `client-cli` (`QCoreApplication` exe),
  tests via gtest/ctest.
- Integration (Plan C): `docker-compose up -d --build` (Minio + server),
  `go run ./cmd/gen-fixtures --out fixtures`, then `go test` matrix driver.

## Architecture being designed (the big picture)

A single WebSocket per client carries everything, multiplexed via a Protobuf envelope:
catalog RPCs **and** session data streaming on one connection. Streaming is
**bounded-horizon, as-fast-as-possible** (a bulk download with a known size), *not*
wall-clock-paced playback. "Streaming" here means *incremental/progressive download* — bytes arrive
in batches so the client can show already-received data while the rest downloads in the background —
**not** real-time pacing. Supports reconnect-and-resume (short retain window) and
cancel-mid-stream.

- **Go server** — one static binary, five subsystems on one TCP listener: Catalog
  (WS RPC + SQLite reads, WAL + single writer goroutine), Session (WS streaming + S3
  fetcher with producer/consumer split for resume), Indexer (background S3 poller),
  Dashboard (read-only HTML + Prometheus `/metrics` + `/health`). Pure-Go SQLite (no cgo).
- **Qt C++ client** — `client-core` owns the WS/protocol/decompression and exposes a
  `SessionSink` seam; `client-cli`'s `McapWriterSink` reconstructs the streamed session as a
  local MCAP file. **`client-core` is deliberately Widgets-free so a future PJ4 DataSource
  plugin can lift it in unchanged** (that PJ4 integration is explicitly out of v1 scope).
- **Integration harness** — deterministic MCAP fixture matrix (compression, payload sizes,
  multi-file stitching, tags, time-range edges). The v1 gate: original MCAP → server → CLI →
  reconstructed MCAP must be byte-equal on `(topic, log_time, payload)`.

Server-side **stitching**: selecting N consecutive MCAPs presents one continuous logical
session (one time range, union of topics, ordered stream). The client commits to
`(file_ids[], topic_names[], time_range)` before streaming; the server returns pre-flight
estimates (`estimated_chunk_bytes`, `approximate_messages`).

## Working conventions

- The three plans are written **for agentic execution**: each starts with a required
  sub-skill (`superpowers:subagent-driven-development` or `superpowers:executing-plans`) and
  uses `- [ ]` checkboxes for task tracking. Follow that workflow when implementing them, and
  follow the spec's **§13 phased build order** for sequencing across plans.
- Engagement shape (from the proposal): **Milestone 1** = PoC (Go server + Qt CLI,
  round-trip validated), gated on client approval before **Milestone 2** = hardening +
  PlotJuggler plugin integration (with a browser/WASM bonus). Keep v1 work inside the M1
  non-goals (no multi-tenancy, no realtime pacing, no PJ4 plugin, single shared bearer token).
