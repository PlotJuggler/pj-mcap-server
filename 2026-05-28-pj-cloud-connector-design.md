# PJ Cloud Connector — Design Spec

| | |
|---|---|
| Date | 2026-05-28 |
| Status | Draft — pending user review |
| Owner | Davide Faconti |
| Repo (new) | `pj-cloud/` (separate from PJ4) |
| Future PJ4 integration | `PJ4/pj_plugins/pj_cloud/` (deferred; separate spec) |

## 1. Summary

`pj-cloud` is a self-hosted server + client toolkit that serves MCAP recordings from an S3 bucket to PlotJuggler-class clients on demand. The server presents a queryable catalog with rich tag-based filtering, and streams **selected (files × topics × time-range)** subsets of MCAP message data over a single WebSocket connection per client. Streaming is **bounded-horizon, as-fast-as-possible** (not realtime-paced): the client knows the full session size up front and downloads in bulk while reconstructing the data locally.

The v1 deliverable is the Go server plus a Qt C++ test client that round-trips data back to MCAP — together they validate the wire protocol end-to-end. A subsequent project (separate spec) will lift the Qt client-core library into a PlotJuggler 4 DataSource plugin.

## 2. Goals and non-goals

### Goals (v1)

- **Catalog browse** of an S3 bucket of MCAP recordings, indexed by recording-derived metadata (time range, topics, embedded tags) and user-added overrides (sticky tags edited via the server).
- **Filtered selection**: time-range, topic-existence, and tag-predicate filters compose for catalog queries.
- **Server-side stitching**: selecting N consecutive MCAPs presents one continuous logical session (one time range, union of topics, ordered message stream).
- **Pre-selected topics + time-range subset**: before streaming starts, the client commits to a set of `(file_ids[], topic_names[], time_range)`. Server returns approximate pre-flight numbers (`estimated_chunk_bytes` is the S3-fetch upper bound; `approximate_messages` is best-effort from MCAP chunk/message indexes — exact when MessageIndex records are present, an upper bound otherwise).
- **Streaming as fast as the network and S3 allow**, not paced by playback wall clock. The client treats it as a bulk download with a known horizon, not a live source.
- **Per-message wire format**: raw MCAP message records `(topic, ts, schema, payload_bytes)` are forwarded with their schemas; the client (a future `MessageParser` in PJ4, or the test CLI's MCAP writer in v1) is responsible for decoding.
- **Single WebSocket per client** carrying both catalog RPCs and streaming session pushes, multiplexed via a Protobuf envelope.
- **Reconnect-and-resume**: a client that drops the WS within a short retain window (default 60 s) can reattach and continue from the last received sequence without re-downloading.
- **Cancel mid-stream** with bounded wind-down.
- **Read-only HTTP admin dashboard** on the same port: catalog list, file detail, indexer status, active sessions, Prometheus `/metrics`, `/health`.
- **Qt C++ test client**: CLI tool that exercises catalog + session APIs, with an `McapWriterSink` that reconstructs the streamed session as a local MCAP file. Library/driver split (`client-core` Qt-aware static library, `client-cli` `QCoreApplication` exe) so the protocol layer becomes the PJ4 plugin's foundation later.
- **Round-trip correctness test**: original MCAP → server → CLI → reconstructed MCAP → byte-equal message comparison.

### Non-goals (v1, deferred or out of scope)

- **PJ4 plugin integration**. The Qt client-core library is designed for reuse, but the actual `pj_cloud` DataSource plugin (with `DatastoreSink`, `CloudOpenDialog`, qtkeychain integration, `pjcloud://` URI scheme registration, and the `FileSourceBase::launchCustomOpenDialog()` SDK hook) is a separate project after v1 server + CLI ship and stabilize.
- **Multi-tenancy / SaaS deployment**. Single-team, self-hosted, trusted-network is the target. Auth is a single shared bearer token; OIDC/JWT is post-v1.
- **Realtime (wall-clock-paced) streaming**. The model is "download bounded session as fast as possible".
- **Lazy or mid-stream topic subscription**. Topics are committed at session open.
- **Local on-disk caching of streamed sessions on the PJ4 client**. Memory-only via `pj_datastore` (in the future plugin). The test CLI writes a local MCAP because that *is* its output, not as a cache.
- **Persistent retain across server restarts**. Retain buffers are in-memory; a server restart causes in-flight clients to reopen sessions from scratch.
- **Tag editing in the dashboard UI**. Read-only operator view; tag edits go through the WS API from PJ4 (or the CLI tag subcommand).
- **Write actions in the dashboard**. No delete-file, no force-reindex.
- **Per-user audit log**. Premature without real per-user auth.
- **Throughput SLOs and benchmark targets**. Aspirational only; observe + tune post-v1.
- **Fuzz testing of the Protobuf envelope**. Easy to add post-v1.

## 3. Top-level architecture

```
┌────────────────────────┐                         ┌────────────────────────────────────┐
│ Qt C++ test client     │                         │  pj-cloud-server (Go, single bin)  │
│ (later: PJ4 plugin)    │                         │                                    │
│                        │                         │  ┌──────────┐    ┌──────────────┐  │
│  ┌──────────────────┐  │                         │  │ Catalog  │    │  Session     │  │
│  │  client-core     │  │  WSS, binary frames,    │  │ service  │    │  service     │  │
│  │  (Qt-aware lib)  │◄─┼──Protobuf envelope──────┼──┤  (WS RPC)│    │  (WS streams)│  │
│  └────────┬─────────┘  │                         │  └────┬─────┘    └──────┬───────┘  │
│           │            │                         │       │                 │          │
│  ┌────────▼─────────┐  │                         │  ┌────▼──────┐    ┌─────▼──────┐   │
│  │  SessionSink     │  │                         │  │  SQLite   │    │  S3 reader │   │
│  │  (interface)     │  │                         │  │  catalog  │    │  (Range GET│   │
│  └────────┬─────────┘  │                         │  │  DB (WAL) │    │   only)    │   │
│           │            │                         │  └────▲──────┘    └─────┬──────┘   │
│  ┌────────▼─────────┐  │                         │       │                 │          │
│  │ McapWriterSink   │  │                         │  ┌────┴───────┐         │          │
│  │ (test CLI)       │  │                         │  │  Indexer   │◄────────┘          │
│  │                  │  │                         │  │  (poller)  │                    │
│  │ DatastoreSink    │  │                         │  └────────────┘                    │
│  │ (future PJ4)     │  │                         │                                    │
│  └──────────────────┘  │                         │  ┌────────────┐  ┌─────────────┐   │
└────────────────────────┘                         │  │ Dashboard  │  │ Health /    │   │
                                                   │  │ (HTTP HTML)│  │ Metrics     │   │
                                                   │  └────────────┘  └─────────────┘   │
                                                   └─────────────────────────────┬──────┘
                                                                                 │
                                                                                 ▼
                                                                             ┌───────┐
                                                                             │  S3   │
                                                                             │bucket │
                                                                             └───────┘
```

**Two deployable pieces, one shared `.proto` file:**

- `pj-cloud-server` — single Go static binary, five internal subsystems sharing one process and one TCP listener (one TLS cert): Catalog (WS RPC), Session (WS streaming), Indexer (background S3 poller), Dashboard (HTML), Health/Metrics.
- `pjcloud-cli` — Qt C++ test client built on top of a reusable `client-core` static library. The library does the protocol work; the CLI drives it.
- `pj_cloud.proto` — canonical wire schema in the `pj-cloud/proto/` directory; generates Go and C++ bindings at build time.

## 4. Repository layout

```
pj-cloud/                                      # NEW REPO, sibling to PJ4
├── proto/
│   └── pj_cloud.proto                         # canonical wire schema
├── server/                                    # Go server
│   ├── cmd/pj-cloud-server/main.go
│   ├── internal/
│   │   ├── config/                            # YAML + env-var config
│   │   ├── catalog/                           # SQLite catalog (read+write)
│   │   ├── indexer/                           # background S3 poller
│   │   ├── s3reader/                          # io.ReadSeeker over S3 Range GETs
│   │   ├── session/                           # plan, pump, retain, registry
│   │   ├── wire/                              # generated Protobuf bindings (.pb.go)
│   │   ├── ws/                                # WS upgrade + per-conn dispatcher
│   │   └── dashboard/                         # HTML + embedded static assets
│   ├── go.mod
│   └── deploy/
│       ├── Dockerfile                         # ~10 lines, distroless
│       └── config.example.yaml
├── client-core/                               # Qt C++ static library
│   ├── CMakeLists.txt
│   ├── include/pj_cloud_client/
│   │   ├── CloudConnection.h
│   │   ├── MessageDispatcher.h
│   │   ├── CatalogClient.h
│   │   ├── SessionClient.h
│   │   └── SessionSink.h
│   └── src/
│       ├── CloudConnection.cpp
│       ├── MessageDispatcher.cpp
│       ├── CatalogClient.cpp
│       ├── SessionClient.cpp
│       ├── Envelope.cpp
│       ├── Decompression.cpp
│       └── proto/                             # generated Protobuf bindings (.pb.cc)
├── client-cli/                                # Qt C++ console driver
│   ├── CMakeLists.txt
│   └── src/
│       ├── main.cpp                           # QCoreApplication + QCommandLineParser
│       ├── ListCommand.cpp / ShowCommand.cpp / TagCommand.cpp / DownloadCommand.cpp
│       └── McapWriterSink.{h,cpp}             # SessionSink → libmcap writer
├── integration-tests/
│   ├── docker-compose.yml                     # server + minio
│   ├── fixtures/                              # canned MCAPs (committed; ~5 MB)
│   ├── gen_fixtures.cpp                       # deterministic fixture generator
│   ├── fake-server/                           # Go binary speaking canned protocol scripts
│   └── test_roundtrip.cpp                     # the headline correctness test
├── CMakeLists.txt                             # top-level: client-core + client-cli + tests
├── conanfile.txt                              # Qt, protobuf, zstd, lz4, mcap, gtest, spdlog
└── README.md
```

The Go server has its own `go.mod` and is independent of the CMake side; both share `proto/pj_cloud.proto` via `protoc` codegen targets configured in each build system.

## 5. Catalog model

### 5.1 SQLite schema

```sql
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE files (
    id                INTEGER PRIMARY KEY,
    s3_key            TEXT    NOT NULL UNIQUE,
    s3_etag           TEXT    NOT NULL,
    s3_last_modified  INTEGER NOT NULL,         -- unix ns, from S3 metadata
    size_bytes        INTEGER NOT NULL,
    indexed_at        INTEGER NOT NULL,         -- unix ns, when we indexed
    start_time_ns     INTEGER NOT NULL,
    end_time_ns       INTEGER NOT NULL,
    chunk_count       INTEGER NOT NULL,
    message_count     INTEGER NOT NULL,
    has_message_index INTEGER NOT NULL,          -- bool: MessageIndex records present in all chunks
    mcap_summary      BLOB                       -- cached MCAP statistics record
);
CREATE INDEX idx_files_time ON files(start_time_ns, end_time_ns);

CREATE TABLE topics (
    file_id         INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    name            TEXT    NOT NULL,
    schema_name     TEXT    NOT NULL,
    schema_encoding TEXT    NOT NULL,           -- "ros2msg", "protobuf", "jsonschema", ...
    message_count   INTEGER NOT NULL,
    PRIMARY KEY (file_id, name)
);
CREATE INDEX idx_topics_name ON topics(name);

CREATE TABLE tags_embedded (
    file_id  INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    key      TEXT    NOT NULL,
    value    TEXT    NOT NULL,
    PRIMARY KEY (file_id, key)
);

CREATE TABLE tags_override (
    file_id    INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    key        TEXT    NOT NULL,
    value      TEXT,                            -- NULL = "unset" (mask embedded)
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (file_id, key)
);

CREATE INDEX idx_tags_embedded_kv ON tags_embedded(key, value);
CREATE INDEX idx_tags_override_kv ON tags_override(key, value);

-- Merged view used by all filter queries.
CREATE VIEW tags_effective AS
SELECT file_id, key, value FROM tags_override WHERE value IS NOT NULL
UNION
SELECT e.file_id, e.key, e.value FROM tags_embedded e
LEFT JOIN tags_override o ON (o.file_id = e.file_id AND o.key = e.key)
WHERE o.file_id IS NULL;

CREATE TABLE indexer_failures (
    s3_key      TEXT NOT NULL PRIMARY KEY,
    failed_at   INTEGER NOT NULL,
    error_text  TEXT NOT NULL
);
```

**Tag merge semantics:** override-wins. An override row with non-NULL `value` replaces the embedded value for that `(file_id, key)`. An override row with NULL `value` masks the embedded tag (user explicitly removed it). Re-indexing a file refreshes `tags_embedded` but never touches `tags_override`, so user edits survive re-indexing.

### 5.2 Indexer

A single Go goroutine running on a configurable poll interval (default 5 min):

```
startup:
  indexer.runOnce()                            # warm DB before serving traffic
  go indexer.loop(interval=cfg.PollInterval)   # background

runOnce:
  for page in s3.ListObjectsV2(bucket, prefix):
    for obj in page:
      # Match on (etag, size, last_modified) — ETag alone is unreliable across
      # multipart uploads / server-side encryption. Any mismatch triggers re-index.
      row = files.lookup(s3_key=obj.Key)
      if row && row.s3_etag == obj.ETag && row.size_bytes == obj.Size
              && row.s3_last_modified == obj.LastModified:
        skip
      elif row:                                  # changed in place
        re-index (preserves overrides — tags_override is keyed on file_id, never deleted by reindex)
      else:
        index new file

indexFile(obj):
  reader = s3reader.New(obj)                   # io.ReadSeeker via Range GETs
  mcapReader = mcap.NewReader(reader)
  info = mcapReader.Info()                     # reads footer + summary; small Range GETs
  tx = db.Begin()
    upsert files(...)
    delete from topics where file_id = ...; insert each channel
    delete from tags_embedded where file_id = ...; insert each metadata record
  tx.Commit()
  on any error: insert indexer_failures(s3_key, err); skip
```

**Performance:** 10k files at steady state takes ~10 paginated `ListObjectsV2` calls plus zero tail-fetches per poll (existing files are skipped via ETag check); poll latency dominated by `ListObjectsV2` (~hundreds of ms).

## 6. Wire protocol

### 6.1 Transport

Single persistent WebSocket per client (`wss://server:port/api/ws`). Binary frames carry length-delimited Protobuf envelopes (`ClientMessage` and `ServerMessage`). One WS multiplexes:

- **RPC-style catalog requests** (correlated by `request_id`).
- **Streaming session pushes** (tagged with `subscription_id`).
- **Server-initiated control messages** (`Error`, future change-notifications).

The WS closes after each session completes (or when the user dismisses the connector dialog without loading). The server does not retain per-client state between connections; each new connection re-authenticates via `Hello`.

### 6.2 Envelope

```proto
message ClientMessage {
  uint64 request_id = 1;            // matched by server on response; 0 = unsolicited (Ack, Cancel)
  oneof payload {
    Hello              hello         = 10;
    ListFilesRequest   list_files    = 11;
    GetFileRequest     get_file      = 12;
    UpdateTagsRequest  update_tags   = 13;
    OpenSessionRequest open_session  = 14;
    CancelSession      cancel        = 15;
    SessionAck         ack           = 16;
  }
}

message ServerMessage {
  uint64 request_id      = 1;       // echoed on RPC responses; 0 on stream pushes
  uint64 subscription_id = 2;       // set on stream pushes; 0 otherwise
  oneof payload {
    HelloResponse        hello_response = 10;
    ListFilesResponse    list_files     = 11;
    GetFileResponse      get_file       = 12;
    UpdateTagsResponse   update_tags    = 13;
    OpenSessionResponse  open_session   = 14;
    MessageBatch         batch          = 15;
    Progress             progress       = 16;
    Eos                  eos            = 17;
    Error                error          = 18;
  }
}
```

**Routing:** if `request_id != 0`, the message is an RPC response. If `subscription_id != 0`, it's a stream push. Both zero is a protocol bug.

### 6.3 Session open request and response

`OpenSessionRequest` is a `oneof` so the client cannot ambiguously mix fresh-open and resume fields in one message:

```proto
message OpenSessionRequest {
  oneof mode {
    OpenFresh fresh   = 1;
    OpenResume resume = 2;
  }
}

message OpenFresh {
  repeated uint64 file_ids    = 1;     // catalog file ids, time-ordered set
  repeated string topic_names = 2;     // empty = "all topics in the union"
  TimeRange       time_range  = 3;     // optional; default = stitched union
}

message OpenResume {
  uint64 subscription_id  = 1;         // from prior OpenSessionResponse
  uint64 resume_after_seq = 2;         // last seq the client durably has
}
```

**File-overlap validation.** On `OpenFresh`, the server checks that the selected files' `[start_time, end_time]` ranges are pairwise non-overlapping and orderable by `start_time`. Overlapping or non-orderable selections return `Error{INVALID_REQUEST, "overlapping file time ranges: <a> and <b>"}`. This keeps the pump as a single-pass scan-and-emit over time-ordered files rather than a k-way merge (the v1 use case — "consecutive recordings" — never violates this; the multi-recording UI in PJ4 will reject overlap selections client-side).

```proto
message OpenSessionResponse {
  uint64    subscription_id        = 1;
  TimeRange merged_time_range      = 2;    // stitched horizon across selected files
  uint64    estimated_chunk_bytes  = 3;    // upper bound: sum of intersecting compressed chunk sizes on S3
                                            //   (server fetch budget; the client receives ≤ this minus topic-filter savings)
  uint64    approximate_messages   = 4;    // exact when MCAP MessageIndex is present in every selected chunk,
                                            //   upper bound from chunk-level message counts otherwise
  repeated TopicBinding  topic_id_map = 5; // small uint32 ↔ topic name + schema_id
  repeated SchemaBinding schemas      = 6; // small uint32 ↔ schema name + encoding + bytes
}
```

Topic names and schemas come over the wire **once**, identified by small `uint32`s in `MessageBatch` afterwards.

**Empty-plan contract.** Requested topics that are absent from every selected file are silently dropped during plan computation (they are not an error). If the resulting plan is empty — either because no requested topic appears in any file, or because the time-range intersects no chunk — the server still sends a normal `OpenSessionResponse` with `estimated_chunk_bytes = 0`, `approximate_messages = 0`, and an empty `topic_id_map` / `schemas`, followed immediately by `Eos{reason=COMPLETE, total_messages_sent=0}`. The client should treat this as a successful zero-result session, not an error. (Bad request shape — e.g. unknown `file_id`, `time_range.end < time_range.start`, overlapping file ranges — remains `Error{INVALID_REQUEST}`.)

### 6.4 Message batching

```proto
enum PayloadEncoding {
  PAYLOAD_ENCODING_UNSPECIFIED = 0;
  PAYLOAD_ENCODING_RAW         = 1;
  PAYLOAD_ENCODING_ZSTD        = 2;         // per-message ZSTD frame
  PAYLOAD_ENCODING_LZ4         = 3;         // per-message LZ4 frame
}

message Message {
  uint32 topic_id        = 1;
  uint32 schema_id       = 2;
  sint64 log_time_ns     = 3;
  sint64 publish_time_ns = 4;
  PayloadEncoding payload_encoding = 5;
  bytes  payload         = 6;
}

message MessageBatch {
  uint64 seq             = 1;               // monotonic per subscription, used by SessionAck
  uint64 source_file_id  = 2;               // which source MCAP this batch came from
  repeated Message messages = 3;
  reserved 4;                                // reserved for future batch-level body_encoding
  reserved "body_encoding";
}
```

**Server-side MCAP chunk handling.** Chunks fetched from S3 are decompressed in full server-side before message-level filtering. MCAP's chunk compression (`zstd` / `lz4` at the chunk container level) is a *storage* property — once a chunk's container is decompressed, the individual `Message.data` records inside are raw bytes. There is no per-message compressed form to "pass through". So the server's encoding choice for `Message.payload_encoding` is independent of the source MCAP's chunk compression: the server decompresses the chunk, filters out messages it doesn't want, and decides per-message what encoding to send.

**Batching policy on the server:**

- **Target size:** accumulate filtered messages until either (a) accumulated `payload.size()` bytes exceed `max_batch_bytes` (default 512 KB) or (b) `max_batch_age_ms` (default 50 ms) has elapsed since the first message in the batch — then flush.
- **Oversized messages:** a single message whose payload exceeds `max_batch_bytes` is sent as a **singleton batch** (one `Message` in the `messages` list, batch size > `max_batch_bytes`). This is rare-by-design (camera images, point clouds) but supported.
- **Hard frame limit:** `max_message_bytes` (default 16 MB) is the maximum size of any single `Message.payload` the server will emit. Messages exceeding this are dropped from the stream with a warning logged and the count surfaced in `Progress.dropped_messages` and (eventually) on the dashboard. This prevents pathological payloads from blocking the WS for arbitrarily long.
- **Head-of-line bound:** worst-case time a `Progress` or new RPC waits behind a session frame is `frame_size / write_throughput` — i.e. up to ~`max_message_bytes / link_speed` for a singleton oversized batch (~130 ms on a gigabit link with a 16 MB message). For the common case (typical batches under 1 MB), this is a few milliseconds.

**Per-message encoding choice on the server:** RAW for payloads `< compress_threshold_bytes` (default 4 KB); ZSTD otherwise. LZ4 is a supported inbound decode but the v1 server does not emit LZ4 (the enum value is reserved so a future server or alternative implementation can choose it without a proto change).

**Future extension (reserved, not in v1):** batch-level cross-message compression — exploiting redundancy across many small messages of the same topic (e.g. `/tf` at 1 kHz with similar transforms) — will use the reserved `MessageBatch.body_encoding` field number. v1 clients must reject batches with an unknown `body_encoding` value (defensive parsing) so when the field is added later, clients without support fail loudly rather than silently misinterpret.

**Multiplexing fairness:** the WS writer goroutine pulls from a small high-priority channel first (RPC responses + errors), then from a bulk channel (session batches). Catalog latency stays low between batches even during streaming — the priority channel can only win at frame boundaries, so the head-of-line bound above is the worst-case latency for a high-priority message. The bulk channel has a small capacity (16 frames) so a slow client backpressures the S3 fetcher naturally instead of unbounded buffering.

### 6.5 Session ack and retain

```proto
message SessionAck {
  uint64 subscription_id = 1;
  uint64 through_seq     = 2;               // client has successfully received batches with seq ≤ this
}
```

**Retain semantics.** Every emitted `MessageBatch` is appended to a per-session **retain buffer** with its `seq`. `SessionAck{through_seq=M}` prunes every batch with `seq ≤ M` from the buffer. The buffer is bounded by `retain_max_seqs` (default 256) AND `retain_max_bytes` (default 64 MB) — whichever cap is hit first applies backpressure to the producer.

**Producer/consumer split** (this is the mechanism that makes reconnect-and-resume work — see §6.6 and §8.2):

- The **producer** is the S3 chunk fetcher + MCAP iterator. It writes batches into the retain buffer.
- The **consumer** is the WS writer. It pulls batches from the retain buffer in `seq` order and writes them to the socket.
- When the retain buffer reaches `retain_max_seqs` or `retain_max_bytes`, the producer blocks (cannot produce another batch until the consumer has drained one).
- On WS disconnect, the **consumer detaches**; the producer continues until the retain buffer fills, then blocks. The session state remains live for `retain_after_disconnect` (default 60 s).
- On client reconnect with `OpenResume{subscription_id, resume_after_seq=M}`, the server: (a) prunes retain buffer entries with `seq ≤ M`, (b) re-attaches a new consumer to the WS, (c) consumer streams remaining retained batches in order, (d) producer unblocks once the buffer is drained below the caps and resumes.
- If `retain_after_disconnect` expires with no reconnect, the producer is cancelled and the session state is dropped.

### 6.6 Cancel and end-of-stream

```proto
message CancelSession { uint64 subscription_id = 1; }

enum EosReason {
  EOS_REASON_UNSPECIFIED = 0;
  EOS_REASON_COMPLETE    = 1;
  EOS_REASON_CANCELLED   = 2;
  EOS_REASON_ERROR       = 3;
}

message Eos {
  EosReason reason            = 1;
  uint64    total_messages_sent = 2;
  uint64    total_bytes_sent    = 3;
}
```

### 6.7 Errors

```proto
enum ErrorCode {
  ERROR_UNSPECIFIED         = 0;
  ERROR_AUTH_FAILED         = 1;
  ERROR_PROTOCOL_VERSION    = 2;
  ERROR_NOT_FOUND           = 3;
  ERROR_INVALID_REQUEST     = 4;
  ERROR_RESOURCE_LIMIT      = 5;
  ERROR_S3_UNAVAILABLE      = 6;
  ERROR_INTERNAL            = 7;
  reserved 8;
  reserved "ERROR_CANCELLED";   // cancellation is reported via Eos.reason, not as an Error
  ERROR_RESUME_NOT_POSSIBLE = 9;
}

message Error {
  ErrorCode code     = 1;
  string    message  = 2;     // human-readable; safe to log; ≤ 256 bytes
  string    details  = 3;     // structured/internal; truncated server-side to ≤ 2048 bytes;
                              //   may include S3 status, internal IDs — UI should display only when "verbose" mode is on
}
```

**Routing:** a non-zero `request_id` on `Error` means a failed RPC. A non-zero `subscription_id` on `Error` means a fatal stream error and is followed immediately by `Eos{reason=ERROR}`.

**Size caps:** server truncates `message` to 256 bytes and `details` to 2048 bytes before sending. Clients should not surface `details` by default (it can include infrastructure-leaky strings like AWS request IDs).

## 7. End-to-end user flow

```
1. User invokes "Open Cloud Session" in client (CLI subcommand or, later, PJ4 menu).
2. Client opens WSS to configured server URL.
   → Hello{auth_token, protocol_version=1}
   ← HelloResponse{server_version, capabilities}
3. Client browses catalog (multiple round trips as user filters/scrolls).
   → ListFilesRequest{filter:{time_range, topics_any_of, tag_predicates}, page_token, limit}
   ← ListFilesResponse{files:[…summary…], next_page_token}
   → GetFileRequest{file_id}   (lazy, per selected file)
   ← GetFileResponse{file_detail{all_topics, all_tags, exact_chunk_count}}
4. User picks files + topics + (optional) time range.
   → OpenSessionRequest{ fresh: OpenFresh{file_ids, topic_names, time_range} }
   ← OpenSessionResponse{subscription_id, merged_time_range,
                          estimated_chunk_bytes, approximate_messages,
                          topic_id_map, schemas}
5. Client initializes SessionSink with the schemas + topic_id_map.
6. Server pumps:
   loop:
     ← MessageBatch{seq, source_file_id, messages:[…]}
     ← Progress{bytes_sent, messages_sent}      (every ~1 s)
   Client routes each Message via topic_id → schema_id → sink->writeMessage.
   Client periodically:
     → SessionAck{subscription_id, through_seq=last_received_seq}
7. End:
   ← Eos{subscription_id, reason=COMPLETE, total_messages_sent, total_bytes_sent}
8. Client calls sink->end(COMPLETE). Closes WS.
```

**Cancel:** at any time client may send `CancelSession{subscription_id}`. Server wind-down is bounded by the in-flight S3 fetch (~ms to seconds); `Eos{CANCELLED}` arrives, sink discards partial output.

**Reconnect-and-resume:** if the WS drops mid-stream, the server detaches the WS-writer consumer; the producer keeps filling the retain buffer until the caps are reached, then blocks. The session state is held for `retain_after_disconnect` (default 60 s). Client reconnects, re-sends `Hello`, then sends `OpenSessionRequest{ resume: OpenResume{subscription_id, resume_after_seq=last_received_seq} }`. Server prunes acked-or-already-seen batches (`seq ≤ resume_after_seq`), re-attaches the WS-writer, replays remaining buffered batches in order, and the producer resumes when the buffer drains below the caps. If `retain_after_disconnect` expires before reconnect, the producer is cancelled and server returns `Error{RESUME_NOT_POSSIBLE}` on the next resume attempt — client must reopen from scratch (`OpenFresh`).

## 8. Server design (Go)

### 8.1 Concurrency model

| Goroutine | Count | Lifetime |
|---|---|---|
| HTTP/WS listener | 1 | server lifetime |
| Per-WS read loop | 1 per connection | connection lifetime |
| Per-WS write loop | 1 per connection | connection lifetime; serializes via two channels (priority + bulk) |
| Per-active-session **producer** (S3 fetcher + MCAP iterator) | 1 per session | session lifetime; ctx-cancelled on `Cancel`, on retain expiry after disconnect, or on shutdown |
| Per-active-session **consumer** (retain-buffer drain → WS bulk channel) | 1 per session, attached to a WS | re-bound on resume; detaches (does not exit) on WS disconnect |
| Indexer | 1 | server lifetime |
| **Catalog writer** (serializes SQLite writes) | 1 | server lifetime; receives indexer + tag-update jobs over a channel |
| S3 request semaphore | implicit | per Range GET; bounded by `s3_concurrent_requests` |

Typical load (~10 users × 1 active session each): ~40 goroutines. Trivial.

**Producer/consumer split.** As described in §6.5, each active session has two cooperating goroutines: the producer fetches chunks from S3 and writes batches into the retain buffer; the consumer drains the buffer in `seq` order to the WS bulk channel. They communicate through the retain buffer (which doubles as the resume buffer) plus a small "buffer has room" condition variable. On WS disconnect, only the consumer detaches — the producer keeps running until the retain caps are reached, then blocks on the condition. This is the mechanism that makes resume work; see §6.5 for the wire-level semantics.

**SQLite writer policy.** SQLite WAL allows many readers concurrent with one writer. Catalog reads (catalog WS handlers, dashboard) hit the DB directly via a small connection pool. Catalog writes (indexer upserts, `UpdateTagsRequest` handlers) are funneled through a single **catalog writer goroutine** with a buffered job channel: each job is a closure that takes a `*sql.Tx`. The writer goroutine opens a transaction per job, commits, replies on a result channel. `PRAGMA busy_timeout=5000` is set on every connection as a backstop. This eliminates the SQLite-writer contention class entirely without needing reader/writer locks in app code.

**Panic recovery.** All goroutines run inside `defer recover()` wrappers scoped to the smallest reasonable unit (one WS connection, one session, one indexer iteration). On recovery the wrapper: (a) logs the stack with `slog.Error`, (b) increments a Prometheus counter `pj_cloud_panic_total{scope=<...>}`, (c) closes the affected scope cleanly (the WS connection, the session, the iteration). One bad request cannot crash the process; one bad session cannot break other sessions on the same connection.

### 8.2 Session plan, producer, and consumer

**Plan.** `session.BuildPlan` runs synchronously inside the `OpenFresh` handler:

```
plan = session.BuildPlan(file_ids, topic_names, time_range)
  # Validate: file ranges pairwise non-overlapping (§6.3).
  # For each file in time order:
  #   Read MCAP summary section (cached after first fetch; see §8.3).
  #   For each requested topic that exists in the file:
  #     Find chunks intersecting time_range; append (file_id, chunk_offset, chunk_length, topics).
  #   Sum compressed chunk sizes → estimated_chunk_bytes.
  #   Sum per-chunk per-channel message counts (MessageIndex if present, else channel-level count) → approximate_messages.
sub = session.Register(plan, wsWriter)        # creates retain buffer, registers in registry
send OpenSessionResponse{ subscription_id: sub.id, ... }
go sub.RunProducer(ctx)                       # detached from WS lifetime
go sub.RunConsumer(ctx, wsWriter)             # bound to WS; re-spawned on resume
```

**Producer goroutine.** Owns S3 fetches + MCAP iteration + batch assembly. Writes into the retain buffer.

```
sub.RunProducer(ctx):
  batch = newBatchBuilder(maxBytes=cfg.MaxBatchBytes, maxAge=cfg.MaxBatchAgeMs)
  for chunkRef in plan:
    if ctx.Err() != nil: return                       # cancelled (Cancel / retain expiry / shutdown)
    chunkBytes = s3reader.RangeGet(chunkRef.file, chunkRef.offset, chunkRef.length)
    decompressed = decompressMcapChunk(chunkBytes)    # ZSTD/LZ4/none → raw chunk records (see §6.4)
    for msg in mcap.IterateMessages(decompressed, filter=chunkRef.topics, time_range):
      if len(msg.data) > cfg.MaxMessageBytes:
        log.Warn(...); sub.dropped++; continue
      payload, encoding = maybeCompress(msg.data)     # ZSTD if > threshold (§6.4)
      if batch.wouldExceedTargetWith(payload) and batch.notEmpty():
        sub.RetainAppend(batch.build(chunkRef.file_id))    # blocks if retain caps reached
        batch.reset()
      batch.add(topic_id_for(msg.topic), schema_id_for(msg.schema),
                msg.log_time, msg.publish_time, payload, encoding)
      if batch.bytes() >= cfg.MaxBatchBytes or batch.age() >= cfg.MaxBatchAgeMs:
        sub.RetainAppend(batch.build(chunkRef.file_id))
        batch.reset()
  if batch.notEmpty(): sub.RetainAppend(batch.build(lastFileId))
  sub.MarkProducerDone()                              # consumer will send Eos when buffer drains
```

`sub.RetainAppend(b)`: assigns `seq = sub.nextSeq()`, appends `(seq, bytes, b)` to the retain buffer, blocks while `buffer.seqs > cfg.RetainMaxSeqs` or `buffer.bytes > cfg.RetainMaxBytes` until the consumer drains room.

**Consumer goroutine.** Bound to a single WS for the life of one attachment. Re-spawned on resume.

```
sub.RunConsumer(ctx, w):
  for {
    select {
    case batch := <-sub.RetainNext():                 # blocks until next batch is ready in seq order
      if !w.SendBulk(MessageBatch{batch.seq, batch.source_file_id, batch.messages}):
        sub.DetachConsumer(); return                  # WS write failed; producer keeps running until retain caps
      sub.MarkConsumed(batch.seq)                     # waits for next SessionAck before pruning beyond this
    case ack := <-sub.AckCh():
      sub.PruneRetain(throughSeq=ack.through_seq)     # frees buffer; producer may unblock
    case <-progressTicker.C:
      w.SendPriority(Progress{ bytes_sent, messages_sent, dropped_messages: sub.dropped })
    case <-sub.ProducerDoneCh():
      if sub.RetainEmpty():
        w.SendPriority(Eos{COMPLETE, total_messages_sent, total_bytes_sent})
        sub.Close()
        return
    case <-ctx.Done():
      sub.DetachConsumer(); return
    }
  }
```

**Cancel:** `CancelSession{subscription_id}` from the client triggers `sub.Cancel()` which cancels both producer and consumer contexts and sends `Eos{CANCELLED}`. Retain buffer is discarded; session de-registered. Wind-down is bounded by the in-flight S3 Range GET (which has its own ctx cancellation).

**Resume:** on `OpenResume{subscription_id, resume_after_seq=M}`, server: (a) looks up `sub` in registry; if missing or evicted → `Error{RESUME_NOT_POSSIBLE}`. (b) validates the new WS's auth matches the original. (c) calls `sub.PruneRetain(throughSeq=M)`. (d) attaches the new WS and spawns a fresh `RunConsumer`. (e) returns `OpenSessionResponse` with the same `subscription_id`, `topic_id_map`, `schemas`, and updated `estimated_chunk_bytes` / `approximate_messages` reflecting what is *still pending*. The producer was never stopped (if retain expiry hadn't fired), so it continues without restart.

`session.Registry` maps `subscription_id → *Session` and runs an eviction timer per detached session: when the WS detaches, `time.AfterFunc(cfg.RetainAfterDisconnect, sub.Evict)` is armed; reattach cancels the timer.

### 8.3 S3 reader

`internal/s3reader` exposes an `io.ReadSeeker` for each S3 object via Range GETs. A small server-wide semaphore (default 32 concurrent in-flight GETs) prevents one greedy session from starving others. For large chunks the S3 response body is streamed (the MCAP-Go lexer can read incrementally) so chunk bytes are never fully buffered before iteration begins.

### 8.4 Failure handling

| Failure | Detection | Recovery |
|---|---|---|
| S3 transient (503, 429, timeout) | per-Range-GET | Exponential backoff 50→100→200→400→800 ms, max 5 retries |
| S3 permanent (403, 404, NoSuchBucket) | per-Range-GET | No retry; fail the session with `Error{S3_UNAVAILABLE}` + `Eos{ERROR}` |
| Indexer S3 list fail | per-poll-cycle | Retry next cycle; surface on dashboard `/dashboard/indexer` |
| Indexer file parse fail | per-file | Record `(s3_key, error)` in `indexer_failures`; skip; surface on dashboard |
| Indexer DB write fail | per-transaction | Rollback; retry next cycle; WAL means stale-but-consistent catalog remains servable |
| WS write timeout > 30 s | per-conn | Cancel WS write context, close conn, subscriptions enter retain |
| Server panic in handler | `defer recover()` | Log + close the affected scope (WS conn or session); process keeps running |
| Server crash | OS / supervisor | systemd / Docker restart; retain buffers lost; clients reopen from scratch |

### 8.5 HTTP admin dashboard

Same Go binary, same TCP listener, same TLS cert. Routes:

```
GET  /                      → 301 /dashboard/
GET  /dashboard/            → overview (server stats, indexer status, active sessions)
GET  /dashboard/files       → paginated file list, filterable
GET  /dashboard/files/{id}  → file detail (topics, schemas, tags, both layers)
GET  /dashboard/sessions    → active sessions: subscription_id, client addr, files, progress
GET  /dashboard/indexer     → indexer status, last run, recent errors, queued keys
GET  /health                → 200 / 503; no auth
GET  /metrics               → Prometheus exposition; optional auth
GET  /static/{file}         → CSS, favicon (go:embed)
```

Auth: HTTP Basic, configured via `dashboard.basic_auth` block; dashboard disabled until configured. `/health` and `/metrics` unauthenticated by default.

Templates: stdlib `html/template`, embedded via `//go:embed`. Styling: [pico.css](https://picocss.com/) (single ~10 KB CSS file, embedded). No JS framework. Optional ~20 lines of vanilla JS for `/dashboard/sessions` auto-refresh.

Data queries reuse the same `internal/catalog.Store` methods used by the WS handlers. Session info comes from the existing `session.Registry`.

### 8.6 Config

```yaml
server:
  listen: ":8443"
  tls:
    cert: /etc/pj-cloud/server.crt
    key:  /etc/pj-cloud/server.key
auth:
  bearer_token: ${PJ_CLOUD_TOKEN}              # env-expanded
storage:
  s3:
    bucket: my-team-recordings
    region: us-east-1
    prefix: ""
catalog:
  db_path: /var/lib/pj-cloud/catalog.db
indexer:
  poll_interval: 5m
  startup_scan: true
session:
  max_concurrent: 16
  retain_after_disconnect: 60s
  retain_max_seqs: 256
  retain_max_bytes: 67108864                   # 64 MB
  s3_concurrent_requests: 32
  max_batch_bytes: 524288                      # 512 KB
  max_batch_age_ms: 50
  compress_threshold_bytes: 4096
  write_timeout: 30s
dashboard:
  enabled: true
  basic_auth:
    username: admin
    password: ${PJ_CLOUD_DASHBOARD_PASSWORD}
metrics:
  enabled: true
  require_auth: false
```

## 9. Client design (Qt C++ test client)

### 9.1 Library / driver split

```
client-core   → static library; Qt-aware (QtCore, QtNetwork, QtWebSockets), NO Qt6::Widgets
client-cli    → QCoreApplication executable; links client-core + libmcap
```

The `Qt6::Widgets`-free boundary matches `pj_runtime`'s rule in PJ4 and ensures the library can later land in PJ4 without any layering refactor. Headless `QTest` + `QCoreApplication` event loops make it CI-friendly.

### 9.2 SessionSink — the seam

```cpp
class SessionSink {
 public:
  virtual ~SessionSink() = default;
  virtual PJ::Expected<void> begin(const wire::OpenSessionResponse& session) = 0;
  virtual PJ::Expected<void> writeMessage(
      uint32_t topic_id, uint32_t schema_id,
      int64_t log_time_ns, int64_t publish_time_ns,
      std::span<const std::byte> payload) = 0;
  virtual void onProgress(uint64_t bytes_received, uint64_t messages_written) {}
  virtual PJ::Expected<void> end(wire::EosReason reason) = 0;
};
```

The library does not know whether messages end up in an MCAP file, `pj_datastore`, or `/dev/null`. Three sinks land at different points in the project's life:

| Sink | Owner | Lives in |
|---|---|---|
| `McapWriterSink` | test CLI | `client-cli/src/` |
| `NullSink` / `CountingSink` | tests + benchmarks | `client-core/tests/` |
| `DatastoreSink` | future PJ4 plugin | `PJ4/pj_plugins/pj_cloud/` (separate spec) |

### 9.3 Core classes

- **`CloudConnection`** — `QObject` wrapping `QWebSocket`; owns the TLS connection, performs Hello handshake, routes incoming `binaryMessageReceived` to `MessageDispatcher`. Exposes `connect()`, `disconnect()`, `disconnected(QString reason)` signal.
- **`MessageDispatcher`** — request_id / subscription_id routing. `request<Resp>(msg) → QFuture<Expected<Resp>>` for RPC; `subscribe(sub_id, callback)` + `unsubscribe` for streams. Thread-safe lookup tables.
- **`CatalogClient`** — convenience wrappers over `Dispatcher::request` for `ListFiles`, `GetFile`, `UpdateTags`.
- **`SessionClient`** — drives one session lifecycle: open → batches → Eos. Owns the per-session decompression scratch buffers and the SessionAck pacing (every ~64 batches or ~500 ms).

### 9.4 Threading

One `QThread` owns `CloudConnection` + `SessionClient` + the active sink. `QCoreApplication`'s event loop drives async work via `QFuture`. The CLI's `main` spins the event loop until the top-level future resolves, then exits. Decompression runs inline on the worker thread with reused thread-local scratch buffers; no separate worker pool needed at the target throughput.

### 9.5 CLI shape

```bash
pjcloud-cli --server wss://localhost:8443 --token "$PJ_CLOUD_TOKEN" \
    files list --filter 'time_after=2026-05-01T00:00:00Z'

pjcloud-cli files show <file_id>
pjcloud-cli files tag <file_id> --set vehicle=7 --set verified=yes
pjcloud-cli files tag <file_id> --unset wrongkey

pjcloud-cli session download \
    --files 142,143,144 \
    --topics /imu/data,/gps/fix,/tf \
    --time-range '2026-05-12T14:03:00Z,2026-05-12T14:10:00Z' \
    --output reconstructed.mcap \
    --progress

pjcloud-cli session debug --files 142 --topics /imu/data --max-messages 10
```

`--json` flag on read commands for machine-readable output. Token resolved from `--token` flag or `$PJ_CLOUD_TOKEN` env var. No keychain integration in the CLI (the PJ4 plugin gets qtkeychain later).

### 9.6 Dependencies (Conan)

```
qt/6.8.3                    # QtCore, QtNetwork, QtWebSockets
protobuf/3.21
zstd/1.5
lz4/1.9
mcap/2.0                    # Foxglove mcap-cpp
gtest/1.14
spdlog/1.13
```

All standard; same Qt version already used by PJ4.

## 10. Failure handling, cancel, resume — wire-level details

(Refer to §6.7 for error codes, §7 for the canonical flow, §8.4 for server failure modes. This section summarizes the wire contract.)

| Scenario | Wire signal | Client recovery |
|---|---|---|
| Bad token | `Error{AUTH_FAILED}` (on Hello's `request_id`) | Abort; re-prompt user / fail CLI |
| Protocol mismatch | `Error{PROTOCOL_VERSION}` | Abort; report to user |
| Bad request params | `Error{INVALID_REQUEST}` on `request_id` | Report to caller |
| Server at session cap | `Error{RESOURCE_LIMIT}` on OpenSession `request_id` | Client may retry later |
| S3 read fails after retries | `Error{S3_UNAVAILABLE}` + `Eos{ERROR}` on `subscription_id` | Abort session; surface error |
| WS drop, retain window open | (silent on the wire; client observes connection close) | Reconnect → `Hello` → `OpenSessionRequest{ resume: OpenResume{subscription_id, resume_after_seq} }` → server re-attaches consumer and replays |
| WS drop, retain expired | `Error{RESUME_NOT_POSSIBLE}` on resume attempt | Reopen session from scratch |
| User cancels | `CancelSession` → `Eos{CANCELLED}` | Sink discards partial output |
| Server crashes | WS just drops; no wire signal | Reconnect; resume fails; reopen from scratch |

**Client reconnect-and-resume loop (illustrative):**

```cpp
PJ::Expected<wire::EosReason> SessionClient::runSession(wire::OpenSessionRequest req) {
  uint64_t last_seq = 0;
  for (int attempt = 0; attempt < kMaxReconnectAttempts; ++attempt) {
    auto open_result = co_await sendOpenSession(req);
    if (!open_result) return open_result.error();
    if (attempt == 0) co_await sink_->begin(open_result->session);

    while (auto frame = co_await dispatcher_.nextFrameFor(subscription_id_)) {
      // ...process batches, update last_seq, send SessionAck periodically...
    }

    if (connection_dropped) {
      co_await delay(reconnect_backoff(attempt));    // 1s, 4s, 16s
      co_await conn_.reconnect();                    // new WSS + Hello
      auto* resume = req.mutable_resume();
      resume->set_subscription_id(subscription_id_);
      resume->set_resume_after_seq(last_seq);
      continue;
    }

    if (eos_received) {
      co_await sink_->end(eos_reason);
      return eos_reason;
    }
  }
  return PJ::Error("max reconnect attempts exhausted");
}
```

`kMaxReconnectAttempts = 3`; backoff `[1 s, 4 s, 16 s]`; after exhaustion the session aborts with an error.

## 11. Testing strategy

### Layer 1 — Unit (per-language, fast, hermetic)

| Target | Lang | Proves |
|---|---|---|
| `catalog.Store` query methods | Go | Filter predicates produce right SQL; merged tags view returns expected rows; pagination boundaries |
| `indexer.extractor` | Go | Given a known MCAP fixture, extracts correct `(start_time, end_time, topics, tags)` |
| `session.plan` | Go | Synthetic chunk index + `(topics, time_range)` → right ordered chunk refs + estimates |
| `s3reader` | Go | Range GET concat correctness; retry policy fires for 503 and bails on 403 |
| `wire/envelope` | Go AND C++ | Encode → decode round-trip preserves every field; same test corpus on both sides |
| `MessageDispatcher` | C++ QTest | request_id correlation; out-of-order replies; subscription routing |
| `SessionClient` state machine | C++ QTest | Open→batches→Eos; cancel; resume path with right `resume_after_seq` |
| `Decompression` | C++ | ZSTD/LZ4 round trip on fixtures; truncated input fails cleanly |

Target runtime: full unit suite < 30 s per language. Run on every commit.

### Layer 2 — Component integration (single language, live deps)

**Server (Go):** real SQLite DB + in-process `minio` (or moto), exercising the public WS protocol with a Go-internal client. Coverage: catalog browse with filters, tag override flow, indexer warm-start and incremental updates, full session lifecycle (single + multi-file + time-subset + compressed source), cancel, resume (success + eviction), S3 transient + permanent failure, backpressure under slow consumer, concurrency cap (`max_concurrent` reached → `Error{RESOURCE_LIMIT}`), dashboard rendering + auth, `/health` and `/metrics` reachable without auth.

**Client-core (C++):** a `fake-pj-cloud-server` Go binary in `integration-tests/fake-server/` replays canned protocol scripts. C++ tests point `CloudConnection` at it, verify state machine: normal lifecycle, decompression dispatch per `PayloadEncoding`, cancel path, reconnect-and-resume with simulated drop, malformed envelope rejection.

### Layer 3 — End-to-end round-trip (cross-language)

Real Go server + real C++ CLI + real Minio, full byte-equal MCAP comparison. Matrix:

```
files × topic-selection × time-range × concurrency
  └── files: { 1, 3, 10 }
  └── topic-selection: { all, half, single, none-matching }
  └── time-range: { full, middle-30%, spans-boundary, outside-range }
  └── concurrency: { sequential, 4-parallel-sessions }
```

~32 test cases, each asserting **logical equality** (not container-byte-equality — MCAP writers may legitimately differ on chunking strategy, summary record ordering, and chunk compression while preserving the same messages):

- Exit code as expected.
- Reconstructed MCAP opens cleanly with `mcap-cpp`.
- For each `(topic, log_time)` in the original's time-range slice: a corresponding record exists in the reconstructed MCAP with byte-equal `payload`, equal `publish_time`, and equal `channel.schema.name` + `channel.schema.encoding` + `channel.schema.data`.
- `estimated_chunk_bytes` is within 5 % of the actual S3 fetch volume reported by the server.

Runtime budget < 5 min on CI.

### Layer 4 — V1 benchmark gate (small but real)

Three quick benchmarks gating release, run nightly and on tagged commits. Not aspirational SLOs — just smoke checks that the wire choices don't have an order-of-magnitude problem we'd be stuck with once the C++ plugin ships:

| Benchmark | Setup | Pass criterion |
|---|---|---|
| Streaming throughput | Server + Minio on same host, 1 GB session, 4 KB messages, RAW encoding | ≥ 200 MB/s sustained on a developer workstation; flag regressions > 25 % |
| Compression CPU cost | Same session, ZSTD encoding everywhere | Throughput ≥ 80 MB/s; CPU usage observable but not pegged |
| Slow-consumer backpressure | Client throttled to 1 MB/s, server runs full session | No goroutine leak (count stable); priority frame (`Progress`) latency < 200 ms p99 even while bulk frames are being written; no OOM (RSS bounded) |

Each benchmark writes a JSON result file; a small Go program in `integration-tests/bench/` compares against a committed baseline and fails CI on regression. Baseline is updated explicitly by PRs that intend to move it.

### Fixtures

`integration-tests/gen_fixtures.{cpp,go}` deterministically produces:

- `single-topic-uncompressed.mcap`
- `multi-topic-zstd.mcap` (8 topics, chunk ZSTD)
- `with-embedded-tags.mcap` (metadata records carrying `vehicle=…`, `run=…`)
- `large-payloads.mcap` (4 MB per message; exercises compression threshold + batch byte cap)
- `tiny-payloads.mcap` (1000 Hz, 40-byte messages; exercises batching)
- `consecutive-{1,2,3}of3.mcap` (contiguous time ranges, overlapping topics, schema drift on file 3)
- `empty-time-range.mcap` (short burst near start; verify time-range subset that misses entirely)
- `corrupt-chunk.mcap` (hand-edited; error-path tests only, gated behind a flag)

Fixtures committed to git (~5 MB total) so CI is hermetic. Generator script retained.

### CI matrix

| Job | Trigger | Runtime |
|---|---|---|
| Go unit + lint | every push | ~30 s |
| Go race detector unit | every push | ~60 s |
| C++ unit (QTest) + clang-tidy | every push | ~2 min |
| Server component integration (minio, no C++) | every push | ~2 min |
| Client-core component integration (fake-server, no S3) | every push | ~1 min |
| Cross-language E2E round-trip | every push to main, nightly on branches | ~5 min |

### Not in v1 test scope

- Performance benchmarks beyond "completes within timeout". Add `go test -bench` and Grafana post-v1.
- Real AWS S3 (vs Minio). Periodic manual smoke test.
- Multi-day soak. Manual before each release.
- Envelope fuzz testing. Post-v1.

## 12. Decisions captured (quick reference)

| | |
|---|---|
| **Playback model** | Progressive streaming, lazy by topic+range, as-fast-as-possible (not realtime) |
| **Catalog** | Separate metadata DB queried by server. SQLite + WAL. |
| **Topic selection** | Pre-selected at session start (no mid-stream subscribe) |
| **Stitching** | Server-side; client sees one continuous session |
| **Wire format** | Raw MCAP message records `(topic, ts, schema, payload)` + schemas in `OpenSessionResponse` |
| **Client storage (v1 test CLI)** | Reconstructed MCAP file via `McapWriterSink` |
| **Client storage (future PJ4)** | `pj_datastore` memory via `DatastoreSink`; no local disk cache |
| **Deployment scope** | Single team, self-hosted, trusted network |
| **Auth** | Single shared bearer token (placeholder for OIDC later) |
| **Indexer** | Server-internal background poller; S3 List + tail Range GET per new key |
| **Tags** | Two-layer DB model: embedded (re-derived) + override (sticky). Query view = override ∪ embedded. v1 tags are freeform `string key, string value`. Typed tags post-v1. |
| **Pre-flight** | `estimated_chunk_bytes` (S3-fetch upper bound) + `approximate_messages` (exact when MessageIndex present) in `OpenSessionResponse` |
| **Transport** | Single persistent WebSocket per client; binary frames; Protobuf envelope; mini-RPC over WS (request_id + subscription_id) |
| **Session open** | `OpenSessionRequest` is a `oneof { OpenFresh, OpenResume }` — no ambiguous-fields mode |
| **File-overlap rule** | Selected files' time ranges must be pairwise non-overlapping; otherwise `Error{INVALID_REQUEST}` |
| **Message granularity** | Per-message (raw MCAP message records, decompressed from source chunks); batched ~512 KB / ~50 ms; oversized messages become singleton batches up to `max_message_bytes` (16 MB) |
| **Payload encoding** | Per-message `RAW`/`ZSTD`/`LZ4` (wire-level, independent of source MCAP chunk compression); server compresses when payload > 4 KB. Reserved future: batch-level `body_encoding`. |
| **File-boundary tracking** | `source_file_id` on every batch; no separate boundaries map |
| **Resume** | Producer/consumer split; retain buffer (256 seqs / 64 MB) backpressures producer; `SessionAck` prunes; consumer detaches on WS drop, re-binds on `OpenResume`; 60 s expiry |
| **WS lifecycle** | Closes after each session (or dialog dismissal); no cross-session state |
| **Server language** | Go (mcap-go, aws-sdk-go-v2, nhooyr.io/websocket, modernc.org/sqlite, html/template, pico.css) |
| **Server packaging** | Single static binary; Docker image ~10-line distroless |
| **Client language** | Qt C++ (QtCore + QtNetwork + QtWebSockets); library/driver split |
| **Client packaging** | CMake + Conan, same toolchain as PJ4 |
| **DB** | SQLite with WAL; portable SQL to allow Postgres migration later |
| **Dashboard** | Same Go binary, same port, same TLS cert; HTTP Basic auth |

## 13. Phased build order

| Phase | Deliverable | Where |
|---|---|---|
| **1a** | `pj_cloud.proto` checked in; `protoc` codegen wired for Go + C++ | `pj-cloud/proto/` |
| **1b** | Go server v1 (Sections 4 + 6 + 8 of this spec) | `pj-cloud/server/` |
| **2** | `client-core` Qt C++ library (Section 9.1–9.4) | `pj-cloud/client-core/` |
| **3** | `client-cli` CLI driver + `McapWriterSink` + round-trip tests | `pj-cloud/client-cli/` + `pj-cloud/integration-tests/` |
| **4** (deferred) | PJ4 `pj_cloud` DataSource plugin: lift `client-core` into PJ4; add `DatastoreSink`; add `CloudOpenDialog` + `ProgressDialog`; vendor `qtkeychain`; register `pjcloud://` URI scheme; add `FileSourceBase::launchCustomOpenDialog()` SDK hook in `plotjuggler_core/pj_plugins/` | `PJ4/pj_plugins/pj_cloud/` — separate spec |

Each phase is independently testable. Phase 4 is captured here only by reference; it gets its own design + plan when v1 ships and stabilizes.

## 14. Open items intentionally left for the implementation plan

- **`ListFilesRequest.filter` Protobuf shape.** The filter envelope is referenced throughout the spec (time predicates, topic-existence predicates, tag predicates) but the exact Protobuf message is not pinned. Implementation plan will define `FileFilter` as a structured predicate (e.g. `repeated TagPredicate tag_any` + `repeated TagPredicate tag_all` + `TimeRange recorded_between` + `repeated string topics_any_of`) rather than a free-form string DSL. Pagination: `string page_token` + `uint32 limit` (default 200, max 1000).
- Exact `nhooyr.io/websocket` vs `gorilla/websocket` choice — both viable; deferred.
- Whether the Go server vendors a small Prometheus client library or exposes `expvar`-style metrics — deferred to phase 1b.
- Whether the CLI uses `QCommandLineParser` or a richer library (e.g. `argparse` C++) — `QCommandLineParser` is fine; revisit only if subcommand UX becomes painful.
- Reverse-proxy guidance (nginx / Caddy snippets) — deferred to deploy docs.
- Specific Conan recipe versions — pinned in `conanfile.txt` at the start of phase 2.
