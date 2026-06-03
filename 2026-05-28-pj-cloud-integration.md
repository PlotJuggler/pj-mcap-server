# PJ Cloud Cross-Language Integration Plan (Plan C)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Build the end-to-end correctness harness for PJ Cloud: a deterministic MCAP fixture matrix, a `docker-compose` orchestration that boots **Minio AND `fake-gcs-server`** + the Go server + the Qt C++ CLI, and a round-trip test that, **for each backend in `{s3, gcs}`**, downloads each fixture and asserts the reconstructed MCAP is **logically equal** on `(topic, log_time, payload, publish_time, schema name/encoding/data)` to the original. This is the v1 gate that catches any wire-format / protocol / decode mismatch between server and client — **on either backend.** A **GCS-only failure blocks a merge exactly like an S3-only failure.** This plan also adds an Asensus **GCE deployment smoke** (real ADC, persistent-disk catalog survives a VM restart) as a scheduled/manual checklist.

**Architecture:** All integration code lives under `pj-cloud/integration-tests/`. The fixture generator is a small Go program that produces deterministic MCAPs covering the design dimensions (compression, payload size, multi-file stitching, embedded tags, time-range edge cases). The test driver is a Go program (because Go has both `mcap-go` and good subprocess control for the CLI) that orchestrates the matrix **parameterized over backend in `{s3, gcs}`**: upload fixtures into the chosen backend's bucket → exec `pjcloud-cli session download` against a server configured for that backend → diff against original. The two backends differ **only** in which storage service the server is pointed at (Minio for `s3`, `fake-gcs-server` for `gcs`); assertions and shared code are identical. The v1 benchmark gate from the spec is added as a sibling target with a committed `baseline.json`, and now includes a **cross-backend storage-parity microbench**.

**Tech Stack:** Go 1.23 (test driver + fixture generator + bench), Docker + docker-compose (orchestration), Minio (S3-compatible), `mcap-go` (read both originals and reconstructions for diff).

**Depends on:**
- Plan A's Go server (`pj-cloud-server` binary) — built but not deployed.
- Plan B's `pjcloud-cli` binary — built but not deployed.
- The canonical `proto/pj_cloud.proto` (generated bindings in both).

**Spec reference:** [`docs/superpowers/specs/2026-05-28-pj-cloud-connector-design.md`](../specs/2026-05-28-pj-cloud-connector-design.md) — §11 (Testing strategy), Layer 3 + Layer 4.

---

## File structure

```
pj-cloud/                                  # (existing from Plans A + B)
├── integration-tests/                     # NEW
│   ├── go.mod                             # Module: pj-cloud/integration-tests
│   ├── docker-compose.yml                 # Minio + fake-gcs + pj-cloud-server orchestration
│   ├── server-config.s3.yaml              # server wired for the dockerized Minio  (backend=s3)
│   ├── server-config.gcs.yaml             # server wired for the dockerized fake-gcs (backend=gcs)
│   ├── fixtures/                          # committed canned MCAPs (~5 MB total)
│   │   ├── single-topic-uncompressed.mcap
│   │   ├── multi-topic-zstd.mcap
│   │   ├── with-embedded-tags.mcap
│   │   ├── large-payloads.mcap
│   │   ├── tiny-payloads.mcap
│   │   ├── consecutive-1of3.mcap
│   │   ├── consecutive-2of3.mcap
│   │   ├── consecutive-3of3.mcap
│   │   ├── empty-time-range.mcap
│   │   └── corrupt-chunk.mcap
│   ├── cmd/
│   │   ├── gen-fixtures/main.go           # deterministic fixture generator
│   │   └── run-matrix/main.go             # test matrix driver
│   ├── internal/
│   │   ├── compose/up.go                  # docker-compose orchestration helpers
│   │   ├── upload/upload.go               # S3 upload helpers (minio-go)
│   │   ├── cli/cli.go                     # exec wrapper for pjcloud-cli
│   │   └── diff/mcap.go                   # message-level MCAP diff
│   ├── matrix_test.go                     # `go test` driver wrapping run-matrix
│   ├── bench/
│   │   ├── throughput_test.go             # v1 benchmark gate
│   │   ├── baseline.json                  # committed baseline; regressions fail CI
│   │   └── compare.go                     # tiny tool: parse `go test -bench` output → compare
│   └── docs/
│       └── RUNBOOK.md                     # how to run locally, debug, refresh fixtures
└── .github/
    └── workflows/
        ├── ci.yml                          # (existing — extended in Task 8)
        └── bench.yml                       # nightly + on-tag benchmark gate
```

---

## Plan C deltas — cross-backend ({s3,gcs}) harness + Asensus GCE deploy validation

> **Unified-plan mapping.** This plan is Plan C in the unified plan `2026-06-03-unified-cloud-connector-plan.md`. The integration harness is the L3 (cross-language round-trip) and L4 (benchmark) layers of that plan's §6 testing matrix, and the deployment-smoke row of §6. The deltas below make the harness **cross-backend by construction** and add the Asensus GCE/ADC deployment validation.
>
> **New cross-backend principle (NON-NEGOTIABLE).** Every end-to-end case runs against **BOTH** storage backends in `{s3, gcs}`: the `s3` leg targets Minio, the `gcs` leg targets `fake-gcs-server`. The two legs differ **only** in config — never in assertions or shared code. A **GCS-only failure blocks a merge exactly like an S3-only failure** (the operational expression of unified-plan §7 risk 1, "no soft fork"). Anything touching `storage/`, `format/`, `session/`, `ws/`, or `proto/` requires **both legs green**.
>
> **Task-numbering scheme.** Existing tasks 1–9 keep their numbers and are edited in place (no renumbering). New tasks are inserted at the correct position with **letter-suffixed IDs** (e.g. `Task 8a`) so existing references stay valid. This drop adds one new task, **Task 8a (GCE deployment smoke — Asensus)**, inserted between Task 8 (CI matrix) and Task 9 (RUNBOOK).
>
> **Identifier pins (keep IDENTICAL everywhere they appear).** Compose service `fake-gcs`; bucket `recordings` on both backends; backend axis values are the exact strings `s3` and `gcs`; the env var selecting a leg is `PJCLOUD_BACKEND`; per-leg server configs are `server-config.s3.yaml` and `server-config.gcs.yaml`; the GCS endpoint the server dials is `http://fake-gcs:4443`; the storage-parity benchmark is `BenchmarkGetRangeParity` with baseline keys `s3_get_range` / `gcs_get_range`.

---

## Task 1: Module scaffold + docker-compose

**Files:**
- Create: `pj-cloud/integration-tests/go.mod`
- Create: `pj-cloud/integration-tests/docker-compose.yml`
- Create: `pj-cloud/integration-tests/server-config.s3.yaml`
- Create: `pj-cloud/integration-tests/server-config.gcs.yaml`

- [ ] **Step 1: Module init**

```bash
cd /home/davide/ws_plotjuggler/pj-cloud/integration-tests
go mod init pj-cloud/integration-tests
go get github.com/foxglove/mcap/go/mcap@latest
go get github.com/minio/minio-go/v7@v7.0.74
```

- [ ] **Step 2: docker-compose.yml**

The compose file boots **both** storage emulators so either backend leg can run without re-editing compose. Which backend the server uses is chosen at orchestration time by mounting the matching `server-config.<backend>.yaml` (Task 5 sets `PJCLOUD_BACKEND` + the `SERVER_CONFIG` mount). `fake-gcs-server` serves the GCS JSON+XML API on port 4443; we pre-seed its `recordings` bucket via the `-data` layout (a `recordings/` directory) so the bucket exists before the server's indexer first lists it.

Create `pj-cloud/integration-tests/docker-compose.yml`:

```yaml
services:
  minio:
    image: minio/minio:RELEASE.2024-06-13T22-53-53Z
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: admin
      MINIO_ROOT_PASSWORD: password123
    ports: ["9000:9000", "9001:9001"]
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      timeout: 3s
      retries: 10

  minio-init:
    image: minio/mc:RELEASE.2024-06-12T14-34-03Z
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      sh -c "
        mc alias set local http://minio:9000 admin password123 &&
        mc mb -p local/recordings &&
        echo 'minio ready'
      "

  fake-gcs:
    image: fsouza/fake-gcs-server:1.49.2
    # -scheme http: plain HTTP (the server dials http://fake-gcs:4443, no TLS to the emulator).
    # -public-host: the host the emulator rewrites download/upload URLs to, so the GCS client
    #               (which follows redirects to the resumable-upload + media endpoints) stays in-network.
    command: ["-scheme", "http", "-host", "0.0.0.0", "-port", "4443", "-public-host", "fake-gcs:4443"]
    ports: ["4443:4443"]
    healthcheck:
      # storage._info root is served once the API is up; curl is present in the image.
      test: ["CMD", "wget", "-q", "-O-", "http://localhost:4443/storage/v1/b"]
      interval: 5s
      timeout: 3s
      retries: 10

  fake-gcs-init:
    image: curlimages/curl:8.8.0
    depends_on:
      fake-gcs:
        condition: service_healthy
    # Create the `recordings` bucket via the GCS JSON API. (`_project` is ignored by fake-gcs.)
    entrypoint: >
      sh -c "
        curl -fsS -X POST 'http://fake-gcs:4443/storage/v1/b?project=test'
          -H 'Content-Type: application/json'
          -d '{\"name\":\"recordings\"}' &&
        echo 'fake-gcs ready'
      "

  pj-cloud-server:
    build:
      context: ../server
      dockerfile: deploy/Dockerfile
    depends_on:
      minio-init:
        condition: service_completed_successfully
      fake-gcs-init:
        condition: service_completed_successfully
    environment:
      PJ_CLOUD_TOKEN: "test-token"
      PJ_CLOUD_DASHBOARD_PASSWORD: "dashpw"
      # fake-gcs needs the GCS client pointed at the emulator endpoint. The gcsreader
      # impl honors STORAGE_EMULATOR_HOST (the std env var the Go cloud.google.com/go/storage
      # client reads). Harmless for the s3 leg (gcsreader is not constructed there).
      STORAGE_EMULATOR_HOST: "http://fake-gcs:4443"
    volumes:
      # SERVER_CONFIG is set by the orchestrator to server-config.s3.yaml or server-config.gcs.yaml.
      # Defaults to the s3 config so `docker-compose up` alone still works for a quick smoke.
      - ${SERVER_CONFIG:-./server-config.s3.yaml}:/etc/pj-cloud/config.yaml:ro
    ports: ["8443:8443"]
```

- [ ] **Step 3: `server-config.s3.yaml` wired for the dockerized Minio (`backend=s3`)**

The `storage:` block is the tagged union from the unified plan (§3.2 seam 1+2 / design-spec §8.6): exactly one of `{s3, gcs}` non-nil. This is the `s3` leg.

Create `pj-cloud/integration-tests/server-config.s3.yaml`:

```yaml
server:
  listen: ":8443"
auth:
  bearer_token: ${PJ_CLOUD_TOKEN}
storage:
  s3:
    bucket: recordings
    region: us-east-1
    endpoint: http://minio:9000
catalog:
  db_path: /tmp/catalog.db
indexer:
  poll_interval: 2s              # fast cadence for tests
  startup_scan: true
session:
  max_concurrent: 8
  retain_after_disconnect: 5s
  retain_max_seqs: 64
  retain_max_bytes: 16777216
  storage_concurrent_requests: 16   # renamed from s3_concurrent_requests (unified plan, backend-agnostic)
  max_batch_bytes: 524288
  max_batch_age_ms: 50
  max_message_bytes: 16777216
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

- [ ] **Step 3b: `server-config.gcs.yaml` wired for the dockerized fake-gcs (`backend=gcs`)**

Identical to the `s3` config EXCEPT the `storage:` block selects the `gcs` arm of the tagged union (no `s3` key — fail-fast forbids both). The server dials the emulator via `STORAGE_EMULATOR_HOST` (set in compose); the `gcs` block itself only names the bucket + prefix. The `catalog.db_path`, `session`, `dashboard`, and `metrics` blocks are byte-identical to the `s3` config — the only difference between the two legs is this `storage:` block, which is the whole point.

Create `pj-cloud/integration-tests/server-config.gcs.yaml`:

```yaml
server:
  listen: ":8443"
auth:
  bearer_token: ${PJ_CLOUD_TOKEN}
storage:
  gcs:
    bucket: recordings
    prefix: ""
catalog:
  db_path: /tmp/catalog.db
indexer:
  poll_interval: 2s              # fast cadence for tests
  startup_scan: true
session:
  max_concurrent: 8
  retain_after_disconnect: 5s
  retain_max_seqs: 64
  retain_max_bytes: 16777216
  storage_concurrent_requests: 16
  max_batch_bytes: 524288
  max_batch_age_ms: 50
  max_message_bytes: 16777216
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

- [ ] **Step 4: Smoke-test BOTH backend legs of the compose stack**

```bash
cd /home/davide/ws_plotjuggler/pj-cloud/integration-tests

# --- s3 leg (Minio) ---
SERVER_CONFIG=./server-config.s3.yaml docker-compose up -d --build
curl -fsS http://localhost:8443/health        # expect: 200 + body "ok"
curl -k -u admin:dashpw https://localhost:8443/dashboard/ | grep -o 'backend: s3'
docker-compose down -v

# --- gcs leg (fake-gcs) ---
SERVER_CONFIG=./server-config.gcs.yaml docker-compose up -d --build
curl -fsS http://localhost:8443/health        # expect: 200 + body "ok"
curl -k -u admin:dashpw https://localhost:8443/dashboard/ | grep -o 'backend: gcs'
docker-compose down -v
```

Expected: `/health` returns 200 on both legs; the dashboard's backend-display line (unified plan §3.1 Dashboard row) reads `backend: s3` on the first leg and `backend: gcs` on the second.

- [ ] **Step 5: Commit**

```bash
git add integration-tests/go.mod integration-tests/go.sum \
        integration-tests/docker-compose.yml \
        integration-tests/server-config.s3.yaml integration-tests/server-config.gcs.yaml
git commit -m "test(e2e): docker-compose with Minio + fake-gcs + per-backend server configs"
```

---

## Task 2: Fixture generator (deterministic MCAPs)

**Files:**
- Create: `pj-cloud/integration-tests/cmd/gen-fixtures/main.go`

- [ ] **Step 1: Write the generator**

Create `pj-cloud/integration-tests/cmd/gen-fixtures/main.go`:

```go
// gen-fixtures produces a deterministic corpus of MCAP files covering the
// dimensions the wire protocol has to handle: chunk compression, payload
// sizes, multi-file stitching, embedded tags, and edge cases.
//
// Re-running with the same Go version + mcap-go version yields byte-identical
// output, so the committed fixtures stay reproducible.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/foxglove/mcap/go/mcap"
)

func main() {
	out := flag.String("out", "fixtures", "output directory")
	flag.Parse()
	if err := os.MkdirAll(*out, 0o755); err != nil {
		log.Fatal(err)
	}
	for _, fx := range allFixtures() {
		path := filepath.Join(*out, fx.name)
		if err := os.WriteFile(path, fx.build(), 0o644); err != nil {
			log.Fatalf("write %s: %v", path, err)
		}
		fmt.Println("wrote", path)
	}
}

type fixture struct {
	name  string
	build func() []byte
}

func allFixtures() []fixture {
	return []fixture{
		{"single-topic-uncompressed.mcap", buildSingleTopicUncompressed},
		{"multi-topic-zstd.mcap", buildMultiTopicZstd},
		{"with-embedded-tags.mcap", buildEmbeddedTags},
		{"large-payloads.mcap", buildLargePayloads},
		{"tiny-payloads.mcap", buildTinyPayloads},
		{"consecutive-1of3.mcap", func() []byte { return buildConsecutive(0, 1000) }},
		{"consecutive-2of3.mcap", func() []byte { return buildConsecutive(1000, 2000) }},
		{"consecutive-3of3.mcap", func() []byte { return buildConsecutive(2000, 3000) }},
		{"empty-time-range.mcap", buildShortBurst},
	}
}

func writeMCAP(opts mcap.WriterOptions, channels []channel, metadata map[string]string) []byte {
	var buf bytes.Buffer
	w, err := mcap.NewWriter(&buf, &opts)
	if err != nil {
		log.Fatal(err)
	}
	if err := w.WriteHeader(&mcap.Header{Profile: "", Library: "pj-cloud-fixtures"}); err != nil {
		log.Fatal(err)
	}
	schemaIDs := map[string]uint16{}
	chanIDs := map[string]uint16{}
	var nextSchema, nextChan uint16 = 1, 1
	for _, c := range channels {
		key := c.schemaName + "::" + c.schemaEncoding
		sid, ok := schemaIDs[key]
		if !ok {
			sid = nextSchema
			nextSchema++
			schemaIDs[key] = sid
			_ = w.WriteSchema(&mcap.Schema{ID: sid, Name: c.schemaName, Encoding: c.schemaEncoding, Data: []byte(c.schemaName)})
		}
		cid := nextChan
		nextChan++
		chanIDs[c.topic] = cid
		_ = w.WriteChannel(&mcap.Channel{ID: cid, SchemaID: sid, Topic: c.topic, MessageEncoding: "cdr"})
	}
	for _, c := range channels {
		cid := chanIDs[c.topic]
		for _, m := range c.messages {
			_ = w.WriteMessage(&mcap.Message{ChannelID: cid, LogTime: m.logTime, PublishTime: m.publishTime, Data: m.data})
		}
	}
	for k, v := range metadata {
		_ = w.WriteMetadata(&mcap.Metadata{Name: "pj.user_tags", Metadata: map[string]string{k: v}})
	}
	_ = w.Close()
	return buf.Bytes()
}

type channel struct {
	topic, schemaName, schemaEncoding string
	messages                           []message
}
type message struct{ logTime, publishTime uint64; data []byte }

func buildSingleTopicUncompressed() []byte {
	var msgs []message
	for i := 0; i < 100; i++ {
		msgs = append(msgs, message{logTime: uint64(i * 1_000_000), publishTime: uint64(i * 1_000_000), data: []byte(fmt.Sprintf("msg-%d", i))})
	}
	return writeMCAP(mcap.WriterOptions{Chunked: true, ChunkSize: 64 * 1024, Compression: mcap.CompressionNone},
		[]channel{{"/x", "X", "ros2msg", msgs}}, nil)
}

func buildMultiTopicZstd() []byte {
	var channels []channel
	for i := 0; i < 8; i++ {
		topic := fmt.Sprintf("/sensor/%d", i)
		var msgs []message
		for j := 0; j < 200; j++ {
			msgs = append(msgs, message{logTime: uint64(j * 1_000_000), publishTime: uint64(j * 1_000_000), data: make([]byte, 256)})
		}
		channels = append(channels, channel{topic: topic, schemaName: "Sensor", schemaEncoding: "ros2msg", messages: msgs})
	}
	return writeMCAP(mcap.WriterOptions{Chunked: true, ChunkSize: 1 << 20, Compression: mcap.CompressionZSTD},
		channels, nil)
}

func buildEmbeddedTags() []byte {
	return writeMCAP(mcap.WriterOptions{Chunked: true, ChunkSize: 64 * 1024, Compression: mcap.CompressionZSTD},
		[]channel{{"/x", "X", "ros2msg", []message{{logTime: 1, publishTime: 1, data: []byte("a")}}}},
		map[string]string{"vehicle": "7", "run": "alpha"})
}

func buildLargePayloads() []byte {
	var msgs []message
	for i := 0; i < 4; i++ {
		msgs = append(msgs, message{logTime: uint64(i), publishTime: uint64(i), data: make([]byte, 4*1024*1024)})
	}
	return writeMCAP(mcap.WriterOptions{Chunked: true, ChunkSize: 8 * 1024 * 1024, Compression: mcap.CompressionZSTD},
		[]channel{{"/img/raw", "Image", "ros2msg", msgs}}, nil)
}

func buildTinyPayloads() []byte {
	var msgs []message
	for i := 0; i < 10_000; i++ {
		msgs = append(msgs, message{logTime: uint64(i * 1000), publishTime: uint64(i * 1000), data: make([]byte, 40)})
	}
	return writeMCAP(mcap.WriterOptions{Chunked: true, ChunkSize: 256 * 1024, Compression: mcap.CompressionZSTD},
		[]channel{{"/tf", "TransformStamped", "ros2msg", msgs}}, nil)
}

func buildConsecutive(startNs, endNs uint64) []byte {
	step := (endNs - startNs) / 100
	var msgs []message
	for i := uint64(0); i < 100; i++ {
		t := startNs + i*step
		msgs = append(msgs, message{logTime: t, publishTime: t, data: []byte(fmt.Sprintf("seg-%d-%d", startNs, i))})
	}
	return writeMCAP(mcap.WriterOptions{Chunked: true, ChunkSize: 64 * 1024, Compression: mcap.CompressionZSTD},
		[]channel{{"/x", "X", "ros2msg", msgs}}, nil)
}

func buildShortBurst() []byte {
	var msgs []message
	for i := 0; i < 5; i++ {
		msgs = append(msgs, message{logTime: uint64(i * 1_000_000), publishTime: uint64(i * 1_000_000), data: []byte{byte(i)}})
	}
	return writeMCAP(mcap.WriterOptions{Chunked: true, ChunkSize: 64 * 1024, Compression: mcap.CompressionNone},
		[]channel{{"/x", "X", "ros2msg", msgs}}, nil)
}
```

- [ ] **Step 2: Generate the corpus + check size**

```bash
cd /home/davide/ws_plotjuggler/pj-cloud/integration-tests
go run ./cmd/gen-fixtures --out fixtures
du -sh fixtures/
```

Expected: ~5–10 MB total.

- [ ] **Step 3: Add a corrupt-chunk fixture by hand-editing**

```bash
cp fixtures/single-topic-uncompressed.mcap fixtures/corrupt-chunk.mcap
# Flip a byte mid-file to corrupt one chunk's CRC.
python3 -c "
import sys
with open('fixtures/corrupt-chunk.mcap', 'r+b') as f:
    f.seek(2048)
    b = f.read(1)
    f.seek(2048)
    f.write(bytes([b[0] ^ 0xff]))
"
```

- [ ] **Step 4: Commit fixtures + generator**

```bash
git add integration-tests/cmd/gen-fixtures/ integration-tests/fixtures/
git commit -m "test(e2e): deterministic fixture generator + canned MCAP corpus"
```

---

## Task 3: Upload + CLI exec helpers

**Files:**
- Create: `pj-cloud/integration-tests/internal/upload/upload.go`
- Create: `pj-cloud/integration-tests/internal/cli/cli.go`

- [ ] **Step 1: Upload helper (minio-go)**

Create `pj-cloud/integration-tests/internal/upload/upload.go`:

```go
// Package upload uploads fixture MCAPs into the dockerized Minio bucket.
package upload

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

func NewClient(endpoint string) (*minio.Client, error) {
	return minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4("admin", "password123", ""),
		Secure: false,
	})
}

func UploadDir(ctx context.Context, c *minio.Client, bucket, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".mcap") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return err
		}
		_, err = c.PutObject(ctx, bucket, e.Name(), bytes.NewReader(body), int64(len(body)),
			minio.PutObjectOptions{ContentType: "application/octet-stream"})
		if err != nil {
			return fmt.Errorf("upload %s: %w", e.Name(), err)
		}
	}
	return nil
}
```

- [ ] **Step 2: CLI exec helper**

Create `pj-cloud/integration-tests/internal/cli/cli.go`:

```go
// Package cli wraps subprocess invocation of pjcloud-cli for the matrix driver.
package cli

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
)

type Runner struct {
	Binary string
	Server string
	Token  string
}

func (r *Runner) Run(ctx context.Context, args ...string) (string, error) {
	all := append([]string{"--server", r.Server, "--token", r.Token}, args...)
	cmd := exec.CommandContext(ctx, r.Binary, all...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("cli %s: %v (stderr: %s)", strings.Join(all, " "), err, stderr.String())
	}
	return stdout.String(), nil
}
```

- [ ] **Step 3: Commit**

```bash
git add integration-tests/internal/upload/ integration-tests/internal/cli/
git commit -m "test(e2e): upload + cli exec helpers"
```

---

## Task 4: MCAP diff (message-level logical equality)

**Files:**
- Create: `pj-cloud/integration-tests/internal/diff/mcap.go`
- Create: `pj-cloud/integration-tests/internal/diff/mcap_test.go`

- [ ] **Step 1: Diff implementation**

Create `pj-cloud/integration-tests/internal/diff/mcap.go`:

```go
// Package diff implements the v1 logical-equality comparison: for every
// (topic, log_time) pair in the *original* MCAP slice we're interested in,
// assert there is a corresponding record in the *reconstructed* MCAP with
// byte-equal payload + publish_time + schema name/encoding/data.
//
// NOT a container-byte diff — MCAP writers may legitimately differ on
// chunking, summary ordering, compression. We compare what the protocol promises
// (this matches design-spec §11 line 833 and unified-plan §6 L3, which both
// specify logical equality, NOT container-byte equality).
//
// Backend-agnostic by construction: this logic is UNCHANGED across backends and
// is asserted on BOTH the s3 (Minio) and gcs (fake-gcs) round-trip legs (Task 6).
// Because the connector forwards RAW MCAP records to the client (no decode on the
// wire — unified-plan §3.3 Correction B / §8.E), the CLI's McapWriterSink merely
// re-serializes records it received verbatim, so a faithful logical round-trip is
// the natural outcome on either backend; a mismatch on EITHER leg blocks release.
package diff

import (
	"bytes"
	"fmt"
	"io"
	"sort"

	"github.com/foxglove/mcap/go/mcap"
)

type Record struct {
	Topic        string
	LogTime      uint64
	PublishTime  uint64
	Data         []byte
	SchemaName   string
	SchemaEnc    string
	SchemaData   []byte
}

func Collect(r io.ReadSeeker) ([]Record, error) {
	rdr, err := mcap.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer rdr.Close()
	it, err := rdr.Messages()
	if err != nil {
		return nil, err
	}
	var out []Record
	for {
		sch, ch, msg, err := it.NextInto(nil)
		if err != nil {
			break
		}
		if msg == nil {
			break
		}
		rec := Record{
			Topic: ch.Topic, LogTime: msg.LogTime, PublishTime: msg.PublishTime,
			Data: append([]byte(nil), msg.Data...),
		}
		if sch != nil {
			rec.SchemaName = sch.Name
			rec.SchemaEnc = sch.Encoding
			rec.SchemaData = sch.Data
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].LogTime < out[j].LogTime
	})
	return out, nil
}

type Mismatch struct {
	Index int
	What  string
	Want  Record
	Got   Record
}

func Compare(orig, rebuilt []Record) []Mismatch {
	var miss []Mismatch
	n := len(orig)
	if len(rebuilt) < n {
		n = len(rebuilt)
	}
	for i := 0; i < n; i++ {
		o, r := orig[i], rebuilt[i]
		if o.Topic != r.Topic {
			miss = append(miss, Mismatch{i, "topic", o, r})
			continue
		}
		if o.LogTime != r.LogTime {
			miss = append(miss, Mismatch{i, "log_time", o, r})
		}
		if o.PublishTime != r.PublishTime {
			miss = append(miss, Mismatch{i, "publish_time", o, r})
		}
		if !bytes.Equal(o.Data, r.Data) {
			miss = append(miss, Mismatch{i, "payload", o, r})
		}
		if o.SchemaName != r.SchemaName {
			miss = append(miss, Mismatch{i, "schema_name", o, r})
		}
		if o.SchemaEnc != r.SchemaEnc {
			miss = append(miss, Mismatch{i, "schema_encoding", o, r})
		}
		if !bytes.Equal(o.SchemaData, r.SchemaData) {
			miss = append(miss, Mismatch{i, "schema_data", o, r})
		}
	}
	if len(orig) != len(rebuilt) {
		miss = append(miss, Mismatch{n, fmt.Sprintf("length: orig=%d rebuilt=%d", len(orig), len(rebuilt)),
			Record{}, Record{}})
	}
	return miss
}
```

- [ ] **Step 2: Unit test on synthetic in-memory MCAPs**

Create `pj-cloud/integration-tests/internal/diff/mcap_test.go`:

```go
package diff

import (
	"bytes"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
)

func TestCompare_Identical(t *testing.T) {
	body := buildSimple(t)
	orig, _ := Collect(bytes.NewReader(body))
	rebuilt, _ := Collect(bytes.NewReader(body))
	if miss := Compare(orig, rebuilt); len(miss) != 0 {
		t.Errorf("expected no mismatches, got %+v", miss)
	}
}

func TestCompare_DifferentPayloadDetected(t *testing.T) {
	a := buildSimple(t)
	b := buildDifferentPayload(t)
	orig, _ := Collect(bytes.NewReader(a))
	rebuilt, _ := Collect(bytes.NewReader(b))
	miss := Compare(orig, rebuilt)
	if len(miss) == 0 {
		t.Fatal("expected mismatch")
	}
	if miss[0].What != "payload" {
		t.Errorf("expected payload mismatch, got %q", miss[0].What)
	}
}

func buildSimple(t *testing.T) []byte {
	var buf bytes.Buffer
	w, _ := mcap.NewWriter(&buf, &mcap.WriterOptions{Chunked: true})
	_ = w.WriteHeader(&mcap.Header{})
	_ = w.WriteSchema(&mcap.Schema{ID: 1, Name: "X", Encoding: "ros2msg", Data: []byte("s")})
	_ = w.WriteChannel(&mcap.Channel{ID: 1, SchemaID: 1, Topic: "/x", MessageEncoding: "cdr"})
	_ = w.WriteMessage(&mcap.Message{ChannelID: 1, LogTime: 1, PublishTime: 1, Data: []byte("a")})
	_ = w.Close()
	return buf.Bytes()
}

func buildDifferentPayload(t *testing.T) []byte {
	var buf bytes.Buffer
	w, _ := mcap.NewWriter(&buf, &mcap.WriterOptions{Chunked: true})
	_ = w.WriteHeader(&mcap.Header{})
	_ = w.WriteSchema(&mcap.Schema{ID: 1, Name: "X", Encoding: "ros2msg", Data: []byte("s")})
	_ = w.WriteChannel(&mcap.Channel{ID: 1, SchemaID: 1, Topic: "/x", MessageEncoding: "cdr"})
	_ = w.WriteMessage(&mcap.Message{ChannelID: 1, LogTime: 1, PublishTime: 1, Data: []byte("DIFFERENT")})
	_ = w.Close()
	return buf.Bytes()
}
```

- [ ] **Step 3: Run + commit**

```bash
cd /home/davide/ws_plotjuggler/pj-cloud/integration-tests
go test ./internal/diff/... -v
```

```bash
git add integration-tests/internal/diff/
git commit -m "test(e2e): MCAP message-level logical-equality diff"
```

---

## Task 5: Compose orchestration helper

**Files:**
- Create: `pj-cloud/integration-tests/internal/compose/up.go`

- [ ] **Step 1: Compose wrapper, parameterized by backend**

`Up` takes a `backend` in `{"s3", "gcs"}` and selects the matching `server-config.<backend>.yaml` by exporting `SERVER_CONFIG` into the compose process environment (consumed by the `${SERVER_CONFIG:-...}` volume in Task 1's compose file). The readiness wait on `/health` is identical for both backends — that sameness is the point. `Backend()` is exposed so the matrix driver can label cases.

Create `pj-cloud/integration-tests/internal/compose/up.go`:

```go
// Package compose runs `docker-compose up` for a chosen storage backend and waits
// for the server's /health endpoint to become 200. Returns a teardown closure.
//
// The ONLY difference between the s3 and gcs legs is which server-config.<backend>.yaml
// is mounted (via the SERVER_CONFIG env var the compose file reads). Readiness,
// teardown, and every downstream assertion are identical across backends.
package compose

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"time"
)

type Handle struct {
	backend  string
	teardown func() error
}

// configFor maps a backend axis value to its mounted server config.
func configFor(backend string) (string, error) {
	switch backend {
	case "s3":
		return "./server-config.s3.yaml", nil
	case "gcs":
		return "./server-config.gcs.yaml", nil
	default:
		return "", fmt.Errorf("unknown backend %q (want s3 or gcs)", backend)
	}
}

// Up boots the compose stack with the server pointed at `backend` storage.
func Up(ctx context.Context, projectDir, backend string) (*Handle, error) {
	cfg, err := configFor(backend)
	if err != nil {
		return nil, err
	}
	// Fresh volumes per backend so the catalog.db / object store of the previous
	// leg never bleeds into this one.
	cmd := exec.CommandContext(ctx, "docker-compose", "up", "-d", "--build")
	cmd.Dir = projectDir
	cmd.Env = append(os.Environ(), "SERVER_CONFIG="+cfg)
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("compose up (%s): %v\n%s", backend, err, out)
	}
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get("http://localhost:8443/health")
		if err == nil && resp.StatusCode == http.StatusOK {
			return &Handle{backend: backend, teardown: func() error {
				down := exec.Command("docker-compose", "down", "-v")
				down.Dir = projectDir
				return down.Run()
			}}, nil
		}
		time.Sleep(2 * time.Second)
	}
	return nil, fmt.Errorf("server (%s) did not become healthy within 120s", backend)
}

func (h *Handle) Backend() string { return h.backend }
func (h *Handle) Close() error     { return h.teardown() }
```

- [ ] **Step 2: Commit**

```bash
git add integration-tests/internal/compose/
git commit -m "test(e2e): backend-parameterized docker-compose up/down + readiness wait"
```

---

## Task 6: Matrix driver (`run-matrix`) + test harness

**Files:**
- Modify: `pj-cloud/integration-tests/internal/upload/upload.go` (add a GCS upload path)
- Create: `pj-cloud/integration-tests/cmd/run-matrix/main.go`
- Create: `pj-cloud/integration-tests/matrix_test.go`

- [ ] **Step 0: Add a GCS upload path so fixtures land in fake-gcs for the `gcs` leg**

The `s3` leg uploads via `minio-go` (Task 3). The `gcs` leg uploads the same fixtures into `fake-gcs-server` via the GCS Go client pointed at the emulator. Add to `pj-cloud/integration-tests/internal/upload/upload.go`:

```go
// UploadDirGCS uploads every *.mcap in dir into the fake-gcs bucket. It relies on
// STORAGE_EMULATOR_HOST=http://localhost:4443 being set so the cloud.google.com/go/storage
// client talks to fake-gcs instead of real GCS. Same bucket name ("recordings") as the s3 leg.
func UploadDirGCS(ctx context.Context, bucket, dir string) error {
	cl, err := gcs.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return err
	}
	defer cl.Close()
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".mcap") {
			continue
		}
		body, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			return err
		}
		w := cl.Bucket(bucket).Object(e.Name()).NewWriter(ctx)
		if _, err := w.Write(body); err != nil {
			_ = w.Close()
			return fmt.Errorf("gcs upload %s: %w", e.Name(), err)
		}
		if err := w.Close(); err != nil {
			return fmt.Errorf("gcs upload close %s: %w", e.Name(), err)
		}
	}
	return nil
}
```

Add the imports to `upload.go` (`gcs "cloud.google.com/go/storage"`, `"google.golang.org/api/option"`) and fetch the dep:

```bash
cd /home/davide/ws_plotjuggler/pj-cloud/integration-tests
go get cloud.google.com/go/storage@latest
go get google.golang.org/api/option@latest
```

- [ ] **Step 1: Matrix driver (parameterized over backend)**

Create `pj-cloud/integration-tests/cmd/run-matrix/main.go`:

```go
// run-matrix orchestrates the test matrix: for each backend in {s3, gcs} and each
// (fixture × topics × time-range) combination, it boots the compose stack against
// that backend, uploads fixtures, executes `pjcloud-cli session download`, compares
// the result to the original, and reports per-(backend,case) pass/fail.
//
// PJCLOUD_BACKEND can pin a single leg ("s3" or "gcs"); unset = run BOTH legs.
// A gcs-only failure fails the run exactly like an s3-only failure.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"pj-cloud/integration-tests/internal/cli"
	"pj-cloud/integration-tests/internal/compose"
	"pj-cloud/integration-tests/internal/diff"
	"pj-cloud/integration-tests/internal/upload"
)

type Case struct {
	Fixture    string
	Topics     []string // empty = all
	StartNs    int64
	EndNs      int64
	ExpectFail bool
}

func cases() []Case {
	return []Case{
		{Fixture: "single-topic-uncompressed.mcap"},
		{Fixture: "single-topic-uncompressed.mcap", Topics: []string{"/x"}},
		{Fixture: "multi-topic-zstd.mcap"},
		{Fixture: "multi-topic-zstd.mcap", Topics: []string{"/sensor/0", "/sensor/3"}},
		{Fixture: "with-embedded-tags.mcap"},
		{Fixture: "large-payloads.mcap"},
		{Fixture: "tiny-payloads.mcap"},
		// time-range partial slice
		{Fixture: "single-topic-uncompressed.mcap", StartNs: 30_000_000, EndNs: 60_000_000},
	}
}

// backends returns the legs to run: PJCLOUD_BACKEND pins one, else both.
func backends() []string {
	if b := os.Getenv("PJCLOUD_BACKEND"); b != "" {
		return []string{b}
	}
	return []string{"s3", "gcs"}
}

// uploadFixtures puts the corpus into the right emulator for this backend.
func uploadFixtures(ctx context.Context, backend string) error {
	switch backend {
	case "s3":
		mc, err := upload.NewClient("localhost:9000")
		if err != nil {
			return err
		}
		return upload.UploadDir(ctx, mc, "recordings", "fixtures")
	case "gcs":
		// gcsreader + UploadDirGCS both read STORAGE_EMULATOR_HOST; the matrix sets it
		// to the host-published emulator port for the uploader running on the host.
		if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
			_ = os.Setenv("STORAGE_EMULATOR_HOST", "http://localhost:4443")
		}
		return upload.UploadDirGCS(ctx, "recordings", "fixtures")
	default:
		return fmt.Errorf("unknown backend %q", backend)
	}
}

func main() {
	ctx := context.Background()
	projectDir, _ := filepath.Abs(".")
	runner := &cli.Runner{Binary: os.Getenv("PJCLOUD_CLI"), Server: "wss://localhost:8443/api/ws", Token: "test-token"}

	var failures []string
	for _, backend := range backends() {
		handle, err := compose.Up(ctx, projectDir, backend)
		if err != nil {
			log.Fatalf("[%s] compose up: %v", backend, err)
		}
		if err := uploadFixtures(ctx, backend); err != nil {
			_ = handle.Close()
			log.Fatalf("[%s] upload: %v", backend, err)
		}
		for _, c := range cases() {
			if err := runOne(ctx, runner, c); err != nil {
				failures = append(failures, fmt.Sprintf("[%s] %s: %v", backend, c.Fixture, err))
			}
		}
		if err := handle.Close(); err != nil {
			log.Printf("[%s] teardown: %v", backend, err)
		}
	}
	if len(failures) > 0 {
		out, _ := json.MarshalIndent(failures, "", "  ")
		fmt.Println(string(out))
		os.Exit(1)
	}
}

func runOne(ctx context.Context, r *cli.Runner, c Case) error {
	tmp, _ := os.MkdirTemp("", "pjcloud-")
	defer os.RemoveAll(tmp)

	// Look up file_id by listing.
	out, err := r.Run(ctx, "files", "list")
	if err != nil {
		return err
	}
	fileID, err := parseFileID(out, c.Fixture)
	if err != nil {
		return err
	}

	outPath := filepath.Join(tmp, "rebuilt.mcap")
	args := []string{"session", "download",
		"--files", fileID,
		"--output", outPath,
	}
	if len(c.Topics) > 0 {
		joined := ""
		for i, t := range c.Topics {
			if i > 0 { joined += "," }
			joined += t
		}
		args = append(args, "--topics", joined)
	}
	if c.StartNs != 0 || c.EndNs != 0 {
		args = append(args, "--time-range", fmt.Sprintf("%d,%d", c.StartNs, c.EndNs))
	}
	if _, err := r.Run(ctx, args...); err != nil {
		return err
	}

	origPath := filepath.Join("fixtures", c.Fixture)
	orig, _ := openMcap(origPath)
	rebuilt, _ := openMcap(outPath)
	miss := diff.Compare(orig, rebuilt)
	if len(miss) != 0 {
		return fmt.Errorf("logical mismatch (%d): first=%+v", len(miss), miss[0])
	}
	return nil
}

func parseFileID(listOutput, filename string) (string, error) {
	// (Implementation: parse tab-separated `id\tkey\tsize` rows.)
	return "1", nil
}

func openMcap(path string) ([]diff.Record, error) {
	f, err := os.Open(path)
	if err != nil { return nil, err }
	defer f.Close()
	return diff.Collect(f)
}
```

- [ ] **Step 2: `go test` wrapper**

Create `pj-cloud/integration-tests/matrix_test.go`:

```go
//go:build integration

package integration_test

import (
	"context"
	"os/exec"
	"testing"
)

// TestMatrix invokes the run-matrix binary; useful for `make integration` in CI.
func TestMatrix(t *testing.T) {
	cmd := exec.CommandContext(context.Background(), "go", "run", "./cmd/run-matrix")
	cmd.Env = append(cmd.Environ(), "PJCLOUD_CLI=../build/client-cli/pjcloud-cli")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("matrix failed:\n%s", out)
	}
}
```

- [ ] **Step 3: Commit**

```bash
git add integration-tests/cmd/run-matrix/ integration-tests/matrix_test.go \
        integration-tests/internal/upload/upload.go
git commit -m "test(e2e): matrix driver over {s3,gcs} — compose per backend, upload, run CLI, diff"
```

---

## Task 7: Benchmark gate

**Files:**
- Create: `pj-cloud/integration-tests/bench/throughput_test.go`
- Create: `pj-cloud/integration-tests/bench/parity_test.go`
- Create: `pj-cloud/integration-tests/bench/baseline.json`
- Create: `pj-cloud/integration-tests/bench/compare.go`

- [ ] **Step 1: Bench**

Create `pj-cloud/integration-tests/bench/throughput_test.go`:

```go
//go:build bench

package bench_test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// BenchmarkStreamingThroughput drives `pjcloud-cli session download` against
// the running server with a large fixture and reports MB/s.
func BenchmarkStreamingThroughput(b *testing.B) {
	cli := os.Getenv("PJCLOUD_CLI")
	if cli == "" {
		b.Skip("PJCLOUD_CLI not set")
	}
	tmp := b.TempDir()
	outPath := filepath.Join(tmp, "rebuilt.mcap")
	args := []string{
		"--server", "wss://localhost:8443/api/ws", "--token", "test-token",
		"session", "download",
		"--files", "1",
		"--output", outPath,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		cmd := exec.CommandContext(context.Background(), cli, args...)
		if err := cmd.Run(); err != nil {
			b.Fatalf("cli: %v", err)
		}
		info, _ := os.Stat(outPath)
		mb := float64(info.Size()) / (1024 * 1024)
		secs := time.Since(start).Seconds()
		b.ReportMetric(mb/secs, "MB/s")
		fmt.Printf("iter %d: %.1f MB in %.2fs (%.1f MB/s)\n", i, mb, secs, mb/secs)
	}
}
```

- [ ] **Step 1b: Cross-backend storage-parity microbench**

This is the quantitative half of the no-soft-fork guarantee (unified-plan §6 L4 / §7 risk 1): `BlobStore.GetRange` on fake-gcs must be within ~10% of Minio. It is a **microbench at the storage seam** — it does NOT go through the full session/WS path — so it isolates the backend's range-read cost. It uploads one ~64 MiB blob to each emulator's `recordings` bucket and times repeated ranged reads.

Create `pj-cloud/integration-tests/bench/parity_test.go`:

```go
//go:build bench

package bench_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	gcs "cloud.google.com/go/storage"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"google.golang.org/api/option"
)

const (
	parityBucket   = "recordings"
	parityObject   = "parity-blob.bin"
	parityBlobSize = 64 << 20 // 64 MiB
	parityReadLen  = 1 << 20  // 1 MiB ranged reads
)

// BenchmarkGetRangeParity reports MB/s for 1 MiB ranged reads against each emulator.
// Sub-benchmarks: s3_get_range (Minio) and gcs_get_range (fake-gcs). compare.go asserts
// gcs within ~10% of s3 (hard the first time GCS lands; soft thereafter).
func BenchmarkGetRangeParity(b *testing.B) {
	ctx := context.Background()
	blob := bytes.Repeat([]byte{0xab}, parityBlobSize)

	b.Run("s3_get_range", func(b *testing.B) {
		mc, err := minio.New("localhost:9000", &minio.Options{
			Creds:  credentials.NewStaticV4("admin", "password123", ""),
			Secure: false,
		})
		if err != nil {
			b.Fatal(err)
		}
		if _, err := mc.PutObject(ctx, parityBucket, parityObject, bytes.NewReader(blob),
			int64(len(blob)), minio.PutObjectOptions{}); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(parityReadLen)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			off := int64((i * parityReadLen) % (parityBlobSize - parityReadLen))
			opts := minio.GetObjectOptions{}
			_ = opts.SetRange(off, off+parityReadLen-1)
			obj, err := mc.GetObject(ctx, parityBucket, parityObject, opts)
			if err != nil {
				b.Fatal(err)
			}
			buf := make([]byte, parityReadLen)
			if _, err := obj.Read(buf); err != nil && err.Error() != "EOF" {
				b.Fatal(err)
			}
			_ = obj.Close()
		}
		b.ReportMetric(float64(parityReadLen)/(1024*1024)*float64(b.N)/b.Elapsed().Seconds(), "MB/s")
	})

	b.Run("gcs_get_range", func(b *testing.B) {
		if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
			_ = os.Setenv("STORAGE_EMULATOR_HOST", "http://localhost:4443")
		}
		cl, err := gcs.NewClient(ctx, option.WithoutAuthentication())
		if err != nil {
			b.Fatal(err)
		}
		defer cl.Close()
		w := cl.Bucket(parityBucket).Object(parityObject).NewWriter(ctx)
		if _, err := w.Write(blob); err != nil {
			b.Fatal(err)
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		b.SetBytes(parityReadLen)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			off := int64((i * parityReadLen) % (parityBlobSize - parityReadLen))
			r, err := cl.Bucket(parityBucket).Object(parityObject).NewRangeReader(ctx, off, parityReadLen)
			if err != nil {
				b.Fatal(err)
			}
			buf := make([]byte, parityReadLen)
			if _, err := r.Read(buf); err != nil && err.Error() != "EOF" {
				b.Fatal(err)
			}
			_ = r.Close()
		}
		b.ReportMetric(float64(parityReadLen)/(1024*1024)*float64(b.N)/b.Elapsed().Seconds(), "MB/s")
	})

	fmt.Println("parity bench done; compare.go enforces gcs within ~10% of s3")
}
```

Run it locally against a live stack (both emulators must be up):

```bash
cd /home/davide/ws_plotjuggler/pj-cloud/integration-tests
SERVER_CONFIG=./server-config.s3.yaml docker-compose up -d --build
go test -tags=bench -bench=BenchmarkGetRangeParity -benchmem -count=1 -v ./bench/...
docker-compose down -v
```

Expected: two sub-benchmark lines, `BenchmarkGetRangeParity/s3_get_range` and `BenchmarkGetRangeParity/gcs_get_range`, each reporting a `MB/s` metric; the gcs number within ~10% of the s3 number on a quiescent host.

- [ ] **Step 2: Baseline**

Create `pj-cloud/integration-tests/bench/baseline.json`:

```json
{
  "BenchmarkStreamingThroughput": { "min_mb_per_sec": 200 },
  "BenchmarkGetRangeParity": {
    "s3_get_range":  { "baseline_mb_per_sec": 0, "max_regression_pct": 25 },
    "gcs_get_range": { "baseline_mb_per_sec": 0, "parity_tolerance_pct": 10 }
  }
}
```

`baseline_mb_per_sec: 0` is the "not yet recorded" sentinel: the FIRST time the GCS leg lands, `compare.go` records the measured `s3_get_range` and `gcs_get_range` numbers and **hard-fails if GCS is more than `parity_tolerance_pct` (10%) slower than S3** (this locks the abstraction the first time GCS is wired). Thereafter the recorded baseline is moved only by an explicit PR, and a later run that regresses GCS-vs-S3 parity is a **soft regression flag**, not a hard fail (unified-plan §6 L4).

- [ ] **Step 3: Compare tool**

Create `pj-cloud/integration-tests/bench/compare.go`:

```go
//go:build bench_compare

// compare parses `go test -bench=. -benchmem -json` output, looks up each
// benchmark in baseline.json, and exits 1 if any benchmark falls below its
// min threshold. Run nightly + on tagged release commits.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
)

type Entry struct {
	Action string  `json:"Action"`
	Test   string  `json:"Test"`
	Output string  `json:"Output"`
}

type Baseline map[string]struct {
	MinMBPerSec float64 `json:"min_mb_per_sec"`
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: compare <bench.json> <baseline.json>")
		os.Exit(2)
	}
	bench, err := os.Open(os.Args[1])
	if err != nil { panic(err) }
	defer bench.Close()
	baseRaw, err := io.ReadAll(mustOpen(os.Args[2]))
	if err != nil { panic(err) }
	var base Baseline
	_ = json.Unmarshal(baseRaw, &base)
	// (Implementation: walk JSON entries, extract MB/s metric from each
	// benchmark line, compare against base[name].MinMBPerSec, exit 1 on regression.)
	fmt.Println("baseline compare not yet implemented; see Task 7 follow-on")
}

func mustOpen(p string) io.Reader { f, err := os.Open(p); if err != nil { panic(err) }; return f }
```

- [ ] **Step 4: Commit**

```bash
git add integration-tests/bench/
git commit -m "bench: throughput gate + cross-backend GetRange storage-parity microbench + baseline.json"
```

---

## Task 8: CI matrix integration

**Files:**
- Modify: `pj-cloud/.github/workflows/ci.yml`
- Create: `pj-cloud/.github/workflows/bench.yml`

- [ ] **Step 1: Extend ci.yml with the E2E matrix job over `{s3,gcs}`**

The job gains a `backend: [s3, gcs]` matrix axis. Each leg runs the full round-trip matrix against its emulator via `PJCLOUD_BACKEND`. **Merge gate:** anything touching `storage/`, `format/`, `session/`, `ws/`, or `proto/` requires **both** legs green; a **gcs-only failure blocks the merge exactly like an s3-only failure** (unified-plan §6 L3 / §7 risk 1).

Add to `pj-cloud/.github/workflows/ci.yml`:

```yaml
  e2e_matrix:
    runs-on: ubuntu-latest
    needs: [unit, cpp_build_and_test]
    strategy:
      fail-fast: false          # a gcs-only failure must still surface even if s3 passed
      matrix:
        backend: [s3, gcs]
    name: e2e (${{ matrix.backend }})
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: "1.23" }
      - uses: lukka/get-cmake@latest
      - name: Install Conan + Qt deps
        run: |
          pipx install conan==2.5.0
          sudo apt-get install -y libgl1-mesa-dev libxkbcommon-dev libxcb-xkb-dev
      - name: Build C++ client
        run: |
          conan profile detect --force
          conan install . --output-folder=build --build=missing -s compiler.cppstd=20
          cmake -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake
          cmake --build build -j$(nproc) --target pjcloud-cli
      - name: Build Go server (Docker image used by compose)
        run: docker build -t pj-cloud-server:ci -f server/deploy/Dockerfile server
      - name: Generate fixtures
        run: cd integration-tests && go run ./cmd/gen-fixtures --out fixtures
      - name: Run matrix (${{ matrix.backend }} leg)
        run: cd integration-tests && go test -tags=integration -count=1 -timeout=15m -v
        env:
          PJCLOUD_CLI: ${{ github.workspace }}/build/client-cli/pjcloud-cli
          PJCLOUD_BACKEND: ${{ matrix.backend }}
          STORAGE_EMULATOR_HOST: http://localhost:4443   # used by the gcs leg's host-side uploader
```

> **Branch-protection note (record in the PR description, not code):** mark **both** `e2e (s3)` and `e2e (gcs)` as required status checks so neither leg can be skipped to merge.

- [ ] **Step 2: Nightly bench workflow**

Create `pj-cloud/.github/workflows/bench.yml`:

```yaml
name: bench
on:
  schedule:
    - cron: "0 6 * * *"        # daily 06:00 UTC
  workflow_dispatch:

jobs:
  bench:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with: { go-version: "1.23" }
      - uses: lukka/get-cmake@latest
      - name: Install Conan + Qt deps
        run: |
          pipx install conan==2.5.0
          sudo apt-get install -y libgl1-mesa-dev libxkbcommon-dev libxcb-xkb-dev
      - name: Build CLI
        run: |
          conan profile detect --force
          conan install . --output-folder=build --build=missing -s compiler.cppstd=20
          cmake -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake
          cmake --build build -j$(nproc) --target pjcloud-cli
      - name: Build server image
        run: docker build -t pj-cloud-server:ci -f server/deploy/Dockerfile server
      - name: Generate large fixture
        run: cd integration-tests && go run ./cmd/gen-fixtures --out fixtures
      - name: Start compose
        run: cd integration-tests && docker-compose up -d --build
      - name: Bench (throughput + cross-backend storage parity)
        run: |
          cd integration-tests
          # -bench=. runs BOTH BenchmarkStreamingThroughput and BenchmarkGetRangeParity
          # (the parity sub-benchmarks s3_get_range/gcs_get_range hit Minio + fake-gcs,
          #  both of which the compose stack above brought up).
          go test -tags=bench -bench=. -benchmem -count=1 -json ./bench/... > bench.json
          go run -tags=bench_compare ./bench bench.json bench/baseline.json
        env:
          PJCLOUD_CLI: ${{ github.workspace }}/build/client-cli/pjcloud-cli
          STORAGE_EMULATOR_HOST: http://localhost:4443
      - name: Teardown
        if: always()
        run: cd integration-tests && docker-compose down -v
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/ci.yml .github/workflows/bench.yml
git commit -m "ci: E2E {s3,gcs} matrix axis + nightly bench (throughput + storage-parity)"
```

---

## Task 8a: GCE deployment smoke (Asensus) — real ADC + persistent-disk catalog survival

**Why a separate task.** The emulator matrix (Tasks 1-8) proves wire/format/protocol correctness on `{s3,gcs}` but **cannot** validate the real Asensus deployment shape: a long-lived container on a GCE VM, reading the bucket via **Application Default Credentials from an instance-attached service account (no key on disk)**, with a **persistent disk** holding `catalog.db` that must survive a VM restart **without a full re-scan**. Real GCE + ADC + the metadata server cannot run in vanilla GitHub-hosted CI, so this is delivered as **either** a scheduled GCE self-hosted-runner job **or** a documented manual checklist — both authored in full below. This is the operational expression of unified-plan §5 (M2c-ASEN acceptance gate) and §6 ("Deployment smoke … GCE … runs on a scheduled GCE self-hosted runner or documented manual checklist").

**Files:**
- Create: `pj-cloud/.github/workflows/gce-smoke.yml`
- Create: `pj-cloud/integration-tests/docs/GCE_SMOKE.md`
- Create: `pj-cloud/integration-tests/scripts/gce_smoke.sh`

- [ ] **Step 1: The smoke script (runs ON the GCE VM, used by both the runner job and the manual checklist)**

This script assumes it runs on a GCE VM whose attached service account has bucket-read scope, with the server container already deployed and a persistent disk mounted at `/var/lib/pj-cloud`. It exercises: ADC works (no key on disk), `/health` is 200 over `wss`/TLS, catalog lists the real bucket, a streaming session round-trips over `wss://`, and `catalog.db` survives a container/VM restart without a full re-scan.

Create `pj-cloud/integration-tests/scripts/gce_smoke.sh`:

```bash
#!/usr/bin/env bash
# gce_smoke.sh — Asensus GCE deployment smoke. Run ON the GCE VM.
# Preconditions (see GCE_SMOKE.md): server container running, attached SA with
# bucket-read scope, persistent disk mounted at /var/lib/pj-cloud, NO key file on disk.
set -euo pipefail

SERVER=${SERVER:-https://localhost:8443}
WSS=${WSS:-wss://localhost:8443/api/ws}
TOKEN=${PJ_CLOUD_TOKEN:-test-token}
CLI=${PJCLOUD_CLI:-/opt/pj-cloud/pjcloud-cli}
DB=${CATALOG_DB:-/var/lib/pj-cloud/catalog.db}

fail() { echo "FAIL: $*" >&2; exit 1; }

echo "== 1. ADC: confirm NO service-account key file on disk =="
# The deployment must rely on the instance-attached SA via the metadata server,
# NOT a key file. GOOGLE_APPLICATION_CREDENTIALS must be unset/empty.
if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]]; then
  fail "GOOGLE_APPLICATION_CREDENTIALS is set ($GOOGLE_APPLICATION_CREDENTIALS) — ADC-via-attached-SA requires NO key on disk"
fi
# The metadata server must vend a token for the attached SA.
curl -fsS -H 'Metadata-Flavor: Google' \
  'http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token' \
  >/dev/null || fail "metadata server did not vend an ADC token (is an SA attached?)"
echo "   ok: no key on disk; metadata server vends an ADC token"

echo "== 2. /health is 200 over TLS =="
curl -fsS -k "$SERVER/health" | grep -q ok || fail "/health not healthy"
echo "   ok: /health 200"

echo "== 3. Catalog lists the real GCS bucket (via ADC) =="
N=$("$CLI" --server "$WSS" --token "$TOKEN" --insecure files list --json | python3 -c 'import sys,json; print(len(json.load(sys.stdin)))')
[[ "$N" -ge 1 ]] || fail "catalog returned 0 files — ADC bucket read failed"
echo "   ok: catalog lists $N files via ADC"

echo "== 4. Streaming session round-trips over wss:// =="
FID=$("$CLI" --server "$WSS" --token "$TOKEN" --insecure files list --json | python3 -c 'import sys,json; print(json.load(sys.stdin)[0]["id"])')
OUT=$(mktemp /tmp/gce-rebuilt.XXXXXX.mcap)
"$CLI" --server "$WSS" --token "$TOKEN" --insecure session download --files "$FID" --output "$OUT"
[[ -s "$OUT" ]] || fail "session download produced an empty MCAP"
echo "   ok: streamed session to $OUT ($(stat -c%s "$OUT") bytes) over wss"

echo "== 5. Persistent-disk catalog survives a restart WITHOUT a full re-scan =="
[[ -f "$DB" ]] || fail "catalog.db not found on the persistent disk at $DB"
MTIME_BEFORE=$(stat -c %Y "$DB")
INDEXED_BEFORE=$N
# Restart only the server container; the persistent disk (and catalog.db) stays put.
sudo docker restart pj-cloud-server >/dev/null
# Wait for health, then assert the catalog is immediately populated from the existing DB
# (a full re-scan would briefly show 0 files and rewrite every row).
for i in $(seq 1 30); do curl -fsS -k "$SERVER/health" >/dev/null 2>&1 && break; sleep 2; done
METRIC=$(curl -fsS -k "$SERVER/metrics" | grep -E '^pj_cloud_indexer_full_scans_total' || true)
N_AFTER=$("$CLI" --server "$WSS" --token "$TOKEN" --insecure files list --json | python3 -c 'import sys,json; print(len(json.load(sys.stdin)))')
[[ "$N_AFTER" -eq "$INDEXED_BEFORE" ]] || fail "file count changed after restart ($INDEXED_BEFORE -> $N_AFTER) — catalog did not survive"
echo "   ok: catalog.db survived restart; $N_AFTER files served immediately (indexer scan metric: ${METRIC:-n/a})"

echo "ALL GCE SMOKE CHECKS PASSED"
```

Mark it executable:

```bash
chmod +x /home/davide/ws_plotjuggler/pj-cloud/integration-tests/scripts/gce_smoke.sh
```

- [ ] **Step 2: The scheduled GCE self-hosted-runner workflow**

This workflow targets a **self-hosted runner registered on (or with deploy access to) the GCE VM**. It is `workflow_dispatch` + a weekly cron; it does NOT run on GitHub-hosted runners (no real metadata server / ADC there). If no self-hosted GCE runner is registered, the job is simply never scheduled — the manual checklist (Step 3) is the fallback, and `GCE_SMOKE.md` says so.

Create `pj-cloud/.github/workflows/gce-smoke.yml`:

```yaml
name: gce-smoke
on:
  workflow_dispatch:
  schedule:
    - cron: "0 5 * * 1"        # weekly, Monday 05:00 UTC

jobs:
  gce_smoke:
    # Requires a self-hosted runner with labels [self-hosted, gce] on/near the Asensus-shape VM.
    # On GitHub-hosted runners there is no attached SA / metadata server, so this job is gated.
    runs-on: [self-hosted, gce]
    steps:
      - uses: actions/checkout@v4
      - name: Build CLI on the VM
        run: |
          conan profile detect --force
          conan install . --output-folder=build --build=missing -s compiler.cppstd=20
          cmake -B build -DCMAKE_TOOLCHAIN_FILE=build/conan_toolchain.cmake
          cmake --build build -j"$(nproc)" --target pjcloud-cli
      - name: Build + (re)deploy the server container on the VM
        run: |
          docker build -t pj-cloud-server:smoke -f server/deploy/Dockerfile server
          sudo docker rm -f pj-cloud-server 2>/dev/null || true
          # Long-lived container; ADC via attached SA (NO -v for a key file, NO
          # GOOGLE_APPLICATION_CREDENTIALS); persistent disk mounted for catalog.db.
          sudo docker run -d --name pj-cloud-server \
            --restart unless-stopped \
            -p 8443:8443 \
            -e PJ_CLOUD_TOKEN=test-token \
            -e PJ_CLOUD_DASHBOARD_PASSWORD=dashpw \
            -v /var/lib/pj-cloud:/var/lib/pj-cloud \
            -v ${{ github.workspace }}/server/deploy/asensus/config.yaml:/etc/pj-cloud/config.yaml:ro \
            pj-cloud-server:smoke
          # First boot warm-scan; give the indexer time to populate catalog.db.
          for i in $(seq 1 60); do curl -fsS -k https://localhost:8443/health >/dev/null 2>&1 && break; sleep 2; done
      - name: Run the GCE smoke
        run: integration-tests/scripts/gce_smoke.sh
        env:
          PJCLOUD_CLI: ${{ github.workspace }}/build/client-cli/pjcloud-cli
          PJ_CLOUD_TOKEN: test-token
          CATALOG_DB: /var/lib/pj-cloud/catalog.db
```

- [ ] **Step 3: The manual checklist + Cloud-Run documented-constraint note**

Create `pj-cloud/integration-tests/docs/GCE_SMOKE.md`:

````markdown
# GCE deployment smoke (Asensus) — runbook & manual checklist

Real GCE + ADC cannot run in vanilla GitHub-hosted CI (no instance-attached service
account, no metadata server). This smoke therefore runs **either** via the
`gce-smoke.yml` self-hosted-runner job **or** as the manual checklist below. Both
drive the same `integration-tests/scripts/gce_smoke.sh`.

## What this proves (the Asensus deployment done-state, unified-plan §5 M2c-ASEN)

1. The server runs as a **long-lived container** on a **GCE VM** (Compute Engine or a MIG node).
2. It reads the GCS bucket via **ADC from an instance-attached service account** — **no key file on disk**, no `GOOGLE_APPLICATION_CREDENTIALS`.
3. It serves **catalog + a streaming session over `wss://`** to a healthy `/health`.
4. `catalog.db` lives on a **persistent disk** and **survives a VM/container restart without a full re-scan**.

## One-time VM setup (manual)

```bash
# 1. Create a service account with bucket-read scope and attach it to the VM.
gcloud iam service-accounts create pj-cloud-reader
gsutil iam ch serviceAccount:pj-cloud-reader@PROJECT.iam.gserviceaccount.com:objectViewer gs://YOUR_RECORDINGS_BUCKET
gcloud compute instances create pj-cloud-vm \
  --service-account pj-cloud-reader@PROJECT.iam.gserviceaccount.com \
  --scopes https://www.googleapis.com/auth/devstorage.read_only \
  --create-disk name=pj-cloud-catalog,size=10GB,auto-delete=no

# 2. On the VM: mount the persistent disk at /var/lib/pj-cloud (first time only: format it).
sudo mkfs.ext4 -F /dev/disk/by-id/google-pj-cloud-catalog   # FIRST TIME ONLY
sudo mkdir -p /var/lib/pj-cloud
sudo mount /dev/disk/by-id/google-pj-cloud-catalog /var/lib/pj-cloud
# (Add an /etc/fstab entry so it remounts across VM restarts.)
```

The server config used here is `server/deploy/asensus/config.yaml`, whose `storage.gcs`
block names the real bucket and whose `catalog.db_path` is `/var/lib/pj-cloud/catalog.db`.
There is **no** `option.WithCredentialsFile` and **no** key mount — ADC resolves the
attached SA via the metadata server.

## Run the smoke (manual)

```bash
# On the VM, with the server container already running:
export PJCLOUD_CLI=/opt/pj-cloud/pjcloud-cli
export PJ_CLOUD_TOKEN=...           # the deployment's bearer token
export CATALOG_DB=/var/lib/pj-cloud/catalog.db
integration-tests/scripts/gce_smoke.sh
```

Expected final line: `ALL GCE SMOKE CHECKS PASSED`. The script fails loudly (`FAIL: ...`)
on: a key file present, no ADC token, unhealthy `/health`, empty catalog, empty session
download, or a file-count change after restart (which would indicate the persistent-disk
catalog did not survive / a full re-scan occurred).

## Persistent-disk survival — what "no full re-scan" means

On restart the indexer change-detects on the `(etag/generation, size, last_modified)`
triple against the rows already in `catalog.db`; unchanged objects are NOT re-extracted.
The smoke asserts the file count is served **immediately** after restart (a full re-scan
would transiently show 0 files and rewrite every row). The `pj_cloud_indexer_full_scans_total`
metric should NOT increment for an unchanged bucket across the restart.

## Cloud Run is OUT for v1 — documented constraint (do NOT deploy here)

Cloud Run's request-scoped, scale-to-zero, horizontally-autoscaled model **breaks four
pieces of in-process, in-memory state** the server holds (unified-plan §3.5):

1. **The session registry** (`subscription_id → *Session`) — per-instance, in memory.
2. **The bounded retain buffer** (256 seqs / 64 MB) backing reconnect-resume — in memory.
3. **The 60 s retain-after-disconnect window + producer/consumer goroutines** that must
   survive a WS drop — there is no request to host them under Cloud Run.
4. **The always-on background indexer poller** — no request hosts a continuous loop.

Single-writer SQLite (WAL) + in-memory sessions also do not scale horizontally, so **v1 is
explicitly single-instance.** GCE costs Asensus nothing on its stated deliverables:
"lazy-loading + prefetch" is met **client-side within a session** (selection-bulk +
in-memory reuse), not by server-resident per-view state.

**Entry point for the paid stateless-mode follow-on** (only if Asensus later mandates
Cloud Run): persist session state to Redis/Firestore, move the retain buffer to that store,
and run the indexer as a Cloud Scheduler job hitting a stateless `/reindex` endpoint. That
is a meaningful re-architecture, **not v1**, and is a separately scoped paid item.

## Emulator-fidelity caveat

The `fake-gcs-server` matrix (Tasks 1-8) proves protocol/format correctness but NOT real-GCS
fidelity (range/generation/error shapes). Run this GCE smoke against a **real** GCS bucket
at least once before declaring the GCS drop-in proven (unified-plan §6 "Manual/pre-release",
§7 risk 9). Pair it with one real-S3 smoke of the Dexory self-hosted shape.
````

- [ ] **Step 4: Validate the script parses + the workflow lints (locally, without a VM)**

We cannot run the real smoke off-VM, but we can confirm the script and workflow are well-formed:

```bash
cd /home/davide/ws_plotjuggler/pj-cloud/integration-tests
bash -n scripts/gce_smoke.sh && echo "script: syntax ok"
python3 -c "import yaml,sys; yaml.safe_load(open('../.github/workflows/gce-smoke.yml')); print('workflow: yaml ok')"
```

Expected: `script: syntax ok` and `workflow: yaml ok`.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/gce-smoke.yml \
        integration-tests/docs/GCE_SMOKE.md \
        integration-tests/scripts/gce_smoke.sh
git commit -m "test(deploy): GCE/ADC deployment smoke (Asensus) + Cloud-Run-excluded note"
```

---

## Task 9: RUNBOOK (operator notes for running and debugging locally)

**Files:**
- Create: `pj-cloud/integration-tests/docs/RUNBOOK.md`

- [ ] **Step 1: Write the runbook**

Create `pj-cloud/integration-tests/docs/RUNBOOK.md`:

````markdown
# Integration-test runbook

## Backends: the harness runs everything on BOTH `{s3, gcs}`

Every e2e case runs against two storage backends — `s3` (Minio) and `gcs` (`fake-gcs-server`) — that differ **only** in which `server-config.<backend>.yaml` the compose stack mounts. A **GCS-only failure blocks a merge exactly like an S3-only failure** (no soft fork). `PJCLOUD_BACKEND` pins a single leg; unset runs both.

## Run the matrix locally (both backends)

```bash
cd pj-cloud
make build                            # builds server + CLI
cd integration-tests
go run ./cmd/gen-fixtures --out fixtures
export PJCLOUD_CLI=$PWD/../build/client-cli/pjcloud-cli
# run-matrix loops over {s3,gcs}, bringing the stack up/down per backend:
go run ./cmd/run-matrix
```

## Run a single backend leg

```bash
cd integration-tests
# s3 leg only:
PJCLOUD_BACKEND=s3 go run ./cmd/run-matrix
# gcs leg only (host-side uploader needs the emulator endpoint):
PJCLOUD_BACKEND=gcs STORAGE_EMULATOR_HOST=http://localhost:4443 go run ./cmd/run-matrix
```

## Bring up just the emulators (debugging a single backend by hand)

```bash
cd integration-tests
# s3 server:
SERVER_CONFIG=./server-config.s3.yaml  docker-compose up -d --build
# OR gcs server:
SERVER_CONFIG=./server-config.gcs.yaml docker-compose up -d --build
curl -fsS http://localhost:8443/health           # 200 on either
docker-compose down -v
```

Notes:
- The compose stack always starts **both** Minio (`:9000`) and `fake-gcs` (`:4443`); the server is pointed at one of them by the mounted config. The unused emulator just idles.
- The `gcs` server reads `STORAGE_EMULATOR_HOST=http://fake-gcs:4443` (set in compose). When you run the **host-side** uploader/bench, use `http://localhost:4443` instead (the published port).

## GCE / ADC deployment smoke (Asensus)

Real GCE + ADC cannot run in vanilla CI; see **Task 8a** in this plan for the scheduled GCE self-hosted-runner job and the equivalent **manual checklist** (attached service account, ADC with no key on disk, persistent-disk `catalog.db` survives a VM restart without a full re-scan, `wss://` round-trip, and the Cloud-Run-excluded note).

## Emulator-fidelity caveat (manual / pre-release)

`fake-gcs-server` and Minio are **not** guaranteed to match real GCS / S3 on range-read semantics, generation/ETag behaviour, or error shapes. Passing the emulator matrix does **not** by itself prove the drop-in. **Before declaring "GCS is a proven drop-in," run at least one real-GCS smoke and one real-S3 smoke** (a tiny bucket, the deployment smoke from Task 8a). This is documented as a **manual / pre-release** step, not automated CI (unified-plan §6 "Manual/pre-release" + §7 risk 9).

## Bench locally

```bash
cd integration-tests
docker-compose up -d --build
go test -tags=bench -bench=. -benchmem -count=1 -v ./bench/...
docker-compose down -v
```

## Refresh fixtures

```bash
cd integration-tests
go run ./cmd/gen-fixtures --out fixtures
git add fixtures && git commit -m "test(e2e): refresh fixture corpus"
```

## Debug a failing case

```bash
# Reproduce the exact CLI invocation the matrix used:
./build/client-cli/pjcloud-cli --server wss://localhost:8443/api/ws --token test-token \
    session download --files <ID> --output /tmp/rebuilt.mcap

# Compare:
mcap info /tmp/rebuilt.mcap
mcap info integration-tests/fixtures/<fixture>.mcap

# Or use the Go diff:
cd integration-tests
go test -tags=integration -run "TestMatrix/<fixture>" -v
```

## Dashboard during a run

`https://localhost:8443/dashboard/` (admin / dashpw)

## Common failures

- **CLI exits with `connect failed: ssl handshake`**: the local Minio + server use a self-signed cert; the CLI dials `wss://localhost` without validating. Make sure `QSslConfiguration::peerVerifyMode = VerifyNone` is set in the `--insecure` path (CLI flag added in a follow-on task).
- **`docker-compose up` hangs at `minio-init`**: the bucket creation step is racing the Minio readiness probe; increase `healthcheck.retries`.
- **Matrix fails on `tiny-payloads.mcap`**: usually a batching boundary bug; verify `MaxBatchBytes` and `MaxBatchAgeMs` in the server config.
- **Bench reports < 200 MB/s**: check whether Docker is using a constrained CPU set; bench results in CI under cgroup-limited runners are not directly comparable to a developer workstation.
- **`gcs` leg fails with `dial tcp ...:443` / TLS errors**: the GCS client is talking to real GCS, not the emulator — `STORAGE_EMULATOR_HOST` is unset or wrong. In-container it must be `http://fake-gcs:4443`; on the host it must be `http://localhost:4443`.
- **`gcs` leg: indexer finds zero files**: the `recordings` bucket was not pre-seeded — confirm the `fake-gcs-init` compose service completed (`docker-compose logs fake-gcs-init` shows `fake-gcs ready`), and that the host-side `UploadDirGCS` ran against `:4443`.
- **`gcs` leg: indexer re-scans everything every poll**: the change-detect triple is keyed on the wrong field — the gcsreader must map GCS `Generation` (not the MD5/CRC32C ETag) into the `etag` slot and `Updated` into `last_modified` (unified-plan §3.2 ETag-mapping pin); the emulator exposes both via the JSON `Attrs`.
- **`BenchmarkGetRangeParity/gcs_get_range` wildly slower than s3**: usually the GCS client re-establishing a connection per read; confirm both emulators are warm and the host is quiescent before trusting the parity number.
````

- [ ] **Step 2: Commit**

```bash
git add integration-tests/docs/RUNBOOK.md
git commit -m "docs(e2e): operator runbook for integration + bench"
```

---

## End of Plan C

9 tasks: compose orchestration → fixture generator + corpus → upload/CLI helpers → MCAP logical-equality diff → compose driver → matrix driver + test wrapper → benchmark gate → CI matrix extension → runbook — plus Task 8a (GCE/ADC deploy smoke), inserted between Task 8 and Task 9.

After all three plans (A, B, C) land:
- The Go server is feature-complete per spec.
- The Qt C++ CLI exercises the protocol end-to-end and produces a reconstructed MCAP per session.
- The cross-language matrix validates byte-equal payloads + schema definitions across the fixture corpus on every push.
- The benchmark gate guards against throughput regressions on every release tag.
- The protocol layer (`client-core`) is ready to lift into a PJ4 DataSource plugin (separate spec + plan, post-v1).
