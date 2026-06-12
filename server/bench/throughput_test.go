//go:build bench

// Package bench holds the minimal, HONEST streaming-throughput gate for the PJ
// Cloud Connector (Plan A Task 45, scoped). It is build-tagged `bench` so it is
// invisible to `go test ./...` and `make smoke`; run it with `make bench`
// (= `go test -tags=bench -bench=. ./bench/...`).
//
// WHAT IT MEASURES, AND WHAT IT DOES NOT
//
//	It downloads the LARGEST corpus file (nissan_zala_90_country_road_2_0.mcap,
//	~15 MB on disk) over a REAL WebSocket session from a freshly-started server
//	against the local Minio dev bucket, N iterations, and reports the aggregate
//	wire throughput in MB/s (decompressed payload bytes / wall-clock seconds).
//	This is a LOCALHOST LOWER BOUND: server + Minio + client all share one box,
//	so it is dominated by ZSTD (de)compression + loopback, NOT a network link.
//	The 200 MB/s reference-machine gate is a SEPARATE M2a SOW item and is NOT
//	what this asserts.
//
// REGRESSION FLOOR
//
//	If server/bench/baseline.json exists, the bench FAILS when the measured
//	throughput drops below baselineFloorFraction (0.30) of the recorded baseline
//	— a deliberately GENEROUS floor that catches gross regressions (e.g. an
//	accidental O(n^2) on the hot path) without flapping on a noisy shared box.
//	If the baseline is absent it is WRITTEN (first run / re-baseline) and the run
//	passes. Re-baseline intentionally by deleting the file.
//
// PRECONDITIONS (the bench SKIPS, never fails, when unmet — it is opt-in):
//   - Minio up on :9000 with the `recordings` bucket seeded (infra/minio).
//   - The Go toolchain able to `go build ./cmd/pj-cloud-server`.
//
// It owns port :8082 for its throwaway server and ALWAYS reaps it (never touches
// the user's :8080 nor the smoke harness :8081 / matrix :8082-at-other-times —
// the bench and matrix never run concurrently).
package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
	"nhooyr.io/websocket"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

const (
	// benchKey is the largest corpus file — the most throughput-representative.
	benchKey = "nissan_zala_90_country_road_2_0.mcap"
	// benchPort is this bench's throwaway server port (NOT :8080/:8081).
	benchPort = 8082
	// benchIterations is how many full downloads to time (aggregated).
	benchIterations = 3
	// minioHealth is the local Minio liveness probe; absent => SKIP.
	minioHealth = "http://localhost:9000/minio/health/live"
	// baselineFloorFraction: fail under 30% of the recorded baseline MB/s.
	baselineFloorFraction = 0.30
)

// baseline is the persisted MB/s reference at server/bench/baseline.json.
type baseline struct {
	Key            string  `json:"key"`
	IterationCount int     `json:"iterations"`
	MBPerSec       float64 `json:"mb_per_sec"`
	PayloadBytes   uint64  `json:"payload_bytes_per_iteration"`
	Note           string  `json:"note"`
	RecordedAt     string  `json:"recorded_at"`
}

func baselinePath() string { return filepath.Join("baseline.json") }

// TestThroughputGate is the bench entry point (a Test, not a Benchmark, so it can
// own the heavy server/Minio setup once and assert the regression floor with a
// normal t.Fatalf). `go test -bench=.` still runs it because it is a Test.
func TestThroughputGate(t *testing.T) {
	if !minioReachable() {
		t.Skipf("Minio not reachable at %s — bench is opt-in (start infra/minio)", minioHealth)
	}

	bin := buildServer(t)
	srvURL, stop := startServer(t, bin)
	defer stop()

	waitCatalogReady(t, srvURL)

	wsURL := fmt.Sprintf("ws://localhost:%d/api/ws", benchPort)

	// Warm-up download (priming Minio range-read caches / decoder pools) — not timed.
	if _, err := downloadOnce(t, wsURL, benchKey); err != nil {
		t.Fatalf("warm-up download failed: %v", err)
	}

	var totalPayload uint64
	var perIterPayload uint64
	start := time.Now()
	for i := 0; i < benchIterations; i++ {
		got, err := downloadOnce(t, wsURL, benchKey)
		if err != nil {
			t.Fatalf("iteration %d download failed: %v", i, err)
		}
		if perIterPayload == 0 {
			perIterPayload = got
		} else if got != perIterPayload {
			t.Fatalf("iteration %d delivered %d payload bytes; iteration 0 delivered %d (non-deterministic stream)", i, got, perIterPayload)
		}
		totalPayload += got
	}
	elapsed := time.Since(start)

	mbPerSec := (float64(totalPayload) / (1024 * 1024)) / elapsed.Seconds()
	t.Logf("BENCH throughput: %.1f MB/s (%d iterations, %d payload bytes each, %.2fs wall)",
		mbPerSec, benchIterations, perIterPayload, elapsed.Seconds())
	t.Logf("NOTE: localhost lower-bound (server+Minio+client co-resident); the 200 MB/s reference-machine gate is a separate M2a SOW item")

	// Emit a machine-readable line the `make bench` target / harness can grep.
	fmt.Printf("BENCH_MBPS=%.1f\n", mbPerSec)

	base, err := loadBaseline()
	if err == nil {
		floor := base.MBPerSec * baselineFloorFraction
		t.Logf("baseline: %.1f MB/s (recorded %s); regression floor %.1f MB/s (%.0f%%)",
			base.MBPerSec, base.RecordedAt, floor, baselineFloorFraction*100)
		if mbPerSec < floor {
			t.Fatalf("THROUGHPUT REGRESSION: %.1f MB/s < floor %.1f MB/s (%.0f%% of baseline %.1f MB/s)",
				mbPerSec, floor, baselineFloorFraction*100, base.MBPerSec)
		}
		return
	}

	// No baseline yet: record one (first run / explicit re-baseline) and pass.
	nb := baseline{
		Key:            benchKey,
		IterationCount: benchIterations,
		MBPerSec:       mbPerSec,
		PayloadBytes:   perIterPayload,
		Note:           "localhost lower-bound; server+Minio+client co-resident. 200 MB/s reference-machine gate is a separate M2a SOW item. Regression floor = 30% of this.",
		RecordedAt:     time.Now().UTC().Format(time.RFC3339),
	}
	if werr := writeBaseline(nb); werr != nil {
		t.Fatalf("write baseline: %v", werr)
	}
	t.Logf("wrote new baseline %s: %.1f MB/s", baselinePath(), mbPerSec)
}

func minioReachable() bool {
	c := &http.Client{Timeout: 2 * time.Second}
	resp, err := c.Get(minioHealth)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// buildServer compiles cmd/pj-cloud-server into a temp binary (matches what the
// deploy Dockerfile builds), returning its path.
func buildServer(t *testing.T) string {
	t.Helper()
	out := filepath.Join(t.TempDir(), "pj-cloud-server")
	cmd := exec.Command("go", "build", "-o", out, "./cmd/pj-cloud-server")
	cmd.Dir = serverDir(t)
	cmd.Env = append(os.Environ(), "GOTOOLCHAIN=local")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build pj-cloud-server: %v\n%s", err, b)
	}
	return out
}

// serverDir returns the absolute server/ module dir (this test lives in
// server/bench/, so the module root is one level up).
func serverDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	return filepath.Dir(wd)
}

// startServer launches the built binary on :benchPort against the local Minio
// with a throwaway DB, returning a base http URL and a reaper.
func startServer(t *testing.T, bin string) (string, func()) {
	t.Helper()
	db := filepath.Join(t.TempDir(), "bench-catalog.db")
	cmd := exec.Command(bin,
		"-listen", fmt.Sprintf(":%d", benchPort),
		"-db", db,
		"-poll-interval", "30s",
		"-log-level", "warn",
	)
	// No PJ_CLOUD_TOKEN => dev-anonymous; defaults already point at local Minio.
	cmd.Env = append(os.Environ(), "PJ_CLOUD_DB="+db)
	logf, _ := os.Create(filepath.Join(t.TempDir(), "bench-server.log"))
	cmd.Stdout = logf
	cmd.Stderr = logf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start server: %v", err)
	}
	stop := func() {
		if cmd.Process != nil {
			_ = cmd.Process.Signal(os.Interrupt)
			done := make(chan struct{})
			go func() { _, _ = cmd.Process.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = cmd.Process.Kill()
			}
		}
		if logf != nil {
			_ = logf.Close()
		}
	}
	return fmt.Sprintf("http://localhost:%d", benchPort), stop
}

// waitCatalogReady blocks until /health is 200 AND ListFiles can resolve benchKey
// (the indexer warm-start has populated the catalog), or fails after a timeout.
func waitCatalogReady(t *testing.T, baseURL string) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	hc := &http.Client{Timeout: 2 * time.Second}
	wsURL := fmt.Sprintf("ws://localhost:%d/api/ws", benchPort)
	for time.Now().Before(deadline) {
		resp, err := hc.Get(baseURL + "/health")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				if id := resolveFileID(wsURL, benchKey); id != 0 {
					return
				}
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	t.Fatalf("server/catalog not ready within 60s (health + %s indexed)", benchKey)
}

// resolveFileID does a Hello+ListFiles and returns benchKey's file id (0 if not
// present / not yet indexed).
func resolveFileID(wsURL, key string) uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return 0
	}
	defer conn.CloseNow()
	conn.SetReadLimit(64 << 20)
	if _, err := rpc(ctx, conn, &pb.ClientMessage{
		RequestId: 1,
		Payload:   &pb.ClientMessage_Hello{Hello: &pb.Hello{ProtocolVersion: 1}},
	}); err != nil {
		return 0
	}
	resp, err := rpc(ctx, conn, &pb.ClientMessage{
		RequestId: 2,
		Payload:   &pb.ClientMessage_ListFiles{ListFiles: &pb.ListFilesRequest{Limit: 1000}},
	})
	if err != nil || resp.GetListFiles() == nil {
		return 0
	}
	for _, f := range resp.GetListFiles().GetFiles() {
		if f.GetS3Key() == key {
			return f.GetId()
		}
	}
	return 0
}

// downloadOnce performs a full FRESH-session download of key, decoding every
// batch (ZSTD one-shot) exactly as the reference devprobe/CLI downloaders do,
// and returns the total DECOMPRESSED payload bytes delivered. It does not write
// an MCAP — throughput is about the transport+decode hot path, not disk.
func downloadOnce(t *testing.T, wsURL, key string) (uint64, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return 0, fmt.Errorf("dial: %w", err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(64 << 20)

	if _, err := rpc(ctx, conn, &pb.ClientMessage{
		RequestId: 1,
		Payload:   &pb.ClientMessage_Hello{Hello: &pb.Hello{ProtocolVersion: 1}},
	}); err != nil {
		return 0, err
	}
	id := resolveFileIDOnConn(ctx, conn, key)
	if id == 0 {
		return 0, fmt.Errorf("file id for %q not found", key)
	}

	if err := writeMsg(ctx, conn, &pb.ClientMessage{
		RequestId: 3,
		Payload: &pb.ClientMessage_OpenSession{OpenSession: &pb.OpenSessionRequest{
			Mode: &pb.OpenSessionRequest_Fresh{Fresh: &pb.OpenFresh{FileIds: []uint64{id}}},
		}},
	}); err != nil {
		return 0, err
	}
	openMsg, err := readMsg(ctx, conn)
	if err != nil {
		return 0, err
	}
	or := openMsg.GetOpenSession()
	if or == nil {
		return 0, fmt.Errorf("expected OpenSessionResponse, got %T (err %v)", openMsg.GetPayload(), openMsg.GetError())
	}
	subID := or.GetSubscriptionId()

	dec, err := zstd.NewReader(nil)
	if err != nil {
		return 0, err
	}
	defer dec.Close()

	var payloadBytes uint64
	var lastSeq uint64
	batchesSinceAck := 0
	for {
		msg, err := readMsg(ctx, conn)
		if err != nil {
			return 0, err
		}
		switch {
		case msg.GetBatch() != nil:
			b := msg.GetBatch()
			msgs, derr := decodeBatch(dec, b)
			if derr != nil {
				return 0, derr
			}
			for _, m := range msgs {
				payloadBytes += uint64(len(decodePayload(dec, m)))
			}
			lastSeq = b.GetSeq()
			batchesSinceAck++
			if batchesSinceAck >= 64 {
				if err := sendAck(ctx, conn, subID, lastSeq); err != nil {
					return 0, err
				}
				batchesSinceAck = 0
			}
		case msg.GetProgress() != nil:
			// observed only
		case msg.GetEos() != nil:
			e := msg.GetEos()
			_ = sendAck(ctx, conn, subID, lastSeq)
			_ = conn.Close(websocket.StatusNormalClosure, "done")
			if e.GetReason() != pb.EosReason_EOS_REASON_COMPLETE {
				return 0, fmt.Errorf("stream ended %v (not COMPLETE)", e.GetReason())
			}
			return payloadBytes, nil
		case msg.GetError() != nil:
			return 0, fmt.Errorf("server error: %s (%s)", msg.GetError().GetMessage(), msg.GetError().GetCode())
		}
	}
}

func resolveFileIDOnConn(ctx context.Context, conn *websocket.Conn, key string) uint64 {
	resp, err := rpc(ctx, conn, &pb.ClientMessage{
		RequestId: 2,
		Payload:   &pb.ClientMessage_ListFiles{ListFiles: &pb.ListFilesRequest{Limit: 1000}},
	})
	if err != nil || resp.GetListFiles() == nil {
		return 0
	}
	for _, f := range resp.GetListFiles().GetFiles() {
		if f.GetS3Key() == key {
			return f.GetId()
		}
	}
	return 0
}

func decodeBatch(dec *zstd.Decoder, b *pb.MessageBatch) ([]*pb.Message, error) {
	switch b.GetBodyEncoding() {
	case pb.BodyEncoding_BODY_ENCODING_ZSTD:
		raw, err := dec.DecodeAll(b.GetBody(), nil)
		if err != nil {
			return nil, fmt.Errorf("batch %d zstd: %w", b.GetSeq(), err)
		}
		var body pb.MessageBatchBody
		if err := proto.Unmarshal(raw, &body); err != nil {
			return nil, fmt.Errorf("batch %d unmarshal: %w", b.GetSeq(), err)
		}
		return body.GetMessages(), nil
	case pb.BodyEncoding_BODY_ENCODING_NONE:
		return b.GetMessages(), nil
	default:
		return nil, fmt.Errorf("batch %d unknown body_encoding %v", b.GetSeq(), b.GetBodyEncoding())
	}
}

func decodePayload(dec *zstd.Decoder, m *pb.Message) []byte {
	if m.GetPayloadEncoding() == pb.PayloadEncoding_PAYLOAD_ENCODING_ZSTD {
		if raw, err := dec.DecodeAll(m.GetPayload(), nil); err == nil {
			return raw
		}
	}
	return m.GetPayload()
}

func sendAck(ctx context.Context, conn *websocket.Conn, subID, throughSeq uint64) error {
	return writeMsg(ctx, conn, &pb.ClientMessage{
		Payload: &pb.ClientMessage_Ack{Ack: &pb.SessionAck{SubscriptionId: subID, ThroughSeq: throughSeq}},
	})
}

func rpc(ctx context.Context, conn *websocket.Conn, msg *pb.ClientMessage) (*pb.ServerMessage, error) {
	if err := writeMsg(ctx, conn, msg); err != nil {
		return nil, err
	}
	return readMsg(ctx, conn)
}

func writeMsg(ctx context.Context, conn *websocket.Conn, msg *pb.ClientMessage) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageBinary, data)
}

func readMsg(ctx context.Context, conn *websocket.Conn) (*pb.ServerMessage, error) {
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, fmt.Errorf("expected binary frame, got %v", typ)
	}
	var m pb.ServerMessage
	if err := proto.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func loadBaseline() (baseline, error) {
	var b baseline
	raw, err := os.ReadFile(baselinePath())
	if err != nil {
		return b, err
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		return b, err
	}
	if b.MBPerSec <= 0 {
		return b, fmt.Errorf("baseline mb_per_sec must be > 0")
	}
	return b, nil
}

func writeBaseline(b baseline) error {
	raw, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(baselinePath(), append(raw, '\n'), 0o644)
}
