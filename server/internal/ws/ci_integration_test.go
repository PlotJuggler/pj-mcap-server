//go:build ci_integration

// ci_integration_test.go — the Plan A Task 46 / 46a {s3,gcs} CI integration leg,
// adapted to the AS-BULT layout (the shell smoke/matrix gates can't run in CI:
// they hard-require the on-disk ground-truth corpus + the C++ plugin build).
//
// It boots the SAME in-process server stack the component tests use
// (newTestServerWithBlob: real SQLite catalog + real indexer.Scanner + real
// session.Registry + the WS handler on an httptest server), but injects a REAL
// storage.BlobStore built by storage.New from the PJ_CLOUD_BACKEND env (s3 ->
// Minio via S3Config.Endpoint; gcs -> fake-gcs via STORAGE_EMULATOR_HOST). The
// bucket is pre-seeded EXTERNALLY (mc for Minio, the fake-gcs JSON upload API for
// GCS — see `make ci-integration` and .github/workflows/ci.yml) with the
// deterministic synthetic fixtures from internal/genmcap, whose KNOWN counts /
// time-ranges this test asserts against.
//
// ANTI-DRIFT (unified plan §7): the assertions are TABLE-DRIVEN with ZERO
// backend branching — s3 and gcs run the exact same checks. The matrix
// (.github/workflows/ci.yml) is fail-fast:false with both legs REQUIRED, so a
// gcs-only failure blocks exactly like an s3-only failure.
//
// Build tag `ci_integration` keeps this OUT of the default `go test ./...` (and
// thus out of the hermetic suite + smoke): it only compiles/runs when the leg is
// explicitly requested with `-tags=ci_integration` AND a seeded backend is up.
package ws

import (
	"context"
	"io"
	"log/slog"
	"os"
	"sort"
	"testing"
	"time"

	"pj-cloud/server/internal/config"
	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/genmcap"
	"pj-cloud/server/internal/indexer"
	"pj-cloud/server/internal/storage"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// ciBucket is the bucket the seed step creates on BOTH emulators. Overridable via
// PJ_CLOUD_CI_BUCKET for parallel/isolated runs.
func ciBucket() string {
	if b := os.Getenv("PJ_CLOUD_CI_BUCKET"); b != "" {
		return b
	}
	return "ci-fixtures"
}

// ciStorageConfig builds the StorageConfig for the requested backend, pointing at
// the locally-running emulator. This is the ONLY backend-aware code in the file;
// every assertion below is backend-agnostic.
func ciStorageConfig(t *testing.T, backend string) config.StorageConfig {
	t.Helper()
	switch backend {
	case "s3":
		endpoint := os.Getenv("PJ_CLOUD_S3_ENDPOINT")
		if endpoint == "" {
			endpoint = "http://localhost:9000"
		}
		access := os.Getenv("PJ_CLOUD_S3_ACCESS_KEY")
		if access == "" {
			access = "admin"
		}
		secret := os.Getenv("PJ_CLOUD_S3_SECRET_KEY")
		if secret == "" {
			secret = "password123"
		}
		return config.StorageConfig{S3: &config.S3Config{
			Bucket:    ciBucket(),
			Region:    "us-east-1",
			Endpoint:  endpoint,
			AccessKey: access,
			SecretKey: secret,
		}}
	case "gcs":
		// The emulator endpoint is auto-selected by the SDK from
		// STORAGE_EMULATOR_HOST (set by the leg); GCSConfig has no Endpoint field.
		if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
			t.Fatal("gcs leg requires STORAGE_EMULATOR_HOST (the fake-gcs emulator)")
		}
		return config.StorageConfig{GCS: &config.GCSConfig{Bucket: ciBucket()}}
	default:
		t.Fatalf("unknown PJ_CLOUD_BACKEND %q (want s3|gcs)", backend)
		return config.StorageConfig{}
	}
}

// TestCIIntegration_Backend is the single entrypoint. PJ_CLOUD_BACKEND pins the
// leg (the workflow's matrix dimension); unset runs BOTH legs in-process if both
// emulators are reachable (the local `make ci-integration` path sets it per
// leg). Each leg is a t.Run subtest (46a's t.Run(backend) intent).
func TestCIIntegration_Backend(t *testing.T) {
	backends := []string{}
	if b := os.Getenv("PJ_CLOUD_BACKEND"); b != "" {
		backends = []string{b}
	} else {
		backends = []string{"s3", "gcs"}
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			runCILeg(t, backend)
		})
	}
}

// runCILeg is the backend-AGNOSTIC body: build the real BlobStore, boot the
// in-process server over it (cold indexer extract), then assert catalog +
// download round-trips against the KNOWN synthetic-fixture counts.
func runCILeg(t *testing.T, backend string) {
	cfg := ciStorageConfig(t, backend)
	blob, err := storage.New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("storage.New(%s): %v", backend, err)
	}

	// Sanity: the seed step must have uploaded the fixtures. Fail loudly (not
	// skip) if the bucket is empty — a CI leg with no data is a seed bug, and the
	// matrix REQUIRES both legs to actually run.
	objs, _, err := blob.List(context.Background(), "", "")
	if err != nil {
		t.Fatalf("%s List (is the emulator up + bucket seeded?): %v", backend, err)
	}
	specs := genmcap.DefaultSpecs()
	if len(objs) < len(specs) {
		t.Fatalf("%s bucket under-seeded: listed %d objects, want >= %d synthetic fixtures (seed step failed?)",
			backend, len(objs), len(specs))
	}

	// Boot the full in-process stack over the REAL backend. newTestServerWithBlob
	// runs the indexer Scanner ONCE (cold extract) during construction.
	ts := newTestServerWithBlob(t, blob, defaultTestSessionCfg())

	// ── assertion 1: cold-extract catalog counts == known fixtures ───────────
	totalFixtureMsgs := uint64(0)
	for _, s := range specs {
		totalFixtureMsgs += s.TotalMessages()
	}
	c := dialClient(t, ts.url)
	c.hello()

	files := ciListFiles(t, c)
	// The bucket may legitimately contain ONLY our fixtures (fresh CI bucket); if
	// a shared bucket has extra .mcap objects the indexer would catalog them too,
	// so assert our fixtures are PRESENT with exact counts rather than ==len.
	byKey := map[string]*pb.FileSummary{}
	for _, f := range files {
		byKey[f.GetS3Key()] = f
	}
	for _, s := range specs {
		f, ok := byKey[s.Key]
		if !ok {
			t.Fatalf("%s: fixture %q missing from catalog (cold extract failed?)", backend, s.Key)
		}
		if f.GetMessageCount() != s.TotalMessages() {
			t.Errorf("%s: %q message_count: got %d want %d", backend, s.Key, f.GetMessageCount(), s.TotalMessages())
		}
		if f.GetRecorded().GetStartNs() != s.StartNs {
			t.Errorf("%s: %q start_ns: got %d want %d", backend, s.Key, f.GetRecorded().GetStartNs(), s.StartNs)
		}
		if f.GetRecorded().GetEndNs() != s.EndNs() {
			t.Errorf("%s: %q end_ns: got %d want %d", backend, s.Key, f.GetRecorded().GetEndNs(), s.EndNs())
		}
	}

	// ── assertion 2: full single-file download round-trip (exact counts) ─────
	specA := specs[0]
	idA := byKey[specA.Key].GetId()
	gotA := ciDownload(t, ts, &pb.OpenFresh{FileIds: []uint64{idA}})
	if gotA.total != int(specA.TotalMessages()) {
		t.Errorf("%s: full download %q: got %d msgs want %d", backend, specA.Key, gotA.total, specA.TotalMessages())
	}
	if gotA.eosReason != pb.EosReason_EOS_REASON_COMPLETE {
		t.Errorf("%s: full download eos reason: got %v want COMPLETE", backend, gotA.eosReason)
	}
	if gotA.seqGaps {
		t.Errorf("%s: full download had seq gaps", backend)
	}

	// ── assertion 3: topic-subset download (exact per-topic count, no over-deliver)
	subTopic := specA.Topics[0].Topic // /clock
	subWant := specA.Topics[0].MessageCount
	gotSub := ciDownload(t, ts, &pb.OpenFresh{FileIds: []uint64{idA}, TopicNames: []string{subTopic}})
	if gotSub.total != subWant {
		t.Errorf("%s: subset %q@%q: got %d msgs want %d (over/under-delivery)", backend, specA.Key, subTopic, gotSub.total, subWant)
	}
	if gotSub.perTopic[subTopic] != subWant {
		t.Errorf("%s: subset per-topic %q: got %d want %d", backend, subTopic, gotSub.perTopic[subTopic], subWant)
	}

	// ── assertion 4: time-window download (first half of file by log-time) ───
	// Window covers steps [0, half) inclusive of the boundary message. Count the
	// expected messages by walking the spec deterministically.
	half := specA.StartNs + (specA.EndNs()-specA.StartNs)/2
	wantWindow := countInWindow(specA, specA.StartNs, half)
	gotWin := ciDownload(t, ts, &pb.OpenFresh{
		FileIds:   []uint64{idA},
		TimeRange: &pb.TimeRange{StartNs: specA.StartNs, EndNs: half},
	})
	if gotWin.total != wantWindow {
		t.Errorf("%s: time-window [%d,%d] on %q: got %d msgs want %d", backend, specA.StartNs, half, specA.Key, gotWin.total, wantWindow)
	}

	// ── assertion 5: stitched multi-file download (union, monotonic, exact) ──
	// All three fixtures are time-disjoint, so the stitch is the sum.
	allIDs := make([]uint64, 0, len(specs))
	wantStitch := 0
	for _, s := range specs {
		allIDs = append(allIDs, byKey[s.Key].GetId())
		wantStitch += int(s.TotalMessages())
	}
	gotStitch := ciDownload(t, ts, &pb.OpenFresh{FileIds: allIDs})
	if gotStitch.total != wantStitch {
		t.Errorf("%s: stitched download: got %d msgs want %d", backend, gotStitch.total, wantStitch)
	}
	if gotStitch.seqGaps {
		t.Errorf("%s: stitched download had seq gaps / non-monotonic", backend)
	}
	if gotStitch.eosReason != pb.EosReason_EOS_REASON_COMPLETE {
		t.Errorf("%s: stitched eos reason: got %v want COMPLETE", backend, gotStitch.eosReason)
	}

	// ── assertion 5b: C2 fixture-dimension coverage (uncompressed + tiny + lz4) ─
	// The DefaultSpecs matrix pins an UNCOMPRESSED chunk-container fixture, a TINY
	// single-chunk fixture, and an LZ4-FRAME fixture (genmcap.Compression /
	// ChunkSize knobs). Assert each downloads to its EXACT message total so the
	// codec's no-compression decode path, the small-file edge, AND the LZ4-frame
	// decode path (chunks.go lz4.NewReader) are exercised end-to-end on both legs,
	// not just incidentally summed into the stitch.
	for _, key := range []string{"ci_synth_d_none.mcap", "ci_synth_e_tiny.mcap", "ci_synth_f_lz4.mcap"} {
		f, ok := byKey[key]
		if !ok {
			t.Errorf("%s: C2 dimension fixture %q missing from catalog", backend, key)
			continue
		}
		var want int
		for _, s := range specs {
			if s.Key == key {
				want = int(s.TotalMessages())
			}
		}
		got := ciDownload(t, ts, &pb.OpenFresh{FileIds: []uint64{f.GetId()}})
		if got.total != want {
			t.Errorf("%s: dimension %q download: got %d msgs want %d", backend, key, got.total, want)
		}
		if got.eosReason != pb.EosReason_EOS_REASON_COMPLETE {
			t.Errorf("%s: dimension %q eos reason: got %v want COMPLETE", backend, key, got.eosReason)
		}
	}

	// ── assertion 6: warm-start == 0 re-extracts ─────────────────────────────
	// A SECOND scanner pass over the SAME catalog + SAME (byte-identical) fixtures
	// must skip everything (unchanged) — the change-detect / warm-start contract.
	warm := ciRescan(t, ts, blob)
	if warm.NewFiles != 0 || warm.Reindexed != 0 {
		t.Errorf("%s: warm-start re-extracted (new=%d reindexed=%d), want 0/0 — change-detect broken",
			backend, warm.NewFiles, warm.Reindexed)
	}
	if warm.Unchanged < len(specs) {
		t.Errorf("%s: warm-start unchanged=%d, want >= %d (our fixtures)", backend, warm.Unchanged, len(specs))
	}

	t.Logf("%s leg OK: %d fixtures, %d total msgs, full+subset+window+stitch round-trips clean, warm-start 0 re-extracts",
		backend, len(specs), totalFixtureMsgs)
}

// countInWindow counts spec messages whose log_time is within [start, end]
// inclusive, by replaying the deterministic emission. Mirrors the server's
// inclusive time-range filter so the expected count is computed identically.
func countInWindow(spec genmcap.FileSpec, start, end int64) int {
	n := 0
	for _, tp := range spec.Topics {
		for i := 0; i < tp.MessageCount; i++ {
			lt := spec.StartNs + int64(i)*spec.StepNs
			if lt >= start && lt <= end {
				n++
			}
		}
	}
	return n
}

// ── small helpers built on the package-private component-test client ─────────

func ciListFiles(t *testing.T, c *wsClient) []*pb.FileSummary {
	t.Helper()
	c.send(&pb.ClientMessage{RequestId: 2, Payload: &pb.ClientMessage_ListFiles{ListFiles: &pb.ListFilesRequest{Limit: 1000}}})
	resp := c.recv()
	lf := resp.GetListFiles()
	if lf == nil {
		t.Fatalf("expected ListFilesResponse, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}
	files := lf.GetFiles()
	sort.Slice(files, func(i, j int) bool { return files[i].GetS3Key() < files[j].GetS3Key() })
	return files
}

type ciDownloadResult struct {
	total     int
	perTopic  map[string]int
	seqGaps   bool
	eosReason pb.EosReason
}

func ciDownload(t *testing.T, ts *testServer, fresh *pb.OpenFresh) ciDownloadResult {
	t.Helper()
	c := dialClient(t, ts.url)
	c.hello()
	c.send(&pb.ClientMessage{RequestId: 10, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{Fresh: fresh}},
	}})
	open := c.recv()
	or := open.GetOpenSession()
	if or == nil {
		t.Fatalf("expected OpenSessionResponse, got %T (err=%v)", open.GetPayload(), open.GetError())
	}
	res := &collectResult{perTopic: map[string]int{}, openResp: or}
	c.streamToEnd(res, or.GetSubscriptionId(), 64)
	if res.errFrame != nil {
		t.Fatalf("unexpected Error frame: %v", res.errFrame)
	}
	reason := pb.EosReason_EOS_REASON_COMPLETE
	if res.eos != nil {
		reason = res.eos.GetReason()
	}
	return ciDownloadResult{
		total:     res.total,
		perTopic:  res.perTopic,
		seqGaps:   res.seqGaps,
		eosReason: reason,
	}
}

// ciRescan runs a SECOND indexer pass against the same catalog + blob store and
// returns the run stats — for the warm-start (0 re-extract) assertion. It builds
// a fresh Scanner over ts.cat (the catalog the server already populated) so the
// change-detect fast-path is exercised exactly as the production Loop's ticker
// would on its next tick.
func ciRescan(t *testing.T, ts *testServer, blob storage.BlobStore) rescanStats {
	t.Helper()
	codec, err := format.NewCodec("mcap")
	if err != nil {
		t.Fatal(err)
	}
	scanner := &indexer.Scanner{
		Store:     ts.cat,
		Lister:    indexer.NewBlobStoreLister(blob),
		Extractor: indexer.NewCodecExtractor(blob, codec, nil),
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	stats, err := scanner.RunOnce(ctx)
	if err != nil {
		t.Fatalf("warm rescan: %v", err)
	}
	return rescanStats{NewFiles: stats.NewFiles, Reindexed: stats.Reindexed, Unchanged: stats.Unchanged}
}

type rescanStats struct {
	NewFiles  int
	Reindexed int
	Unchanged int
}
