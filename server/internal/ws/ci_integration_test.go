//go:build ci_integration

// ci_integration_test.go — the {s3,gcs} CI integration leg, migrated (M6 §5.4,
// the catalog migration) from the retired in-process Go indexer.Scanner path to
// the PRODUCTION shape: the Python mcap_catalog builder (submodule
// mcap_catalog/) is invoked as a real subprocess (`--once`) against the seeded
// bucket, and the Go side only ever OPENS the resulting SQLite catalog
// READ-ONLY (catalog.OpenReadOnly) — exactly the external-builder mode
// cmd/pj-cloud-server/main.go runs in production. It boots the SAME in-process
// WS/session stack the component tests use (newTestServerFromCatalog: the real
// httptest WS handler + session.Registry, wired over that read-only Store and a
// REAL storage.BlobStore built by storage.New from the PJ_CLOUD_BACKEND env (s3
// -> Minio via S3Config.Endpoint; gcs -> fake-gcs via STORAGE_EMULATOR_HOST).
// The bucket is pre-seeded EXTERNALLY with a Hive-partitioned fixture layout
// (mc / the Go `seed` tool for Minio, the fake-gcs JSON upload API for GCS —
// see `make ci-integration` and .github/workflows/ci.yml) — the auryn builder
// ONLY catalogs Hive-partitioned keys (mcap_catalog_builder/keyparse.py); a flat
// key quarantines into catalog_failures instead (see scripts/ci-integration.sh's
// sabotage leg).
//
// ANTI-DRIFT (unified plan §7): the assertions are TABLE-DRIVEN with ZERO
// backend branching — s3 and gcs run the exact same checks. The matrix
// (.github/workflows/ci.yml) is fail-fast:false with both legs REQUIRED, so a
// gcs-only failure blocks exactly like an s3-only failure.
//
// Build tag `ci_integration` keeps this OUT of the default `go test ./...` (and
// thus out of the hermetic suite + smoke): it only compiles/runs when the leg is
// explicitly requested with `-tags=ci_integration` AND a seeded backend + a
// provisioned Python builder interpreter (PJ_CI_BUILDER_PYTHON) are up.
package ws

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/config"
	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/genmcap"
	"pj-cloud/server/internal/session"
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
// the locally-running emulator. This is the ONLY backend-aware code in the file
// besides builderEnv/bucketForBackend (which read the identical knobs); every
// download/catalog assertion below is backend-agnostic.
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
		// The emulator endpoint is auto-selected by the SDK (both Go's and the
		// Python builder's) from STORAGE_EMULATOR_HOST (set by the leg);
		// GCSConfig has no Endpoint field.
		if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
			t.Fatal("gcs leg requires STORAGE_EMULATOR_HOST (the fake-gcs emulator)")
		}
		return config.StorageConfig{GCS: &config.GCSConfig{Bucket: ciBucket()}}
	default:
		t.Fatalf("unknown PJ_CLOUD_BACKEND %q (want s3|gcs)", backend)
		return config.StorageConfig{}
	}
}

// bucketForBackend reads the bucket name back out of a ciStorageConfig result —
// the single place both storage.New and the Python builder's --s3-bucket/
// --gcs-bucket flag get the bucket name from, so they can never disagree.
func bucketForBackend(t *testing.T, cfg config.StorageConfig, backend string) string {
	t.Helper()
	switch backend {
	case "s3":
		return cfg.S3.Bucket
	case "gcs":
		return cfg.GCS.Bucket
	default:
		t.Fatalf("bucketForBackend: unknown backend %q", backend)
		return ""
	}
}

// builderEnv returns the extra environment the Python builder subprocess needs
// for this backend, ON TOP OF the test process's own os.Environ() (inherited by
// runBuilderOnce). s3: boto3 needs AWS_* env vars pointed at the Minio/emulator
// endpoint (mirroring scripts/smoke.sh's start_builder_daemon) — built from the
// SAME PJ_CLOUD_S3_* knobs ciStorageConfig just read, so the Go BlobStore and
// the Python builder can never point at different endpoints/creds. gcs: nothing
// extra — STORAGE_EMULATOR_HOST is already in this process's environment (set
// by the caller: the workflow / ci-integration.sh, for the gcs leg) and flows
// through os.Environ() unchanged; both the Go and Python GCS SDKs auto-detect it.
func builderEnv(t *testing.T, backend string, cfg config.StorageConfig) []string {
	t.Helper()
	switch backend {
	case "s3":
		return []string{
			"AWS_ACCESS_KEY_ID=" + cfg.S3.AccessKey,
			"AWS_SECRET_ACCESS_KEY=" + cfg.S3.SecretKey,
			"AWS_ENDPOINT_URL=" + cfg.S3.Endpoint,
			"AWS_REGION=" + cfg.S3.Region,
			"AWS_DEFAULT_REGION=" + cfg.S3.Region,
		}
	case "gcs":
		return nil
	default:
		t.Fatalf("builderEnv: unknown backend %q", backend)
		return nil
	}
}

// builderPython resolves the Python interpreter used to run the mcap_catalog
// builder subprocess, from PJ_CI_BUILDER_PYTHON — set by scripts/ci-integration.sh
// / .github/workflows/ci.yml to a venv with boto3, google-cloud-storage, mcap,
// and watchdog installed (a bare system `python3` lacks the cloud SDKs the s3/gcs
// Source backends import lazily). Skips (not fails) when unset: a missing local
// venv is an environment-provisioning gap, not a code bug — mirrors
// internal/catalog/crosslang_test.go's requireBuilder skip convention.
func builderPython(t *testing.T) string {
	t.Helper()
	py := os.Getenv("PJ_CI_BUILDER_PYTHON")
	if py == "" {
		t.Skip("PJ_CI_BUILDER_PYTHON not set (path to a python3 interpreter with " +
			"boto3, google-cloud-storage, mcap, and watchdog installed, e.g. a venv " +
			"bootstrapped per scripts/smoke.sh's start_builder_daemon comment) — " +
			"skipping the Python-builder CI integration leg")
	}
	if _, err := exec.LookPath(py); err != nil {
		if _, statErr := os.Stat(py); statErr != nil {
			t.Fatalf("PJ_CI_BUILDER_PYTHON=%q is not runnable: %v", py, statErr)
		}
	}
	return py
}

// mcapCatalogDir resolves the mcap_catalog submodule dir (the Python builder's
// package root) relative to this test file's package dir
// (server/internal/ws), mirroring internal/catalog/crosslang_test.go's
// requireBuilder. Skips (not fails) when the submodule isn't initialized.
func mcapCatalogDir(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd() // = the package dir (server/internal/ws)
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	dir, err := filepath.Abs(filepath.Join(wd, "..", "..", "..", "mcap_catalog"))
	if err != nil {
		t.Fatalf("abs: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "mcap_catalog_builder", "__main__.py")); err != nil {
		t.Skipf("mcap_catalog builder not present at %s (submodule not initialized?)", dir)
	}
	return dir
}

// runBuilderOnce invokes the Python builder in `--once` (single synchronous
// full-reconcile, then exit) mode against the given backend/bucket, writing (or,
// on a second call against the same dbPath, incrementally re-reconciling)
// dbPath. Fails the test loudly on a non-zero exit (a CI leg with a broken
// builder invocation is a real failure, never a skip). Returns combined
// stdout+stderr for the caller to log / grep (e.g. the reconcile tally line).
func runBuilderOnce(t *testing.T, python, mcapDir, backend, bucket, dbPath string, extraEnv []string) string {
	t.Helper()
	args := []string{"-m", "mcap_catalog_builder", "--source", backend}
	switch backend {
	case "s3":
		args = append(args, "--s3-bucket", bucket)
	case "gcs":
		args = append(args, "--gcs-bucket", bucket)
	default:
		t.Fatalf("runBuilderOnce: unknown backend %q", backend)
	}
	args = append(args, "--once", "--db", dbPath, "--log-level", "INFO")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, python, args...)
	cmd.Dir = mcapDir // so `python3 -m mcap_catalog_builder` resolves the package
	cmd.Env = append(os.Environ(), extraEnv...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: python builder --once failed: %v\n%s", backend, err, out)
	}
	return string(out)
}

// reconcileTallyRE matches reconcile.py's full_reconcile log line:
// "reconcile: cataloged=%d skipped=%d failed=%d deleted=%d".
var reconcileTallyRE = regexp.MustCompile(`reconcile: cataloged=(\d+) skipped=(\d+) failed=(\d+) deleted=(\d+)`)

// parseReconcileTally extracts the LAST reconcile tally line from a builder
// run's combined output (there is exactly one full_reconcile per --once run).
func parseReconcileTally(t *testing.T, out string) (cataloged, skipped, failed, deleted int) {
	t.Helper()
	matches := reconcileTallyRE.FindAllStringSubmatch(out, -1)
	if len(matches) == 0 {
		t.Fatalf("could not find a 'reconcile: cataloged=... skipped=... failed=... deleted=...' line in builder output:\n%s", out)
	}
	m := matches[len(matches)-1]
	atoi := func(s string) int {
		n := 0
		for _, c := range s {
			n = n*10 + int(c-'0')
		}
		return n
	}
	return atoi(m[1]), atoi(m[2]), atoi(m[3]), atoi(m[4])
}

// assertNoCatalogFailures fails the test if catalog_failures is non-empty,
// including the quarantined keys for diagnostics.
func assertNoCatalogFailures(t *testing.T, cat *catalog.Store, backend, when string) {
	t.Helper()
	rows, err := cat.DB().Query("SELECT s3_key, error_text FROM catalog_failures")
	if err != nil {
		t.Fatalf("%s: query catalog_failures (%s): %v", backend, when, err)
	}
	defer rows.Close()
	var failures []string
	for rows.Next() {
		var key, errText string
		if err := rows.Scan(&key, &errText); err != nil {
			t.Fatalf("%s: scan catalog_failures row (%s): %v", backend, when, err)
		}
		failures = append(failures, key+": "+errText)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: iterate catalog_failures (%s): %v", backend, when, err)
	}
	if len(failures) != 0 {
		t.Fatalf("%s: catalog_failures is non-empty after %s (%d entries): %v", backend, when, len(failures), failures)
	}
}

// snapshotFilesTable (N1) returns a deterministic, byte-comparable rendering of
// every row in `files` — the schema's authoritative record of what the builder
// catalogued, including cataloged_at_ns (bumped on every re-catalog, even one
// that lands the SAME logical values) and the path-derived dimension FKs (so an
// id/etag/size/mtime-identical but re-keyed row would also show up as a diff).
// There's no stored s3_key column in the M6 schema (it's rebuilt from
// customer/site/robot/source/date + filename — see catalog.rebuildHiveKey), so
// this snapshots the underlying dimension columns directly instead of trying to
// recompute the key. Comparing this string before/after the warm rerun is the
// structural, non-brittle counterpart to parseReconcileTally's log-line regex:
// it proves directly, from the DB, that NOTHING changed — not just that a log
// line claimed zero re-catalogs.
func snapshotFilesTable(t *testing.T, cat *catalog.Store, backend string) string {
	t.Helper()
	rows, err := cat.DB().Query(
		`SELECT id, filename, etag, size_bytes, last_modified_ns, cataloged_at_ns,
		        customer_id, site_id, robot_id, source_id, date
		 FROM files ORDER BY id`)
	if err != nil {
		t.Fatalf("%s: snapshotFilesTable query: %v", backend, err)
	}
	defer rows.Close()
	var sb strings.Builder
	for rows.Next() {
		var (
			id                                       int64
			filename, etag, date                     string
			sizeBytes, lastModifiedNs, catalogedAtNs int64
			customerID, siteID, robotID, sourceID    int64
		)
		if err := rows.Scan(&id, &filename, &etag, &sizeBytes, &lastModifiedNs, &catalogedAtNs,
			&customerID, &siteID, &robotID, &sourceID, &date); err != nil {
			t.Fatalf("%s: snapshotFilesTable scan: %v", backend, err)
		}
		fmt.Fprintf(&sb, "%d|%s|%s|%d|%d|%d|%d|%d|%d|%d|%s\n",
			id, filename, etag, sizeBytes, lastModifiedNs, catalogedAtNs,
			customerID, siteID, robotID, sourceID, date)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("%s: snapshotFilesTable iterate: %v", backend, err)
	}
	return sb.String()
}

// newTestServerFromCatalog is newTestServerWithBlob (handlers_session_test.go),
// but over an ALREADY-BUILT read-only catalog.Store (opened via
// catalog.OpenReadOnly against a DB the Python mcap_catalog builder wrote)
// instead of running the Go indexer.Scanner to populate one — the M6
// external-builder production shape: the Python builder is the sole catalog
// writer; the Go server (and this harness) only ever reads it.
func newTestServerFromCatalog(t *testing.T, cat *catalog.Store, blob storage.BlobStore, cfg config.SessionConfig) *testServer {
	t.Helper()
	codec, err := format.NewCodec("mcap")
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	idxCache := format.NewChunkIndexCache(1024)

	reg := session.NewRegistry(session.RegistryOpts{
		MaxConcurrent:         cfg.MaxConcurrent,
		RetainAfterDisconnect: cfg.RetainAfterDisconnect,
	})
	deps := &SessionDeps{Store: cat, Codec: codec, Blob: blob, Registry: reg, Cfg: cfg, Log: log, IdxCache: idxCache}
	h := NewHandlerWithSession(cat, "", log, deps)

	mux := http.NewServeMux()
	mux.Handle("/api/ws", h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return &testServer{
		httptest: srv,
		url:      "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/ws",
		cat:      cat,
		reg:      reg,
		store:    blob,
		idxCache: idxCache,
	}
}

// TestCIIntegration_Backend is the single entrypoint. PJ_CLOUD_BACKEND pins the
// leg (the workflow's matrix dimension); unset runs BOTH legs in-process if both
// emulators are reachable (the local `make ci-integration` path sets it per
// leg). Each leg is a t.Run subtest (46a's t.Run(backend) intent).
func TestCIIntegration_Backend(t *testing.T) {
	python := builderPython(t)
	mcapDir := mcapCatalogDir(t)

	backends := []string{}
	if b := os.Getenv("PJ_CLOUD_BACKEND"); b != "" {
		backends = []string{b}
	} else {
		backends = []string{"s3", "gcs"}
	}
	for _, backend := range backends {
		backend := backend
		t.Run(backend, func(t *testing.T) {
			runCILeg(t, backend, python, mcapDir)
		})
	}
}

// runCILeg is the backend-AGNOSTIC body: build the real BlobStore, have the
// Python builder catalog the (Hive-keyed) seeded bucket into a fresh SQLite DB
// (`--once`), open that DB read-only, boot the in-process WS/session stack over
// it, then assert catalog + download round-trips against the KNOWN synthetic-
// fixture counts — and finally that a second `--once` run against the
// UNCHANGED bucket is an all-skip (etag) reconcile: zero re-catalogs.
func runCILeg(t *testing.T, backend, python, mcapDir string) {
	cfg := ciStorageConfig(t, backend)
	blob, err := storage.New(context.Background(), cfg)
	if err != nil {
		t.Fatalf("storage.New(%s): %v", backend, err)
	}

	// Sanity: the seed step must have uploaded the Hive-keyed fixtures. Fail
	// loudly (not skip) if the bucket is empty — a CI leg with no data is a seed
	// bug, and the matrix REQUIRES both legs to actually run.
	objs, _, err := blob.List(context.Background(), "", "")
	if err != nil {
		t.Fatalf("%s List (is the emulator up + bucket seeded?): %v", backend, err)
	}
	specs := genmcap.DefaultSpecs()
	if len(objs) < len(specs) {
		t.Fatalf("%s bucket under-seeded: listed %d objects, want >= %d synthetic Hive fixtures (seed step failed?)",
			backend, len(objs), len(specs))
	}

	// flatToHive[spec.Key] is the SAME genmcap.HiveKeyFor mapping the seed step
	// used (gen-ci-fixtures -hive) — the single source of truth for the mapping,
	// so the key names used here to look files up in the catalog can never drift
	// from the ones actually uploaded.
	flatToHive := make(map[string]string, len(specs))
	for i, s := range specs {
		flatToHive[s.Key] = genmcap.HiveKeyFor(s, i)
	}

	// ── build the catalog with the Python builder (`--once`), NOT the retired Go
	// indexer.Scanner — the M6 external-builder production shape. ──
	dbPath := filepath.Join(t.TempDir(), "catalog.db")
	bucket := bucketForBackend(t, cfg, backend)
	env := builderEnv(t, backend, cfg)
	coldOut := runBuilderOnce(t, python, mcapDir, backend, bucket, dbPath, env)
	t.Logf("%s: builder --once (cold build) output:\n%s", backend, coldOut)

	cat, err := catalog.OpenReadOnly(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("%s: catalog.OpenReadOnly: %v", backend, err)
	}
	t.Cleanup(func() { _ = cat.Close() })

	// ── assertion 0: catalog_failures is EMPTY — every seeded fixture is a
	// valid Hive key (gen-ci-fixtures -hive), so nothing should quarantine. ──
	assertNoCatalogFailures(t, cat, backend, "the cold build")

	// ── assertion 1: cold-build catalog counts == known fixtures, looked up by
	// their REBUILT Hive s3_key (not the flat spec.Key) ───────────────────────
	totalFixtureMsgs := uint64(0)
	for _, s := range specs {
		totalFixtureMsgs += s.TotalMessages()
	}
	ts := newTestServerFromCatalog(t, cat, blob, defaultTestSessionCfg())
	c := dialClient(t, ts.url)
	c.hello()

	files := ciListFiles(t, c)
	byKey := map[string]*pb.FileSummary{}
	for _, f := range files {
		byKey[f.GetS3Key()] = f
	}
	for _, s := range specs {
		key := flatToHive[s.Key]
		f, ok := byKey[key]
		if !ok {
			t.Fatalf("%s: fixture %q (hive key %q) missing from catalog (cold build failed?)", backend, s.Key, key)
		}
		if f.GetMessageCount() != s.TotalMessages() {
			t.Errorf("%s: %q message_count: got %d want %d", backend, key, f.GetMessageCount(), s.TotalMessages())
		}
		if f.GetRecorded().GetStartNs() != s.StartNs {
			t.Errorf("%s: %q start_ns: got %d want %d", backend, key, f.GetRecorded().GetStartNs(), s.StartNs)
		}
		if f.GetRecorded().GetEndNs() != s.EndNs() {
			t.Errorf("%s: %q end_ns: got %d want %d", backend, key, f.GetRecorded().GetEndNs(), s.EndNs())
		}
	}

	// ── assertion 2: full single-file download round-trip (exact counts) ─────
	specA := specs[0]
	idA := byKey[flatToHive[specA.Key]].GetId()
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
	// All fixtures are time-disjoint, so the stitch is the sum.
	allIDs := make([]uint64, 0, len(specs))
	wantStitch := 0
	for _, s := range specs {
		allIDs = append(allIDs, byKey[flatToHive[s.Key]].GetId())
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
		f, ok := byKey[flatToHive[key]]
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

	// ── assertion 6: rerunning the builder `--once` against the UNCHANGED bucket
	// is a ZERO-RE-CATALOG (etag-skip) reconcile — the change-detect / warm-start
	// contract, now proven against the PYTHON builder instead of the retired Go
	// indexer.Scanner. The rerun is an IN-PLACE reconcile (args.db already
	// exists, no --rebuild), so files.id is preserved (CATALOG_CONTRACT.md §7)
	// and the already-open read-only `cat` connection observes the new commit
	// via WAL on its next query — no reopen dance needed (that's only required
	// across a --rebuild's atomic-publish, which this is not). ──
	//
	// N1: the load-bearing check here is the STRUCTURAL snapshot of the `files`
	// table (byte-identical before/after, including cataloged_at_ns) — not the
	// log-line regex below, which is kept only as a secondary, human-readable
	// signal (and the only source for the "skipped >= len(specs)" reconcile-tally
	// check, which the row snapshot alone can't express).
	beforeSnapshot := snapshotFilesTable(t, cat, backend)

	warmOut := runBuilderOnce(t, python, mcapDir, backend, bucket, dbPath, env)
	t.Logf("%s: builder --once (warm rerun) output:\n%s", backend, warmOut)
	cataloged, skipped, failed, _ := parseReconcileTally(t, warmOut)
	if cataloged != 0 || failed != 0 {
		t.Errorf("%s: warm rerun re-catalogued/failed (cataloged=%d failed=%d), want 0/0 — change-detect broken", backend, cataloged, failed)
	}
	if skipped < len(specs) {
		t.Errorf("%s: warm rerun skipped=%d, want >= %d (our fixtures, all etag-unchanged)", backend, skipped, len(specs))
	}
	assertNoCatalogFailures(t, cat, backend, "the warm rerun")

	afterSnapshot := snapshotFilesTable(t, cat, backend)
	if beforeSnapshot != afterSnapshot {
		t.Errorf("%s: files table changed across the warm (etag-unchanged) rerun — want byte-identical rows"+
			" (id/etag/size/mtime/dims/cataloged_at_ns), a real re-catalog occurred:\n--- before ---\n%s\n--- after ---\n%s",
			backend, beforeSnapshot, afterSnapshot)
	}

	bi, err := catalog.GetBuildInfo(context.Background(), cat)
	if err != nil {
		t.Fatalf("%s: GetBuildInfo (post-warm-rerun): %v", backend, err)
	}
	if !bi.Present || bi.BuildID < 2 || bi.Outcome != "ok" {
		t.Errorf("%s: build_metadata after warm rerun = %+v, want present/build_id>=2/outcome=ok", backend, bi)
	}

	t.Logf("%s leg OK: %d fixtures, %d total msgs, full+subset+window+stitch round-trips clean, warm rerun 0 re-catalogs",
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
