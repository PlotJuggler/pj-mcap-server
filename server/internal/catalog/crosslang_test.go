//go:build crosslang

// Package catalog cross-language proof (catalog-migration plan §1.0/§2.1/§5.1, the
// M1 end-to-end gate): the Python mcap_catalog builder writes a catalog DB from
// Hive-keyed fixtures, and the Go read-only reader opens it, passes the
// schema_version compat check, and reads the dimensions/files back. This is THE
// cross-language contract proof — it must hold before the heavier query rewrite.
//
// Build-tagged `crosslang` so the default (hermetic) `go test ./...` never depends
// on Python. Run it with:
//
//	go test -tags crosslang ./internal/catalog/ -run TestCrossLang
//
// It also runtime-skips when python3 / the builder module / its pip deps are
// absent, so an accidental invocation in a bare container is a skip, not a failure.
package catalog

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"pj-cloud/server/internal/genmcap"
)

// hiveFixture lays a spec out under a Hive-partitioned relative key, mirroring
// gen-ci-fixtures -hive (kept local so the proof is self-contained).
type hiveFixture struct {
	spec genmcap.FileSpec
	key  string // Hive relative path
}

func TestCrossLang_PythonBuilds_GoReads(t *testing.T) {
	mcapCatalogDir := requireBuilder(t)

	// 1. Write 3 deterministic MCAPs under a strict Hive tree (2 robots, 2 dates).
	root := t.TempDir()
	specs := genmcap.DefaultSpecs()
	if len(specs) < 3 {
		t.Fatalf("need >=3 default specs, have %d", len(specs))
	}
	fixtures := []hiveFixture{
		{specs[0], "customer=test/customer_site=lab/robot=r1/source=synthetic/date=2026-06-22/" + specs[0].Key},
		{specs[1], "customer=test/customer_site=lab/robot=r2/source=synthetic/date=2026-06-23/" + specs[1].Key},
		{specs[2], "customer=acme/customer_site=hq/robot=r9/source=synthetic/date=2026-06-22/" + specs[2].Key},
	}
	for _, fx := range fixtures {
		p := filepath.Join(root, filepath.FromSlash(fx.key))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		f, err := os.Create(p)
		if err != nil {
			t.Fatalf("create %s: %v", p, err)
		}
		if err := genmcap.Write(f, fx.spec); err != nil {
			_ = f.Close()
			t.Fatalf("write fixture %s: %v", p, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close %s: %v", p, err)
		}
	}

	// 2. Python builder writes the catalog (one-shot, the --once primitive).
	db := filepath.Join(t.TempDir(), "catalog.db")
	cmd := exec.Command("python3", "-m", "mcap_catalog_builder", "--once", root, "--db", db)
	cmd.Dir = mcapCatalogDir // so `python3 -m mcap_catalog_builder` resolves the package
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("python builder --once failed: %v\n%s", err, out)
	}

	// 3. Go reads it read-only — this exercises OpenReadOnly + the schema_version
	//    compat check (a wrong-version DB would fail here).
	st, err := OpenReadOnly(context.Background(), db)
	if err != nil {
		t.Fatalf("OpenReadOnly on the Python-built DB: %v", err)
	}
	defer st.Close()

	// 4. The contract: file rows and the dimension hierarchy are readable from Go.
	var nFiles int
	if err := st.DB().QueryRow("SELECT COUNT(*) FROM files").Scan(&nFiles); err != nil {
		t.Fatalf("count files: %v", err)
	}
	if nFiles != len(fixtures) {
		t.Fatalf("files = %d, want %d", nFiles, len(fixtures))
	}

	var nCustomers, nSites, nRobots, nSources int
	row := st.DB().QueryRow(`SELECT
		(SELECT COUNT(*) FROM customers),
		(SELECT COUNT(*) FROM sites),
		(SELECT COUNT(*) FROM robots),
		(SELECT COUNT(*) FROM sources)`)
	if err := row.Scan(&nCustomers, &nSites, &nRobots, &nSources); err != nil {
		t.Fatalf("count dimensions: %v", err)
	}
	// The Hive tree above encodes 2 customers, 2 sites, 3 robots, 1 source.
	if nCustomers != 2 || nSites != 2 || nRobots != 3 || nSources != 1 {
		t.Fatalf("dimensions = (customers=%d sites=%d robots=%d sources=%d), want (2,2,3,1)",
			nCustomers, nSites, nRobots, nSources)
	}

	// 5. A joined read returns the dimensions the keys encoded — proving Go reads
	//    the same shape Python wrote.
	var customer, site, robot, source, filename string
	q := `SELECT c.name, s.name, r.name, src.name, f.filename
	      FROM files f
	      JOIN customers c ON c.id = f.customer_id
	      JOIN sites s     ON s.id = f.site_id
	      JOIN robots r    ON r.id = f.robot_id
	      JOIN sources src ON src.id = f.source_id
	      WHERE f.filename = ?`
	if err := st.DB().QueryRow(q, specs[2].Key).Scan(&customer, &site, &robot, &source, &filename); err != nil {
		t.Fatalf("joined read: %v", err)
	}
	if customer != "acme" || site != "hq" || robot != "r9" || source != "synthetic" {
		t.Fatalf("joined dims = (%s/%s/%s/%s), want (acme/hq/r9/synthetic)",
			customer, site, robot, source)
	}

	// 6. The AURYN READER (FilterFiles / GetFile / ListTopicsForFile over the
	//    OpenReadOnly store) produces correct wire data — the M2 query rewrite.
	assertAurynReader(t, st, fixtures)
}

// assertAurynReader drives the read-only reader functions (which branch to the
// auryn-schema queries) and checks they reproduce each fixture's wire facts:
// rebuilt s3_key, summed message_count, topic_count/topics, etag, and a working
// topics-any-of filter.
func assertAurynReader(t *testing.T, st *Store, fixtures []hiveFixture) {
	t.Helper()
	ctx := context.Background()

	// FilterFiles (no predicates) returns all fixtures with derived facts.
	files, next, err := FilterFiles(ctx, st, FilterArgs{})
	if err != nil {
		t.Fatalf("FilterFiles: %v", err)
	}
	if next != "" {
		t.Fatalf("unexpected next page token %q (only %d files)", next, len(fixtures))
	}
	if len(files) != len(fixtures) {
		t.Fatalf("FilterFiles returned %d, want %d", len(files), len(fixtures))
	}
	byKey := map[string]FileSummary{}
	for _, f := range files {
		byKey[f.S3Key] = f
	}
	for _, fx := range fixtures {
		f, ok := byKey[fx.key] // s3_key was rebuilt from dimensions == the Hive key
		if !ok {
			t.Fatalf("FilterFiles missing rebuilt s3_key %q; got keys %v", fx.key, keysOf(byKey))
		}
		if f.MessageCount != fx.spec.TotalMessages() {
			t.Fatalf("%s message_count = %d, want %d (summed from topic_counts)",
				fx.key, f.MessageCount, fx.spec.TotalMessages())
		}
		if int(f.TopicCount) != len(fx.spec.Topics) {
			t.Fatalf("%s topic_count = %d, want %d", fx.key, f.TopicCount, len(fx.spec.Topics))
		}
		// chunk_count crosses the language boundary: the Python builder wrote it
		// from MCAP Statistics.chunk_count; the fixtures are chunked+summarized, so
		// the Go reader must see a non-zero count.
		if f.ChunkCount == 0 {
			t.Fatalf("%s chunk_count = 0, want > 0 (chunked fixture)", fx.key)
		}

		// GetFile round-trips s3_key + etag + counts.
		rec, err := GetFile(ctx, st, f.ID)
		if err != nil {
			t.Fatalf("GetFile(%d): %v", f.ID, err)
		}
		if rec.S3Key != fx.key {
			t.Fatalf("GetFile s3_key = %q, want %q", rec.S3Key, fx.key)
		}
		if rec.S3ETag == "" {
			t.Fatalf("GetFile %s: empty etag", fx.key)
		}
		if rec.MessageCount != fx.spec.TotalMessages() || rec.StartTimeNs != fx.spec.StartNs {
			t.Fatalf("GetFile %s: msgs=%d start=%d, want msgs=%d start=%d",
				fx.key, rec.MessageCount, rec.StartTimeNs, fx.spec.TotalMessages(), fx.spec.StartNs)
		}

		// ListTopicsForFile reconstructs the topic set + per-topic counts.
		topics, err := ListTopicsForFile(ctx, st, f.ID)
		if err != nil {
			t.Fatalf("ListTopicsForFile(%d): %v", f.ID, err)
		}
		if len(topics) != len(fx.spec.Topics) {
			t.Fatalf("%s: %d topics, want %d", fx.key, len(topics), len(fx.spec.Topics))
		}
		wantCounts := map[string]uint64{}
		for _, ts := range fx.spec.Topics {
			wantCounts[ts.Topic] = uint64(ts.MessageCount)
		}
		var summed uint64
		for _, tr := range topics {
			if wc, ok := wantCounts[tr.Name]; !ok || wc != tr.MessageCount {
				t.Fatalf("%s topic %q count=%d, want %d (present=%v)",
					fx.key, tr.Name, tr.MessageCount, wc, ok)
			}
			summed += tr.MessageCount
		}
		if summed != fx.spec.TotalMessages() {
			t.Fatalf("%s: topic counts sum to %d, want %d", fx.key, summed, fx.spec.TotalMessages())
		}
	}

	// A topics-any-of filter narrows to files carrying that topic. Pick the first
	// topic of fixture[0] and assert every returned file actually contains it.
	probe := fixtures[0].spec.Topics[0].Topic
	filtered, _, err := FilterFiles(ctx, st, FilterArgs{TopicsAnyOf: []string{probe}})
	if err != nil {
		t.Fatalf("FilterFiles(topics_any_of=%q): %v", probe, err)
	}
	if len(filtered) == 0 {
		t.Fatalf("topics_any_of=%q returned no files, expected >=1", probe)
	}
	for _, f := range filtered {
		topics, err := ListTopicsForFile(ctx, st, f.ID)
		if err != nil {
			t.Fatalf("ListTopicsForFile(%d): %v", f.ID, err)
		}
		found := false
		for _, tr := range topics {
			if tr.Name == probe {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("file %d returned by topics_any_of=%q does not contain it", f.ID, probe)
		}
	}
}

func keysOf(m map[string]FileSummary) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// requireBuilder resolves the mcap_catalog submodule dir and skips (not fails)
// when python3 / the builder package / its pip deps are unavailable.
func requireBuilder(t *testing.T) string {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not on PATH")
	}
	wd, err := os.Getwd() // = the package dir (server/internal/catalog)
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
	// `python3 -m mcap_catalog_builder` eagerly imports BOTH mcap and watchdog
	// (watcher.py imports watchdog at module load, pulled in by __main__). Probe
	// both so a host missing either gets a clean skip, not a loud failure.
	check := exec.Command("python3", "-c", "import mcap, watchdog")
	if out, err := check.CombinedOutput(); err != nil {
		t.Skipf("python deps missing (import mcap, watchdog): %v\n%s", err, out)
	}
	return dir
}
