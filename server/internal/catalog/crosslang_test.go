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
