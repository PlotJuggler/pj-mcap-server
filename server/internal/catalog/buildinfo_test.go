package catalog

import (
	"context"
	"path/filepath"
	"testing"
)

func TestGetBuildInfo_Auryn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auryn.db")
	db := openAurynTestDB(t, path)
	if _, err := db.Exec(`INSERT INTO build_metadata
		(id, build_id, last_build_ns, files_scanned, files_failed, build_outcome, builder_version)
		VALUES (1, 7, 1700000000000000000, 104, 2, 'partial', '0.1.0')`); err != nil {
		t.Fatalf("seed: %v", err)
	}
	db.Close()

	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()

	bi, err := GetBuildInfo(context.Background(), st)
	if err != nil {
		t.Fatalf("GetBuildInfo: %v", err)
	}
	if !bi.Present || bi.BuildID != 7 || bi.FilesScanned != 104 || bi.FilesFailed != 2 ||
		bi.Outcome != "partial" || bi.BuilderVersion != "0.1.0" || bi.LastBuildNs != 1700000000000000000 {
		t.Fatalf("BuildInfo = %+v, mismatch", bi)
	}
}

func TestGetBuildInfo_NoRowYet(t *testing.T) {
	// Table exists (v3 schema) but no build stamped => Present=false, no error.
	path := filepath.Join(t.TempDir(), "auryn.db")
	openAurynTestDB(t, path).Close()
	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()
	bi, err := GetBuildInfo(context.Background(), st)
	if err != nil || bi.Present {
		t.Fatalf("GetBuildInfo = (%+v, %v), want not-present + nil", bi, err)
	}
}

func TestGetBuildInfo_QueryErrorPropagates(t *testing.T) {
	// A REAL query error (not ErrNoRows) must propagate, NOT be mapped to
	// Present=false (Codex M6 review): replace build_metadata with a shape missing
	// builder_version so the SELECT fails on an existing row.
	path := filepath.Join(t.TempDir(), "auryn.db")
	db := openAurynTestDB(t, path)
	for _, s := range []string{
		`DROP TABLE build_metadata`,
		`CREATE TABLE build_metadata (id INTEGER PRIMARY KEY CHECK (id=1), build_id INTEGER NOT NULL,
			last_build_ns INTEGER NOT NULL, files_scanned INTEGER NOT NULL, files_failed INTEGER NOT NULL,
			build_outcome TEXT NOT NULL)`, // no builder_version column
		`INSERT INTO build_metadata(id, build_id, last_build_ns, files_scanned, files_failed, build_outcome)
			VALUES (1, 1, 1, 1, 0, 'ok')`,
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("setup %q: %v", s, err)
		}
	}
	db.Close()

	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()
	if _, err := GetBuildInfo(context.Background(), st); err == nil {
		t.Fatal("GetBuildInfo on a malformed build_metadata = nil err, want a propagated query error")
	}
}

func TestGetBuildInfo_LegacyNotPresent(t *testing.T) {
	st, err := Open(context.Background(), filepath.Join(t.TempDir(), "legacy.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	bi, err := GetBuildInfo(context.Background(), st)
	if err != nil || bi.Present {
		t.Fatalf("legacy GetBuildInfo = (%+v, %v), want not-present + nil", bi, err)
	}
}
