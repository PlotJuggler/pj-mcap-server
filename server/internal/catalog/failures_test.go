package catalog

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
)

func TestRecentFailures_Auryn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "auryn.db")
	db := openAurynTestDB(t, path)
	rows := []struct {
		key  string
		when int64
		msg  string
	}{
		{"customer=a/.../old.mcap", 100, "unparseable key"},
		{"customer=a/.../new.mcap", 300, "no summary/statistics in MCAP"},
		{"customer=a/.../mid.mcap", 200, "boom"},
	}
	for _, r := range rows {
		if _, err := db.Exec(`INSERT INTO catalog_failures(s3_key, failed_at_ns, error_text) VALUES (?,?,?)`,
			r.key, r.when, r.msg); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	db.Close()

	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()

	fails, err := RecentFailures(context.Background(), st, 20)
	if err != nil {
		t.Fatalf("RecentFailures: %v", err)
	}
	if len(fails) != 3 {
		t.Fatalf("got %d failures, want 3", len(fails))
	}
	// Newest-first ordering by failed_at_ns DESC: new(300), mid(200), old(100).
	if fails[0].WhenNs != 300 || fails[1].WhenNs != 200 || fails[2].WhenNs != 100 {
		t.Fatalf("order = %d,%d,%d, want 300,200,100", fails[0].WhenNs, fails[1].WhenNs, fails[2].WhenNs)
	}
	if fails[0].ErrText != "no summary/statistics in MCAP" {
		t.Fatalf("newest err = %q", fails[0].ErrText)
	}

	// limit caps the result.
	if got, _ := RecentFailures(context.Background(), st, 1); len(got) != 1 || got[0].WhenNs != 300 {
		t.Fatalf("limit=1 should return only the newest, got %+v", got)
	}
}

func TestRecentFailures_Legacy(t *testing.T) {
	st, err := Open(context.Background(), filepath.Join(t.TempDir(), "legacy.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer st.Close()
	// Insert into the legacy indexer_failures table via the writer.
	for i := 0; i < 3; i++ {
		i := i
		if err := st.Write(context.Background(), func(tx *sql.Tx) error {
			_, err := tx.Exec(`INSERT INTO indexer_failures(s3_key, failed_at, error_text) VALUES (?,?,?)`,
				fmt.Sprintf("k%d.mcap", i), int64(100*(i+1)), "boom")
			return err
		}); err != nil {
			t.Fatalf("write failure %d: %v", i, err)
		}
	}
	fails, err := RecentFailures(context.Background(), st, 20)
	if err != nil {
		t.Fatalf("RecentFailures (legacy): %v", err)
	}
	if len(fails) != 3 || fails[0].WhenNs != 300 {
		t.Fatalf("legacy failures = %+v, want 3 newest-first", fails)
	}
}
