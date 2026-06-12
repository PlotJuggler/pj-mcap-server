package catalog

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := Open(ctx, filepath.Join(dir, "catalog.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestStoreOpenAppliesSchema(t *testing.T) {
	s := newTestStore(t)
	for _, tbl := range []string{"files", "topics", "tags_embedded", "tags_override", "indexer_failures"} {
		var name string
		row := s.DB().QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, tbl)
		if err := row.Scan(&name); err != nil {
			t.Fatalf("table %q missing: %v", tbl, err)
		}
	}
	// the override-wins view must exist too.
	var view string
	row := s.DB().QueryRow(`SELECT name FROM sqlite_master WHERE type='view' AND name='tags_effective'`)
	if err := row.Scan(&view); err != nil {
		t.Fatalf("tags_effective view missing: %v", err)
	}
}

func TestStoreWriteRoundTrip(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	err := s.Write(ctx, func(tx *sql.Tx) error {
		_, err := tx.Exec(
			`INSERT INTO files (s3_key, s3_etag, s3_last_modified, size_bytes,
			    indexed_at, start_time_ns, end_time_ns, chunk_count, message_count, has_message_index)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			"path/to/file.mcap", "etag-123", 100, 4096, 200, 1000, 2000, 4, 1024, 1,
		)
		return err
	})
	if err != nil {
		t.Fatalf("Write: %v", err)
	}

	var key string
	row := s.DB().QueryRow(`SELECT s3_key FROM files WHERE id = 1`)
	if err := row.Scan(&key); err != nil {
		t.Fatalf("query: %v", err)
	}
	if key != "path/to/file.mcap" {
		t.Errorf("s3_key: got %q want %q", key, "path/to/file.mcap")
	}
}

// TestStoreWriteAfterCloseReturnsError asserts that Write after Close returns an
// error instead of panicking on a send to the closed writer channel (the
// closed-channel guard, Slice 10 flagged defect 2). A late Write can happen on a
// shutdown race (an indexer iteration finishing its extract just as Close fires);
// it must fail gracefully, not crash the process.
func TestStoreWriteAfterCloseReturnsError(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	s, err := Open(ctx, filepath.Join(dir, "catalog.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// This MUST NOT panic. Before the guard it sent on a closed channel
	// ("send on closed channel" panic); after the guard it returns ErrStoreClosed.
	werr := s.Write(ctx, func(tx *sql.Tx) error {
		_, e := tx.Exec(`INSERT INTO files (s3_key, s3_etag, s3_last_modified, size_bytes,
		    indexed_at, start_time_ns, end_time_ns, chunk_count, message_count, has_message_index)
		 VALUES ('late', 'e', 1, 1, 1, 1, 2, 0, 0, 0)`)
		return e
	})
	if werr == nil {
		t.Fatal("Write after Close: want an error, got nil")
	}
	if !errors.Is(werr, ErrStoreClosed) {
		t.Errorf("Write after Close: want ErrStoreClosed, got %v", werr)
	}
}

// TestStoreCloseIsIdempotent guards that a double Close (also a shutdown race)
// does not panic on a second close of the writer channel.
func TestStoreCloseIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(context.Background(), filepath.Join(dir, "catalog.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	// Second Close must be a no-op, not a panic / double db.Close error.
	if err := s.Close(); err != nil {
		t.Errorf("second Close: want nil, got %v", err)
	}
}

func TestStoreWriteRollsBackOnError(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	wantErr := "boom"
	err := s.Write(ctx, func(tx *sql.Tx) error {
		_, _ = tx.Exec(
			`INSERT INTO files (s3_key, s3_etag, s3_last_modified, size_bytes,
			    indexed_at, start_time_ns, end_time_ns, chunk_count, message_count, has_message_index)
			 VALUES ('x', 'e', 1, 1, 1, 1, 2, 0, 0, 0)`)
		return &boomError{wantErr}
	})
	if err == nil || err.Error() != wantErr {
		t.Fatalf("want error %q, got %v", wantErr, err)
	}

	var count int
	_ = s.DB().QueryRow(`SELECT COUNT(*) FROM files`).Scan(&count)
	if count != 0 {
		t.Errorf("rollback failed: %d rows present", count)
	}
}

type boomError struct{ msg string }

func (e *boomError) Error() string { return e.msg }

// TestStoreWriteFnPanicRecovers proves a panicking WriteFn does NOT kill the
// single writer goroutine (which would deadlock all later writers) and does NOT
// leave its own caller hanging: the caller gets an error, and a subsequent
// normal write still succeeds (spec §8.1 per-catalog-writer-job recovery).
func TestStoreWriteFnPanicRecovers(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	err := s.Write(ctx, func(*sql.Tx) error {
		panic("kaboom in write job")
	})
	if err == nil {
		t.Fatalf("panicking WriteFn returned nil error; want a recovered-panic error")
	}

	// The writer goroutine survived: a normal write completes.
	ok := s.Write(ctx, func(tx *sql.Tx) error {
		_, e := tx.Exec(
			`INSERT INTO files (s3_key, s3_etag, s3_last_modified, size_bytes,
			    indexed_at, start_time_ns, end_time_ns, chunk_count, message_count, has_message_index)
			 VALUES ('survivor', 'e', 1, 1, 1, 1, 2, 0, 0, 0)`)
		return e
	})
	if ok != nil {
		t.Fatalf("write after recovered panic failed: %v (writer goroutine died?)", ok)
	}
	var count int
	_ = s.DB().QueryRow(`SELECT COUNT(*) FROM files WHERE s3_key='survivor'`).Scan(&count)
	if count != 1 {
		t.Fatalf("survivor row count = %d; want 1", count)
	}
}
