package catalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

// writeStampedDB creates a minimal catalog DB at path with a schema_version row
// at the given version (and a trivial table to read back), mimicking what the
// Python builder stamps. It uses the same pure-Go driver the reader uses.
func writeStampedDB(t *testing.T, path string, version int) {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	defer db.Close()
	stmts := []string{
		"CREATE TABLE schema_version (id INTEGER PRIMARY KEY CHECK (id = 1), version INTEGER NOT NULL)",
		fmt.Sprintf("INSERT INTO schema_version(id, version) VALUES (1, %d)", version),
		"CREATE TABLE customers (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
		"INSERT INTO customers(id, name) VALUES (1, 'dexory')",
	}
	for _, s := range stmts {
		if _, err := db.Exec(s); err != nil {
			t.Fatalf("stamp DB (%s): %v", s, err)
		}
	}
}

func TestOpenReadOnly_HappyPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.db")
	writeStampedDB(t, path, SchemaVersion)

	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer st.Close()

	// Reads work over the same handle the rest of the reader uses.
	var name string
	if err := st.DB().QueryRow("SELECT name FROM customers WHERE id = 1").Scan(&name); err != nil {
		t.Fatalf("read customers: %v", err)
	}
	if name != "dexory" {
		t.Fatalf("customer name = %q, want dexory", name)
	}

	// Write must fail fast on a read-only store (no writer goroutine).
	if err := st.Write(context.Background(), func(*sql.Tx) error { return nil }); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("Write on RO store = %v, want ErrReadOnly", err)
	}
}

// TestStore_ReadOnly proves the exported accessor mirrors how the Store was
// opened, so callers outside the package (the ws Hello handler) can gate a
// capability on it without reaching into the unexported field.
func TestStore_ReadOnly(t *testing.T) {
	roPath := filepath.Join(t.TempDir(), "ro.db")
	writeStampedDB(t, roPath, SchemaVersion)
	ro, err := OpenReadOnly(context.Background(), roPath)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	defer ro.Close()
	if !ro.ReadOnly() {
		t.Error("Store opened via OpenReadOnly: ReadOnly() = false, want true")
	}

	rw, err := Open(context.Background(), filepath.Join(t.TempDir(), "rw.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer rw.Close()
	if rw.ReadOnly() {
		t.Error("Store opened via Open: ReadOnly() = true, want false")
	}
}

func TestOpenReadOnly_VersionMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.db")
	writeStampedDB(t, path, SchemaVersion+1)

	_, err := OpenReadOnly(context.Background(), path)
	var sve *SchemaVersionError
	if !errors.As(err, &sve) {
		t.Fatalf("OpenReadOnly err = %v, want *SchemaVersionError", err)
	}
	if sve.Got != int64(SchemaVersion+1) || sve.Want != SchemaVersion {
		t.Fatalf("SchemaVersionError got=%d want=%d, expected got=%d want=%d",
			sve.Got, sve.Want, SchemaVersion+1, SchemaVersion)
	}
}

func TestOpenReadOnly_Unstamped(t *testing.T) {
	// A DB with no schema_version table at all (a pre-contract / legacy DB) must be
	// refused — the reader cannot know it speaks the same shape.
	path := filepath.Join(t.TempDir(), "legacy.db")
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec("CREATE TABLE files (id INTEGER PRIMARY KEY)"); err != nil {
		t.Fatalf("create: %v", err)
	}
	db.Close()

	_, err = OpenReadOnly(context.Background(), path)
	var sve *SchemaVersionError
	if !errors.As(err, &sve) {
		t.Fatalf("OpenReadOnly err = %v, want *SchemaVersionError", err)
	}
	if sve.Got != 0 {
		t.Fatalf("SchemaVersionError.Got = %d, want 0 (no version)", sve.Got)
	}
}

func TestOpenReadOnly_MissingFile(t *testing.T) {
	// mode=ro must NOT create the DB: a missing file is a fail-fast (the builder
	// has to run first), not a silent empty catalog.
	path := filepath.Join(t.TempDir(), "does-not-exist.db")
	if _, err := OpenReadOnly(context.Background(), path); err == nil {
		t.Fatal("OpenReadOnly on a missing file = nil, want error")
	}
}
