package catalog

// Tests for the DEGRADED-START store state: the server may now boot before the
// Python builder has published its first catalog. NewAwaiting constructs a
// Store with no snapshot; the regular ReopenIfSwapped tick picks the catalog up
// the moment the builder's atomic os.replace lands. OpenReadOnly keeps its
// fail-fast contract (readonly_test.go pins it) — awaiting is an explicit,
// opt-in state chosen by main() when the DB does not exist yet.

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestNewAwaiting_NotReadyAndNilSafe(t *testing.T) {
	dir := t.TempDir()
	st := newAwaitingT(t, filepath.Join(dir, "catalog.db"))
	defer st.Close()

	if st.Ready() {
		t.Fatal("awaiting store must not be Ready before the first catalog appears")
	}
	if db := st.DB(); db != nil {
		t.Fatalf("DB() on awaiting store = %v, want nil", db)
	}
	if gen := st.Generation(); gen != nil {
		t.Fatalf("Generation() on awaiting store = %v, want nil", gen)
	}
	lease := st.Acquire()
	if lease.DB() != nil {
		t.Fatal("Acquire().DB() on awaiting store must be nil")
	}
	if lease.Generation() != nil {
		t.Fatal("Acquire().Generation() on awaiting store must be nil")
	}
	lease.Release() // must not panic; idempotent
	lease.Release()
}

func TestAwaiting_ReopenWithAbsentFileIsQuietNoop(t *testing.T) {
	dir := t.TempDir()
	st := newAwaitingT(t, filepath.Join(dir, "catalog.db"))
	defer st.Close()

	swapped, err := st.ReopenIfSwapped(context.Background())
	if err != nil {
		t.Fatalf("ReopenIfSwapped with no file yet: unexpected error %v", err)
	}
	if swapped {
		t.Fatal("ReopenIfSwapped with no file yet: swapped=true")
	}
	if st.Ready() {
		t.Fatal("store became Ready with no catalog file")
	}
}

func TestAwaiting_PicksUpFirstPublish(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.db")
	st := newAwaitingT(t, path)
	defer st.Close()

	// Builder publishes its first catalog: build to a temp name, atomic rename.
	tmp := filepath.Join(dir, "catalog.db.building")
	buildNamedAurynDB(t, tmp, "first")
	replaceFile(t, path, tmp)

	swapped, err := st.ReopenIfSwapped(context.Background())
	if err != nil {
		t.Fatalf("ReopenIfSwapped after first publish: %v", err)
	}
	if !swapped {
		t.Fatal("ReopenIfSwapped after first publish: swapped=false")
	}
	if !st.Ready() {
		t.Fatal("store not Ready after picking up the first catalog")
	}
	lease := st.Acquire()
	defer lease.Release()
	if got := lease.Generation(); len(got) != generationTokenLen {
		t.Fatalf("generation token length = %d, want %d", len(got), generationTokenLen)
	}
	var name string
	if err := lease.DB().QueryRowContext(context.Background(),
		"SELECT name FROM customers WHERE id=1").Scan(&name); err != nil {
		t.Fatalf("query through first-publish snapshot: %v", err)
	}
	if name != "first" {
		t.Fatalf("customer = %q, want %q", name, "first")
	}

	// Second call: steady-state no-op.
	swapped, err = st.ReopenIfSwapped(context.Background())
	if err != nil || swapped {
		t.Fatalf("second ReopenIfSwapped = (%v, %v), want (false, nil)", swapped, err)
	}
}

func TestAwaiting_InvalidFirstPublishFailsClosedThenRecovers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.db")
	st := newAwaitingT(t, path)
	defer st.Close()

	if err := os.WriteFile(path, []byte("this is not a sqlite database"), 0o644); err != nil {
		t.Fatal(err)
	}
	swapped, err := st.ReopenIfSwapped(context.Background())
	if err == nil {
		t.Fatal("ReopenIfSwapped on a garbage file: want verification error")
	}
	if swapped || st.Ready() {
		t.Fatal("garbage file must not make the store Ready")
	}

	// The builder replaces the garbage with a real catalog: recovery.
	tmp := filepath.Join(dir, "catalog.db.building")
	buildNamedAurynDB(t, tmp, "recovered")
	replaceFile(t, path, tmp)
	swapped, err = st.ReopenIfSwapped(context.Background())
	if err != nil || !swapped {
		t.Fatalf("recovery ReopenIfSwapped = (%v, %v), want (true, nil)", swapped, err)
	}
	if !st.Ready() {
		t.Fatal("store not Ready after recovery")
	}
}

// newAwaitingT is the test-side constructor wrapper (NewAwaiting only errors if
// crypto/rand fails, which is fatal for a test anyway).
func newAwaitingT(t *testing.T, dbPath string) *Store {
	t.Helper()
	st, err := NewAwaiting(dbPath)
	if err != nil {
		t.Fatalf("NewAwaiting: %v", err)
	}
	return st
}
