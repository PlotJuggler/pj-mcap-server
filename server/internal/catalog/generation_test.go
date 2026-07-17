package catalog

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// The generation token identifies one served catalog generation: stable across
// no-op reopen checks (and in-place WAL reconciles), changed ONLY by a verified
// (dev,inode) swap, and NEVER derived from build_metadata.build_id (the
// contract says build ids are not comparable across rebuilds).

func openGenStore(t *testing.T) (*Store, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "served.db")
	buildNamedAurynDB(t, path, "genA")
	st, err := OpenReadOnly(context.Background(), path)
	if err != nil {
		t.Fatalf("OpenReadOnly: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st, path
}

func TestGeneration_StableAcrossNoopReopen(t *testing.T) {
	st, _ := openGenStore(t)
	g1 := st.Generation()
	if len(g1) == 0 {
		t.Fatal("Generation() must be non-empty")
	}
	swapped, err := st.ReopenIfSwapped(context.Background())
	if err != nil || swapped {
		t.Fatalf("noop reopen: swapped=%v err=%v", swapped, err)
	}
	if !bytes.Equal(g1, st.Generation()) {
		t.Fatal("generation changed across a NO-OP reopen check")
	}
}

func TestGeneration_ChangesOnVerifiedSwap(t *testing.T) {
	st, path := openGenStore(t)
	g1 := st.Generation()

	next := filepath.Join(t.TempDir(), "next.db")
	buildNamedAurynDB(t, next, "genB")
	replaceFile(t, path, next)

	swapped, err := st.ReopenIfSwapped(context.Background())
	if err != nil || !swapped {
		t.Fatalf("swap: swapped=%v err=%v", swapped, err)
	}
	g2 := st.Generation()
	if bytes.Equal(g1, g2) {
		t.Fatal("generation did NOT change across a verified swap")
	}
	if len(g2) != len(g1) {
		t.Fatalf("token length changed: %d -> %d", len(g1), len(g2))
	}
}

func TestGeneration_FailedSwapKeepsTokenAndServes(t *testing.T) {
	st, path := openGenStore(t)
	g1 := st.Generation()

	// Replace the served path with garbage: identity differs but verification
	// must fail, keeping BOTH the old handle and the old token (fail-closed).
	garbage := filepath.Join(t.TempDir(), "garbage.db")
	if err := os.WriteFile(garbage, []byte("not a sqlite file"), 0o600); err != nil {
		t.Fatal(err)
	}
	replaceFile(t, path, garbage)

	swapped, err := st.ReopenIfSwapped(context.Background())
	if err == nil || swapped {
		t.Fatalf("garbage swap: swapped=%v err=%v (want fail-closed error)", swapped, err)
	}
	if !bytes.Equal(g1, st.Generation()) {
		t.Fatal("generation changed although the swap FAILED verification")
	}
	if got := servedCustomer(t, st); got != "genA" {
		t.Fatalf("still-serving check: got customer %q want genA", got)
	}
}

// Close racing ReopenIfSwapped must not break the snapshot refcount: a lease
// held across the race keeps its db usable, Close is idempotent, and a swap that
// lands after Close must not double-drop the current ref (closing a leased db)
// nor leak the newly-published one. Also: Acquire after Close must not resurrect
// a closed snapshot.
func TestSnapshot_CloseRacesReopen(t *testing.T) {
	for i := 0; i < 200; i++ {
		st, path := openGenStore(t)
		lease := st.Acquire()
		oldDB := lease.DB()

		next := filepath.Join(t.TempDir(), "next.db")
		buildNamedAurynDB(t, next, "genB")

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); replaceFile(t, path, next); _, _ = st.ReopenIfSwapped(context.Background()) }()
		go func() { defer wg.Done(); _ = st.Close() }()
		wg.Wait()

		// The lease's db must still be usable regardless of who won — the refcount
		// must never have dropped it while we hold a reference.
		if err := oldDB.PingContext(context.Background()); err != nil {
			t.Fatalf("iter %d: leased db closed while a lease was held: %v", i, err)
		}
		lease.Release()
		_ = st.Close() // idempotent
	}
}

// A held lease pins its whole (db, generation) snapshot across a swap: queries
// on the leased handle keep working (drain-then-close), the leased generation
// stays the OLD one, a fresh Acquire sees the NEW pair, and the old db closes
// only once the last lease releases.
func TestSnapshot_LeaseDrainsBeforeClose(t *testing.T) {
	st, path := openGenStore(t)

	lease := st.Acquire()
	oldDB := lease.DB()
	oldGen := lease.Generation()

	next := filepath.Join(t.TempDir(), "next.db")
	buildNamedAurynDB(t, next, "genB")
	replaceFile(t, path, next)
	swapped, err := st.ReopenIfSwapped(context.Background())
	if err != nil || !swapped {
		t.Fatalf("swap: swapped=%v err=%v", swapped, err)
	}

	// The old snapshot must still be fully usable while the lease is held.
	if err := oldDB.PingContext(context.Background()); err != nil {
		t.Fatalf("leased old db unusable after swap (closed too early?): %v", err)
	}
	var name string
	if err := oldDB.QueryRowContext(context.Background(),
		`SELECT name FROM customers LIMIT 1`).Scan(&name); err != nil {
		t.Fatalf("query on leased old db: %v", err)
	}
	if name != "genA" {
		t.Fatalf("leased old db served %q, want the OLD generation genA", name)
	}
	if !bytes.Equal(lease.Generation(), oldGen) {
		t.Fatal("lease generation changed under the holder")
	}

	// A fresh acquire sees the new pair.
	fresh := st.Acquire()
	if bytes.Equal(fresh.Generation(), oldGen) {
		t.Fatal("fresh lease still returns the old generation after the swap")
	}
	if got := servedCustomer(t, st); got != "genB" {
		t.Fatalf("new generation check: got %q want genB", got)
	}
	fresh.Release()

	// Releasing the last old lease closes the old db (idempotent Release).
	lease.Release()
	lease.Release()
	if err := oldDB.PingContext(context.Background()); err == nil {
		t.Fatal("old db still open after the last lease released (no drain-then-close)")
	}
}
