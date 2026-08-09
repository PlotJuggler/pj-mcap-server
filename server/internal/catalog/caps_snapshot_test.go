package catalog

import (
	"context"
	"sync"
	"testing"
)

// The contract this type exists to enforce: reading capabilities is a pure
// memory read. Every WebSocket handshake used to run `SELECT DISTINCT key FROM
// tags_effective` plus a files probe on the ONE pinned catalog connection
// (readonly.go), so while a slow catalog RPC held that connection a brand-new
// client could not finish its handshake at all — measured 2026-08-09 against an
// 8.78M-file catalog, where GetVocabulary took 40.7 s and concurrent connects
// failed with "no response to handshake".
func TestCapsSnapshotServesWithoutTouchingTheDatabase(t *testing.T) {
	st := openCapsFixtureStore(t, []string{"flat_a.mcap"}, nil)

	snap := NewCapsSnapshot()

	// Before any refresh: a safe, non-empty floor. A degraded start (no catalog
	// published yet) must still let clients connect.
	vocab, hierarchy := snap.Get()
	if len(vocab) == 0 {
		t.Fatal("pre-refresh Get must return the derived-key floor, not empty")
	}
	if hierarchy {
		t.Fatal("pre-refresh hierarchy must default false")
	}

	if err := snap.Refresh(context.Background(), st); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	vocab2, _ := snap.Get()
	if len(vocab2) == 0 {
		t.Fatal("post-refresh vocabulary must be non-empty")
	}

	// Concurrent reads must be safe and must never block (run under -race).
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				if v, _ := snap.Get(); len(v) == 0 {
					t.Error("Get returned an empty vocabulary")
					return
				}
			}
		}()
	}
	wg.Wait()
}

// A failed refresh must never blank out a previously-good value: the handshake
// keeps working with slightly stale capabilities rather than degrading. This is
// the whole reason Refresh publishes only on success.
func TestCapsSnapshotRefreshFailureKeepsLastGood(t *testing.T) {
	st := openCapsFixtureStore(t, []string{"flat_a.mcap"}, nil)
	snap := NewCapsSnapshot()
	if err := snap.Refresh(context.Background(), st); err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	before, hierBefore := snap.Get()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // a cancelled context makes the underlying queries fail
	if err := snap.Refresh(ctx, st); err == nil {
		t.Fatal("expected Refresh to fail on a cancelled context")
	}

	after, hierAfter := snap.Get()
	if len(after) != len(before) || hierAfter != hierBefore {
		t.Fatalf("a failed refresh clobbered the last-good value: %v/%v -> %v/%v",
			before, hierBefore, after, hierAfter)
	}
}
