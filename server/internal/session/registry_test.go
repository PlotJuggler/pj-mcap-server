package session

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func newState() *SessionState {
	return &SessionState{
		Plan:         Plan{},
		Retain:       NewRetainBuffer(RetainOpts{MaxSeqs: 8, MaxBytes: 1024}),
		AckCh:        make(chan uint64, 4),
		ProducerDone: make(chan struct{}),
	}
}

func TestRegistry_RegisterLookupCapacity(t *testing.T) {
	reg := NewRegistry(RegistryOpts{MaxConcurrent: 2, RetainAfterDisconnect: time.Minute})
	ctx := context.Background()

	s1, err := reg.Register(ctx, newState())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Register(ctx, newState()); err != nil {
		t.Fatal(err)
	}
	if _, err := reg.Register(ctx, newState()); err != ErrAtCapacity {
		t.Errorf("want ErrAtCapacity, got %v", err)
	}
	if got, ok := reg.Lookup(s1.ID); !ok || got != s1 {
		t.Error("Lookup returned wrong session")
	}
	if s1.ID == 0 {
		t.Error("Register must assign a non-zero subscription id")
	}
}

func TestRegistry_CancelCancelsProducerAndDiscardsRetain(t *testing.T) {
	reg := NewRegistry(RegistryOpts{MaxConcurrent: 4, RetainAfterDisconnect: time.Minute})
	s := newState()
	_, prodCancel := context.WithCancel(context.Background())
	var cancelled atomic.Bool
	s.ProducerCancel = func() { cancelled.Store(true); prodCancel() }
	var evicted atomic.Bool
	reg.SetOnEvict(func(*SessionState) { evicted.Store(true) })

	reg.Register(context.Background(), s)
	reg.Cancel(s.ID)

	if _, ok := reg.Lookup(s.ID); ok {
		t.Error("cancelled session still present in registry")
	}
	if !cancelled.Load() {
		t.Error("Cancel must cancel the producer")
	}
	if !evicted.Load() {
		t.Error("Cancel must invoke onEvict")
	}
	// Retain must be closed (discarded): an Append after Close is a no-op.
	s.Retain.Append(BatchEnvelope{Seq: 1, Bytes: 1})
	if s.Retain.Len() != 0 {
		t.Error("retain buffer should be discarded (closed) on Cancel")
	}
}

func TestRegistry_DetachArmsEvictionTimer(t *testing.T) {
	reg := NewRegistry(RegistryOpts{MaxConcurrent: 4, RetainAfterDisconnect: 30 * time.Millisecond})
	s := newState()
	var consumerCancelled atomic.Bool
	reg.Register(context.Background(), s)
	gen := reg.BindConsumer(s.ID, func() { consumerCancelled.Store(true) })

	reg.Detach(s.ID, gen)
	if !consumerCancelled.Load() {
		t.Error("Detach must cancel the active consumer")
	}
	time.Sleep(70 * time.Millisecond)
	if _, ok := reg.Lookup(s.ID); ok {
		t.Error("session not evicted after retain-after-disconnect expiry")
	}
}

// A resume that binds a NEW consumer must atomically cancel the prior one —
// otherwise a reconnect racing a half-open old connection leaves TWO consumers
// draining the same retain buffer (pre-merge review finding #2).
func TestRegistry_BindConsumerReplacesAndCancelsPrior(t *testing.T) {
	reg := NewRegistry(RegistryOpts{MaxConcurrent: 4})
	s := newState()
	reg.Register(context.Background(), s)

	var oldCancelled, newCancelled atomic.Bool
	gen1 := reg.BindConsumer(s.ID, func() { oldCancelled.Store(true) })
	gen2 := reg.BindConsumer(s.ID, func() { newCancelled.Store(true) })
	if !oldCancelled.Load() {
		t.Error("binding a new consumer must cancel the prior one")
	}
	if newCancelled.Load() {
		t.Error("the new consumer must not be cancelled by its own bind")
	}
	if gen2 <= gen1 {
		t.Errorf("consumer generations must increase: gen1=%d gen2=%d", gen1, gen2)
	}
}

// A STALE detach (the old connection's teardown arriving after a takeover) must
// be a no-op: it must neither cancel the new consumer nor arm eviction on a
// session that has a live attachment.
func TestRegistry_DetachStaleGenerationIsNoop(t *testing.T) {
	reg := NewRegistry(RegistryOpts{MaxConcurrent: 4, RetainAfterDisconnect: 30 * time.Millisecond})
	s := newState()
	reg.Register(context.Background(), s)

	gen1 := reg.BindConsumer(s.ID, func() {})
	var newCancelled atomic.Bool
	_ = reg.BindConsumer(s.ID, func() { newCancelled.Store(true) }) // takeover

	reg.Detach(s.ID, gen1) // the old connection finally times out
	if newCancelled.Load() {
		t.Error("stale detach cancelled the NEW consumer")
	}
	time.Sleep(70 * time.Millisecond)
	if _, ok := reg.Lookup(s.ID); !ok {
		t.Error("stale detach armed eviction: the actively-attached session was evicted")
	}
}

// The ledger's high watermark: the largest appended seq (0 before any append).
// openResume validates the client cursor against it.
func TestSessionState_HighestAppendedSeq(t *testing.T) {
	s := newState()
	if got := s.HighestAppendedSeq(); got != 0 {
		t.Fatalf("empty ledger: got %d want 0", got)
	}
	s.RecordAppend(1, 10, 100)
	s.RecordAppend(2, 10, 100)
	s.RecordAppend(3, 5, 50)
	if got := s.HighestAppendedSeq(); got != 3 {
		t.Fatalf("got %d want 3", got)
	}
}

func TestRegistry_ReattachCancelsEviction(t *testing.T) {
	reg := NewRegistry(RegistryOpts{MaxConcurrent: 4, RetainAfterDisconnect: 30 * time.Millisecond})
	s := newState()
	reg.Register(context.Background(), s)
	reg.Detach(s.ID, 0) // never bound: gen 0 matches
	time.Sleep(10 * time.Millisecond)
	if err := reg.Reattach(s.ID); err != nil {
		t.Fatalf("Reattach: %v", err)
	}
	time.Sleep(40 * time.Millisecond)
	if _, ok := reg.Lookup(s.ID); !ok {
		t.Error("Reattach failed to cancel the eviction timer")
	}
}

func TestRegistry_ReattachEvictedIsResumeNotPossible(t *testing.T) {
	reg := NewRegistry(RegistryOpts{MaxConcurrent: 4, RetainAfterDisconnect: time.Hour})
	if err := reg.Reattach(12345); err != ErrSessionMissing {
		t.Errorf("Reattach on unknown id: got %v want ErrSessionMissing", err)
	}
}

// TestSessionState_CountersThroughLedger locks the resume counter-carry contract:
// CountersThrough(seq) returns the cumulative (messages,bytes) for all batches with
// Seq <= seq, so a resume that replays seqs > resume_after_seq does NOT double-count
// them (the new consumer re-counts them as it re-delivers). It is the source of
// truth behind the post-resume Eos.total_*_sent monotonicity fix (Slice 10).
func TestSessionState_CountersThroughLedger(t *testing.T) {
	s := &SessionState{}
	// Producer appends 5 batches: 10 msgs/100 bytes each.
	for seq := uint64(1); seq <= 5; seq++ {
		s.RecordAppend(seq, 10, 100)
	}
	// Fresh open (seq 0) -> nothing carried.
	if m, b := s.CountersThrough(0); m != 0 || b != 0 {
		t.Errorf("CountersThrough(0): got (%d,%d) want (0,0)", m, b)
	}
	// Resume after seq 3 -> cumulative through seq 3 only (30 msgs / 300 bytes);
	// seqs 4,5 are replayed and must be excluded here.
	if m, b := s.CountersThrough(3); m != 30 || b != 300 {
		t.Errorf("CountersThrough(3): got (%d,%d) want (30,300)", m, b)
	}
	// Through the last seq -> the full total.
	if m, b := s.CountersThrough(5); m != 50 || b != 500 {
		t.Errorf("CountersThrough(5): got (%d,%d) want (50,500)", m, b)
	}
	// A seq beyond the ledger clamps to the full total (defensive).
	if m, b := s.CountersThrough(99); m != 50 || b != 500 {
		t.Errorf("CountersThrough(99): got (%d,%d) want (50,500)", m, b)
	}
}
