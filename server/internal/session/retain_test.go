package session

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRetain_AppendAndNextInOrder(t *testing.T) {
	r := NewRetainBuffer(RetainOpts{MaxSeqs: 8, MaxBytes: 1024})
	r.Append(BatchEnvelope{Seq: 1, Bytes: 10, Payload: []byte("a")})
	r.Append(BatchEnvelope{Seq: 2, Bytes: 10, Payload: []byte("b")})

	got, ok := r.Next(context.Background(), 0)
	if !ok || got.Seq != 1 {
		t.Errorf("Next(0): seq=%d ok=%v", got.Seq, ok)
	}
	got, ok = r.Next(context.Background(), 1)
	if !ok || got.Seq != 2 {
		t.Errorf("Next(1): seq=%d ok=%v", got.Seq, ok)
	}
}

func TestRetain_PruneByCountFreesSpaceForBlockedProducer(t *testing.T) {
	r := NewRetainBuffer(RetainOpts{MaxSeqs: 2, MaxBytes: 1 << 20})
	var wg sync.WaitGroup
	wg.Add(1)
	appended := make(chan uint64, 3)
	go func() {
		defer wg.Done()
		r.Append(BatchEnvelope{Seq: 1, Bytes: 1, Payload: []byte("a")})
		appended <- 1
		r.Append(BatchEnvelope{Seq: 2, Bytes: 1, Payload: []byte("b")})
		appended <- 2
		r.Append(BatchEnvelope{Seq: 3, Bytes: 1, Payload: []byte("c")}) // blocks: count cap
		appended <- 3
	}()
	// First two go in immediately; the third blocks until we prune.
	<-appended
	<-appended
	select {
	case <-appended:
		t.Fatal("third Append should have blocked at MaxSeqs=2")
	case <-time.After(30 * time.Millisecond):
	}
	r.Prune(1) // frees seq 1
	select {
	case s := <-appended:
		if s != 3 {
			t.Errorf("unblocked append was seq %d", s)
		}
	case <-time.After(time.Second):
		t.Fatal("Append did not unblock after Prune")
	}
	wg.Wait()
	if r.Len() != 2 {
		t.Errorf("expected 2 retained (seqs 2,3), got %d", r.Len())
	}
}

func TestRetain_PruneByBytes(t *testing.T) {
	r := NewRetainBuffer(RetainOpts{MaxSeqs: 100, MaxBytes: 50})
	for i := uint64(1); i <= 5; i++ {
		r.Append(BatchEnvelope{Seq: i, Bytes: 10, Payload: []byte("x")})
	}
	if r.Len() != 5 || r.Bytes() != 50 {
		t.Fatalf("setup: len=%d bytes=%d", r.Len(), r.Bytes())
	}
	r.Prune(3) // free seqs 1..3 → 30 bytes back
	if r.Len() != 2 || r.Bytes() != 20 {
		t.Errorf("after prune: len=%d bytes=%d want 2/20", r.Len(), r.Bytes())
	}
}

func TestRetain_ByteCapBackpressuresProducer(t *testing.T) {
	r := NewRetainBuffer(RetainOpts{MaxSeqs: 100, MaxBytes: 25})
	done := make(chan struct{})
	go func() {
		r.Append(BatchEnvelope{Seq: 1, Bytes: 10})
		r.Append(BatchEnvelope{Seq: 2, Bytes: 10})
		r.Append(BatchEnvelope{Seq: 3, Bytes: 10}) // 30 > 25 -> blocks
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("third Append should block on the byte cap")
	case <-time.After(30 * time.Millisecond):
	}
	r.Prune(1) // free 10 bytes
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Append did not unblock after byte prune")
	}
}

func TestRetain_NextBlocksUntilCancelled(t *testing.T) {
	r := NewRetainBuffer(RetainOpts{MaxSeqs: 8, MaxBytes: 1024})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, ok := r.Next(ctx, 0)
	if ok {
		t.Error("Next on an empty buffer should return ok=false on ctx cancel")
	}
}

func TestRetain_CloseUnblocksAppendAndNext(t *testing.T) {
	r := NewRetainBuffer(RetainOpts{MaxSeqs: 1, MaxBytes: 1})
	// Park a producer at the cap.
	parked := make(chan struct{})
	go func() {
		r.Append(BatchEnvelope{Seq: 1, Bytes: 1})
		r.Append(BatchEnvelope{Seq: 2, Bytes: 1}) // blocks
		close(parked)
	}()
	time.Sleep(20 * time.Millisecond)
	// Park a consumer waiting for a (never-coming) higher seq.
	waited := make(chan struct{})
	go func() {
		r.Next(context.Background(), 99)
		close(waited)
	}()
	time.Sleep(20 * time.Millisecond)
	r.Close()
	select {
	case <-parked:
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock parked Append")
	}
	select {
	case <-waited:
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock waiting Next")
	}
}
