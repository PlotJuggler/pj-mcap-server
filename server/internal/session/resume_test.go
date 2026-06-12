package session

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// TestResume_BufferLevelReplayNoGapsNoDupes models the buffer-level resume
// contract (spec 6.5/6.6): a first attachment delivers seqs 1..N, the client
// acks through M (< N), the WS drops, the producer keeps filling; on resume the
// server prunes seq<=resume_after_seq and a fresh consumer replays the remainder
// IN ORDER with no gaps and no duplicates.
func TestResume_BufferLevelReplayNoGapsNoDupes(t *testing.T) {
	r := NewRetainBuffer(RetainOpts{MaxSeqs: 256, MaxBytes: 64 << 20})
	// Producer appends 10 batches.
	for i := uint64(1); i <= 10; i++ {
		r.Append(batchEnv(i, 1))
	}

	// --- First attachment: deliver 1..5, then "disconnect" (cancel ctx). ---
	w1 := &recWriter{}
	c1 := &Consumer{
		SubscriptionID: 1, Writer: w1, Retain: r,
		ProducerDone: make(chan struct{}), AckCh: make(chan uint64, 4),
		ProgressEvery: time.Hour,
	}
	ctx1, cancel1 := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); c1.Run(ctx1) }()
	// Wait until at least 5 batches have been delivered.
	waitFor(t, func() bool { return countBatches(w1.snapshot()) >= 5 })
	cancel1()
	wg.Wait()

	delivered1 := batchSeqs(w1.snapshot())
	if len(delivered1) < 5 {
		t.Fatalf("first attachment delivered only %d batches", len(delivered1))
	}
	resumeAfter := delivered1[4] // client durably has through the 5th delivered seq

	// --- Resume: prune acked, replay from resume_after_seq with a fresh consumer. ---
	r.Prune(resumeAfter)
	w2 := &recWriter{}
	c2 := &Consumer{
		SubscriptionID: 1, Writer: w2, Retain: r,
		ProducerDone:  closedChan(), // producer is done; replay then Eos
		AckCh:         make(chan uint64, 4),
		ProgressEvery: time.Hour,
		StartAfterSeq: resumeAfter,
		MessagesSent:  uint64(len(delivered1)), // carry forward totals
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	c2.Run(ctx2)

	delivered2 := batchSeqs(w2.snapshot())

	// No dupes across attachments: every seq in delivered2 must be > resumeAfter.
	for _, s := range delivered2 {
		if s <= resumeAfter {
			t.Errorf("resume replayed an already-acked seq %d (<= %d)", s, resumeAfter)
		}
	}
	// No gaps: the union of delivered1 (<=resumeAfter) + delivered2 covers 1..10
	// exactly once, in order on the resume leg.
	seen := map[uint64]int{}
	for _, s := range delivered1 {
		if s <= resumeAfter {
			seen[s]++
		}
	}
	for _, s := range delivered2 {
		seen[s]++
	}
	for i := uint64(1); i <= 10; i++ {
		if seen[i] != 1 {
			t.Errorf("seq %d delivered %d times across resume (want exactly 1)", i, seen[i])
		}
	}
	// Resume leg in strictly increasing order.
	for i := 1; i < len(delivered2); i++ {
		if delivered2[i] <= delivered2[i-1] {
			t.Errorf("resume leg not strictly increasing: %v", delivered2)
			break
		}
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for !cond() {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for condition")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func countBatches(frames []*pb.ServerMessage) int { return len(batchSeqs(frames)) }

func batchSeqs(frames []*pb.ServerMessage) []uint64 {
	var out []uint64
	for _, f := range frames {
		if b := f.GetBatch(); b != nil {
			out = append(out, b.Seq)
		}
	}
	return out
}
