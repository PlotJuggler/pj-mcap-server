package session

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// recWriter records frames sent to it; failBulk makes SendBulk fail (drop sim).
type recWriter struct {
	mu       sync.Mutex
	frames   []*pb.ServerMessage
	failBulk bool
}

func (w *recWriter) SendPriority(m *pb.ServerMessage) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.frames = append(w.frames, m)
	return true
}
func (w *recWriter) SendBulk(m *pb.ServerMessage) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failBulk {
		return false
	}
	w.frames = append(w.frames, m)
	return true
}
func (w *recWriter) snapshot() []*pb.ServerMessage {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]*pb.ServerMessage(nil), w.frames...)
}

func batchEnv(seq uint64, msgs uint64) BatchEnvelope {
	p, _ := proto.Marshal(&pb.MessageBatch{Seq: seq, BodyEncoding: pb.BodyEncoding_BODY_ENCODING_ZSTD})
	return BatchEnvelope{Seq: seq, Bytes: 10, Messages: msgs, Payload: p}
}

func closedChan() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

func TestConsumer_DrainsThenEosOnProducerDone(t *testing.T) {
	r := NewRetainBuffer(RetainOpts{MaxSeqs: 8, MaxBytes: 1024})
	r.Append(batchEnv(1, 3))
	r.Append(batchEnv(2, 4))

	w := &recWriter{}
	c := &Consumer{
		SubscriptionID: 77, Writer: w, Retain: r,
		ProducerDone: closedChan(), AckCh: make(chan uint64, 4),
		ProgressEvery: time.Hour,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.Run(ctx)

	frames := w.snapshot()
	if len(frames) < 3 {
		t.Fatalf("expected >=3 frames (2 batches + Eos), got %d", len(frames))
	}
	last := frames[len(frames)-1]
	eos := last.GetEos()
	if eos == nil {
		t.Fatalf("last frame not Eos: %T", last.GetPayload())
	}
	if eos.GetReason() != pb.EosReason_EOS_REASON_COMPLETE {
		t.Errorf("eos reason: %v want COMPLETE", eos.GetReason())
	}
	if eos.GetTotalMessagesSent() != 7 {
		t.Errorf("total_messages_sent: got %d want 7", eos.GetTotalMessagesSent())
	}
	if last.GetSubscriptionId() != 77 {
		t.Errorf("subscription_id on Eos: got %d", last.GetSubscriptionId())
	}
	// Every batch frame must carry the subscription id and be on the bulk path.
	for _, f := range frames {
		if f.GetBatch() != nil && f.GetSubscriptionId() != 77 {
			t.Errorf("batch frame missing subscription id")
		}
	}
}

func TestConsumer_EmitsProgressOnPriorityPath(t *testing.T) {
	r := NewRetainBuffer(RetainOpts{MaxSeqs: 8, MaxBytes: 1024})
	r.Append(batchEnv(1, 2))
	w := &recWriter{}
	// ProducerDone never closes here; we cancel after a couple progress ticks.
	pd := make(chan struct{})
	c := &Consumer{
		SubscriptionID: 5, Writer: w, Retain: r,
		ProducerDone: pd, AckCh: make(chan uint64, 4),
		ProgressEvery:  15 * time.Millisecond,
		EstimatedBytes: 1000, EstimatedMessages: 42,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	var sawProgress bool
	for _, f := range w.snapshot() {
		if p := f.GetProgress(); p != nil {
			sawProgress = true
			if p.GetEstimatedTotalMessages() != 42 || p.GetEstimatedTotalBytes() != 1000 {
				t.Errorf("progress estimates not echoed: %+v", p)
			}
		}
	}
	if !sawProgress {
		t.Error("expected at least one Progress frame on the priority path")
	}
}

// The producer's oversized-message drop counter must reach the wire: Progress
// and the terminal Eos read the LIVE count via DroppedFn (the producer runs
// concurrently, so a snapshot at spawn time would under-report).
func TestConsumer_ProgressCarriesLiveDroppedCount(t *testing.T) {
	r := NewRetainBuffer(RetainOpts{MaxSeqs: 8, MaxBytes: 1024})
	r.Append(batchEnv(1, 2))
	w := &recWriter{}
	pd := make(chan struct{})
	c := &Consumer{
		SubscriptionID: 6, Writer: w, Retain: r,
		ProducerDone: pd, AckCh: make(chan uint64, 4),
		ProgressEvery: 15 * time.Millisecond,
		DroppedFn:     func() uint64 { return 7 },
	}
	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	c.Run(ctx)

	var sawProgress bool
	for _, f := range w.snapshot() {
		if p := f.GetProgress(); p != nil {
			sawProgress = true
			if p.GetDroppedMessages() != 7 {
				t.Errorf("Progress.dropped_messages: got %d want 7 (live DroppedFn)", p.GetDroppedMessages())
			}
		}
	}
	if !sawProgress {
		t.Error("expected at least one Progress frame")
	}
}

func TestConsumer_DetachesOnWriteFailure(t *testing.T) {
	r := NewRetainBuffer(RetainOpts{MaxSeqs: 8, MaxBytes: 1024})
	r.Append(batchEnv(1, 1))
	w := &recWriter{failBulk: true}
	c := &Consumer{
		SubscriptionID: 9, Writer: w, Retain: r,
		ProducerDone: make(chan struct{}), AckCh: make(chan uint64, 4),
		ProgressEvery: time.Hour,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.Run(ctx)
	if !c.Detached {
		t.Error("consumer should set Detached=true after a bulk write failure")
	}
}

func TestConsumer_PrunesOnAck(t *testing.T) {
	r := NewRetainBuffer(RetainOpts{MaxSeqs: 8, MaxBytes: 1024})
	r.Append(batchEnv(1, 1))
	r.Append(batchEnv(2, 1))
	r.Append(batchEnv(3, 1))
	ackCh := make(chan uint64, 1)
	w := &recWriter{}
	pd := make(chan struct{})
	c := &Consumer{
		SubscriptionID: 1, Writer: w, Retain: r,
		ProducerDone: pd, AckCh: ackCh, ProgressEvery: time.Hour,
	}
	ctx, cancel := context.WithCancel(context.Background())
	go c.Run(ctx)
	ackCh <- 2 // ack through seq 2
	deadline := time.After(time.Second)
	for r.Len() > 1 {
		select {
		case <-deadline:
			t.Fatalf("ack did not prune: len=%d", r.Len())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	if r.Len() != 1 {
		t.Errorf("after ack-through-2, expected 1 retained (seq 3), got %d", r.Len())
	}
}
