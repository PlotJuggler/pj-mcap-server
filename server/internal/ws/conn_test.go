package ws

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// recordWS records every frame written, in order.
type recordWS struct {
	mu     sync.Mutex
	frames [][]byte
}

func (r *recordWS) Write(_ context.Context, buf []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := append([]byte(nil), buf...)
	r.frames = append(r.frames, cp)
	return nil
}

func (r *recordWS) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.frames)
}

func TestConn_PriorityBeatsBulk(t *testing.T) {
	w := &recordWS{}
	c := newConnFromAdapter(w, 5*time.Second)

	// Queue a bulk frame then a priority frame BEFORE the write loop starts. When
	// the loop runs, the priority frame must be written first (frame 0).
	bulkMsg := &pb.ServerMessage{Payload: &pb.ServerMessage_Batch{Batch: &pb.MessageBatch{Seq: 99}}}
	priMsg := &pb.ServerMessage{Payload: &pb.ServerMessage_Progress{Progress: &pb.Progress{MessagesSent: 7}}}
	if !c.SendBulk(bulkMsg) {
		t.Fatal("SendBulk should succeed")
	}
	if !c.SendPriority(priMsg) {
		t.Fatal("SendPriority should succeed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go c.runWriteLoop(ctx)

	deadline := time.Now().Add(time.Second)
	for w.count() < 2 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if w.count() < 2 {
		t.Fatalf("expected 2 frames, got %d", w.count())
	}

	// Decode the first frame: it must be the Progress (priority), not the batch.
	var first pb.ServerMessage
	w.mu.Lock()
	firstBytes := w.frames[0]
	w.mu.Unlock()
	if err := proto.Unmarshal(firstBytes, &first); err != nil {
		t.Fatalf("decode first frame: %v", err)
	}
	if first.GetProgress() == nil {
		t.Errorf("first frame should be the priority Progress, got %T", first.GetPayload())
	}
}

func TestConn_SendBulkReturnsFalseWhenFull(t *testing.T) {
	w := &recordWS{}
	c := newConnFromAdapter(w, 5*time.Second)
	c.bulkCh = make(chan []byte, 1)
	// Do not start the write loop; fill the channel.
	if !c.SendBulk(&pb.ServerMessage{}) {
		t.Fatal("first SendBulk should succeed")
	}
	if c.SendBulk(&pb.ServerMessage{}) {
		t.Error("expected SendBulk to return false when channel full")
	}
}

func TestConn_SendReturnsFalseAfterClose(t *testing.T) {
	w := &recordWS{}
	c := newConnFromAdapter(w, 5*time.Second)
	c.bulkCh = make(chan []byte, 1)
	c.priorityCh = make(chan []byte, 1)
	// Fill both channels so the non-blocking send would otherwise just return
	// false on "full"; closeDone is the explicit teardown signal.
	_ = c.SendBulk(&pb.ServerMessage{})
	_ = c.SendPriority(&pb.ServerMessage{})
	c.closeDone()
	c.closeDone() // idempotent
	if c.SendBulk(&pb.ServerMessage{}) {
		t.Error("SendBulk after closeDone must be false")
	}
	if c.SendPriority(&pb.ServerMessage{}) {
		t.Error("SendPriority after closeDone must be false")
	}
}
