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

// closeDone can race itself: the write loop calls it on write failure while the
// read loop calls it on teardown. A select-then-close pair is NOT concurrently
// idempotent — both goroutines can observe "open" and the second close panics.
func TestConn_CloseDoneConcurrentIdempotent(t *testing.T) {
	for i := 0; i < 5000; i++ {
		c := newConnFromAdapter(&recordWS{}, time.Second, nil)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				c.closeDone() // must never panic ("close of closed channel")
			}()
		}
		close(start)
		wg.Wait()
	}
}

// SendBulk must BLOCK on a momentarily-full queue (real backpressure) and
// return false ONLY when the connection is gone: the session consumer treats
// false as "WS dropped" and detaches, so a transient burst must never detach a
// live stream (pre-merge review finding #1).
func TestConn_SendBulkBlocksWhenFullFalseOnlyWhenDone(t *testing.T) {
	c := newConnFromAdapter(&recordWS{}, time.Second, nil) // no write loop: queue never drains
	m := &pb.ServerMessage{Payload: &pb.ServerMessage_Batch{Batch: &pb.MessageBatch{Seq: 1}}}
	for i := 0; i < bulkChanCap; i++ {
		if !c.SendBulk(m) {
			t.Fatalf("SendBulk %d should queue", i)
		}
	}
	res := make(chan bool, 1)
	go func() { res <- c.SendBulk(m) }()
	select {
	case v := <-res:
		t.Fatalf("SendBulk on a full queue returned %v immediately; must block until space frees or the conn dies", v)
	case <-time.After(100 * time.Millisecond):
		// blocked — correct
	}
	c.closeDone()
	select {
	case v := <-res:
		if v {
			t.Fatal("SendBulk after closeDone must return false")
		}
	case <-time.After(time.Second):
		t.Fatal("SendBulk still blocked after closeDone")
	}
}

// A blocked SendBulk must complete (true) once the write loop frees a slot —
// the backpressure path delivers, it does not drop.
func TestConn_SendBulkUnblocksWhenDrained(t *testing.T) {
	w := &recordWS{}
	c := newConnFromAdapter(w, time.Second, nil)
	m := &pb.ServerMessage{Payload: &pb.ServerMessage_Batch{Batch: &pb.MessageBatch{Seq: 1}}}
	for i := 0; i < bulkChanCap; i++ {
		if !c.SendBulk(m) {
			t.Fatalf("SendBulk %d should queue", i)
		}
	}
	res := make(chan bool, 1)
	go func() { res <- c.SendBulk(m) }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.runWriteLoop(ctx)
	select {
	case v := <-res:
		if !v {
			t.Fatal("blocked SendBulk must return true once the queue drains")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendBulk still blocked while the write loop drains")
	}
}

// SendPriority carries Error/Progress/Eos — control frames must never be
// silently dropped on a full queue either; same block-then-deliver contract.
func TestConn_SendPriorityBlocksWhenFullFalseOnlyWhenDone(t *testing.T) {
	c := newConnFromAdapter(&recordWS{}, time.Second, nil)
	m := &pb.ServerMessage{Payload: &pb.ServerMessage_Progress{Progress: &pb.Progress{MessagesSent: 1}}}
	for i := 0; i < priorityChanCap; i++ {
		if !c.SendPriority(m) {
			t.Fatalf("SendPriority %d should queue", i)
		}
	}
	res := make(chan bool, 1)
	go func() { res <- c.SendPriority(m) }()
	select {
	case v := <-res:
		t.Fatalf("SendPriority on a full queue returned %v immediately; must block", v)
	case <-time.After(100 * time.Millisecond):
	}
	c.closeDone()
	select {
	case v := <-res:
		if v {
			t.Fatal("SendPriority after closeDone must return false")
		}
	case <-time.After(time.Second):
		t.Fatal("SendPriority still blocked after closeDone")
	}
}

func TestConn_PriorityBeatsBulk(t *testing.T) {
	w := &recordWS{}
	c := newConnFromAdapter(w, 5*time.Second, nil)

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

// NOTE: the former TestConn_SendBulkReturnsFalseWhenFull pinned the OLD
// nonblocking contract ("queue full => false"), which conflated a transient
// burst with a dead socket and made the consumer detach live streams (pre-merge
// review finding #1). The contract is now block-when-full / false-only-when-done
// — see TestConn_SendBulkBlocksWhenFullFalseOnlyWhenDone.

func TestConn_SendReturnsFalseAfterClose(t *testing.T) {
	w := &recordWS{}
	c := newConnFromAdapter(w, 5*time.Second, nil)
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
