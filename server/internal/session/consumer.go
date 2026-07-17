package session

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// FrameWriter is the WS write side seen by the session. SendPriority carries
// control frames (Progress, Eos, Error) and wins over SendBulk (MessageBatch) at
// frame boundaries (spec 6.4 multiplexing fairness). A send on a momentarily
// full queue BLOCKS (backpressure); either returns false ONLY when the
// connection is gone — the consumer then detaches and the producer keeps
// filling the retain buffer for resume.
type FrameWriter interface {
	SendPriority(m *pb.ServerMessage) bool
	SendBulk(m *pb.ServerMessage) bool
}

// Consumer drains the retain buffer into the WS in seq order, prunes on
// SessionAck, emits Progress on the priority path (~1 s), and emits the terminal
// Eos when the producer is done AND the buffer is drained. One Consumer per WS
// attachment; re-spawned (with a fresh lastSeq) on resume.
type Consumer struct {
	SubscriptionID uint64
	Writer         FrameWriter
	Retain         *RetainBuffer
	ProducerDone   <-chan struct{} // closed when the producer will append no more
	AckCh          <-chan uint64   // through_seq from incoming SessionAck frames
	ProgressEvery  time.Duration   // default 1s

	// StartAfterSeq lets resume skip already-delivered batches: the first Next()
	// asks for Seq > StartAfterSeq. Zero for a fresh attachment.
	StartAfterSeq uint64

	// Pre-flight estimates echoed in Progress (spec Progress.estimated_total_*).
	EstimatedBytes    uint64
	EstimatedMessages uint64

	// Counters (updated as frames go out; surfaced in Progress + Eos). On resume
	// these carry forward from the prior attachment so totals stay monotonic.
	BytesSent    uint64
	MessagesSent uint64
	Dropped      uint64

	// DroppedFn, when set, supplies the LIVE producer-side dropped-message count
	// for Progress (the producer runs concurrently, so a value snapshotted at
	// spawn time would under-report). Overrides Dropped.
	DroppedFn func() uint64

	// Detached is set true when the consumer exits because a WS write failed (vs
	// ctx cancellation / clean Eos). The handler uses it to decide whether to arm
	// the retain-after-disconnect eviction timer.
	Detached bool
}

// Run drives the attachment until ctx is cancelled, a WS write fails (sets
// Detached), or the producer is done and the buffer is drained (sends Eos).
func (c *Consumer) Run(ctx context.Context) {
	if c.ProgressEvery <= 0 {
		c.ProgressEvery = time.Second
	}
	tick := time.NewTicker(c.ProgressEvery)
	defer tick.Stop()

	lastSeq := c.StartAfterSeq

	// A helper goroutine runs the blocking Retain.Next so the main loop can also
	// service AckCh, the Progress ticker, ProducerDone and ctx. nextCh is
	// capacity-1; we only ever have one outstanding request.
	type nextResult struct {
		env BatchEnvelope
		ok  bool
	}
	nextCh := make(chan nextResult, 1)
	requestNext := func() {
		go func(last uint64) {
			env, ok := c.Retain.Next(ctx, last)
			nextCh <- nextResult{env, ok}
		}(lastSeq)
	}
	requestNext()

	for {
		select {
		case <-ctx.Done():
			return
		case ack := <-c.AckCh:
			c.Retain.Prune(ack)
		case <-tick.C:
			if !c.sendProgress() {
				c.Detached = true
				return
			}
		case res := <-nextCh:
			if !res.ok {
				// ctx cancelled or buffer closed inside Next; let the loop pick up
				// ctx.Done / ProducerDone on the next pass.
				continue
			}
			if !c.sendBatch(res.env) {
				c.Detached = true
				return // WS write failed: detach; producer keeps running.
			}
			lastSeq = res.env.Seq
			requestNext()
		case <-c.ProducerDone:
			// ProducerDone closes on BOTH natural exhaustion AND a cancel/shutdown
			// (the producer's deferred close fires when its ctx is cancelled by
			// Registry.Cancel / CancelAll). If THIS consumer's ctx is also already
			// cancelled, the session is being torn down — do NOT drain+Eos{COMPLETE}
			// (that would be a spurious "completed" terminal for a cancelled session,
			// and would log bogus per-session accounting on shutdown). Let the cancel
			// path own the teardown (handleCancel sends Eos{CANCELLED}; CancelAll just
			// closes the WS). When ctx is live this is a true natural completion.
			if ctx.Err() != nil {
				return
			}
			c.drainAndEos(ctx, lastSeq)
			return
		}
	}
}

func (c *Consumer) sendProgress() bool {
	dropped := c.Dropped
	if c.DroppedFn != nil {
		dropped = c.DroppedFn()
	}
	return c.Writer.SendPriority(&pb.ServerMessage{
		SubscriptionId: c.SubscriptionID,
		Payload: &pb.ServerMessage_Progress{Progress: &pb.Progress{
			BytesSent:              c.BytesSent,
			MessagesSent:           c.MessagesSent,
			DroppedMessages:        dropped,
			EstimatedTotalBytes:    c.EstimatedBytes,
			EstimatedTotalMessages: c.EstimatedMessages,
		}},
	})
}

func (c *Consumer) sendBatch(env BatchEnvelope) bool {
	var batch pb.MessageBatch
	if err := proto.Unmarshal(env.Payload, &batch); err != nil {
		// A corrupt retained payload is a stream-fatal internal error; treat as a
		// write failure so the attachment tears down.
		return false
	}
	if !c.Writer.SendBulk(&pb.ServerMessage{
		SubscriptionId: c.SubscriptionID,
		Payload:        &pb.ServerMessage_Batch{Batch: &batch},
	}) {
		return false
	}
	// Count only after a successful write so totals reflect delivered messages.
	c.MessagesSent += env.Messages
	c.BytesSent += uint64(env.Bytes)
	return true
}

// drainAndEos pushes any remaining retained batches (producer is done; no more
// appends) and then sends the terminal Eos{COMPLETE}. startSeq is the last seq
// already delivered on this attachment.
func (c *Consumer) drainAndEos(ctx context.Context, startSeq uint64) {
	lastSeq := startSeq
	for {
		// Service any pending acks first (frees the buffer, harmless mid-drain).
		select {
		case ack := <-c.AckCh:
			c.Retain.Prune(ack)
		default:
		}
		// Bounded attempt to fetch the next undelivered batch. ProducerDone is
		// closed, so the only batches that can exist are already-appended ones;
		// a short timeout returns promptly once the buffer is exhausted.
		drainCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
		env, ok := c.Retain.Next(drainCtx, lastSeq)
		cancel()
		if !ok {
			break
		}
		if !c.sendBatch(env) {
			c.Detached = true
			return
		}
		lastSeq = env.Seq
	}
	c.Writer.SendPriority(&pb.ServerMessage{
		SubscriptionId: c.SubscriptionID,
		Payload: &pb.ServerMessage_Eos{Eos: &pb.Eos{
			Reason:            pb.EosReason_EOS_REASON_COMPLETE,
			TotalMessagesSent: c.MessagesSent,
			TotalBytesSent:    c.BytesSent,
		}},
	})
}
