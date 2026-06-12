package session

import (
	"context"
	"sync"
)

// BatchEnvelope is the unit of retention: one already-marshaled MessageBatch
// ready to ship, plus the bookkeeping the buffer needs. Payload is the OPAQUE
// marshaled MessageBatch bytes — the retain/ack/resume machinery never looks
// inside, which is exactly why the one-shot-per-batch ZSTD invariant (spec 6.4)
// lets resume replay whole retained batches with no decoder state.
type BatchEnvelope struct {
	Seq          uint64
	SourceFileID uint64
	Bytes        int64  // serialized batch size (retain byte-budget accounting)
	Messages     uint64 // message count in this batch (producer knows it; the ZSTD body is opaque to the consumer)
	Payload      []byte // marshaled pb.MessageBatch
}

// RetainOpts bounds the buffer: MaxSeqs (default 256) AND MaxBytes (default
// 64 MiB) — whichever cap is hit first backpressures the producer (spec 6.5).
type RetainOpts struct {
	MaxSeqs  int
	MaxBytes int64
}

// RetainBuffer is a bounded FIFO of batches ordered by seq. The producer
// Appends (blocks at caps); the consumer Next()s in seq order and Prune()s on
// SessionAck. Append unblocks once Prune frees room.
//
// Concurrency: one producer (Append) + one consumer (Next/Prune), which may be
// re-spawned on resume. A single mutex + cond serializes all of it.
type RetainBuffer struct {
	mu         sync.Mutex
	cond       *sync.Cond
	opts       RetainOpts
	queue      []BatchEnvelope // seq-ascending
	totalBytes int64
	closed     bool
}

// NewRetainBuffer builds a buffer with the given caps. A non-positive cap is
// treated as the spec default.
func NewRetainBuffer(opts RetainOpts) *RetainBuffer {
	if opts.MaxSeqs <= 0 {
		opts.MaxSeqs = 256
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = 64 << 20
	}
	r := &RetainBuffer{opts: opts}
	r.cond = sync.NewCond(&r.mu)
	return r
}

// Append blocks until there is room (by seq count AND byte count) and then
// enqueues. A single batch larger than MaxBytes is admitted when the buffer is
// otherwise empty (oversized-singleton batches must still make progress).
// Append is the producer's only caller. If the buffer is Closed, Append returns
// immediately without enqueuing (the producer is being torn down).
func (r *RetainBuffer) Append(b BatchEnvelope) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for !r.closed && len(r.queue) > 0 &&
		(len(r.queue) >= r.opts.MaxSeqs || r.totalBytes+b.Bytes > r.opts.MaxBytes) {
		r.cond.Wait()
	}
	if r.closed {
		return
	}
	r.queue = append(r.queue, b)
	r.totalBytes += b.Bytes
	r.cond.Broadcast()
}

// Next returns the lowest-seq batch with Seq > lastSeq, blocking until one is
// available, ctx is cancelled, or the buffer is Closed. Returns (env, true) on
// success or (zero, false) on cancellation/close. The consumer is the only
// caller (re-spawned on resume with the appropriate lastSeq).
func (r *RetainBuffer) Next(ctx context.Context, lastSeq uint64) (BatchEnvelope, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// One ctx-watcher per call: broadcasts on cancellation so a blocked Wait
	// wakes. stopWatch tears it down on return (no per-iteration goroutines).
	watchDone := make(chan struct{})
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			r.mu.Lock()
			r.cond.Broadcast()
			r.mu.Unlock()
		case <-stop:
		}
		close(watchDone)
	}()
	defer func() {
		close(stop)
		// Drain the watcher (it may already be exiting); does not hold the lock.
		r.mu.Unlock()
		<-watchDone
		r.mu.Lock()
	}()

	for {
		for i := range r.queue {
			if r.queue[i].Seq > lastSeq {
				return r.queue[i], true
			}
		}
		if r.closed || ctx.Err() != nil {
			return BatchEnvelope{}, false
		}
		r.cond.Wait()
	}
}

// Prune removes all batches with Seq <= throughSeq, freeing room for the
// producer. Called by the consumer on SessionAck (spec 6.5).
func (r *RetainBuffer) Prune(throughSeq uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	cut := 0
	freed := int64(0)
	for cut < len(r.queue) && r.queue[cut].Seq <= throughSeq {
		freed += r.queue[cut].Bytes
		cut++
	}
	if cut > 0 {
		r.queue = append(r.queue[:0], r.queue[cut:]...)
		r.totalBytes -= freed
		r.cond.Broadcast()
	}
}

// Close marks the buffer dead: pending+future Appends return without enqueuing
// and blocked Next()s wake up returning false. Used on Cancel and full teardown
// (discard retain, spec 6.6). Idempotent.
func (r *RetainBuffer) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.cond.Broadcast()
}

// Len returns the current retained-batch count (test/observability).
func (r *RetainBuffer) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.queue)
}

// Bytes returns the current retained byte total (test/observability).
func (r *RetainBuffer) Bytes() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.totalBytes
}
