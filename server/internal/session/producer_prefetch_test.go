package session

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// batchMessages decodes a retain envelope's wire payload into its messages,
// accepting both the ZSTD-body default and the singleton NONE fallback.
func batchMessages(t *testing.T, payload []byte) []*pb.Message {
	t.Helper()
	var b pb.MessageBatch
	if err := proto.Unmarshal(payload, &b); err != nil {
		t.Fatalf("unmarshal MessageBatch: %v", err)
	}
	if b.BodyEncoding == pb.BodyEncoding_BODY_ENCODING_NONE {
		return b.Messages
	}
	return decodeBatchBody(t, &b)
}

// nextWithTimeout drains one envelope, treating "nothing arrives in 200ms"
// as end-of-stream (RetainBuffer.Next blocks for future batches by design).
func nextWithTimeout(rb *RetainBuffer, after uint64) (BatchEnvelope, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	return rb.Next(ctx, after)
}

// slowReader fakes WAN chunk GETs: fixed per-chunk latency, optional failure
// at one offset, and an in-flight high-water mark proving fetch overlap.
type slowReader struct {
	delay        time.Duration
	failAtOffset int64

	mu          sync.Mutex
	inFlight    int
	maxInFlight int
}

func (r *slowReader) ReadChunk(ctx context.Context, ref ChunkRef) ([]byte, error) {
	r.mu.Lock()
	r.inFlight++
	if r.inFlight > r.maxInFlight {
		r.maxInFlight = r.inFlight
	}
	r.mu.Unlock()
	defer func() {
		r.mu.Lock()
		r.inFlight--
		r.mu.Unlock()
	}()
	if r.failAtOffset != 0 && ref.Offset == r.failAtOffset {
		return nil, fmt.Errorf("synthetic storage failure")
	}
	select {
	case <-time.After(r.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return []byte{byte(ref.Offset)}, nil
}

// A WAN plan of many chunks must not pay one full GET latency per chunk
// back-to-back: the producer prefetches a bounded window of chunks
// concurrently while decoding/sending the current one. 12 chunks x 40ms
// sequential is >=480ms; the prefetch pipeline must land well under that and
// must show >1 fetch in flight.
func TestProducer_PrefetchOverlapsChunkFetches(t *testing.T) {
	const numChunks = 12
	const delay = 40 * time.Millisecond

	var refs []ChunkRef
	byOffset := map[int64][]RawMessage{}
	for i := 0; i < numChunks; i++ {
		off := int64(i + 1)
		refs = append(refs, chunk(1, int64(i*10), int64(i*10+9), off, 1, "/x"))
		byOffset[off] = []RawMessage{msg("/x", int64(i*10), "payload")}
	}
	reader := &slowReader{delay: delay}
	rb := NewRetainBuffer(RetainOpts{MaxSeqs: 64, MaxBytes: 1 << 20})
	p := &Producer{
		Plan: Plan{Chunks: refs}, ChunkReader: reader, ChunkIter: fakeIter{byOffset: byOffset}, Retain: rb,
		Opts:     ProducerOpts{MaxBatchBytes: 1, MaxMessageBytes: 1 << 20, CompressThresholdBytes: 4096},
		TopicID:  map[string]uint32{"/x": 7},
		SchemaID: map[string]uint32{"/x": 3},
	}

	start := time.Now()
	runProducer(t, p)
	elapsed := time.Since(start)

	if sequential := time.Duration(numChunks) * delay; elapsed > sequential*3/4 {
		t.Errorf("producer took %v; want well under the sequential %v (chunk fetches must overlap)", elapsed, sequential)
	}
	if reader.maxInFlight < 2 {
		t.Errorf("max in-flight chunk fetches = %d, want >= 2 (prefetch)", reader.maxInFlight)
	}

	// Order is sacred regardless of fetch concurrency: batches must carry the
	// plan-order messages (ascending log times here).
	var lastSeq uint64
	var lastLogTime int64 = -1
	for {
		env, ok := nextWithTimeout(rb, lastSeq)
		if !ok {
			break
		}
		lastSeq = env.Seq
		for _, m := range batchMessages(t, env.Payload) {
			if m.LogTimeNs < lastLogTime {
				t.Fatalf("out-of-order message: log_time %d after %d", m.LogTimeNs, lastLogTime)
			}
			lastLogTime = m.LogTimeNs
		}
	}
	if lastLogTime != int64((numChunks-1)*10) {
		t.Errorf("last message log_time = %d, want %d (all chunks delivered)", lastLogTime, (numChunks-1)*10)
	}
}

// A failed chunk GET mid-plan aborts the run with the read-chunk error, never
// hangs, and never delivers messages from chunks after the failure point.
func TestProducer_PrefetchAbortsInOrderOnFetchError(t *testing.T) {
	const numChunks = 8
	var refs []ChunkRef
	byOffset := map[int64][]RawMessage{}
	for i := 0; i < numChunks; i++ {
		off := int64(i + 1)
		refs = append(refs, chunk(1, int64(i*10), int64(i*10+9), off, 1, "/x"))
		byOffset[off] = []RawMessage{msg("/x", int64(i*10), "payload")}
	}
	reader := &slowReader{delay: time.Millisecond, failAtOffset: 5}
	rb := NewRetainBuffer(RetainOpts{MaxSeqs: 64, MaxBytes: 1 << 20})
	p := &Producer{
		Plan: Plan{Chunks: refs}, ChunkReader: reader, ChunkIter: fakeIter{byOffset: byOffset}, Retain: rb,
		Opts:     ProducerOpts{MaxBatchBytes: 1, MaxMessageBytes: 1 << 20, CompressThresholdBytes: 4096},
		TopicID:  map[string]uint32{"/x": 7},
		SchemaID: map[string]uint32{"/x": 3},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := p.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "read chunk") {
		t.Fatalf("Run error = %v, want a read-chunk failure", err)
	}

	var lastSeq uint64
	var lastLogTime int64 = -1
	for {
		env, ok := nextWithTimeout(rb, lastSeq)
		if !ok {
			break
		}
		lastSeq = env.Seq
		for _, m := range batchMessages(t, env.Payload) {
			lastLogTime = m.LogTimeNs
		}
	}
	// Chunks 1-4 (offsets 1..4) may have been delivered; the failing offset 5
	// and anything after it must not be.
	if lastLogTime >= 40 {
		t.Errorf("messages delivered past the failed chunk: last log_time %d", lastLogTime)
	}
}

// Cancelling the run context mid-plan unblocks promptly (no goroutine deadlock
// between the prefetcher and the consumer).
func TestProducer_PrefetchCancelUnblocksPromptly(t *testing.T) {
	const numChunks = 50
	var refs []ChunkRef
	byOffset := map[int64][]RawMessage{}
	for i := 0; i < numChunks; i++ {
		off := int64(i + 1)
		refs = append(refs, chunk(1, int64(i*10), int64(i*10+9), off, 1, "/x"))
		byOffset[off] = []RawMessage{msg("/x", int64(i*10), "payload")}
	}
	reader := &slowReader{delay: 30 * time.Millisecond}
	rb := NewRetainBuffer(RetainOpts{MaxSeqs: 64, MaxBytes: 1 << 20})
	p := &Producer{
		Plan: Plan{Chunks: refs}, ChunkReader: reader, ChunkIter: fakeIter{byOffset: byOffset}, Retain: rb,
		Opts:     ProducerOpts{MaxBatchBytes: 1, MaxMessageBytes: 1 << 20, CompressThresholdBytes: 4096},
		TopicID:  map[string]uint32{"/x": 7},
		SchemaID: map[string]uint32{"/x": 3},
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(60 * time.Millisecond)
		cancel()
	}()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run returned nil after cancel, want a context error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of cancellation")
	}
}
