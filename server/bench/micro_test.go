//go:build bench

// This file adds the two in-process microbenchmarks pinned by Plan A Task 45's
// baseline keys: BenchmarkCompressionCPU (one-shot ZSTD encode throughput of a
// batch-sized payload) and BenchmarkBackpressureLatency (p50/p99 batch-delivery
// latency under a deliberately slow consumer). Unlike TestThroughputGate they
// touch NEITHER a server NOR Minio — they run fully in-process, so they never
// need :8082 (which matrix owns) and never SKIP. Both are REPORT-ONLY with
// generous, non-flapping floors (CI is cgroup-limited; the plan's 80 MB/s and
// 200 ms-p99 numbers are reference-machine targets, asserted softly here).
//
// Run via `make bench` (= `go test -tags=bench -bench=. -benchmem ./bench/...`).
package bench

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"pj-cloud/server/internal/session"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

const (
	// compressBatchBytes is the producer's default MaxBatchBytes (512 KiB,
	// producer.go ProducerOpts) — the representative one-shot-ZSTD body size.
	compressBatchBytes = 512 << 10
	// compressMinMBPerSec is the SOFT reference floor for encode throughput.
	// The plan's reference target is 80 MB/s; we assert a fraction of it so a
	// shared/cgroup-limited runner does not flap. Report is the real signal.
	compressMinMBPerSec = 20.0
	// backpressureMaxP99Ms is the SOFT reference ceiling for p99 batch-delivery
	// latency under the slow consumer. The plan's target is 200 ms; the slow
	// consumer here injects a fixed per-batch delay, so the floor is generous.
	backpressureMaxP99Ms = 2000.0
)

// realisticBatchPayload builds a ~512 KiB buffer with mixed compressibility: a
// run of structured/repetitive bytes (timestamps, ids) plus random noise, so the
// ZSTD encode does real work rather than trivially squashing zeros.
func realisticBatchPayload(n int) []byte {
	rng := rand.New(rand.NewSource(0xC0FFEE))
	buf := make([]byte, n)
	for i := 0; i < n; i++ {
		switch {
		case i%16 < 6:
			// Repetitive structured region (compresses well).
			buf[i] = byte(i / 16)
		default:
			// Incompressible noise.
			buf[i] = byte(rng.Intn(256))
		}
	}
	return buf
}

// BenchmarkCompressionCPU measures one-shot ZSTD EncodeAll throughput of a
// batch-sized payload — the exact hot operation the producer performs per batch
// (producer.go: zstd.NewWriter + EncodeAll, level 3, never carried across
// flushes). It reports MB/s and allocs. No server / no Minio.
func BenchmarkCompressionCPU(b *testing.B) {
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(3)))
	if err != nil {
		b.Fatalf("zstd init: %v", err)
	}
	defer enc.Close()

	payload := realisticBatchPayload(compressBatchBytes)
	dst := make([]byte, 0, compressBatchBytes)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()

	start := time.Now()
	for i := 0; i < b.N; i++ {
		dst = enc.EncodeAll(payload, dst[:0])
		if len(dst) == 0 {
			b.Fatal("empty encode")
		}
	}
	b.StopTimer()

	elapsed := time.Since(start)
	mbPerSec := (float64(len(payload)) * float64(b.N) / (1024 * 1024)) / elapsed.Seconds()
	b.ReportMetric(mbPerSec, "MB/s")
	// Machine-readable line the bench harness / CI can grep.
	fmt.Printf("BENCH_COMPRESSION_MBPS=%.1f\n", mbPerSec)
	if mbPerSec < compressMinMBPerSec {
		b.Errorf("COMPRESSION REGRESSION (soft floor): %.1f MB/s < %.1f MB/s reference floor", mbPerSec, compressMinMBPerSec)
	}
}

// slowWriter is a FrameWriter that sleeps a fixed time before "delivering" each
// bulk frame — a stand-in for a slow consumer / slow network. SendPriority is
// instant (control frames). It records when each batch was admitted.
type slowWriter struct {
	delay     time.Duration
	delivered int
}

func (s *slowWriter) SendPriority(_ *pb.ServerMessage) bool { return true }
func (s *slowWriter) SendBulk(_ *pb.ServerMessage) bool {
	time.Sleep(s.delay)
	s.delivered++
	return true
}

// chunkRefReader serves canned bytes per ref (content irrelevant — the iterator
// synthesizes messages). Mirrors the session producer-test fake, kept local so
// the bench needs no exported test helpers.
type chunkRefReader struct{}

func (chunkRefReader) ReadChunk(_ context.Context, ref session.ChunkRef) ([]byte, error) {
	return []byte{byte(ref.Offset)}, nil
}

// fixedMsgIter emits msgsPerChunk messages of payloadBytes each for every chunk,
// all on one topic the session wants.
type fixedMsgIter struct {
	msgsPerChunk int
	payloadBytes int
	topic        string
}

func (f fixedMsgIter) Iterate(_ []byte, ref session.ChunkRef, _ *session.TimeWindow, emit func(session.RawMessage) error) error {
	if _, want := ref.ChannelTopics[f.topic]; !want {
		return nil
	}
	payload := make([]byte, f.payloadBytes)
	for i := 0; i < f.msgsPerChunk; i++ {
		ts := ref.StartNs + int64(i)
		if err := emit(session.RawMessage{
			Topic:         f.topic,
			LogTimeNs:     ts,
			PublishTimeNs: ts,
			Payload:       payload,
		}); err != nil {
			return err
		}
	}
	return nil
}

// BenchmarkBackpressureLatency measures the wall-clock latency from a batch
// becoming available (appended to the retain buffer by the producer) to it being
// delivered by the consumer, when the consumer is deliberately slow. It uses the
// real in-process session harness (RetainBuffer 256 seqs / 64 MiB, Producer.Run,
// Consumer.Run) and reports p50/p99. Report-only with a generous p99 ceiling.
//
// The chunk count is chosen so the stream's total batch count exceeds the
// 256-seq retain cap: the producer therefore BLOCKS on RetainBuffer.Append until
// the slow consumer's acks prune room — i.e. real backpressure. The consumer
// acks each delivered batch (through its recording writer) so the producer can
// make progress and the stream completes (Eos) promptly instead of hanging.
func BenchmarkBackpressureLatency(b *testing.B) {
	const (
		topic        = "/bench"
		chunks       = 400 // ~800 batches >> 256-seq cap => producer backpressures
		msgsPerChunk = 64
		payloadBytes = 4096
		slowDelay    = 1 * time.Millisecond // per-batch consumer delay
	)

	// One run streams `chunks` chunks; b.N controls how many runs we aggregate so
	// -benchtime can scale it. We measure per-batch delivery latency across runs.
	var latencies []time.Duration

	for n := 0; n < b.N; n++ {
		plan := session.Plan{Chunks: make([]session.ChunkRef, chunks)}
		for i := 0; i < chunks; i++ {
			plan.Chunks[i] = session.ChunkRef{
				FileID:        1,
				Offset:        int64(i),
				StartNs:       int64(i * 1_000_000),
				EndNs:         int64(i*1_000_000 + msgsPerChunk),
				ChannelTopics: map[string]struct{}{topic: {}},
			}
		}

		rb := session.NewRetainBuffer(session.RetainOpts{MaxSeqs: 256, MaxBytes: 64 << 20})
		appendTimes := make(map[uint64]time.Time)
		var appendMu chan struct{} = make(chan struct{}, 1)
		appendMu <- struct{}{}

		prod := &session.Producer{
			Plan:        plan,
			ChunkReader: chunkRefReader{},
			ChunkIter:   fixedMsgIter{msgsPerChunk: msgsPerChunk, payloadBytes: payloadBytes, topic: topic},
			Retain:      rb,
			Opts: session.ProducerOpts{
				MaxBatchBytes:   128 << 10,
				MaxMessageBytes: 1 << 20,
				BodyZstdLevel:   3,
			},
			TopicID:  map[string]uint32{topic: 1},
			SchemaID: map[string]uint32{topic: 1},
			OnAppend: func(seq, _, _ uint64) {
				<-appendMu
				appendTimes[seq] = time.Now()
				appendMu <- struct{}{}
			},
		}

		producerDone := make(chan struct{})
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)

		// Run producer; close producerDone + the buffer when it finishes.
		go func() {
			_ = prod.Run(ctx)
			close(producerDone)
			rb.Close()
		}()

		// A recording writer stamps each batch's delivery time (after the slow
		// consumer's per-batch delay), keyed by the batch SEQ, and acks it so the
		// retain buffer prunes and the (backpressured) producer can make progress.
		ackCh := make(chan uint64, 1024)
		rw := &recordingWriter{slow: &slowWriter{delay: slowDelay}, deliveredAt: map[uint64]time.Time{}, ackCh: ackCh}
		cons := &session.Consumer{
			SubscriptionID: 1,
			Writer:         rw,
			Retain:         rb,
			ProducerDone:   producerDone,
			AckCh:          ackCh,
			ProgressEvery:  time.Hour, // suppress progress noise
		}
		cons.Run(ctx)

		// Pair append→deliver by seq. Latency = deliveredAt - appendTime.
		<-appendMu
		for seq, dt := range rw.deliveredAt {
			if at, ok := appendTimes[seq]; ok {
				latencies = append(latencies, dt.Sub(at))
			}
		}
		appendMu <- struct{}{}

		cancel()
	}

	if len(latencies) == 0 {
		b.Fatal("no batches delivered — harness produced nothing")
	}
	p50 := percentile(latencies, 50)
	p99 := percentile(latencies, 99)
	b.ReportMetric(float64(p50.Microseconds())/1000.0, "p50_ms")
	b.ReportMetric(float64(p99.Microseconds())/1000.0, "p99_ms")
	fmt.Printf("BENCH_BACKPRESSURE_P50_MS=%.2f BENCH_BACKPRESSURE_P99_MS=%.2f BATCHES=%d\n",
		float64(p50.Microseconds())/1000.0, float64(p99.Microseconds())/1000.0, len(latencies))
	if p99ms := float64(p99.Microseconds()) / 1000.0; p99ms > backpressureMaxP99Ms {
		b.Errorf("BACKPRESSURE REGRESSION (soft ceiling): p99 %.2f ms > %.0f ms reference ceiling", p99ms, backpressureMaxP99Ms)
	}
}

// recordingWriter wraps the slowWriter to stamp the delivery time of each bulk
// frame (after the slow-consumer delay), keyed by the batch seq, and feeds an ack
// for that seq back to the consumer so the retain buffer prunes. Sends are
// non-blocking (the ack channel is generously buffered) to avoid any
// self-deadlock with the single-goroutine consumer loop.
type recordingWriter struct {
	slow        *slowWriter
	deliveredAt map[uint64]time.Time
	ackCh       chan<- uint64
}

func (r *recordingWriter) SendPriority(m *pb.ServerMessage) bool { return r.slow.SendPriority(m) }
func (r *recordingWriter) SendBulk(m *pb.ServerMessage) bool {
	ok := r.slow.SendBulk(m)
	if ok {
		seq := m.GetBatch().GetSeq()
		r.deliveredAt[seq] = time.Now()
		select {
		case r.ackCh <- seq:
		default:
		}
	}
	return ok
}

func percentile(d []time.Duration, p int) time.Duration {
	if len(d) == 0 {
		return 0
	}
	cp := make([]time.Duration, len(d))
	copy(cp, d)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := (p * len(cp)) / 100
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}
