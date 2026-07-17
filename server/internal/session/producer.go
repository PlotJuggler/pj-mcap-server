package session

import (
	"context"
	"fmt"
	"math"
	"sort"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// ChunkReader fetches one MCAP chunk-record's raw bytes (the Range-GET of
// [ref.Offset, ref.Offset+ref.Length)). Test fakes serve from an in-memory map;
// production wires through storage.BlobStore.GetRange. The reader is keyed by
// ref.FileID so multi-file stitching works through one Producer. Implementations
// must tolerate concurrent ReadChunk calls (the producer prefetches a bounded
// window of chunks — see Run).
type ChunkReader interface {
	ReadChunk(ctx context.Context, ref ChunkRef) ([]byte, error)
}

// kChunkPrefetch is the bounded read-ahead window of concurrently fetched
// chunks (so in-flight GETs and fetched-but-unconsumed blobs are each capped
// at this, ~chunk-size x this of extra memory per session). Sized to hide WAN
// GET latency without hammering the object store.
const kChunkPrefetch = 4

// ChunkIterator walks a chunk's raw bytes and calls emit for each message the
// session wants (topic + time filtered). Production is backed by
// format.FileChunkIndex.Iterate (which carries the file's channel/schema table);
// tests use an in-memory parser. ref.FileID selects the per-file iterator.
type ChunkIterator interface {
	Iterate(chunkBytes []byte, ref ChunkRef, tr *TimeWindow, emit func(RawMessage) error) error
}

// ProducerOpts is the batching policy (spec 6.4 defaults in parentheses).
type ProducerOpts struct {
	MaxBatchBytes          int           // flush at this accumulated payload size (512 KiB)
	MaxBatchAge            time.Duration // flush this long after the batch's first message (50 ms)
	MaxMessageBytes        int           // hard per-message cap; over => drop + count (16 MiB)
	BodyZstdLevel          int           // one-shot ZSTD level for the batch body (3)
	CompressThresholdBytes int           // NONE-fallback only: per-message RAW/ZSTD threshold (4 KiB)
	TimeRange              *TimeWindow   // optional intra-chunk window passed to the iterator
}

// Producer is the per-session "fetch chunk -> iterate -> batch -> retain.Append"
// goroutine (spec 6.5 + 8.2). It blocks on RetainBuffer.Append at the caps — the
// natural backpressure for a slow/absent consumer.
type Producer struct {
	Plan        Plan
	ChunkReader ChunkReader
	ChunkIter   ChunkIterator
	Retain      *RetainBuffer
	Opts        ProducerOpts

	// TopicID / SchemaID are the wire-binding ids the session handler assigned,
	// keyed by topic name (each topic has exactly one schema in v1).
	TopicID  map[string]uint32
	SchemaID map[string]uint32

	// OnDrop is called once per message dropped for exceeding MaxMessageBytes
	// (surfaced as Progress.dropped_messages). Optional.
	OnDrop func(reason string)

	// OnAppend is called once per batch right after it is appended to the retain
	// buffer, in monotonic seq order, with that batch's (seq, messages, bytes).
	// The WS layer uses it to build the per-seq cumulative ledger that seeds
	// resume counter carry-forward. Optional.
	OnAppend func(seq, messages, bytes uint64)

	nextSeq uint64

	// fetchedBytes accumulates the actual chunk-record bytes Range-GET from the
	// blob store for THIS session's stream (the producer's fetch budget). It is
	// the ground truth the estimated_chunk_bytes pre-flight is asserted against
	// (must be within 5%). Read via FetchedBytes(); written only from Run.
	fetchedBytes atomic.Uint64
}

// FetchedBytes returns the running total of chunk-record bytes the producer has
// Range-GET from the blob store for this session. After Run returns it is the
// final fetched-bytes total. Concurrency-safe.
func (p *Producer) FetchedBytes() uint64 { return p.fetchedBytes.Load() }

// Run streams the plan into the retain buffer, returning when the plan is
// exhausted (nil) or ctx is cancelled (ctx.Err()). The HARD INVARIANT (spec 6.4)
// is enforced here: each batch body is one self-contained EncodeAll frame; the
// encoder is NEVER carried across flushes.
func (p *Producer) Run(ctx context.Context) error {
	level := p.Opts.BodyZstdLevel
	if level <= 0 {
		level = 3
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
	if err != nil {
		return fmt.Errorf("session: zstd init: %w", err)
	}
	defer enc.Close()

	bb := newBatchBuilder(p.Opts.MaxBatchBytes, p.Opts.MaxBatchAge)

	flush := func(sourceFileID uint64) error {
		if bb.empty() {
			return nil
		}
		msgs := bb.takeMessages()
		p.nextSeq++
		batch := &pb.MessageBatch{Seq: p.nextSeq, SourceFileId: sourceFileID}

		if len(msgs) == 1 && len(msgs[0].Payload) > p.Opts.MaxBatchBytes {
			// Singleton oversized payload (camera/point-cloud): NONE path, the one
			// Message rides in the legacy `messages` field with per-message encoding.
			m := msgs[0]
			if p.Opts.CompressThresholdBytes > 0 && len(m.Payload) >= p.Opts.CompressThresholdBytes {
				m.Payload = enc.EncodeAll(m.Payload, nil)
				m.PayloadEncoding = pb.PayloadEncoding_PAYLOAD_ENCODING_ZSTD
			}
			batch.BodyEncoding = pb.BodyEncoding_BODY_ENCODING_NONE
			batch.Messages = []*pb.Message{m}
		} else {
			// Default path: marshal the surviving messages as MessageBatchBody and
			// compress the WHOLE body as ONE self-contained ZSTD frame. Inner
			// payload_encoding stays RAW. One-shot EncodeAll; no cross-flush state.
			rawBody, mErr := proto.Marshal(&pb.MessageBatchBody{Messages: msgs})
			if mErr != nil {
				return fmt.Errorf("session: marshal batch body: %w", mErr)
			}
			batch.BodyEncoding = pb.BodyEncoding_BODY_ENCODING_ZSTD
			batch.BodyUncompressedSize = uint64(len(rawBody))
			batch.Body = enc.EncodeAll(rawBody, nil)
		}

		payload, mErr := proto.Marshal(batch)
		if mErr != nil {
			return fmt.Errorf("session: marshal batch: %w", mErr)
		}
		// Record the ledger (the resume watermark) BEFORE making the batch
		// visible: Retain.Append wakes a parked consumer, which can deliver this
		// seq to the client before we return. If the ledger lagged (OnAppend
		// after Append), a drop in that window would let openResume reject a
		// VALID cursor N as "beyond watermark N-1". Ledger-first makes
		// HighestAppendedSeq always >= any seq a consumer could have delivered.
		if p.OnAppend != nil {
			p.OnAppend(batch.Seq, uint64(len(msgs)), uint64(len(payload)))
		}
		p.Retain.Append(BatchEnvelope{
			Seq:          batch.Seq,
			SourceFileID: sourceFileID,
			Bytes:        int64(len(payload)),
			Messages:     uint64(len(msgs)),
			Payload:      payload,
		})
		return nil
	}

	// emit re-frames one RawMessage into the in-progress batch (flush-on-full /
	// per-file boundary handled by the caller). Shared by the seed phase and the
	// main window loop so both honor the same batching + binding contract.
	emit := func(m RawMessage, sourceFileID uint64) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if p.Opts.MaxMessageBytes > 0 && len(m.Payload) > p.Opts.MaxMessageBytes {
			if p.OnDrop != nil {
				p.OnDrop("oversized")
			}
			return nil
		}
		topicID, ok := p.TopicID[m.Topic]
		if !ok {
			return nil // not in this session's binding (plan shouldn't surface it)
		}
		pbMsg := &pb.Message{
			TopicId:         topicID,
			SchemaId:        p.SchemaID[m.Topic],
			LogTimeNs:       m.LogTimeNs,
			PublishTimeNs:   m.PublishTimeNs,
			PayloadEncoding: pb.PayloadEncoding_PAYLOAD_ENCODING_RAW,
			Payload:         m.Payload,
		}
		size := protoMsgSize(pbMsg)
		// Flush BEFORE adding if this message would push the batch past target
		// (unless empty — an oversized singleton must be allowed to form).
		if !bb.empty() && bb.bytes()+size > p.Opts.MaxBatchBytes {
			if err := flush(sourceFileID); err != nil {
				return err
			}
		}
		bb.add(pbMsg, size)
		if bb.bytes() >= p.Opts.MaxBatchBytes || bb.ageExceeded() {
			if err := flush(sourceFileID); err != nil {
				return err
			}
		}
		return nil
	}

	// SEED PHASE (latched / transient-local replay, OpenFresh.include_latched).
	// For each seed topic, scan its pre-window chunk(s) for the NEWEST message
	// with log_time < SeedBeforeNs and emit those (in log_time order) BEFORE the
	// window stream — so a map/costmap/static pose published once at the start is
	// present even when the window opens later. Seed chunks lie entirely before
	// the window (BuildPlan guarantees it), so they never overlap p.Plan.Chunks.
	if len(p.Plan.SeedChunks) > 0 {
		last := make(map[string]RawMessage, len(p.Plan.SeedTopics))
		lastFile := make(map[string]uint64, len(p.Plan.SeedTopics))
		seedWin := &TimeWindow{StartNs: math.MinInt64, EndNs: p.Plan.SeedBeforeNs}
		for _, ref := range p.Plan.SeedChunks {
			chunkBytes, err := p.ChunkReader.ReadChunk(ctx, ref)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("session: read seed chunk f=%d off=%d: %w", ref.FileID, ref.Offset, err)
			}
			p.fetchedBytes.Add(uint64(len(chunkBytes)))
			iterErr := p.ChunkIter.Iterate(chunkBytes, ref, seedWin, func(m RawMessage) error {
				if _, ok := p.Plan.SeedTopics[m.Topic]; !ok {
					return nil
				}
				if cur, ok := last[m.Topic]; !ok || m.LogTimeNs > cur.LogTimeNs {
					cp := m
					cp.Payload = append([]byte(nil), m.Payload...) // chunk buffer is reused
					last[m.Topic] = cp
					lastFile[m.Topic] = ref.FileID
				}
				return nil
			})
			if iterErr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("session: iterate seed chunk f=%d off=%d: %w", ref.FileID, ref.Offset, iterErr)
			}
		}
		seeded := make([]RawMessage, 0, len(last))
		for _, m := range last {
			seeded = append(seeded, m)
		}
		sort.Slice(seeded, func(i, j int) bool { return seeded[i].LogTimeNs < seeded[j].LogTimeNs })
		// Emit in log_time order; flush at file boundaries (a batch is per-file,
		// and time-disjoint files sort into contiguous runs).
		var curFile uint64
		started := false
		for _, m := range seeded {
			f := lastFile[m.Topic]
			if started && f != curFile {
				if err := flush(curFile); err != nil {
					return err
				}
			}
			curFile, started = f, true
			if err := emit(m, f); err != nil {
				return err
			}
		}
		if started {
			if err := flush(curFile); err != nil {
				return err
			}
		}
	}

	// Chunk fetches are PIPELINED: a bounded window of GETs runs concurrently
	// ahead of the decode/append consumer, but results are consumed strictly in
	// plan order. Over WAN this is the difference between paying one full GET
	// round trip per chunk back-to-back (a 700MB / ~1500-chunk session crawls
	// for tens of minutes) and keeping the link saturated. The window also
	// bounds memory: at most kChunkPrefetch fetched-but-unconsumed chunk blobs
	// exist, and the retain buffer's Append backpressure still stalls the
	// consumer (and therefore the dispatcher) exactly as before.
	type fetchedChunk struct {
		bytes []byte
		err   error
		// A panic inside ReadChunk is captured here and RE-RAISED on the
		// producer goroutine when its slot is consumed, so spec §8.1's panic
		// Guard (which wraps the producer goroutine, not these helpers) keeps
		// counting + containing it exactly as it did when fetches were inline.
		panicVal any
	}
	fctx, fcancel := context.WithCancel(ctx)
	defer fcancel() // aborting mid-plan stops outstanding GETs
	slots := make(chan chan fetchedChunk, kChunkPrefetch)
	go func() {
		defer close(slots)
		for _, ref := range p.Plan.Chunks {
			slot := make(chan fetchedChunk, 1)
			select {
			case slots <- slot: // blocks while the window is full
			case <-fctx.Done():
				return
			}
			go func(ref ChunkRef, slot chan fetchedChunk) {
				var fc fetchedChunk
				func() {
					defer func() { fc.panicVal = recover() }()
					fc.bytes, fc.err = p.ChunkReader.ReadChunk(fctx, ref)
				}()
				slot <- fc // buffered: never blocks
			}(ref, slot)
		}
	}()

	chunkIdx := 0
	for slot := range slots {
		ref := p.Plan.Chunks[chunkIdx]
		chunkIdx++
		fc := <-slot
		if fc.panicVal != nil {
			panic(fc.panicVal)
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if fc.err != nil {
			return fmt.Errorf("session: read chunk f=%d off=%d: %w", ref.FileID, ref.Offset, fc.err)
		}
		chunkBytes := fc.bytes
		// Count the actual bytes fetched for the stream (the producer's fetch
		// budget vs. the pre-flight estimated_chunk_bytes). Use the bytes returned
		// (not ref.Length) so any short/over read shows up in the 5% gate.
		p.fetchedBytes.Add(uint64(len(chunkBytes)))
		iterErr := p.ChunkIter.Iterate(chunkBytes, ref, p.Opts.TimeRange, func(m RawMessage) error {
			return emit(m, ref.FileID)
		})
		if iterErr != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("session: iterate chunk f=%d off=%d: %w", ref.FileID, ref.Offset, iterErr)
		}
		// Flush per-file boundary so a batch never spans two source files
		// (source_file_id is per-batch).
		if err := flush(ref.FileID); err != nil {
			return err
		}
	}
	return nil
}

// batchBuilder accumulates Message pointers + an approximate byte total + the
// first-message timestamp (for age-based flush).
type batchBuilder struct {
	msgs       []*pb.Message
	totalBytes int
	startedAt  time.Time
	maxBytes   int
	maxAge     time.Duration
}

func newBatchBuilder(maxBytes int, maxAge time.Duration) *batchBuilder {
	return &batchBuilder{maxBytes: maxBytes, maxAge: maxAge}
}

func (b *batchBuilder) empty() bool { return len(b.msgs) == 0 }
func (b *batchBuilder) bytes() int  { return b.totalBytes }
func (b *batchBuilder) ageExceeded() bool {
	return b.maxAge > 0 && !b.startedAt.IsZero() && time.Since(b.startedAt) >= b.maxAge
}
func (b *batchBuilder) add(m *pb.Message, size int) {
	if len(b.msgs) == 0 {
		b.startedAt = time.Now()
	}
	b.msgs = append(b.msgs, m)
	b.totalBytes += size
}
func (b *batchBuilder) takeMessages() []*pb.Message {
	out := b.msgs
	b.msgs = nil
	b.totalBytes = 0
	b.startedAt = time.Time{}
	return out
}

// protoMsgSize approximates the wire size of one Message for batch-target
// accounting: payload + a fixed tag/varint overhead estimate. The retain
// buffer's MaxBytes is the authoritative cap; this only governs flush cadence.
func protoMsgSize(m *pb.Message) int { return len(m.Payload) + 32 }
