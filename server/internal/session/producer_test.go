package session

import (
	"context"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// fakeReader serves canned bytes per chunk ref (content is irrelevant — the
// fakeIter ignores it and returns the messages keyed by ref offset).
type fakeReader struct{}

func (fakeReader) ReadChunk(_ context.Context, ref ChunkRef) ([]byte, error) {
	return []byte{byte(ref.Offset)}, nil
}

// fakeIter returns the messages registered for a chunk ref's offset, honoring
// the ref's topic filter + the optional time range (so producer tests exercise
// the same emit contract the real codec uses).
type fakeIter struct {
	byOffset map[int64][]RawMessage
}

func (f fakeIter) Iterate(_ []byte, ref ChunkRef, tr *TimeWindow, emit func(RawMessage) error) error {
	for _, m := range f.byOffset[ref.Offset] {
		if _, want := ref.ChannelTopics[m.Topic]; !want {
			continue
		}
		if tr != nil && (m.LogTimeNs < tr.StartNs || m.LogTimeNs >= tr.EndNs) {
			continue
		}
		if err := emit(m); err != nil {
			return err
		}
	}
	return nil
}

func msg(topic string, logTime int64, payload string) RawMessage {
	return RawMessage{Topic: topic, LogTimeNs: logTime, PublishTimeNs: logTime, Payload: []byte(payload)}
}

// decodeBatchBody decompresses + unmarshals a ZSTD-body MessageBatch's messages.
func decodeBatchBody(t *testing.T, b *pb.MessageBatch) []*pb.Message {
	t.Helper()
	if b.BodyEncoding != pb.BodyEncoding_BODY_ENCODING_ZSTD {
		t.Fatalf("expected ZSTD body, got %v", b.BodyEncoding)
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	raw, err := dec.DecodeAll(b.Body, nil)
	if err != nil {
		t.Fatalf("body must decompress standalone as one ZSTD frame: %v", err)
	}
	if uint64(len(raw)) != b.BodyUncompressedSize {
		t.Errorf("body_uncompressed_size: got %d want %d", b.BodyUncompressedSize, len(raw))
	}
	var body pb.MessageBatchBody
	if err := proto.Unmarshal(raw, &body); err != nil {
		t.Fatalf("unmarshal MessageBatchBody: %v", err)
	}
	return body.Messages
}

func runProducer(t *testing.T, p *Producer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := p.Run(ctx); err != nil {
		t.Fatalf("producer Run: %v", err)
	}
}

func TestProducer_DefaultPathZstdBodyOneShot(t *testing.T) {
	plan := Plan{Chunks: []ChunkRef{chunk(1, 0, 100, 0, 100, "/x")}}
	iter := fakeIter{byOffset: map[int64][]RawMessage{
		0: {msg("/x", 1, "aaa"), msg("/x", 2, "bbb")},
	}}
	rb := NewRetainBuffer(RetainOpts{MaxSeqs: 8, MaxBytes: 1 << 20})
	p := &Producer{
		Plan: plan, ChunkReader: fakeReader{}, ChunkIter: iter, Retain: rb,
		Opts:     ProducerOpts{MaxBatchBytes: 1 << 16, MaxMessageBytes: 1 << 20, CompressThresholdBytes: 4096},
		TopicID:  map[string]uint32{"/x": 7},
		SchemaID: map[string]uint32{"/x": 3},
	}
	runProducer(t, p)

	env, ok := rb.Next(context.Background(), 0)
	if !ok || env.Seq != 1 {
		t.Fatalf("first batch: ok=%v seq=%d", ok, env.Seq)
	}
	if env.Messages != 2 {
		t.Errorf("envelope message count: got %d want 2", env.Messages)
	}
	var b pb.MessageBatch
	if err := proto.Unmarshal(env.Payload, &b); err != nil {
		t.Fatal(err)
	}
	if b.SourceFileId != 1 {
		t.Errorf("source_file_id: %d want 1", b.SourceFileId)
	}
	if len(b.Messages) != 0 {
		t.Errorf("default path must leave legacy `messages` empty, got %d", len(b.Messages))
	}
	msgs := decodeBatchBody(t, &b)
	if len(msgs) != 2 {
		t.Fatalf("decoded body messages: got %d want 2", len(msgs))
	}
	if msgs[0].TopicId != 7 || msgs[0].SchemaId != 3 {
		t.Errorf("binding ids: topic=%d schema=%d want 7/3", msgs[0].TopicId, msgs[0].SchemaId)
	}
	if msgs[0].PayloadEncoding != pb.PayloadEncoding_PAYLOAD_ENCODING_RAW {
		t.Errorf("inner payload_encoding must stay RAW on the batched path, got %v", msgs[0].PayloadEncoding)
	}
	if string(msgs[0].Payload) != "aaa" || string(msgs[1].Payload) != "bbb" {
		t.Errorf("payload round-trip mismatch: %q %q", msgs[0].Payload, msgs[1].Payload)
	}
}

func TestProducer_SeqStrictlyMonotonic(t *testing.T) {
	// Tiny MaxBatchBytes so each message forms its own batch -> several seqs.
	plan := Plan{Chunks: []ChunkRef{chunk(1, 0, 100, 0, 100, "/x")}}
	iter := fakeIter{byOffset: map[int64][]RawMessage{
		0: {msg("/x", 1, "aaaa"), msg("/x", 2, "bbbb"), msg("/x", 3, "cccc")},
	}}
	rb := NewRetainBuffer(RetainOpts{MaxSeqs: 64, MaxBytes: 1 << 20})
	p := &Producer{
		Plan: plan, ChunkReader: fakeReader{}, ChunkIter: iter, Retain: rb,
		Opts:     ProducerOpts{MaxBatchBytes: 1, MaxMessageBytes: 1 << 20, CompressThresholdBytes: 4096},
		TopicID:  map[string]uint32{"/x": 1},
		SchemaID: map[string]uint32{"/x": 1},
	}
	runProducer(t, p)
	if rb.Len() != 3 {
		t.Fatalf("expected 3 single-message batches, got %d", rb.Len())
	}
	var last uint64
	for got := uint64(0); ; {
		env, ok := rb.Next(context.Background(), got)
		if !ok {
			break
		}
		if env.Seq != last+1 {
			t.Errorf("seq not strictly monotonic: got %d after %d", env.Seq, last)
		}
		last = env.Seq
		got = env.Seq
		if last == 3 {
			break
		}
	}
}

func TestProducer_OversizedSingletonFallbackNonePath(t *testing.T) {
	big := make([]byte, 200) // > MaxBatchBytes(100) -> singleton NONE path
	for i := range big {
		big[i] = byte('z')
	}
	plan := Plan{Chunks: []ChunkRef{chunk(1, 0, 100, 0, 100, "/x")}}
	iter := fakeIter{byOffset: map[int64][]RawMessage{
		0: {{Topic: "/x", LogTimeNs: 1, Payload: big}},
	}}
	rb := NewRetainBuffer(RetainOpts{MaxSeqs: 8, MaxBytes: 1 << 20})
	p := &Producer{
		Plan: plan, ChunkReader: fakeReader{}, ChunkIter: iter, Retain: rb,
		Opts:     ProducerOpts{MaxBatchBytes: 100, MaxMessageBytes: 1 << 20, CompressThresholdBytes: 4096},
		TopicID:  map[string]uint32{"/x": 1},
		SchemaID: map[string]uint32{"/x": 1},
	}
	runProducer(t, p)
	env, _ := rb.Next(context.Background(), 0)
	var b pb.MessageBatch
	if err := proto.Unmarshal(env.Payload, &b); err != nil {
		t.Fatal(err)
	}
	if b.BodyEncoding != pb.BodyEncoding_BODY_ENCODING_NONE {
		t.Errorf("oversized singleton must use NONE body, got %v", b.BodyEncoding)
	}
	if len(b.Messages) != 1 {
		t.Fatalf("NONE path carries the message in the legacy field, got %d", len(b.Messages))
	}
	if len(b.Body) != 0 {
		t.Error("NONE path must not set body")
	}
	// 200-byte payload >= 4 KiB threshold? No -> stays RAW.
	if b.Messages[0].PayloadEncoding != pb.PayloadEncoding_PAYLOAD_ENCODING_RAW {
		t.Errorf("payload below compress threshold should stay RAW, got %v", b.Messages[0].PayloadEncoding)
	}
}

func TestProducer_OversizedSingletonCompressedAboveThreshold(t *testing.T) {
	big := make([]byte, 8192) // > MaxBatchBytes(100) AND >= CompressThreshold(4096)
	plan := Plan{Chunks: []ChunkRef{chunk(1, 0, 100, 0, 100, "/x")}}
	iter := fakeIter{byOffset: map[int64][]RawMessage{0: {{Topic: "/x", LogTimeNs: 1, Payload: big}}}}
	rb := NewRetainBuffer(RetainOpts{MaxSeqs: 8, MaxBytes: 1 << 20})
	p := &Producer{
		Plan: plan, ChunkReader: fakeReader{}, ChunkIter: iter, Retain: rb,
		Opts:     ProducerOpts{MaxBatchBytes: 100, MaxMessageBytes: 1 << 20, CompressThresholdBytes: 4096},
		TopicID:  map[string]uint32{"/x": 1},
		SchemaID: map[string]uint32{"/x": 1},
	}
	runProducer(t, p)
	env, _ := rb.Next(context.Background(), 0)
	var b pb.MessageBatch
	_ = proto.Unmarshal(env.Payload, &b)
	if b.Messages[0].PayloadEncoding != pb.PayloadEncoding_PAYLOAD_ENCODING_ZSTD {
		t.Errorf("payload above threshold should be ZSTD, got %v", b.Messages[0].PayloadEncoding)
	}
}

func TestProducer_DropsOverMaxMessageBytes(t *testing.T) {
	plan := Plan{Chunks: []ChunkRef{chunk(1, 0, 100, 0, 100, "/x")}}
	iter := fakeIter{byOffset: map[int64][]RawMessage{
		0: {msg("/x", 1, "ok"), {Topic: "/x", LogTimeNs: 2, Payload: make([]byte, 5000)}},
	}}
	rb := NewRetainBuffer(RetainOpts{MaxSeqs: 8, MaxBytes: 1 << 20})
	var drops int
	p := &Producer{
		Plan: plan, ChunkReader: fakeReader{}, ChunkIter: iter, Retain: rb,
		Opts:     ProducerOpts{MaxBatchBytes: 1 << 16, MaxMessageBytes: 1024, CompressThresholdBytes: 4096},
		TopicID:  map[string]uint32{"/x": 1},
		SchemaID: map[string]uint32{"/x": 1},
		OnDrop:   func(string) { drops++ },
	}
	runProducer(t, p)
	if drops != 1 {
		t.Errorf("expected 1 drop (over max_message_bytes), got %d", drops)
	}
	env, _ := rb.Next(context.Background(), 0)
	if env.Messages != 1 {
		t.Errorf("surviving batch should carry the 1 in-bounds message, got %d", env.Messages)
	}
}

func TestProducer_BlocksOnRetainCapsUntilDrained(t *testing.T) {
	// Tiny caps (1 seq) + many single-message batches: the producer must block
	// after the first batch until a consumer prunes.
	plan := Plan{Chunks: []ChunkRef{chunk(1, 0, 100, 0, 100, "/x")}}
	var msgs []RawMessage
	for i := 0; i < 5; i++ {
		msgs = append(msgs, msg("/x", int64(i+1), "data"))
	}
	iter := fakeIter{byOffset: map[int64][]RawMessage{0: msgs}}
	rb := NewRetainBuffer(RetainOpts{MaxSeqs: 1, MaxBytes: 1 << 20})
	p := &Producer{
		Plan: plan, ChunkReader: fakeReader{}, ChunkIter: iter, Retain: rb,
		Opts:     ProducerOpts{MaxBatchBytes: 1, MaxMessageBytes: 1 << 20, CompressThresholdBytes: 4096},
		TopicID:  map[string]uint32{"/x": 1},
		SchemaID: map[string]uint32{"/x": 1},
	}
	done := make(chan error, 1)
	go func() { done <- p.Run(context.Background()) }()

	// Producer fills exactly one batch then blocks.
	select {
	case <-done:
		t.Fatal("producer finished without blocking on a 1-seq retain cap")
	case <-time.After(40 * time.Millisecond):
	}
	if rb.Len() != 1 {
		t.Fatalf("expected exactly 1 retained while blocked, got %d", rb.Len())
	}
	// Drain in lockstep to release the producer.
	var last uint64
	for i := 0; i < 5; i++ {
		env, ok := rb.Next(context.Background(), last)
		if !ok {
			t.Fatalf("Next returned not-ok at i=%d", i)
		}
		last = env.Seq
		rb.Prune(env.Seq)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("producer Run err: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("producer did not finish after draining")
	}
}

func TestProducer_CancelStopsPromptly(t *testing.T) {
	// Many chunks, each with messages; cancel after the first batch lands. The
	// producer must return ctx.Err() promptly instead of finishing the plan.
	var chunks []ChunkRef
	byOffset := map[int64][]RawMessage{}
	for i := int64(0); i < 50; i++ {
		chunks = append(chunks, chunk(1, i*10, i*10+10, i, 100, "/x"))
		byOffset[i] = []RawMessage{msg("/x", i*10+1, "data")}
	}
	plan := Plan{Chunks: chunks}
	iter := fakeIter{byOffset: byOffset}
	rb := NewRetainBuffer(RetainOpts{MaxSeqs: 1, MaxBytes: 1 << 20})
	ctx, cancel := context.WithCancel(context.Background())
	p := &Producer{
		Plan: plan, ChunkReader: fakeReader{}, ChunkIter: iter, Retain: rb,
		Opts:     ProducerOpts{MaxBatchBytes: 1, MaxMessageBytes: 1 << 20, CompressThresholdBytes: 4096},
		TopicID:  map[string]uint32{"/x": 1},
		SchemaID: map[string]uint32{"/x": 1},
	}
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	// Wait until the first batch lands (producer is now parked at the 1-seq cap).
	deadline := time.After(time.Second)
	for rb.Len() == 0 {
		select {
		case <-deadline:
			t.Fatal("producer never produced its first batch")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	rb.Close() // release the producer parked in Append (mimics registry teardown)
	select {
	case err := <-done:
		if err == nil {
			t.Error("cancelled producer should return ctx.Err(), got nil")
		}
	case <-time.After(time.Second):
		t.Fatal("producer did not stop promptly after cancel")
	}
}
