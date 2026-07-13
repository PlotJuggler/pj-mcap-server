package session

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	gomcap "github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/storage"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// memStore is a hermetic in-memory storage.BlobStore over the real testdata MCAP
// bytes. It is also a RangeGetter (GetRange) so the production blobChunkReader
// works against it with no network / Minio.
type memStore struct{ data map[string][]byte }

func (m memStore) GetRange(_ context.Context, key string, off, length int64) ([]byte, error) {
	d := m.data[key]
	if off >= int64(len(d)) {
		return nil, nil
	}
	end := off + length
	if length <= 0 || end > int64(len(d)) {
		end = int64(len(d))
	}
	out := make([]byte, end-off)
	copy(out, d[off:end])
	return out, nil
}
func (m memStore) Head(_ context.Context, key string) (storage.ObjectInfo, error) {
	return storage.ObjectInfo{Key: key, ETag: "mem", Size: int64(len(m.data[key]))}, nil
}
func (m memStore) List(context.Context, string, string) ([]storage.ObjectInfo, string, error) {
	return nil, "", nil
}

const zegRelPath = "../format/testdata/nissan_zala_50_zeg_1_0.mcap"

func loadZeg(t *testing.T) (memStore, FileChunkIndex, []TopicSchemaInfo) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Clean(zegRelPath))
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	key := "zeg.mcap"
	store := memStore{data: map[string][]byte{key: raw}}
	codec, err := format.NewCodec("mcap")
	if err != nil {
		t.Fatal(err)
	}
	idx, err := codec.ChunkIndex(context.Background(), store, key, 1)
	if err != nil {
		t.Fatalf("ChunkIndex: %v", err)
	}
	return store, idx, idx.Schemas
}

// bindings assigns topic + schema ids the way the WS handler will (used to wire
// the producer + to decode batches back to topic names in the test).
func bindings(schemas []TopicSchemaInfo) (topicID, schemaID map[string]uint32, topicName map[uint32]string) {
	topicID = map[string]uint32{}
	schemaID = map[string]uint32{}
	topicName = map[uint32]string{}
	var nt, ns uint32 = 1, 1
	for _, s := range schemas {
		topicID[s.TopicName] = nt
		topicName[nt] = s.TopicName
		schemaID[s.TopicName] = ns
		nt++
		ns++
	}
	return
}

// drainAll runs producer+consumer to completion over a fresh fakeWriter that
// decodes each batch (NONE and ZSTD paths) and tallies per-topic counts +
// captures the first message. Returns (perTopic, total, firstMsg).
type tally struct {
	perTopic map[string]int
	total    int
	first    *pb.Message
	firstSet bool
}

// decodeWriter records every batch's messages into a tally, asserting each
// non-singleton body is one standalone ZSTD frame with the right
// body_uncompressed_size.
type decodeWriter struct {
	t       *testing.T
	mu      sync.Mutex
	dec     *zstd.Decoder
	topicNm map[uint32]string
	tally   tally
	eos     *pb.Eos
	lastSeq uint64
	seqGaps bool
}

func (w *decodeWriter) SendPriority(m *pb.ServerMessage) bool {
	if e := m.GetEos(); e != nil {
		w.eos = e
	}
	return true
}
func (w *decodeWriter) SendBulk(m *pb.ServerMessage) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	b := m.GetBatch()
	if b == nil {
		return true
	}
	if w.lastSeq != 0 && b.Seq != w.lastSeq+1 {
		w.seqGaps = true
	}
	w.lastSeq = b.Seq

	var msgs []*pb.Message
	switch b.BodyEncoding {
	case pb.BodyEncoding_BODY_ENCODING_ZSTD:
		raw, err := w.dec.DecodeAll(b.Body, nil)
		if err != nil {
			w.t.Fatalf("batch %d body must decompress standalone: %v", b.Seq, err)
		}
		if uint64(len(raw)) != b.BodyUncompressedSize {
			w.t.Errorf("batch %d body_uncompressed_size: got %d want %d", b.Seq, b.BodyUncompressedSize, len(raw))
		}
		var body pb.MessageBatchBody
		if err := proto.Unmarshal(raw, &body); err != nil {
			w.t.Fatalf("batch %d unmarshal body: %v", b.Seq, err)
		}
		msgs = body.Messages
	case pb.BodyEncoding_BODY_ENCODING_NONE:
		msgs = b.Messages
	default:
		w.t.Fatalf("batch %d unknown body_encoding %v", b.Seq, b.BodyEncoding)
	}
	for _, mm := range msgs {
		w.tally.perTopic[w.topicNm[mm.TopicId]]++
		w.tally.total++
		if !w.tally.firstSet {
			w.tally.first = mm
			w.tally.firstSet = true
		}
	}
	return true
}

func runE2E(t *testing.T, topics []string) (*decodeWriter, *Plan) {
	t.Helper()
	store, idx, schemas := loadZeg(t)
	topicID, schemaID, topicName := bindings(schemas)

	plan, err := BuildPlan(context.Background(),
		[]FileRecord{{ID: 1, StartTimeNs: idx.Chunks[0].StartNs, EndTimeNs: idx.Chunks[len(idx.Chunks)-1].EndNs}},
		[]FileChunkIndex{idx},
		PlanArgs{TopicNames: topics})
	if err != nil {
		t.Fatal(err)
	}

	rb := NewRetainBuffer(RetainOpts{MaxSeqs: 256, MaxBytes: 64 << 20})
	prod := &Producer{
		Plan:        plan,
		ChunkReader: NewChunkReader(store, FileKeys{1: "zeg.mcap"}),
		ChunkIter:   NewChunkIterator([]FileChunkIndex{idx}),
		Retain:      rb,
		Opts:        ProducerOpts{MaxBatchBytes: 512 << 10, MaxBatchAge: 50 * time.Millisecond, MaxMessageBytes: 16 << 20, BodyZstdLevel: 3, CompressThresholdBytes: 4096},
		TopicID:     topicID,
		SchemaID:    schemaID,
	}
	dec, _ := zstd.NewReader(nil)
	defer dec.Close()
	w := &decodeWriter{t: t, dec: dec, topicNm: topicName, tally: tally{perTopic: map[string]int{}}}

	producerDone := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() {
		if err := prod.Run(ctx); err != nil {
			t.Errorf("producer: %v", err)
		}
		close(producerDone)
	}()
	cons := &Consumer{
		SubscriptionID: 1, Writer: w, Retain: rb,
		ProducerDone: producerDone, AckCh: make(chan uint64, 8),
		ProgressEvery: time.Hour,
	}
	cons.Run(ctx)

	if w.seqGaps {
		t.Error("seq numbers had gaps / were not strictly monotonic")
	}
	if w.eos == nil || w.eos.GetReason() != pb.EosReason_EOS_REASON_COMPLETE {
		t.Errorf("expected Eos{COMPLETE}, got %+v", w.eos)
	}
	return w, &plan
}

func TestE2E_AllTopics_ExactlyAllMessages(t *testing.T) {
	w, plan := runE2E(t, nil)
	if w.tally.total != 33670 {
		t.Errorf("end-to-end total messages: got %d want 33670", w.tally.total)
	}
	if w.eos.GetTotalMessagesSent() != 33670 {
		t.Errorf("Eos.total_messages_sent: got %d want 33670", w.eos.GetTotalMessagesSent())
	}
	if w.tally.perTopic["/nissan/gps/duro/imu"] != 14904 {
		t.Errorf("imu: got %d want 14904", w.tally.perTopic["/nissan/gps/duro/imu"])
	}
	if w.tally.perTopic["/nissan/vehicle_speed"] != 4513 {
		t.Errorf("vehicle_speed: got %d want 4513", w.tally.perTopic["/nissan/vehicle_speed"])
	}
	if plan.ApproximateMessages != 33670 {
		t.Errorf("plan.ApproximateMessages: got %d want 33670", plan.ApproximateMessages)
	}

	// Spot-check the first emitted message against an mcap-go direct read.
	raw, _ := os.ReadFile(filepath.Clean(zegRelPath))
	r, _ := gomcap.NewReader(bytes.NewReader(raw))
	defer r.Close()
	it, _ := r.Messages()
	_, _, dm, _ := it.NextInto(nil)
	if w.tally.first.LogTimeNs != int64(dm.LogTime) {
		t.Errorf("first message log_time: got %d want %d", w.tally.first.LogTimeNs, dm.LogTime)
	}
	if !bytes.Equal(w.tally.first.Payload, dm.Data) {
		t.Errorf("first message payload mismatch")
	}
}

func TestE2E_TopicSubset_ExactlyVehicleSpeed(t *testing.T) {
	w, plan := runE2E(t, []string{"/nissan/vehicle_speed"})
	if w.tally.total != 4513 {
		t.Errorf("vehicle_speed end-to-end total: got %d want 4513", w.tally.total)
	}
	for topic := range w.tally.perTopic {
		if topic != "/nissan/vehicle_speed" {
			t.Errorf("subset stream leaked topic %q", topic)
		}
	}
	if plan.ApproximateMessages != 4513 {
		t.Errorf("plan.ApproximateMessages for subset: got %d want 4513", plan.ApproximateMessages)
	}
}
