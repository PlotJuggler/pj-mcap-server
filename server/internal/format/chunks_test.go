package format

import (
	"bytes"
	"context"
	"testing"

	gomcap "github.com/foxglove/mcap/go/mcap"
)

// Ground truth for testdata/nissan_zala_50_zeg_1_0.mcap (verified directly with
// mcap-go): 9 chunks, 33670 messages, uncompressed chunk containers, every chunk
// carries MessageIndex records (so per-chunk per-topic counts are EXACT).
const (
	zegKey       = "nissan_zala_50_zeg_1_0.mcap"
	zegChunks    = 9
	zegTotalMsgs = 33670
	zegSpeedMsgs = 4513 // /nissan/vehicle_speed
	zegImuMsgs   = 14904
)

func newZegIndex(t *testing.T) FileChunkIndex {
	t.Helper()
	bs := fileStore{root: "testdata"}
	codec, err := NewCodec("mcap")
	if err != nil {
		t.Fatalf("NewCodec: %v", err)
	}
	idx, err := codec.ChunkIndex(context.Background(), bs, zegKey, 42)
	if err != nil {
		t.Fatalf("ChunkIndex: %v", err)
	}
	return idx
}

func TestChunkIndex_StructureAndCounts(t *testing.T) {
	idx := newZegIndex(t)

	if idx.FileID != 42 {
		t.Errorf("FileID: got %d want 42", idx.FileID)
	}
	if len(idx.Chunks) != zegChunks {
		t.Fatalf("chunks: got %d want %d", len(idx.Chunks), zegChunks)
	}
	if len(idx.Schemas) != 6 {
		t.Errorf("schemas: got %d want 6 (one binding per topic)", len(idx.Schemas))
	}
	for _, s := range idx.Schemas {
		if s.MessageEncoding != "cdr" {
			t.Errorf("topic %q: message_encoding got %q want cdr", s.TopicName, s.MessageEncoding)
		}
		if s.SchemaEncoding != "ros2msg" {
			t.Errorf("topic %q: schema_encoding got %q want ros2msg", s.TopicName, s.SchemaEncoding)
		}
		if len(s.SchemaData) == 0 {
			t.Errorf("topic %q: schema data empty", s.TopicName)
		}
	}

	// Chunk refs must be offset-ordered and stamped with the file id; the summed
	// MessageCount over all topics must be exact (== file total).
	var total uint64
	var lastOff int64 = -1
	for _, c := range idx.Chunks {
		if c.FileID != 42 {
			t.Errorf("chunk FileID: got %d want 42", c.FileID)
		}
		if c.Offset <= lastOff {
			t.Errorf("chunks not offset-ordered: %d after %d", c.Offset, lastOff)
		}
		lastOff = c.Offset
		if c.EndNs <= c.StartNs {
			t.Errorf("chunk time range non-positive: [%d,%d]", c.StartNs, c.EndNs)
		}
		total += c.MessageCount
	}
	if total != zegTotalMsgs {
		t.Errorf("summed exact MessageCount: got %d want %d", total, zegTotalMsgs)
	}
}

func TestPlanChunks_AllTopics(t *testing.T) {
	idx := newZegIndex(t)
	plan := idx.PlanChunks(nil, nil) // all topics, full horizon
	if len(plan) != zegChunks {
		t.Fatalf("all-topics plan: got %d chunks want %d", len(plan), zegChunks)
	}
	var estBytes, approxMsgs uint64
	for _, c := range plan {
		estBytes += uint64(c.Length)
		approxMsgs += c.MessageCount
		if len(c.ChannelTopics) != 6 {
			t.Errorf("all-topics chunk should keep all 6 topics, got %d", len(c.ChannelTopics))
		}
	}
	if estBytes == 0 {
		t.Error("estimated_chunk_bytes must be > 0 for an all-topics plan")
	}
	if approxMsgs != zegTotalMsgs {
		t.Errorf("approximate_messages: got %d want %d", approxMsgs, zegTotalMsgs)
	}
}

func TestPlanChunks_TopicSubset(t *testing.T) {
	idx := newZegIndex(t)
	plan := idx.PlanChunks([]string{"/nissan/vehicle_speed"}, nil)
	// Every chunk in this file carries vehicle_speed, so all 9 are planned, but
	// each narrowed to the single requested topic with its exact count.
	if len(plan) != zegChunks {
		t.Fatalf("subset plan: got %d chunks want %d", len(plan), zegChunks)
	}
	var approxMsgs uint64
	for _, c := range plan {
		if len(c.ChannelTopics) != 1 {
			t.Errorf("subset chunk should keep exactly 1 topic, got %d", len(c.ChannelTopics))
		}
		if _, ok := c.ChannelTopics["/nissan/vehicle_speed"]; !ok {
			t.Errorf("subset chunk missing requested topic: %+v", c.ChannelTopics)
		}
		approxMsgs += c.MessageCount
	}
	if approxMsgs != zegSpeedMsgs {
		t.Errorf("vehicle_speed approximate_messages: got %d want %d", approxMsgs, zegSpeedMsgs)
	}
}

func TestPlanChunks_AbsentTopicDropped(t *testing.T) {
	idx := newZegIndex(t)
	plan := idx.PlanChunks([]string{"/does/not/exist"}, nil)
	if len(plan) != 0 {
		t.Errorf("absent topic should yield empty plan, got %d chunks", len(plan))
	}
}

func TestPlanChunks_TimeRange(t *testing.T) {
	idx := newZegIndex(t)
	// Intersect a window covering only the first chunk's time span.
	first := idx.Chunks[0]
	tr := &TimeWindow{StartNs: first.StartNs, EndNs: first.EndNs} // half-open, so just chunk[0]
	plan := idx.PlanChunks(nil, tr)
	if len(plan) == 0 {
		t.Fatal("time-range plan unexpectedly empty")
	}
	// Chunk[0] must be included; chunks entirely after the window must not.
	if plan[0].Offset != first.Offset {
		t.Errorf("first planned chunk offset %d != %d", plan[0].Offset, first.Offset)
	}
	for _, c := range plan {
		if c.StartNs >= tr.EndNs || c.EndNs <= tr.StartNs {
			t.Errorf("chunk [%d,%d] does not intersect window [%d,%d]", c.StartNs, c.EndNs, tr.StartNs, tr.EndNs)
		}
	}
}

// TestIterate_AllTopics_RoundTrips iterates every planned chunk and asserts the
// total message count + a spot-check against an mcap-go direct read.
func TestIterate_AllTopics(t *testing.T) {
	idx := newZegIndex(t)
	bs := fileStore{root: "testdata"}
	plan := idx.PlanChunks(nil, nil)

	perTopic := map[string]int{}
	var total int
	var firstPayload []byte
	var firstLogTime int64
	for _, ref := range plan {
		chunkBytes, err := bs.GetRange(context.Background(), zegKey, ref.Offset, ref.Length)
		if err != nil {
			t.Fatalf("GetRange: %v", err)
		}
		err = idx.Iterate(chunkBytes, ref, nil, func(m RawMessage) error {
			perTopic[m.Topic]++
			total++
			if firstPayload == nil {
				firstPayload = m.Payload
				firstLogTime = m.LogTimeNs
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Iterate: %v", err)
		}
	}
	if total != zegTotalMsgs {
		t.Errorf("iterated total: got %d want %d", total, zegTotalMsgs)
	}
	if perTopic["/nissan/vehicle_speed"] != zegSpeedMsgs {
		t.Errorf("vehicle_speed iterated: got %d want %d", perTopic["/nissan/vehicle_speed"], zegSpeedMsgs)
	}
	if perTopic["/nissan/gps/duro/imu"] != zegImuMsgs {
		t.Errorf("imu iterated: got %d want %d", perTopic["/nissan/gps/duro/imu"], zegImuMsgs)
	}

	// Spot-check the very first emitted message against an mcap-go direct read
	// (indexed, file order). The first chunk starts at the file start, so the
	// first message in file order is the first one we emit too.
	want := firstDirectMessage(t)
	if firstLogTime != int64(want.LogTime) {
		t.Errorf("first message log_time: got %d want %d", firstLogTime, want.LogTime)
	}
	if !bytes.Equal(firstPayload, want.Data) {
		t.Errorf("first message payload mismatch (len got=%d want=%d)", len(firstPayload), len(want.Data))
	}
}

func TestIterate_TopicSubset(t *testing.T) {
	idx := newZegIndex(t)
	bs := fileStore{root: "testdata"}
	plan := idx.PlanChunks([]string{"/nissan/vehicle_speed"}, nil)

	var total int
	for _, ref := range plan {
		chunkBytes, err := bs.GetRange(context.Background(), zegKey, ref.Offset, ref.Length)
		if err != nil {
			t.Fatalf("GetRange: %v", err)
		}
		err = idx.Iterate(chunkBytes, ref, nil, func(m RawMessage) error {
			if m.Topic != "/nissan/vehicle_speed" {
				t.Errorf("subset iterate leaked topic %q", m.Topic)
			}
			total++
			return nil
		})
		if err != nil {
			t.Fatalf("Iterate: %v", err)
		}
	}
	if total != zegSpeedMsgs {
		t.Errorf("vehicle_speed iterate total: got %d want %d", total, zegSpeedMsgs)
	}
}

// TestIterate_TimeRange_NoChunkEndDrop guards the time-range filter against a
// regression where Iterate clamped the per-message upper bound to the chunk's
// own MessageEndTime (an INCLUSIVE last-message timestamp) and so dropped the
// last message of every chunk whose end fell strictly below the requested
// window end. The ground truth is an mcap-go direct read filtered with the SAME
// half-open [start,end) window. See design-spec §6.3 (intra-file time window).
func TestIterate_TimeRange_NoChunkEndDrop(t *testing.T) {
	idx := newZegIndex(t)
	bs := fileStore{root: "testdata"}

	// A window that fully contains several interior chunks and ends inside a
	// later chunk — exactly the shape that exposed the chunk-end drop.
	tr := &TimeWindow{
		StartNs: 1696577469299761084,
		EndNs:   1696577514415840735,
	}

	// Ground truth: mcap-go direct read, same half-open window.
	want := directCountInWindow(t, tr)

	plan := idx.PlanChunks(nil, tr)
	got := map[string]int{}
	var total int
	for _, ref := range plan {
		chunkBytes, err := bs.GetRange(context.Background(), zegKey, ref.Offset, ref.Length)
		if err != nil {
			t.Fatalf("GetRange: %v", err)
		}
		err = idx.Iterate(chunkBytes, ref, tr, func(m RawMessage) error {
			if m.LogTimeNs < tr.StartNs || m.LogTimeNs >= tr.EndNs {
				t.Errorf("Iterate leaked out-of-window message lt=%d (window [%d,%d))", m.LogTimeNs, tr.StartNs, tr.EndNs)
			}
			got[m.Topic]++
			total++
			return nil
		})
		if err != nil {
			t.Fatalf("Iterate: %v", err)
		}
	}

	var wantTotal int
	for _, n := range want {
		wantTotal += n
	}
	if total != wantTotal {
		t.Errorf("windowed total: got %d want %d (over/under-delivery in time-range filter)", total, wantTotal)
	}
	for topic, n := range want {
		if got[topic] != n {
			t.Errorf("windowed topic %q: got %d want %d", topic, got[topic], n)
		}
	}
}

// directCountInWindow reads the file with mcap-go (indexed) and counts, per
// topic, the messages whose log_time falls in the half-open [tr.StartNs,
// tr.EndNs) window — the time-range ground truth.
func directCountInWindow(t *testing.T, tr *TimeWindow) map[string]int {
	t.Helper()
	bs := fileStore{root: "testdata"}
	head, err := bs.Head(context.Background(), zegKey)
	if err != nil {
		t.Fatal(err)
	}
	data, err := bs.GetRange(context.Background(), zegKey, 0, head.Size)
	if err != nil {
		t.Fatal(err)
	}
	r, err := gomcap.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	it, err := r.Messages()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int{}
	for {
		_, ch, msg, err := it.NextInto(nil)
		if err != nil || msg == nil {
			break
		}
		lt := int64(msg.LogTime)
		if lt < tr.StartNs || lt >= tr.EndNs {
			continue
		}
		out[ch.Topic]++
	}
	return out
}

// firstDirectMessage reads the file with mcap-go (indexed, file order) and
// returns the first message — the byte-level ground-truth anchor.
func firstDirectMessage(t *testing.T) *gomcap.Message {
	t.Helper()
	bs := fileStore{root: "testdata"}
	head, err := bs.Head(context.Background(), zegKey)
	if err != nil {
		t.Fatal(err)
	}
	data, err := bs.GetRange(context.Background(), zegKey, 0, head.Size)
	if err != nil {
		t.Fatal(err)
	}
	r, err := gomcap.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	it, err := r.Messages()
	if err != nil {
		t.Fatal(err)
	}
	_, _, msg, err := it.NextInto(nil)
	if err != nil || msg == nil {
		t.Fatalf("direct read first message: %v", err)
	}
	return msg
}
