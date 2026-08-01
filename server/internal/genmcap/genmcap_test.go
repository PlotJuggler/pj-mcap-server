package genmcap

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/foxglove/mcap/go/mcap"
	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/storage"
)

// memBlob is a tiny in-memory BlobStore over a single key, enough to drive
// format.Codec.Extract (Head + ranged GetRange) hermetically.
type memBlob struct {
	key  string
	data []byte
}

func (m memBlob) GetRange(_ context.Context, key string, off, length int64) ([]byte, error) {
	if key != m.key {
		return nil, storage.ErrPermanent
	}
	if off >= int64(len(m.data)) {
		return nil, nil
	}
	end := off + length
	if length <= 0 || end > int64(len(m.data)) {
		end = int64(len(m.data))
	}
	out := make([]byte, end-off)
	copy(out, m.data[off:end])
	return out, nil
}

func (m memBlob) Head(_ context.Context, key string) (storage.ObjectInfo, error) {
	if key != m.key {
		return storage.ObjectInfo{}, storage.ErrPermanent
	}
	return storage.ObjectInfo{Key: key, Size: int64(len(m.data))}, nil
}

func (m memBlob) List(_ context.Context, _, _ string) ([]storage.ObjectInfo, string, error) {
	return []storage.ObjectInfo{{Key: m.key, Size: int64(len(m.data))}}, "", nil
}

// TestFixturesExtractCleanly proves every DefaultSpecs fixture is a valid
// chunked+summarized MCAP the production codec accepts (the format.go:91
// statistics requirement) AND that the codec-reported counts/time-range match
// the spec. This is the hermetic guard the CI integration legs lean on: if the
// generator ever produces a fixture the codec rejects, this fails in the plain
// (no-docker) suite, long before the integration leg.
func TestFixturesExtractCleanly(t *testing.T) {
	codec, err := format.NewCodec("mcap")
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range DefaultSpecs() {
		spec := spec
		t.Run(spec.Key, func(t *testing.T) {
			var buf bytes.Buffer
			if err := Write(&buf, spec); err != nil {
				t.Fatalf("Write: %v", err)
			}
			bs := memBlob{key: spec.Key, data: buf.Bytes()}
			fs, err := codec.Extract(context.Background(), bs, spec.Key)
			if err != nil {
				t.Fatalf("Extract (codec must accept a chunked+summarized fixture): %v", err)
			}
			if fs.MessageCount != spec.TotalMessages() {
				t.Errorf("message_count: got %d want %d", fs.MessageCount, spec.TotalMessages())
			}
			if fs.StartNs != spec.StartNs {
				t.Errorf("start_ns: got %d want %d", fs.StartNs, spec.StartNs)
			}
			if fs.EndNs != spec.EndNs() {
				t.Errorf("end_ns: got %d want %d", fs.EndNs, spec.EndNs())
			}
			if int(fs.TopicCount) != len(spec.Topics) {
				t.Errorf("topic_count: got %d want %d", fs.TopicCount, len(spec.Topics))
			}
			if fs.ChunkCount == 0 {
				t.Error("chunk_count must be > 0 (fixture must be chunked)")
			}
			// Per-topic counts.
			gotPerTopic := map[string]uint64{}
			for _, tp := range fs.Topics {
				gotPerTopic[tp.Name] = tp.MessageCount
			}
			for _, ts := range spec.Topics {
				if gotPerTopic[ts.Topic] != uint64(ts.MessageCount) {
					t.Errorf("topic %s count: got %d want %d", ts.Topic, gotPerTopic[ts.Topic], ts.MessageCount)
				}
			}
		})
	}
}

// TestDeterministic proves byte-identity across two writes of the same spec —
// the property the change-detect / warm-start (0 re-extract) assertion needs.
func TestDeterministic(t *testing.T) {
	spec := DefaultSpecs()[0]
	var a, b bytes.Buffer
	if err := Write(&a, spec); err != nil {
		t.Fatal(err)
	}
	if err := Write(&b, spec); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatalf("non-deterministic output: %d vs %d bytes differ", a.Len(), b.Len())
	}
}

// iterateAll drives the streaming half (ChunkIndex -> PlanChunks -> Iterate) over
// an in-memory fixture, returning the total messages iterated or the first
// decode error. It is the path the session subsystem uses — the one that
// actually decompresses chunk bodies (Extract is summary-only and never does).
func iterateAll(t *testing.T, key string, data []byte) (int, error) {
	t.Helper()
	codec, err := format.NewCodec("mcap")
	if err != nil {
		t.Fatal(err)
	}
	bs := memBlob{key: key, data: data}
	idx, err := codec.ChunkIndex(context.Background(), bs, key, 1)
	if err != nil {
		return 0, err
	}
	total := 0
	for _, ref := range idx.PlanChunks(nil, nil) {
		chunkBytes, gErr := bs.GetRange(context.Background(), key, ref.Offset, ref.Length)
		if gErr != nil {
			return total, gErr
		}
		iErr := idx.Iterate(chunkBytes, ref, nil, func(format.RawMessage) error {
			total++
			return nil
		})
		if iErr != nil {
			return total, iErr
		}
	}
	return total, nil
}

// TestCorruptChunk_Rejected is the C2 integrity gate. It proves CorruptChunkBody
// produces a fixture the format codec REJECTS when it decodes the corrupted
// chunk. CRITICAL FINDING (recorded in CorruptChunkBody's doc): the codec does
// NOT verify a chunk's UncompressedCRC — so the rejection comes from the ZSTD
// DECODE failing on the flipped compressed bytes, surfaced as the
// "format: zstd decode chunk" error from chunks.go's Iterate. The valid fixture
// must iterate cleanly first (the corruption is what breaks it, nothing else).
func TestCorruptChunk_Rejected(t *testing.T) {
	// Both COMPRESSED codecs are integrity surfaces: a flipped byte in the
	// compressed Records blob fails the decode (the codec does not CRC-check). LZ4
	// is decoded as a frame (chunks.go lz4.NewReader), so a corrupted LZ4 frame
	// still fails decode after the frame fix — verified here alongside ZSTD.
	cases := []struct {
		name string
		comp Compression
	}{
		{"zstd", CompressionZSTD},
		{"lz4", CompressionLZ4},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			spec := FileSpec{
				Key:     "corrupt_src_" + tc.name + ".mcap",
				StartNs: 5000 * 1_000_000_000,
				StepNs:  10 * 1_000_000,
				// A single fat chunk so the flip lands inside one compressed Records
				// blob (the codec does not CRC-check; only the decode can fail).
				Compression: tc.comp,
				ChunkSize:   1 << 20,
				Topics: []TopicSpec{
					{Topic: "/clock", SchemaName: "rosgraph_msgs/msg/Clock", SchemaEnc: "ros2msg", MessageCount: 200},
				},
			}
			var buf bytes.Buffer
			if err := Write(&buf, spec); err != nil {
				t.Fatalf("Write: %v", err)
			}
			valid := buf.Bytes()

			// Sanity: the pristine fixture iterates cleanly with the full count.
			gotValid, err := iterateAll(t, spec.Key, valid)
			if err != nil {
				t.Fatalf("valid fixture must iterate cleanly, got: %v", err)
			}
			if gotValid != 200 {
				t.Fatalf("valid fixture iterated %d msgs, want 200", gotValid)
			}

			// Corrupt one byte inside the compressed chunk body, summary left intact.
			bad, err := CorruptChunkBody(valid)
			if err != nil {
				t.Fatalf("CorruptChunkBody: %v", err)
			}
			if bytes.Equal(bad, valid) {
				t.Fatal("CorruptChunkBody returned identical bytes (no flip applied)")
			}

			// ChunkIndex still succeeds — it reads ONLY the (intact) summary section,
			// so a corrupt chunk BODY does not surface there. The codec rejects on DECODE.
			codec, _ := format.NewCodec("mcap")
			if _, ciErr := codec.ChunkIndex(context.Background(), memBlob{key: spec.Key, data: bad}, spec.Key, 1); ciErr != nil {
				t.Fatalf("ChunkIndex should still parse the intact summary, got: %v", ciErr)
			}

			// Iterate MUST reject the corrupted chunk — the actual integrity surface.
			_, decErr := iterateAll(t, spec.Key, bad)
			if decErr == nil {
				t.Fatalf("corrupted %s chunk was accepted — codec failed to reject decode corruption", tc.name)
			}
			if !strings.Contains(decErr.Error(), "decode chunk") {
				t.Errorf("rejection error %q does not mention chunk decode (the verified surface)", decErr.Error())
			}
		})
	}
}

// TestDimensions_RoundTrip proves the new FileSpec dimensions (compression
// variants, large-payload singletons, tiny single-chunk files, embedded tag
// metadata) each produce a codec-valid fixture that Extracts with the expected
// counts/time-range AND iterates to the exact message total. This is the
// hermetic guard for the C2 dimension knobs.
func TestDimensions_RoundTrip(t *testing.T) {
	const sec = 1_000_000_000
	codec, err := format.NewCodec("mcap")
	if err != nil {
		t.Fatal(err)
	}
	// All THREE chunk-container codecs the streaming half decodes are covered:
	// ZSTD (klauspost DecodeAll), LZ4 (pierrec frame reader — the foxglove writer
	// emits LZ4 chunk bodies as LZ4 *frames* via lz4.NewWriter, so chunks.go must
	// decode them with lz4.NewReader, NOT lz4.UncompressBlock), and None.
	specs := []FileSpec{
		{
			Key: "dim_zstd.mcap", StartNs: 10_000 * sec, StepNs: 10 * 1_000_000,
			Compression: CompressionZSTD,
			Topics:      []TopicSpec{{Topic: "/clock", SchemaName: "rosgraph_msgs/msg/Clock", SchemaEnc: "ros2msg", MessageCount: 30}},
		},
		{
			Key: "dim_lz4.mcap", StartNs: 10_500 * sec, StepNs: 10 * 1_000_000,
			Compression: CompressionLZ4,
			Topics:      []TopicSpec{{Topic: "/scan", SchemaName: "sensor_msgs/msg/LaserScan", SchemaEnc: "ros2msg", MessageCount: 35}},
		},
		{
			Key: "dim_none.mcap", StartNs: 11_000 * sec, StepNs: 10 * 1_000_000,
			Compression: CompressionNone,
			Topics:      []TopicSpec{{Topic: "/odom", SchemaName: "nav_msgs/msg/Odometry", SchemaEnc: "ros2msg", MessageCount: 25}},
		},
		{
			Key: "dim_large.mcap", StartNs: 12_000 * sec, StepNs: 1 * sec,
			PayloadBytes: 512 * 1024, ChunkSize: 1 << 20,
			Topics: []TopicSpec{{Topic: "/img", SchemaName: "sensor_msgs/msg/Image", SchemaEnc: "ros2msg", MessageCount: 4}},
		},
		{
			Key: "dim_tiny.mcap", StartNs: 13_000 * sec, StepNs: 10 * 1_000_000,
			ChunkSize: 1 << 20, // one chunk, a few messages
			Topics:    []TopicSpec{{Topic: "/tf", SchemaName: "tf2_msgs/msg/TFMessage", SchemaEnc: "ros2msg", MessageCount: 3}},
		},
		{
			Key: "dim_tags.mcap", StartNs: 14_000 * sec, StepNs: 10 * 1_000_000,
			Metadata: map[string]string{"robot_id": "zala-7", "operator": "alice"},
			Topics:   []TopicSpec{{Topic: "/clock", SchemaName: "rosgraph_msgs/msg/Clock", SchemaEnc: "ros2msg", MessageCount: 10}},
		},
	}
	for _, spec := range specs {
		spec := spec
		t.Run(spec.Key, func(t *testing.T) {
			var buf bytes.Buffer
			if err := Write(&buf, spec); err != nil {
				t.Fatalf("Write: %v", err)
			}
			data := buf.Bytes()
			bs := memBlob{key: spec.Key, data: data}
			fs, err := codec.Extract(context.Background(), bs, spec.Key)
			if err != nil {
				t.Fatalf("Extract: %v", err)
			}
			if fs.MessageCount != spec.TotalMessages() {
				t.Errorf("message_count: got %d want %d", fs.MessageCount, spec.TotalMessages())
			}
			if fs.StartNs != spec.StartNs {
				t.Errorf("start_ns: got %d want %d", fs.StartNs, spec.StartNs)
			}
			if fs.EndNs != spec.EndNs() {
				t.Errorf("end_ns: got %d want %d", fs.EndNs, spec.EndNs())
			}
			// Embedded tag metadata must round-trip through Extract.
			for k, v := range spec.Metadata {
				if fs.Metadata[k] != v {
					t.Errorf("metadata[%q]: got %q want %q", k, fs.Metadata[k], v)
				}
			}
			// Streaming half must decode each compression variant to the exact total.
			got, err := iterateAll(t, spec.Key, data)
			if err != nil {
				t.Fatalf("Iterate: %v", err)
			}
			if uint64(got) != spec.TotalMessages() {
				t.Errorf("iterated total: got %d want %d", got, spec.TotalMessages())
			}
		})
	}
}

// TestOverrides_SchemaDataAndPayloadFn proves the two additive TopicSpec
// overrides take effect AND that determinism is preserved across two writes.
// A fixture with real SchemaData + a PayloadFn is written twice; the outputs
// must be byte-identical (warm-start property). A raw mcap.Reader parse then
// asserts that the schema record carries the override bytes and that a sampled
// message payload matches the PayloadFn output.
func TestOverrides_SchemaDataAndPayloadFn(t *testing.T) {
	realSchema := []byte("real schema text")
	payloadFn := func(i int) []byte { return []byte{byte(i), 0xAB} }

	spec := FileSpec{
		Key:     "overrides_test.mcap",
		StartNs: 9000 * 1_000_000_000,
		StepNs:  10 * 1_000_000,
		Topics: []TopicSpec{
			{
				Topic:        "/data",
				SchemaName:   "test_msgs/msg/Data",
				SchemaEnc:    "ros2msg",
				MessageCount: 4,
				SchemaData:   realSchema,
				PayloadFn:    payloadFn,
			},
		},
	}

	// Write twice and assert byte-identical outputs (determinism gate).
	var a, b bytes.Buffer
	if err := Write(&a, spec); err != nil {
		t.Fatalf("Write (first): %v", err)
	}
	if err := Write(&b, spec); err != nil {
		t.Fatalf("Write (second): %v", err)
	}
	if !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatal("non-deterministic output with SchemaData+PayloadFn overrides")
	}

	// Parse back with a raw mcap.Reader to inspect schema data and a message payload.
	r, err := mcap.NewReader(bytes.NewReader(a.Bytes()))
	if err != nil {
		t.Fatalf("mcap.NewReader: %v", err)
	}
	defer r.Close()

	it, err := r.Messages()
	if err != nil {
		t.Fatalf("r.Messages: %v", err)
	}

	// Collect schemas from the Info summary, then iterate messages.
	info, err := r.Info()
	if err != nil {
		t.Fatalf("r.Info: %v", err)
	}
	if len(info.Schemas) == 0 {
		t.Fatal("no schemas in parsed MCAP")
	}
	// The one schema must carry our override bytes.
	var gotSchema *mcap.Schema
	for _, s := range info.Schemas {
		gotSchema = s
		break
	}
	if !bytes.Equal(gotSchema.Data, realSchema) {
		t.Errorf("schema data: got %q want %q", gotSchema.Data, realSchema)
	}

	// Iterate messages and check each payload matches PayloadFn(idx).
	msgIdx := 0
	for {
		_, _, msg, mErr := it.Next(nil)
		if mErr != nil {
			break // io.EOF or end
		}
		want := payloadFn(msgIdx)
		if !bytes.Equal(msg.Data, want) {
			t.Errorf("message[%d] payload: got %v want %v", msgIdx, msg.Data, want)
		}
		msgIdx++
	}
	if msgIdx != 4 {
		t.Errorf("iterated %d messages, want 4", msgIdx)
	}
}

// TestDeterministic_WithMetadata proves byte-identity holds even with embedded
// metadata (map iteration is sorted before writing) — the warm-start property.
func TestDeterministic_WithMetadata(t *testing.T) {
	spec := FileSpec{
		Key: "det_tags.mcap", StartNs: 1, StepNs: 1,
		Metadata: map[string]string{"z": "1", "a": "2", "m": "3"},
		Topics:   []TopicSpec{{Topic: "/x", SchemaName: "X", SchemaEnc: "ros2msg", MessageCount: 5}},
	}
	var a, b bytes.Buffer
	if err := Write(&a, spec); err != nil {
		t.Fatal(err)
	}
	if err := Write(&b, spec); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a.Bytes(), b.Bytes()) {
		t.Fatal("non-deterministic output with embedded metadata")
	}
}

// TestRealRos2Payloads_WireShape pins the ros2msg CDR encoders OFFLINE.
//
// These encoders are the only thing standing between a synthetic fixture and
// "Missing ROSType in library" in the real PJ4 parser stack — and every other
// assertion in this package (and in smoke, and in the CLI round-trips) is
// payload-AGNOSTIC: message bytes are only ever compared to themselves. A
// broken encoder therefore stays invisible to Go CI and only surfaces in a
// cross-repo live GUI run (scripts/e2e-layout-import.sh). This test closes
// that gap without linking the C++ parser: the encapsulation prefix and the
// exact encoded length are precisely what the alignment math gets wrong.
//
// The golden lengths are MEASURED values at targetBytes=0 (no frame_id
// padding); they do not depend on idx (every field is fixed-width and the
// frame_id strings are constant). A deliberate change to a message's field
// set must update them.
func TestRealRos2Payloads_WireShape(t *testing.T) {
	const idx = 7
	cases := []struct {
		schemaName string
		goldenLen  int
	}{
		{"rosgraph_msgs/msg/Clock", 12},
		{"sensor_msgs/msg/Imu", 324},
		{"nav_msgs/msg/Odometry", 724},
		{"sensor_msgs/msg/LaserScan", 124},
		{"tf2_msgs/msg/TFMessage", 100},
	}
	for _, tc := range cases {
		t.Run(tc.schemaName, func(t *testing.T) {
			schema, fn, ok := realRos2Payload(tc.schemaName)
			if !ok {
				t.Fatalf("realRos2Payload(%q): not in the table", tc.schemaName)
			}
			if len(schema) == 0 {
				t.Fatalf("realRos2Payload(%q): empty schema text", tc.schemaName)
			}
			payload := fn(idx, 0)
			// (a) XCDR1 little-endian encapsulation header. parser_ros reads
			// this first; a wrong prefix mis-selects endianness/version and
			// every field after it decodes as garbage.
			if want := []byte{0x00, 0x01, 0x00, 0x00}; !bytes.HasPrefix(payload, want) {
				t.Fatalf("encapsulation prefix = % x, want % x", payload[:min(4, len(payload))], want)
			}
			// (b) the alignment math, pinned.
			if len(payload) != tc.goldenLen {
				t.Fatalf("encoded length = %d, want %d (field set or alignment changed)", len(payload), tc.goldenLen)
			}
			// Padding is best-effort but must never SHRINK a message below
			// its natural size (padFrame only grows frame_id).
			if padded := fn(idx, tc.goldenLen+256); len(padded) < tc.goldenLen {
				t.Fatalf("padded length %d < natural %d", len(padded), tc.goldenLen)
			}
		})
	}

	// (c) every ros2msg topic the shipped corpus generates must resolve to a
	// real schema + encoder — an unresolved one silently falls back to the
	// bare-type-name schema that mcap-loader rejects outright.
	for _, spec := range DefaultSpecs() {
		for _, topic := range spec.Topics {
			if topic.SchemaEnc != "ros2msg" || topic.SchemaData != nil || topic.PayloadFn != nil {
				continue
			}
			if _, _, ok := realRos2Payload(topic.SchemaName); !ok {
				t.Errorf("%s topic %s: SchemaName %q has no real payload encoder — the fixture would carry a bare type-name schema",
					spec.Key, topic.Topic, topic.SchemaName)
			}
		}
	}

	// An unknown type must NOT resolve (the legacy synthetic fallback path).
	if _, _, ok := realRos2Payload("not_a_pkg/msg/Nope"); ok {
		t.Fatal("realRos2Payload resolved an unknown type")
	}
}
