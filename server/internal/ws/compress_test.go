package ws

import (
	"testing"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	"pj-cloud/server/internal/config"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// testZstdDecoder is a shared stateless decoder for unwrapping frames in tests.
var testZstdDecoder = func() *zstd.Decoder {
	d, err := zstd.NewReader(nil)
	if err != nil {
		panic(err)
	}
	return d
}()

// newTestCompressor builds an enabled compressor with a small threshold so tests
// can exercise the wrap path without megabyte fixtures.
func newTestCompressor(t *testing.T, threshold int) *responseCompressor {
	t.Helper()
	rc, err := newResponseCompressor(config.ResponseCompressionConfig{
		Enabled:        true,
		Level:          3,
		ThresholdBytes: threshold,
		Concurrency:    2,
	}, nil)
	if err != nil {
		t.Fatalf("newResponseCompressor: %v", err)
	}
	if rc == nil {
		t.Fatal("expected non-nil compressor when enabled")
	}
	return rc
}

// bigListFiles returns a ListFilesResponse whose marshaled form is well above any
// small threshold and is HIGHLY compressible (repeated Hive-style keys) — the
// real-world shape.
func bigListFiles(n int) *pb.ServerMessage {
	files := make([]*pb.FileSummary, n)
	for i := range files {
		files[i] = &pb.FileSummary{
			Id:     uint64(i),
			S3Key:  "customer=dexory/customer_site=warehouse-01/robot=r7/source=lidar/date=2026-05-19/rosbox_2026-05-19_18-27-17.mcap",
			SizeBytes: 21_900_000,
			Recorded:  &pb.TimeRange{StartNs: 1_700_000_000_000_000_000, EndNs: 1_700_000_600_000_000_000},
			TopicCount:   6,
			MessageCount: 649_557,
		}
	}
	return &pb.ServerMessage{
		RequestId: 42,
		Payload:   &pb.ServerMessage_ListFiles{ListFiles: &pb.ListFilesResponse{Files: files}},
	}
}

// decodeMaybeWrapped parses bytes as a ServerMessage and, if it is an
// EncodedServerMessage, decompresses + parses the inner message. Mirrors the
// client's unwrap-before-route contract.
func decodeMaybeWrapped(t *testing.T, buf []byte) *pb.ServerMessage {
	t.Helper()
	var outer pb.ServerMessage
	if err := proto.Unmarshal(buf, &outer); err != nil {
		t.Fatalf("unmarshal outer: %v", err)
	}
	enc := outer.GetEncoded()
	if enc == nil {
		return &outer
	}
	// Outer envelope must carry ZERO routing ids — the inner message is authority.
	if outer.GetRequestId() != 0 || outer.GetSubscriptionId() != 0 {
		t.Errorf("wrapped envelope must have zero outer ids, got req=%d sub=%d",
			outer.GetRequestId(), outer.GetSubscriptionId())
	}
	if enc.GetEncoding() != pb.CompressionEncoding_COMPRESSION_ENCODING_ZSTD {
		t.Fatalf("unexpected encoding %v", enc.GetEncoding())
	}
	dec := zstdDecodeForTest(t, enc.GetBody())
	if uint64(len(dec)) != enc.GetUncompressedSize() {
		t.Fatalf("uncompressed_size mismatch: header=%d actual=%d", enc.GetUncompressedSize(), len(dec))
	}
	var inner pb.ServerMessage
	if err := proto.Unmarshal(dec, &inner); err != nil {
		t.Fatalf("unmarshal inner: %v", err)
	}
	return &inner
}

func TestMarshalResponse_WrapsLargeAllowlistedWhenNegotiated(t *testing.T) {
	rc := newTestCompressor(t, 512)
	m := bigListFiles(200)

	buf, err := rc.marshalResponse(m, true)
	if err != nil {
		t.Fatalf("marshalResponse: %v", err)
	}
	raw, _ := proto.Marshal(m)
	if len(buf) >= len(raw) {
		t.Fatalf("expected compressed frame smaller than raw: got=%d raw=%d", len(buf), len(raw))
	}
	inner := decodeMaybeWrapped(t, buf)
	if inner.GetEncoded() != nil {
		t.Fatal("inner must not itself be wrapped (no double-wrap)")
	}
	if inner.GetRequestId() != 42 {
		t.Errorf("inner request_id lost: got %d want 42", inner.GetRequestId())
	}
	if got := len(inner.GetListFiles().GetFiles()); got != 200 {
		t.Errorf("inner payload lost: got %d files want 200", got)
	}
	t.Logf("ListFiles(200): raw=%d wrapped=%d ratio=%.1fx", len(raw), len(buf), float64(len(raw))/float64(len(buf)))
}

func TestMarshalResponse_RawWhenNotNegotiated(t *testing.T) {
	rc := newTestCompressor(t, 512)
	m := bigListFiles(200)

	buf, err := rc.marshalResponse(m, false)
	if err != nil {
		t.Fatalf("marshalResponse: %v", err)
	}
	if decodeMaybeWrapped(t, buf).GetEncoded() != nil {
		t.Fatal("must not wrap when the client did not negotiate")
	}
	raw, _ := proto.Marshal(m)
	if len(buf) != len(raw) {
		t.Errorf("un-negotiated response must be byte-identical raw: got=%d raw=%d", len(buf), len(raw))
	}
}

func TestMarshalResponse_RawBelowThreshold(t *testing.T) {
	rc := newTestCompressor(t, 1<<20) // 1 MiB threshold: a 200-file response is under it
	m := bigListFiles(200)

	buf, err := rc.marshalResponse(m, true)
	if err != nil {
		t.Fatalf("marshalResponse: %v", err)
	}
	if decodeMaybeWrapped(t, buf).GetEncoded() != nil {
		t.Fatal("must not wrap a response below the size threshold")
	}
}

func TestMarshalResponse_NeverWrapsNonAllowlisted(t *testing.T) {
	rc := newTestCompressor(t, 1) // threshold 1: size is never the reason it stays raw

	// HelloResponse with a large, compressible vocabulary — big enough to clear
	// the threshold, but NOT allowlisted, so it must stay raw.
	vocab := make([]string, 4096)
	for i := range vocab {
		vocab[i] = "robot_id"
	}
	hello := &pb.ServerMessage{
		RequestId: 1,
		Payload: &pb.ServerMessage_HelloResponse{HelloResponse: &pb.HelloResponse{
			ServerVersion: "test",
			Backend:       &pb.BackendCapabilities{MetadataKeyVocabulary: vocab},
		}},
	}
	buf, err := rc.marshalResponse(hello, true)
	if err != nil {
		t.Fatalf("marshalResponse: %v", err)
	}
	if decodeMaybeWrapped(t, buf).GetEncoded() != nil {
		t.Fatal("HelloResponse must NEVER be wrapped (handshake stays raw)")
	}

	// A MessageBatch (already zstd inside) must also stay raw even on the priority
	// allowlist check.
	batch := &pb.ServerMessage{
		Payload: &pb.ServerMessage_Batch{Batch: &pb.MessageBatch{
			BodyEncoding: pb.BodyEncoding_BODY_ENCODING_ZSTD,
			Body:         make([]byte, 8192),
		}},
	}
	buf, err = rc.marshalResponse(batch, true)
	if err != nil {
		t.Fatalf("marshalResponse batch: %v", err)
	}
	if decodeMaybeWrapped(t, buf).GetEncoded() != nil {
		t.Fatal("MessageBatch must NEVER be wrapped (already compressed)")
	}
}

func TestMarshalResponse_NeverLargerThanRaw(t *testing.T) {
	rc := newTestCompressor(t, 8)
	// A high-entropy, poorly-compressible payload (7-bit-masked so it stays valid
	// UTF-8 for the proto string field). Whatever the codec decides — wrap or the
	// no-savings raw fallback — the emitted frame must NEVER exceed raw. This is
	// the safety invariant the fallback branch exists to guarantee.
	blob := make([]byte, 8192)
	for i := range blob {
		blob[i] = byte((i*2654435761 + i*i*40503) >> 11 & 0x7f) // spread, 7-bit
	}
	m := &pb.ServerMessage{
		RequestId: 7,
		Payload: &pb.ServerMessage_GetVocabulary{GetVocabulary: &pb.GetVocabularyResponse{
			Sources: []*pb.DimSource{{Id: 1, Name: string(blob)}},
		}},
	}
	raw, _ := proto.Marshal(m)
	buf, err := rc.marshalResponse(m, true)
	if err != nil {
		t.Fatalf("marshalResponse: %v", err)
	}
	if len(buf) > len(raw) {
		t.Errorf("emitted frame must never exceed raw: emitted=%d > raw=%d", len(buf), len(raw))
	}
}

func TestMarshalResponse_NilCompressorIsRaw(t *testing.T) {
	var rc *responseCompressor // nil => feature off
	m := bigListFiles(200)
	buf, err := rc.marshalResponse(m, true)
	if err != nil {
		t.Fatalf("marshalResponse on nil compressor: %v", err)
	}
	raw, _ := proto.Marshal(m)
	if len(buf) != len(raw) {
		t.Errorf("nil compressor must emit raw: got=%d raw=%d", len(buf), len(raw))
	}
}

func TestNewResponseCompressor_DisabledReturnsNil(t *testing.T) {
	rc, err := newResponseCompressor(config.ResponseCompressionConfig{Enabled: false}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rc != nil {
		t.Fatal("disabled config must yield a nil compressor")
	}
}

func TestClientAcceptsZstd(t *testing.T) {
	if clientAcceptsZstd(&pb.Hello{}) {
		t.Error("empty accepted_response_encodings must be false")
	}
	if !clientAcceptsZstd(&pb.Hello{AcceptedResponseEncodings: []pb.CompressionEncoding{
		pb.CompressionEncoding_COMPRESSION_ENCODING_ZSTD,
	}}) {
		t.Error("ZSTD in the list must be true")
	}
	if clientAcceptsZstd(&pb.Hello{AcceptedResponseEncodings: []pb.CompressionEncoding{
		pb.CompressionEncoding_COMPRESSION_ENCODING_UNSPECIFIED,
	}}) {
		t.Error("only UNSPECIFIED must be false")
	}
}

// zstdDecodeForTest decodes one zstd frame for the test's unwrap step.
func zstdDecodeForTest(t *testing.T, in []byte) []byte {
	t.Helper()
	out, err := testZstdDecoder.DecodeAll(in, nil)
	if err != nil {
		t.Fatalf("zstd decode: %v", err)
	}
	return out
}
