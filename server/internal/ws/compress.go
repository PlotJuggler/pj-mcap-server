package ws

import (
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"

	"pj-cloud/server/internal/config"
	"pj-cloud/server/internal/metrics"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// responseCompressor wraps allowlisted catalog RPC responses in an
// EncodedServerMessage (the compressed-envelope path). It owns ONE shared,
// stateless klauspost zstd encoder: EncodeAll is safe for concurrent use, so the
// per-connection write paths can all call through it without a mutex. A nil
// *responseCompressor means the feature is OFF (the marshal falls back to raw) —
// the zero-dependency default for unit tests that build a conn directly.
type responseCompressor struct {
	enc       *zstd.Encoder
	threshold int
	// mx, if set, records compression-effectiveness counters (input vs output
	// bytes on the compression path). Nil-safe.
	mx *metrics.Metrics
}

// newResponseCompressor builds the shared encoder from the transport config.
// Returns (nil, nil) when the feature is disabled so the caller can treat a nil
// compressor as "never wrap".
func newResponseCompressor(cfg config.ResponseCompressionConfig, mx *metrics.Metrics) (*responseCompressor, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	// cfg is a value copy; fill any zero numeric fields from the single source of
	// the level/threshold/concurrency defaults (a hand-built config may set only
	// Enabled). Production configs are already filled by config.Load.
	cfg.WithDefaults()
	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(cfg.Level)),
		zstd.WithEncoderConcurrency(cfg.Concurrency),
	)
	if err != nil {
		return nil, err
	}
	return &responseCompressor{enc: enc, threshold: cfg.ThresholdBytes, mx: mx}, nil
}

// recordCompression bumps the effectiveness counters (nil-safe).
func (rc *responseCompressor) recordCompression(inputLen, outputLen int) {
	if rc.mx == nil {
		return
	}
	rc.mx.WSResponseCompressInputBytes.Add(float64(inputLen))
	rc.mx.WSResponseCompressOutputBytes.Add(float64(outputLen))
}

// compressible reports whether a ServerMessage payload may ride the compressed
// envelope. The allowlist is EXPLICIT (Codex review): only the bulky catalog RPC
// responses are eligible. HelloResponse (handshake must stay raw), Error/Progress/
// Eos (control-plane latency + pre-handshake ordering), MessageBatch (already one
// self-contained ZSTD frame), the Encoded wrapper itself (never double-wrap), and
// any unknown/future payload all fall through to raw.
func compressible(m *pb.ServerMessage) bool {
	switch m.GetPayload().(type) {
	case *pb.ServerMessage_ListFiles,
		*pb.ServerMessage_GetFile,
		*pb.ServerMessage_OpenSession,
		*pb.ServerMessage_GetVocabulary,
		*pb.ServerMessage_UpdateTags:
		return true
	default:
		return false
	}
}

// marshalResponse produces the bytes to queue for a ServerMessage. When the
// client negotiated compression AND this compressor is enabled AND the payload is
// allowlisted AND the marshaled form clears the threshold AND compression
// actually saves bytes, it returns a marshaled EncodedServerMessage wrapping the
// inner ServerMessage; otherwise it returns the raw marshaled ServerMessage. Any
// error along the compression path is non-fatal — it falls back to raw. The outer
// EncodedServerMessage carries ZERO request_id/subscription_id: the inner message
// is the sole routing authority (the client unwraps before it routes).
func (rc *responseCompressor) marshalResponse(m *pb.ServerMessage, negotiated bool) ([]byte, error) {
	raw, err := proto.Marshal(m)
	if err != nil {
		return nil, err
	}
	if rc == nil || !negotiated || !compressible(m) || len(raw) < rc.threshold {
		return raw, nil
	}

	body := rc.enc.EncodeAll(raw, nil)
	// No-savings fallback: a wrapped frame only helps if body + wrapper overhead
	// is smaller than raw. If zstd couldn't shrink it (incompressible / tiny),
	// ship raw and account the raw bytes as the compression-path output.
	if len(body) >= len(raw) {
		rc.recordCompression(len(raw), len(raw))
		return raw, nil
	}
	wrapped, err := proto.Marshal(&pb.ServerMessage{
		Payload: &pb.ServerMessage_Encoded{Encoded: &pb.EncodedServerMessage{
			Encoding:         pb.CompressionEncoding_COMPRESSION_ENCODING_ZSTD,
			UncompressedSize: uint64(len(raw)),
			Body:             body,
		}},
	})
	if err != nil {
		// Wrapping failed unexpectedly: fall back to the already-marshaled raw.
		rc.recordCompression(len(raw), len(raw))
		return raw, nil
	}
	rc.recordCompression(len(raw), len(wrapped))
	return wrapped, nil
}
