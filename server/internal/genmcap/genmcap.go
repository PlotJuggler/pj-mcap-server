// Package genmcap produces small, deterministic, chunked+summarized MCAP
// fixtures for the CI integration legs (Plan A Task 46 / 46a).
//
// WHY IT EXISTS: the smoke/matrix shell gates seed Minio/fake-gcs from the
// on-disk ground-truth corpus at /home/gn/ws/jkk_dataset02 — UNAVAILABLE in CI.
// The CI {s3,gcs} integration legs instead seed the emulator buckets with these
// synthetic fixtures and assert against the KNOWN counts/time-ranges this
// package pins. The fixtures are the single source of truth shared by the
// generator cmd (cmd/gen-ci-fixtures, which writes them to disk for external
// upload via mc/curl) AND the in-process harness test (which reads the same
// Spec to know what to assert), so the two can never drift.
//
// HARD CODEC REQUIREMENT (internal/format/format.go:91): the MCAP codec REJECTS
// files with no Statistics record, and the chunk-index path needs the summary
// section. So every fixture here is written Chunked:true with statistics + the
// summary section ENABLED (the foxglove writer does this by default — we never
// set Skip*). A naive unchunked writer would fail Extract at indexer-scan time.
//
// DETERMINISM: message payloads and timestamps are pure functions of the file
// index and message index — no time.Now, no randomness — so byte-identity holds
// across runs (re-uploading the same fixture is a no-op change-detect, which the
// warm-start assertion relies on).
package genmcap

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/foxglove/mcap/go/mcap"
)

// TopicSpec describes one channel in a fixture: its topic name, ROS2 schema
// name/encoding, and how many messages it carries.
type TopicSpec struct {
	Topic        string
	SchemaName   string
	SchemaEnc    string
	MessageCount int

	// SchemaData, when non-nil, is written verbatim as the MCAP schema record
	// data (real concatenated .msg text) instead of the synthetic
	// []byte(SchemaName) — lets fixtures carry parser_ros-decodable schemas.
	SchemaData []byte
	// PayloadFn, when non-nil, supplies each message body (idx = per-topic
	// message index) instead of the synthetic payload(). Must be
	// deterministic.
	PayloadFn func(idx int) []byte
}

// Compression selects the chunk-container codec a fixture is written with. The
// zero value (CompressionDefault) means "ZSTD" so existing specs stay
// byte-identical; the explicit values let the CI matrix cover the other two
// chunk codecs the streaming half decodes (chunks.go decompressChunk).
type Compression int

const (
	// CompressionDefault == ZSTD (keeps the original DefaultSpecs byte-identical).
	CompressionDefault Compression = iota
	CompressionZSTD
	CompressionLZ4
	CompressionNone
)

// mcapCompression maps a genmcap.Compression onto the foxglove writer's codec.
func (c Compression) mcapCompression() mcap.CompressionFormat {
	switch c {
	case CompressionLZ4:
		return mcap.CompressionLZ4
	case CompressionNone:
		return mcap.CompressionNone
	default: // CompressionDefault, CompressionZSTD
		return mcap.CompressionZSTD
	}
}

// FileSpec is the full, deterministic description of one synthetic MCAP. The
// time range is [StartNs, StartNs + (MaxMessages-1)*StepNs] across all topics;
// fixtures are pinned NON-OVERLAPPING so stitch/overlap assertions are valid.
type FileSpec struct {
	Key     string
	StartNs int64
	StepNs  int64
	Topics  []TopicSpec

	// Compression is the chunk-container codec (zero value = ZSTD). It threads
	// straight into the writer options so a fixture can exercise lz4/none decode.
	Compression Compression
	// PayloadBytes overrides the per-message body size (default 16). Set this
	// >=512KiB to produce large-payload singletons; the bytes stay deterministic.
	PayloadBytes int
	// ChunkSize overrides the writer ChunkSize (default 4096). A large value with
	// few messages yields a "tiny file" with exactly one chunk.
	ChunkSize uint64
	// Metadata, when non-empty, is written as an embedded MCAP Metadata record
	// (name "pj.user_tags") so tag-extraction fixtures carry real key/values.
	Metadata map[string]string
}

// payloadSize returns the per-message body size for this spec (default 16).
func (f FileSpec) payloadSize() int {
	if f.PayloadBytes > 0 {
		return f.PayloadBytes
	}
	return defaultPayloadBytes
}

// chunkSize returns the writer ChunkSize for this spec (default 4096).
func (f FileSpec) chunkSize() uint64 {
	if f.ChunkSize > 0 {
		return f.ChunkSize
	}
	return defaultChunkSize
}

const (
	defaultPayloadBytes = 16
	defaultChunkSize    = 4096
)

// TotalMessages is the sum of per-topic message counts.
func (f FileSpec) TotalMessages() uint64 {
	var n uint64
	for _, t := range f.Topics {
		n += uint64(t.MessageCount)
	}
	return n
}

// EndNs is the log_time of the last message written (max over topics of
// StartNs + (count-1)*StepNs). Matches what the codec reports as MessageEndTime.
func (f FileSpec) EndNs() int64 {
	var end int64 = f.StartNs
	for _, t := range f.Topics {
		if t.MessageCount == 0 {
			continue
		}
		e := f.StartNs + int64(t.MessageCount-1)*f.StepNs
		if e > end {
			end = e
		}
	}
	return end
}

// DefaultSpecs returns the pinned CI fixture matrix: three time-disjoint MCAPs
// with known per-topic counts. Topic sets overlap across files (a /clock and
// /odom present in all three) so a topic-subset stitch has a real union, plus
// per-file-unique topics so subset selection is meaningful.
//
// Counts are intentionally small (tens of messages) so the whole upload+scan is
// sub-second per leg. The values are arbitrary-but-fixed; the harness reads them
// straight off these specs, so changing them here changes the assertions too.
func DefaultSpecs() []FileSpec {
	const sec = 1_000_000_000
	// File A: t in [1000s, ~). File B starts strictly after A ends; C after B.
	return []FileSpec{
		{
			Key:     "ci_synth_a.mcap",
			StartNs: 1000 * sec,
			StepNs:  10 * 1_000_000, // 10ms
			Topics: []TopicSpec{
				{Topic: "/clock", SchemaName: "rosgraph_msgs/msg/Clock", SchemaEnc: "ros2msg", MessageCount: 50},
				{Topic: "/odom", SchemaName: "nav_msgs/msg/Odometry", SchemaEnc: "ros2msg", MessageCount: 40},
				{Topic: "/imu", SchemaName: "sensor_msgs/msg/Imu", SchemaEnc: "ros2msg", MessageCount: 30},
			},
		},
		{
			Key:     "ci_synth_b.mcap",
			StartNs: 2000 * sec,
			StepNs:  10 * 1_000_000,
			Topics: []TopicSpec{
				{Topic: "/clock", SchemaName: "rosgraph_msgs/msg/Clock", SchemaEnc: "ros2msg", MessageCount: 50},
				{Topic: "/odom", SchemaName: "nav_msgs/msg/Odometry", SchemaEnc: "ros2msg", MessageCount: 60},
				{Topic: "/scan", SchemaName: "sensor_msgs/msg/LaserScan", SchemaEnc: "ros2msg", MessageCount: 20},
			},
		},
		{
			Key:     "ci_synth_c.mcap",
			StartNs: 3000 * sec,
			StepNs:  10 * 1_000_000,
			Topics: []TopicSpec{
				{Topic: "/clock", SchemaName: "rosgraph_msgs/msg/Clock", SchemaEnc: "ros2msg", MessageCount: 50},
				{Topic: "/odom", SchemaName: "nav_msgs/msg/Odometry", SchemaEnc: "ros2msg", MessageCount: 25},
				{Topic: "/tf", SchemaName: "tf2_msgs/msg/TFMessage", SchemaEnc: "ros2msg", MessageCount: 15},
			},
		},
		// File D (C2 dimension: UNCOMPRESSED chunk container). Time-disjoint from
		// A/B/C so it joins the stitch sum cleanly and exercises the codec's
		// no-compression decode path end-to-end through the integration leg.
		{
			Key:         "ci_synth_d_none.mcap",
			StartNs:     4000 * sec,
			StepNs:      10 * 1_000_000,
			Compression: CompressionNone,
			Topics: []TopicSpec{
				{Topic: "/clock", SchemaName: "rosgraph_msgs/msg/Clock", SchemaEnc: "ros2msg", MessageCount: 20},
				{Topic: "/odom", SchemaName: "nav_msgs/msg/Odometry", SchemaEnc: "ros2msg", MessageCount: 10},
			},
		},
		// File E (C2 dimension: TINY single-chunk file). A large ChunkSize with a
		// handful of messages yields exactly one chunk — the small-file edge.
		{
			Key:       "ci_synth_e_tiny.mcap",
			StartNs:   5000 * sec,
			StepNs:    10 * 1_000_000,
			ChunkSize: 1 << 20,
			Topics: []TopicSpec{
				{Topic: "/clock", SchemaName: "rosgraph_msgs/msg/Clock", SchemaEnc: "ros2msg", MessageCount: 3},
				{Topic: "/tf", SchemaName: "tf2_msgs/msg/TFMessage", SchemaEnc: "ros2msg", MessageCount: 2},
			},
		},
		// File F (C2 dimension: LZ4 chunk container). Time-disjoint from A-E so it
		// joins the stitch sum cleanly and exercises the codec's LZ4-FRAME decode
		// path end-to-end (chunks.go lz4.NewReader). The foxglove writer emits LZ4
		// chunk bodies as frames; the codec must decode frames, not raw blocks.
		{
			Key:         "ci_synth_f_lz4.mcap",
			StartNs:     6000 * sec,
			StepNs:      10 * 1_000_000,
			Compression: CompressionLZ4,
			Topics: []TopicSpec{
				{Topic: "/clock", SchemaName: "rosgraph_msgs/msg/Clock", SchemaEnc: "ros2msg", MessageCount: 30},
				{Topic: "/scan", SchemaName: "sensor_msgs/msg/LaserScan", SchemaEnc: "ros2msg", MessageCount: 15},
			},
		},
	}
}

// payload produces a deterministic body of n bytes for (topic, idx). The exact
// bytes do not matter to the catalog/streaming assertions (they assert COUNTS,
// not content), but determinism keeps re-uploads byte-identical for the
// change-detect path. n>=512KiB yields a large-payload singleton.
func payload(topicID, idx, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte((topicID*131 + idx*7 + i) & 0xff)
	}
	return b
}

// Write emits one FileSpec as a chunked, summarized MCAP to w. The summary
// section (chunk index + statistics + repeated schemas/channels) is written by
// Close — required for the codec's Extract to succeed.
func Write(w io.Writer, spec FileSpec) error {
	writer, err := mcap.NewWriter(w, &mcap.WriterOptions{
		Chunked:     true,
		ChunkSize:   int64(spec.chunkSize()), // small by default so fixtures get multiple chunk indexes
		Compression: spec.Compression.mcapCompression(),
		IncludeCRC:  true,
		// All Skip* left false: statistics + chunk index + summary offsets ON.
	})
	if err != nil {
		return fmt.Errorf("new writer: %w", err)
	}
	if err := writer.WriteHeader(&mcap.Header{Profile: "ros2", Library: "pj-cloud-genmcap"}); err != nil {
		return fmt.Errorf("header: %w", err)
	}

	// One schema+channel per topic. IDs are 1-based and stable in topic order.
	for ti, t := range spec.Topics {
		schemaID := uint16(ti + 1)
		channelID := uint16(ti + 1)
		schemaData := []byte(t.SchemaName) // synthetic default — non-empty, deterministic
		if t.SchemaData != nil {
			schemaData = t.SchemaData // real concatenated .msg text when provided
		}
		if err := writer.WriteSchema(&mcap.Schema{
			ID:       schemaID,
			Name:     t.SchemaName,
			Encoding: t.SchemaEnc,
			Data:     schemaData,
		}); err != nil {
			return fmt.Errorf("schema %s: %w", t.Topic, err)
		}
		if err := writer.WriteChannel(&mcap.Channel{
			ID:              channelID,
			SchemaID:        schemaID,
			Topic:           t.Topic,
			MessageEncoding: "cdr",
		}); err != nil {
			return fmt.Errorf("channel %s: %w", t.Topic, err)
		}
	}

	// Interleave messages across topics in log-time order so chunks span topics.
	type cursor struct {
		channelID uint16
		idx       int
		count     int
	}
	cursors := make([]cursor, len(spec.Topics))
	maxCount := 0
	for ti, t := range spec.Topics {
		cursors[ti] = cursor{channelID: uint16(ti + 1), count: t.MessageCount}
		if t.MessageCount > maxCount {
			maxCount = t.MessageCount
		}
	}
	psize := spec.payloadSize()
	for step := 0; step < maxCount; step++ {
		logTime := uint64(spec.StartNs + int64(step)*spec.StepNs)
		for ti := range cursors {
			c := &cursors[ti]
			if c.idx >= c.count {
				continue
			}
			msgData := payload(ti, c.idx, psize)
			if spec.Topics[ti].PayloadFn != nil {
				msgData = spec.Topics[ti].PayloadFn(c.idx)
			}
			if err := writer.WriteMessage(&mcap.Message{
				ChannelID:   c.channelID,
				Sequence:    uint32(c.idx),
				LogTime:     logTime,
				PublishTime: logTime,
				Data:        msgData,
			}); err != nil {
				return fmt.Errorf("message t=%d step=%d: %w", ti, step, err)
			}
			c.idx++
		}
	}

	// Embedded metadata records (tag fixtures). Keys are written in sorted order
	// so the bytes stay deterministic across runs (map iteration is random).
	if len(spec.Metadata) > 0 {
		keys := make([]string, 0, len(spec.Metadata))
		for k := range spec.Metadata {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		md := make(map[string]string, len(spec.Metadata))
		for _, k := range keys {
			md[k] = spec.Metadata[k]
		}
		if err := writer.WriteMetadata(&mcap.Metadata{Name: "pj.user_tags", Metadata: md}); err != nil {
			return fmt.Errorf("metadata: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	return nil
}

// mcapMagicLen is the leading + trailing MCAP magic length (0x89 M C A P 0x30 \r \n).
const mcapMagicLen = 8

// CorruptChunkBody takes a VALID, written MCAP (e.g. from Write) and returns a
// copy with one byte flipped INSIDE the first Chunk record's compressed body
// (the Records region), leaving the file framing AND the summary section (chunk
// index / statistics / footer) intact. This models on-the-wire / at-rest
// corruption the codec must reject when it actually decodes a chunk.
//
// IMPORTANT (verified against the codec): the MCAP codec here does NOT verify a
// chunk's UncompressedCRC (mcap v1.7.4 ParseChunk reads it but never checks it).
// So a corrupted NONE-compressed chunk would NOT be rejected unless the flip
// happens to break inner record framing. The reliable integrity surface is a
// COMPRESSED chunk: flipping a byte in a ZSTD/LZ4 Records blob makes the decode
// (zstd DecodeAll / lz4 frame reader) fail, which surfaces as the
// "format: zstd decode chunk" / "format: lz4 decode chunk" error from chunks.go's
// Iterate path. Callers MUST pass a fixture written with CompressionZSTD (or LZ4)
// for the rejection to fire.
func CorruptChunkBody(valid []byte) ([]byte, error) {
	out := append([]byte(nil), valid...)
	recordsOff, recordsLen, err := firstChunkRecordsRegion(out)
	if err != nil {
		return nil, err
	}
	if recordsLen <= 0 {
		return nil, fmt.Errorf("genmcap: first chunk has empty Records region")
	}
	// Flip a byte near the middle of the compressed Records blob (avoids the
	// frame header magic at the very start, which some decoders reject before
	// touching the payload — we want a payload-corruption failure, not a header
	// one, but either still fails the decode).
	pos := recordsOff + recordsLen/2
	out[pos] ^= 0xff
	return out, nil
}

// firstChunkRecordsRegion walks the top-level record stream (after the leading
// magic) and returns the byte offset + length of the first Chunk record's
// Records (the compressed inner-records blob). It parses just enough of the
// chunk body header to locate Records; it never decompresses.
func firstChunkRecordsRegion(data []byte) (off, length int, err error) {
	if len(data) < mcapMagicLen+recordHeaderLen {
		return 0, 0, fmt.Errorf("genmcap: file too short (%d bytes)", len(data))
	}
	p := mcapMagicLen // skip leading magic
	for p+recordHeaderLen <= len(data) {
		op := mcap.OpCode(data[p])
		bodyLen := int(binary.LittleEndian.Uint64(data[p+1 : p+recordHeaderLen]))
		bodyStart := p + recordHeaderLen
		bodyEnd := bodyStart + bodyLen
		if bodyEnd > len(data) {
			return 0, 0, fmt.Errorf("genmcap: record at %d overruns file (need %d, have %d)", p, bodyEnd, len(data))
		}
		if op == mcap.OpChunk {
			return chunkRecordsRegion(data, bodyStart, bodyLen)
		}
		p = bodyEnd
	}
	return 0, 0, fmt.Errorf("genmcap: no chunk record found")
}

// recordHeaderLen mirrors format.recordHeaderLen (opcode + uint64 length). It is
// redefined here so genmcap stays free of an import cycle on package format.
const recordHeaderLen = 9

// chunkRecordsRegion parses the chunk-record body header (start/end/uncompressed
// size/CRC/compression string/records length) and returns the absolute offset +
// length of the Records blob within data. Layout per the MCAP spec:
//
//	uint64 message_start_time | uint64 message_end_time | uint64 uncompressed_size
//	uint32 uncompressed_crc   | uint32 compression_len + compression bytes
//	uint64 records_length     | records[records_length]
func chunkRecordsRegion(data []byte, bodyStart, bodyLen int) (off, length int, err error) {
	q := bodyStart
	need := func(n int) error {
		if q+n > bodyStart+bodyLen || q+n > len(data) {
			return fmt.Errorf("genmcap: chunk body truncated at %d", q)
		}
		return nil
	}
	if err := need(8 + 8 + 8 + 4 + 4); err != nil {
		return 0, 0, err
	}
	q += 8 + 8 + 8 + 4 // start, end, uncompressed_size, uncompressed_crc
	compLen := int(binary.LittleEndian.Uint32(data[q : q+4]))
	q += 4
	if err := need(compLen + 8); err != nil {
		return 0, 0, err
	}
	q += compLen // skip compression string
	recordsLen := int(binary.LittleEndian.Uint64(data[q : q+8]))
	q += 8
	if q+recordsLen > bodyStart+bodyLen || q+recordsLen > len(data) {
		return 0, 0, fmt.Errorf("genmcap: chunk records overrun (need %d, have %d)", q+recordsLen, bodyStart+bodyLen)
	}
	return q, recordsLen, nil
}
