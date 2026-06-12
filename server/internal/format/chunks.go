// chunks.go is the streaming half of the MCAP FormatCodec: the chunk-level API
// the session subsystem consumes (Plan A Task 31 + M1a Task 15a, adapted to our
// slice-1 layout). It is the *deferred* half referenced by format.go's SCOPE
// NOTE ("The PlanChunks/Iterate streaming half lands with the session
// subsystem").
//
// DEVIATION FROM PLAN A (documented): Plan A's M1a puts ChunkRef / RawMessage /
// TimeWindow in package session and makes format import session. To keep the
// slice-1 dependency graph acyclic (catalog -> format already exists, and
// session will likewise import format), THESE chunk-level types live in package
// format and session imports them. No type is duplicated; session aliases them.
//
// REALITY CORRECTION (verified against testdata/nissan_zala_50_zeg_1_0.mcap):
// Plan A Task 31's ProductionChunkIter does mcap.NewReader(chunkBytes) on a bare
// chunk-range blob. That does NOT work: a Range-GET of (offset,length) yields a
// raw Chunk *record* (opcode 0x06 + len + body), which has no MCAP magic, and
// messages inside reference channels declared in EARLIER chunks (only the first
// chunks carry Channel records). So Iterate here (a) parses the chunk record
// header manually, (b) ParseChunk-decompresses the body (zstd/lz4/none), (c)
// walks the inner records by hand, and (d) resolves each Message.ChannelID
// through the file's GLOBAL channel/schema table captured at ChunkIndex time.
package format

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"

	"pj-cloud/server/internal/storage"
)

// recordHeaderLen is the MCAP record framing: 1-byte opcode + 8-byte uint64 len.
const recordHeaderLen = 9

// TimeWindow is a half-open [StartNs, EndNs) range in unix nanoseconds. A nil
// *TimeWindow means "the whole stitched horizon".
type TimeWindow struct {
	StartNs int64
	EndNs   int64
}

// TopicSchemaInfo is one topic's wire-binding info: its schema name/encoding/
// bytes plus the channel message-encoding. Names + schema bytes cross the wire
// ONCE (OpenSessionResponse.topic_id_map + schemas); MessageBatch then uses the
// small uint32 ids the session layer assigns.
type TopicSchemaInfo struct {
	TopicName       string
	SchemaName      string
	SchemaEncoding  string // schema definition language ("ros2msg", "protobuf", ...)
	SchemaData      []byte
	MessageEncoding string // e.g. "cdr", "protobuf"
}

// ChunkRef points at one MCAP chunk in a source file plus the session-requested
// topics that occur in it. The chunk's compressed bytes are Range-GET'd verbatim
// (Offset, Length address the chunk *record*, header included).
type ChunkRef struct {
	FileID        uint64
	StartNs       int64               // chunk MessageStartTime
	EndNs         int64               // chunk MessageEndTime
	Offset        int64               // chunk record start offset in the file
	Length        int64               // chunk record length (incl. 9-byte header)
	Compression   string              // "" / "zstd" / "lz4" (chunk-container codec)
	ChannelTopics map[string]struct{} // topics present in this chunk the session wants
	// MessageCount is the count of messages this ref contributes to the session:
	// EXACT (summed from MessageIndex over the selected topics) when message
	// indexes are present, else a chunk-level upper bound (all messages in the
	// chunk). See spec 6.3 (approximate_messages).
	MessageCount uint64
}

// RawMessage is Iterate's per-message output: decompressed payload bytes plus
// topic + resolved schema info + timestamps. The producer re-frames these into
// MessageBatch records.
type RawMessage struct {
	Topic           string
	SchemaName      string
	SchemaEncoding  string
	MessageEncoding string
	LogTimeNs       int64
	PublishTimeNs   int64
	Payload         []byte
}

// channelInfo is the file's global per-channel resolution captured once.
type channelInfo struct {
	topic           string
	schemaName      string
	schemaEncoding  string
	messageEncoding string
}

// FileChunkIndex is the per-file chunk index + global channel/schema table,
// produced by ChunkIndex and consumed by PlanChunks + Iterate. The channel table
// is unexported because callers only reach it through Iterate (which resolves
// Message.ChannelID through it).
type FileChunkIndex struct {
	FileID  uint64
	Chunks  []ChunkRef        // ordered by chunk start offset
	Schemas []TopicSchemaInfo // one per topic (each topic has exactly one schema in v1)

	channels    map[uint16]channelInfo // global channel-id -> resolution
	chunkCounts []chunkCounts          // parallel to Chunks; per-chunk per-topic counts
}

// ChunkIndex reads a file's summary section (chunk indexes + message indexes +
// channel/schema tables) over ranged reads and builds a FileChunkIndex. It never
// reads chunk bodies — only the index/summary section — so it is cheap.
//
// fileID is stamped onto every ChunkRef so the producer knows which object to
// Range-GET and so MessageBatch.source_file_id is correct.
func (c mcapCodec) ChunkIndex(ctx context.Context, bs storage.BlobStore, key string, fileID uint64) (FileChunkIndex, error) {
	_, reader, info, err := openSummary(ctx, bs, key)
	if err != nil {
		return FileChunkIndex{}, err
	}
	defer reader.Close()
	return c.chunkIndexFromInfo(info, fileID), nil
}

// ExtractAndIndex reads the summary ONCE and produces both the catalog summary
// and the streaming chunk index. The indexer uses this so its background scan
// pre-warms the chunk-index cache without a second WAN read.
func (c mcapCodec) ExtractAndIndex(ctx context.Context, bs storage.BlobStore, key string, fileID uint64) (FileSummary, FileChunkIndex, error) {
	head, reader, info, err := openSummary(ctx, bs, key)
	if err != nil {
		return FileSummary{}, FileChunkIndex{}, err
	}
	defer reader.Close()
	summary, err := c.summaryFromInfo(key, head, reader, info)
	if err != nil {
		return FileSummary{}, FileChunkIndex{}, err
	}
	return summary, c.chunkIndexFromInfo(info, fileID), nil
}

// chunkIndexFromInfo builds the per-file chunk index from an already-read mcap
// Info (no further I/O). Splitting it out lets ExtractAndIndex reuse one read.
func (mcapCodec) chunkIndexFromInfo(info *mcap.Info, fileID uint64) FileChunkIndex {
	idx := FileChunkIndex{
		FileID:   fileID,
		channels: make(map[uint16]channelInfo, len(info.Channels)),
	}

	// Global channel/schema table — resolves every Message.ChannelID inside any
	// chunk, even chunks that don't re-declare their channels (the common case).
	seenTopic := make(map[string]struct{}, len(info.Channels))
	for chID, ch := range info.Channels {
		ci := channelInfo{topic: ch.Topic, messageEncoding: ch.MessageEncoding}
		if sch := info.Schemas[ch.SchemaID]; sch != nil {
			ci.schemaName = sch.Name
			ci.schemaEncoding = sch.Encoding
		}
		idx.channels[chID] = ci
		if _, ok := seenTopic[ch.Topic]; !ok {
			seenTopic[ch.Topic] = struct{}{}
			ts := TopicSchemaInfo{
				TopicName:       ch.Topic,
				MessageEncoding: ch.MessageEncoding,
				SchemaName:      ci.schemaName,
				SchemaEncoding:  ci.schemaEncoding,
			}
			if sch := info.Schemas[ch.SchemaID]; sch != nil {
				ts.SchemaData = append([]byte(nil), sch.Data...)
			}
			idx.Schemas = append(idx.Schemas, ts)
		}
	}
	sort.Slice(idx.Schemas, func(i, j int) bool { return idx.Schemas[i].TopicName < idx.Schemas[j].TopicName })

	// Per-chunk refs. MessageIndexOffsets keys are the channel ids present in the
	// chunk -> we map those to topics for the plan's topic-intersection test.
	// That membership comes FREE with the summary's ChunkIndex records; the
	// MessageIndex records themselves are deliberately NOT read. They cost ~16
	// bytes per message (~10MB on a real 630k-msg staging file) and were only
	// feeding the spec-APPROXIMATE message estimates — reading them made a 0.5MB
	// windowed download pay a ~10MB / ~12s plan-build tax over WAN. Per-chunk
	// per-topic counts are instead ESTIMATED from the file-level Statistics
	// channel counts, distributed evenly across the chunks that carry each topic
	// (sums stay exact for whole-file selections; windowed/filtered selections
	// were already chunk-granular upper bounds).
	for _, ci := range info.ChunkIndexes {
		topics := make(map[string]struct{}, len(ci.MessageIndexOffsets))
		for chID := range ci.MessageIndexOffsets {
			c, ok := idx.channels[chID]
			if !ok {
				continue
			}
			topics[c.topic] = struct{}{}
		}
		idx.Chunks = append(idx.Chunks, ChunkRef{
			FileID:        fileID,
			StartNs:       int64(ci.MessageStartTime),
			EndNs:         int64(ci.MessageEndTime),
			Offset:        int64(ci.ChunkStartOffset),
			Length:        int64(ci.ChunkLength),
			Compression:   string(ci.Compression),
			ChannelTopics: topics,
		})
	}
	idx.fillEstimatedCounts(info)
	return idx
}

// fillEstimatedCounts populates per-chunk per-topic message-count ESTIMATES
// from the summary Statistics: each topic's file-level count is distributed
// evenly across the chunks that carry the topic (remainder to the earliest
// chunks, so per-file sums stay exact). Feeds ChunkRef.MessageCount and the
// PlanChunks filtered estimates — both surface only as the wire's
// approximate_messages. Files without Statistics get zero counts (same shape
// as the old no-message-index fallback).
func (idx *FileChunkIndex) fillEstimatedCounts(info *mcap.Info) {
	if info.Statistics == nil {
		idx.chunkCounts = make([]chunkCounts, len(idx.Chunks))
		return
	}
	// File-level per-topic totals (channels sharing a topic sum).
	topicTotal := make(map[string]uint64, len(idx.channels))
	for chID, n := range info.Statistics.ChannelMessageCounts {
		if c, ok := idx.channels[chID]; ok {
			topicTotal[c.topic] += n
		}
	}
	// How many chunks carry each topic.
	topicChunks := make(map[string]uint64, len(topicTotal))
	for _, ref := range idx.Chunks {
		for topic := range ref.ChannelTopics {
			topicChunks[topic]++
		}
	}
	remainder := make(map[string]uint64, len(topicTotal))
	for topic, total := range topicTotal {
		if n := topicChunks[topic]; n > 0 {
			remainder[topic] = total % n
		}
	}
	idx.chunkCounts = make([]chunkCounts, 0, len(idx.Chunks))
	for i := range idx.Chunks {
		ref := &idx.Chunks[i]
		perTopic := make(map[string]uint64, len(ref.ChannelTopics))
		var total uint64
		for topic := range ref.ChannelTopics {
			n := topicChunks[topic]
			if n == 0 {
				continue
			}
			share := topicTotal[topic] / n
			if remainder[topic] > 0 {
				share++
				remainder[topic]--
			}
			perTopic[topic] = share
			total += share
		}
		ref.MessageCount = total
		idx.chunkCounts = append(idx.chunkCounts, chunkCounts{exact: true, perTopic: perTopic})
	}
}

// chunkCounts carries the per-chunk per-topic message counts parallel to
// FileChunkIndex.Chunks (same index). exact is false when the chunk lacked
// message indexes (or one failed to read), in which case perTopic is empty and
// callers must use the chunk-level upper bound.
type chunkCounts struct {
	exact    bool
	perTopic map[string]uint64
}

// PlanChunks intersects this file's chunk index with the requested topic set and
// time range, returning the chunk refs the session must fetch (ordered by chunk
// offset). topics==nil/empty means "all topics in the file". Each returned ref's
// ChannelTopics is narrowed to the selected topics and MessageCount is the exact
// (or upper-bound) count over JUST those topics. See spec 6.3.
func (idx FileChunkIndex) PlanChunks(topics []string, tr *TimeWindow) []ChunkRef {
	wanted := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		wanted[t] = struct{}{}
	}
	wantAll := len(wanted) == 0

	out := make([]ChunkRef, 0, len(idx.Chunks))
	for i, c := range idx.Chunks {
		// Skip chunks that cannot contain an in-window message. c.EndNs is the
		// chunk's MessageEndTime (INCLUSIVE last log_time) and the window is
		// half-open [tr.StartNs, tr.EndNs): a chunk whose last message sits
		// exactly at tr.StartNs still intersects, so the lower test is strict
		// "<" (not "<="). c.StartNs >= tr.EndNs correctly excludes a chunk whose
		// first message is at/after the (excluded) window end.
		if tr != nil && (c.EndNs < tr.StartNs || c.StartNs >= tr.EndNs) {
			continue
		}
		sel := make(map[string]struct{})
		for topic := range c.ChannelTopics {
			if wantAll {
				sel[topic] = struct{}{}
			} else if _, ok := wanted[topic]; ok {
				sel[topic] = struct{}{}
			}
		}
		if len(sel) == 0 {
			continue
		}
		ref := c
		ref.ChannelTopics = sel
		// Exact selected count when message indexes were present; else fall back
		// to the chunk-level total (upper bound).
		var cc chunkCounts
		if i < len(idx.chunkCounts) {
			cc = idx.chunkCounts[i]
		}
		if cc.exact {
			var n uint64
			for topic := range sel {
				n += cc.perTopic[topic]
			}
			ref.MessageCount = n
		}
		// else: ref.MessageCount keeps the chunk-level total (upper bound).
		out = append(out, ref)
	}
	return out
}

// Iterate decodes the messages in one already-fetched chunk-record blob and
// calls emit for each message whose topic is in ref.ChannelTopics and whose
// log_time is within the (optional) time range. chunkBytes is the raw Range-GET
// result of [ref.Offset, ref.Offset+ref.Length): an MCAP Chunk *record*
// (opcode + len + body). Message.ChannelID is resolved through the file's global
// channel table, so chunks that don't re-declare channels iterate correctly.
//
// emit's returned error aborts iteration and is returned to the caller.
func (idx FileChunkIndex) Iterate(chunkBytes []byte, ref ChunkRef, tr *TimeWindow, emit func(RawMessage) error) error {
	records, err := decodeChunkRecords(chunkBytes)
	if err != nil {
		return err
	}

	// The per-message filter is the REQUESTED half-open window [tr.StartNs,
	// tr.EndNs) and nothing else. It must NOT be clamped to the chunk's own
	// [ref.StartNs, ref.EndNs]: ref.EndNs is the chunk's MessageEndTime — the
	// INCLUSIVE log_time of its last message — so clamping the exclusive upper
	// bound down to it would drop the last message of every chunk whose end
	// falls below tr.EndNs (a within-window, within-chunk message). PlanChunks
	// has already excluded chunks that don't intersect the window, so a bare
	// tr-window test here is both correct and sufficient.
	off := 0
	for off+recordHeaderLen <= len(records) {
		op := mcap.OpCode(records[off])
		bodyLen := binary.LittleEndian.Uint64(records[off+1 : off+recordHeaderLen])
		bodyStart := off + recordHeaderLen
		bodyEnd := bodyStart + int(bodyLen)
		if bodyEnd > len(records) {
			return fmt.Errorf("format: truncated record in chunk (op=0x%02x len=%d avail=%d)", op, bodyLen, len(records)-bodyStart)
		}
		body := records[bodyStart:bodyEnd]
		off = bodyEnd

		if op != mcap.OpMessage {
			continue
		}
		m, err := mcap.ParseMessage(body)
		if err != nil {
			return fmt.Errorf("format: parse message: %w", err)
		}
		ci, ok := idx.channels[m.ChannelID]
		if !ok {
			continue // channel unknown (shouldn't happen with a complete summary)
		}
		if _, want := ref.ChannelTopics[ci.topic]; !want {
			continue
		}
		lt := int64(m.LogTime)
		if tr != nil && (lt < tr.StartNs || lt >= tr.EndNs) {
			continue
		}
		if err := emit(RawMessage{
			Topic:           ci.topic,
			SchemaName:      ci.schemaName,
			SchemaEncoding:  ci.schemaEncoding,
			MessageEncoding: ci.messageEncoding,
			LogTimeNs:       lt,
			PublishTimeNs:   int64(m.PublishTime),
			Payload:         append([]byte(nil), m.Data...),
		}); err != nil {
			return err
		}
	}
	return nil
}

// decodeChunkRecords takes a raw Chunk-record blob (opcode + len + body) and
// returns the decompressed inner record stream (a concatenation of
// Schema/Channel/Message records, each opcode+len framed).
func decodeChunkRecords(chunkBytes []byte) ([]byte, error) {
	if len(chunkBytes) < recordHeaderLen {
		return nil, fmt.Errorf("format: chunk blob too short (%d bytes)", len(chunkBytes))
	}
	if mcap.OpCode(chunkBytes[0]) != mcap.OpChunk {
		return nil, fmt.Errorf("format: expected chunk record (op 0x06), got op 0x%02x", chunkBytes[0])
	}
	bodyLen := binary.LittleEndian.Uint64(chunkBytes[1:recordHeaderLen])
	bodyEnd := recordHeaderLen + int(bodyLen)
	if bodyEnd > len(chunkBytes) {
		return nil, fmt.Errorf("format: chunk record truncated (need %d, have %d)", bodyEnd, len(chunkBytes))
	}
	chunk, err := mcap.ParseChunk(chunkBytes[recordHeaderLen:bodyEnd])
	if err != nil {
		return nil, fmt.Errorf("format: parse chunk: %w", err)
	}
	return decompressChunk(chunk)
}

// decompressChunk returns the chunk's inner records, decompressing per the
// chunk-container codec. zstd via klauspost/compress, lz4 via pierrec/lz4.
func decompressChunk(chunk *mcap.Chunk) ([]byte, error) {
	switch mcap.CompressionFormat(chunk.Compression) {
	case mcap.CompressionNone:
		return chunk.Records, nil
	case mcap.CompressionZSTD:
		dec, err := zstd.NewReader(nil)
		if err != nil {
			return nil, fmt.Errorf("format: zstd reader: %w", err)
		}
		defer dec.Close()
		out := make([]byte, 0, chunk.UncompressedSize)
		out, err = dec.DecodeAll(chunk.Records, out)
		if err != nil {
			return nil, fmt.Errorf("format: zstd decode chunk: %w", err)
		}
		return out, nil
	case mcap.CompressionLZ4:
		// The MCAP spec's chunk compression "lz4" is the LZ4 *frame* format, not a
		// raw block: the foxglove writer emits frames (writer.go lz4.NewWriter) and
		// the foxglove lexer decodes them with lz4.NewReader. So we must use the
		// frame reader here — lz4.UncompressBlock (raw block) fails with
		// "invalid source or destination buffer too short" on a frame body.
		r := lz4.NewReader(bytes.NewReader(chunk.Records))
		out := make([]byte, 0, chunk.UncompressedSize)
		buf := bytes.NewBuffer(out)
		if _, err := io.Copy(buf, r); err != nil {
			return nil, fmt.Errorf("format: lz4 decode chunk: %w", err)
		}
		return buf.Bytes(), nil
	default:
		return nil, fmt.Errorf("format: unsupported chunk compression %q", chunk.Compression)
	}
}
