// Package format is the FormatCodec seam: it turns an opaque blob into the
// catalog's view of a recording. v1 ships exactly one impl, MCAP, built on
// github.com/foxglove/mcap/go/mcap. Extraction is summary-only — it reads the
// footer/summary section through ranged reads (storage.ReaderAt), never the
// whole object — so cataloging a multi-GB recording costs a few KB of transfer.
//
// SCOPE NOTE: this is the Extract half of Plan A's FormatCodec (Task 15a). The
// PlanChunks/Iterate streaming half lands with the session subsystem; the
// method names here match Plan A so it slots in unchanged.
package format

import (
	"context"
	"fmt"
	"sort"

	"github.com/foxglove/mcap/go/mcap"

	"pj-cloud/server/internal/storage"
)

// TopicInfo is one topic's catalog view (maps onto wire TopicInfo).
type TopicInfo struct {
	Name           string
	SchemaName     string
	SchemaEncoding string
	MessageCount   uint64
}

// FileSummary is the codec's full extract of one recording.
type FileSummary struct {
	Key          string
	Size         int64
	ETag         string
	StartNs      int64
	EndNs        int64
	MessageCount uint64
	ChunkCount   uint32
	TopicCount   uint32
	Topics       []TopicInfo
	// Metadata is the recording's embedded key->value metadata (union of all
	// MCAP Metadata records). The catalog/ws layer augments this with derived
	// entries before sending it on the wire.
	Metadata map[string]string
}

// Codec extracts catalog metadata from a recording (Extract, the catalog half)
// and the per-file chunk index used by the session streaming subsystem
// (ChunkIndex, the streaming half — see chunks.go). One impl, MCAP.
type Codec interface {
	Extract(ctx context.Context, bs storage.BlobStore, key string) (FileSummary, error)
	// ChunkIndex reads the recording's summary section (chunk + message indexes,
	// channel/schema tables) over ranged reads and returns the per-file chunk
	// index the session layer plans + iterates over. fileID is stamped onto every
	// ChunkRef. It never reads chunk bodies.
	ChunkIndex(ctx context.Context, bs storage.BlobStore, key string, fileID uint64) (FileChunkIndex, error)
	// ExtractAndIndex produces BOTH the catalog summary and the streaming chunk
	// index from a SINGLE summary read — a background scanner (the retired Go
	// in-process indexer; today, test fixtures standing in for the external
	// Python builder's own scan) uses it to pre-warm the chunk-index cache at
	// no extra WAN cost. The live server's own chunk-index pre-warm (M4,
	// internal/warm) instead calls ChunkIndex directly.
	ExtractAndIndex(ctx context.Context, bs storage.BlobStore, key string, fileID uint64) (FileSummary, FileChunkIndex, error)
}

// NewCodec returns the codec for kind ("mcap" or "" => mcap).
func NewCodec(kind string) (Codec, error) {
	switch kind {
	case "", "mcap":
		return mcapCodec{}, nil
	default:
		return nil, fmt.Errorf("format: unsupported codec %q", kind)
	}
}

type mcapCodec struct{}

// openSummary does the shared WAN phase for Extract / ChunkIndex /
// ExtractAndIndex: Head + the summarySource (footer + summary section in a
// handful of ranged GETs) + mcap.NewReader + Info(). The caller owns the
// returned reader (must Close it). summarySource collapses what used to be a
// one-GET-per-Read scan into ~2 ranged GETs.
func openSummary(ctx context.Context, bs storage.BlobStore, key string) (storage.ObjectInfo, *mcap.Reader, *mcap.Info, error) {
	head, err := bs.Head(ctx, key)
	if err != nil {
		return storage.ObjectInfo{}, nil, nil, fmt.Errorf("format: head %q: %w", key, err)
	}
	rs, err := newSummarySource(ctx, bs, key, head.Size)
	if err != nil {
		return storage.ObjectInfo{}, nil, nil, err
	}
	reader, err := mcap.NewReader(rs)
	if err != nil {
		return storage.ObjectInfo{}, nil, nil, fmt.Errorf("format: open mcap %q: %w", key, err)
	}
	info, err := reader.Info()
	if err != nil {
		reader.Close()
		return storage.ObjectInfo{}, nil, nil, fmt.Errorf("format: read mcap summary %q: %w", key, err)
	}
	return head, reader, info, nil
}

// Extract reads the MCAP summary section over ranged reads and builds a
// FileSummary. It requires the object's size (one Head) so the seekable reader
// can address the footer.
func (c mcapCodec) Extract(ctx context.Context, bs storage.BlobStore, key string) (FileSummary, error) {
	head, reader, info, err := openSummary(ctx, bs, key)
	if err != nil {
		return FileSummary{}, err
	}
	defer reader.Close()
	return c.summaryFromInfo(key, head, reader, info)
}

// summaryFromInfo builds the catalog FileSummary from an already-read mcap Info.
// reader is used only for the (usually absent) embedded metadata records.
func (mcapCodec) summaryFromInfo(key string, head storage.ObjectInfo, reader *mcap.Reader, info *mcap.Info) (FileSummary, error) {
	if info.Statistics == nil {
		return FileSummary{}, fmt.Errorf("format: mcap %q has no statistics record (not summarized)", key)
	}

	summary := FileSummary{
		Key:          key,
		Size:         head.Size,
		ETag:         head.ETag,
		StartNs:      int64(info.Statistics.MessageStartTime),
		EndNs:        int64(info.Statistics.MessageEndTime),
		MessageCount: info.Statistics.MessageCount,
		ChunkCount:   info.Statistics.ChunkCount,
		Metadata:     map[string]string{},
	}

	// One TopicInfo per channel, joined to its schema, with per-channel counts.
	topics := make([]TopicInfo, 0, len(info.Channels))
	for chanID, ch := range info.Channels {
		t := TopicInfo{
			Name:         ch.Topic,
			MessageCount: info.Statistics.ChannelMessageCounts[chanID],
		}
		if sch := info.Schemas[ch.SchemaID]; sch != nil {
			t.SchemaName = sch.Name
			t.SchemaEncoding = sch.Encoding
		}
		topics = append(topics, t)
	}
	sort.Slice(topics, func(i, j int) bool { return topics[i].Name < topics[j].Name })
	summary.Topics = topics
	summary.TopicCount = uint32(len(topics))

	// Embedded metadata records (read each via its index offset). Later names
	// win on key collision; the recordings here typically carry none.
	for _, mi := range info.MetadataIndexes {
		md, mErr := reader.GetMetadata(mi.Offset)
		if mErr != nil {
			// Non-fatal: metadata is best-effort for the catalog view.
			continue
		}
		for k, v := range md.Metadata {
			summary.Metadata[k] = v
		}
	}

	return summary, nil
}
