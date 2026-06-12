// Package indexer reconciles the S3/Minio bucket with the SQLite catalog: it
// lists objects, change-detects on the (etag, size, last_modified) triple, and
// (re-)extracts new/changed recordings into files/topics/tags_embedded rows,
// recording failures in indexer_failures. Tag OVERRIDES are never touched by a
// reindex (that is the point of the two-layer tag model — overrides survive).
//
// This REPLACES the in-memory catalog-lite's Scan/StartPolling. The
// change-detect rule, warm-start (serve existing rows, no re-extract), and the
// per-object decision log lines are the harness contract (smoke step g).
package indexer

import (
	"context"
	"sort"
	"time"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/storage"
)

// S3Object is the listing view of one bucket object (the change-detect triple).
type S3Object struct {
	Key          string
	ETag         string
	Size         int64
	LastModified time.Time
}

// Lister returns the recording objects under a prefix.
type Lister interface {
	List(ctx context.Context, prefix string) ([]S3Object, error)
}

// SummaryExtractor extracts one recording's catalog view. Production wraps
// format.Codec + storage.BlobStore (ranged summary reads, never the whole
// object); tests can fake it.
type SummaryExtractor interface {
	Extract(ctx context.Context, key string) (format.FileSummary, error)
}

// ExtractResult is the indexer's mapping of a codec summary onto catalog rows.
type ExtractResult struct {
	File         catalog.FileRecord
	Topics       []catalog.TopicRecord
	EmbeddedTags []catalog.TagKV
}

// isRecording reports whether a key should be cataloged (MCAP files only).
func isRecording(key string) bool {
	return len(key) >= 5 && key[len(key)-5:] == ".mcap"
}

// --- production adapters over the storage/format seams ----------------------

// NewBlobStoreLister adapts storage.BlobStore.List (paginated) to Lister,
// filtering to recordings (.mcap). It mirrors the catalog-lite startup scan.
func NewBlobStoreLister(bs storage.BlobStore) Lister { return &bsLister{bs: bs} }

type bsLister struct{ bs storage.BlobStore }

func (l *bsLister) List(ctx context.Context, prefix string) ([]S3Object, error) {
	var out []S3Object
	token := ""
	for {
		objs, next, err := l.bs.List(ctx, prefix, token)
		if err != nil {
			return nil, err
		}
		for _, o := range objs {
			if isRecording(o.Key) {
				out = append(out, S3Object{
					Key:          o.Key,
					ETag:         o.ETag,
					Size:         o.Size,
					LastModified: time.Unix(0, o.LastModifiedNs),
				})
			}
		}
		if next == "" {
			break
		}
		token = next
	}
	// Deterministic order so a cold-start assigns rowids in sorted-key order for a
	// fixed corpus (keeps ids stable run-to-run; see store_design risk note).
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// NewCodecExtractor adapts format.Codec over a BlobStore to SummaryExtractor.
// When idxCache is non-nil, each extraction ALSO builds the chunk index from
// the SAME summary read and pre-warms the cache — so a later OpenSession plans
// from memory with no WAN read (the indexer already paid for the summary). The
// chunk index is cached under the placeholder file id 0; the session read path
// restamps it to the real catalog id.
func NewCodecExtractor(bs storage.BlobStore, codec format.Codec, idxCache *format.ChunkIndexCache) SummaryExtractor {
	return &codecExtractor{bs: bs, codec: codec, idxCache: idxCache}
}

type codecExtractor struct {
	bs       storage.BlobStore
	codec    format.Codec
	idxCache *format.ChunkIndexCache
}

func (e *codecExtractor) Extract(ctx context.Context, key string) (format.FileSummary, error) {
	if e.idxCache == nil {
		return e.codec.Extract(ctx, e.bs, key)
	}
	summary, idx, err := e.codec.ExtractAndIndex(ctx, e.bs, key, 0)
	if err != nil {
		return format.FileSummary{}, err
	}
	e.idxCache.Put(key, summary.ETag, idx)
	return summary, nil
}

// summaryToResult maps a codec FileSummary onto catalog rows. The S3-object
// fields (key/etag/last_modified) are filled by the scanner from the listing,
// not the summary, since the summary's ETag is only the head's.
func summaryToResult(fs format.FileSummary) ExtractResult {
	topics := make([]catalog.TopicRecord, 0, len(fs.Topics))
	for _, t := range fs.Topics {
		topics = append(topics, catalog.TopicRecord{
			Name:           t.Name,
			SchemaName:     t.SchemaName,
			SchemaEncoding: t.SchemaEncoding,
			MessageCount:   t.MessageCount,
		})
	}
	tags := make([]catalog.TagKV, 0, len(fs.Metadata))
	keys := make([]string, 0, len(fs.Metadata))
	for k := range fs.Metadata {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic embedded-tag insertion order
	for _, k := range keys {
		tags = append(tags, catalog.TagKV{Key: k, Value: fs.Metadata[k]})
	}
	return ExtractResult{
		File: catalog.FileRecord{
			SizeBytes:    fs.Size,
			StartTimeNs:  fs.StartNs,
			EndTimeNs:    fs.EndNs,
			ChunkCount:   fs.ChunkCount,
			MessageCount: fs.MessageCount,
			// HasMessageIndex: "summary present" => true when there are chunks.
			// Not load-bearing in this slice (chunk index is on-demand).
			HasMessageIndex: fs.ChunkCount > 0,
			McapSummary:     nil, // see store.go DESIGN NOTE: NULL in this slice.
		},
		Topics:       topics,
		EmbeddedTags: tags,
	}
}
