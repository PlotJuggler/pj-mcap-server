// Package session is the heart of the streaming subsystem: plan, producer,
// consumer, retain buffer, registry, eviction. See design spec 6.3-6.7 + 8.1-8.4
// and Plan A Tasks 18-22 + 31.
//
// LAYERING (deviation from Plan A's M1a, documented): the chunk-level primitives
// (ChunkRef, RawMessage, TimeWindow, TopicSchemaInfo) live in package format
// (see internal/format/chunks.go) and session imports them — the reverse of Plan
// A's M1a, which would have created an import cycle in our slice-1 layout
// (catalog already imports format). session aliases them so call sites read the
// same as Plan A. Multi-file stitching + the pairwise overlap rule + the
// empty-plan contract + pre-flight estimates all live HERE, above the per-file
// codec, exactly as Plan A specifies.
package session

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"pj-cloud/server/internal/format"
)

// Re-exported chunk-level types so session callers (and the WS layer) need not
// reach into package format directly. They are the SAME types — no conversion.
type (
	// TimeWindow is a half-open [StartNs, EndNs) range in unix nanoseconds.
	TimeWindow = format.TimeWindow
	// ChunkRef points at one MCAP chunk + the session-requested topics in it.
	ChunkRef = format.ChunkRef
	// RawMessage is the codec iterator's per-message output.
	RawMessage = format.RawMessage
	// TopicSchemaInfo is one topic's wire-binding info (name/encoding/bytes).
	TopicSchemaInfo = format.TopicSchemaInfo
	// FileChunkIndex is one file's chunk index + global channel/schema table.
	FileChunkIndex = format.FileChunkIndex
)

// FileRecord is the minimal catalog view BuildPlan needs: an id + the file's
// stitched time bounds (for the pairwise non-overlap check + merged horizon).
// The WS handler builds these from catalog entries; session does not import
// catalog (keeps the overlap rule storage/catalog-agnostic).
type FileRecord struct {
	ID          uint64
	StartTimeNs int64
	EndTimeNs   int64
}

// PlanArgs is the OpenFresh selection above the per-file codec.
type PlanArgs struct {
	TopicNames []string
	TimeRange  *TimeWindow
	// IncludeLatched requests latched/transient-local replay (OpenFresh.include_latched):
	// when set AND TimeRange != nil, BuildPlan also computes SeedChunks so the
	// producer can deliver, per requested topic absent from the window, its last
	// message BEFORE TimeRange.Start. See buildLatchedSeed.
	IncludeLatched bool
}

// Plan is BuildPlan's output: the ordered chunk list to fetch plus the
// pre-flight estimates that go into OpenSessionResponse.
type Plan struct {
	Chunks              []ChunkRef
	EstimatedChunkBytes uint64
	ApproximateMessages uint64
	MergedStartNs       int64
	MergedEndNs         int64

	// Latched-seed plan (only when PlanArgs.IncludeLatched). SeedChunks are chunks
	// ENTIRELY before SeedBeforeNs that hold the last-before-window message of a
	// seed topic; the producer scans them first and emits, per SeedTopics topic,
	// its newest message with log_time < SeedBeforeNs (in log_time order, before
	// the window stream). A seed topic is absent from every in-window chunk, so a
	// seed chunk never overlaps the main plan above.
	SeedChunks   []ChunkRef
	SeedTopics   map[string]struct{}
	SeedBeforeNs int64
}

// Empty reports whether the plan would produce zero messages (the empty-plan
// contract, spec 6.3: a normal OpenSessionResponse followed immediately by
// Eos{COMPLETE, total_messages_sent=0} — success, not error). A latched seed
// counts: a window with no in-range messages but a latched value to replay is
// NOT empty.
func (p Plan) Empty() bool { return len(p.Chunks) == 0 && len(p.SeedChunks) == 0 }

var errOverlap = errors.New("overlapping file time ranges")

// ErrIsOverlap reports whether err is the pairwise-overlap rejection (the WS
// handler maps it to Error{INVALID_REQUEST}).
func ErrIsOverlap(err error) bool { return errors.Is(err, errOverlap) }

// BuildPlan validates the file selection, intersects each file's chunk index
// with the requested topic set + time range, and returns an ordered chunk list
// with pre-flight estimates.
//
// Rules (spec 6.3 + 8.2):
//   - Files are ordered by start time; ranges must be pairwise NON-OVERLAPPING
//     (else errOverlap -> Error{INVALID_REQUEST}). The v1 "consecutive
//     recordings" use case never violates this.
//   - Requested topics absent from every file are silently dropped (not an
//     error). A plan with no surviving chunks is the EMPTY-PLAN case (Plan.Empty).
//   - estimated_chunk_bytes = sum of intersecting chunk lengths (compressed
//     on-disk bytes — the server fetch budget).
//   - approximate_messages = sum of per-chunk selected-topic counts (exact when
//     MessageIndex present, chunk-level upper bound otherwise — computed in the
//     codec's PlanChunks).
//
// indexes are the per-file chunk indexes (from format.Codec.ChunkIndex), one per
// selected file; files carries the time bounds for the overlap check.
func BuildPlan(ctx context.Context, files []FileRecord, indexes []FileChunkIndex, args PlanArgs) (Plan, error) {
	if len(files) == 0 {
		return Plan{}, nil
	}
	if args.TimeRange != nil && args.TimeRange.EndNs < args.TimeRange.StartNs {
		return Plan{}, fmt.Errorf("invalid time range: end %d < start %d", args.TimeRange.EndNs, args.TimeRange.StartNs)
	}

	ordered := append([]FileRecord(nil), files...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].StartTimeNs < ordered[j].StartTimeNs })

	// Pairwise non-overlap check (half-open ranges: file[i] must start at or
	// after file[i-1] ends).
	for i := 1; i < len(ordered); i++ {
		if ordered[i].StartTimeNs < ordered[i-1].EndTimeNs {
			return Plan{}, fmt.Errorf("%w: file %d ends at %d, file %d starts at %d",
				errOverlap,
				ordered[i-1].ID, ordered[i-1].EndTimeNs,
				ordered[i].ID, ordered[i].StartTimeNs)
		}
	}

	idxByFile := make(map[uint64]FileChunkIndex, len(indexes))
	for _, ix := range indexes {
		idxByFile[ix.FileID] = ix
	}

	plan := Plan{
		MergedStartNs: ordered[0].StartTimeNs,
		MergedEndNs:   ordered[len(ordered)-1].EndTimeNs,
	}

	for _, f := range ordered {
		idx, ok := idxByFile[f.ID]
		if !ok {
			continue
		}
		for _, ref := range idx.PlanChunks(args.TopicNames, args.TimeRange) {
			plan.Chunks = append(plan.Chunks, ref)
			plan.EstimatedChunkBytes += uint64(ref.Length)
			plan.ApproximateMessages += ref.MessageCount
		}
	}

	if args.IncludeLatched && args.TimeRange != nil {
		buildLatchedSeed(&plan, ordered, idxByFile, args)
	}
	return plan, nil
}

// buildLatchedSeed populates plan.Seed* (latched/transient-local replay). A
// "seed topic" is a requested topic with NO message inside the window (it is
// absent from every in-window plan chunk). For each, we pick the LATEST chunk
// that starts before the window and contains it; because the topic is absent
// from the window, that chunk lies entirely before TimeRange.Start, so it holds
// the topic's last-before-window message and never overlaps plan.Chunks. The
// producer scans these and emits the newest message per seed topic. A topic
// whose latest pre-window chunk is straddling (so it could not be a seed topic
// by construction) is handled by the normal window path instead.
func buildLatchedSeed(plan *Plan, ordered []FileRecord, idxByFile map[uint64]FileChunkIndex, args PlanArgs) {
	startNs := args.TimeRange.StartNs

	inWindow := make(map[string]struct{})
	for _, c := range plan.Chunks {
		for tn := range c.ChannelTopics {
			inWindow[tn] = struct{}{}
		}
	}

	// Requested set: explicit topic_names, or ALL topics across the selected files.
	requested := make(map[string]struct{})
	if len(args.TopicNames) > 0 {
		for _, tn := range args.TopicNames {
			requested[tn] = struct{}{}
		}
	} else {
		for _, f := range ordered {
			if ix, ok := idxByFile[f.ID]; ok {
				for _, c := range ix.Chunks {
					for tn := range c.ChannelTopics {
						requested[tn] = struct{}{}
					}
				}
			}
		}
	}

	seed := make(map[string]struct{})
	for tn := range requested {
		if _, ok := inWindow[tn]; !ok {
			seed[tn] = struct{}{}
		}
	}
	if len(seed) == 0 {
		return
	}

	// Latest pre-window chunk per seed topic (across files, by chunk StartNs).
	bestByTopic := make(map[string]ChunkRef)
	for _, f := range ordered {
		ix, ok := idxByFile[f.ID]
		if !ok {
			continue
		}
		for _, c := range ix.Chunks {
			if c.StartNs >= startNs {
				continue
			}
			for tn := range c.ChannelTopics {
				if _, want := seed[tn]; !want {
					continue
				}
				if prev, ok := bestByTopic[tn]; !ok || c.StartNs > prev.StartNs {
					bestByTopic[tn] = c
				}
			}
		}
	}

	// Coalesce topics that share a chunk into one seed ChunkRef (ChannelTopics
	// narrowed to the seed topics it covers), ordered by (FileID, Offset).
	type bucket struct {
		ref    ChunkRef
		topics map[string]struct{}
	}
	byKey := make(map[int64]*bucket)
	for tn, c := range bestByTopic {
		key := int64(c.FileID)<<40 ^ c.Offset
		b, ok := byKey[key]
		if !ok {
			ref := c
			ref.ChannelTopics = make(map[string]struct{})
			b = &bucket{ref: ref, topics: ref.ChannelTopics}
			byKey[key] = b
		}
		b.topics[tn] = struct{}{}
	}
	for _, b := range byKey {
		plan.SeedChunks = append(plan.SeedChunks, b.ref)
		plan.EstimatedChunkBytes += uint64(b.ref.Length)
		plan.ApproximateMessages += uint64(len(b.topics)) // ~1 latched message per topic
	}
	sort.Slice(plan.SeedChunks, func(i, j int) bool {
		if plan.SeedChunks[i].FileID != plan.SeedChunks[j].FileID {
			return plan.SeedChunks[i].FileID < plan.SeedChunks[j].FileID
		}
		return plan.SeedChunks[i].Offset < plan.SeedChunks[j].Offset
	})
	plan.SeedTopics = seed
	plan.SeedBeforeNs = startNs
}
