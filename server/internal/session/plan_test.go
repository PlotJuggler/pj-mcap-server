package session

import (
	"context"
	"testing"
)

// chunk is a tiny ChunkRef constructor for plan tests.
func chunk(fileID uint64, start, end, off, length int64, topics ...string) ChunkRef {
	ct := map[string]struct{}{}
	for _, t := range topics {
		ct[t] = struct{}{}
	}
	return ChunkRef{FileID: fileID, StartNs: start, EndNs: end, Offset: off, Length: length, ChannelTopics: ct, MessageCount: uint64(len(topics))}
}

func TestBuildPlan_RejectsOverlappingFiles(t *testing.T) {
	files := []FileRecord{
		{ID: 1, StartTimeNs: 0, EndTimeNs: 1000},
		{ID: 2, StartTimeNs: 500, EndTimeNs: 1500}, // overlaps file 1
	}
	_, err := BuildPlan(context.Background(), files, nil, PlanArgs{})
	if err == nil {
		t.Fatal("expected overlap error")
	}
	if !ErrIsOverlap(err) {
		t.Errorf("error not classified as overlap: %v", err)
	}
}

func TestBuildPlan_OrdersByStartAndSumsEstimates(t *testing.T) {
	files := []FileRecord{
		{ID: 2, StartTimeNs: 1000, EndTimeNs: 2000},
		{ID: 1, StartTimeNs: 0, EndTimeNs: 900},
	}
	indexes := []FileChunkIndex{
		{FileID: 1, Chunks: []ChunkRef{chunk(1, 0, 900, 100, 50, "/x")}},
		{FileID: 2, Chunks: []ChunkRef{chunk(2, 1000, 2000, 100, 60, "/x")}},
	}
	plan, err := BuildPlan(context.Background(), files, indexes, PlanArgs{TopicNames: []string{"/x"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Chunks) != 2 {
		t.Fatalf("chunks: %d", len(plan.Chunks))
	}
	if plan.Chunks[0].FileID != 1 || plan.Chunks[1].FileID != 2 {
		t.Errorf("not time-ordered: %+v", plan.Chunks)
	}
	if plan.EstimatedChunkBytes != 110 {
		t.Errorf("estimated_chunk_bytes: got %d want 110", plan.EstimatedChunkBytes)
	}
	if plan.MergedStartNs != 0 || plan.MergedEndNs != 2000 {
		t.Errorf("merged horizon: [%d,%d] want [0,2000]", plan.MergedStartNs, plan.MergedEndNs)
	}
	if plan.Empty() {
		t.Error("plan should not be empty")
	}
}

func TestBuildPlan_AbsentTopicYieldsEmptyPlan(t *testing.T) {
	files := []FileRecord{{ID: 1, StartTimeNs: 0, EndTimeNs: 1000}}
	indexes := []FileChunkIndex{{FileID: 1, Chunks: []ChunkRef{chunk(1, 0, 500, 0, 50, "/x")}}}
	plan, err := BuildPlan(context.Background(), files, indexes, PlanArgs{TopicNames: []string{"/absent"}})
	if err != nil {
		t.Fatalf("absent topic is silently dropped, not an error: %v", err)
	}
	if !plan.Empty() {
		t.Errorf("expected empty plan, got %+v", plan.Chunks)
	}
	if plan.EstimatedChunkBytes != 0 || plan.ApproximateMessages != 0 {
		t.Errorf("empty plan estimates must be zero: bytes=%d msgs=%d", plan.EstimatedChunkBytes, plan.ApproximateMessages)
	}
}

func TestBuildPlan_TimeRangeIntersectsChunks(t *testing.T) {
	files := []FileRecord{{ID: 1, StartTimeNs: 0, EndTimeNs: 10_000}}
	indexes := []FileChunkIndex{{FileID: 1, Chunks: []ChunkRef{
		chunk(1, 0, 1000, 0, 100, "/x"),
		chunk(1, 5000, 6000, 100, 200, "/x"),
	}}}
	plan, err := BuildPlan(context.Background(), files, indexes, PlanArgs{
		TopicNames: []string{"/x"},
		TimeRange:  &TimeWindow{StartNs: 4000, EndNs: 7000},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Chunks) != 1 || plan.Chunks[0].StartNs != 5000 {
		t.Errorf("expected only the [5000,6000] chunk, got %+v", plan.Chunks)
	}
}

func TestBuildPlan_RejectsInvertedTimeRange(t *testing.T) {
	files := []FileRecord{{ID: 1, StartTimeNs: 0, EndTimeNs: 1000}}
	_, err := BuildPlan(context.Background(), files, nil, PlanArgs{TimeRange: &TimeWindow{StartNs: 900, EndNs: 100}})
	if err == nil {
		t.Fatal("expected error for end < start time range")
	}
	if ErrIsOverlap(err) {
		t.Error("inverted time range should not be classified as overlap")
	}
}

func TestBuildPlan_EmptySelection(t *testing.T) {
	plan, err := BuildPlan(context.Background(), nil, nil, PlanArgs{})
	if err != nil {
		t.Fatal(err)
	}
	if !plan.Empty() {
		t.Error("no files => empty plan")
	}
}
