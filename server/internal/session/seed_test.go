package session

import (
	"context"
	"testing"
)

// A latched topic (/map, present only in the first chunk at t<100) must be
// SEEDED into a later window when IncludeLatched is set, and left out otherwise.
func TestBuildPlan_LatchedSeed(t *testing.T) {
	files := []FileRecord{{ID: 1, StartTimeNs: 0, EndTimeNs: 300}}
	indexes := []FileChunkIndex{{FileID: 1, Chunks: []ChunkRef{
		chunk(1, 0, 100, 0, 50, "/map", "/pose"), // chunk0: latched /map + /pose
		chunk(1, 100, 200, 50, 50, "/pose"),      // chunk1: /pose only
		chunk(1, 200, 300, 100, 50, "/pose"),     // chunk2: /pose only
	}}}
	win := &TimeWindow{StartNs: 150, EndNs: 300}

	// Strict window (no flag): /map (only at t<100) is correctly absent, no seed.
	plan, err := BuildPlan(context.Background(), files, indexes, PlanArgs{
		TopicNames: []string{"/map", "/pose"}, TimeRange: win})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.SeedChunks) != 0 {
		t.Fatalf("no seed expected without IncludeLatched, got %d", len(plan.SeedChunks))
	}

	// With the flag: /map is absent from the window -> seeded from chunk0; /pose
	// is present in the window -> NOT seeded.
	plan, err = BuildPlan(context.Background(), files, indexes, PlanArgs{
		TopicNames: []string{"/map", "/pose"}, TimeRange: win, IncludeLatched: true})
	if err != nil {
		t.Fatal(err)
	}
	if plan.SeedBeforeNs != 150 {
		t.Errorf("SeedBeforeNs: got %d want 150", plan.SeedBeforeNs)
	}
	if _, ok := plan.SeedTopics["/map"]; !ok {
		t.Errorf("/map should be a seed topic; got %v", plan.SeedTopics)
	}
	if _, ok := plan.SeedTopics["/pose"]; ok {
		t.Errorf("/pose is in the window and must NOT be seeded")
	}
	if len(plan.SeedChunks) != 1 || plan.SeedChunks[0].StartNs != 0 {
		t.Fatalf("expected one seed chunk [0,100], got %+v", plan.SeedChunks)
	}
	if _, ok := plan.SeedChunks[0].ChannelTopics["/map"]; !ok {
		t.Errorf("seed chunk must carry /map, got %v", plan.SeedChunks[0].ChannelTopics)
	}
	if plan.Empty() {
		t.Error("a plan with a seed must not be Empty()")
	}
}

// When the only requested topic is latched and absent from the window, the plan
// has zero window chunks but is NOT empty (it seeds) — this is the exact fix for
// "no messages in the selected time range" on latched topics like /map_amcl.
func TestBuildPlan_LatchedSeed_RescuesEmptyWindow(t *testing.T) {
	files := []FileRecord{{ID: 1, StartTimeNs: 0, EndTimeNs: 300}}
	indexes := []FileChunkIndex{{FileID: 1, Chunks: []ChunkRef{
		chunk(1, 0, 100, 0, 50, "/map"),     // latched only
		chunk(1, 100, 200, 50, 50, "/pose"), // unrelated active topic
	}}}
	win := &TimeWindow{StartNs: 150, EndNs: 300}
	plan, err := BuildPlan(context.Background(), files, indexes, PlanArgs{
		TopicNames: []string{"/map"}, TimeRange: win, IncludeLatched: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Chunks) != 0 {
		t.Fatalf("no in-window chunks expected for /map, got %d", len(plan.Chunks))
	}
	if plan.Empty() {
		t.Fatal("must NOT be empty: /map has a latched value to seed")
	}
	if len(plan.SeedChunks) != 1 {
		t.Errorf("expected one seed chunk, got %d", len(plan.SeedChunks))
	}
}
