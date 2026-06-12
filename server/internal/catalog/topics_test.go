package catalog

import (
	"context"
	"testing"
)

func TestReplaceTopicsForFile_Insert(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	fid, _, err := UpsertFile(ctx, s, minimalRec("k", "e", 1))
	if err != nil {
		t.Fatal(err)
	}

	topics := []TopicRecord{
		{Name: "/imu/data", SchemaName: "sensor_msgs/Imu", SchemaEncoding: "ros2msg", MessageCount: 1000},
		{Name: "/gps/fix", SchemaName: "sensor_msgs/NavSatFix", SchemaEncoding: "ros2msg", MessageCount: 50},
	}
	if err := ReplaceTopicsForFile(ctx, s, fid, topics); err != nil {
		t.Fatalf("ReplaceTopicsForFile: %v", err)
	}

	got, err := ListTopicsForFile(ctx, s, fid)
	if err != nil {
		t.Fatalf("ListTopicsForFile: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len: got %d want 2", len(got))
	}
	// name-sorted: /gps/fix before /imu/data.
	if got[0].Name != "/gps/fix" || got[1].Name != "/imu/data" {
		t.Errorf("not name-sorted: %+v", got)
	}
}

func TestReplaceTopicsForFile_ReplacesAll(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	fid, _, _ := UpsertFile(ctx, s, minimalRec("k", "e", 1))

	_ = ReplaceTopicsForFile(ctx, s, fid, []TopicRecord{
		{Name: "/a", SchemaName: "A", SchemaEncoding: "ros2msg", MessageCount: 1},
		{Name: "/b", SchemaName: "B", SchemaEncoding: "ros2msg", MessageCount: 2},
	})
	_ = ReplaceTopicsForFile(ctx, s, fid, []TopicRecord{
		{Name: "/b", SchemaName: "B", SchemaEncoding: "ros2msg", MessageCount: 22},
	})
	got, _ := ListTopicsForFile(ctx, s, fid)
	if len(got) != 1 || got[0].Name != "/b" || got[0].MessageCount != 22 {
		t.Errorf("replace did not drop absent topics: %+v", got)
	}
}

func TestReplaceTopicsForFile_Empty(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	fid, _, _ := UpsertFile(ctx, s, minimalRec("k", "e", 1))
	if err := ReplaceTopicsForFile(ctx, s, fid, nil); err != nil {
		t.Fatal(err)
	}
	got, _ := ListTopicsForFile(ctx, s, fid)
	if len(got) != 0 {
		t.Errorf("expected no topics, got %+v", got)
	}
}
