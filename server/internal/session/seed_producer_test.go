package session

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// The producer must run the SEED phase first: for each seed topic, emit its
// newest message before SeedBeforeNs (the latched value), in log_time order,
// BEFORE the window stream. Here /map has two pre-window updates (t=50, t=100);
// only the latest (t=100) is replayed, then the window's /pose@200.
func TestProducer_LatchedSeedEmitsLastBeforeWindow(t *testing.T) {
	seedChunk := chunk(1, 0, 100, 0, 50, "/map")   // pre-window chunk
	winChunk := chunk(1, 150, 250, 1, 50, "/pose") // in-window chunk
	iter := fakeIter{byOffset: map[int64][]RawMessage{
		0: {msg("/map", 50, "old-map"), msg("/map", 100, "new-map")},
		1: {msg("/pose", 200, "pose")},
	}}
	plan := Plan{
		Chunks:       []ChunkRef{winChunk},
		SeedChunks:   []ChunkRef{seedChunk},
		SeedTopics:   map[string]struct{}{"/map": {}},
		SeedBeforeNs: 150,
	}
	rb := NewRetainBuffer(RetainOpts{MaxSeqs: 16, MaxBytes: 1 << 20})
	p := &Producer{
		Plan: plan, ChunkReader: fakeReader{}, ChunkIter: iter, Retain: rb,
		Opts: ProducerOpts{MaxBatchBytes: 1 << 16, MaxMessageBytes: 1 << 20, CompressThresholdBytes: 4096,
			TimeRange: &TimeWindow{StartNs: 150, EndNs: 250}},
		TopicID:  map[string]uint32{"/map": 1, "/pose": 2},
		SchemaID: map[string]uint32{"/map": 10, "/pose": 20},
	}
	runProducer(t, p)

	// Batch 1: the seed — /map's LAST-before-window message (t=100, "new-map").
	env, ok := rb.Next(context.Background(), 0)
	if !ok {
		t.Fatal("no seed batch produced")
	}
	var b1 pb.MessageBatch
	if err := proto.Unmarshal(env.Payload, &b1); err != nil {
		t.Fatal(err)
	}
	m1 := decodeBatchBody(t, &b1)
	if len(m1) != 1 || m1[0].TopicId != 1 || m1[0].LogTimeNs != 100 || string(m1[0].Payload) != "new-map" {
		t.Fatalf("seed batch must be /map@100 (new-map); got %+v", m1)
	}

	// Batch 2: the window message /pose@200.
	env2, ok := rb.Next(context.Background(), env.Seq)
	if !ok {
		t.Fatal("no window batch produced")
	}
	var b2 pb.MessageBatch
	if err := proto.Unmarshal(env2.Payload, &b2); err != nil {
		t.Fatal(err)
	}
	m2 := decodeBatchBody(t, &b2)
	if len(m2) != 1 || m2[0].TopicId != 2 || m2[0].LogTimeNs != 200 {
		t.Fatalf("window batch must be /pose@200; got %+v", m2)
	}
}
