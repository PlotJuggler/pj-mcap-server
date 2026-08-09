package ws

import (
	"testing"

	"google.golang.org/protobuf/proto"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// Catalog responses MUST be marshaled deterministically before compression.
//
// ListFilesResponse.metadata is a protobuf map with ~8 derived keys per file.
// Go randomizes map iteration, so plain proto.Marshal emits those keys in a
// different order for every file — identical key sets landing at different
// offsets, which defeats ZSTD's cross-file matching. Measured on a 32,000-file
// listing: 1,111,112 B on the wire non-deterministic vs 512,385 B deterministic,
// from the SAME 15,163,143 marshaled bytes (13.6x -> 29.6x compression).
//
// Nothing else catches a regression here: dropping Deterministic changes no
// behaviour, breaks no test, and silently doubles catalog bandwidth. This fails
// if the compression path stops producing stable bytes.
func TestCatalogResponsesMarshalDeterministically(t *testing.T) {
	// A response shaped like a real ListFiles page: several files, each with the
	// full derived-key map. One key order is not enough — the bug only shows
	// across repeated marshals of the same message.
	msg := &pb.ServerMessage{
		RequestId: 7,
		Payload: &pb.ServerMessage_ListFiles{ListFiles: &pb.ListFilesResponse{
			Metadata: map[string]*pb.FlatMetadata{},
		}},
	}
	lf := msg.GetListFiles()
	for i := 0; i < 24; i++ {
		lf.Files = append(lf.Files, &pb.FileSummary{
			Id:    uint64(i),
			S3Key: "customer=c/customer_site=s/robot=r/source=src/date=2026-07-13/f.mcap",
			Tags:  []*pb.Tag{{Key: "vehicle", Value: "arri-182"}},
		})
		lf.Metadata[string(rune('0'+i%10))+"x"] = &pb.FlatMetadata{Entries: map[string]string{
			"s3_key":     "customer=c/customer_site=s/robot=r/source=src/date=2026-07-13/f.mcap",
			"size_bytes": "2200000", "message_count": "14904", "topic_count": "174",
			"chunk_count": "10", "duration_ns": "60000000000",
			"start_ns": "1752000000000000000", "end_ns": "1752000060000000000",
		}}
	}

	// marshalForCompression is what the compression path uses.
	first, err := marshalForCompression(msg)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	for i := 0; i < 40; i++ {
		again, err := marshalForCompression(msg)
		if err != nil {
			t.Fatalf("marshal %d: %v", i, err)
		}
		if string(again) != string(first) {
			t.Fatalf("catalog response marshaling is NOT deterministic (differs on iteration %d). "+
				"Map fields are emitted in Go's randomized order, which defeats ZSTD's cross-file "+
				"matching and roughly doubles catalog bandwidth (measured 512 KB -> 1,111 KB at 32k files).", i)
		}
	}

	// Guard against the test passing for the wrong reason: plain proto.Marshal on
	// this same message must NOT be stable, or the map is too small to detect the
	// difference and this test proves nothing.
	unstable := false
	base, _ := proto.Marshal(msg)
	for i := 0; i < 40 && !unstable; i++ {
		other, _ := proto.Marshal(msg)
		unstable = string(other) != string(base)
	}
	if !unstable {
		t.Skip("plain proto.Marshal happened to be stable for this fixture — " +
			"the determinism assertion above cannot distinguish the two paths here")
	}
}
