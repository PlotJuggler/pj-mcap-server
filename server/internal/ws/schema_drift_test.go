package ws

import (
	"bytes"
	"strings"
	"testing"

	"pj-cloud/server/internal/genmcap"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// driftFile builds one synthetic MCAP whose /drift topic carries the given
// schema name (genmcap synthesizes SchemaData from the name, so a different
// name is a full name+data drift).
func driftFile(t *testing.T, key string, startSec int64, schemaName string) []byte {
	t.Helper()
	const sec = int64(1_000_000_000)
	var buf bytes.Buffer
	spec := genmcap.FileSpec{
		Key:     key,
		StartNs: startSec * sec,
		StepNs:  sec / 10,
		Topics: []genmcap.TopicSpec{
			{Topic: "/drift", SchemaName: schemaName, SchemaEnc: "ros2msg", MessageCount: 10},
		},
	}
	if err := genmcap.Write(&buf, spec); err != nil {
		t.Fatalf("genmcap.Write %s: %v", key, err)
	}
	return buf.Bytes()
}

// A stitched selection where the SAME topic carries DIFFERENT schemas across
// files is unrepresentable in the session model (one schema binding per topic;
// first-occurrence-wins would silently decode file B's bytes with file A's
// schema — pre-merge review, Codex architectural finding #2). The open must be
// REJECTED, not silently mis-bound.
func TestOpenSession_SchemaDriftAcrossFilesRejected(t *testing.T) {
	files := map[string][]byte{
		"a.mcap": driftFile(t, "a.mcap", 1000, "pkg/msg/V1"),
		"b.mcap": driftFile(t, "b.mcap", 2000, "pkg/msg/V2"),
	}
	ts := newTestServer(t, files, defaultTestSessionCfg())
	c := dialClient(t, ts.url)
	c.hello()

	c.send(&pb.ClientMessage{RequestId: 50, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{S3Keys: fileKeys("a.mcap", "b.mcap")},
		}},
	}})
	resp := c.recv()
	e := resp.GetError()
	if e == nil {
		t.Fatalf("expected Error{INVALID_REQUEST} for schema drift, got %T", resp.GetPayload())
	}
	if e.GetCode() != pb.ErrorCode_ERROR_INVALID_REQUEST {
		t.Fatalf("expected INVALID_REQUEST, got %v (%q)", e.GetCode(), e.GetMessage())
	}
	if !strings.Contains(e.GetMessage(), "/drift") {
		t.Errorf("drift error must name the topic: %q", e.GetMessage())
	}
}

// Control: the SAME schema across stitched files must still open fine.
func TestOpenSession_SameSchemaAcrossFilesAccepted(t *testing.T) {
	files := map[string][]byte{
		"a.mcap": driftFile(t, "a.mcap", 1000, "pkg/msg/V1"),
		"b.mcap": driftFile(t, "b.mcap", 2000, "pkg/msg/V1"),
	}
	ts := newTestServer(t, files, defaultTestSessionCfg())
	c := dialClient(t, ts.url)
	c.hello()

	c.send(&pb.ClientMessage{RequestId: 51, Payload: &pb.ClientMessage_OpenSession{
		OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{
			Fresh: &pb.OpenFresh{S3Keys: fileKeys("a.mcap", "b.mcap")},
		}},
	}})
	resp := c.recv()
	if resp.GetOpenSession() == nil {
		t.Fatalf("same-schema stitch must open, got %T (err=%v)", resp.GetPayload(), resp.GetError())
	}
}
