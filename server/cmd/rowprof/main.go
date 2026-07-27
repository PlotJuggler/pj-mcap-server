// rowprof attributes ListFiles bytes to individual FileSummary fields.
//
// For each field it re-marshals a real page with that field cleared and reports
// the delta, both raw and after ZSTD (level 1, the server default). The
// compressed delta is the one that matters: long Hive s3_keys and repeated tag
// keys share prefixes, so their raw cost and their wire cost differ a lot.
//
// Usage: rowprof --url wss://host [--token T] [--limit 500]
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
	"nhooyr.io/websocket"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

func main() {
	url := flag.String("url", "ws://localhost:8080/api/ws", "server WebSocket URL")
	token := flag.String("token", os.Getenv("MCAP_CLOUD_API_KEY"), "bearer token")
	limit := flag.Uint("limit", 500, "page size")
	flag.Parse()
	if err := run(*url, *token, uint32(*limit)); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// clearers maps a field name to a mutator that removes it from a FileSummary.
var clearers = []struct {
	name  string
	clear func(*pb.FileSummary)
}{
	{"tags", func(f *pb.FileSummary) { f.Tags = nil }},
	{"s3_key", func(f *pb.FileSummary) { f.S3Key = "" }},
	{"recorded", func(f *pb.FileSummary) { f.Recorded = nil }},
	{"message_count", func(f *pb.FileSummary) { f.MessageCount = 0 }},
	{"size_bytes", func(f *pb.FileSummary) { f.SizeBytes = 0 }},
	{"id", func(f *pb.FileSummary) { f.Id = 0 }},
	{"topic_count", func(f *pb.FileSummary) { f.TopicCount = 0 }},
}

func run(url, token string, limit uint32) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return err
	}
	defer conn.CloseNow()
	conn.SetReadLimit(64 << 20)

	// Ask for RAW responses so we measure the payload, not an envelope.
	if _, err := rpc(ctx, conn, &pb.ClientMessage{
		RequestId: 1,
		Payload:   &pb.ClientMessage_Hello{Hello: &pb.Hello{ProtocolVersion: 2, AuthToken: token}},
	}); err != nil {
		return err
	}
	resp, err := rpc(ctx, conn, &pb.ClientMessage{
		RequestId: 2,
		Payload:   &pb.ClientMessage_ListFiles{ListFiles: &pb.ListFilesRequest{Limit: limit}},
	})
	if err != nil {
		return err
	}
	_ = conn.Close(websocket.StatusNormalClosure, "done")

	lf := resp.GetListFiles()
	if lf == nil {
		return fmt.Errorf("no ListFilesResponse (error: %v)", resp.GetError())
	}
	rows := len(lf.GetFiles())
	if rows == 0 {
		return fmt.Errorf("empty page")
	}

	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		return err
	}
	defer enc.Close()

	size := func(m *pb.ListFilesResponse) (int, int) {
		raw, _ := proto.Marshal(m)
		return len(raw), len(enc.EncodeAll(raw, nil))
	}
	baseRaw, baseZ := size(lf)

	// The top-level ListFilesResponse.metadata map (keyed by decimal file id) is
	// NOT a FileSummary field, so the per-field clearers below never touch it —
	// measure it explicitly.
	{
		clone := proto.Clone(lf).(*pb.ListFilesResponse)
		clone.Metadata = nil
		r, z := size(clone)
		fmt.Printf("top-level metadata map: %.1f raw B/row (%.1f%%), %.1f wire B/row (%.1f%%)\n\n",
			float64(baseRaw-r)/float64(rows), 100*float64(baseRaw-r)/float64(baseRaw),
			float64(baseZ-z)/float64(rows), 100*float64(baseZ-z)/float64(baseZ))
	}

	type row struct {
		name     string
		dRaw, dZ int
	}
	var out []row
	for _, c := range clearers {
		clone := proto.Clone(lf).(*pb.ListFilesResponse)
		for _, f := range clone.GetFiles() {
			c.clear(f)
		}
		r, z := size(clone)
		out = append(out, row{c.name, baseRaw - r, baseZ - z})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].dZ > out[j].dZ })

	// Tag-shape detail: how many tags per row, and how much of that is keys.
	var tagCount, keyBytes, valBytes int
	for _, f := range lf.GetFiles() {
		for _, t := range f.GetTags() {
			tagCount++
			keyBytes += len(t.GetKey())
			valBytes += len(t.GetValue())
		}
	}

	fmt.Printf("page: %d rows | raw %d B (%.0f B/row) | zstd %d B (%.0f B/row) | %.2fx\n\n",
		rows, baseRaw, float64(baseRaw)/float64(rows), baseZ, float64(baseZ)/float64(rows),
		float64(baseRaw)/float64(baseZ))
	fmt.Printf("%-14s %12s %8s %12s %8s\n", "field", "raw B/row", "raw %", "wire B/row", "wire %")
	for _, r := range out {
		fmt.Printf("%-14s %12.1f %7.1f%% %12.1f %7.1f%%\n", r.name,
			float64(r.dRaw)/float64(rows), 100*float64(r.dRaw)/float64(baseRaw),
			float64(r.dZ)/float64(rows), 100*float64(r.dZ)/float64(baseZ))
	}
	fmt.Printf("\ntags: %.1f per row, key %.0f B/row, value %.0f B/row (raw)\n",
		float64(tagCount)/float64(rows), float64(keyBytes)/float64(rows), float64(valBytes)/float64(rows))
	if n := len(lf.GetFiles()); n > 0 {
		f := lf.GetFiles()[0]
		fmt.Printf("sample s3_key (%d B): %s\n", len(f.GetS3Key()), f.GetS3Key())
		for i, t := range f.GetTags() {
			if i >= 8 {
				fmt.Printf("  ... +%d more tags\n", len(f.GetTags())-8)
				break
			}
			fmt.Printf("  tag %-22s = %s\n", t.GetKey(), t.GetValue())
		}
	}
	return nil
}

func rpc(ctx context.Context, conn *websocket.Conn, msg *pb.ClientMessage) (*pb.ServerMessage, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		return nil, err
	}
	_, respData, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	var resp pb.ServerMessage
	if err := proto.Unmarshal(respData, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}
