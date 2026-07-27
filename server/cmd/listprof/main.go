// listprof decomposes the cost of a full ListFiles pagination sweep.
//
// It answers three questions the plain CLI cannot: how much of the wall clock is
// round-trip latency (page count x RTT) versus server think-time, how many bytes
// actually cross the wire, and how much the ZSTD response envelope is saving.
//
// Usage:
//
//	listprof --url wss://host [--token T] [--limit 200] [--zstd] [--pages N]
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
	limit := flag.Uint("limit", 200, "page size (ListFiles.limit); 0 = server default")
	useZstd := flag.Bool("zstd", true, "negotiate the ZSTD response envelope at Hello")
	maxPages := flag.Int("pages", 0, "stop after N pages (0 = all)")
	flag.Parse()

	if err := run(*url, *token, uint32(*limit), *useZstd, *maxPages); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

type pageStat struct {
	latency  time.Duration
	wireLen  int // bytes actually read off the socket
	plainLen int // bytes after ZSTD inflate (== wireLen when uncompressed)
	rows     int
}

func run(url, token string, limit uint32, useZstd bool, maxPages int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	dialStart := time.Now()
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", url, err)
	}
	defer conn.CloseNow()
	// nhooyr defaults to a 32 KiB read limit; a 1000-row page exceeds that.
	conn.SetReadLimit(64 << 20)
	dialTime := time.Since(dialStart)

	dec, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer dec.Close()

	hello := &pb.Hello{ProtocolVersion: 2, AuthToken: token}
	if useZstd {
		hello.AcceptedResponseEncodings = []pb.CompressionEncoding{pb.CompressionEncoding_COMPRESSION_ENCODING_ZSTD}
	}
	helloStart := time.Now()
	if _, _, _, err := rpc(ctx, conn, dec, &pb.ClientMessage{
		RequestId: 1, Payload: &pb.ClientMessage_Hello{Hello: hello},
	}); err != nil {
		return fmt.Errorf("hello: %w", err)
	}
	helloTime := time.Since(helloStart)

	var stats []pageStat
	var pageToken string
	reqID := uint64(2)
	sweepStart := time.Now()

	for {
		start := time.Now()
		resp, wireLen, plainLen, err := rpc(ctx, conn, dec, &pb.ClientMessage{
			RequestId: reqID,
			Payload: &pb.ClientMessage_ListFiles{ListFiles: &pb.ListFilesRequest{
				Limit: limit, PageToken: pageToken,
			}},
		})
		if err != nil {
			return err
		}
		elapsed := time.Since(start)
		reqID++

		lf := resp.GetListFiles()
		if lf == nil {
			return fmt.Errorf("expected ListFilesResponse, got %T (error: %v)", resp.Payload, resp.GetError())
		}
		stats = append(stats, pageStat{elapsed, wireLen, plainLen, len(lf.GetFiles())})

		pageToken = lf.GetNextPageToken()
		if pageToken == "" || (maxPages > 0 && len(stats) >= maxPages) {
			break
		}
	}
	sweep := time.Since(sweepStart)
	_ = conn.Close(websocket.StatusNormalClosure, "done")

	report(stats, dialTime, helloTime, sweep, limit, useZstd)
	return nil
}

func report(stats []pageStat, dial, hello, sweep time.Duration, limit uint32, useZstd bool) {
	var wire, plain, rows int
	lat := make([]time.Duration, 0, len(stats))
	for _, s := range stats {
		wire += s.wireLen
		plain += s.plainLen
		rows += s.rows
		lat = append(lat, s.latency)
	}
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })

	pct := func(p float64) time.Duration {
		if len(lat) == 0 {
			return 0
		}
		i := int(float64(len(lat))*p) - 1
		if i < 0 {
			i = 0
		}
		return lat[i]
	}
	var total time.Duration
	for _, d := range lat {
		total += d
	}
	mean := time.Duration(0)
	if len(lat) > 0 {
		mean = total / time.Duration(len(lat))
	}

	fmt.Printf("limit=%d zstd=%v\n", limit, useZstd)
	fmt.Printf("  dial+TLS      %8.0f ms\n", float64(dial.Microseconds())/1000)
	fmt.Printf("  hello         %8.0f ms\n", float64(hello.Microseconds())/1000)
	fmt.Printf("  sweep         %8.0f ms   <- %d pages, %d rows\n",
		float64(sweep.Microseconds())/1000, len(stats), rows)
	fmt.Printf("  per page      min %5.0f  p50 %5.0f  mean %5.0f  p95 %5.0f  max %5.0f ms\n",
		ms(lat[0]), ms(pct(0.50)), ms(mean), ms(pct(0.95)), ms(lat[len(lat)-1]))
	fmt.Printf("  bytes on wire %8.2f MiB\n", float64(wire)/(1<<20))
	fmt.Printf("  bytes decoded %8.2f MiB", float64(plain)/(1<<20))
	if plain > 0 && wire != plain {
		fmt.Printf("   (compression %.2fx)", float64(plain)/float64(wire))
	}
	fmt.Println()
	if rows > 0 {
		fmt.Printf("  per row       %8.0f B on wire\n", float64(wire)/float64(rows))
	}
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

// rpc sends msg and returns the reply plus (wire bytes, post-inflate bytes).
func rpc(ctx context.Context, conn *websocket.Conn, dec *zstd.Decoder, msg *pb.ClientMessage) (
	*pb.ServerMessage, int, int, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, 0, 0, err
	}
	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		return nil, 0, 0, err
	}
	typ, respData, err := conn.Read(ctx)
	if err != nil {
		return nil, 0, 0, err
	}
	if typ != websocket.MessageBinary {
		return nil, 0, 0, fmt.Errorf("expected binary frame, got %v", typ)
	}
	wireLen := len(respData)

	var resp pb.ServerMessage
	if err := proto.Unmarshal(respData, &resp); err != nil {
		return nil, 0, 0, err
	}
	// Unwrap the opt-in compressed envelope; the inner message is the sole
	// routing authority (see CLAUDE.md), so unwrap BEFORE inspecting the payload.
	if enc := resp.GetEncoded(); enc != nil {
		plain, derr := dec.DecodeAll(enc.GetBody(), nil)
		if derr != nil {
			return nil, 0, 0, fmt.Errorf("zstd inflate: %w", derr)
		}
		var inner pb.ServerMessage
		if err := proto.Unmarshal(plain, &inner); err != nil {
			return nil, 0, 0, err
		}
		return &inner, wireLen, len(plain), nil
	}
	return &resp, wireLen, wireLen, nil
}
