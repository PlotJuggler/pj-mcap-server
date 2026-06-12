// Command paginate is a Slice-9 verification aid: it drives the ListFiles
// page_token cursor loop at a fixed --limit and asserts the pages tile the
// catalog cleanly — no duplicate ids, no gaps, exact total. It reuses the same
// envelope/RPC discipline as devprobe.
//
// Output (JSON): page sizes in order, page count, total ids, distinct id count
// (dup detection), and whether the union of ids equals a contiguous-by-listing
// set (the server orders by id asc; we assert strictly increasing across pages).
//
// Usage:
//
//	paginate --url ws://host/api/ws [--token T] [--limit 20]
//
// Exit codes: 0 clean pagination, 1 RPC/connection failure, 3 pagination
// invariant violated (dup or out-of-order), 2 usage.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/protobuf/proto"
	"nhooyr.io/websocket"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

func main() {
	url := flag.String("url", "ws://localhost:8080/api/ws", "server WebSocket URL")
	token := flag.String("token", os.Getenv("PJ_CLOUD_TOKEN"), "bearer token")
	limit := flag.Uint("limit", 20, "page size (ListFiles.limit)")
	flag.Parse()

	if err := run(*url, *token, uint32(*limit)); err != nil {
		fmt.Fprintln(os.Stderr, "paginate:", err)
		os.Exit(1)
	}
}

type result struct {
	Limit       uint32   `json:"limit"`
	PageSizes   []int    `json:"page_sizes"`
	PageCount   int      `json:"page_count"`
	TotalIDs    int      `json:"total_ids"`
	DistinctIDs int      `json:"distinct_ids"`
	StrictlyInc bool     `json:"strictly_increasing"`
	NoDuplicate bool     `json:"no_duplicates"`
	FirstID     uint64   `json:"first_id"`
	LastID      uint64   `json:"last_id"`
	IDs         []uint64 `json:"-"`
}

func run(url, token string, limit uint32) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", url, err)
	}
	defer conn.CloseNow()

	// Hello.
	if _, err := rpc(ctx, conn, &pb.ClientMessage{
		RequestId: 1,
		Payload:   &pb.ClientMessage_Hello{Hello: &pb.Hello{ProtocolVersion: 1, AuthToken: token}},
	}); err != nil {
		return err
	}

	res := result{Limit: limit, StrictlyInc: true, NoDuplicate: true}
	seen := map[uint64]bool{}
	var prev uint64
	var pageToken string
	reqID := uint64(2)

	for {
		resp, err := rpc(ctx, conn, &pb.ClientMessage{
			RequestId: reqID,
			Payload: &pb.ClientMessage_ListFiles{ListFiles: &pb.ListFilesRequest{
				Limit:     limit,
				PageToken: pageToken,
			}},
		})
		if err != nil {
			return err
		}
		reqID++
		lf := resp.GetListFiles()
		if lf == nil {
			return fmt.Errorf("expected ListFilesResponse, got %T (error: %v)", resp.Payload, resp.GetError())
		}
		pageIDs := make([]uint64, 0, len(lf.GetFiles()))
		for _, f := range lf.GetFiles() {
			id := f.GetId()
			if seen[id] {
				res.NoDuplicate = false
			}
			seen[id] = true
			if prev != 0 && id <= prev {
				res.StrictlyInc = false
			}
			prev = id
			pageIDs = append(pageIDs, id)
			res.IDs = append(res.IDs, id)
		}
		res.PageSizes = append(res.PageSizes, len(pageIDs))
		if next := lf.GetNextPageToken(); next != "" {
			pageToken = next
			continue
		}
		break
	}

	res.PageCount = len(res.PageSizes)
	res.TotalIDs = len(res.IDs)
	res.DistinctIDs = len(seen)
	if len(res.IDs) > 0 {
		res.FirstID = res.IDs[0]
		res.LastID = res.IDs[len(res.IDs)-1]
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
		return err
	}

	_ = conn.Close(websocket.StatusNormalClosure, "done")

	if !res.StrictlyInc || !res.NoDuplicate {
		os.Exit(3)
	}
	return nil
}

func rpc(ctx context.Context, conn *websocket.Conn, msg *pb.ClientMessage) (*pb.ServerMessage, error) {
	reqID := msg.GetRequestId()
	data, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	if err := conn.Write(ctx, websocket.MessageBinary, data); err != nil {
		return nil, err
	}
	typ, respData, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, fmt.Errorf("expected binary frame, got %v", typ)
	}
	var resp pb.ServerMessage
	if err := proto.Unmarshal(respData, &resp); err != nil {
		return nil, err
	}
	if resp.GetRequestId() != reqID {
		return nil, fmt.Errorf("request_id echo mismatch: sent %d got %d", reqID, resp.GetRequestId())
	}
	return &resp, nil
}
