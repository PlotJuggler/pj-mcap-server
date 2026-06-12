// Command devprobe is a tiny verification client for the browse slice: it
// connects to the server's WebSocket, runs Hello -> ListFiles -> GetFile(first
// id), and prints the results as JSON to stdout. It is a dev/CI aid, not part
// of the shipped product (the real client is the Qt plugin).
package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"nhooyr.io/websocket"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// dialOpts carries optional dial settings (currently just TLS skip-verify for
// wss:// against a self-signed dev cert). Set once in main from the -insecure
// flag; read by dialWS. nil => default verification.
var dialOpts *websocket.DialOptions

// dialWS dials the server WebSocket, applying any package-level dialOpts (e.g.
// the -insecure TLS skip-verify for self-signed dev certs). All devprobe modes
// go through this so the flag applies uniformly.
func dialWS(ctx context.Context, url string) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(ctx, url, dialOpts)
	return conn, err
}

// stringList is a repeatable string flag (e.g. -set-tag k=v -set-tag a=b).
type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func main() {
	var (
		url       = flag.String("url", "ws://localhost:8080/api/ws", "server WebSocket URL")
		token     = flag.String("token", os.Getenv("PJ_CLOUD_TOKEN"), "bearer token (default $PJ_CLOUD_TOKEN)")
		getFileID = flag.Uint64("get-file-id", 0, "file id to GetFile (0 = first listed id)")

		// -download mode: open a fresh streaming session for an s3_key, receive the
		// batches, and reconstruct a local MCAP. This is the Go-side reference
		// downloader the integration harness uses.
		download  = flag.String("download", "", "s3_key (or comma-separated keys for a stitched multi-file session) to open a fresh session for and reconstruct to -out (download mode)")
		out       = flag.String("out", "", "output MCAP path (required with -download)")
		topicsCSV = flag.String("topics", "", "comma-separated topic names to stream (empty = all)")
		timeRange = flag.String("time-range", "", "optional 'startNs,endNs' half-open window")

		// tag-edit mode (UpdateTags, arm 13): target a file via -file-id or -key.
		tagFileID       = flag.Uint64("file-id", 0, "tag-edit: file id to target (alt to -key)")
		fileKey         = flag.String("key", "", "s3_key to target for tag edits / GetFile (resolved via ListFiles)")
		recordedBetween = flag.String("recorded-between", "", "ListFiles filter: 'startNs,endNs' recorded-between window")

		// ListFiles FileFilter predicates (prove server-side filtering).
		filterTopics = flag.String("filter-topics", "", "ListFiles filter: comma-separated topics_any_of")

		// -insecure: skip TLS verification for wss:// against a self-signed dev
		// cert (the C++ side has the equivalent allow_insecure). Plaintext ws://
		// is unaffected.
		insecure = flag.Bool("insecure", false, "wss://: skip TLS certificate verification (self-signed dev certs)")
	)
	var (
		setTags  stringList
		unsetTag stringList
		tagAll   stringList
		tagAny   stringList
	)
	flag.Var(&setTags, "set-tag", "tag-edit: set override key=value (repeatable)")
	flag.Var(&unsetTag, "unset-tag", "tag-edit: unset key (repeatable)")
	flag.Var(&tagAll, "filter-tag-all", "ListFiles filter: tag_all predicate key[=value] (repeatable)")
	flag.Var(&tagAny, "filter-tag-any", "ListFiles filter: tag_any predicate key[=value] (repeatable)")
	flag.Parse()

	if *insecure {
		dialOpts = &websocket.DialOptions{
			HTTPClient: &http.Client{
				Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}},
			},
		}
	}

	if *download != "" {
		if *out == "" {
			fmt.Fprintln(os.Stderr, "devprobe: -download requires -out PATH")
			os.Exit(2)
		}
		if err := runDownload(*url, *token, *download, *out, *topicsCSV, *timeRange); err != nil {
			fmt.Fprintln(os.Stderr, "devprobe: download error:", err)
			os.Exit(1)
		}
		return
	}

	// tag-edit mode: any of -set-tag / -unset-tag present. Target via -file-id
	// (preferred) or -key.
	if len(setTags) > 0 || len(unsetTag) > 0 {
		if err := runTagEdit(*url, *token, *tagFileID, *fileKey, setTags, unsetTag); err != nil {
			fmt.Fprintln(os.Stderr, "devprobe: tag-edit error:", err)
			os.Exit(1)
		}
		return
	}

	flt := &listFilter{
		recordedBetween: *recordedBetween,
		topicsAnyOf:     *filterTopics,
		tagAll:          tagAll,
		tagAny:          tagAny,
	}
	if err := run(*url, *token, *getFileID, flt); err != nil {
		fmt.Fprintln(os.Stderr, "devprobe: error:", err)
		os.Exit(1)
	}
}

// listFilter holds the optional ListFiles FileFilter predicates.
type listFilter struct {
	recordedBetween string // "startNs,endNs"
	topicsAnyOf     string // CSV
	tagAll          []string
	tagAny          []string
}

func (f *listFilter) toProto() (*pb.FileFilter, error) {
	if f == nil {
		return nil, nil
	}
	var ff pb.FileFilter
	set := false
	if f.recordedBetween != "" {
		parts := strings.SplitN(f.recordedBetween, ",", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("recorded-between must be 'startNs,endNs'")
		}
		s, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("recorded-between start: %w", err)
		}
		e, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("recorded-between end: %w", err)
		}
		ff.RecordedBetween = &pb.TimeRange{StartNs: s, EndNs: e}
		set = true
	}
	if f.topicsAnyOf != "" {
		for _, t := range strings.Split(f.topicsAnyOf, ",") {
			if t = strings.TrimSpace(t); t != "" {
				ff.TopicsAnyOf = append(ff.TopicsAnyOf, t)
			}
		}
		set = true
	}
	for _, p := range f.tagAll {
		ff.TagAll = append(ff.TagAll, parseTagPredicate(p))
		set = true
	}
	for _, p := range f.tagAny {
		ff.TagAny = append(ff.TagAny, parseTagPredicate(p))
		set = true
	}
	if !set {
		return nil, nil
	}
	return &ff, nil
}

// parseTagPredicate splits "key=value" (empty value => key-exists).
func parseTagPredicate(s string) *pb.TagPredicate {
	if i := strings.IndexByte(s, '='); i >= 0 {
		return &pb.TagPredicate{Key: s[:i], Value: s[i+1:]}
	}
	return &pb.TagPredicate{Key: s}
}

type report struct {
	HelloResponse map[string]any   `json:"hello_response"`
	FileCount     int              `json:"file_count"`
	Files         []map[string]any `json:"files"`
	GetFile       map[string]any   `json:"get_file,omitempty"`
}

func run(url, token string, getFileID uint64, flt *listFilter) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := dialWS(ctx, url)
	if err != nil {
		return fmt.Errorf("dial %s: %w", url, err)
	}
	// Best-effort clean close at the end so the server logs a normal closure
	// rather than an abrupt EOF. CloseNow is the fallback on early error paths.
	defer conn.CloseNow()
	closeClean := func() { _ = conn.Close(websocket.StatusNormalClosure, "done") }

	rep := report{}

	// Hello (payload arm 10)
	helloResp, err := rpc(ctx, conn, &pb.ClientMessage{
		RequestId: 1,
		Payload:   &pb.ClientMessage_Hello{Hello: &pb.Hello{ProtocolVersion: 1, AuthToken: token}},
	})
	if err != nil {
		return err
	}
	hr := helloResp.GetHelloResponse()
	if hr == nil {
		return fmt.Errorf("expected HelloResponse, got %T (error: %v)", helloResp.Payload, helloResp.GetError())
	}
	rep.HelloResponse = map[string]any{
		"server_version":          hr.GetServerVersion(),
		"resume_supported":        hr.GetCapabilities().GetResumeSupported(),
		"tag_edit_supported":      hr.GetCapabilities().GetTagEditSupported(),
		"supports_file_hierarchy": hr.GetBackend().GetSupportsFileHierarchy(),
		"metadata_key_vocabulary": hr.GetBackend().GetMetadataKeyVocabulary(),
	}

	// ListFiles (payload arm 11), with optional FileFilter predicates.
	filterPB, err := flt.toProto()
	if err != nil {
		return err
	}
	listResp, err := rpc(ctx, conn, &pb.ClientMessage{
		RequestId: 2,
		Payload:   &pb.ClientMessage_ListFiles{ListFiles: &pb.ListFilesRequest{Limit: 1000, Filter: filterPB}},
	})
	if err != nil {
		return err
	}
	lf := listResp.GetListFiles()
	if lf == nil {
		return fmt.Errorf("expected ListFilesResponse, got %T (error: %v)", listResp.Payload, listResp.GetError())
	}
	rep.FileCount = len(lf.GetFiles())
	var firstID uint64
	for i, f := range lf.GetFiles() {
		if i == 0 {
			firstID = f.GetId()
		}
		entry := map[string]any{
			"id":            f.GetId(),
			"s3_key":        f.GetS3Key(),
			"size_bytes":    f.GetSizeBytes(),
			"topic_count":   f.GetTopicCount(),
			"message_count": f.GetMessageCount(),
			"start_ns":      f.GetRecorded().GetStartNs(),
			"end_ns":        f.GetRecorded().GetEndNs(),
		}
		if fm := lf.GetMetadata()[fmt.Sprintf("%d", f.GetId())]; fm != nil {
			entry["metadata"] = fm.GetEntries()
		}
		// Per-tag effective view (with is_override) so the harness can assert the
		// override layer (smoke step h).
		tags := make([]map[string]any, 0, len(f.GetTags()))
		for _, tg := range f.GetTags() {
			tags = append(tags, map[string]any{
				"key": tg.GetKey(), "value": tg.GetValue(), "is_override": tg.GetIsOverride(),
			})
		}
		entry["tags"] = tags
		rep.Files = append(rep.Files, entry)
	}

	// GetFile (payload arm 12). Targets -get-file-id, or the first listed id.
	targetID := getFileID
	if targetID == 0 {
		targetID = firstID
	}
	if targetID != 0 {
		getResp, gErr := rpc(ctx, conn, &pb.ClientMessage{
			RequestId: 3,
			Payload:   &pb.ClientMessage_GetFile{GetFile: &pb.GetFileRequest{FileId: targetID}},
		})
		if gErr != nil {
			return gErr
		}
		gf := getResp.GetGetFile()
		if gf == nil {
			return fmt.Errorf("expected GetFileResponse, got %T (error: %v)", getResp.Payload, getResp.GetError())
		}
		topics := make([]map[string]any, 0, len(gf.GetTopics()))
		names := make([]string, 0, len(gf.GetTopics()))
		for _, t := range gf.GetTopics() {
			topics = append(topics, map[string]any{
				"name":            t.GetName(),
				"schema_name":     t.GetSchemaName(),
				"schema_encoding": t.GetSchemaEncoding(),
				"message_count":   t.GetMessageCount(),
			})
			names = append(names, t.GetName())
		}
		sort.Strings(names)
		rep.GetFile = map[string]any{
			"file_id":     gf.GetSummary().GetId(),
			"s3_key":      gf.GetSummary().GetS3Key(),
			"topic_count": len(topics),
			"topic_names": names,
			"topics":      topics,
		}
	}

	closeClean()

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}

// runTagEdit is the Go-side twin of the C++ CLI `tag` verb: Hello, resolve the
// target file (by -file-id or -key), send UpdateTags (set/unset), print the
// resulting effective tags as JSON. Exit non-zero on error.
func runTagEdit(url, token string, fileID uint64, key string, setTags, unsetKeys []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := dialWS(ctx, url)
	if err != nil {
		return fmt.Errorf("dial %s: %w", url, err)
	}
	defer conn.CloseNow()
	closeClean := func() { _ = conn.Close(websocket.StatusNormalClosure, "done") }

	helloResp, err := rpc(ctx, conn, &pb.ClientMessage{
		RequestId: 1,
		Payload:   &pb.ClientMessage_Hello{Hello: &pb.Hello{ProtocolVersion: 1, AuthToken: token}},
	})
	if err != nil {
		return err
	}
	if helloResp.GetHelloResponse() == nil {
		return fmt.Errorf("expected HelloResponse, got %T (error: %v)", helloResp.Payload, helloResp.GetError())
	}

	// Resolve the file id if a -key was given (or none).
	if fileID == 0 {
		listResp, lErr := rpc(ctx, conn, &pb.ClientMessage{
			RequestId: 2,
			Payload:   &pb.ClientMessage_ListFiles{ListFiles: &pb.ListFilesRequest{Limit: 1000}},
		})
		if lErr != nil {
			return lErr
		}
		lf := listResp.GetListFiles()
		if lf == nil {
			return fmt.Errorf("expected ListFilesResponse, got %T (error: %v)", listResp.Payload, listResp.GetError())
		}
		for _, f := range lf.GetFiles() {
			if key == "" || f.GetS3Key() == key {
				fileID = f.GetId()
				if key != "" {
					break
				}
			}
		}
		if fileID == 0 {
			return fmt.Errorf("could not resolve file id (key=%q)", key)
		}
	}

	var (
		set   []*pb.Tag
		unset []string
	)
	for _, kv := range setTags {
		i := strings.IndexByte(kv, '=')
		if i < 0 {
			return fmt.Errorf("-set-tag must be key=value, got %q", kv)
		}
		set = append(set, &pb.Tag{Key: kv[:i], Value: kv[i+1:]})
	}
	unset = append(unset, unsetKeys...)

	upResp, err := rpc(ctx, conn, &pb.ClientMessage{
		RequestId: 3,
		Payload: &pb.ClientMessage_UpdateTags{UpdateTags: &pb.UpdateTagsRequest{
			FileId: fileID, SetTags: set, UnsetKeys: unset,
		}},
	})
	if err != nil {
		return err
	}
	ut := upResp.GetUpdateTags()
	if ut == nil {
		return fmt.Errorf("expected UpdateTagsResponse, got %T (error: %v)", upResp.Payload, upResp.GetError())
	}

	closeClean()

	effective := make([]map[string]any, 0, len(ut.GetEffectiveTags()))
	for _, tg := range ut.GetEffectiveTags() {
		effective = append(effective, map[string]any{
			"key": tg.GetKey(), "value": tg.GetValue(), "is_override": tg.GetIsOverride(),
		})
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{"file_id": fileID, "effective_tags": effective})
}

// rpc sends one ClientMessage, then reads one ServerMessage back (this slice
// answers each request with exactly one frame). It verifies the echoed request_id.
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
