// download.go implements devprobe's -download mode: the Go-side REFERENCE
// downloader the integration harness uses. It opens a FRESH streaming session
// for an s3_key (resolved via ListFiles), receives the MessageBatch stream
// (decompressing ZSTD bodies one-shot per batch — no cross-batch decoder state,
// matching the server's one-shot-per-batch invariant), acks periodically so the
// server's retain buffer drains, and reconstructs a local MCAP with the mcap-go
// writer. It prints a JSON summary to stdout on success.
//
// The reconstructed MCAP carries the schemas + channels from
// OpenSessionResponse (names + schema bytes cross the wire once) and one Message
// record per streamed message, so mcap-go can count it back to the exact total
// (the harness's byte-level ground-truth gate).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	gomcap "github.com/foxglove/mcap/go/mcap"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/proto"
	"nhooyr.io/websocket"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

const (
	ackEveryBatches = 64
	ackEveryDur     = 500 * time.Millisecond
)

type downloadSummary struct {
	S3Key         string `json:"s3_key"`
	FileID        uint64 `json:"file_id"`
	Out           string `json:"out"`
	Messages      uint64 `json:"messages"`
	Bytes         uint64 `json:"bytes"`
	Batches       uint64 `json:"batches"`
	EosReason     string `json:"eos_reason"`
	EosTotalMsgs  uint64 `json:"eos_total_messages_sent"`
	EosTotalBytes uint64 `json:"eos_total_bytes_sent"`
	Topics        int    `json:"topic_count"`
	Schemas       int    `json:"schema_count"`
}

// runDownload opens a FRESH session for one OR MORE s3_keys (the -download flag
// accepts a comma-separated list: a single key is the common case; multiple
// consecutive keys exercise server-side STITCHING — one logical session over the
// union of files, per design-spec §6.3). The keys are resolved to file_ids via
// ListFiles in the order given; the server validates pairwise non-overlap and
// merges into one ordered stream.
func runDownload(url, token, s3key, outPath, topicsCSV, timeRangeCSV string) error {
	// A bulk download can take a while; allow generous time. Streaming itself is
	// as-fast-as-possible, so this is just an upper bound.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	conn, err := dialWS(ctx, url)
	if err != nil {
		return fmt.Errorf("dial %s: %w", url, err)
	}
	defer conn.CloseNow()
	conn.SetReadLimit(64 << 20)

	// Hello.
	helloResp, err := rpc(ctx, conn, &pb.ClientMessage{
		RequestId: 1,
		Payload:   &pb.ClientMessage_Hello{Hello: &pb.Hello{ProtocolVersion: 1, AuthToken: token}},
	})
	if err != nil {
		return err
	}
	if helloResp.GetHelloResponse() == nil {
		return fmt.Errorf("expected HelloResponse, got error %v", helloResp.GetError())
	}

	// Resolve the file id from the s3_key via ListFiles.
	listResp, err := rpc(ctx, conn, &pb.ClientMessage{
		RequestId: 2,
		Payload:   &pb.ClientMessage_ListFiles{ListFiles: &pb.ListFilesRequest{Limit: 1000}},
	})
	if err != nil {
		return err
	}
	lf := listResp.GetListFiles()
	if lf == nil {
		return fmt.Errorf("expected ListFilesResponse, got error %v", listResp.GetError())
	}
	byKey := map[string]uint64{}
	for _, f := range lf.GetFiles() {
		byKey[f.GetS3Key()] = f.GetId()
	}
	// -download accepts a comma-separated list of keys (stitched session).
	var fileIDs []uint64
	for _, k := range strings.Split(s3key, ",") {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		id, ok := byKey[k]
		if !ok || id == 0 {
			return fmt.Errorf("s3_key %q not found in catalog", k)
		}
		fileIDs = append(fileIDs, id)
	}
	if len(fileIDs) == 0 {
		return fmt.Errorf("no s3_keys resolved from %q", s3key)
	}
	fileID := fileIDs[0]

	// Build OpenFresh.
	fresh := &pb.OpenFresh{FileIds: fileIDs}
	if topicsCSV != "" {
		for _, t := range strings.Split(topicsCSV, ",") {
			if t = strings.TrimSpace(t); t != "" {
				fresh.TopicNames = append(fresh.TopicNames, t)
			}
		}
	}
	if timeRangeCSV != "" {
		tr, err := parseTimeRange(timeRangeCSV)
		if err != nil {
			return err
		}
		fresh.TimeRange = tr
	}

	if err := writeMsg(ctx, conn, &pb.ClientMessage{
		RequestId: 3,
		Payload:   &pb.ClientMessage_OpenSession{OpenSession: &pb.OpenSessionRequest{Mode: &pb.OpenSessionRequest_Fresh{Fresh: fresh}}},
	}); err != nil {
		return err
	}

	// The next priority frame should be the OpenSessionResponse (request_id 3).
	openMsg, err := readMsg(ctx, conn)
	if err != nil {
		return err
	}
	or := openMsg.GetOpenSession()
	if or == nil {
		return fmt.Errorf("expected OpenSessionResponse, got %T (error %v)", openMsg.GetPayload(), openMsg.GetError())
	}
	subID := or.GetSubscriptionId()

	// Open the output MCAP + register schemas/channels from the bindings.
	f, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create %q: %w", outPath, err)
	}
	defer f.Close()
	writer, err := gomcap.NewWriter(f, &gomcap.WriterOptions{
		Chunked:     true,
		ChunkSize:   4 << 20,
		Compression: gomcap.CompressionZSTD,
		IncludeCRC:  true,
	})
	if err != nil {
		return fmt.Errorf("mcap writer: %w", err)
	}
	if err := writer.WriteHeader(&gomcap.Header{Profile: "", Library: "devprobe-download"}); err != nil {
		return fmt.Errorf("write header: %w", err)
	}
	// Schemas: schema_id -> Schema record. (schema_id 0 means "no schema".)
	for _, s := range or.GetSchemas() {
		if s.GetSchemaId() == 0 {
			continue
		}
		if err := writer.WriteSchema(&gomcap.Schema{
			ID:       uint16(s.GetSchemaId()),
			Name:     s.GetName(),
			Encoding: s.GetEncoding(),
			Data:     s.GetData(),
		}); err != nil {
			return fmt.Errorf("write schema %d: %w", s.GetSchemaId(), err)
		}
	}
	// Channels: topic_id -> Channel record (channel id == topic_id).
	for _, b := range or.GetTopicIdMap() {
		if err := writer.WriteChannel(&gomcap.Channel{
			ID:              uint16(b.GetTopicId()),
			SchemaID:        uint16(b.GetSchemaId()),
			Topic:           b.GetTopicName(),
			MessageEncoding: b.GetMessageEncoding(),
			Metadata:        map[string]string{},
		}); err != nil {
			return fmt.Errorf("write channel %d (%s): %w", b.GetTopicId(), b.GetTopicName(), err)
		}
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		return fmt.Errorf("zstd reader: %w", err)
	}
	defer dec.Close()

	sum := downloadSummary{
		S3Key:   s3key,
		FileID:  fileID,
		Out:     outPath,
		Topics:  len(or.GetTopicIdMap()),
		Schemas: len(or.GetSchemas()),
	}
	seqByChannel := map[uint16]uint32{}

	batchesSinceAck := 0
	lastAck := time.Now()
	var lastSeq uint64

	for {
		msg, err := readMsg(ctx, conn)
		if err != nil {
			return err
		}
		switch {
		case msg.GetBatch() != nil:
			b := msg.GetBatch()
			msgs, derr := decodeBatchMessages(dec, b)
			if derr != nil {
				return derr
			}
			for _, m := range msgs {
				chID := uint16(m.GetTopicId())
				if err := writer.WriteMessage(&gomcap.Message{
					ChannelID:   chID,
					Sequence:    seqByChannel[chID],
					LogTime:     uint64(m.GetLogTimeNs()),
					PublishTime: uint64(m.GetPublishTimeNs()),
					Data:        decodeMessagePayload(dec, m),
				}); err != nil {
					return fmt.Errorf("write message: %w", err)
				}
				seqByChannel[chID]++
				sum.Messages++
				sum.Bytes += uint64(len(m.GetPayload()))
			}
			sum.Batches++
			lastSeq = b.GetSeq()
			batchesSinceAck++
			if batchesSinceAck >= ackEveryBatches || time.Since(lastAck) >= ackEveryDur {
				if err := sendAck(ctx, conn, subID, lastSeq); err != nil {
					return err
				}
				batchesSinceAck = 0
				lastAck = time.Now()
			}
		case msg.GetProgress() != nil:
			// observed only; nothing to write
		case msg.GetEos() != nil:
			e := msg.GetEos()
			sum.EosReason = eosReasonString(e.GetReason())
			sum.EosTotalMsgs = e.GetTotalMessagesSent()
			sum.EosTotalBytes = e.GetTotalBytesSent()
			// Final ack so the server can prune fully.
			_ = sendAck(ctx, conn, subID, lastSeq)
			if err := writer.Close(); err != nil {
				return fmt.Errorf("close mcap: %w", err)
			}
			_ = conn.Close(websocket.StatusNormalClosure, "done")
			if e.GetReason() != pb.EosReason_EOS_REASON_COMPLETE {
				// Still emit the summary so the harness sees what happened, then fail.
				_ = json.NewEncoder(os.Stdout).Encode(sum)
				return fmt.Errorf("stream ended with %s (not COMPLETE)", sum.EosReason)
			}
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(sum)
		case msg.GetError() != nil:
			return fmt.Errorf("server error during stream: %s (%s)", msg.GetError().GetMessage(), msg.GetError().GetCode())
		}
	}
}

// decodeBatchMessages returns the messages in a batch, rejecting an unknown
// body_encoding (defensive parsing, spec §6.4). The ZSTD body is one
// self-contained frame decoded with a one-shot DecodeAll.
func decodeBatchMessages(dec *zstd.Decoder, b *pb.MessageBatch) ([]*pb.Message, error) {
	switch b.GetBodyEncoding() {
	case pb.BodyEncoding_BODY_ENCODING_ZSTD:
		raw, err := dec.DecodeAll(b.GetBody(), nil)
		if err != nil {
			return nil, fmt.Errorf("batch %d: zstd body decode: %w", b.GetSeq(), err)
		}
		if uint64(len(raw)) != b.GetBodyUncompressedSize() {
			return nil, fmt.Errorf("batch %d: body_uncompressed_size %d != decoded %d", b.GetSeq(), b.GetBodyUncompressedSize(), len(raw))
		}
		var body pb.MessageBatchBody
		if err := proto.Unmarshal(raw, &body); err != nil {
			return nil, fmt.Errorf("batch %d: unmarshal body: %w", b.GetSeq(), err)
		}
		return body.GetMessages(), nil
	case pb.BodyEncoding_BODY_ENCODING_NONE:
		return b.GetMessages(), nil
	default:
		return nil, fmt.Errorf("batch %d: unknown body_encoding %v (client rejects)", b.GetSeq(), b.GetBodyEncoding())
	}
}

// decodeMessagePayload returns the message's raw payload, decompressing the
// per-message ZSTD case used only on the NONE singleton fallback path.
func decodeMessagePayload(dec *zstd.Decoder, m *pb.Message) []byte {
	if m.GetPayloadEncoding() == pb.PayloadEncoding_PAYLOAD_ENCODING_ZSTD {
		raw, err := dec.DecodeAll(m.GetPayload(), nil)
		if err == nil {
			return raw
		}
	}
	return m.GetPayload()
}

func sendAck(ctx context.Context, conn *websocket.Conn, subID, throughSeq uint64) error {
	return writeMsg(ctx, conn, &pb.ClientMessage{
		Payload: &pb.ClientMessage_Ack{Ack: &pb.SessionAck{SubscriptionId: subID, ThroughSeq: throughSeq}},
	})
}

func parseTimeRange(csv string) (*pb.TimeRange, error) {
	parts := strings.SplitN(csv, ",", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("time-range must be 'startNs,endNs', got %q", csv)
	}
	start, err := strconv.ParseInt(strings.TrimSpace(parts[0]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("time-range start: %w", err)
	}
	end, err := strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("time-range end: %w", err)
	}
	return &pb.TimeRange{StartNs: start, EndNs: end}, nil
}

func eosReasonString(r pb.EosReason) string {
	switch r {
	case pb.EosReason_EOS_REASON_COMPLETE:
		return "COMPLETE"
	case pb.EosReason_EOS_REASON_CANCELLED:
		return "CANCELLED"
	case pb.EosReason_EOS_REASON_ERROR:
		return "ERROR"
	default:
		return "UNSPECIFIED"
	}
}

// writeMsg / readMsg are the raw frame helpers (the browse-mode rpc() helper
// pairs one request with one response; streaming needs them separately).
func writeMsg(ctx context.Context, conn *websocket.Conn, msg *pb.ClientMessage) error {
	data, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageBinary, data)
}

func readMsg(ctx context.Context, conn *websocket.Conn) (*pb.ServerMessage, error) {
	typ, data, err := conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if typ != websocket.MessageBinary {
		return nil, fmt.Errorf("expected binary frame, got %v", typ)
	}
	var msg pb.ServerMessage
	if err := proto.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
