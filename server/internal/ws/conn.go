package ws

import (
	"context"
	"time"

	"google.golang.org/protobuf/proto"
	"nhooyr.io/websocket"

	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// ConnAPI is what handlers + the session consumer see when they need to send a
// frame on the WS. SendPriority carries catalog RPC responses, Error, Progress
// and Eos; SendBulk carries MessageBatch (the only thing on the bulk channel).
// Per design spec §6.4, the write loop drains priority before bulk at every
// frame boundary, and the bulk channel's small capacity (16) lets a slow client
// backpressure the producer naturally. Both return false when the frame could
// not be queued (channel full or connection torn down) — the session consumer
// treats a false from SendBulk as "WS dropped" and detaches.
type ConnAPI interface {
	SendPriority(m *pb.ServerMessage) bool
	SendBulk(m *pb.ServerMessage) bool
}

// writeAdapter is the minimal write surface — abstracted so unit tests can drive
// the write loop without a real WebSocket.
type writeAdapter interface {
	Write(ctx context.Context, buf []byte) error
}

type wsAdapter struct{ c *websocket.Conn }

func (a wsAdapter) Write(ctx context.Context, buf []byte) error {
	return a.c.Write(ctx, websocket.MessageBinary, buf)
}

// bulkChanCap is the bulk (MessageBatch) channel capacity (spec §6.4: "small
// capacity (16 frames) so a slow client backpressures the S3 fetcher").
const bulkChanCap = 16

// priorityChanCap is the high-priority channel capacity. Control frames (RPC
// responses, Error, Progress, Eos) are low-volume; a generous buffer keeps
// SendPriority from ever blocking the read loop or a session consumer.
const priorityChanCap = 64

// conn owns one WebSocket's write side: two channels (priority + bulk) drained
// by a single write loop goroutine. The read loop and any number of session
// consumers feed the channels; only the write loop touches the socket, so writes
// are serialized without a write mutex.
type conn struct {
	w            writeAdapter
	writeTimeout time.Duration
	priorityCh   chan []byte
	bulkCh       chan []byte
	// ws is the underlying WebSocket for the read side (nil in write-only tests).
	ws *websocket.Conn
	// done is closed by the read loop when the connection is finished; Send*
	// observe it so a queue attempt cannot block forever after teardown.
	done chan struct{}
}

func newConn(c *websocket.Conn, writeTimeout time.Duration) *conn {
	cc := newConnFromAdapter(wsAdapter{c: c}, writeTimeout)
	cc.ws = c
	return cc
}

// read reads one frame from the underlying WebSocket. Only the read loop calls
// it. Tests that drive only the write side never set ws.
func (c *conn) read(ctx context.Context) (websocket.MessageType, []byte, error) {
	return c.ws.Read(ctx)
}

// unmarshalClient decodes a ClientMessage frame.
func unmarshalClient(data []byte, msg *pb.ClientMessage) error {
	return proto.Unmarshal(data, msg)
}

func newConnFromAdapter(w writeAdapter, writeTimeout time.Duration) *conn {
	return &conn{
		w:            w,
		writeTimeout: writeTimeout,
		priorityCh:   make(chan []byte, priorityChanCap),
		bulkCh:       make(chan []byte, bulkChanCap),
		done:         make(chan struct{}),
	}
}

// runWriteLoop drains priority then bulk until ctx is done OR a socket write
// fails. Implements spec §6.4 multiplexing fairness: a ready priority frame
// always wins over a bulk frame at a frame boundary. On the first write failure
// the loop signals teardown via closeDone (so subsequent Send* return false and
// the session consumer detaches instead of draining into a dead socket — the
// precondition for reconnect-resume).
func (c *conn) runWriteLoop(ctx context.Context) {
	for {
		// First, prefer priority: if a priority frame is ready, send it and loop
		// (without ever looking at bulk).
		select {
		case <-ctx.Done():
			return
		case buf := <-c.priorityCh:
			if !c.writeOne(ctx, buf) {
				c.closeDone()
				return
			}
			continue
		default:
		}
		// No priority frame ready right now: block on either channel.
		select {
		case <-ctx.Done():
			return
		case buf := <-c.priorityCh:
			if !c.writeOne(ctx, buf) {
				c.closeDone()
				return
			}
		case buf := <-c.bulkCh:
			if !c.writeOne(ctx, buf) {
				c.closeDone()
				return
			}
		}
	}
}

// writeOne writes one frame; returns false if the socket write failed.
func (c *conn) writeOne(ctx context.Context, buf []byte) bool {
	wctx, cancel := context.WithTimeout(ctx, c.writeTimeout)
	defer cancel()
	return c.w.Write(wctx, buf) == nil
}

func (c *conn) SendPriority(m *pb.ServerMessage) bool {
	if c.isDone() {
		return false
	}
	buf, err := proto.Marshal(m)
	if err != nil {
		return false
	}
	select {
	case c.priorityCh <- buf:
		return true
	case <-c.done:
		return false
	default:
		return false
	}
}

func (c *conn) SendBulk(m *pb.ServerMessage) bool {
	if c.isDone() {
		return false
	}
	buf, err := proto.Marshal(m)
	if err != nil {
		return false
	}
	select {
	case c.bulkCh <- buf:
		return true
	case <-c.done:
		return false
	default:
		return false
	}
}

func (c *conn) isDone() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// closeDone signals Send* that the connection is gone (idempotent). The write
// loop is stopped separately by cancelling its context.
func (c *conn) closeDone() {
	select {
	case <-c.done:
	default:
		close(c.done)
	}
}
