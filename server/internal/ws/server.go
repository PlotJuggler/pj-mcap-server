// Package ws hosts the WebSocket protocol endpoint.
//
// Per design spec §6.1/§6.4 it uses nhooyr.io/websocket with BINARY protobuf
// frames over ONE connection that multiplexes catalog RPCs and session data
// streaming. Each connection runs:
//   - one READ loop (this file's connState.loop): decodes ClientMessages and
//     dispatches them; catalog RPCs answer synchronously on the PRIORITY channel,
//     session control spawns producer/consumer goroutines;
//   - one WRITE loop (conn.runWriteLoop, see conn.go): drains a PRIORITY channel
//     (RPC responses, Error, Progress, Eos) ahead of a small-capacity BULK
//     channel (MessageBatch) at every frame boundary — the §6.4 fairness rule.
//
// On disconnect the read loop DETACHES every session attached to this connection
// (Registry.Detach): the producer keeps filling its retain buffer and the
// per-session eviction timer (retain_after_disconnect) governs whether a later
// OpenResume can reattach (spec §6.5/§6.6).
package ws

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"nhooyr.io/websocket"

	"pj-cloud/server/internal/authn"
	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/metrics"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// ServerVersion returns the server version string reported in HelloResponse and
// the dashboard footer.
func ServerVersion() string { return serverVersion }

const (
	protocolVersion  = 1
	serverVersion    = "0.2.0-session-slice"
	defaultListLimit = 200
	maxListLimit     = 1000
	writeTimeout     = 30 * time.Second
	// Liveness is ping-based, NOT a per-frame read deadline: a browser/GUI
	// legitimately idles for minutes between catalog RPCs, and the old
	// 30s-without-a-frame deadline silently killed exactly those connections
	// (the client held a dead socket and every later RPC timed out — the
	// "topics only show after reconnect" bug). A dead peer instead fails
	// keepaliveFailLimit consecutive pings. The threshold is 2 because a ping's
	// pong is processed by the read loop's Read call: a single ping can starve
	// while dispatch is busy (e.g. an OpenSession plan-build), and that must
	// not be mistaken for a dead peer.
	keepaliveInterval    = 30 * time.Second
	keepalivePingTimeout = 10 * time.Second
	keepaliveFailLimit   = 2
	// readLimit caps inbound frames. Session control frames are tiny; the cap is
	// generous for OpenFresh with many file_ids.
	readLimit = 8 << 20 // 8 MiB
)

// readOnlyTagEditMessage is the single operator-facing message for "tag
// editing is unavailable because the catalog is read-only (external-builder
// mode)". Both handleUpdateTags call sites that reject on catalog.ErrReadOnly
// (the fast-path pre-check and the defense-in-depth branch below it) send
// this exact string, so the two sites can't drift apart.
const readOnlyTagEditMessage = "tag editing is disabled: the catalog is read-only (external-builder mode)"

// Handler serves the WS protocol against the SQLite catalog Store + (optionally)
// a session subsystem. When sess is nil the session arms (14/15/16) return
// INVALID_REQUEST — used by the catalog-browse-only configuration / tests.
type Handler struct {
	store *catalog.Store
	log   *slog.Logger
	// authToken is the single shared bearer token. Empty => DEV ANONYMOUS mode
	// (no Hello credential check; every client is accepted with a warning).
	authToken string
	// auth is the ClientAuthenticator (Plan A Task 24a / authn seam) built from
	// authToken when it is non-empty. nil in dev-anonymous mode. The Hello check
	// routes the presented token through auth.Verify (constant-time compare).
	auth authn.ClientAuthenticator
	// sess carries the shared session dependencies (registry, codec, storage,
	// tuning). nil disables streaming.
	sess *SessionDeps
	// metrics, if set, records ws-connection counters + guards the per-connection
	// read/write loops against panics (spec §8.1). Optional.
	metrics *metrics.Metrics
}

// NewHandler builds a catalog-only WS handler (no streaming). authToken == ""
// enables dev anonymous mode.
func NewHandler(store *catalog.Store, authToken string, log *slog.Logger) *Handler {
	return &Handler{store: store, log: log, authToken: authToken, auth: newAuthenticator(authToken)}
}

// NewHandlerWithSession builds a WS handler with the session/streaming subsystem
// wired in. The SessionDeps.Store should resolve files against the same catalog.
func NewHandlerWithSession(store *catalog.Store, authToken string, log *slog.Logger, sess *SessionDeps) *Handler {
	return &Handler{store: store, log: log, authToken: authToken, auth: newAuthenticator(authToken), sess: sess, metrics: sess.Metrics}
}

// newAuthenticator returns a bearer-token ClientAuthenticator for a non-empty
// token, or nil for dev-anonymous mode (empty token => accept all). Building it
// once at construction keeps the per-Hello path a single constant-time Verify.
func newAuthenticator(token string) authn.ClientAuthenticator {
	if token == "" {
		return nil
	}
	return authn.NewBearerToken(token)
}

// SetMetrics wires the metrics collectors into the handler (ws-connection
// counters + per-connection panic recovery). Idempotent; call once at startup.
func (h *Handler) SetMetrics(m *metrics.Metrics) {
	h.metrics = m
	if h.sess != nil {
		h.sess.Metrics = m
	}
}

// metrics returns the connection's metrics (may be nil). Helper for the session
// goroutine Guards.
func (c *connState) metrics() *metrics.Metrics { return c.h.metrics }

// ServeHTTP upgrades the request and runs the per-connection loops.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	wsConn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true, // single-origin server; client is a desktop app
	})
	if err != nil {
		h.log.Warn("ws: accept failed", "err", err, "remote", r.RemoteAddr)
		return
	}
	defer wsConn.CloseNow()
	wsConn.SetReadLimit(readLimit)

	remote := r.RemoteAddr
	h.log.Info("ws: client connected", "remote", remote)
	if h.metrics != nil {
		h.metrics.WSConnectionsTotal.Inc()
		h.metrics.WSConnectionsActive.Inc()
		defer h.metrics.WSConnectionsActive.Dec()
	}

	// Write loop lives on its own context so it can outlive the read loop briefly
	// to flush a final Eos/Error before the socket closes.
	writeCtx, writeCancel := context.WithCancel(r.Context())
	defer writeCancel()

	// Keepalive pinger: reaps dead peers without imposing an idle deadline on
	// live ones. Ping round-trips a pong (processed by the read loop), so it
	// proves the peer is reachable; keepaliveFailLimit consecutive failures
	// close the socket, which unblocks the read loop with an error.
	keepaliveCtx, keepaliveCancel := context.WithCancel(r.Context())
	defer keepaliveCancel()
	go metrics.Guard(h.metrics, h.log, "ws-keepalive", func() {
		ticker := time.NewTicker(keepaliveInterval)
		defer ticker.Stop()
		failures := 0
		for {
			select {
			case <-keepaliveCtx.Done():
				return
			case <-ticker.C:
				pingCtx, cancel := context.WithTimeout(keepaliveCtx, keepalivePingTimeout)
				err := wsConn.Ping(pingCtx)
				cancel()
				if err == nil {
					failures = 0
					continue
				}
				if keepaliveCtx.Err() != nil {
					return
				}
				failures++
				if failures >= keepaliveFailLimit {
					h.log.Warn("ws: keepalive failed; closing connection",
						"remote", remote, "consecutive_failures", failures, "err", err)
					wsConn.CloseNow()
					return
				}
			}
		}
	})

	c := &connState{
		h:      h,
		conn:   newConn(wsConn, writeTimeout),
		remote: remote,
		subs:   map[uint64]*subHandle{},
	}
	// Per-connection panic recovery (spec §8.1): a panic in the write loop is
	// recovered + counted; the connection tears down but the process survives.
	go metrics.Guard(h.metrics, h.log, "ws-write-loop", func() { c.conn.runWriteLoop(writeCtx) })

	// The read loop is likewise guarded: a panic dispatching one bad frame closes
	// only THIS connection (loopErr stays nil so it's treated as a clean close —
	// the recovered panic is already logged + counted by the Guard).
	var loopErr error
	metrics.Guard(h.metrics, h.log, "ws-read-loop", func() {
		loopErr = c.loop(r.Context())
	})

	// Connection is finished: detach every session attached here so producers
	// keep running (retain) and the eviction timer governs resume.
	c.detachAll()
	c.conn.closeDone()

	if loopErr != nil && !isNormalClose(loopErr) {
		h.log.Warn("ws: connection ended", "remote", remote, "err", loopErr)
	}
	h.log.Info("ws: client disconnected", "remote", remote)
}

type connState struct {
	h             *Handler
	conn          *conn
	remote        string
	authenticated bool

	// subs maps each subscription id whose consumer is attached to this connection
	// to its handle (consumer cancel + a "detaching" flag). disconnect detaches
	// exactly these. The detaching flag lets the consumer's post-Run logic tell a
	// connection-teardown cancel (KEEP the session alive for resume) apart from a
	// natural completion (tear the session down).
	mu   sync.Mutex
	subs map[uint64]*subHandle
}

// subHandle tracks one attached consumer for disconnect-detach.
type subHandle struct {
	cancel    context.CancelFunc
	detaching bool // set true by detachAll before cancelling: "keep session, resume"
}

func (c *connState) trackSubscription(id uint64, h *subHandle) {
	c.mu.Lock()
	c.subs[id] = h
	c.mu.Unlock()
}

func (c *connState) untrackSubscription(id uint64) {
	c.mu.Lock()
	delete(c.subs, id)
	c.mu.Unlock()
}

// handleIsDetaching reports whether the given handle was flagged as detaching due
// to connection teardown (read under the connection lock, since detachAll writes
// the flag under it).
func (c *connState) handleIsDetaching(h *subHandle) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return h.detaching
}

// detachAll detaches every session attached to this connection: it flags each
// handle as "detaching", cancels its consumer goroutine, and arms the per-session
// eviction timer (Registry.Detach). The producer keeps running until its retain
// caps block it; a later OpenResume reattaches (spec §6.5/§6.6).
func (c *connState) detachAll() {
	if c.h.sess == nil {
		return
	}
	c.mu.Lock()
	ids := make([]uint64, 0, len(c.subs))
	for id, h := range c.subs {
		h.detaching = true
		ids = append(ids, id)
	}
	c.mu.Unlock()
	for _, id := range ids {
		c.h.sess.Registry.Detach(id)
	}
}

func (c *connState) loop(ctx context.Context) error {
	for {
		// No per-frame deadline: idle connections are legitimate and liveness is
		// the keepalive pinger's job (see ServeHTTP).
		typ, data, err := c.conn.read(ctx)
		if err != nil {
			return err
		}
		if typ != websocket.MessageBinary {
			c.sendError(0, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, "expected binary protobuf frame", "")
			continue
		}
		var msg pb.ClientMessage
		if err := unmarshalClient(data, &msg); err != nil {
			c.sendError(0, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, "malformed ClientMessage", err.Error())
			continue
		}
		c.dispatch(ctx, &msg)
	}
}

func (c *connState) dispatch(ctx context.Context, msg *pb.ClientMessage) {
	reqID := msg.GetRequestId()
	switch payload := msg.Payload.(type) {
	case *pb.ClientMessage_Hello:
		c.handleHello(reqID, payload.Hello)
	case *pb.ClientMessage_ListFiles:
		if !c.requireAuth(reqID) {
			return
		}
		c.handleListFiles(ctx, reqID, payload.ListFiles)
	case *pb.ClientMessage_GetFile:
		if !c.requireAuth(reqID) {
			return
		}
		c.handleGetFile(ctx, reqID, payload.GetFile)
	case *pb.ClientMessage_OpenSession:
		if !c.requireAuth(reqID) {
			return
		}
		if c.h.sess == nil {
			c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST,
				"session streaming not enabled on this server", "")
			return
		}
		// OpenSession plan-building does real storage I/O (seconds over WAN,
		// scaling with the stitched file count) — it must NOT run inline on
		// this read loop. Inline, it (a) blocked every other RPC on the
		// connection, and (b) starved the pong processing the keepalive
		// pinger depends on, so the server killed its OWN connection mid-open
		// ("no response to OpenSession" client-side). All shared state the
		// handler touches is already synchronized (registry/catalog/conn
		// sends/subs were built for cross-goroutine resume), and replies
		// correlate by request_id, so out-of-order completion is fine.
		go metrics.Guard(c.h.metrics, c.h.log, "ws-open-session", func() {
			c.handleOpenSession(ctx, reqID, payload.OpenSession)
		})
	case *pb.ClientMessage_Cancel:
		if !c.requireAuth(reqID) || c.h.sess == nil {
			return
		}
		c.handleCancel(payload.Cancel)
	case *pb.ClientMessage_Ack:
		if !c.requireAuth(reqID) || c.h.sess == nil {
			return
		}
		c.handleAck(payload.Ack)
	case *pb.ClientMessage_UpdateTags:
		if !c.requireAuth(reqID) {
			return
		}
		c.handleUpdateTags(ctx, reqID, payload.UpdateTags)
	case *pb.ClientMessage_GetVocabulary:
		if !c.requireAuth(reqID) {
			return
		}
		c.handleGetVocabulary(ctx, reqID, payload.GetVocabulary)
	default:
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, "empty or unknown payload", "")
	}
}

func (c *connState) requireAuth(reqID uint64) bool {
	if c.authenticated {
		return true
	}
	c.sendError(reqID, 0, pb.ErrorCode_ERROR_AUTH_FAILED, "Hello handshake required before this operation", "")
	return false
}

func (c *connState) handleHello(reqID uint64, hello *pb.Hello) {
	c.h.log.Info("ws: Hello", "remote", c.remote, "request_id", reqID,
		"protocol_version", hello.GetProtocolVersion())

	if hello.GetProtocolVersion() != protocolVersion {
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_PROTOCOL_VERSION,
			fmt.Sprintf("server speaks protocol v%d, client offered v%d", protocolVersion, hello.GetProtocolVersion()), "")
		return
	}

	// Dev-anonymous mode (empty token => c.h.auth nil) accepts every client; a
	// configured token routes the presented credential through the authn seam's
	// constant-time compare (Plan A Task 24a — no plain != on the secret).
	if c.h.auth != nil {
		if _, err := c.h.auth.Verify(context.Background(), hello.GetAuthToken(), c.remote); err != nil {
			c.h.log.Warn("ws: auth failed", "remote", c.remote)
			c.sendError(reqID, 0, pb.ErrorCode_ERROR_AUTH_FAILED, "invalid bearer token", "")
			return
		}
	}
	c.authenticated = true

	// BackendCapabilities are DERIVED LIVE from the catalog (Plan D Task 8), not
	// hardcoded: the metadata-key vocabulary is the constant derived keys UNION the
	// catalog's distinct effective-tag keys, and supports_file_hierarchy is true
	// iff any indexed OBJECT key bears a '/'. The flat Dexory nissan corpus thus
	// reports hierarchy=false + the derived-key vocabulary (the C++ B3 live test's
	// ground truth). A query error is non-fatal: fall back to the stable derived
	// floor / no-hierarchy so a transient DB hiccup never blocks connect.
	ctx := context.Background()
	vocab, err := catalog.DistinctMetadataKeys(ctx, c.h.store)
	if err != nil {
		c.h.log.Warn("ws: metadata vocabulary derive failed; using derived floor", "err", err)
		vocab = catalog.DerivedMetadataKeys()
	}
	hierarchy, err := catalog.HasHierarchicalKey(ctx, c.h.store)
	if err != nil {
		c.h.log.Warn("ws: hierarchy derive failed; defaulting false", "err", err)
		hierarchy = false
	}

	c.conn.SendPriority(&pb.ServerMessage{
		RequestId: reqID,
		Payload: &pb.ServerMessage_HelloResponse{
			HelloResponse: &pb.HelloResponse{
				ServerVersion: serverVersion,
				Capabilities: &pb.Capabilities{
					ResumeSupported: true,
					// tag_edit_supported mirrors the catalog's write-ability: over a
					// Store opened via OpenReadOnly (auryn-migration external-builder
					// mode) Write always fails with catalog.ErrReadOnly, so advertising
					// true here would let a client open an "Edit Tags" UI that is
					// guaranteed to fail at runtime. See handleUpdateTags's ErrReadOnly
					// branch for the matching wire-error path.
					TagEditSupported: !c.h.store.ReadOnly(),
				},
				Backend: &pb.BackendCapabilities{
					SupportsFileHierarchy: hierarchy,
					MetadataKeyVocabulary: vocab,
				},
			},
		},
	})
}

func (c *connState) catalogHandler() *CatalogHandler { return &CatalogHandler{Store: c.h.store} }

func (c *connState) handleListFiles(ctx context.Context, reqID uint64, req *pb.ListFilesRequest) {
	// Clamp the limit before handing off (the store also clamps, but logging the
	// effective value is clearer).
	if req.GetLimit() == 0 {
		req.Limit = defaultListLimit
	} else if req.GetLimit() > maxListLimit {
		req.Limit = maxListLimit
	}

	resp, err := c.catalogHandler().ListFiles(ctx, req)
	if err != nil {
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, "ListFiles failed", err.Error())
		return
	}

	c.h.log.Info("ws: ListFiles served", "remote", c.remote, "request_id", reqID,
		"returned", len(resp.GetFiles()), "limit", req.GetLimit(), "next_token", resp.GetNextPageToken())

	c.conn.SendPriority(&pb.ServerMessage{
		RequestId: reqID,
		Payload:   &pb.ServerMessage_ListFiles{ListFiles: resp},
	})
}

func (c *connState) handleGetVocabulary(ctx context.Context, reqID uint64, req *pb.GetVocabularyRequest) {
	resp, err := c.catalogHandler().GetVocabulary(ctx, req)
	if err != nil {
		// GetVocabulary takes no client-supplied fields, so a failure is an internal
		// DB fault, not a bad request.
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_INTERNAL, "GetVocabulary failed", err.Error())
		return
	}
	c.h.log.Info("ws: GetVocabulary served", "remote", c.remote, "request_id", reqID,
		"customers", len(resp.GetCustomers()), "sources", len(resp.GetSources()), "tag_facets", len(resp.GetTags()))
	c.conn.SendPriority(&pb.ServerMessage{
		RequestId: reqID,
		Payload:   &pb.ServerMessage_GetVocabulary{GetVocabulary: resp},
	})
}

func (c *connState) handleGetFile(ctx context.Context, reqID uint64, req *pb.GetFileRequest) {
	resp, err := c.catalogHandler().GetFile(ctx, req)
	if err != nil {
		var nf errFileNotFound
		if errors.As(err, &nf) {
			c.sendError(reqID, 0, pb.ErrorCode_ERROR_NOT_FOUND, nf.Error(), "")
			return
		}
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, "GetFile failed", err.Error())
		return
	}

	c.h.log.Info("ws: GetFile served", "remote", c.remote, "request_id", reqID,
		"file_id", resp.GetSummary().GetId(), "key", resp.GetSummary().GetS3Key(),
		"topics", len(resp.GetTopics()))

	c.conn.SendPriority(&pb.ServerMessage{
		RequestId: reqID,
		Payload:   &pb.ServerMessage_GetFile{GetFile: resp},
	})
}

func (c *connState) handleUpdateTags(ctx context.Context, reqID uint64, req *pb.UpdateTagsRequest) {
	// Fast-path: reject before touching CatalogHandler at all. An UpdateTags
	// with EMPTY set_tags/unset_keys performs zero writes downstream (the loops
	// below just don't iterate), so without this check it would read back
	// EffectiveTags and return success even on a read-only store — an
	// inconsistent capability story given tag_edit_supported=false. The
	// errors.Is(err, catalog.ErrReadOnly) branch below stays as a backstop for
	// any future write path that bubbles the same error.
	if c.h.store.ReadOnly() {
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, readOnlyTagEditMessage, catalog.ErrReadOnly.Error())
		return
	}

	resp, err := c.catalogHandler().UpdateTags(ctx, req)
	if err != nil {
		var nf errFileNotFound
		if errors.As(err, &nf) {
			c.sendError(reqID, 0, pb.ErrorCode_ERROR_NOT_FOUND, nf.Error(), "")
			return
		}
		if errors.Is(err, catalog.ErrReadOnly) {
			c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, readOnlyTagEditMessage, err.Error())
			return
		}
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, "UpdateTags failed", err.Error())
		return
	}

	c.h.log.Info("ws: UpdateTags served", "remote", c.remote, "request_id", reqID,
		"file_id", req.GetFileId(), "set", len(req.GetSetTags()), "unset", len(req.GetUnsetKeys()),
		"effective", len(resp.GetEffectiveTags()))

	c.conn.SendPriority(&pb.ServerMessage{
		RequestId: reqID,
		Payload:   &pb.ServerMessage_UpdateTags{UpdateTags: resp},
	})
}

// sendError queues an Error frame on the priority channel. reqID routes a failed
// RPC; subID routes a fatal stream error (spec §6.7).
func (c *connState) sendError(reqID, subID uint64, code pb.ErrorCode, message, details string) {
	if len(message) > 256 {
		message = message[:256]
	}
	if len(details) > 2048 {
		details = details[:2048]
	}
	c.conn.SendPriority(&pb.ServerMessage{
		RequestId:      reqID,
		SubscriptionId: subID,
		Payload: &pb.ServerMessage_Error{
			Error: &pb.Error{Code: code, Message: message, Details: details},
		},
	})
}

func isNormalClose(err error) bool {
	if errors.Is(err, context.Canceled) {
		return true
	}
	status := websocket.CloseStatus(err)
	return status == websocket.StatusNormalClosure || status == websocket.StatusGoingAway
}
