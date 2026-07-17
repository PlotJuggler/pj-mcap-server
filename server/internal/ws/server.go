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
	"sync/atomic"
	"time"

	"nhooyr.io/websocket"

	"pj-cloud/server/internal/authn"
	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/config"
	"pj-cloud/server/internal/metrics"
	"pj-cloud/server/internal/tagipc"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// ServerVersion returns the server version string reported in HelloResponse and
// the dashboard footer.
func ServerVersion() string { return serverVersion }

const (
	// protocolVersion 2 (2026-07-17): OpenFresh is key-addressed (s3_keys, not
	// file_ids). A v1 client that would send catalog rowids is rejected at Hello
	// with ERROR_PROTOCOL_VERSION rather than failing later with "no s3_keys".
	protocolVersion  = 2
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
	// generous for OpenFresh with many s3_keys.
	readLimit = 8 << 20 // 8 MiB
	// defaultHelloDeadline bounds how long a connection may sit unauthenticated:
	// a peer that never completes a successful Hello is closed at the deadline
	// rather than lingering until the keepalive reaper notices it.
	defaultHelloDeadline = 10 * time.Second
	// authFailFlushGrace is the short window closeAfterAuthFailure gives the
	// write loop to flush the queued AUTH_FAILED error frame before the socket
	// is torn down.
	authFailFlushGrace = 250 * time.Millisecond
)

// readOnlyTagEditMessage is the operator-facing message for "tag editing is
// unavailable: no tag-IPC forwarder is configured". The catalog is ALWAYS
// read-only (the Python builder is the sole writer), so "the catalog is
// read-only" is never the actionable fact here — every deployment is
// read-only; what varies is whether catalog.tag_ipc_socket (-tag-ipc-socket /
// PJ_CLOUD_TAG_IPC_SOCKET) points at a running builder's IPC endpoint. Name
// that condition instead so an operator knows exactly what to configure.
const readOnlyTagEditMessage = "tag editing is disabled: no tag-edit IPC socket is configured (catalog.tag_ipc_socket)"

// tagIPCUnavailableMessage is the caller-facing message for a D2 forward
// failure (busy, connection refused, timeout, ...) — CATALOG_CONTRACT.md §10:
// not a guarantee the edit never lands, just that this request couldn't
// confirm it; the caller should retry. The underlying error goes in Details.
const tagIPCUnavailableMessage = "tag edit service unavailable; try again"

// tagIPCKeyGoneMessage (S1) is the caller-facing message when phase 1 resolved
// the wire file_id to a key, but by the time the edit reached the Python
// builder that key no longer names a cataloged file (tagipc.ErrNotFound) — a
// catalog rebuild raced the request. "file <id> not found" would be
// misleading here: id WAS a real file a moment ago; it's the CURRENT catalog
// that no longer has it under that key.
const tagIPCKeyGoneMessage = "file no longer present in the catalog (it may have been re-cataloged); refresh the list"

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
	// tagIPC, if set, is the D2 tag-edit IPC forwarder (CATALOG_CONTRACT.md
	// §10): when the store is read-only AND this is non-nil, handleUpdateTags
	// forwards set/unset edits to the Python catalog builder's UNIX-socket
	// endpoint instead of rejecting them outright. nil disables forwarding
	// (the pre-D2 read-only rejection behavior).
	tagIPC *tagipc.Client
	// compressor wraps allowlisted RPC responses in an EncodedServerMessage when a
	// client opts in via Hello (the compressed-envelope path). nil => the feature
	// is off (the default until SetResponseCompression is called; catalog-only
	// test handlers stay raw).
	compressor *responseCompressor
	// helloDeadline overrides defaultHelloDeadline when > 0 (tests).
	helloDeadline time.Duration
}

// SetHelloDeadline overrides the unauthenticated-connection deadline (tests;
// 0 keeps the default).
func (h *Handler) SetHelloDeadline(d time.Duration) { h.helloDeadline = d }

// SetResponseCompression enables (or disables) the compressed-envelope path from
// the transport config. Builds the shared zstd encoder once; a disabled config
// leaves the compressor nil (every response marshals raw). Call once at startup
// before serving. Returns an error only if the encoder cannot be constructed.
func (h *Handler) SetResponseCompression(cfg config.ResponseCompressionConfig) error {
	rc, err := newResponseCompressor(cfg, h.metrics)
	if err != nil {
		return err
	}
	h.compressor = rc
	return nil
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

// SetTagIPC wires the D2 tag-edit IPC forwarder into the handler (see the
// Handler.tagIPC field doc). Idempotent; call once at startup, only when
// catalog.tag_ipc_socket is configured.
func (h *Handler) SetTagIPC(c *tagipc.Client) {
	h.tagIPC = c
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
		conn:   newConn(wsConn, writeTimeout, h.compressor),
		remote: remote,
		subs:   map[uint64]*subHandle{},
	}
	// Hello deadline: a connection that never authenticates must not linger
	// until the keepalive reaper notices it — close it at the deadline. Checked
	// at fire time so a successful Hello simply lets the timer lapse.
	hd := h.helloDeadline
	if hd <= 0 {
		hd = defaultHelloDeadline
	}
	helloTimer := time.AfterFunc(hd, func() {
		if !c.authenticated.Load() {
			h.log.Warn("ws: no successful Hello within deadline; closing", "remote", remote, "deadline", hd)
			_ = wsConn.Close(websocket.StatusPolicyViolation, "Hello deadline exceeded")
		}
	})
	defer helloTimer.Stop()
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
	h      *Handler
	conn   *conn
	remote string
	// authenticated flips true on the first successful Hello. Atomic because the
	// Hello-deadline timer goroutine reads it while the read loop writes it.
	authenticated atomic.Bool

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
	// gen is the registry ownership token from BindConsumer: detachAll presents
	// it so a stale connection's teardown cannot detach a consumer that a newer
	// connection has since taken over (resume takeover).
	gen uint64
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
	type ownedSub struct{ id, gen uint64 }
	c.mu.Lock()
	subs := make([]ownedSub, 0, len(c.subs))
	for id, h := range c.subs {
		h.detaching = true
		subs = append(subs, ownedSub{id: id, gen: h.gen})
	}
	c.mu.Unlock()
	for _, s := range subs {
		// Generation-checked: a no-op if a resume on another connection has
		// since taken the binding over (this connection is a stale owner).
		c.h.sess.Registry.Detach(s.id, s.gen)
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
	if c.authenticated.Load() {
		return true
	}
	c.sendError(reqID, 0, pb.ErrorCode_ERROR_AUTH_FAILED, "Hello handshake required before this operation", "")
	return false
}

// closeAfterAuthFailure tears the socket down after a failed Hello credential
// check, giving the write loop a short window to flush the queued AUTH_FAILED
// error frame first. Closing unblocks the read loop, so a peer with a bad token
// gets exactly one Error frame per connection — never an in-connection retry
// loop against the constant-time compare.
func (c *connState) closeAfterAuthFailure() {
	ws := c.conn.ws
	if ws == nil {
		return // write-only unit fixtures have no real socket
	}
	go func() {
		deadline := time.Now().Add(authFailFlushGrace)
		for time.Now().Before(deadline) && len(c.conn.priorityCh) > 0 {
			time.Sleep(2 * time.Millisecond)
		}
		// The channel empties when the write loop DEQUEUES the frame, not when
		// the socket write completes — settle briefly so the in-flight error
		// write finishes before the close frame follows it onto the wire.
		time.Sleep(50 * time.Millisecond)
		_ = ws.Close(websocket.StatusPolicyViolation, "authentication failed")
	}()
}

// clientAcceptsZstd reports whether the client's Hello advertised the ZSTD
// response encoding in accepted_response_encodings. Empty list (old clients /
// proto3 default) => false => raw frames only.
func clientAcceptsZstd(hello *pb.Hello) bool {
	for _, e := range hello.GetAcceptedResponseEncodings() {
		if e == pb.CompressionEncoding_COMPRESSION_ENCODING_ZSTD {
			return true
		}
	}
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
			c.closeAfterAuthFailure()
			return
		}
	}
	c.authenticated.Store(true)

	// Negotiate the compressed-envelope path AFTER auth: record whether this
	// client advertised a compression encoding the server supports. Effective only
	// when the server compressor is also enabled (h.compressor != nil). The
	// HelloResponse below is NOT allowlisted, so it always goes raw regardless.
	c.conn.setAcceptEncoding(clientAcceptsZstd(hello))

	// BackendCapabilities are DERIVED LIVE from the catalog (Plan D Task 8), not
	// hardcoded: the metadata-key vocabulary is the constant derived keys UNION the
	// catalog's distinct effective-tag keys, and supports_file_hierarchy is true
	// iff any indexed OBJECT key bears a '/'. The flat S3 nissan corpus thus
	// reports hierarchy=false + the derived-key vocabulary (the C++ B3 live test's
	// ground truth). BackendCaps pins ONE db handle for both queries (B1) so a
	// catalog swap between them cannot advertise a mixed-generation view. A query
	// error is non-fatal: fall back to the stable derived floor / no-hierarchy so
	// a transient DB hiccup never blocks connect.
	ctx := context.Background()
	vocab, hierarchy, err := catalog.BackendCaps(ctx, c.h.store)
	if err != nil {
		c.h.log.Warn("ws: backend-capabilities derive failed; using derived floor", "err", err)
		vocab, hierarchy = catalog.DerivedMetadataKeys(), false
	}

	c.conn.SendPriority(&pb.ServerMessage{
		RequestId: reqID,
		Payload: &pb.ServerMessage_HelloResponse{
			HelloResponse: &pb.HelloResponse{
				ServerVersion: serverVersion,
				Capabilities: &pb.Capabilities{
					ResumeSupported: true,
					// tag_edit_supported: the catalog is always read-only (the Python
					// builder is the sole writer), so a tag edit can only ever succeed
					// when a D2 tag-IPC forwarder is configured (c.h.tagIPC != nil) —
					// forwarded to the Python builder over the UNIX socket
					// (CATALOG_CONTRACT.md §10). Advertising true with no forwarder
					// configured would let a client open an "Edit Tags" UI that is
					// guaranteed to fail at runtime. See handleUpdateTags for the
					// matching reject/forward branches.
					TagEditSupported: c.h.tagIPC != nil,
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
		// Stale generation handles (a rebuild renumbered the ids) are RETRYABLE
		// and get their own code so the client can transparently re-fetch the
		// vocabulary + restart the listing; everything else is a bad request.
		var stale errStaleCatalog
		if errors.As(err, &stale) {
			c.sendError(reqID, 0, pb.ErrorCode_ERROR_STALE_CATALOG, stale.Error(), "")
			return
		}
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
		var nfk errFileNotFoundByKey
		if errors.As(err, &nfk) {
			c.sendError(reqID, 0, pb.ErrorCode_ERROR_NOT_FOUND, nfk.Error(), "")
			return
		}
		var empty errEmptyS3Key
		if errors.As(err, &empty) {
			c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, empty.Error(), "")
			return
		}
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, "GetFile failed", err.Error())
		return
	}

	// Key-addressed requests IGNORE file_id by design (ids are generation-scoped
	// handles that renumber across rebuilds — the key surviving a stale id is
	// the feature). Debug-visibility only when both were sent and they disagree;
	// NEVER a failure.
	if req.S3Key != nil && req.GetFileId() != 0 && req.GetFileId() != resp.GetSummary().GetId() {
		c.h.log.Debug("ws: GetFile id/key disagree; key wins (ids renumber across rebuilds)",
			"remote", c.remote, "request_id", reqID, "key", req.GetS3Key(),
			"wire_file_id", req.GetFileId(), "current_file_id", resp.GetSummary().GetId())
	}

	c.h.log.Info("ws: GetFile served", "remote", c.remote, "request_id", reqID,
		"file_id", resp.GetSummary().GetId(), "key", resp.GetSummary().GetS3Key(),
		"topics", len(resp.GetTopics()))

	c.conn.SendPriority(&pb.ServerMessage{
		RequestId: reqID,
		Payload:   &pb.ServerMessage_GetFile{GetFile: resp},
	})
}

// handleUpdateTags is IPC-only: the catalog is always read-only (the Python
// builder is the sole writer, catalog-migration §2.6), so an edit can only
// ever be forwarded to the Python builder's tag-edit IPC endpoint. With no
// forwarder configured, every UpdateTags is rejected outright — including an
// EMPTY set_tags/unset_keys request, so tag_edit_supported=false is never
// contradicted by a no-op "success".
func (c *connState) handleUpdateTags(ctx context.Context, reqID uint64, req *pb.UpdateTagsRequest) {
	// Malformed-request validation FIRST (Codex review): a present-but-EMPTY
	// s3_key is invalid regardless of whether tag editing is available, and the
	// client should learn about the malformation, not the capability — so it
	// gets the same errEmptyS3Key answer on a no-IPC server as on the forward
	// path below (which re-checks for its own callers).
	if req.S3Key != nil && req.GetS3Key() == "" {
		c.h.log.Warn("ws: UpdateTags rejected (empty s3_key)", "remote", c.remote,
			"request_id", reqID)
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, errEmptyS3Key{}.Error(), "")
		return
	}
	if c.h.tagIPC == nil {
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, readOnlyTagEditMessage, "")
		return
	}
	c.handleUpdateTagsForwarded(ctx, reqID, req)
}

// handleUpdateTagsForwarded is the D2 forward path (CATALOG_CONTRACT.md §10):
// the store is read-only and a tag-IPC client is configured, so the edit is
// forwarded to the Python catalog builder over its UNIX socket rather than
// rejected.
//
// TRUST BOUNDARY: the socket has NO auth of its own (CATALOG_CONTRACT.md §10
// "Socket permissions & trust boundary") — the Go WS bearer-auth check in
// Hello (see the c.h.auth.Verify call above) is the ONLY gate a tag edit
// passes through before reaching this forward. Every forwarded edit is logged
// with the WS remote address for that reason.
func (c *connState) handleUpdateTagsForwarded(ctx context.Context, reqID uint64, req *pb.UpdateTagsRequest) {
	// Phase 1: determine the object key the edit addresses (the endpoint
	// addresses files by key, not id — §10 "Key-based addressing").
	//
	// Key-addressed requests (s3_key PRESENT) use the client's key VERBATIM —
	// no id->key resolve at all, so a stale generation-scoped file_id can never
	// redirect the edit onto the wrong file (file_id is IGNORED by design; no
	// mismatch validation). Present-but-EMPTY is rejected outright — never a
	// silent fallback to file_id. An unknown key is NOT pre-validated here:
	// the IPC's own 404 maps to tagIPCKeyGoneMessage below, same as the legacy
	// path's phase-1-resolved-then-vanished race.
	//
	// Legacy id-addressed requests (s3_key ABSENT) resolve the wire file_id ->
	// object key exactly as before. A single db.QueryContext call, so no B1
	// pinning is needed for that phase alone.
	var key string
	if req.S3Key != nil {
		key = req.GetS3Key()
		if key == "" {
			c.h.log.Warn("ws: UpdateTags rejected (empty s3_key)", "remote", c.remote,
				"request_id", reqID)
			c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, errEmptyS3Key{}.Error(), "")
			return
		}
	} else {
		// NOTE on generations: this request intentionally spans MULTIPLE db
		// pinning windows — phase 1 resolves id→key on the current generation,
		// phase 2 forwards over the IPC (the builder may publish a rebuild), and
		// phase 3 deliberately reopens to the NEWEST generation to re-read the
		// authoritative tags. A rebuild landing between phases degrades cleanly:
		// key vanished => ERROR_NOT_FOUND (tagIPCKeyGoneMessage, S1); re-read
		// failed => the IPC response's own tags are returned as the fallback.
		var err error
		key, err = catalog.ObjectKeyForFile(ctx, c.h.store.DB(), req.GetFileId())
		if err != nil {
			if errors.Is(err, catalog.ErrFileNotFound) {
				c.h.log.Warn("ws: UpdateTags rejected (file not found)", "remote", c.remote,
					"request_id", reqID, "file_id", req.GetFileId())
				c.sendError(reqID, 0, pb.ErrorCode_ERROR_NOT_FOUND, errFileNotFound{id: req.GetFileId()}.Error(), "")
				return
			}
			c.h.log.Warn("ws: UpdateTags rejected (key resolve failed)", "remote", c.remote,
				"request_id", reqID, "file_id", req.GetFileId(), "err", err)
			c.sendError(reqID, 0, pb.ErrorCode_ERROR_INTERNAL, "UpdateTags failed (key resolve)", err.Error())
			return
		}
	}

	set := make(map[string]string, len(req.GetSetTags()))
	for _, t := range req.GetSetTags() {
		set[t.GetKey()] = t.GetValue()
	}

	// Phase 2: forward set/unset VERBATIM to the Python builder — it implements
	// the same mask-on-unset semantics the legacy Go writer had, so this side is
	// a pure transport, no business logic. Keep the response tags (ipcTags):
	// they are the writer's own authoritative post-edit tags_effective, and are
	// the B1-fix-part-2 fallback below if the local re-read can't confirm them.
	ipcTags, err := c.h.tagIPC.UpdateTags(ctx, key, set, req.GetUnsetKeys())
	if err != nil {
		if c.h.metrics != nil {
			c.h.metrics.TagIPCFailuresTotal.Inc()
		}
		if errors.Is(err, tagipc.ErrNotFound) {
			// S1: phase 1 resolved a real file, but the key vanished from the
			// catalog before the Python builder could act on it (a rebuild raced
			// this request) — "file <id> not found" would misleadingly imply id
			// never existed.
			c.h.log.Warn("ws: UpdateTags rejected (key gone before IPC apply)", "remote", c.remote,
				"request_id", reqID, "key", key, "err", err)
			c.sendError(reqID, 0, pb.ErrorCode_ERROR_NOT_FOUND, tagIPCKeyGoneMessage, err.Error())
			return
		}
		// ErrBusy, connection-refused, timeouts, and any other transport/protocol
		// failure all map to the SAME caller-facing message (§10 "the caller
		// should retry" — not a guarantee the edit never lands) — only the
		// details differ.
		c.h.log.Warn("ws: UpdateTags IPC forward failed", "remote", c.remote,
			"request_id", reqID, "key", key, "err", err)
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, tagIPCUnavailableMessage, err.Error())
		return
	}

	// B1: a catalog rebuild can land BETWEEN the IPC write above and the
	// re-read below, and only the 30s freshness ticker (main.go) otherwise
	// calls ReopenIfSwapped — without this call here, a rebuild landing in
	// that exact window would make the re-read answer from the OLD generation
	// (stale/missing tags) even though the edit already applied to the NEW
	// one. Log-only on error (the previous generation just keeps being served,
	// same contract as the ticker's own failure handling); count it as a
	// reopen like the ticker does when it does swap — this call and the
	// ticker's are just two different triggers for the same underlying event.
	if swapped, rerr := c.h.store.ReopenIfSwapped(ctx); rerr != nil {
		c.h.log.Warn("ws: tag-edit pre-re-read reopen-if-swapped failed; still serving the previous generation",
			"remote", c.remote, "request_id", reqID, "err", rerr)
	} else if swapped && c.h.metrics != nil {
		c.h.metrics.CatalogReopensTotal.Inc()
	}

	// Finding A1 (design review, MANDATORY): build the response by re-resolving
	// key -> CURRENT file_id -> tags_effective in ONE pinned generation. NEVER
	// reuse req.GetFileId() here — a catalog rebuild between phase 1 and now can
	// renumber file ids (§7), so the original id may by now name a DIFFERENT
	// file. EffectiveTagsByKey re-derives everything from the stable key.
	//
	// Precedence: the local re-read is PRIMARY — unlike the IPC response, it
	// proves THIS reader (the one about to answer the client) can actually see
	// the edit against its own live catalog handle, which is the property A1
	// cares about. Only when that re-read itself fails (ErrFileNotFound — e.g.
	// the key was renamed/removed by a rebuild racing this very request even
	// after the reopen above — or any other, transient, error) do we fall back
	// to the IPC response's own tags: the edit DID apply (phase 2 returned 200),
	// so answering ERROR_INTERNAL there would be wrong — the writer already
	// told us the resulting tags_effective.
	var effectiveTags []*pb.Tag
	if tags, terr := catalog.EffectiveTagsByKey(ctx, c.h.store, key); terr != nil {
		c.h.log.Warn("ws: tag-edit post-edit re-read failed; falling back to the IPC response's tags",
			"remote", c.remote, "request_id", reqID, "key", key, "err", terr)
		effectiveTags = tagipcTagsToProto(ipcTags)
	} else {
		effectiveTags = tagsToProto(tags)
	}

	if c.h.metrics != nil {
		c.h.metrics.TagIPCForwardsTotal.Inc()
	}
	c.h.log.Info("ws: UpdateTags forwarded to tag-IPC", "remote", c.remote, "request_id", reqID,
		"key", key, "set", len(req.GetSetTags()), "unset", len(req.GetUnsetKeys()), "effective", len(effectiveTags))

	c.conn.SendPriority(&pb.ServerMessage{
		RequestId: reqID,
		Payload:   &pb.ServerMessage_UpdateTags{UpdateTags: &pb.UpdateTagsResponse{EffectiveTags: effectiveTags}},
	})
}

// tagipcTagsToProto converts the tag-IPC client's response tags to the wire
// shape — used only as the B1-fix fallback in handleUpdateTagsForwarded when
// the mandatory local post-edit re-read (finding A1) itself fails.
func tagipcTagsToProto(tags []tagipc.Tag) []*pb.Tag {
	out := make([]*pb.Tag, 0, len(tags))
	for _, t := range tags {
		out = append(out, &pb.Tag{Key: t.Key, Value: t.Value, IsOverride: t.IsOverride})
	}
	return out
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
