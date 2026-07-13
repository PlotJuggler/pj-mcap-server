// handlers_session.go implements the session/streaming half of the WS protocol
// (Plan A Tasks 28/29/30, design spec §6.3-6.7 + §8.2). It plans a fresh session
// from the SQLite catalog Store + the MCAP chunk index, registers it, spawns the
// producer/consumer pair, and routes Resume/Cancel/Ack.
//
// DEVIATIONS FROM PLAN A (documented, semantics unchanged):
//   - Plan A's SessionHandler keys off the SQLite *catalog.Store + a separate
//     ChunkIndexLoader interface. Our slice uses catalog.GetFile for the file
//     record (id + time bounds + key) and format.Codec.ChunkIndex directly for
//     the per-file chunk index (KEPT on-demand — the DB never stored it). The
//     plan/producer/consumer/registry calls are byte-identical to Plan A.
//   - The Plan A test uses ProducerOpts.MaxBatchAgeMs (an int); our merged session
//     engine uses ProducerOpts.MaxBatchAge (a time.Duration) — wired from config.
package ws

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	"pj-cloud/server/internal/catalog"
	"pj-cloud/server/internal/config"
	"pj-cloud/server/internal/format"
	"pj-cloud/server/internal/metrics"
	"pj-cloud/server/internal/session"
	"pj-cloud/server/internal/storage"
	pb "pj-cloud/server/internal/wire/pj_cloud"
)

// SessionDeps is everything the session handler needs that is shared across all
// connections: the catalog Store (file lookup), the codec (chunk index), the
// storage blob store (ranged reads for both the chunk-index load and the
// producer), the registry, and the tuning block.
type SessionDeps struct {
	Store    *catalog.Store
	Codec    format.Codec
	Blob     storage.BlobStore // satisfies session.RangeGetter (GetRange) AND the codec's Head/GetRange
	Registry *session.Registry
	Cfg      config.SessionConfig
	Log      *slog.Logger
	// Metrics, if set, records session/throughput counters and guards the
	// producer/consumer goroutines against panics (spec §8.1). Optional.
	Metrics *metrics.Metrics

	// IdxCache is the shared per-file chunk-index cache. The INDEXER pre-warms
	// it from its background scan (which already reads each file's summary), so
	// a download's plan phase is a pure in-memory hit — no WAN read. A cold miss
	// (a file not yet indexed) falls back to a direct codec read + cache fill.
	// Nil => no caching (every plan reads storage).
	IdxCache *format.ChunkIndexCache

	// idxSF dedupes concurrent COLD loads of the same (key,etag): a stitched
	// multi-file open + the background warmer + parallel sessions can all miss the
	// same file at once; without this each would issue its own WAN summary read.
	// singleflight collapses them to ONE load (the rest wait + share the result).
	// Zero-value usable.
	idxSF singleflight.Group
}

// chunkIndexLoadTimeout bounds a single detached chunk-index load (the storage
// layer has no HTTP client timeout, so a black-holed WAN read would otherwise
// strand the singleflight leader goroutine — and every later opener of the same
// file — forever). Generous: a summary read is normally sub-second; this only
// fires on a hung connection, after which the file self-heals.
const chunkIndexLoadTimeout = 2 * time.Minute

// cachedChunkIndex returns the chunk index for (key,etag) from IdxCache, loading +
// caching it on a miss. Concurrent cold misses of the same file are collapsed to a
// single codec read via singleflight (3.1).
func (d *SessionDeps) cachedChunkIndex(
	ctx context.Context, key, etag string, fileID uint64) (session.FileChunkIndex, error) {
	if d.IdxCache == nil {
		// No cache: a single direct load (the singleflight+Put+re-Get dance below
		// would otherwise do two WAN reads on a nil cache — Put/Get are no-ops).
		return d.Codec.ChunkIndex(ctx, d.Blob, key, fileID)
	}
	if idx, ok := d.IdxCache.Get(key, etag, fileID); ok {
		return idx, nil
	}
	// Dedup the load: one leader fetches + caches, the rest wait + share. The
	// leader runs detached from any single caller's cancellation
	// (context.WithoutCancel) so a bailing leader doesn't fail the waiters; DoChan +
	// a per-caller select lets each waiter still bail on its OWN ctx.
	sfKey := key + "|" + etag
	ch := d.idxSF.DoChan(sfKey, func() (interface{}, error) {
		// Detach from any single caller's cancellation (WithoutCancel) BUT keep a
		// bound: WithoutCancel also drops the deadline, so a hung WAN summary read
		// would otherwise strand this goroutine forever. The per-load timeout caps it.
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), chunkIndexLoadTimeout)
		defer cancel()
		idx, err := d.Codec.ChunkIndex(loadCtx, d.Blob, key, fileID)
		if err != nil {
			return session.FileChunkIndex{}, err
		}
		d.IdxCache.Put(key, etag, idx)
		return idx, nil
	})
	select {
	case res := <-ch:
		if res.Err != nil {
			return session.FileChunkIndex{}, res.Err
		}
		// The cache restamps for THIS caller's fileID. On the rare race where the
		// entry was already evicted between Put and this Get, load directly.
		if idx, ok := d.IdxCache.Get(key, etag, fileID); ok {
			return idx, nil
		}
		idx, err := d.Codec.ChunkIndex(ctx, d.Blob, key, fileID)
		if err != nil {
			return session.FileChunkIndex{}, err
		}
		d.IdxCache.Put(key, etag, idx)
		return idx, nil
	case <-ctx.Done():
		return session.FileChunkIndex{}, ctx.Err()
	}
}

// producerOpts maps the config block onto the session engine's ProducerOpts.
func (d *SessionDeps) producerOpts(tr *session.TimeWindow) session.ProducerOpts {
	return session.ProducerOpts{
		MaxBatchBytes:          d.Cfg.MaxBatchBytes,
		MaxBatchAge:            d.Cfg.MaxBatchAge,
		MaxMessageBytes:        d.Cfg.MaxMessageBytes,
		BodyZstdLevel:          d.Cfg.BodyZstdLevel,
		CompressThresholdBytes: d.Cfg.CompressThresholdBytes,
		TimeRange:              tr,
	}
}

// handleOpenSession routes a fresh-open or resume on this connection.
func (c *connState) handleOpenSession(ctx context.Context, reqID uint64, req *pb.OpenSessionRequest) {
	switch mode := req.GetMode().(type) {
	case *pb.OpenSessionRequest_Fresh:
		c.openFresh(ctx, reqID, mode.Fresh)
	case *pb.OpenSessionRequest_Resume:
		c.openResume(ctx, reqID, mode.Resume)
	default:
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, "OpenSessionRequest.mode must be set", "")
	}
}

// openFresh is the largest handler: validate selection, load file records +
// chunk indexes, build the plan, register, reply, spawn producer + consumer.
func (c *connState) openFresh(ctx context.Context, reqID uint64, fresh *pb.OpenFresh) {
	d := c.h.sess
	if len(fresh.GetFileIds()) == 0 {
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, "no file_ids in OpenFresh", "")
		return
	}

	// DEBUG: log the request verbatim so an empty plan (chunks=0) can be traced
	// to exactly which file_ids / topics / time-range the client asked for.
	{
		tw := fresh.GetTimeRange()
		trStr := "whole-range"
		if tw != nil {
			trStr = fmt.Sprintf("[%d,%d]", tw.GetStartNs(), tw.GetEndNs())
		}
		names := fresh.GetTopicNames()
		shown := names
		if len(shown) > 8 {
			shown = shown[:8]
		}
		d.Log.Info("ws: OpenFresh request", "remote", c.remote, "file_ids", fresh.GetFileIds(),
			"topic_count", len(names), "topics", shown, "time_range", trStr)
	}

	// (a) Resolve file records (id + time bounds) and object keys from the SQLite
	// catalog. Unknown id => NOT_FOUND.
	files := make([]session.FileRecord, 0, len(fresh.GetFileIds()))
	keys := make(session.FileKeys, len(fresh.GetFileIds()))
	etags := make(map[uint64]string, len(fresh.GetFileIds()))
	for _, id := range fresh.GetFileIds() {
		rec, err := catalog.GetFile(ctx, d.Store, id)
		if err != nil {
			c.sendError(reqID, 0, pb.ErrorCode_ERROR_NOT_FOUND, fmt.Sprintf("no file with id %d", id), "")
			return
		}
		files = append(files, session.FileRecord{
			ID:          rec.ID,
			StartTimeNs: rec.StartTimeNs,
			EndTimeNs:   rec.EndTimeNs,
		})
		keys[rec.ID] = rec.S3Key
		etags[rec.ID] = rec.S3ETag
	}

	// Parse the optional retrieval window up front so we can skip files that lie
	// entirely outside it (below).
	var tw *session.TimeWindow
	if tr := fresh.GetTimeRange(); tr != nil {
		tw = &session.TimeWindow{StartNs: tr.GetStartNs(), EndNs: tr.GetEndNs()}
	}

	// (b) Load each in-window file's chunk index (summary-only ranged reads) +
	// collect the per-topic schema bindings (first occurrence wins across
	// stitched files). The per-file loads run CONCURRENTLY (bounded). CRUCIAL
	// WAN OPTIMIZATION: a file whose [StartTimeNs, EndTimeNs] does not intersect
	// the requested window contributes ZERO chunks, so its summary read (~1 MB
	// of schemas per 171-topic file, BANDWIDTH-bound — concurrency can't hide it
	// over a saturated link) is pure waste. Skipping it leaves an empty
	// FileChunkIndex; BuildPlan's overlap check + merged horizon use the file
	// RECORDS (not the indexes), so they stay correct, and PlanChunks over an
	// empty index simply yields nothing. A ~9-min window over a 34-file/7.4h
	// aggregate drops the plan from 34 summary reads to ~5.
	// include_latched needs the chunk indexes of files BEFORE the window too, to
	// seed each absent topic's last-before-window value (a chunk-index cache hit
	// on a warm server). Files AFTER the window are always skipped.
	includeLatched := fresh.GetIncludeLatched()
	planStart := time.Now()
	indexes := make([]session.FileChunkIndex, len(files))
	skipped := 0
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for i, f := range files {
		afterWindow := tw != nil && f.StartTimeNs >= tw.EndNs
		beforeWindow := tw != nil && f.EndTimeNs < tw.StartNs
		if afterWindow || (beforeWindow && !includeLatched) {
			indexes[i] = session.FileChunkIndex{FileID: f.ID} // outside window: no chunks
			skipped++
			continue
		}
		g.Go(func() error {
			idx, err := d.cachedChunkIndex(gctx, keys[f.ID], etags[f.ID], f.ID)
			if err != nil {
				return fmt.Errorf("load chunk index for file %d: %w", f.ID, err)
			}
			indexes[i] = idx
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		// The cause matters operationally (storage outage vs throttling vs a
		// corrupt object) and the GUI's per-topic ledger shows only the message
		// field — log the full error AND make the message diagnosable. This is
		// a self-hosted single-tenant server; the bucket error text is the
		// operator's own data.
		c.h.log.Warn("ws: OpenSession plan-build failed", "remote", c.remote,
			"files", len(files), "elapsed", time.Since(planStart).Round(time.Millisecond), "err", err)
		msg := err.Error()
		if len(msg) > 256 {
			msg = msg[:256]
		}
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_S3_UNAVAILABLE, msg, err.Error())
		return
	}
	topicSchemas := map[string]format.TopicSchemaInfo{}
	for _, idx := range indexes {
		for _, ts := range idx.Schemas {
			if _, ok := topicSchemas[ts.TopicName]; !ok {
				topicSchemas[ts.TopicName] = ts
			}
		}
	}

	// (c) Build the plan (overlap + inverted-range reject, silent topic drop).
	// tw was parsed above (used to skip out-of-window chunk-index loads).
	plan, err := session.BuildPlan(ctx, files, indexes, session.PlanArgs{
		TopicNames:     fresh.GetTopicNames(),
		TimeRange:      tw,
		IncludeLatched: includeLatched,
	})
	if err != nil {
		if session.ErrIsOverlap(err) {
			c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, err.Error(), "")
			return
		}
		// Inverted time range etc. are invalid requests too.
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_INVALID_REQUEST, err.Error(), "")
		return
	}

	// (d) Assign topic_id_map + schemas (the wire bindings) over ONLY the topics
	// that survive into the plan (so the empty-plan contract yields empty maps,
	// and absent-everywhere topics never leak into the bindings).
	survivingTopics := planTopicSet(plan)
	topicBindings, schemaBindings, topicIDByName, schemaIDByName :=
		assignBindings(topicSchemas, survivingTopics)

	// (e) Register the session (RESOURCE_LIMIT at capacity).
	retain := session.NewRetainBuffer(session.RetainOpts{
		MaxSeqs:  d.Cfg.RetainMaxSeqs,
		MaxBytes: d.Cfg.RetainMaxBytes,
	})
	state := &session.SessionState{
		Plan:           plan,
		Retain:         retain,
		AckCh:          make(chan uint64, 8),
		ProducerDone:   make(chan struct{}),
		TopicBindings:  toSessionTopicBindings(topicBindings),
		SchemaBindings: toSessionSchemaBindings(schemaBindings),
	}
	state.Producer = &session.Producer{
		Plan:        plan,
		ChunkReader: session.NewChunkReader(d.Blob, keys),
		ChunkIter:   session.NewChunkIterator(indexes),
		Retain:      retain,
		Opts:        d.producerOpts(tw),
		TopicID:     topicIDByName,
		SchemaID:    schemaIDByName,
	}
	dropped := &droppedCounter{}
	state.Producer.OnDrop = func(string) { dropped.inc() }
	// Build the per-seq cumulative ledger as batches are appended; it seeds resume
	// counter carry-forward so the terminal Eos totals stay monotonic AND count
	// each delivered message exactly once across a reconnect-resume.
	state.Producer.OnAppend = func(seq, messages, bytes uint64) {
		state.RecordAppend(seq, messages, bytes)
	}

	registered, err := d.Registry.Register(ctx, state)
	if err != nil {
		retain.Close()
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_RESOURCE_LIMIT, err.Error(), "")
		return
	}
	if d.Metrics != nil {
		d.Metrics.SessionsTotal.Inc()
		d.Metrics.SessionsActive.Inc()
	}
	c.h.log.Info("session: open", "sub", registered.ID, "remote", c.remote,
		"files", len(files), "files_loaded", len(files)-skipped, "files_skipped_out_of_window", skipped,
		"chunks", len(plan.Chunks),
		"estimated_chunk_bytes", plan.EstimatedChunkBytes,
		"approximate_messages", plan.ApproximateMessages,
		"plan_elapsed", time.Since(planStart).Round(time.Millisecond))

	// (f) Reply with OpenSessionResponse (echoing subscription_id + bindings).
	c.conn.SendPriority(&pb.ServerMessage{
		RequestId: reqID,
		Payload: &pb.ServerMessage_OpenSession{OpenSession: &pb.OpenSessionResponse{
			SubscriptionId:      registered.ID,
			MergedTimeRange:     &pb.TimeRange{StartNs: plan.MergedStartNs, EndNs: plan.MergedEndNs},
			EstimatedChunkBytes: plan.EstimatedChunkBytes,
			ApproximateMessages: plan.ApproximateMessages,
			TopicIdMap:          topicBindings,
			Schemas:             schemaBindings,
		}},
	})

	// (g) Spawn the consumer bound to THIS connection FIRST, so the producer's
	// stream-fatal path can cancel it (preventing a spurious Eos{COMPLETE} from
	// racing the Eos{ERROR}).
	c.spawnConsumer(registered, dropped, 0)

	// (h) Spawn the producer (detached from WS lifetime). The whole body runs
	// inside a per-session Guard (spec §8.1): a producer panic must not crash the
	// process nor break sibling sessions — it is recovered, logged + counted, and
	// treated as a stream-fatal error (Error + Eos{ERROR}), while ProducerDone is
	// ALWAYS closed (deferred) so the consumer never hangs waiting on it.
	prodCtx, prodCancel := context.WithCancel(context.Background())
	registered.ProducerCancel = prodCancel
	go func() {
		defer close(registered.ProducerDone)
		var runErr error
		panicked := metrics.Guard(c.metrics(), d.Log, "session-producer", func() {
			runErr = registered.Producer.Run(prodCtx)
		})
		if d.Metrics != nil {
			d.Metrics.FetchedBytesTotal.Add(float64(registered.Producer.FetchedBytes()))
		}
		streamFatal := panicked || (runErr != nil && prodCtx.Err() == nil)
		if streamFatal {
			detail := "producer panicked (recovered)"
			if runErr != nil {
				detail = runErr.Error()
			}
			d.Log.Warn("session: producer ended with error", "sub", registered.ID, "err", detail, "panicked", panicked)
			// Stream-fatal (spec §6.7 routing): cancel the session FIRST (this
			// cancels the consumer's ctx so it exits without sending COMPLETE and
			// discards the retain buffer), then emit Error{subscription_id} +
			// Eos{ERROR} on the priority path. Best-effort; the WS may be gone.
			c.untrackSubscription(registered.ID)
			d.Registry.Cancel(registered.ID)
			c.conn.SendPriority(&pb.ServerMessage{
				SubscriptionId: registered.ID,
				Payload: &pb.ServerMessage_Error{Error: &pb.Error{
					Code: pb.ErrorCode_ERROR_S3_UNAVAILABLE, Message: "stream failed", Details: truncate(detail, 2048),
				}},
			})
			c.conn.SendPriority(&pb.ServerMessage{
				SubscriptionId: registered.ID,
				Payload:        &pb.ServerMessage_Eos{Eos: &pb.Eos{Reason: pb.EosReason_EOS_REASON_ERROR}},
			})
		}
	}()
}

// openResume reattaches a (still-live) detached session to this connection and
// replays everything after resume_after_seq (spec §6.6).
func (c *connState) openResume(ctx context.Context, reqID uint64, r *pb.OpenResume) {
	d := c.h.sess
	state, ok := d.Registry.Lookup(r.GetSubscriptionId())
	if !ok {
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_RESUME_NOT_POSSIBLE, "session evicted or never existed", "")
		return
	}
	if err := d.Registry.Reattach(r.GetSubscriptionId()); err != nil {
		c.sendError(reqID, 0, pb.ErrorCode_ERROR_RESUME_NOT_POSSIBLE, err.Error(), "")
		return
	}
	// Prune already-durable batches; the new consumer starts after resume_after_seq.
	state.Retain.Prune(r.GetResumeAfterSeq())

	// Re-send the SAME subscription_id + topic_id_map + schemas (no re-plan).
	c.conn.SendPriority(&pb.ServerMessage{
		RequestId: reqID,
		Payload: &pb.ServerMessage_OpenSession{OpenSession: &pb.OpenSessionResponse{
			SubscriptionId:      state.ID,
			MergedTimeRange:     &pb.TimeRange{StartNs: state.Plan.MergedStartNs, EndNs: state.Plan.MergedEndNs},
			EstimatedChunkBytes: state.Plan.EstimatedChunkBytes,
			ApproximateMessages: state.Plan.ApproximateMessages,
			TopicIdMap:          fromSessionTopicBindings(state.TopicBindings),
			Schemas:             fromSessionSchemaBindings(state.SchemaBindings),
		}},
	})

	c.spawnConsumer(state, &droppedCounter{}, r.GetResumeAfterSeq())
}

// spawnConsumer attaches a fresh consumer to this connection for the given
// session, tracks the subscription for disconnect-detach, and binds its cancel
// in the registry. startAfterSeq is 0 for a fresh open, resume_after_seq for a
// resume.
//
// The consumer's post-Run disposition has THREE cases:
//   - reached terminal Eos{COMPLETE} (producer done + buffer drained): tear the
//     session down (discard retain + deregister);
//   - cancelled because THIS connection is being torn down (handleDetaching):
//     keep the session alive — the producer keeps filling retain and the registry
//     eviction timer governs whether a later OpenResume can reattach;
//   - WS write failed mid-stream (consumer.Detached): same as detach — keep the
//     session; the read loop will Detach it (arm eviction) on teardown.
func (c *connState) spawnConsumer(state *session.SessionState, dropped *droppedCounter, startAfterSeq uint64) {
	d := c.h.sess
	consCtx, consCancel := context.WithCancel(context.Background())
	handle := &subHandle{cancel: consCancel}
	d.Registry.BindConsumer(state.ID, consCancel)
	c.trackSubscription(state.ID, handle)

	// Carry forward the cumulative delivered counters THROUGH startAfterSeq so the
	// terminal Eos.total_*_sent stays monotonic across a reconnect-resume AND
	// counts each delivered message exactly once (Slice 10 flagged defect 1).
	// Seeding from the ledger-through-resume_after_seq (rather than the prior
	// consumer's raw running total) excludes in-flight batches beyond the resume
	// point that the new consumer re-delivers — preventing a double count. On a
	// fresh open startAfterSeq==0 so these are 0.
	priorMsgs, priorBytes := state.CountersThrough(startAfterSeq)
	consumer := &session.Consumer{
		SubscriptionID:    state.ID,
		Writer:            c.conn, // *conn satisfies session.FrameWriter
		Retain:            state.Retain,
		ProducerDone:      state.ProducerDone,
		AckCh:             state.AckCh,
		ProgressEvery:     0, // engine default 1s
		StartAfterSeq:     startAfterSeq,
		EstimatedBytes:    state.Plan.EstimatedChunkBytes,
		EstimatedMessages: state.Plan.ApproximateMessages,
		MessagesSent:      priorMsgs,
		BytesSent:         priorBytes,
	}
	go func() {
		// Per-session consumer panic recovery (spec §8.1): a panic here must not
		// crash the process nor break siblings. On a recovered panic we fall
		// through to the teardown switch as if the consumer had stopped; the
		// connection teardown / eviction path handles the rest.
		panicked := metrics.Guard(c.metrics(), d.Log, "session-consumer", func() {
			consumer.Run(consCtx)
		})
		switch {
		case panicked:
			// The consumer goroutine died mid-stream; tear the session down so it
			// does not leak (a panicked consumer cannot reliably resume).
			c.untrackSubscription(state.ID)
			d.Registry.Cancel(state.ID)
		case consumer.Detached:
			// WS write failed mid-stream: leave the session live for resume; the
			// connection teardown path (detachAll -> Registry.Detach) arms eviction.
			// No counter persistence needed — the resumed consumer reseeds from the
			// producer's cumulative ledger through resume_after_seq.
		case c.handleIsDetaching(handle):
			// Cancelled because the connection is tearing down: keep the session live
			// (the producer keeps filling retain; the eviction timer governs resume).
		case consCtx.Err() != nil:
			// This consumer's ctx was cancelled by the registry for a reason OTHER
			// than connection-teardown: the session was Cancel'd (client
			// CancelSession), evicted (retain-window expiry), or torn down by
			// CancelAll on graceful shutdown. Registry.Cancel already did the teardown
			// (deregister + retain discard + onEvict), and the terminal frame
			// (Eos{CANCELLED} for handleCancel, or just the WS close on shutdown) is
			// owned by that path. Do NOT treat this as a terminal COMPLETE: do not Add
			// the throughput counters and do NOT log the per-session "session:
			// complete" accounting (that would emit a bogus delta_pct for a
			// partial/cancelled session — caught live during the shutdown gate).
		default:
			// Terminal completion (Eos sent): record throughput counters, log the
			// per-session fetch accounting (the estimated_chunk_bytes-within-5%
			// gate, log-only — no proto change), then discard retain + deregister.
			if m := c.metrics(); m != nil {
				m.MessagesSentTotal.Add(float64(consumer.MessagesSent))
				m.BytesSentTotal.Add(float64(consumer.BytesSent))
			}
			logSessionAccounting(d.Log, state, consumer.MessagesSent, consumer.BytesSent)
			c.untrackSubscription(state.ID)
			d.Registry.Cancel(state.ID)
		}
		_ = dropped
	}()
}

// logSessionAccounting emits the per-session completion slog line comparing the
// pre-flight estimated_chunk_bytes against the actual bytes the producer
// Range-GET from the blob store. delta_pct is |fetched - estimated| / fetched
// (the spec's "within 5%" preflight-accuracy contract, asserted in the Go
// component test TestEstimateWithin5Pct). This is observability only — no wire
// field carries fetched_bytes (the canonical proto is frozen).
func logSessionAccounting(log *slog.Logger, state *session.SessionState, msgsSent, bytesSent uint64) {
	if log == nil {
		return
	}
	estimated := state.Plan.EstimatedChunkBytes
	var fetched uint64
	if state.Producer != nil {
		fetched = state.Producer.FetchedBytes()
	}
	deltaPct := 0.0
	if fetched > 0 {
		diff := int64(fetched) - int64(estimated)
		if diff < 0 {
			diff = -diff
		}
		deltaPct = 100.0 * float64(diff) / float64(fetched)
	}
	log.Info("session: complete",
		"sub", state.ID,
		"fetched_bytes", fetched,
		"estimated_chunk_bytes", estimated,
		"delta_pct", deltaPct,
		"messages_sent", msgsSent,
		"bytes_sent", bytesSent,
	)
}

// --- binding assignment helpers ---------------------------------------------

// planTopicSet returns the set of topic names that actually survive into the
// plan's chunk refs (the only topics that should appear in the wire bindings).
func planTopicSet(p session.Plan) map[string]struct{} {
	set := map[string]struct{}{}
	for _, ref := range p.Chunks {
		for topic := range ref.ChannelTopics {
			set[topic] = struct{}{}
		}
	}
	// Latched/transient-local seed topics are ABSENT from the window (that is
	// why they are seeded), so they never appear in p.Chunks. The producer still
	// emits each seed topic's pre-window message, but the per-message emit drops
	// any topic missing from the session binding (p.TopicID). Bind them too, so
	// the topic_id_map carries them and the client registers their channel.
	for topic := range p.SeedTopics {
		set[topic] = struct{}{}
	}
	return set
}

// assignBindings deterministically assigns small uint32 topic ids (and dedup'd
// schema ids by schema name+encoding) over the surviving topics, in sorted topic
// order. Returns the wire bindings plus the name->id maps the producer needs.
func assignBindings(
	topicSchemas map[string]format.TopicSchemaInfo,
	surviving map[string]struct{},
) (topicBindings []*pb.TopicBinding, schemaBindings []*pb.SchemaBinding,
	topicIDByName, schemaIDByName map[string]uint32) {

	topicIDByName = map[string]uint32{}
	schemaIDByName = map[string]uint32{}
	schemaIDByKey := map[string]uint32{}

	names := make([]string, 0, len(surviving))
	for name := range surviving {
		if _, ok := topicSchemas[name]; ok {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	var nextTopicID, nextSchemaID uint32 = 1, 1
	for _, name := range names {
		ts := topicSchemas[name]
		schemaKey := ts.SchemaName + "::" + ts.SchemaEncoding
		schemaID, ok := schemaIDByKey[schemaKey]
		if !ok {
			schemaID = nextSchemaID
			nextSchemaID++
			schemaIDByKey[schemaKey] = schemaID
			schemaBindings = append(schemaBindings, &pb.SchemaBinding{
				SchemaId: schemaID, Name: ts.SchemaName, Encoding: ts.SchemaEncoding, Data: ts.SchemaData,
			})
		}
		tid := nextTopicID
		nextTopicID++
		topicIDByName[name] = tid
		schemaIDByName[name] = schemaID
		topicBindings = append(topicBindings, &pb.TopicBinding{
			TopicId: tid, TopicName: name, SchemaId: schemaID, MessageEncoding: ts.MessageEncoding,
		})
	}
	return topicBindings, schemaBindings, topicIDByName, schemaIDByName
}

// toSessionTopicBindings / toSessionSchemaBindings convert the wire bindings into
// the session.SessionState mirror types so resume can echo them verbatim without
// session importing the wire package.
func toSessionTopicBindings(in []*pb.TopicBinding) []session.TopicBinding {
	out := make([]session.TopicBinding, 0, len(in))
	for _, b := range in {
		out = append(out, session.TopicBinding{
			TopicID: b.GetTopicId(), TopicName: b.GetTopicName(),
			SchemaID: b.GetSchemaId(), MessageEncoding: b.GetMessageEncoding(),
		})
	}
	return out
}

func toSessionSchemaBindings(in []*pb.SchemaBinding) []session.SchemaBinding {
	out := make([]session.SchemaBinding, 0, len(in))
	for _, b := range in {
		out = append(out, session.SchemaBinding{
			SchemaID: b.GetSchemaId(), Name: b.GetName(), Encoding: b.GetEncoding(), Data: b.GetData(),
		})
	}
	return out
}

func fromSessionTopicBindings(in []session.TopicBinding) []*pb.TopicBinding {
	out := make([]*pb.TopicBinding, 0, len(in))
	for _, b := range in {
		out = append(out, &pb.TopicBinding{
			TopicId: b.TopicID, TopicName: b.TopicName, SchemaId: b.SchemaID, MessageEncoding: b.MessageEncoding,
		})
	}
	return out
}

func fromSessionSchemaBindings(in []session.SchemaBinding) []*pb.SchemaBinding {
	out := make([]*pb.SchemaBinding, 0, len(in))
	for _, b := range in {
		out = append(out, &pb.SchemaBinding{
			SchemaId: b.SchemaID, Name: b.Name, Encoding: b.Encoding, Data: b.Data,
		})
	}
	return out
}

// --- Cancel / Ack ------------------------------------------------------------

// handleCancel cancels a session: ctx-cancel producer+consumer, discard retain,
// deregister. Eos{CANCELLED} is sent here on the priority path before teardown
// (the Registry.Cancel cancels the consumer, so it would not get a chance to).
func (c *connState) handleCancel(req *pb.CancelSession) {
	d := c.h.sess
	state, ok := d.Registry.Lookup(req.GetSubscriptionId())
	if !ok {
		return // already gone; cancel is idempotent/best-effort
	}
	c.conn.SendPriority(&pb.ServerMessage{
		SubscriptionId: state.ID,
		Payload: &pb.ServerMessage_Eos{Eos: &pb.Eos{
			Reason: pb.EosReason_EOS_REASON_CANCELLED,
		}},
	})
	c.untrackSubscription(state.ID)
	d.Registry.Cancel(state.ID)
}

// handleAck routes a SessionAck.through_seq to the session's consumer (which
// prunes the retain buffer). A dropped ack is fine — the next one supersedes it.
func (c *connState) handleAck(req *pb.SessionAck) {
	d := c.h.sess
	state, ok := d.Registry.Lookup(req.GetSubscriptionId())
	if !ok {
		return
	}
	select {
	case state.AckCh <- req.GetThroughSeq():
	default:
	}
}

// droppedCounter is a tiny concurrency-safe counter wired into Producer.OnDrop.
// (The Progress dropped count is surfaced by the engine's consumer via its own
// Dropped field; this counter exists for logging/observability symmetry.)
type droppedCounter struct {
	n int64
}

func (d *droppedCounter) inc() { d.n++ }

func truncate(s string, max int) string {
	if len(s) > max {
		return s[:max]
	}
	return s
}
