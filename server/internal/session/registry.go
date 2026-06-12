package session

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrAtCapacity is returned by Register when MaxConcurrent is reached
	// (maps to Error{RESOURCE_LIMIT}).
	ErrAtCapacity = errors.New("session: server at max concurrent sessions")
	// ErrSessionMissing is returned by Reattach when the session was evicted or
	// never existed (maps to Error{RESUME_NOT_POSSIBLE}).
	ErrSessionMissing = errors.New("session: not found (may have been evicted)")
)

// SessionState carries everything the producer + consumer need plus the retain
// buffer (which lives across reattachments — the resume mechanism). The
// per-attachment consumer cancel is re-bound on each (re)attachment.
type SessionState struct {
	ID     uint64
	Plan   Plan
	Retain *RetainBuffer
	AckCh  chan uint64

	// Producer lifetime is independent of the WS (spec 8.1): it keeps running
	// after the consumer detaches, until the retain caps block it or ctx is
	// cancelled (Cancel / eviction / shutdown).
	Producer       *Producer
	ProducerCancel context.CancelFunc
	ProducerDone   chan struct{}

	// Wire bindings (echoed verbatim on resume, spec 6.6): the same
	// subscription_id + topic_id_map + schemas the fresh-open returned. The WS
	// layer (next phase) populates these so OpenResume can re-send the identical
	// topic_id_map / schemas without re-planning. session does not import the
	// wire package, so these mirror the wire shapes; the WS layer converts.
	TopicBindings  []TopicBinding
	SchemaBindings []SchemaBinding

	mu             sync.Mutex
	consumerCancel context.CancelFunc

	// ledger is the per-seq cumulative (messages, bytes) record the producer
	// builds as it appends batches (it is the single writer of seq/Messages, in
	// monotonic seq order). It is the source of truth for resume counter carry-
	// forward: on OpenResume the new consumer seeds its counters with the
	// cumulative totals THROUGH resume_after_seq (= messages in seqs <=
	// resume_after_seq, which are NOT replayed), so the terminal
	// Eos.total_*_sent counts every delivered message exactly once across the
	// drop — neither under- (the old zero-restart bug) nor over-counting (the
	// in-flight-beyond-resume batches that get re-delivered, hence excluded here).
	ledger []seqCumulative
}

// seqCumulative is one ledger entry: the cumulative messages/bytes through Seq
// (inclusive), in producer-append (seq-ascending) order.
type seqCumulative struct {
	Seq      uint64
	CumMsgs  uint64
	CumBytes uint64
}

// RecordAppend extends the cumulative ledger with one just-appended batch. Called
// from the producer's append path (single writer, monotonic seq). Concurrency-safe
// vs. the reader (CountersThrough).
func (s *SessionState) RecordAppend(seq, messages, bytes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cumMsgs, cumBytes := uint64(0), uint64(0)
	if n := len(s.ledger); n > 0 {
		cumMsgs = s.ledger[n-1].CumMsgs
		cumBytes = s.ledger[n-1].CumBytes
	}
	s.ledger = append(s.ledger, seqCumulative{
		Seq:      seq,
		CumMsgs:  cumMsgs + messages,
		CumBytes: cumBytes + bytes,
	})
}

// CountersThrough returns the cumulative (messages, bytes) delivered through
// throughSeq (inclusive) — i.e. the totals for all batches with Seq <=
// throughSeq. A reattaching consumer seeds its counters with this so the totals
// stay monotonic AND count each delivered message exactly once across a resume
// (replayed seqs > throughSeq are excluded here and re-counted by the new
// consumer as it re-delivers them). throughSeq==0 (fresh open) returns 0,0.
func (s *SessionState) CountersThrough(throughSeq uint64) (messages, bytes uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// ledger is seq-ascending; walk to the last entry with Seq <= throughSeq.
	for _, e := range s.ledger {
		if e.Seq > throughSeq {
			break
		}
		messages, bytes = e.CumMsgs, e.CumBytes
	}
	return messages, bytes
}

// TopicBinding mirrors wire OpenSessionResponse.topic_id_map (the WS layer
// converts to/from pb.TopicBinding). Kept here so SessionState can retain it for
// resume echo without session importing the wire package.
type TopicBinding struct {
	TopicID         uint32
	TopicName       string
	SchemaID        uint32
	MessageEncoding string
}

// SchemaBinding mirrors wire OpenSessionResponse.schemas.
type SchemaBinding struct {
	SchemaID uint32
	Name     string
	Encoding string
	Data     []byte
}

// RegistryOpts configures concurrency + the retain-after-disconnect window.
type RegistryOpts struct {
	MaxConcurrent         int
	RetainAfterDisconnect time.Duration
}

// Registry tracks active SessionStates + per-session eviction timers. All
// methods are concurrency-safe.
type Registry struct {
	mu       sync.Mutex
	sessions map[uint64]*SessionState
	evictAt  map[uint64]*time.Timer
	nextID   atomic.Uint64
	opts     RegistryOpts

	// onEvict, if set, is called (outside the registry lock) when a session is
	// removed (cancel or eviction). The WS layer uses it to discard the retain
	// buffer + close ProducerDone-dependent goroutines.
	onEvict func(s *SessionState)
}

func NewRegistry(opts RegistryOpts) *Registry {
	if opts.MaxConcurrent <= 0 {
		opts.MaxConcurrent = 64
	}
	return &Registry{
		sessions: make(map[uint64]*SessionState),
		evictAt:  make(map[uint64]*time.Timer),
		opts:     opts,
	}
}

// SetOnEvict registers a teardown callback invoked when a session leaves the
// registry. Set once at construction; not concurrency-safe with itself.
func (r *Registry) SetOnEvict(fn func(s *SessionState)) { r.onEvict = fn }

// Register assigns a subscription_id and stores the session, or returns
// ErrAtCapacity. The id is monotonic across the registry's lifetime.
func (r *Registry) Register(ctx context.Context, s *SessionState) (*SessionState, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.sessions) >= r.opts.MaxConcurrent {
		return nil, ErrAtCapacity
	}
	s.ID = r.nextID.Add(1)
	r.sessions[s.ID] = s
	return s, nil
}

// Lookup returns the live SessionState for id, if any.
func (r *Registry) Lookup(id uint64) (*SessionState, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[id]
	return s, ok
}

// BindConsumer records the active consumer's cancel func (called right after
// spawning Consumer.Run). Replaces any prior binding (resume).
func (r *Registry) BindConsumer(id uint64, cancel context.CancelFunc) {
	r.mu.Lock()
	s, ok := r.sessions[id]
	r.mu.Unlock()
	if !ok {
		return
	}
	s.mu.Lock()
	s.consumerCancel = cancel
	s.mu.Unlock()
}

// Detach marks the session as having no active WS consumer: it cancels the
// consumer goroutine (the producer keeps running) and arms the
// retain-after-disconnect eviction timer (spec 6.5). The producer fills the
// retain buffer until the caps block it.
func (r *Registry) Detach(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[id]
	if !ok {
		return
	}
	s.mu.Lock()
	if s.consumerCancel != nil {
		s.consumerCancel()
		s.consumerCancel = nil
	}
	s.mu.Unlock()
	r.armEvictLocked(id)
}

func (r *Registry) armEvictLocked(id uint64) {
	if t, ok := r.evictAt[id]; ok {
		t.Stop()
	}
	if r.opts.RetainAfterDisconnect <= 0 {
		// No retain window: evict immediately on the next tick of the scheduler.
		r.evictAt[id] = time.AfterFunc(time.Nanosecond, func() { r.Cancel(id) })
		return
	}
	r.evictAt[id] = time.AfterFunc(r.opts.RetainAfterDisconnect, func() { r.Cancel(id) })
}

// Reattach cancels any pending eviction so the producer's buffered batches can
// be replayed to a new consumer. Returns ErrSessionMissing if already evicted.
func (r *Registry) Reattach(id uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.sessions[id]; !ok {
		return ErrSessionMissing
	}
	if t, ok := r.evictAt[id]; ok {
		t.Stop()
		delete(r.evictAt, id)
	}
	return nil
}

// Cancel terminates the session: cancels producer + consumer, discards the
// retain buffer, removes from the registry, and invokes onEvict. Idempotent and
// safe to call from an eviction timer. Used for both client CancelSession and
// retain-window expiry (spec 6.6).
func (r *Registry) Cancel(id uint64) {
	r.mu.Lock()
	s, ok := r.sessions[id]
	if !ok {
		r.mu.Unlock()
		return
	}
	if t, ok := r.evictAt[id]; ok {
		t.Stop()
		delete(r.evictAt, id)
	}
	delete(r.sessions, id)
	r.mu.Unlock()

	s.mu.Lock()
	if s.consumerCancel != nil {
		s.consumerCancel()
		s.consumerCancel = nil
	}
	s.mu.Unlock()
	if s.ProducerCancel != nil {
		s.ProducerCancel()
	}
	if s.Retain != nil {
		s.Retain.Close() // discard retain; unblock a producer parked at the caps
	}
	if r.onEvict != nil {
		r.onEvict(s)
	}
}

// ActiveCount returns the current registered-session count (dashboard).
func (r *Registry) ActiveCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.sessions)
}

// CancelAll cancels every registered session (graceful shutdown, spec §8.4):
// each producer + consumer ctx is cancelled, retain buffers discarded, and the
// session removed. Idempotent. Snapshots ids under the lock then Cancels each
// outside it (Cancel takes the lock itself).
func (r *Registry) CancelAll() {
	r.mu.Lock()
	ids := make([]uint64, 0, len(r.sessions))
	for id := range r.sessions {
		ids = append(ids, id)
	}
	r.mu.Unlock()
	for _, id := range ids {
		r.Cancel(id)
	}
}

// SessionInfo is a read-only snapshot of one live session for the dashboard.
type SessionInfo struct {
	ID           uint64
	ChunkCount   int
	FileCount    int
	ProducerDone bool
}

// Snapshot returns a read-only view of every registered session, ordered by id.
// Concurrency-safe; used by the dashboard /dashboard/sessions page (spec §8.5).
func (r *Registry) Snapshot() []SessionInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]SessionInfo, 0, len(r.sessions))
	for _, s := range r.sessions {
		files := map[uint64]struct{}{}
		for _, ch := range s.Plan.Chunks {
			files[ch.FileID] = struct{}{}
		}
		done := false
		if s.ProducerDone != nil {
			select {
			case <-s.ProducerDone:
				done = true
			default:
			}
		}
		out = append(out, SessionInfo{
			ID:           s.ID,
			ChunkCount:   len(s.Plan.Chunks),
			FileCount:    len(files),
			ProducerDone: done,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
