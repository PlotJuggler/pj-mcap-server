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
	//
	// ProducerCancel MUST be set BEFORE Register publishes the state: Cancel
	// reads it with no per-field synchronization, so a post-Register write is a
	// data race AND a missed-cancellation window (a fast client Cancel would
	// observe nil and leave the producer running unbounded).
	Producer       *Producer
	ProducerCancel context.CancelFunc
	ProducerDone   chan struct{}

	// DroppedMessages counts messages the producer dropped for exceeding
	// MaxMessageBytes. It lives on the SESSION (not the attachment) so the count
	// survives detach/resume; the consumer surfaces it live in Progress.
	DroppedMessages atomic.Uint64

	// Wire bindings (echoed verbatim on resume, spec 6.6): the same
	// subscription_id + topic_id_map + schemas the fresh-open returned. The WS
	// layer (next phase) populates these so OpenResume can re-send the identical
	// topic_id_map / schemas without re-planning. session does not import the
	// wire package, so these mirror the wire shapes; the WS layer converts.
	TopicBindings  []TopicBinding
	SchemaBindings []SchemaBinding

	mu             sync.Mutex
	consumerCancel context.CancelFunc
	// consumerGen is the ownership token for the CURRENT consumer binding: it
	// increments on every BindConsumer, and Detach is a no-op unless the caller
	// presents the matching generation. This makes detach→attach an atomic
	// ownership transition — a stale connection's late teardown can neither
	// cancel a successor consumer nor arm eviction on an actively-attached
	// session.
	consumerGen uint64

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

// HighestAppendedSeq returns the largest seq the producer has appended (0 before
// any append) — the watermark a resume cursor is validated against: a client
// cannot have durably received a batch that was never produced.
func (s *SessionState) HighestAppendedSeq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := len(s.ledger); n > 0 {
		return s.ledger[n-1].Seq
	}
	return 0
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
	// evictEpoch guards against an eviction timer that has ALREADY FIRED and is
	// blocked on r.mu when a Reattach (or a re-arm) supersedes it: every arming
	// captures the current epoch, and the fired callback only proceeds when the
	// epoch still matches. Reattach/arm bump it, so a stale fired timer becomes
	// a no-op instead of cancelling a just-reattached session.
	evictEpoch map[uint64]uint64
	nextID     atomic.Uint64
	opts       RegistryOpts

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
		sessions:   make(map[uint64]*SessionState),
		evictAt:    make(map[uint64]*time.Timer),
		evictEpoch: make(map[uint64]uint64),
		opts:       opts,
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

// BindConsumer is the (gen)-only convenience form; gen 0 means the session was
// gone. Prefer BindConsumerChecked when the caller must distinguish "gone".
func (r *Registry) BindConsumer(id uint64, cancel context.CancelFunc) uint64 {
	gen, _ := r.BindConsumerChecked(id, cancel)
	return gen
}

// BindConsumerChecked records the active consumer's cancel func (called right
// after spawning Consumer.Run) and CANCELS any prior binding — a resume takeover
// must never leave two consumers draining the same retain buffer. The
// membership check and the install are ONE critical section under r.mu, so a
// Cancel/eviction cannot remove the session between them (which would strand an
// orphan consumer that later emits a spurious Eos{COMPLETE}). Returns
// (generation, ok): ok=false means the session is gone and NOTHING was bound —
// the caller must not track or spawn a consumer.
func (r *Registry) BindConsumerChecked(id uint64, cancel context.CancelFunc) (uint64, bool) {
	r.mu.Lock()
	s, ok := r.sessions[id]
	if !ok {
		r.mu.Unlock()
		return 0, false
	}
	s.mu.Lock()
	prev := s.consumerCancel
	s.consumerCancel = cancel
	s.consumerGen++
	gen := s.consumerGen
	s.mu.Unlock()
	r.mu.Unlock()
	if prev != nil {
		prev() // the replaced consumer exits via its ctx; the session stays live
	}
	return gen, true
}

// Detach marks the session as having no active WS consumer: it cancels the
// consumer goroutine (the producer keeps running) and arms the
// retain-after-disconnect eviction timer (spec 6.5). The producer fills the
// retain buffer until the caps block it.
//
// gen is the ownership token BindConsumer returned to this caller. A stale gen
// (the binding has since been taken over by a newer consumer, e.g. a resume on
// another connection) makes Detach a NO-OP: the late teardown of a zombie
// connection must not cancel the successor nor arm eviction on a session that
// has a live attachment. gen 0 matches a never-bound session.
func (r *Registry) Detach(id uint64, gen uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[id]
	if !ok {
		return
	}
	s.mu.Lock()
	if s.consumerGen != gen {
		s.mu.Unlock()
		return // stale owner: a newer consumer holds the binding
	}
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
	// Bump the epoch and capture it: a timer callback that has already fired and
	// is blocked on r.mu when a later Reattach/arm runs will see a stale epoch
	// and no-op (evictFire), instead of cancelling a reattached session.
	r.evictEpoch[id]++
	epoch := r.evictEpoch[id]
	d := r.opts.RetainAfterDisconnect
	if d <= 0 {
		d = time.Nanosecond // no retain window: evict on the next scheduler tick
	}
	r.evictAt[id] = time.AfterFunc(d, func() { r.evictFire(id, epoch) })
}

// evictFire is the retain-window eviction callback. It cancels the session ONLY
// if the arming that scheduled it is still current (epoch match) — a Reattach or
// a re-arm between the timer firing and this acquiring r.mu bumps the epoch,
// making a stale fired timer a no-op.
func (r *Registry) evictFire(id, epoch uint64) {
	r.mu.Lock()
	if r.evictEpoch[id] != epoch {
		r.mu.Unlock()
		return // superseded by a Reattach / re-arm
	}
	s, ok := r.sessions[id]
	if !ok {
		r.mu.Unlock()
		return
	}
	delete(r.sessions, id)
	delete(r.evictAt, id)
	delete(r.evictEpoch, id)
	r.mu.Unlock()
	r.teardown(s)
}

// Reattach cancels any pending eviction so the producer's buffered batches can
// be replayed to a new consumer. Bumps the eviction epoch so an
// already-fired-but-pending eviction timer becomes a no-op (see evictFire).
// Returns ErrSessionMissing if already evicted.
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
	r.evictEpoch[id]++ // invalidate any fired-but-pending eviction timer
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
	delete(r.evictEpoch, id)
	delete(r.sessions, id)
	r.mu.Unlock()
	r.teardown(s)
}

// teardown cancels a removed session's consumer + producer, discards its retain
// buffer, and fires onEvict. Called (exactly once per session) by Cancel and
// evictFire AFTER the session has been removed from the registry under r.mu.
func (r *Registry) teardown(s *SessionState) {
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
