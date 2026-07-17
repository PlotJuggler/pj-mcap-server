package catalog

import (
	"context"
)

// ReopenIfSwapped implements the READER side of "atomic catalog publish +
// reopen-on-swap" (catalog-migration plan §6.2a; CATALOG_CONTRACT.md §9
// "Publish & reopen protocol"). The Python builder's writer side is already
// landed: a rebuild (as opposed to an in-place WAL reconcile) never mutates the
// served DB in place — it builds a fresh file and os.replaces it onto the served
// path, which is a NEW inode at the same name. This method is how the Go reader
// notices that and swaps its handle over, so a rebuild no longer requires a
// server restart.
//
// Contract (do not re-litigate — see CATALOG_CONTRACT.md §9 point 3):
//   - The swap TRIGGER is file identity — (dev, inode) of the served path — never
//     build_metadata.build_id. build_id is a monotonic freshness/confirmation
//     counter an operator can read to confirm forward progress; it is not
//     guaranteed to be comparable across a rebuild's seeding and must never be
//     used to decide whether to reopen.
//   - An in-place reconcile (the common case) never changes the served path's
//     inode, so ReopenIfSwapped is a cheap no-op stat on every call in the
//     overwhelmingly common case.
//
// Behavior:
//   - Same (dev, inode) as last observed: (false, nil).
//   - Different identity but the new file fails verification (wrong
//     schema_version, unreadable, or the stat/open landed mid-swap and hit the
//     bounded identity-race retry in openVerified): (false, err), and the OLD
//     handle keeps serving — fail-closed. This is deliberate: a rebuild-in-
//     progress or a corrupt replacement must never interrupt service; the
//     next tick (the caller polls on an interval) tries again.
//   - Different identity and verification succeeds: a NEW snapshot
//     {db, identity, generation} is published atomically (the generation
//     ordinal advances — this is the ONLY event that changes the generation
//     token), the OLD snapshot is retired, and (true, nil) is returned. The
//     old handle closes only once every in-flight lease on it releases
//     (drain-then-close — see Store.Acquire).
func (s *Store) ReopenIfSwapped(ctx context.Context) (swapped bool, err error) {
	// Serialize swaps: at most one ReopenIfSwapped does the stat-compare-open-swap
	// sequence at a time. The caller today is a single ticker goroutine, so this
	// is a documented invariant rather than a live contention point.
	s.reopenMu.Lock()
	defer s.reopenMu.Unlock()

	cur := s.identitySnapshot()
	stat, statErr := statIdentity(s.dbPath)
	if statErr != nil {
		return false, statErr
	}
	if cur == stat {
		return false, nil
	}

	// Identity differs: reopen+verify the new generation before touching
	// anything the current readers depend on. openVerified's own bounded
	// identity-race retry (C2) absorbs the case where the stat above landed
	// astride a still-in-flight os.replace.
	newDB, newIdent, openErr := openVerified(ctx, s.dbPath)
	if openErr != nil {
		return false, openErr
	}

	// Publish {db, identity, generation} as ONE snapshot. The ordinal advances
	// exactly here — a failed verification above returns without touching it,
	// so the token is stable across failed swaps and no-op checks.
	s.ordinal++
	next := &snapshot{db: newDB, ident: newIdent, gen: s.newGenToken(s.ordinal)}
	next.refs.Store(1) // the "current" ref
	s.publish(next)
	return true, nil
}
