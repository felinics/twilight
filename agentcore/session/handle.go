package session

import (
	"context"
	"github.com/felinics/twilight/agentcore/ledger"
	"sync"
)

// ledgerHandle is the ownership handle over one loaded Session. It holds no
// copy of the tip's index: membership and stream heads are read from the
// backend's indexes on demand (SES-REP-3), so its memory and its Open cost
// do not grow with the segment. Every such read is bounded by the handle's
// head, which only its own Appends advance, so what it knows is exactly what
// it read at Open or wrote under its lease: a superseded handle never learns
// of a successor's commits and reaches the Epoch fence at Append.
//
// The handle keeps the lease and the tip head. The root and the loaded path
// stay on the Session it was opened from; writes go to that Session's
// backend, not back through Ledger.
type ledgerHandle struct {
	mu      sync.Mutex
	session *Session
	lease   Lease
	opts    OpenOptions
	head    ledger.Head
	// streams caches the tip's head of each stream the handle was asked
	// about, read from the backend once below the handle's head and
	// advanced by this handle's own Appends (SES-REP-3).
	streams map[ledger.Domain]ledger.StreamSeq
	// failed is set once an Append's durable outcome is unknown (SES-APP-1):
	// the handle then answers nothing about the ledger, because what reached
	// storage is exactly what it cannot know. The caller reopens.
	failed error
}

func (w *ledgerHandle) SessionID() SessionID  { return w.session.ID() }
func (w *ledgerHandle) Epoch() ledger.Epoch   { return w.lease.Epoch }
func (w *ledgerHandle) Header() SegmentHeader { return w.session.Header() }

func (w *ledgerHandle) Lease() Lease {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lease
}

// Renew is SES-OWN-1: the adapter moves the expiry only for the current
// lease, so a superseded handle learns of the supersession here as well as
// at Append.
func (w *ledgerHandle) Renew(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return w.failed
	}
	until, err := w.session.st.Renew(ctx, w.lease, w.opts.LeaseDuration)
	if err != nil {
		return err
	}
	w.lease.UntilUnixMilli = until
	return nil
}

func (w *ledgerHandle) Head() ledger.Head {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.head
}

// Committed answers from the segments' indexes (SES-REP-3), bounded by what
// this handle knows: the tip's commits below its head (read at Open or
// written by it) and the immutable inherited prefix. A successor's commits
// lie at or past the head, so a superseded handle does not learn of them
// here and reaches the Epoch fence at Append, exactly as when the index
// lived in its memory.
func (w *ledgerHandle) Committed(id ledger.CommitID) (bool, error) {
	w.mu.Lock()
	failed, head := w.failed, w.head
	w.mu.Unlock()
	if failed != nil {
		return false, failed
	}
	ctx := context.Background()
	seq, ok, err := w.session.st.Locate(ctx, w.session.root.Tip, id)
	if err != nil {
		return false, err
	}
	if ok && seq < head.Next {
		return true, nil
	}
	return w.session.path.ContainsInherited(ctx, w.session.st, id)
}

// countStreams advances the cached head of each stream the commit wrote and
// the handle has been asked about; w.mu is held.
func (w *ledgerHandle) countStreams(c *ledger.Commit) {
	for i := range c.Batches {
		stream := c.Batches[i].Domain
		if _, known := w.streams[stream]; known {
			w.streams[stream] += ledger.StreamSeq(len(c.Batches[i].Events))
		}
	}
}

// StreamHead reads the stream's head below the handle's head from the tip's
// index the first time it is asked about a stream, and advances the cached
// value with each Append (SES-REP-3, SES-FRK-5); the bound keeps a
// superseded handle from seeing its successor's streams.
func (w *ledgerHandle) StreamHead(stream ledger.Domain) (ledger.StreamSeq, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return 0, false
	}
	n, known := w.streams[stream]
	if !known {
		var err error
		n, err = w.session.st.StreamHead(context.Background(), w.session.root.Tip, stream, w.head.Next)
		if err != nil {
			return 0, false
		}
		w.streams[stream] = n
	}
	return n, n > 0
}

// LookupCommit is SES-REP-4, under the same bound as Committed: a tip commit
// below the head, else an inherited one.
func (w *ledgerHandle) LookupCommit(id ledger.CommitID) (ledger.Commit, bool, error) {
	w.mu.Lock()
	failed, head := w.failed, w.head
	w.mu.Unlock()
	if failed != nil {
		return ledger.Commit{}, false, failed
	}
	ctx := context.Background()
	if c, ok, err := w.session.st.LookupCommit(ctx, w.session.root.Tip, id); err != nil {
		return ledger.Commit{}, false, err
	} else if ok && c.Seq < head.Next {
		return c, true, nil
	}
	return w.session.path.LookupInherited(ctx, w.session.st, id)
}

func (w *ledgerHandle) Append(ctx context.Context, p ledger.Proposal) (ledger.Commit, error) {
	if err := ctx.Err(); err != nil {
		return ledger.Commit{}, err
	}
	sid := w.session.ID()
	staged := ledger.Proposal{CommitID: p.CommitID, Batches: cloneBatches(p.Batches)}
	if err := staged.Validate(); err != nil {
		return ledger.Commit{}, newError(ErrInvalid, "append", sid, err.Error())
	}
	if ok, err := w.Committed(p.CommitID); err != nil {
		return ledger.Commit{}, err
	} else if ok {
		return ledger.Commit{}, &Error{Code: ErrConflict, Operation: "append", SessionID: sid, CommitID: p.CommitID, Detail: "CommitID already in the ledger"}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return ledger.Commit{}, w.failed
	}
	c := staged.At(w.head.Next)
	if err := w.session.st.Append(ctx, w.lease, w.session.root.Tip, c); err != nil {
		if IsHandleFailed(err) {
			w.failed = err
		}
		return ledger.Commit{}, err
	}
	w.head = ledger.Head{Next: c.Seq + 1}
	w.countStreams(&c)
	return cloneCommit(c), nil
}

// Close releases the lease.
func (w *ledgerHandle) Close(ctx context.Context) error {
	return w.session.st.Release(ctx, w.lease)
}
