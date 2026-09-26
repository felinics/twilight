package session

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// Ledger is the kernel's Store over a Backend (SES 4 to 6, 8, 9): the
// Session lineage tree in code. Roots (SessionRecord) name the segment they
// append to as their tip; segments (Segment) point to their parents through
// LedgerRef edges; a Session's history is the stitched Ancestry of its segment. Fork
// adds a node and an edge; Delete drops a root; Collect reclaims what no
// root reaches. Every adapter gets these semantics from here and implements
// none of them.
type Ledger struct {
	be        Backend
	segmentID func() (SegmentID, error)
	// graph serializes the operations that change the set of roots and
	// nodes (Create, Delete, Collect) against each other (SES-GC-4): a
	// Create's check that its parent is live, and its write, cannot
	// interleave with a Collect that would reclaim that parent or the new
	// node. Append and reads never take it; a live root's segments are never
	// touched by Collect. The lock is per process: a Backend shared by
	// several processes must provide this exclusion itself.
	graph sync.Mutex
}

// LedgerOption configures a Ledger.
type LedgerOption func(*Ledger)

// WithSegmentIDSource replaces the segment identity generator. Production
// uses NewSegmentID; wire fixtures inject a deterministic source so the
// bytes they freeze are reproducible.
func WithSegmentIDSource(src func() (SegmentID, error)) LedgerOption {
	return func(l *Ledger) { l.segmentID = src }
}

// NewLedger returns the Store over be.
func NewLedger(be Backend, opts ...LedgerOption) *Ledger {
	l := &Ledger{be: be, segmentID: NewSegmentID}
	for _, o := range opts {
		o(l)
	}
	return l
}

// resolve loads a live Session's root and the Ancestry of its segment.
func (l *Ledger) resolve(ctx context.Context, sid SessionID) (SessionRecord, *Ancestry, error) {
	root, err := l.be.Record(ctx, sid)
	if err != nil {
		return SessionRecord{}, nil, err
	}
	a, err := LoadAncestry(ctx, l.be, root.Tip)
	if err != nil {
		return SessionRecord{}, nil, err
	}
	return root, a, nil
}

// --- create -----------------------------------------------------------------------

func (l *Ledger) Create(ctx context.Context, req CreateRequest) (SegmentHeader, error) {
	if err := ctx.Err(); err != nil {
		return SegmentHeader{}, err
	}
	l.graph.Lock()
	defer l.graph.Unlock()
	if err := validIdentity("SessionID", string(req.SessionID)); err != nil {
		return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, err.Error())
	}
	header := SegmentHeader{CausationID: req.CausationID, Ext: req.Ext.Clone()}
	if req.Fork != nil {
		// The edge names the segment that contributes the inherited commit,
		// wherever in the parent's ancestry it lives (SES-FRK-1).
		if req.Fork.Session == req.SessionID {
			return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, "a session cannot fork itself")
		}
		_, parent, err := l.resolve(ctx, req.Fork.Session)
		if err != nil {
			if IsCode(err, ErrNotFound) {
				return SegmentHeader{}, newError(ErrNotFound, "create", req.SessionID, fmt.Sprintf("parent session %s not found", req.Fork.Session))
			}
			return SegmentHeader{}, err
		}
		// The edge names a position in the parent's history (SES-FRK-1).
		owner, ok := parent.Owner(req.Fork.Seq)
		if !ok {
			return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, fmt.Sprintf("parent %s has no commit %d", req.Fork.Session, req.Fork.Seq))
		}
		commits, _, _, err := l.be.ReadSegment(ctx, owner.Segment.ID, req.Fork.Seq, 1)
		if err != nil {
			return SegmentHeader{}, err
		}
		if len(commits) != 1 || commits[0].Seq != req.Fork.Seq {
			return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, fmt.Sprintf("parent %s has no commit %d", req.Fork.Session, req.Fork.Seq))
		}
		header.Parent = &LedgerRef{Segment: owner.Segment.ID, Seq: req.Fork.Seq}
	}
	// Idempotency is judged on what the request determines about the
	// segment, not on its identity or clock: the ID is drawn fresh each time
	// and the creation time is the first writer's (SES-CRT-1).
	if existing, err := l.be.Record(ctx, req.SessionID); err == nil {
		seg, err := l.be.Segment(ctx, existing.Tip)
		if err != nil {
			return SegmentHeader{}, err
		}
		if sameCreation(header, seg.Header) {
			return seg.Header, nil
		}
		return SegmentHeader{}, newError(ErrConflict, "create", req.SessionID, "session exists with a different creation record")
	} else if !IsCode(err, ErrNotFound) {
		return SegmentHeader{}, err
	}
	id, err := l.segmentID()
	if err != nil {
		return SegmentHeader{}, err
	}
	header.ID = id
	if err := ValidateHeader(header); err != nil {
		return SegmentHeader{}, err
	}
	segment := Segment{ID: header.ID, Header: header}
	root := SessionRecord{ID: req.SessionID, Tip: segment.ID, CreatedAtUnixMilli: req.CreatedAtUnixMilli}
	if err := l.be.CreateSession(ctx, segment, root); err != nil {
		return SegmentHeader{}, err
	}
	return header, nil
}

// sameCreation reports whether req would create the Session that exists:
// same resolved edge, same causation, same extensions. The creation time
// is the first writer's and does not decide it, so a retried Create with a
// fresh clock is a replay (SES-CRT-1).
func sameCreation(want, have SegmentHeader) bool {
	if (have.Parent == nil) != (want.Parent == nil) || (have.Parent != nil && *have.Parent != *want.Parent) {
		return false
	}
	return have.CausationID == want.CausationID && have.Ext.Equal(want.Ext)
}

func (l *Ledger) Header(ctx context.Context, sid SessionID) (SegmentHeader, error) {
	if err := ctx.Err(); err != nil {
		return SegmentHeader{}, err
	}
	root, err := l.be.Record(ctx, sid)
	if err != nil {
		return SegmentHeader{}, err
	}
	seg, err := l.be.Segment(ctx, root.Tip)
	if err != nil {
		return SegmentHeader{}, err
	}
	return seg.Header, nil
}

func (l *Ledger) Record(ctx context.Context, sid SessionID) (SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return SessionRecord{}, err
	}
	return l.be.Record(ctx, sid)
}

// LeaseOf is Store.LeaseOf (SES-OWN-5): the adapter's reading of the root's
// current holder.
func (l *Ledger) LeaseOf(ctx context.Context, sid SessionID) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, false, err
	}
	return l.be.LeaseOf(ctx, sid)
}

// ListLeases is Store.ListLeases (SES-OWN-5).
func (l *Ledger) ListLeases(ctx context.Context) ([]Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return l.be.ListLeases(ctx)
}

// ExpiredLeases is Store.ExpiredLeases (SES-OWN-5).
func (l *Ledger) ExpiredLeases(ctx context.Context, beforeUnixMilli int64, limit int) ([]Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return l.be.ExpiredLeases(ctx, beforeUnixMilli, limit)
}

// ExpiredLeasesOf selects from leases what ExpiredLeases returns: the
// shared filter of adapters without an expiry index.
func ExpiredLeasesOf(leases []Lease, beforeUnixMilli int64, limit int) []Lease {
	var out []Lease
	for _, l := range leases {
		if l.UntilUnixMilli != 0 && l.UntilUnixMilli <= beforeUnixMilli {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UntilUnixMilli != out[j].UntilUnixMilli {
			return out[i].UntilUnixMilli < out[j].UntilUnixMilli
		}
		return out[i].Session < out[j].Session
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// --- open -------------------------------------------------------------------------

func (l *Ledger) Open(ctx context.Context, sid SessionID, opts OpenOptions) (Handle, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, a, err := l.resolve(ctx, sid)
	if err != nil {
		return nil, err
	}
	tip := a.Tip()
	lease, err := l.be.Acquire(ctx, sid, opts)
	if err != nil {
		return nil, err
	}
	// Acquire repaired a torn tail; the segment's CommitIndex (SES-REP-5) is
	// checked against the head and rebuilt when it lags, but the handle
	// holds none of it: membership and stream heads are answered by the
	// backend's index on demand (SES-REP-3), so Open costs the same for a
	// tip of ten commits and one of a million.
	head, err := l.checkIndex(ctx, tip)
	if err != nil {
		_ = l.be.Release(ctx, lease)
		return nil, err
	}
	return &ledgerHandle{l: l, root: root, ancestry: a, lease: lease, opts: opts, head: head, streams: make(map[StreamRef]StreamSeq)}, nil
}

// checkIndex returns the segment's head after checking its CommitIndex by
// summary. An index that fails Valid against the head (absent, lagging
// after a crash, or cut) is rebuilt from the segment's own commits and
// written back (SES-REP-5); nothing is read otherwise.
func (l *Ledger) checkIndex(ctx context.Context, seg Segment) (Head, error) {
	summary, head, err := l.be.Summarize(ctx, seg.ID)
	if err != nil {
		return Head{}, err
	}
	if summary.Valid(seg.Seed(), head) {
		return head, nil
	}
	commits, head, _, err := l.be.ReadSegment(ctx, seg.ID, seg.Seed().Next, 0)
	if err != nil {
		return Head{}, err
	}
	if err := l.be.PutIndex(ctx, seg.ID, BuildCommitIndex(seg.Header, commits)); err != nil {
		return Head{}, err
	}
	return head, nil
}

// ledgerHandle is the ownership handle over one root. It holds no copy of
// the tip's index: membership and stream heads are read from the backend's
// indexes on demand (SES-REP-3), so its memory and its Open cost do not
// grow with the segment. Every such read is bounded by the handle's head,
// which only its own Appends advance, so what it knows is exactly what it
// read at Open or wrote under its lease: a superseded handle never learns
// of a successor's commits and reaches the Epoch fence at Append.
type ledgerHandle struct {
	mu       sync.Mutex
	l        *Ledger
	root     SessionRecord
	ancestry *Ancestry
	lease    Lease
	opts     OpenOptions
	head     Head
	// streams caches the tip's head of each stream the handle was asked
	// about, read from the backend once below the handle's head and
	// advanced by this handle's own Appends (SES-REP-3).
	streams map[StreamRef]StreamSeq
	// failed is set once an Append's durable outcome is unknown (SES-APP-1):
	// the handle then answers nothing about the ledger, because what reached
	// storage is exactly what it cannot know. The caller reopens.
	failed error
}

func (w *ledgerHandle) SessionID() SessionID { return w.root.ID }
func (w *ledgerHandle) Epoch() Epoch         { return w.lease.Epoch }

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
	until := w.opts.LeaseUntil(w.opts.Now())
	if err := w.l.be.Renew(ctx, w.lease, until); err != nil {
		return err
	}
	w.lease.UntilUnixMilli = until
	return nil
}

func (w *ledgerHandle) Head() Head {
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
func (w *ledgerHandle) Committed(id CommitID) bool {
	w.mu.Lock()
	failed, head := w.failed, w.head
	w.mu.Unlock()
	if failed != nil {
		return false
	}
	ctx := context.Background()
	if seq, ok, err := w.l.be.Locate(ctx, w.root.Tip, id); err != nil {
		return false
	} else if ok && seq < head.Next {
		return true
	}
	inherited, err := w.ancestry.ContainsInherited(ctx, w.l.be, id)
	return err == nil && inherited
}

// countStreams advances the cached head of each stream the commit wrote and
// the handle has been asked about; w.mu is held.
func (w *ledgerHandle) countStreams(c *Commit) {
	for i := range c.Batches {
		stream := c.Batches[i].Stream
		if _, known := w.streams[stream]; known {
			w.streams[stream] += StreamSeq(len(c.Batches[i].Events))
		}
	}
}

// StreamHead reads the stream's head below the handle's head from the tip's
// index the first time it is asked about a stream, and advances the cached
// value with each Append (SES-REP-3, SES-FRK-5); the bound keeps a
// superseded handle from seeing its successor's streams.
func (w *ledgerHandle) StreamHead(stream StreamRef) (StreamSeq, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return 0, false
	}
	n, known := w.streams[stream]
	if !known {
		var err error
		n, err = w.l.be.StreamHead(context.Background(), w.root.Tip, stream, w.head.Next)
		if err != nil {
			return 0, false
		}
		w.streams[stream] = n
	}
	return n, n > 0
}

// LookupCommit is SES-REP-4, under the same bound as Committed: a tip commit
// below the head, else an inherited one.
func (w *ledgerHandle) LookupCommit(id CommitID) (Commit, bool, error) {
	w.mu.Lock()
	failed, head := w.failed, w.head
	w.mu.Unlock()
	if failed != nil {
		return Commit{}, false, failed
	}
	ctx := context.Background()
	if c, ok, err := w.l.be.LookupCommit(ctx, w.root.Tip, id); err != nil {
		return Commit{}, false, err
	} else if ok && c.Seq < head.Next {
		return c, true, nil
	}
	return w.ancestry.LookupInherited(ctx, w.l.be, id)
}

func (w *ledgerHandle) Append(ctx context.Context, p Proposal) (Commit, error) {
	if err := ctx.Err(); err != nil {
		return Commit{}, err
	}
	sid := w.root.ID
	c := Commit{CommitID: p.CommitID, Batches: cloneBatches(p.Batches)}
	if err := ValidateCommit(&c); err != nil {
		return Commit{}, newError(ErrInvalid, "append", sid, err.Error())
	}
	if w.Committed(p.CommitID) {
		return Commit{}, &Error{Code: ErrConflict, Operation: "append", SessionID: sid, CommitID: p.CommitID, Detail: "CommitID already in the ledger"}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failed != nil {
		return Commit{}, w.failed
	}
	c.Seq = w.head.Next
	if err := w.l.be.Append(ctx, w.lease, w.root.Tip, c); err != nil {
		if IsCode(err, ErrHandleFailed) {
			w.failed = err
		}
		return Commit{}, err
	}
	w.head = Head{Next: c.Seq + 1}
	w.countStreams(&c)
	return cloneCommit(c), nil
}

// Close releases the lease.
func (w *ledgerHandle) Close(ctx context.Context) error {
	return w.l.be.Release(ctx, w.lease)
}

// --- read -------------------------------------------------------------------------

func (l *Ledger) ReadCommits(ctx context.Context, req CommitReadRequest) (CommitPage, error) {
	if err := ctx.Err(); err != nil {
		return CommitPage{}, err
	}
	_, a, err := l.resolve(ctx, req.SessionID)
	if err != nil {
		return CommitPage{}, err
	}
	commits, head, more, err := a.Read(ctx, l.be, req.From, req.Limit)
	if err != nil {
		return CommitPage{}, err
	}
	return CommitPage{Header: a.Header(), Commits: commits, Head: head, HasMore: more}, nil
}

func (l *Ledger) ReadStream(ctx context.Context, req StreamReadRequest) (StreamPage, error) {
	if err := ctx.Err(); err != nil {
		return StreamPage{}, err
	}
	if err := ValidateStreamRef(req.Stream); err != nil {
		return StreamPage{}, newError(ErrInvalid, "read_stream", req.SessionID, err.Error())
	}
	if err := ValidateStreamLineage(req.Lineage); err != nil {
		return StreamPage{}, newError(ErrInvalid, "read_stream", req.SessionID, err.Error())
	}
	// Stream positions count the stream's events from the first commit the
	// read sees (SES-REP-2): the stitched history under LineageSession, the
	// tip segment's own commits under LineageSegment (SES-FRK-5). The kernel
	// applies the mode the read names; the stream's owning module declared
	// which one its domain is.
	all, err := l.ReadCommits(ctx, CommitReadRequest{SessionID: req.SessionID})
	if err != nil {
		return StreamPage{}, err
	}
	commits := all.Commits
	if req.Lineage == LineageSegment && all.Header.Parent != nil {
		own := commits[:0:0]
		for _, c := range commits {
			if c.Seq > all.Header.Parent.Seq {
				own = append(own, c)
			}
		}
		commits = own
	}
	page := StreamPage{Header: all.Header, Stream: req.Stream, Head: all.Head}
	page.Events, page.HasMore = StreamEvents(commits, req.Stream, req.From, req.Limit)
	return page, nil
}

// StreamEvents walks commits in order and returns the events of stream from
// position from, at most limit (0 = unlimited); more reports whether events
// beyond the returned ones exist. It is the one StreamSeq derivation
// (SES-REP-2).
func StreamEvents(commits []Commit, stream StreamRef, from StreamSeq, limit uint32) (events []Event, more bool) {
	var pos StreamSeq
	for i := range commits {
		for j := range commits[i].Batches {
			b := &commits[i].Batches[j]
			if b.Stream != stream {
				continue
			}
			for _, e := range b.Events {
				if pos < from {
					pos++
					continue
				}
				if AtLimit(len(events), limit) {
					return events, true
				}
				events = append(events, e)
				pos++
			}
		}
	}
	return events, false
}

// --- delete and collect (SES-GC) ----------------------------------------------------

func (l *Ledger) Delete(ctx context.Context, sid SessionID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	l.graph.Lock()
	defer l.graph.Unlock()
	return l.be.DeleteRecord(ctx, sid)
}

func (l *Ledger) Collect(ctx context.Context) (CollectReport, error) {
	if err := ctx.Err(); err != nil {
		return CollectReport{}, err
	}
	l.graph.Lock()
	defer l.graph.Unlock()
	ids, err := l.be.ListSegments(ctx)
	if err != nil {
		return CollectReport{}, err
	}
	nodes := make(map[SegmentID]Segment, len(ids))
	for _, id := range ids {
		seg, err := l.be.Segment(ctx, id)
		if err != nil {
			if IsCode(err, ErrNotFound) {
				continue
			}
			return CollectReport{}, err
		}
		nodes[id] = seg
	}
	roots, err := l.be.ListRecords(ctx)
	if err != nil {
		return CollectReport{}, err
	}
	need := Reachable(nodes, roots)
	report := CollectReport{Truncated: map[SegmentID]CommitSeq{}, Dropped: map[SegmentID][]CommitID{}}
	for id := range nodes {
		through, reached := need[id]
		if !reached {
			if err := l.be.RemoveSegment(ctx, id); err != nil {
				return report, err
			}
			report.Removed = append(report.Removed, id)
			continue
		}
		if through == ^CommitSeq(0) {
			continue // a root's tip keeps everything
		}
		_, head, _, err := l.be.ReadSegment(ctx, id, through+1, 1)
		if err != nil {
			return report, err
		}
		if head.Next <= through+1 {
			continue
		}
		// The dropped commits are named before they go, from the index.
		idx, _, err := l.be.Index(ctx, id)
		if err != nil {
			return report, err
		}
		var dropped []CommitID
		for i := range idx.Entries {
			if idx.Entries[i].Seq > through {
				dropped = append(dropped, idx.Entries[i].CommitID)
			}
		}
		newHead, err := l.be.TruncateSegment(ctx, id, through)
		if err != nil {
			return report, err
		}
		if len(dropped) > 0 {
			report.Dropped[id] = dropped
		}
		report.Truncated[id] = newHead.Next
	}
	return report, nil
}

func cloneCommit(c Commit) Commit {
	out := c
	out.Batches = cloneBatches(c.Batches)
	return out
}

func cloneBatches(batches []StreamBatch) []StreamBatch {
	out := make([]StreamBatch, len(batches))
	for i := range batches {
		out[i] = batches[i]
		out[i].Events = append([]Event(nil), batches[i].Events...)
	}
	return out
}

var _ Store = (*Ledger)(nil)
