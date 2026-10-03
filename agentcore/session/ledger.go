package session

import (
	"context"
	"fmt"
	"github.com/felinics/twilight/agentcore/ledger"
	"sort"
	"sync"
)

// Ledger is the kernel's Store over a Storage: the
// Session lineage tree in code. A root stores the path of spans it reads
// and names the last span's segment as its tip. Segments stay append-only
// logs and keep one parent edge, recorded when the segment is created.
// Fork copies the parent's path, closes the span that contains the fork
// seq, and appends a new open segment. The stored path is the reference
// relation: Delete drops that root's spans and reclaims from the greatest
// span that remains; Collect truncates and removes from those same spans.
// Every adapter gets these semantics from here and implements none of them.
type Ledger struct {
	st        Storage
	segmentID func() (SegmentID, error)
	// graph serializes this process's Create, Delete and Collect.
	// Append and reads never take it. Across processes, TruncateSegment and
	// CreateSession share the per-segment lock: truncation re-reads the live
	// span under that lock, and creation checks the retained commits still
	// exist before inserting a span.
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
func NewLedger(st Storage, opts ...LedgerOption) *Ledger {
	l := &Ledger{st: st, segmentID: NewSegmentID}
	for _, o := range opts {
		o(l)
	}
	return l
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
	var parent *Session
	if req.Fork != nil {
		// The edge names the segment that contributes the inherited commit,
		// wherever on the parent's path it lives.
		if req.Fork.Session == req.SessionID {
			return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, "a session cannot fork itself")
		}
		var err error
		parent, err = l.Load(ctx, req.Fork.Session)
		if err != nil {
			if IsCode(err, ErrNotFound) {
				return SegmentHeader{}, newError(ErrNotFound, "create", req.SessionID, fmt.Sprintf("parent session %s not found", req.Fork.Session))
			}
			return SegmentHeader{}, err
		}
		// The edge names a position in the parent's history.
		// EdgeAt reads the commit, so an open span cannot accept a seq the
		// segment does not hold.
		edge, ok, err := parent.EdgeAt(ctx, req.Fork.Seq)
		if err != nil {
			return SegmentHeader{}, err
		}
		if !ok {
			return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, fmt.Sprintf("parent %s has no commit %d", req.Fork.Session, req.Fork.Seq))
		}
		header.Parent = &edge
	}
	// Idempotency is judged on what the request determines about the
	// segment, not on its identity or clock: the ID is drawn fresh each time
	// and the creation time is the first writer's.
	if seg, err := l.tipSegment(ctx, req.SessionID); err == nil {
		if sameCreation(header, seg.Header) {
			return seg.Header, nil
		}
		return SegmentHeader{}, newError(ErrConflict, "create", req.SessionID, "session exists with a different creation record")
	} else if !IsCode(err, ErrNotFound) && !IsCode(err, ErrDeleted) {
		return SegmentHeader{}, err
	}
	id, err := l.segmentID()
	if err != nil {
		return SegmentHeader{}, err
	}
	header.ID = id
	if err := header.Validate(); err != nil {
		return SegmentHeader{}, newError(ErrInvalid, "create", req.SessionID, err.Error())
	}
	path, err := creationPath(parent, header, req.SessionID)
	if err != nil {
		return SegmentHeader{}, err
	}
	segment := Segment{Header: header}
	root := SessionRecord{ID: req.SessionID, Tip: segment.ID(), CreatedAtUnixMilli: req.CreatedAtUnixMilli, Path: path}
	if err := l.st.CreateSession(ctx, segment, root); err != nil {
		// The parent was checked above. Another replica may have removed
		// the parent segment or truncated a retained commit since
		// . CreateSession re-checks both under the segment lock
		// and writes nothing when either is gone.
		return SegmentHeader{}, err
	}
	return header, nil
}

// creationPath is the path stored on a new root. A fork keeps the parent's
// spans through the one that contains the fork seq, closes that span, and
// appends an open span for the new segment. The closed span's segment is
// the parent edge EdgeAt already resolved.
func creationPath(parent *Session, header SegmentHeader, sid SessionID) (Path, error) {
	if header.Parent == nil {
		path := Path{{Segment: header.ID, From: 0, End: OpenBound()}}
		if err := path.Validate(header.ID); err != nil {
			return nil, newError(ErrCorrupt, "create", sid, err.Error())
		}
		return path, nil
	}
	if parent == nil {
		return nil, newError(ErrCorrupt, "create", sid, "fork has no parent session")
	}
	base := parent.Record().Path
	if len(base) == 0 {
		base = pathFromLoaded(parent.Loaded())
	}
	path, edge, err := base.Branch(header.Parent.Seq, header.ID)
	if err != nil {
		return nil, newError(ErrCorrupt, "create", sid, err.Error())
	}
	if edge != *header.Parent {
		return nil, newError(ErrCorrupt, "create", sid, "fork edge does not match the stored path")
	}
	if err := path.Validate(header.ID); err != nil {
		return nil, newError(ErrCorrupt, "create", sid, err.Error())
	}
	return path, nil
}

// sameCreation reports whether req would create the Session that exists:
// same resolved edge, same causation, same extensions. The creation time
// is the first writer's and does not decide it, so a retried Create with a
// fresh clock is a replay.
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
	seg, err := l.tipSegment(ctx, sid)
	if err != nil {
		return SegmentHeader{}, err
	}
	return seg.Header, nil
}

func (l *Ledger) Record(ctx context.Context, sid SessionID) (SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return SessionRecord{}, err
	}
	return l.st.Record(ctx, sid)
}

// LeaseOf is Store.LeaseOf: the adapter's reading of the root's
// current holder.
func (l *Ledger) LeaseOf(ctx context.Context, sid SessionID) (Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return Lease{}, false, err
	}
	return l.st.LeaseOf(ctx, sid)
}

// ListLeases is Store.ListLeases.
func (l *Ledger) ListLeases(ctx context.Context) ([]Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return l.st.ListLeases(ctx)
}

// ExpiredLeases is Store.ExpiredLeases.
func (l *Ledger) ExpiredLeases(ctx context.Context, limit int) ([]Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return l.st.ExpiredLeases(ctx, limit)
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
	s, err := l.Load(ctx, sid)
	if err != nil {
		return nil, err
	}
	lease, err := l.st.Acquire(ctx, sid, opts)
	if err != nil {
		return nil, err
	}
	// Acquire repaired a torn tail; the segment's CommitIndex  is
	// checked against the head and rebuilt when it lags, but the handle
	// holds none of it: membership and stream heads are answered by the
	// backend's index on demand, so Open costs the same for a
	// tip of ten commits and one of a million.
	head, err := s.repairTip(ctx)
	if err != nil {
		_ = l.st.Release(ctx, lease)
		return nil, err
	}
	return &ledgerHandle{session: s, lease: lease, opts: opts, head: head, streams: make(map[ledger.Domain]ledger.StreamSeq)}, nil
}

// --- read -------------------------------------------------------------------------

func (l *Ledger) ReadCommits(ctx context.Context, req CommitReadRequest) (CommitPage, error) {
	if err := ctx.Err(); err != nil {
		return CommitPage{}, err
	}
	s, err := l.Load(ctx, req.SessionID)
	if err != nil {
		return CommitPage{}, err
	}
	return s.ReadCommits(ctx, req.From, req.Limit)
}

func (l *Ledger) ReadStream(ctx context.Context, req StreamReadRequest) (StreamPage, error) {
	if err := ctx.Err(); err != nil {
		return StreamPage{}, err
	}
	// Validate before loading so a malformed read is ErrInvalid even when the
	// Session is absent. Stream positions count the stream's events from the
	// first commit the read sees.
	if err := validateStreamRead(req.SessionID, req.Domain, req.Inheritance); err != nil {
		return StreamPage{}, err
	}
	s, err := l.Load(ctx, req.SessionID)
	if err != nil {
		return StreamPage{}, err
	}
	return s.collectStream(ctx, req.Domain, req.Inheritance, req.From, req.Limit)
}

// StreamEvents walks commits in order and returns the events of stream from
// position from, at most limit (0 = unlimited); more reports whether events
// beyond the returned ones exist. It is the one StreamSeq derivation
func StreamEvents(commits []ledger.Commit, stream ledger.Domain, from ledger.StreamSeq, limit uint32) (events []ledger.Event, more bool) {
	var pos ledger.StreamSeq
	for i := range commits {
		for j := range commits[i].Batches {
			b := &commits[i].Batches[j]
			if b.Domain != stream {
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

// --- delete and collect  ----------------------------------------------------

func (l *Ledger) Delete(ctx context.Context, sid SessionID) (CollectReport, error) {
	if err := ctx.Err(); err != nil {
		return CollectReport{}, err
	}
	l.graph.Lock()
	defer l.graph.Unlock()
	// DeleteRecord tombstones and drops this session's spans in one write.
	// ErrOwned, ErrNotFound and ErrCorrupt return before that write.
	rec, err := l.st.DeleteRecord(ctx, sid)
	if err != nil {
		return CollectReport{}, err
	}
	path, err := l.sessionPath(ctx, rec)
	if err != nil {
		return CollectReport{}, err
	}
	report := CollectReport{Truncated: map[SegmentID]ledger.CommitSeq{}, Dropped: map[SegmentID][]ledger.CommitID{}}
	// Tip first: a segment is removed only after the child edge that names
	// its parent has been removed with the child.
	for i := len(path) - 1; i >= 0; i-- {
		if err := l.reclaim(ctx, path[i].Segment, &report); err != nil {
			return report, err
		}
	}
	return report, nil
}

func (l *Ledger) Collect(ctx context.Context) (CollectReport, error) {
	if err := ctx.Err(); err != nil {
		return CollectReport{}, err
	}
	l.graph.Lock()
	defer l.graph.Unlock()
	if err := l.st.DropOrphanSpans(ctx); err != nil {
		return CollectReport{}, err
	}
	ids, err := l.st.ListSegments(ctx)
	if err != nil {
		return CollectReport{}, err
	}
	nodes := make(map[SegmentID]Segment, len(ids))
	for _, id := range ids {
		seg, err := l.st.Segment(ctx, id)
		if err != nil {
			if IsCode(err, ErrNotFound) {
				continue
			}
			return CollectReport{}, err
		}
		nodes[id] = seg
	}
	// A root stored before paths has no spans. Its edge walk is still a
	// reference, merged with whatever spans name the same segment.
	legacy, err := l.legacyBounds(ctx)
	if err != nil {
		return CollectReport{}, err
	}
	report := CollectReport{Truncated: map[SegmentID]ledger.CommitSeq{}, Dropped: map[SegmentID][]ledger.CommitID{}}
	reached := make(map[SegmentID]Bound, len(nodes))
	for id := range nodes {
		bound, ok, err := l.st.SpanBound(ctx, id)
		if err != nil {
			return report, err
		}
		if leg, has := legacy[id]; has {
			if ok {
				bound = mergeBound(bound, leg)
			} else {
				bound, ok = leg, true
			}
		}
		if !ok {
			continue
		}
		reached[id] = bound
		if err := l.clip(ctx, id, bound, &report); err != nil {
			return report, err
		}
	}
	// Segments no live path names go children first: the adapter refuses to
	// remove a node a child's edge still names.
	for _, id := range removalOrder(nodes, reached) {
		if err := l.st.RemoveSegment(ctx, id); err != nil {
			if IsCode(err, ErrReferenced) {
				continue
			}
			return report, err
		}
		report.Removed = append(report.Removed, id)
	}
	return report, nil
}

// legacyBounds is the retention of roots whose path was never stored.
// Roots that have spans are already visible through SpanBound.
func (l *Ledger) legacyBounds(ctx context.Context) (map[SegmentID]Bound, error) {
	roots, err := l.st.ListRecords(ctx)
	if err != nil {
		return nil, err
	}
	out := map[SegmentID]Bound{}
	for i := range roots {
		if len(roots[i].Path) > 0 {
			continue
		}
		path, err := l.sessionPath(ctx, roots[i])
		if err != nil {
			return nil, err
		}
		for _, span := range path {
			if cur, ok := out[span.Segment]; ok {
				out[span.Segment] = mergeBound(cur, span.End)
			} else {
				out[span.Segment] = span.End
			}
		}
	}
	return out, nil
}

// reclaim applies the greatest span that still names id. No span removes
// the segment. A closed maximum truncates the segment to that commit.
// ErrReferenced leaves the segment for a later Collect.
func (l *Ledger) reclaim(ctx context.Context, id SegmentID, report *CollectReport) error {
	bound, ok, err := l.st.SpanBound(ctx, id)
	if err != nil {
		return err
	}
	if !ok {
		if err := l.st.RemoveSegment(ctx, id); err != nil {
			if IsCode(err, ErrReferenced) {
				return nil
			}
			return err
		}
		report.Removed = append(report.Removed, id)
		return nil
	}
	return l.clip(ctx, id, bound, report)
}

// clip drops id's commits after cov. TruncateSegment re-reads the live span
// under the segment lock and may keep a higher suffix than cov; the report
// records only the commits that call actually removed.
func (l *Ledger) clip(ctx context.Context, id SegmentID, cov Bound, report *CollectReport) error {
	if cov.Open {
		return nil
	}
	head, dropped, err := l.st.TruncateSegment(ctx, id, cov.Through)
	if err != nil {
		return err
	}
	if len(dropped) == 0 {
		return nil
	}
	report.Dropped[id] = dropped
	report.Truncated[id] = head.Next
	return nil
}

// removalOrder lists the segments no live path covers so that every segment
// comes before its parent: a child's edge keeps its parent from being removed.
func removalOrder(nodes map[SegmentID]Segment, need map[SegmentID]Bound) []SegmentID {
	depth := func(id SegmentID) int {
		d := 0
		for seg, ok := nodes[id]; ok && seg.Header.Parent != nil; seg, ok = nodes[seg.Header.Parent.Segment] {
			d++
			if d > len(nodes) {
				break // a cycle cannot exist; guard the walk anyway
			}
		}
		return d
	}
	var out []SegmentID
	for id := range nodes {
		if _, reached := need[id]; !reached {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		di, dj := depth(out[i]), depth(out[j])
		if di != dj {
			return di > dj
		}
		return out[i] < out[j]
	})
	return out
}

func cloneCommit(c ledger.Commit) ledger.Commit {
	out := c
	out.Batches = cloneBatches(c.Batches)
	return out
}

func cloneBatches(batches []ledger.EventBatch) []ledger.EventBatch {
	out := make([]ledger.EventBatch, len(batches))
	for i := range batches {
		out[i] = batches[i]
		out[i].Events = append([]ledger.Event(nil), batches[i].Events...)
	}
	return out
}

var _ Store = (*Ledger)(nil)
