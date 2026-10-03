package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/felinics/twilight/agent/store/postgres/internal/db"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/jackc/pgx/v5/pgtype"
	"math"
	"sort"
	"time"
)

// SessionStore is the session.Store over this database: the kernel Ledger
// (every Session rule: Epoch fencing, lineage, fork, streams) over a
// session.Storage that keeps segments, commits and roots in tables. CreateSession's segment, spans and root are one transaction; the session foreign key is deferred to commit, so the root may be written last. Two
// SessionStores over one database are two processes over one ledger, which
// is what lets a Session's Turns run on different machines.
// It is also a session.ProjectionCacheProvider: folded projection
// states live in the database, so a Session reopened on another replica
// starts from the last saved state and folds only the tail (EXT-PRJ-3),
// instead of the whole history on every activation.
type SessionStore struct {
	*session.Ledger
	backend *sessionStorage
}

var _ session.ProjectionCacheProvider = (*SessionStore)(nil)

// ProjectionCache is the durable projection cache over the projection_cache
// table.
func (s *SessionStore) ProjectionCache() session.ProjectionCache {
	return projectionCache{d: s.backend.d}
}

type projectionCache struct{ d *DB }

// Load returns the saved state; a missing or undecodable entry is a miss,
// never an error, since the cache is derived data the log rebuilds.
func (c projectionCache) Load(ctx context.Context, sid session.SessionID, id module.ProjectionID, v module.ProjectionVersion) (jsonstable.Value, ledger.Head, bool, error) {
	row, err := c.d.q.ProjectionEntry(ctx, db.ProjectionEntryParams{Session: string(sid), Projection: string(id), Version: int64(v)})
	if noRows(err) {
		return jsonstable.Value{}, ledger.Head{}, false, nil
	}
	if err != nil {
		return jsonstable.Value{}, ledger.Head{}, false, err
	}
	state, err := jsonstable.Parse([]byte(row.State))
	if err != nil || row.Through < 0 {
		return jsonstable.Value{}, ledger.Head{}, false, nil
	}
	return state, ledger.Head{Next: ledger.CommitSeq(row.Through)}, true, nil //nolint:gosec // G115: checked non-negative
}

func (c projectionCache) Save(ctx context.Context, sid session.SessionID, id module.ProjectionID, v module.ProjectionVersion, state jsonstable.Value, through ledger.Head) error {
	return c.d.q.UpsertProjectionEntry(ctx, db.UpsertProjectionEntryParams{Session: string(sid), Projection: string(id), Version: int64(v),
		State: string(state.Bytes()), Through: int64(through.Next)}) //nolint:gosec // G115: seq values fit int64
}

// Sessions is the session.Store over this database; opts configure the
// kernel Ledger.
func (d *DB) Sessions(opts ...session.LedgerOption) *SessionStore {
	b := &sessionStorage{d: d}
	return &SessionStore{Ledger: session.NewLedger(b, opts...), backend: b}
}

// sessionStorage is session.Storage over the session tables. Every write
// holds the Session's advisory lock (Append, Acquire, Renew, Release,
// CreateSession) or the segment's (Truncate, Remove). CreateSession also
// takes each retained segment's lock, the same key Truncate holds, before
// it inserts a span. The root row is the ownership authority
// Append re-reads under the session lock.
type sessionStorage struct{ d *DB }

var _ session.Storage = (*sessionStorage)(nil)

func kerr(code session.ErrorCode, op string, sid session.SessionID, detail string) error {
	return &session.Error{Code: code, Operation: op, SessionID: sid, Detail: detail}
}

func segmentNotFound(op string, id session.SegmentID) error {
	return &session.Error{Code: session.ErrNotFound, Operation: op, Detail: fmt.Sprintf("segment %s not found", id)}
}

func (b *sessionStorage) header(ctx context.Context, q *db.Queries, op string, id session.SegmentID) (session.SegmentHeader, error) {
	raw, err := q.Segment(ctx, string(id))
	if noRows(err) {
		return session.SegmentHeader{}, segmentNotFound(op, id)
	}
	if err != nil {
		return session.SegmentHeader{}, err
	}
	var h session.SegmentHeader
	if err := json.Unmarshal([]byte(raw), &h); err != nil {
		return session.SegmentHeader{}, &session.Error{Code: session.ErrCorrupt, Operation: op, Detail: fmt.Sprintf("segment %s: %v", id, err)}
	}
	return h, nil
}

// head is the segment's Head: past its last commit, or its seed when empty.
func (b *sessionStorage) head(ctx context.Context, q *db.Queries, header *session.SegmentHeader) (ledger.Head, error) {
	last, err := q.SegmentHead(ctx, string(header.ID))
	if err != nil {
		return ledger.Head{}, err
	}
	if last < 0 {
		return header.Seed(), nil
	}
	return ledger.Head{Next: ledger.CommitSeq(last) + 1}, nil //nolint:gosec // G115: checked non-negative
}

func decodeCommits(op string, id session.SegmentID, seqs []int64, bodies []string) ([]ledger.Commit, error) {
	out := make([]ledger.Commit, 0, len(bodies))
	for i, body := range bodies {
		var c ledger.Commit
		if err := json.Unmarshal([]byte(body), &c); err != nil {
			return nil, &session.Error{Code: session.ErrCorrupt, Operation: op, Detail: fmt.Sprintf("segment %s commit %d: %v", id, seqs[i], err)}
		}
		out = append(out, c)
	}
	return out, nil
}

func (b *sessionStorage) Segment(ctx context.Context, id session.SegmentID) (session.Segment, error) {
	h, err := b.header(ctx, b.d.q, "segment", id)
	if err != nil {
		return session.Segment{}, err
	}
	return session.Segment{Header: h}, nil
}

func (b *sessionStorage) ListSegments(ctx context.Context) ([]session.SegmentID, error) {
	ids, err := b.d.q.SegmentIDs(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]session.SegmentID, 0, len(ids))
	for _, id := range ids {
		out = append(out, session.SegmentID(id))
	}
	return out, nil
}

func (b *sessionStorage) ReadSegment(ctx context.Context, id session.SegmentID, from ledger.CommitSeq, limit uint32) ([]ledger.Commit, ledger.Head, bool, error) {
	q := b.d.q
	header, err := b.header(ctx, q, "read", id)
	if err != nil {
		return nil, ledger.Head{}, false, err
	}
	if seed := header.Seed(); from < seed.Next {
		from = seed.Next
	}
	head, err := b.head(ctx, q, &header)
	if err != nil {
		return nil, ledger.Head{}, false, err
	}
	if from >= head.Next {
		return nil, head, false, nil
	}
	want := int32(math.MaxInt32)
	if limit > 0 && limit < math.MaxInt32-1 {
		want = int32(limit) + 1 //nolint:gosec // G115: bounded above
	}
	rows, err := q.SegmentCommitsFrom(ctx, db.SegmentCommitsFromParams{Segment: string(id), Seq: int64(from), Limit: want}) //nolint:gosec // G115: seq values fit int64
	if err != nil {
		return nil, ledger.Head{}, false, err
	}
	seqs := make([]int64, len(rows))
	bodies := make([]string, len(rows))
	for i, r := range rows {
		seqs[i], bodies[i] = r.Seq, r.Body
	}
	commits, err := decodeCommits("read", id, seqs, bodies)
	if err != nil {
		return nil, ledger.Head{}, false, err
	}
	more := false
	if limit > 0 && len(commits) > int(limit) {
		commits = commits[:limit]
		more = true
	}
	return commits, head, more, nil
}

// ReadSegmentStream joins the stream index to the commits:
// only the commits that carry the stream are read.
func (b *sessionStorage) ReadSegmentStream(ctx context.Context, id session.SegmentID, stream ledger.Domain, from ledger.CommitSeq, limit uint32) ([]ledger.Commit, bool, error) {
	if _, err := b.header(ctx, b.d.q, "read", id); err != nil {
		return nil, false, err
	}
	want := int32(math.MaxInt32)
	if limit > 0 && limit < math.MaxInt32-1 {
		want = int32(limit) + 1 //nolint:gosec // G115: bounded above
	}
	rows, err := b.d.q.SegmentStreamCommits(ctx, db.SegmentStreamCommitsParams{Segment: string(id), Domain: stream.Name, StreamID: stream.Id, Seq: int64(from), Limit: want}) //nolint:gosec // G115: seq values fit int64
	if err != nil {
		return nil, false, err
	}
	seqs := make([]int64, len(rows))
	bodies := make([]string, len(rows))
	for i, r := range rows {
		seqs[i], bodies[i] = r.Seq, r.Body
	}
	commits, err := decodeCommits("read", id, seqs, bodies)
	if err != nil {
		return nil, false, err
	}
	more := false
	if limit > 0 && len(commits) > int(limit) {
		commits = commits[:limit]
		more = true
	}
	return commits, more, nil
}

func (b *sessionStorage) Locate(ctx context.Context, id session.SegmentID, cid ledger.CommitID) (ledger.CommitSeq, bool, error) {
	if _, err := b.header(ctx, b.d.q, "locate", id); err != nil {
		return 0, false, err
	}
	seq, err := b.d.q.SegmentCommitSeq(ctx, db.SegmentCommitSeqParams{Segment: string(id), CommitID: string(cid)})
	if noRows(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return ledger.CommitSeq(seq), true, nil //nolint:gosec // G115: seq stored from a uint64
}

func (b *sessionStorage) LookupCommit(ctx context.Context, id session.SegmentID, cid ledger.CommitID) (ledger.Commit, bool, error) {
	if _, err := b.header(ctx, b.d.q, "lookup", id); err != nil {
		return ledger.Commit{}, false, err
	}
	body, err := b.d.q.SegmentCommitByID(ctx, db.SegmentCommitByIDParams{Segment: string(id), CommitID: string(cid)})
	if noRows(err) {
		return ledger.Commit{}, false, nil
	}
	if err != nil {
		return ledger.Commit{}, false, err
	}
	commits, err := decodeCommits("lookup", id, []int64{0}, []string{body})
	if err != nil {
		return ledger.Commit{}, false, err
	}
	return commits[0], true, nil
}

// Index derives the segment's CommitIndex from its commits: the
// rows are the index, so it is always current and PutIndex has nothing to
// persist.
func (b *sessionStorage) Index(ctx context.Context, id session.SegmentID) (session.CommitIndex, ledger.Head, error) {
	q := b.d.q
	header, err := b.header(ctx, q, "index", id)
	if err != nil {
		return session.CommitIndex{}, ledger.Head{}, err
	}
	rows, err := q.SegmentIndex(ctx, string(id))
	if err != nil {
		return session.CommitIndex{}, ledger.Head{}, err
	}
	counts, err := q.SegmentStreamCounts(ctx, string(id))
	if err != nil {
		return session.CommitIndex{}, ledger.Head{}, err
	}
	head := header.Seed()
	idx := session.CommitIndex{Through: head, Entries: make([]session.IndexEntry, 0, len(rows))}
	next := 0
	for i := range rows {
		r := &rows[i]
		e := session.IndexEntry{CommitID: ledger.CommitID(r.CommitID), Seq: ledger.CommitSeq(r.Seq)} //nolint:gosec // G115: seq stored from a uint64
		for next < len(counts) && counts[next].Seq == r.Seq {
			c := &counts[next]
			e.Streams = append(e.Streams, session.StreamCount{Domain: ledger.Domain{Name: c.Domain, Id: c.StreamID}, Events: uint32(c.Events)}) //nolint:gosec // G115: a batch holds far fewer than MaxUint32 events
			next++
		}
		idx.Entries = append(idx.Entries, e)
		head = ledger.Head{Next: e.Seq + 1}
	}
	idx.Through = head
	return idx, head, nil
}

// Summarize is one aggregate over the segment's primary key:
// the commits are the index, so the summary is always current.
func (b *sessionStorage) Summarize(ctx context.Context, id session.SegmentID) (session.IndexSummary, ledger.Head, error) {
	header, err := b.header(ctx, b.d.q, "index", id)
	if err != nil {
		return session.IndexSummary{}, ledger.Head{}, err
	}
	row, err := b.d.q.SegmentIndexSummary(ctx, string(id))
	if err != nil {
		return session.IndexSummary{}, ledger.Head{}, err
	}
	head := header.Seed()
	if row.LastSeq >= 0 {
		head = ledger.Head{Next: ledger.CommitSeq(row.LastSeq) + 1} //nolint:gosec // G115: checked non-negative
	}
	s := session.IndexSummary{Entries: uint64(row.Entries), Through: head} //nolint:gosec // G115: a count
	if row.Entries > 0 {
		s.First, s.Last = ledger.CommitSeq(row.FirstSeq), ledger.CommitSeq(row.LastSeq) //nolint:gosec // G115: checked non-negative
	}
	return s, head, nil
}

// StreamHead sums the stream's rows of the segment: one index
// range, whatever the segment's length.
func (b *sessionStorage) StreamHead(ctx context.Context, id session.SegmentID, stream ledger.Domain, before ledger.CommitSeq) (ledger.StreamSeq, error) {
	if _, err := b.header(ctx, b.d.q, "stream_head", id); err != nil {
		return 0, err
	}
	n, err := b.d.q.SegmentStreamHead(ctx, db.SegmentStreamHeadParams{Segment: string(id), Domain: stream.Name, StreamID: stream.Id, Seq: int64(before)}) //nolint:gosec // G115: seq values fit int64
	if err != nil {
		return 0, err
	}
	return ledger.StreamSeq(n), nil //nolint:gosec // G115: a sum of batch sizes
}

func (b *sessionStorage) PutIndex(ctx context.Context, id session.SegmentID, _ session.CommitIndex) error {
	_, err := b.header(ctx, b.d.q, "index", id)
	return err
}

func (b *sessionStorage) Append(ctx context.Context, lease session.Lease, id session.SegmentID, c ledger.Commit) error {
	return b.d.tx(ctx, "session:"+string(lease.Session), func(q *db.Queries) error {
		root, err := b.root(ctx, q, "append", lease.Session)
		if err != nil {
			return err
		}
		if !root.Owned || ledger.Epoch(root.Epoch) != lease.Epoch { //nolint:gosec // G115: epochs stored from a uint64
			return kerr(session.ErrOwnershipLost, "append", lease.Session, fmt.Sprintf("epoch %d superseded by %d", lease.Epoch, root.Epoch))
		}
		if root.Failed != "" {
			return kerr(session.ErrHandleFailed, "append", lease.Session, root.Failed)
		}
		if session.SegmentID(root.Tip) != id {
			return kerr(session.ErrInvalid, "append", lease.Session, "lease does not cover the segment")
		}
		header, err := b.header(ctx, q, "append", id)
		if err != nil {
			return err
		}
		head, err := b.head(ctx, q, &header)
		if err != nil {
			return err
		}
		if c.Seq != head.Next {
			return kerr(session.ErrInvalid, "append", lease.Session, "commit is not at the segment head")
		}
		body, err := json.Marshal(c)
		if err != nil {
			return err
		}
		err = q.InsertSegmentCommit(ctx, db.InsertSegmentCommitParams{Segment: string(id), Seq: int64(c.Seq), CommitID: string(c.CommitID), Body: string(body)}) //nolint:gosec // G115: seq values fit int64
		if isUniqueViolation(err) {
			return kerr(session.ErrConflict, "append", lease.Session, "commit seq or id already in the segment")
		}
		if err != nil {
			return err
		}
		// The index rows are written with the body: one per
		// stream the commit wrote, so Index and StreamHead decode nothing.
		for _, sc := range session.IndexEntryOf(&c).Streams {
			if err := q.InsertSegmentCommitStream(ctx, db.InsertSegmentCommitStreamParams{Segment: string(id), Seq: int64(c.Seq), Domain: sc.Domain.Name, StreamID: sc.Domain.Id, Events: int64(sc.Events)}); err != nil { //nolint:gosec // G115: seq values fit int64
				return err
			}
		}
		return nil
	})
}

func (b *sessionStorage) TruncateSegment(ctx context.Context, id session.SegmentID, through ledger.CommitSeq) (ledger.Head, []ledger.CommitID, error) {
	var head ledger.Head
	var dropped []ledger.CommitID
	err := b.d.tx(ctx, "segment:"+string(id), func(q *db.Queries) error {
		header, err := b.header(ctx, q, "collect", id)
		if err != nil {
			return err
		}
		// A span inserted under this same lock can only raise the cut. The
		// caller's through was read in another transaction.
		cut, truncate, err := spanCut(ctx, q, id, through)
		if err != nil {
			return err
		}
		if truncate {
			rows, err := q.SegmentIndex(ctx, string(id))
			if err != nil {
				return err
			}
			for i := range rows {
				if rows[i].Seq > int64(cut) { //nolint:gosec // G115: seq values fit int64
					dropped = append(dropped, ledger.CommitID(rows[i].CommitID))
				}
			}
		}
		if len(dropped) == 0 {
			head, err = b.head(ctx, q, &header)
			return err
		}
		if err := q.DeleteSegmentCommitStreamsAbove(ctx, db.DeleteSegmentCommitStreamsAboveParams{Segment: string(id), Seq: int64(cut)}); err != nil { //nolint:gosec // G115: seq values fit int64
			return err
		}
		if err := q.DeleteSegmentCommitsAbove(ctx, db.DeleteSegmentCommitsAboveParams{Segment: string(id), Seq: int64(cut)}); err != nil { //nolint:gosec // G115: seq values fit int64
			return err
		}
		head, err = b.head(ctx, q, &header)
		return err
	})
	if err != nil {
		return ledger.Head{}, nil, err
	}
	return head, dropped, nil
}

// spanCut is the last seq truncation may keep. truncate is false when an
// open span still names the segment: that span retains every commit. A
// closed span raises the caller's through when it covers further.
func spanCut(ctx context.Context, q *db.Queries, id session.SegmentID, through ledger.CommitSeq) (ledger.CommitSeq, bool, error) {
	v, err := q.SegmentSpanBound(ctx, string(id))
	if noRows(err) {
		return through, true, nil
	}
	if err != nil {
		return 0, false, err
	}
	bound := boundOf(v)
	if bound.Open {
		return 0, false, nil
	}
	if bound.Through > through {
		return bound.Through, true, nil
	}
	return through, true, nil
}

// RemoveSegment deletes the node unless a live root's tip or a child's
// parent edge names it. The check runs in the removal's
// transaction; the parent edge is also a foreign key, so a child inserted
// by another replica between the check and the delete makes the delete
// fail rather than orphan the child.
func (b *sessionStorage) RemoveSegment(ctx context.Context, id session.SegmentID) error {
	return b.d.tx(ctx, "segment:"+string(id), func(q *db.Queries) error {
		referenced, err := q.SegmentReferenced(ctx, string(id))
		if err != nil {
			return err
		}
		if referenced {
			return &session.Error{Code: session.ErrReferenced, Operation: "collect", Detail: fmt.Sprintf("segment %s is still referenced", id)}
		}
		if err := q.DeleteSegmentCommitStreams(ctx, string(id)); err != nil {
			return err
		}
		if err := q.DeleteSegmentCommits(ctx, string(id)); err != nil {
			return err
		}
		err = q.DeleteSegment(ctx, string(id))
		if isForeignKeyViolation(err) {
			return &session.Error{Code: session.ErrReferenced, Operation: "collect", Detail: fmt.Sprintf("segment %s is still referenced", id)}
		}
		return err
	})
}

// --- roots and leases (SessionStore) --------------------------------------------

// root reads a live root; a tombstone is not found.
func (b *sessionStorage) root(ctx context.Context, q *db.Queries, op string, sid session.SessionID) (db.SessionRoot, error) {
	r, err := q.SessionRoot(ctx, string(sid))
	if noRows(err) {
		return db.SessionRoot{}, kerr(session.ErrNotFound, op, sid, "session not found")
	}
	if err != nil {
		return db.SessionRoot{}, err
	}
	if r.Deleted {
		return db.SessionRoot{}, kerr(session.ErrNotFound, op, sid, "session deleted")
	}
	return r, nil
}

func boundOf(v pgtype.Int8) session.Bound {
	if !v.Valid {
		return session.OpenBound()
	}
	return session.ThroughBound(ledger.CommitSeq(v.Int64)) //nolint:gosec // G115: seq values fit int64
}

func throughValue(end session.Bound) pgtype.Int8 {
	if end.Open {
		return pgtype.Int8{}
	}
	return pgtype.Int8{Int64: int64(end.Through), Valid: true} //nolint:gosec // G115: seq values fit int64
}

func spanOf(segment string, from int64, through pgtype.Int8) session.Span {
	return session.Span{
		Segment: session.SegmentID(segment),
		From:    ledger.CommitSeq(from), //nolint:gosec // G115: seq values fit int64
		End:     boundOf(through),
	}
}

func (b *sessionStorage) recordOf(ctx context.Context, q *db.Queries, r *db.SessionRoot) (session.SessionRecord, error) {
	rows, err := q.SessionPathSpans(ctx, r.ID)
	if err != nil {
		return session.SessionRecord{}, err
	}
	var path session.Path
	if len(rows) > 0 {
		path = make(session.Path, len(rows))
		for i := range rows {
			path[i] = spanOf(rows[i].Segment, rows[i].FromSeq, rows[i].ThroughSeq)
		}
	}
	return session.SessionRecord{ID: session.SessionID(r.ID), Tip: session.SegmentID(r.Tip), CreatedAtUnixMilli: r.CreatedAt, Path: path}, nil
}

func leaseOf(r *db.SessionRoot) session.Lease {
	return session.Lease{Session: session.SessionID(r.ID), Epoch: ledger.Epoch(r.Epoch), Owner: r.Owner, UntilUnixMilli: r.LeaseUntil} //nolint:gosec // G115: epochs stored from a uint64
}

func (b *sessionStorage) CreateSession(ctx context.Context, seg session.Segment, rec session.SessionRecord) error {
	err := b.d.tx(ctx, "session:"+string(rec.ID), func(q *db.Queries) error {
		if existing, err := q.SessionRoot(ctx, string(rec.ID)); err == nil {
			if existing.Deleted {
				return kerr(session.ErrDeleted, "create", rec.ID, "session id was deleted and is not reused")
			}
			return kerr(session.ErrConflict, "create", rec.ID, "session exists")
		} else if !noRows(err) {
			return err
		}
		if _, err := q.Segment(ctx, string(seg.ID())); err == nil {
			return kerr(session.ErrConflict, "create", rec.ID, fmt.Sprintf("segment %s exists", seg.ID()))
		} else if !noRows(err) {
			return err
		}
		header, err := json.Marshal(seg.Header)
		if err != nil {
			return err
		}
		// The parent edge is a foreign key: a parent another
		// replica's Collect removed makes this insert fail, and while the
		// edge stands the parent cannot be removed.
		var parent pgtype.Text
		if seg.Header.Parent != nil {
			parent = pgtype.Text{String: string(seg.Header.Parent.Segment), Valid: true}
		}
		// Same keys TruncateSegment holds. Sorted so two creates that retain
		// overlapping segments cannot deadlock. The checks below and the span
		// inserts stay inside this transaction, so a truncation cannot remove
		// a commit between the check and the insert.
		for _, id := range retainedSegmentIDs(seg.ID(), seg.Header.Parent, rec.Path) {
			if err := q.AdvisoryLock(ctx, "segment:"+id); err != nil {
				return err
			}
		}
		if err := requireRetainedCommits(ctx, q, rec.ID, seg, rec.Path); err != nil {
			return err
		}
		if err := q.InsertSegment(ctx, db.InsertSegmentParams{ID: string(seg.ID()), Header: string(header), ParentSegment: parent}); err != nil {
			if isUniqueViolation(err) {
				return kerr(session.ErrConflict, "create", rec.ID, fmt.Sprintf("segment %s exists", seg.ID()))
			}
			if isForeignKeyViolation(err) {
				return kerr(session.ErrNotFound, "create", rec.ID, fmt.Sprintf("parent segment %s not found", seg.Header.Parent.Segment))
			}
			return err
		}
		for i, span := range rec.Path {
			if err := q.InsertPathSpan(ctx, db.InsertPathSpanParams{
				Session:    string(rec.ID),
				Ordinal:    int64(i),
				Segment:    string(span.Segment),
				FromSeq:    int64(span.From), //nolint:gosec // G115: seq values fit int64
				ThroughSeq: throughValue(span.End),
			}); err != nil {
				if isUniqueViolation(err) {
					return kerr(session.ErrCorrupt, "create", rec.ID, fmt.Sprintf("segment %s repeated on the path", span.Segment))
				}
				if isForeignKeyViolation(err) {
					return kerr(session.ErrNotFound, "create", rec.ID, fmt.Sprintf("segment %s not found", span.Segment))
				}
				return err
			}
		}
		err = q.InsertSessionRoot(ctx, db.InsertSessionRootParams{ID: string(rec.ID), Tip: string(rec.Tip), CreatedAt: rec.CreatedAtUnixMilli})
		if isUniqueViolation(err) {
			return kerr(session.ErrConflict, "create", rec.ID, "session exists")
		}
		return err
	})
	// The session foreign key is checked at commit, after the callback
	// returns. A span whose root was not inserted fails the transaction.
	if isForeignKeyViolation(err) {
		return kerr(session.ErrCorrupt, "create", rec.ID, "path span has no session root")
	}
	return err
}

// retainedSegmentIDs lists the segments a new path keeps, except the new
// tip. Parent is included even when the path omits it. Order is the lock
// order.
func retainedSegmentIDs(tip session.SegmentID, parent *session.CommitRef, path session.Path) []string {
	seen := map[string]struct{}{}
	add := func(id session.SegmentID) {
		if id == "" || id == tip {
			return
		}
		seen[string(id)] = struct{}{}
	}
	for _, span := range path {
		add(span.Segment)
	}
	if parent != nil {
		add(parent.Segment)
	}
	ids := make([]string, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// requireRetainedCommits reports ErrNotFound when a closed span's Through,
// or the parent edge's commit, is no longer in the segment. The caller
// holds those segments' locks.
func requireRetainedCommits(ctx context.Context, q *db.Queries, sid session.SessionID, seg session.Segment, path session.Path) error {
	check := func(id session.SegmentID, seq ledger.CommitSeq) error {
		_, err := q.SegmentCommitAt(ctx, db.SegmentCommitAtParams{Segment: string(id), Seq: int64(seq)}) //nolint:gosec // G115: seq values fit int64
		if noRows(err) {
			return kerr(session.ErrNotFound, "create", sid, fmt.Sprintf("segment %s commit %d is gone", id, seq))
		}
		return err
	}
	for _, span := range path {
		if span.Segment == seg.ID() || span.End.Open {
			continue
		}
		if err := check(span.Segment, span.End.Through); err != nil {
			return err
		}
	}
	if seg.Header.Parent != nil {
		return check(seg.Header.Parent.Segment, seg.Header.Parent.Seq)
	}
	return nil
}

func (b *sessionStorage) SpanBound(ctx context.Context, id session.SegmentID) (session.Bound, bool, error) {
	v, err := b.d.q.SegmentSpanBound(ctx, string(id))
	if noRows(err) {
		return session.Bound{}, false, nil
	}
	if err != nil {
		return session.Bound{}, false, err
	}
	return boundOf(v), true, nil
}

func (b *sessionStorage) DropOrphanSpans(ctx context.Context) error {
	return b.d.tx(ctx, "path-spans", func(q *db.Queries) error {
		return q.DeleteOrphanPathSpans(ctx)
	})
}

func (b *sessionStorage) Record(ctx context.Context, sid session.SessionID) (session.SessionRecord, error) {
	r, err := b.root(ctx, b.d.q, "record", sid)
	if err != nil {
		return session.SessionRecord{}, err
	}
	return b.recordOf(ctx, b.d.q, &r)
}

func (b *sessionStorage) ListRecords(ctx context.Context) ([]session.SessionRecord, error) {
	rows, err := b.d.q.SessionRoots(ctx)
	if err != nil {
		return nil, err
	}
	spans, err := b.d.q.LivePathSpans(ctx)
	if err != nil {
		return nil, err
	}
	paths := make(map[string]session.Path, len(rows))
	for i := range spans {
		paths[spans[i].Session] = append(paths[spans[i].Session], spanOf(spans[i].Segment, spans[i].FromSeq, spans[i].ThroughSeq))
	}
	out := make([]session.SessionRecord, 0, len(rows))
	for i := range rows {
		out = append(out, session.SessionRecord{
			ID:                 session.SessionID(rows[i].ID),
			Tip:                session.SegmentID(rows[i].Tip),
			CreatedAtUnixMilli: rows[i].CreatedAt,
			Path:               paths[rows[i].ID],
		})
	}
	return out, nil
}

func (b *sessionStorage) LeaseOf(ctx context.Context, sid session.SessionID) (session.Lease, bool, error) {
	r, err := b.root(ctx, b.d.q, "lease", sid)
	if err != nil {
		return session.Lease{}, false, err
	}
	if !r.Owned {
		return session.Lease{}, false, nil
	}
	return leaseOf(&r), true, nil
}

func (b *sessionStorage) ListLeases(ctx context.Context) ([]session.Lease, error) {
	rows, err := b.d.q.HeldSessionRoots(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]session.Lease, 0, len(rows))
	for i := range rows {
		out = append(out, leaseOf(&rows[i]))
	}
	return out, nil
}

// ExpiredLeases reads the (owned, lease_until) index: the scan of a
// replica pool costs the page it asks for, not the number of Sessions.
func (b *sessionStorage) ExpiredLeases(ctx context.Context, limit int) ([]session.Lease, error) {
	rows, err := b.d.q.ExpiredSessionRoots(ctx, db.ExpiredSessionRootsParams{Before: b.d.now().UnixMilli(), RowLimit: pageLimit(limit)})
	if err != nil {
		return nil, err
	}
	out := make([]session.Lease, 0, len(rows))
	for i := range rows {
		out = append(out, leaseOf(&rows[i]))
	}
	return out, nil
}

func (b *sessionStorage) Acquire(ctx context.Context, sid session.SessionID, opts session.OpenOptions) (session.Lease, error) {
	var out session.Lease
	err := b.d.tx(ctx, "session:"+string(sid), func(q *db.Queries) error {
		r, err := b.root(ctx, q, "open", sid)
		if err != nil {
			return err
		}
		now := b.d.now()
		if r.Owned && (r.LeaseUntil == 0 || r.LeaseUntil > now.UnixMilli()) && !opts.Takeover {
			return kerr(session.ErrOwned, "open", sid, fmt.Sprintf("owned by epoch %d (%s) until %d", r.Epoch, r.Owner, r.LeaseUntil))
		}
		if _, err := b.header(ctx, q, "open", session.SegmentID(r.Tip)); err != nil {
			return err
		}
		epoch := r.Epoch + 1
		until := leaseUntil(now, opts.LeaseDuration)
		if err := q.UpdateSessionOwnership(ctx, db.UpdateSessionOwnershipParams{Epoch: epoch, Owned: true, Owner: opts.Owner, LeaseUntil: until, Failed: "", ID: string(sid)}); err != nil {
			return err
		}
		out = session.Lease{Session: sid, Epoch: ledger.Epoch(epoch), Owner: opts.Owner, UntilUnixMilli: until} //nolint:gosec // G115: epochs fit uint64
		return nil
	})
	if err != nil {
		return session.Lease{}, err
	}
	return out, nil
}

func (b *sessionStorage) Renew(ctx context.Context, lease session.Lease, duration time.Duration) (int64, error) {
	var until int64
	err := b.d.tx(ctx, "session:"+string(lease.Session), func(q *db.Queries) error {
		r, err := b.root(ctx, q, "renew", lease.Session)
		if err != nil {
			return err
		}
		if !r.Owned || ledger.Epoch(r.Epoch) != lease.Epoch { //nolint:gosec // G115: epochs stored from a uint64
			return kerr(session.ErrOwnershipLost, "renew", lease.Session, fmt.Sprintf("epoch %d superseded by %d", lease.Epoch, r.Epoch))
		}
		until = leaseUntil(b.d.now(), duration)
		return q.UpdateSessionOwnership(ctx, db.UpdateSessionOwnershipParams{Epoch: r.Epoch, Owned: true, Owner: r.Owner, LeaseUntil: until, Failed: r.Failed, ID: r.ID})
	})
	return until, err
}

// leaseUntil is the expiry of a lease taken or renewed at now: zero when it
// never expires.
func leaseUntil(now time.Time, d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return now.Add(d).UnixMilli()
}

func (b *sessionStorage) Release(ctx context.Context, lease session.Lease) error {
	return b.d.tx(ctx, "session:"+string(lease.Session), func(q *db.Queries) error {
		r, err := b.root(ctx, q, "close", lease.Session)
		if err != nil {
			if session.IsCode(err, session.ErrNotFound) {
				return nil // the root was deleted under a released lease
			}
			return err
		}
		if r.Owned && ledger.Epoch(r.Epoch) == lease.Epoch { //nolint:gosec // G115: epochs stored from a uint64
			return q.UpdateSessionOwnership(ctx, db.UpdateSessionOwnershipParams{Epoch: r.Epoch, Owned: false, Owner: r.Owner, LeaseUntil: r.LeaseUntil, Failed: "", ID: r.ID})
		}
		return nil // releasing a superseded lease is a no-op
	})
}

func (b *sessionStorage) DeleteRecord(ctx context.Context, sid session.SessionID) (session.SessionRecord, error) {
	var rec session.SessionRecord
	err := b.d.tx(ctx, "session:"+string(sid), func(q *db.Queries) error {
		r, err := b.root(ctx, q, "delete", sid)
		if err != nil {
			return err
		}
		if r.Owned {
			return kerr(session.ErrOwned, "delete", sid, fmt.Sprintf("owned by epoch %d", r.Epoch))
		}
		rec, err = b.recordOf(ctx, q, &r)
		if err != nil {
			return err
		}
		if len(rec.Path) > 0 {
			if err := rec.Path.Validate(rec.Tip); err != nil {
				return kerr(session.ErrCorrupt, "delete", sid, err.Error())
			}
		}
		if err := q.DeleteProjectionEntries(ctx, string(sid)); err != nil {
			return err
		}
		// A tombstone, not a removal: the row stays so the id is
		// never reused; every read of it is not found. The spans go in the
		// same transaction, so a crash before reclaim leaves the segments
		// and nothing that still names them except other sessions' paths.
		if err := q.TombstoneSessionRoot(ctx, string(sid)); err != nil {
			return err
		}
		return q.DeleteSessionPathSpans(ctx, string(sid))
	})
	if err != nil {
		return session.SessionRecord{}, err
	}
	return rec, nil
}

// RemoveSegment exposes the backend's conditional node removal
// to the conformance suite; Collect is the only production caller.
func (s *SessionStore) RemoveSegment(ctx context.Context, id session.SegmentID) error {
	return s.backend.RemoveSegment(ctx, id)
}
