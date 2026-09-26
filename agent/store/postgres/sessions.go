package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	"github.com/felinics/twilight/agent/store/postgres/internal/db"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// SessionStore is the session.Store over this database: the kernel Ledger
// (every Session rule: Epoch fencing, lineage, fork, streams) over a
// session.Backend that keeps segments, commits and roots in tables. Two
// SessionStores over one database are two processes over one ledger, which
// is what lets a Session's Turns run on different machines.
//
// It is also an extension.ProjectionCacheProvider: folded projection
// states live in the database, so a Session reopened on another replica
// starts from the last saved state and folds only the tail (EXT-PRJ-3),
// instead of the whole history on every activation.
type SessionStore struct {
	*session.Ledger
	backend *sessionBackend
}

var _ extension.ProjectionCacheProvider = (*SessionStore)(nil)

// ProjectionCache is the durable projection cache over the projection_cache
// table.
func (s *SessionStore) ProjectionCache() extension.ProjectionCache {
	return projectionCache{d: s.backend.d}
}

type projectionCache struct{ d *DB }

// Load returns the saved state; a missing or undecodable entry is a miss,
// never an error, since the cache is derived data the log rebuilds.
func (c projectionCache) Load(ctx context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (jsonstable.Value, session.Head, bool, error) {
	row, err := c.d.q.ProjectionEntry(ctx, db.ProjectionEntryParams{Session: string(sid), Projection: string(id), Version: int64(v)})
	if noRows(err) {
		return jsonstable.Value{}, session.Head{}, false, nil
	}
	if err != nil {
		return jsonstable.Value{}, session.Head{}, false, err
	}
	state, err := jsonstable.Parse([]byte(row.State))
	if err != nil || row.Through < 0 {
		return jsonstable.Value{}, session.Head{}, false, nil
	}
	return state, session.Head{Next: session.CommitSeq(row.Through)}, true, nil //nolint:gosec // G115: checked non-negative
}

func (c projectionCache) Save(ctx context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion, state jsonstable.Value, through session.Head) error {
	return c.d.q.UpsertProjectionEntry(ctx, db.UpsertProjectionEntryParams{Session: string(sid), Projection: string(id), Version: int64(v),
		State: string(state.Bytes()), Through: int64(through.Next)}) //nolint:gosec // G115: seq values fit int64
}

// Sessions is the session.Store over this database; opts configure the
// kernel Ledger.
func (d *DB) Sessions(opts ...session.LedgerOption) *SessionStore {
	b := &sessionBackend{d: d}
	return &SessionStore{Ledger: session.NewLedger(b, opts...), backend: b}
}

// sessionBackend is session.Backend over the session tables. Every write
// holds the Session's advisory lock (Append, Acquire, Renew, Release,
// CreateSession) or the segment's (Truncate, Remove), so replicas serialize
// per Session; the root row is the ownership authority Append re-reads
// under that lock (SES-OWN-2).
type sessionBackend struct{ d *DB }

var _ session.Backend = (*sessionBackend)(nil)

func kerr(code session.ErrorCode, op string, sid session.SessionID, detail string) error {
	return &session.Error{Code: code, Operation: op, SessionID: sid, Detail: detail}
}

func segmentNotFound(op string, id session.SegmentID) error {
	return &session.Error{Code: session.ErrNotFound, Operation: op, Detail: fmt.Sprintf("segment %s not found", id)}
}

func (b *sessionBackend) header(ctx context.Context, q *db.Queries, op string, id session.SegmentID) (session.SegmentHeader, error) {
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
func (b *sessionBackend) head(ctx context.Context, q *db.Queries, header *session.SegmentHeader) (session.Head, error) {
	last, err := q.SegmentHead(ctx, string(header.ID))
	if err != nil {
		return session.Head{}, err
	}
	if last < 0 {
		return session.LedgerSeed(*header), nil
	}
	return session.Head{Next: session.CommitSeq(last) + 1}, nil //nolint:gosec // G115: checked non-negative
}

func decodeCommits(op string, id session.SegmentID, seqs []int64, bodies []string) ([]session.Commit, error) {
	out := make([]session.Commit, 0, len(bodies))
	for i, body := range bodies {
		var c session.Commit
		if err := json.Unmarshal([]byte(body), &c); err != nil {
			return nil, &session.Error{Code: session.ErrCorrupt, Operation: op, Detail: fmt.Sprintf("segment %s commit %d: %v", id, seqs[i], err)}
		}
		out = append(out, c)
	}
	return out, nil
}

func (b *sessionBackend) Segment(ctx context.Context, id session.SegmentID) (session.Segment, error) {
	h, err := b.header(ctx, b.d.q, "segment", id)
	if err != nil {
		return session.Segment{}, err
	}
	return session.Segment{ID: id, Header: h}, nil
}

func (b *sessionBackend) ListSegments(ctx context.Context) ([]session.SegmentID, error) {
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

func (b *sessionBackend) ReadSegment(ctx context.Context, id session.SegmentID, from session.CommitSeq, limit uint32) ([]session.Commit, session.Head, bool, error) {
	q := b.d.q
	header, err := b.header(ctx, q, "read", id)
	if err != nil {
		return nil, session.Head{}, false, err
	}
	if seed := session.LedgerSeed(header); from < seed.Next {
		from = seed.Next
	}
	head, err := b.head(ctx, q, &header)
	if err != nil {
		return nil, session.Head{}, false, err
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
		return nil, session.Head{}, false, err
	}
	seqs := make([]int64, len(rows))
	bodies := make([]string, len(rows))
	for i, r := range rows {
		seqs[i], bodies[i] = r.Seq, r.Body
	}
	commits, err := decodeCommits("read", id, seqs, bodies)
	if err != nil {
		return nil, session.Head{}, false, err
	}
	more := false
	if limit > 0 && len(commits) > int(limit) {
		commits = commits[:limit]
		more = true
	}
	return commits, head, more, nil
}

func (b *sessionBackend) Locate(ctx context.Context, id session.SegmentID, cid session.CommitID) (session.CommitSeq, bool, error) {
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
	return session.CommitSeq(seq), true, nil //nolint:gosec // G115: seq stored from a uint64
}

func (b *sessionBackend) LookupCommit(ctx context.Context, id session.SegmentID, cid session.CommitID) (session.Commit, bool, error) {
	if _, err := b.header(ctx, b.d.q, "lookup", id); err != nil {
		return session.Commit{}, false, err
	}
	body, err := b.d.q.SegmentCommitByID(ctx, db.SegmentCommitByIDParams{Segment: string(id), CommitID: string(cid)})
	if noRows(err) {
		return session.Commit{}, false, nil
	}
	if err != nil {
		return session.Commit{}, false, err
	}
	commits, err := decodeCommits("lookup", id, []int64{0}, []string{body})
	if err != nil {
		return session.Commit{}, false, err
	}
	return commits[0], true, nil
}

// Index derives the segment's CommitIndex from its commits (SES-REP-5): the
// rows are the index, so it is always current and PutIndex has nothing to
// persist.
func (b *sessionBackend) Index(ctx context.Context, id session.SegmentID) (session.CommitIndex, session.Head, error) {
	q := b.d.q
	header, err := b.header(ctx, q, "index", id)
	if err != nil {
		return session.CommitIndex{}, session.Head{}, err
	}
	rows, err := q.SegmentIndex(ctx, string(id))
	if err != nil {
		return session.CommitIndex{}, session.Head{}, err
	}
	counts, err := q.SegmentStreamCounts(ctx, string(id))
	if err != nil {
		return session.CommitIndex{}, session.Head{}, err
	}
	head := session.LedgerSeed(header)
	idx := session.CommitIndex{Through: head, Entries: make([]session.IndexEntry, 0, len(rows))}
	next := 0
	for i := range rows {
		r := &rows[i]
		e := session.IndexEntry{CommitID: session.CommitID(r.CommitID), Seq: session.CommitSeq(r.Seq)} //nolint:gosec // G115: seq stored from a uint64
		for next < len(counts) && counts[next].Seq == r.Seq {
			c := &counts[next]
			e.Streams = append(e.Streams, session.StreamCount{Stream: session.StreamRef{Domain: c.Domain, ID: c.StreamID}, Events: uint32(c.Events)}) //nolint:gosec // G115: a batch holds far fewer than MaxUint32 events
			next++
		}
		idx.Entries = append(idx.Entries, e)
		head = session.Head{Next: e.Seq + 1}
	}
	idx.Through = head
	return idx, head, nil
}

// Summarize is one aggregate over the segment's primary key (SES-REP-5):
// the commits are the index, so the summary is always current.
func (b *sessionBackend) Summarize(ctx context.Context, id session.SegmentID) (session.IndexSummary, session.Head, error) {
	header, err := b.header(ctx, b.d.q, "index", id)
	if err != nil {
		return session.IndexSummary{}, session.Head{}, err
	}
	row, err := b.d.q.SegmentIndexSummary(ctx, string(id))
	if err != nil {
		return session.IndexSummary{}, session.Head{}, err
	}
	head := session.LedgerSeed(header)
	if row.LastSeq >= 0 {
		head = session.Head{Next: session.CommitSeq(row.LastSeq) + 1} //nolint:gosec // G115: checked non-negative
	}
	s := session.IndexSummary{Entries: uint64(row.Entries), Through: head} //nolint:gosec // G115: a count
	if row.Entries > 0 {
		s.First, s.Last = session.CommitSeq(row.FirstSeq), session.CommitSeq(row.LastSeq) //nolint:gosec // G115: checked non-negative
	}
	return s, head, nil
}

// StreamHead sums the stream's rows of the segment (SES-REP-3): one index
// range, whatever the segment's length.
func (b *sessionBackend) StreamHead(ctx context.Context, id session.SegmentID, stream session.StreamRef, before session.CommitSeq) (session.StreamSeq, error) {
	if _, err := b.header(ctx, b.d.q, "stream_head", id); err != nil {
		return 0, err
	}
	n, err := b.d.q.SegmentStreamHead(ctx, db.SegmentStreamHeadParams{Segment: string(id), Domain: stream.Domain, StreamID: stream.ID, Seq: int64(before)}) //nolint:gosec // G115: seq values fit int64
	if err != nil {
		return 0, err
	}
	return session.StreamSeq(n), nil //nolint:gosec // G115: a sum of batch sizes
}

func (b *sessionBackend) PutIndex(ctx context.Context, id session.SegmentID, _ session.CommitIndex) error {
	_, err := b.header(ctx, b.d.q, "index", id)
	return err
}

func (b *sessionBackend) Append(ctx context.Context, lease session.Lease, id session.SegmentID, c session.Commit) error {
	return b.d.tx(ctx, "session:"+string(lease.Session), func(q *db.Queries) error {
		root, err := b.root(ctx, q, "append", lease.Session)
		if err != nil {
			return err
		}
		if !root.Owned || session.Epoch(root.Epoch) != lease.Epoch { //nolint:gosec // G115: epochs stored from a uint64
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
		// The index rows are written with the body (SES-REP-5): one per
		// stream the commit wrote, so Index and StreamHead decode nothing.
		for _, sc := range session.IndexEntryOf(&c).Streams {
			if err := q.InsertSegmentCommitStream(ctx, db.InsertSegmentCommitStreamParams{Segment: string(id), Seq: int64(c.Seq), Domain: sc.Stream.Domain, StreamID: sc.Stream.ID, Events: int64(sc.Events)}); err != nil { //nolint:gosec // G115: seq values fit int64
				return err
			}
		}
		return nil
	})
}

func (b *sessionBackend) TruncateSegment(ctx context.Context, id session.SegmentID, through session.CommitSeq) (session.Head, error) {
	var head session.Head
	err := b.d.tx(ctx, "segment:"+string(id), func(q *db.Queries) error {
		header, err := b.header(ctx, q, "collect", id)
		if err != nil {
			return err
		}
		if err := q.DeleteSegmentCommitStreamsAbove(ctx, db.DeleteSegmentCommitStreamsAboveParams{Segment: string(id), Seq: int64(through)}); err != nil { //nolint:gosec // G115: seq values fit int64
			return err
		}
		if err := q.DeleteSegmentCommitsAbove(ctx, db.DeleteSegmentCommitsAboveParams{Segment: string(id), Seq: int64(through)}); err != nil { //nolint:gosec // G115: seq values fit int64
			return err
		}
		head, err = b.head(ctx, q, &header)
		return err
	})
	return head, err
}

func (b *sessionBackend) RemoveSegment(ctx context.Context, id session.SegmentID) error {
	return b.d.tx(ctx, "segment:"+string(id), func(q *db.Queries) error {
		if err := q.DeleteSegmentCommitStreams(ctx, string(id)); err != nil {
			return err
		}
		if err := q.DeleteSegmentCommits(ctx, string(id)); err != nil {
			return err
		}
		return q.DeleteSegment(ctx, string(id))
	})
}

// --- roots and leases (SessionStore) --------------------------------------------

func (b *sessionBackend) root(ctx context.Context, q *db.Queries, op string, sid session.SessionID) (db.SessionRoot, error) {
	r, err := q.SessionRoot(ctx, string(sid))
	if noRows(err) {
		return db.SessionRoot{}, kerr(session.ErrNotFound, op, sid, "session not found")
	}
	if err != nil {
		return db.SessionRoot{}, err
	}
	return r, nil
}

func recordOf(r *db.SessionRoot) session.SessionRecord {
	return session.SessionRecord{ID: session.SessionID(r.ID), Tip: session.SegmentID(r.Tip), CreatedAtUnixMilli: r.CreatedAt}
}

func leaseOf(r *db.SessionRoot) session.Lease {
	return session.Lease{Session: session.SessionID(r.ID), Epoch: session.Epoch(r.Epoch), Owner: r.Owner, UntilUnixMilli: r.LeaseUntil} //nolint:gosec // G115: epochs stored from a uint64
}

func (b *sessionBackend) CreateSession(ctx context.Context, seg session.Segment, rec session.SessionRecord) error {
	return b.d.tx(ctx, "session:"+string(rec.ID), func(q *db.Queries) error {
		if _, err := q.SessionRoot(ctx, string(rec.ID)); err == nil {
			return kerr(session.ErrConflict, "create", rec.ID, "session exists")
		} else if !noRows(err) {
			return err
		}
		if _, err := q.Segment(ctx, string(seg.ID)); err == nil {
			return kerr(session.ErrConflict, "create", rec.ID, fmt.Sprintf("segment %s exists", seg.ID))
		} else if !noRows(err) {
			return err
		}
		header, err := json.Marshal(seg.Header)
		if err != nil {
			return err
		}
		if err := q.InsertSegment(ctx, db.InsertSegmentParams{ID: string(seg.ID), Header: string(header)}); err != nil {
			if isUniqueViolation(err) {
				return kerr(session.ErrConflict, "create", rec.ID, fmt.Sprintf("segment %s exists", seg.ID))
			}
			return err
		}
		err = q.InsertSessionRoot(ctx, db.InsertSessionRootParams{ID: string(rec.ID), Tip: string(rec.Tip), CreatedAt: rec.CreatedAtUnixMilli})
		if isUniqueViolation(err) {
			return kerr(session.ErrConflict, "create", rec.ID, "session exists")
		}
		return err
	})
}

func (b *sessionBackend) Record(ctx context.Context, sid session.SessionID) (session.SessionRecord, error) {
	r, err := b.root(ctx, b.d.q, "record", sid)
	if err != nil {
		return session.SessionRecord{}, err
	}
	return recordOf(&r), nil
}

func (b *sessionBackend) ListRecords(ctx context.Context) ([]session.SessionRecord, error) {
	rows, err := b.d.q.SessionRoots(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]session.SessionRecord, 0, len(rows))
	for i := range rows {
		out = append(out, recordOf(&rows[i]))
	}
	return out, nil
}

func (b *sessionBackend) LeaseOf(ctx context.Context, sid session.SessionID) (session.Lease, bool, error) {
	r, err := b.root(ctx, b.d.q, "lease", sid)
	if err != nil {
		return session.Lease{}, false, err
	}
	if !r.Owned {
		return session.Lease{}, false, nil
	}
	return leaseOf(&r), true, nil
}

func (b *sessionBackend) ListLeases(ctx context.Context) ([]session.Lease, error) {
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
func (b *sessionBackend) ExpiredLeases(ctx context.Context, beforeUnixMilli int64, limit int) ([]session.Lease, error) {
	rows, err := b.d.q.ExpiredSessionRoots(ctx, db.ExpiredSessionRootsParams{LeaseUntil: beforeUnixMilli, Limit: pageLimit(limit)})
	if err != nil {
		return nil, err
	}
	out := make([]session.Lease, 0, len(rows))
	for i := range rows {
		out = append(out, leaseOf(&rows[i]))
	}
	return out, nil
}

func (b *sessionBackend) Acquire(ctx context.Context, sid session.SessionID, opts session.OpenOptions) (session.Lease, error) {
	var out session.Lease
	err := b.d.tx(ctx, "session:"+string(sid), func(q *db.Queries) error {
		r, err := b.root(ctx, q, "open", sid)
		if err != nil {
			return err
		}
		now := opts.Now()
		if r.Owned && (r.LeaseUntil == 0 || r.LeaseUntil > now.UnixMilli()) && !opts.Takeover {
			return kerr(session.ErrOwned, "open", sid, fmt.Sprintf("owned by epoch %d (%s) until %d", r.Epoch, r.Owner, r.LeaseUntil))
		}
		if _, err := b.header(ctx, q, "open", session.SegmentID(r.Tip)); err != nil {
			return err
		}
		epoch := r.Epoch + 1
		until := opts.LeaseUntil(now)
		if err := q.UpdateSessionOwnership(ctx, db.UpdateSessionOwnershipParams{Epoch: epoch, Owned: true, Owner: opts.Owner, LeaseUntil: until, Failed: "", ID: string(sid)}); err != nil {
			return err
		}
		out = session.Lease{Session: sid, Epoch: session.Epoch(epoch), Owner: opts.Owner, UntilUnixMilli: until} //nolint:gosec // G115: epochs fit uint64
		return nil
	})
	if err != nil {
		return session.Lease{}, err
	}
	return out, nil
}

func (b *sessionBackend) Renew(ctx context.Context, lease session.Lease, until int64) error {
	return b.d.tx(ctx, "session:"+string(lease.Session), func(q *db.Queries) error {
		r, err := b.root(ctx, q, "renew", lease.Session)
		if err != nil {
			return err
		}
		if !r.Owned || session.Epoch(r.Epoch) != lease.Epoch { //nolint:gosec // G115: epochs stored from a uint64
			return kerr(session.ErrOwnershipLost, "renew", lease.Session, fmt.Sprintf("epoch %d superseded by %d", lease.Epoch, r.Epoch))
		}
		return q.UpdateSessionOwnership(ctx, db.UpdateSessionOwnershipParams{Epoch: r.Epoch, Owned: true, Owner: r.Owner, LeaseUntil: until, Failed: r.Failed, ID: r.ID})
	})
}

func (b *sessionBackend) Release(ctx context.Context, lease session.Lease) error {
	return b.d.tx(ctx, "session:"+string(lease.Session), func(q *db.Queries) error {
		r, err := b.root(ctx, q, "close", lease.Session)
		if err != nil {
			if session.IsCode(err, session.ErrNotFound) {
				return nil // the root was deleted under a released lease
			}
			return err
		}
		if r.Owned && session.Epoch(r.Epoch) == lease.Epoch { //nolint:gosec // G115: epochs stored from a uint64
			return q.UpdateSessionOwnership(ctx, db.UpdateSessionOwnershipParams{Epoch: r.Epoch, Owned: false, Owner: r.Owner, LeaseUntil: r.LeaseUntil, Failed: "", ID: r.ID})
		}
		return nil // releasing a superseded lease is a no-op
	})
}

func (b *sessionBackend) DeleteRecord(ctx context.Context, sid session.SessionID) error {
	return b.d.tx(ctx, "session:"+string(sid), func(q *db.Queries) error {
		r, err := b.root(ctx, q, "delete", sid)
		if err != nil {
			return err
		}
		if r.Owned {
			return kerr(session.ErrOwned, "delete", sid, fmt.Sprintf("owned by epoch %d", r.Epoch))
		}
		if err := q.DeleteProjectionEntries(ctx, string(sid)); err != nil {
			return err
		}
		return q.DeleteSessionRoot(ctx, string(sid))
	})
}
