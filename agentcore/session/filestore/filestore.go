// Package filestore is the JSONL-backed session.Store: the kernel Ledger over
// a file Storage. CreateSession writes the segment first and the root file last; a crash before the root leaves a segment no root names, which Collect removes. Segments (the nodes of the lineage tree) live under
// segments/<id>/ as header.json plus log.jsonl, one committed line per own
// Commit, index.jsonl, the segment's persisted CommitIndex (SES-REP-5) with
// the byte range of each commit's line, verified.json, the head through
// which the segment was last verified (SES-REP-1).
// Session roots live under sessions/<sid>.json with their writer ownership
// and stored path. That path is the reference relation: SpanBound reads
// live roots. The log is plain JSONL so a stream can be inspected and
// diffed with standard tools. Fork, inherited prefixes and retention are
// the Ledger's; this package stores nodes and roots.
//
// Ownership is arbitrated through the root file, so two Store instances over
// the same root behave as two processes: a takeover through one instance
// fences the other instance's writer on its next Append. Instances inside one
// process serialize through the store lock only — the adapter takes no
// cross-process file locks, so run at most one process per root at a time.
package filestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	segmentsDir = "segments"
	sessionsDir = "sessions"
	headerFile  = "header.json"
	logFile     = "log.jsonl"
)

// Store is the JSONL session.Store: the Ledger's methods are promoted from
// the embedded kernel; the Storage operations below are what the file layout
// implements.
type Store struct {
	*session.Ledger
	root string
	// now is the lease clock (SES-OWN-6).
	now func() time.Time
	mu  sync.Mutex // serializes every backend operation of this instance
	// index holds each segment's CommitIndex as loaded from index.jsonl and
	// verified against log.jsonl (index.go). It is keyed to the log's size and
	// mtime: any change by another instance (append, takeover, truncation)
	// invalidates it and the next use reloads it.
	index map[session.SegmentID]*segIndex
	// sync persists an appended commit; tests inject a failing one to exercise
	// the unknown-outcome path of SES-APP-1. nil means (*os.File).Sync.
	sync func(*os.File) error
}

// New opens the store root, creating it if needed. opts configure the
// kernel Ledger (for example a deterministic segment ID source).
func New(root string, opts ...session.LedgerOption) (*Store, error) {
	return NewWithClock(root, nil, opts...)
}

// NewWithClock is New with the clock leases are judged by (SES-OWN-6); nil
// selects time.Now. Fixtures inject one to age a lease.
func NewWithClock(root string, now func() time.Time, opts ...session.LedgerOption) (*Store, error) {
	for _, d := range []string{root, filepath.Join(root, segmentsDir), filepath.Join(root, sessionsDir)} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			return nil, err
		}
	}
	if now == nil {
		now = time.Now
	}
	s := &Store{root: root, now: now, index: make(map[session.SegmentID]*segIndex)}
	s.Ledger = session.NewLedger(s, opts...)
	return s, nil
}

// LogPath returns the JSONL log of the segment a Session appends to, for
// direct inspection; empty when the Session does not exist.
func (s *Store) LogPath(sid session.SessionID) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, _, err := s.loadRoot(sid, "log_path")
	if err != nil {
		return ""
	}
	return filepath.Join(s.segmentDir(rec.Tip), logFile)
}

func (s *Store) segmentDir(id session.SegmentID) string {
	return filepath.Join(s.root, segmentsDir, encodeID(string(id)))
}

func (s *Store) rootPath(sid session.SessionID) string {
	return filepath.Join(s.root, sessionsDir, encodeID(string(sid))+".json")
}

// sessionDir is where a Session's derived data (the projection cache) lives.
func (s *Store) sessionDir(sid session.SessionID) string {
	return filepath.Join(s.root, sessionsDir, encodeID(string(sid)))
}

// encodeID maps an identity to a safe file name: [A-Za-z0-9._-] bytes stay,
// every other byte is percent-encoded; "." and ".." are fully encoded.
func encodeID(id string) string {
	if id == "." || id == ".." {
		return strings.Repeat("%2E", len(id))
	}
	var b strings.Builder
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '_', c == '-':
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

func kerr(code session.ErrorCode, op string, sid session.SessionID, detail string) error {
	return &session.Error{Code: code, Operation: op, SessionID: sid, Detail: detail}
}

// segerr is kerr for a segment operation: segments name no Session, so the
// segment id goes into the detail.
func segerr(op string, id session.SegmentID, detail string) error {
	return &session.Error{Code: session.ErrCorrupt, Operation: op, Detail: fmt.Sprintf("segment %s: %s", id, detail)}
}

// --- segments (SegmentStore) --------------------------------------------------------

func readHeader(dir string) (session.SegmentHeader, error) {
	raw, err := os.ReadFile(filepath.Join(dir, headerFile))
	if err != nil {
		return session.SegmentHeader{}, err
	}
	var h session.SegmentHeader
	if err := json.Unmarshal(raw, &h); err != nil {
		return session.SegmentHeader{}, fmt.Errorf("%s: %w", headerFile, err)
	}
	return h, nil
}

// loadSegment reads and validates a segment's header. The caller holds the
// lock.
func (s *Store) loadSegment(id session.SegmentID, op string) (session.SegmentHeader, string, error) {
	dir := s.segmentDir(id)
	h, err := readHeader(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return session.SegmentHeader{}, "", &session.Error{Code: session.ErrNotFound, Operation: op, Detail: fmt.Sprintf("segment %s not found", id)}
		}
		return session.SegmentHeader{}, "", segerr(op, id, err.Error())
	}
	if h.ID != id {
		// A valid header of another segment under this directory (a copied or
		// renamed directory) must not be served as id's.
		return session.SegmentHeader{}, "", segerr(op, id, fmt.Sprintf("header names segment %s", h.ID))
	}
	if err := h.Validate(); err != nil {
		return session.SegmentHeader{}, "", err
	}
	return h, dir, nil
}

func (s *Store) Segment(ctx context.Context, id session.SegmentID) (session.Segment, error) {
	if err := ctx.Err(); err != nil {
		return session.Segment{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	h, _, err := s.loadSegment(id, "segment")
	if err != nil {
		return session.Segment{}, err
	}
	return session.Segment{Header: h}, nil
}

func (s *Store) ListSegments(ctx context.Context) ([]session.SegmentID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries, err := os.ReadDir(filepath.Join(s.root, segmentsDir))
	if err != nil {
		return nil, err
	}
	var out []session.SegmentID
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		h, err := readHeader(filepath.Join(s.root, segmentsDir, e.Name()))
		if err != nil {
			continue // not a segment directory
		}
		out = append(out, h.ID)
	}
	return out, nil
}

// ReadSegmentStream is ReadSegment narrowed to the commits whose index
// entry counts events of stream (SES-REP-2/5).
func (s *Store) ReadSegmentStream(ctx context.Context, id session.SegmentID, stream ledger.Domain, from ledger.CommitSeq, limit uint32) ([]ledger.Commit, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "read")
	if err != nil {
		return nil, false, err
	}
	x, err := s.segIndex(id, header, dir)
	if err != nil {
		return nil, false, err
	}
	var out []ledger.Commit
	for i := range x.idx.Entries {
		e := &x.idx.Entries[i]
		if e.Seq < from || !countsStream(e, stream) {
			continue
		}
		if limit > 0 && uint32(len(out)) >= limit { //nolint:gosec // G115: bounded by limit
			return out, true, nil
		}
		commits, _, err := s.commitsFrom(id, header, dir, e.Seq)
		if err != nil {
			return nil, false, err
		}
		if len(commits) == 0 || commits[0].Seq != e.Seq {
			return nil, false, segerr("read", id, "log does not match its index")
		}
		out = append(out, commits[0])
	}
	return out, false, nil
}

func countsStream(e *session.IndexEntry, stream ledger.Domain) bool {
	for _, sc := range e.Streams {
		if sc.Domain == stream && sc.Events > 0 {
			return true
		}
	}
	return false
}

// ReadSegment returns the segment's own commits from from (absolute Seq).
func (s *Store) ReadSegment(ctx context.Context, id session.SegmentID, from ledger.CommitSeq, limit uint32) ([]ledger.Commit, ledger.Head, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, ledger.Head{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "read")
	if err != nil {
		return nil, ledger.Head{}, false, err
	}
	seed := header.Seed()
	if from < seed.Next {
		from = seed.Next
	}
	commits, head, err := s.commitsFrom(id, header, dir, from)
	if err != nil {
		return nil, ledger.Head{}, false, err
	}
	if from >= head.Next {
		return nil, head, false, nil
	}
	start := 0
	end := len(commits)
	more := false
	if limit > 0 && start+int(limit) < end {
		end = start + int(limit)
		more = true
	}
	return append([]ledger.Commit(nil), commits[start:end]...), head, more, nil
}

// Append persists a commit at the segment head under the lease: the root
// file is the ownership authority, re-read here so a takeover through
// another instance fences this writer (SES-OWN-2).
func (s *Store) Append(ctx context.Context, lease session.Lease, id session.SegmentID, c ledger.Commit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, owner, err := s.loadRoot(lease.Session, "append")
	if err != nil {
		return err
	}
	if !owner.Owned || owner.Epoch != lease.Epoch {
		return kerr(session.ErrOwnershipLost, "append", lease.Session, fmt.Sprintf("epoch %d superseded by %d", lease.Epoch, owner.Epoch))
	}
	if owner.Failed != "" {
		return kerr(session.ErrHandleFailed, "append", lease.Session, owner.Failed)
	}
	if rec.Tip != id {
		return kerr(session.ErrInvalid, "append", lease.Session, "lease does not cover the segment")
	}
	header, dir, err := s.loadSegment(id, "append")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, logFile)
	_, head, err := s.commitsFrom(id, header, dir, ^ledger.CommitSeq(0))
	if err != nil {
		return err
	}
	if c.Seq != head.Next {
		return kerr(session.ErrInvalid, "append", lease.Session, "commit is not at the segment head")
	}
	line, err := json.Marshal(c)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	// The whole commit goes down in one write so a crash can only tear the
	// tail, which the next Acquire truncates.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	start, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		f.Close()
		return err
	}
	// From the first byte written the outcome is unknown until sync and close
	// succeed: a failure anywhere in between poisons the lease, because the
	// commit may or may not be on disk and appending after it would produce
	// duplicate Seqs. The next Acquire decides what is there (SES-APP-1/2).
	if _, err := f.Write(line); err != nil {
		f.Close()
		return s.fail(lease, owner, "write", err)
	}
	syncFile := s.sync
	if syncFile == nil {
		syncFile = (*os.File).Sync
	}
	if err := syncFile(f); err != nil {
		f.Close()
		return s.fail(lease, owner, "sync", err)
	}
	if err := f.Close(); err != nil {
		return s.fail(lease, owner, "close", err)
	}
	// The commit is durable; its index line follows (SES-REP-5). A failure
	// here leaves the index one commit short, which the next load repairs
	// from the log tail, so it never fails the Append.
	end := start + int64(len(line))
	entry := session.IndexEntryOf(&c)
	if err := appendFile(filepath.Join(dir, indexFile), appendIndexLine(nil, entry, start, end)); err != nil {
		s.dropIndex(id)
		return nil
	}
	x, ok := s.index[id]
	if !ok || x.end() != start {
		s.dropIndex(id)
		return nil
	}
	x.extend(&c, start, end)
	if st, err := os.Stat(path); err == nil {
		x.logSize, x.logMod = st.Size(), st.ModTime().UnixNano()
	} else {
		s.dropIndex(id)
	}
	return nil
}

// fail records on the root that this lease's last Append had an unknown
// outcome; every later Append under it gets ErrHandleFailed until a reopen.
func (s *Store) fail(lease session.Lease, owner ownerRecord, step string, cause error) error {
	detail := fmt.Sprintf("%s failed, durable outcome unknown: %v", step, cause)
	owner.Failed = detail
	_ = s.saveRoot(lease.Session, owner)
	return kerr(session.ErrHandleFailed, "append", lease.Session, detail)
}

func (s *Store) TruncateSegment(ctx context.Context, id session.SegmentID, through ledger.CommitSeq) (ledger.Head, []ledger.CommitID, error) {
	if err := ctx.Err(); err != nil {
		return ledger.Head{}, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "collect")
	if err != nil {
		return ledger.Head{}, nil, err
	}
	path := filepath.Join(dir, logFile)
	commits, _, _, _, err := readLog(path, "", "collect")
	if err != nil {
		return ledger.Head{}, nil, err
	}
	// CreateSession holds this same lock while it checks a commit and writes
	// the span that names it. A span already stored can only raise the cut.
	bound, ok, err := s.spanBoundLocked(id)
	if err != nil {
		return ledger.Head{}, nil, err
	}
	if ok && bound.Open {
		return headOf(header, commits), nil, nil
	}
	if ok && bound.Through > through {
		through = bound.Through
	}
	keep := 0
	for keep < len(commits) && commits[keep].Seq <= through {
		keep++
	}
	if keep == len(commits) {
		return headOf(header, commits), nil, nil
	}
	dropped := make([]ledger.CommitID, 0, len(commits)-keep)
	for _, c := range commits[keep:] {
		dropped = append(dropped, c.CommitID)
	}
	s.dropIndex(id)
	if err := rewriteLog(path, commits[:keep]); err != nil {
		return ledger.Head{}, nil, err
	}
	if _, err := s.rebuildIndex(id, header, dir); err != nil {
		return ledger.Head{}, nil, err
	}
	return headOf(header, commits[:keep]), dropped, nil
}

// RemoveSegment deletes the node unless a root's tip or a child's edge
// still names it (SES-GC-4); the check and the removal are under the store
// lock, which CreateSession also takes.
func (s *Store) RemoveSegment(ctx context.Context, id session.SegmentID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	referenced, err := s.referenced(id)
	if err != nil {
		return err
	}
	if referenced {
		return &session.Error{Code: session.ErrReferenced, Operation: "collect", Detail: fmt.Sprintf("segment %s is still referenced", id)}
	}
	s.dropIndex(id)
	return os.RemoveAll(s.segmentDir(id))
}

// SpanBound is the greatest end among live roots whose path names id.
// The caller does not hold the store lock.
func (s *Store) SpanBound(ctx context.Context, id session.SegmentID) (session.Bound, bool, error) {
	if err := ctx.Err(); err != nil {
		return session.Bound{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.spanBoundLocked(id)
}

// spanBoundLocked is SpanBound. The caller holds s.mu.
func (s *Store) spanBoundLocked(id session.SegmentID) (session.Bound, bool, error) {
	roots, err := s.readRoots()
	if err != nil {
		return session.Bound{}, false, err
	}
	var ends []session.Bound
	for i := range roots {
		if roots[i].Deleted {
			continue
		}
		for _, span := range roots[i].Path {
			if span.Segment == id {
				ends = append(ends, span.End)
			}
		}
	}
	bound, ok := session.MaxBound(ends)
	return bound, ok, nil
}

// DropOrphanSpans has nothing to drop: the path lives on the root file, and
// a tombstone clears it in the same write that marks the root deleted.
func (s *Store) DropOrphanSpans(ctx context.Context) error {
	return ctx.Err()
}

// referenced reports whether a live root's tip or any segment's parent
// edge names id.
func (s *Store) referenced(id session.SegmentID) (bool, error) {
	roots, err := s.readRoots()
	if err != nil {
		return false, err
	}
	for _, r := range roots {
		if !r.Deleted && r.Tip == id {
			return true, nil
		}
	}
	entries, err := os.ReadDir(filepath.Join(s.root, segmentsDir))
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		h, err := readHeader(filepath.Join(s.root, segmentsDir, e.Name()))
		if err != nil {
			continue
		}
		if h.Parent != nil && h.Parent.Segment == id {
			return true, nil
		}
	}
	return false, nil
}

// --- roots (SessionStore) -----------------------------------------------------------

// ownerRecord is the persisted root: the Session's segment and its writer
// ownership. The file is the ownership authority every Append checks.
type ownerRecord struct {
	session.SessionRecord
	Epoch ledger.Epoch `json:"epoch"`
	Owned bool         `json:"owned"`
	// Owner and LeaseUntilUnixMilli are the lease's holder and expiry
	// (SES-OWN-1); a zero expiry never expires.
	Owner               string `json:"owner,omitempty"`
	LeaseUntilUnixMilli int64  `json:"leaseUntilUnixMilli,omitempty"`
	// Failed records a lease whose last Append had an unknown outcome; it is
	// cleared by the next Acquire, which reads the log as it is.
	Failed string `json:"failed,omitempty"`
	// Deleted marks a tombstone (SES-GC-1): the root file stays so the
	// SessionID is never reused, and every read treats it as not found.
	Deleted bool `json:"deleted,omitempty"`
}

func (s *Store) loadRoot(sid session.SessionID, op string) (session.SessionRecord, ownerRecord, error) {
	raw, err := os.ReadFile(s.rootPath(sid))
	if err != nil {
		if os.IsNotExist(err) {
			return session.SessionRecord{}, ownerRecord{}, kerr(session.ErrNotFound, op, sid, "session not found")
		}
		return session.SessionRecord{}, ownerRecord{}, kerr(session.ErrCorrupt, op, sid, err.Error())
	}
	var rec ownerRecord
	if err := json.Unmarshal(raw, &rec); err != nil {
		return session.SessionRecord{}, ownerRecord{}, kerr(session.ErrCorrupt, op, sid, err.Error())
	}
	if rec.ID != sid || rec.Tip == "" {
		return session.SessionRecord{}, ownerRecord{}, kerr(session.ErrCorrupt, op, sid, fmt.Sprintf("root names session %q", rec.ID))
	}
	if rec.Deleted {
		return session.SessionRecord{}, ownerRecord{}, kerr(session.ErrNotFound, op, sid, "session deleted")
	}
	return rec.SessionRecord, rec, nil
}

func (s *Store) saveRoot(sid session.SessionID, rec ownerRecord) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return writeAtomic(s.rootPath(sid), raw)
}

// CreateSession lands a new node, then the root, under the store lock. The
// path is part of the root file, which is the last atomic write, so a crash
// in between leaves a segment no root names. Collect removes it (SES-GC-2).
// There is never a root without its segment, and never a second root on an
// existing one.
func (s *Store) CreateSession(ctx context.Context, seg session.Segment, rec session.SessionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if raw, err := os.ReadFile(s.rootPath(rec.ID)); err == nil {
		var existing ownerRecord
		if json.Unmarshal(raw, &existing) == nil && existing.Deleted {
			return kerr(session.ErrDeleted, "create", rec.ID, "session id was deleted and is not reused")
		}
		return kerr(session.ErrConflict, "create", rec.ID, "session exists")
	} else if !os.IsNotExist(err) {
		return kerr(session.ErrCorrupt, "create", rec.ID, err.Error())
	}
	dir := s.segmentDir(seg.ID())
	if _, err := readHeader(dir); err == nil {
		return kerr(session.ErrConflict, "create", rec.ID, fmt.Sprintf("segment %s exists", seg.ID()))
	} else if !os.IsNotExist(err) {
		return segerr("create", seg.ID(), err.Error())
	}
	// The parent must still be a node at the moment the edge is written
	// (SES-GC-4); the store lock keeps RemoveSegment and TruncateSegment
	// out meanwhile. A closed span's commit is checked under that same
	// lock, so the span is not written for a commit this store just truncated.
	if seg.Header.Parent != nil {
		if _, err := readHeader(s.segmentDir(seg.Header.Parent.Segment)); err != nil {
			if os.IsNotExist(err) {
				return &session.Error{Code: session.ErrNotFound, Operation: "create", SessionID: rec.ID, Detail: fmt.Sprintf("parent segment %s not found", seg.Header.Parent.Segment)}
			}
			return segerr("create", seg.Header.Parent.Segment, err.Error())
		}
	}
	if err := s.requireRetainedCommits(rec.ID, seg, rec.Path); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	raw, err := json.Marshal(seg.Header)
	if err != nil {
		return err
	}
	if err := writeAtomic(filepath.Join(dir, headerFile), raw); err != nil {
		return err
	}
	return s.saveRoot(rec.ID, ownerRecord{SessionRecord: rec})
}

// requireRetainedCommits reports ErrNotFound when a closed span's Through,
// or the parent edge's commit, is not in the log. The caller holds s.mu.
func (s *Store) requireRetainedCommits(sid session.SessionID, seg session.Segment, path session.Path) error {
	check := func(id session.SegmentID, seq ledger.CommitSeq) error {
		commits, _, _, _, err := readLog(filepath.Join(s.segmentDir(id), logFile), sid, "create")
		if err != nil {
			return err
		}
		for i := range commits {
			if commits[i].Seq == seq {
				return nil
			}
		}
		return &session.Error{Code: session.ErrNotFound, Operation: "create", SessionID: sid, Detail: fmt.Sprintf("segment %s commit %d is gone", id, seq)}
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

func (s *Store) Record(ctx context.Context, sid session.SessionID) (session.SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return session.SessionRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, _, err := s.loadRoot(sid, "record")
	return rec, err
}

func (s *Store) ListRecords(ctx context.Context) ([]session.SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	roots, err := s.readRoots()
	if err != nil {
		return nil, err
	}
	out := make([]session.SessionRecord, 0, len(roots))
	for _, rec := range roots {
		if rec.Deleted {
			continue
		}
		out = append(out, rec.SessionRecord)
	}
	return out, nil
}

// readRoots reads every root file; a file that does not parse as a root is
// skipped. The caller holds mu.
func (s *Store) readRoots() ([]ownerRecord, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, sessionsDir))
	if err != nil {
		return nil, err
	}
	var out []ownerRecord
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(s.root, sessionsDir, e.Name()))
		if err != nil {
			continue
		}
		var rec ownerRecord
		if err := json.Unmarshal(raw, &rec); err != nil || rec.ID == "" {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// LeaseOf is session.SessionStore.LeaseOf (SES-OWN-5): the root's holder as
// the root file records it, whether or not the lease is still live.
func (s *Store) LeaseOf(ctx context.Context, sid session.SessionID) (session.Lease, bool, error) {
	if err := ctx.Err(); err != nil {
		return session.Lease{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, owner, err := s.loadRoot(sid, "lease")
	if err != nil {
		return session.Lease{}, false, err
	}
	if !owner.Owned {
		return session.Lease{}, false, nil
	}
	return owner.lease(), true, nil
}

// ListLeases is session.SessionStore.ListLeases (SES-OWN-5): every held
// root's Lease, expired ones included.
func (s *Store) ListLeases(ctx context.Context) ([]session.Lease, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	roots, err := s.readRoots()
	if err != nil {
		return nil, err
	}
	var out []session.Lease
	for _, rec := range roots {
		if rec.Owned {
			out = append(out, rec.lease())
		}
	}
	return out, nil
}

// lease is the Lease an owned root records.
func (s *Store) ExpiredLeases(ctx context.Context, limit int) ([]session.Lease, error) {
	held, err := s.ListLeases(ctx)
	if err != nil {
		return nil, err
	}
	return session.ExpiredLeasesOf(held, s.now().UnixMilli(), limit), nil
}

func (r ownerRecord) lease() session.Lease {
	return session.Lease{Session: r.ID, Epoch: r.Epoch, Owner: r.Owner, UntilUnixMilli: r.LeaseUntilUnixMilli}
}

// Acquire takes writer ownership (SES-OWN-1) and repairs a torn tail of the
// root's segment before the lease is issued.
func (s *Store) Acquire(ctx context.Context, sid session.SessionID, opts session.OpenOptions) (session.Lease, error) {
	if err := ctx.Err(); err != nil {
		return session.Lease{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, owner, err := s.loadRoot(sid, "open")
	if err != nil {
		return session.Lease{}, err
	}
	now := s.now()
	if owner.Owned && (owner.LeaseUntilUnixMilli == 0 || owner.LeaseUntilUnixMilli > now.UnixMilli()) && !opts.Takeover {
		return session.Lease{}, kerr(session.ErrOwned, "open", sid, fmt.Sprintf("owned by epoch %d (%s) until %d", owner.Epoch, owner.Owner, owner.LeaseUntilUnixMilli))
	}
	_, dir, err := s.loadSegment(rec.Tip, "open")
	if err != nil {
		return session.Lease{}, err
	}
	logPath := filepath.Join(dir, logFile)
	_, _, retained, torn, err := readLog(logPath, sid, "open")
	if err != nil {
		return session.Lease{}, err
	}
	// A torn tail — a partial line or a line that does not parse — is the
	// remnant of a crashed append; the new owner truncates it so the stream
	// continues from the last whole commit.
	if torn {
		if err := os.Truncate(logPath, retained); err != nil {
			return session.Lease{}, err
		}
	}
	owner.Epoch++
	owner.Owned = true
	owner.Failed = ""
	owner.Owner = opts.Owner
	owner.LeaseUntilUnixMilli = leaseUntil(now, opts.LeaseDuration)
	if err := s.saveRoot(sid, owner); err != nil {
		return session.Lease{}, err
	}
	// The truncation, if any, shortened the log under the index; the next use
	// reconciles index.jsonl with it (SES-REP-5).
	s.dropIndex(rec.Tip)
	return session.Lease{Session: sid, Epoch: owner.Epoch, Owner: owner.Owner, UntilUnixMilli: owner.LeaseUntilUnixMilli}, nil
}

func (s *Store) Renew(ctx context.Context, lease session.Lease, duration time.Duration) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, owner, err := s.loadRoot(lease.Session, "renew")
	if err != nil {
		return 0, err
	}
	if !owner.Owned || owner.Epoch != lease.Epoch {
		return 0, kerr(session.ErrOwnershipLost, "renew", lease.Session, fmt.Sprintf("epoch %d superseded by %d", lease.Epoch, owner.Epoch))
	}
	owner.LeaseUntilUnixMilli = leaseUntil(s.now(), duration)
	if err := s.saveRoot(lease.Session, owner); err != nil {
		return 0, err
	}
	return owner.LeaseUntilUnixMilli, nil
}

// leaseUntil is the expiry of a lease taken or renewed at now: zero when it
// never expires.
func leaseUntil(now time.Time, d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return now.Add(d).UnixMilli()
}

func (s *Store) Release(ctx context.Context, lease session.Lease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, owner, err := s.loadRoot(lease.Session, "close")
	if err != nil {
		if session.IsNotFound(err) {
			return nil // the root was deleted under a released lease
		}
		return err
	}
	if owner.Owned && owner.Epoch == lease.Epoch {
		owner.Owned = false
		owner.Failed = ""
		return s.saveRoot(lease.Session, owner)
	}
	return nil // releasing a superseded lease is a no-op
}

func (s *Store) DeleteRecord(ctx context.Context, sid session.SessionID) (session.SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return session.SessionRecord{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, owner, err := s.loadRoot(sid, "delete")
	if err != nil {
		return session.SessionRecord{}, err
	}
	if owner.Owned {
		return session.SessionRecord{}, kerr(session.ErrOwned, "delete", sid, fmt.Sprintf("owned by epoch %d", owner.Epoch))
	}
	prior := owner.SessionRecord
	if len(prior.Path) > 0 {
		if err := prior.Path.Validate(prior.Tip); err != nil {
			return session.SessionRecord{}, kerr(session.ErrCorrupt, "delete", sid, err.Error())
		}
	}
	// A tombstone, not a removal: the SessionID stays taken (SES-GC-1).
	// The path is cleared in the same write, so this root stops naming
	// segments. Tip stays: a tombstone may name a segment Collect removes.
	owner.Path = nil
	owner.Deleted, owner.Owned, owner.Owner, owner.LeaseUntilUnixMilli, owner.Failed = true, false, "", 0, ""
	if err := s.saveRoot(sid, owner); err != nil {
		return session.SessionRecord{}, err
	}
	// The Session's derived data goes with its root.
	if err := os.RemoveAll(s.sessionDir(sid)); err != nil {
		return session.SessionRecord{}, err
	}
	return prior, nil
}

func headOf(h session.SegmentHeader, commits []ledger.Commit) ledger.Head {
	if len(commits) == 0 {
		return h.Seed()
	}
	return ledger.Head{Next: commits[len(commits)-1].Seq + 1}
}

// --- log file ---------------------------------------------------------------------

// readLog parses log.jsonl, one committed line per Commit. A torn tail — a
// final line without its newline or one that does not parse — is excluded;
// retained is the byte length of the retained prefix and torn reports whether
// anything was excluded. Malformed content before the final line is
// ErrCorrupt.
func readLog(path string, sid session.SessionID, op string) (commits []ledger.Commit, offsets []int64, retained int64, torn bool, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil, 0, false, nil
		}
		return nil, nil, 0, false, err
	}
	return parseLog(data, sid, op)
}

// parseLog parses whole lines. offsets has one entry per parsed commit plus a
// final entry holding the end of the last one, so offsets[len(commits)] is
// the retained byte length whether or not a tail was dropped.
func parseLog(data []byte, sid session.SessionID, op string) (commits []ledger.Commit, offsets []int64, retained int64, torn bool, err error) {
	off := 0
	for off < len(data) {
		nl := bytes.IndexByte(data[off:], '\n')
		if nl < 0 {
			torn = true // unterminated tail line
			break
		}
		line := data[off : off+nl]
		var c ledger.Commit
		if uerr := json.Unmarshal(line, &c); uerr != nil {
			if off+nl+1 == len(data) {
				torn = true // torn write of the final line
				break
			}
			return nil, nil, 0, false, kerr(session.ErrCorrupt, op, sid, fmt.Sprintf("commit at byte %d: %v", off, uerr))
		}
		commits = append(commits, c)
		offsets = append(offsets, int64(off))
		off += nl + 1
	}
	offsets = append(offsets, int64(off))
	retained = int64(off)
	return commits, offsets, retained, torn, nil
}

// readRange reads [start, end) of the file.
func readRange(path string, start, end int64) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	data := make([]byte, end-start)
	if _, err := f.ReadAt(data, start); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return data, nil
}

// rewriteLog replaces log.jsonl with exactly these commits.
func rewriteLog(path string, commits []ledger.Commit) error {
	var buf bytes.Buffer
	for i := range commits {
		line, err := json.Marshal(commits[i])
		if err != nil {
			return err
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}
	return writeAtomic(path, buf.Bytes())
}

// --- test hooks -----------------------------------------------------------------

// tip locates the segment a live Session appends to. The caller holds the lock.
func (s *Store) tip(sid session.SessionID, op string) (session.SegmentHeader, string, error) {
	rec, _, err := s.loadRoot(sid, op)
	if err != nil {
		return session.SegmentHeader{}, "", err
	}
	return s.loadSegment(rec.Tip, op)
}

// CrashTail rewrites log.jsonl keeping only the first keep commits, so every
// commit after them never became durable. It exists so conformance can prove
// that Open recovers to the last whole commit (SES-APP-2); production code
// never calls it.
func (s *Store) CrashTail(sid session.SessionID, keep int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.tip(sid, "crash_tail")
	if err != nil {
		return err
	}
	path := filepath.Join(dir, logFile)
	commits, _, _, _, err := readLog(path, sid, "crash_tail")
	if err != nil {
		return err
	}
	if keep < 0 {
		keep = 0
	}
	if keep > len(commits) {
		keep = len(commits)
	}
	s.dropIndex(header.ID)
	return rewriteLog(path, commits[:keep])
}

// --- io helpers -----------------------------------------------------------------

func writeAtomic(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	_, werr := tmp.Write(data)
	serr := tmp.Sync()
	cerr := tmp.Close()
	for _, e := range []error{werr, serr, cerr} {
		if e != nil {
			os.Remove(name)
			return e
		}
	}
	if err := os.Rename(name, path); err != nil {
		os.Remove(name)
		return err
	}
	return nil
}

var _ session.Store = (*Store)(nil)
var _ session.Storage = (*Store)(nil)
