// Package filestore is the JSONL-backed session.Store: the kernel Ledger over
// a file Backend. Segments (the nodes of the lineage tree) live under
// segments/<id>/ as header.json plus log.jsonl, one committed line per own
// Commit, index.jsonl, the segment's persisted CommitIndex (SES-REP-5) with
// the byte range of each commit's line, and verified.json, the head through
// which the segment was last verified (SES-REP-1); Session roots live under
// sessions/<sid>.json with their writer ownership. The log is plain JSONL so a stream can be inspected and diffed
// with standard tools. Fork, inherited prefixes and reachability are the
// Ledger's; this package stores nodes and roots.
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
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/felinics/twilight/agentcore/session"
)

const (
	segmentsDir = "segments"
	sessionsDir = "sessions"
	headerFile  = "header.json"
	logFile     = "log.jsonl"
)

// Store is the JSONL session.Store: the Ledger's methods are promoted from
// the embedded kernel; the Backend operations below are what the file layout
// implements.
type Store struct {
	*session.Ledger
	root string
	mu   sync.Mutex // serializes every backend operation of this instance
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
	for _, d := range []string{root, filepath.Join(root, segmentsDir), filepath.Join(root, sessionsDir)} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			return nil, err
		}
	}
	s := &Store{root: root, index: make(map[session.SegmentID]*segIndex)}
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

// --- segments (LedgerStore) --------------------------------------------------------

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
	if err := session.ValidateHeader(h); err != nil {
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
	return session.Segment{ID: id, Header: h}, nil
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

// ReadSegment returns the segment's own commits from from (absolute Seq).
func (s *Store) ReadSegment(ctx context.Context, id session.SegmentID, from session.CommitSeq, limit uint32) ([]session.Commit, session.Head, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, session.Head{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "read")
	if err != nil {
		return nil, session.Head{}, false, err
	}
	seed := session.LedgerSeed(header)
	if from < seed.Next {
		from = seed.Next
	}
	commits, head, err := s.commitsFrom(id, header, dir, from)
	if err != nil {
		return nil, session.Head{}, false, err
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
	return append([]session.Commit(nil), commits[start:end]...), head, more, nil
}

// Append persists a commit at the segment head under the lease: the root
// file is the ownership authority, re-read here so a takeover through
// another instance fences this writer (SES-OWN-2).
func (s *Store) Append(ctx context.Context, lease session.Lease, id session.SegmentID, c session.Commit) error {
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
	_, head, err := s.commitsFrom(id, header, dir, ^session.CommitSeq(0))
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

func (s *Store) TruncateSegment(ctx context.Context, id session.SegmentID, through session.CommitSeq) (session.Head, error) {
	if err := ctx.Err(); err != nil {
		return session.Head{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "collect")
	if err != nil {
		return session.Head{}, err
	}
	path := filepath.Join(dir, logFile)
	commits, _, _, _, err := readLog(path, "", "collect")
	if err != nil {
		return session.Head{}, err
	}
	keep := 0
	for keep < len(commits) && commits[keep].Seq <= through {
		keep++
	}
	s.dropIndex(id)
	if err := rewriteLog(path, commits[:keep]); err != nil {
		return session.Head{}, err
	}
	if _, err := s.rebuildIndex(id, header, dir); err != nil {
		return session.Head{}, err
	}
	return headOf(header, commits[:keep]), nil
}

func (s *Store) RemoveSegment(ctx context.Context, id session.SegmentID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropIndex(id)
	return os.RemoveAll(s.segmentDir(id))
}

// --- roots (SessionStore) -----------------------------------------------------------

// ownerRecord is the persisted root: the Session's segment and its writer
// ownership. The file is the ownership authority every Append checks.
type ownerRecord struct {
	session.SessionRecord
	Epoch session.Epoch `json:"epoch"`
	Owned bool          `json:"owned"`
	// Owner and LeaseUntilUnixMilli are the lease's holder and expiry
	// (SES-OWN-1); a zero expiry never expires.
	Owner               string `json:"owner,omitempty"`
	LeaseUntilUnixMilli int64  `json:"leaseUntilUnixMilli,omitempty"`
	// Failed records a lease whose last Append had an unknown outcome; it is
	// cleared by the next Acquire, which reads the log as it is.
	Failed string `json:"failed,omitempty"`
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
// root file is the last atomic write, so a crash in between leaves a segment
// no root names: exactly what Collect reclaims (SES-GC-2). There is never a
// root without its segment, and never a second root on an existing one.
func (s *Store) CreateSession(ctx context.Context, seg session.Segment, rec session.SessionRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := os.Stat(s.rootPath(rec.ID)); err == nil {
		return kerr(session.ErrConflict, "create", rec.ID, "session exists")
	} else if !os.IsNotExist(err) {
		return kerr(session.ErrCorrupt, "create", rec.ID, err.Error())
	}
	dir := s.segmentDir(seg.ID)
	if _, err := readHeader(dir); err == nil {
		return kerr(session.ErrConflict, "create", rec.ID, fmt.Sprintf("segment %s exists", seg.ID))
	} else if !os.IsNotExist(err) {
		return segerr("create", seg.ID, err.Error())
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
func (s *Store) ExpiredLeases(ctx context.Context, beforeUnixMilli int64, limit int) ([]session.Lease, error) {
	held, err := s.ListLeases(ctx)
	if err != nil {
		return nil, err
	}
	return session.ExpiredLeasesOf(held, beforeUnixMilli, limit), nil
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
	now := opts.Now()
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
	owner.LeaseUntilUnixMilli = opts.LeaseUntil(now)
	if err := s.saveRoot(sid, owner); err != nil {
		return session.Lease{}, err
	}
	// The truncation, if any, shortened the log under the index; the next use
	// reconciles index.jsonl with it (SES-REP-5).
	s.dropIndex(rec.Tip)
	return session.Lease{Session: sid, Epoch: owner.Epoch, Owner: owner.Owner, UntilUnixMilli: owner.LeaseUntilUnixMilli}, nil
}

func (s *Store) Renew(ctx context.Context, lease session.Lease, until int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, owner, err := s.loadRoot(lease.Session, "renew")
	if err != nil {
		return err
	}
	if !owner.Owned || owner.Epoch != lease.Epoch {
		return kerr(session.ErrOwnershipLost, "renew", lease.Session, fmt.Sprintf("epoch %d superseded by %d", lease.Epoch, owner.Epoch))
	}
	owner.LeaseUntilUnixMilli = until
	return s.saveRoot(lease.Session, owner)
}

func (s *Store) Release(ctx context.Context, lease session.Lease) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, owner, err := s.loadRoot(lease.Session, "close")
	if err != nil {
		if session.IsCode(err, session.ErrNotFound) {
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

func (s *Store) DeleteRecord(ctx context.Context, sid session.SessionID) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, owner, err := s.loadRoot(sid, "delete")
	if err != nil {
		return err
	}
	if owner.Owned {
		return kerr(session.ErrOwned, "delete", sid, fmt.Sprintf("owned by epoch %d", owner.Epoch))
	}
	if err := os.Remove(s.rootPath(sid)); err != nil && !os.IsNotExist(err) {
		return err
	}
	// The Session's derived data goes with its root.
	return os.RemoveAll(s.sessionDir(sid))
}

func headOf(h session.SegmentHeader, commits []session.Commit) session.Head {
	if len(commits) == 0 {
		return session.LedgerSeed(h)
	}
	return session.Head{Next: commits[len(commits)-1].Seq + 1}
}

// --- log file ---------------------------------------------------------------------

// readLog parses log.jsonl, one committed line per Commit. A torn tail — a
// final line without its newline or one that does not parse — is excluded;
// retained is the byte length of the retained prefix and torn reports whether
// anything was excluded. Malformed content before the final line is
// ErrCorrupt.
func readLog(path string, sid session.SessionID, op string) (commits []session.Commit, offsets []int64, retained int64, torn bool, err error) {
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
func parseLog(data []byte, sid session.SessionID, op string) (commits []session.Commit, offsets []int64, retained int64, torn bool, err error) {
	off := 0
	for off < len(data) {
		nl := bytes.IndexByte(data[off:], '\n')
		if nl < 0 {
			torn = true // unterminated tail line
			break
		}
		line := data[off : off+nl]
		var c session.Commit
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
func rewriteLog(path string, commits []session.Commit) error {
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
var _ session.Backend = (*Store)(nil)
