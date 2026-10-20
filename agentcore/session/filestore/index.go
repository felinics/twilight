package filestore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
	"io"
	"os"
	"path/filepath"
)

// indexFile is the persisted CommitIndex of a segment: one line
// per own commit, appended in the same order as log.jsonl. Each line is the
// kernel's IndexEntry plus the byte range of the commit's line in the log, so
// LookupCommit and ReadSegment read exactly the bytes they need. The index is
// derived data: it is appended after the commit it describes, so a crash can
// leave it behind, which load repairs from the log tail; any
// other disagreement with the log rebuilds it from the log.
const indexFile = "index.jsonl"

// indexLine is one persisted index entry.
type indexLine struct {
	session.IndexEntry
	Start int64 `json:"start"`
	End   int64 `json:"end"`
}

// commitSpan is the byte range [start, end) of one committed line in
// log.jsonl.
type commitSpan struct{ start, end int64 }

// segIndex is a segment's index as this instance holds it: the kernel
// CommitIndex, the spans parallel to its entries, the lookup map, and the
// log file size and mtime it was verified against. Any change to the log by
// another instance invalidates it and the next use reloads it.
type segIndex struct {
	idx     session.CommitIndex
	spans   []commitSpan
	byID    map[ledger.CommitID]int
	logSize int64
	logMod  int64
}

func (x *segIndex) end() int64 {
	if len(x.spans) == 0 {
		return 0
	}
	return x.spans[len(x.spans)-1].end
}

func (x *segIndex) extend(c *ledger.Commit, start, end int64) {
	x.idx.Extend(c)
	x.spans = append(x.spans, commitSpan{start, end})
	x.byID[c.CommitID] = len(x.spans) - 1
}

// segIndex returns the segment's index, current against the log at path
// . The caller holds the store lock.
func (s *Store) segIndex(id session.SegmentID, header session.SegmentHeader, dir string) (*segIndex, error) {
	logPath := filepath.Join(dir, logFile)
	st, err := os.Stat(logPath)
	size, mod := int64(0), int64(0)
	if err == nil {
		size, mod = st.Size(), st.ModTime().UnixNano()
	} else if !os.IsNotExist(err) {
		return nil, segerr("index", id, err.Error())
	}
	if x, ok := s.index[id]; ok && x.logSize == size && x.logMod == mod {
		return x, nil
	}
	x, err := s.loadIndex(id, header, dir, size)
	if err != nil {
		return nil, err
	}
	x.logSize, x.logMod = size, mod
	s.index[id] = x
	return x, nil
}

// loadIndex reads index.jsonl and reconciles it with the log: consistent when
// its last span ends where the log ends and its last entry matches the log's
// last line; lagging when the log continues past it, in which case the tail
// is parsed and appended; anything else is rebuilt from the whole log.
func (s *Store) loadIndex(id session.SegmentID, header session.SegmentHeader, dir string, logSize int64) (*segIndex, error) {
	logPath := filepath.Join(dir, logFile)
	x := s.readIndexFile(header, dir)
	switch {
	case x != nil && x.end() == logSize && s.tailMatches(x, logPath):
		return x, nil
	case x != nil && x.end() < logSize:
		if repaired, err := s.repairIndexTail(x, logPath); err == nil && repaired {
			return x, nil
		}
	}
	return s.rebuildIndex(id, header, dir)
}

// readIndexFile parses index.jsonl into a segIndex, or returns nil when the
// file is absent or unusable. A torn final line is dropped like a torn log
// line; malformed content before it makes the file unusable.
func (s *Store) readIndexFile(header session.SegmentHeader, dir string) *segIndex {
	data, err := os.ReadFile(filepath.Join(dir, indexFile))
	if err != nil {
		return nil
	}
	x := &segIndex{idx: session.CommitIndex{Through: header.Seed()}, byID: map[ledger.CommitID]int{}}
	off := 0
	for off < len(data) {
		nl := bytes.IndexByte(data[off:], '\n')
		if nl < 0 {
			break // torn tail
		}
		var line indexLine
		if err := json.Unmarshal(data[off:off+nl], &line); err != nil {
			if off+nl+1 == len(data) {
				break // torn write of the final line
			}
			return nil
		}
		if line.Seq != x.idx.Through.Next || line.Start != x.end() || line.End <= line.Start {
			return nil // not a contiguous index of this log
		}
		x.idx.Entries = append(x.idx.Entries, line.IndexEntry)
		x.idx.Through = ledger.Head{Next: line.Seq + 1}
		x.spans = append(x.spans, commitSpan{line.Start, line.End})
		x.byID[line.CommitID] = len(x.spans) - 1
		off += nl + 1
	}
	return x
}

// tailMatches verifies the index's last entry against the commit line it
// points at: an O(1) read that catches a rewritten log of the same length.
func (s *Store) tailMatches(x *segIndex, logPath string) bool {
	n := len(x.spans)
	if n == 0 {
		return true
	}
	data, err := readRange(logPath, x.spans[n-1].start, x.spans[n-1].end)
	if err != nil {
		return false
	}
	commits, _, _, torn, err := parseLog(data, "", "index")
	if err != nil || torn || len(commits) != 1 {
		return false
	}
	last := &x.idx.Entries[n-1]
	c := &commits[0]
	return c.CommitID == last.CommitID && c.Seq == last.Seq
}

// repairIndexTail parses the log past the index's end and appends the whole
// commits found there when they continue the index; it reports false when
// the tail does not continue it. Persisting the repair is best effort.
func (s *Store) repairIndexTail(x *segIndex, logPath string) (bool, error) {
	if !s.tailMatches(x, logPath) {
		return false, nil
	}
	f, err := os.Open(logPath)
	if err != nil {
		return false, err
	}
	defer f.Close()
	if _, err := f.Seek(x.end(), io.SeekStart); err != nil {
		return false, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return false, err
	}
	commits, offsets, _, _, err := parseLog(data, "", "index")
	if err != nil {
		return false, nil //nolint:nilerr // the log is checked by the full rebuild
	}
	base := x.end()
	var lines []byte
	for i := range commits {
		c := &commits[i]
		if c.Seq != x.idx.Through.Next {
			return false, nil
		}
		start, end := base+offsets[i], base+offsets[i+1]
		x.extend(c, start, end)
		lines = appendIndexLine(lines, x.idx.Entries[len(x.idx.Entries)-1], start, end)
	}
	if len(lines) > 0 {
		_ = appendIndexFile(filepath.Join(filepath.Dir(logPath), indexFile), lines)
	}
	return true, nil
}

// rebuildIndex derives the index from the whole log and rewrites index.jsonl.
func (s *Store) rebuildIndex(id session.SegmentID, header session.SegmentHeader, dir string) (*segIndex, error) {
	commits, offsets, _, _, err := readLog(filepath.Join(dir, logFile), "", "index")
	if err != nil {
		return nil, err
	}
	x := &segIndex{idx: session.CommitIndex{Through: header.Seed()}, byID: make(map[ledger.CommitID]int, len(commits))}
	var buf []byte
	for i := range commits {
		x.extend(&commits[i], offsets[i], offsets[i+1])
		buf = appendIndexLine(buf, x.idx.Entries[i], offsets[i], offsets[i+1])
	}
	if err := writeDerivedAtomic(filepath.Join(dir, indexFile), buf); err != nil {
		return nil, segerr("index", id, err.Error())
	}
	return x, nil
}

// appendIndexLine encodes one index line onto buf.
func appendIndexLine(buf []byte, e session.IndexEntry, start, end int64) []byte {
	line, err := json.Marshal(indexLine{IndexEntry: e, Start: start, End: end})
	if err != nil {
		return buf
	}
	buf = append(buf, line...)
	return append(buf, '\n')
}

// appendIndexFile appends derived bytes after the authoritative log is synced.
// Losing any number of index lines on a crash is harmless: loadIndex repairs
// or rebuilds them from the log. Do not add a second durability barrier here.
func appendIndexFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// dropIndex forgets the in-memory index; the next use reloads it from
// index.jsonl and the log.
func (s *Store) dropIndex(id session.SegmentID) { delete(s.index, id) }

// Locate is: membership and Seq from the index alone.
func (s *Store) Locate(ctx context.Context, id session.SegmentID, cid ledger.CommitID) (ledger.CommitSeq, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "locate")
	if err != nil {
		return 0, false, err
	}
	x, err := s.segIndex(id, header, dir)
	if err != nil {
		return 0, false, err
	}
	i, ok := x.byID[cid]
	if !ok {
		return 0, false, nil
	}
	return x.idx.Entries[i].Seq, true, nil
}

// Index is: the segment's CommitIndex and head.
func (s *Store) Index(ctx context.Context, id session.SegmentID) (session.CommitIndex, ledger.Head, error) {
	if err := ctx.Err(); err != nil {
		return session.CommitIndex{}, ledger.Head{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "index")
	if err != nil {
		return session.CommitIndex{}, ledger.Head{}, err
	}
	x, err := s.segIndex(id, header, dir)
	if err != nil {
		return session.CommitIndex{}, ledger.Head{}, err
	}
	return x.idx.Clone(), x.idx.Through, nil
}

// Summarize is the segment index's summary.
func (s *Store) Summarize(ctx context.Context, id session.SegmentID) (session.IndexSummary, ledger.Head, error) {
	if err := ctx.Err(); err != nil {
		return session.IndexSummary{}, ledger.Head{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "index")
	if err != nil {
		return session.IndexSummary{}, ledger.Head{}, err
	}
	x, err := s.segIndex(id, header, dir)
	if err != nil {
		return session.IndexSummary{}, ledger.Head{}, err
	}
	return x.idx.Summary(), x.idx.Through, nil
}

// StreamHead sums the stream's event counts over the segment's index
// entries.
func (s *Store) StreamHead(ctx context.Context, id session.SegmentID, stream ledger.Domain, before ledger.CommitSeq) (ledger.StreamSeq, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "stream_head")
	if err != nil {
		return 0, err
	}
	x, err := s.segIndex(id, header, dir)
	if err != nil {
		return 0, err
	}
	var n ledger.StreamSeq
	for i := range x.idx.Entries {
		if x.idx.Entries[i].Seq >= before {
			break
		}
		for _, sc := range x.idx.Entries[i].Streams {
			if sc.Domain == stream {
				n += ledger.StreamSeq(sc.Events)
			}
		}
	}
	return n, nil
}

// PutIndex accepts the kernel's rebuild by rebuilding from the log itself,
// which is where the byte spans come from, and checks the two agree.
func (s *Store) PutIndex(ctx context.Context, id session.SegmentID, idx session.CommitIndex) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "index")
	if err != nil {
		return err
	}
	s.dropIndex(id)
	x, err := s.segIndex(id, header, dir)
	if err != nil {
		return err
	}
	if x.idx.Through != idx.Through || len(x.idx.Entries) != len(idx.Entries) {
		return segerr("index", id, fmt.Sprintf("rebuilt index covers %+v, kernel expected %+v", x.idx.Through, idx.Through))
	}
	return nil
}

// LookupCommit is: it reads exactly the commit's byte range.
func (s *Store) LookupCommit(ctx context.Context, id session.SegmentID, cid ledger.CommitID) (ledger.Commit, bool, error) {
	if err := ctx.Err(); err != nil {
		return ledger.Commit{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.loadSegment(id, "lookup")
	if err != nil {
		return ledger.Commit{}, false, err
	}
	x, err := s.segIndex(id, header, dir)
	if err != nil {
		return ledger.Commit{}, false, err
	}
	i, ok := x.byID[cid]
	if !ok {
		return ledger.Commit{}, false, nil
	}
	sp := x.spans[i]
	data, err := readRange(filepath.Join(dir, logFile), sp.start, sp.end)
	if err != nil {
		return ledger.Commit{}, false, segerr("lookup", id, err.Error())
	}
	commits, _, _, torn, err := parseLog(data, "", "lookup")
	if err != nil {
		return ledger.Commit{}, false, err
	}
	if torn || len(commits) != 1 || commits[0].CommitID != cid {
		return ledger.Commit{}, false, segerr("lookup", id, "index span does not hold the commit")
	}
	return commits[0], true, nil
}

// commitsFrom returns at most limit own commits (0 = unlimited) and the full
// segment head. Apply the limit before reading or decoding the log, including
// for the one-commit reads used to validate projection checkpoints. The caller
// holds the lock.
func (s *Store) commitsFrom(id session.SegmentID, header session.SegmentHeader, dir string, from ledger.CommitSeq, limit uint32) ([]ledger.Commit, ledger.Head, error) {
	x, err := s.segIndex(id, header, dir)
	if err != nil {
		return nil, ledger.Head{}, err
	}
	n := len(x.spans)
	base := header.Seed().Next
	if n == 0 || from >= x.idx.Through.Next {
		return nil, x.idx.Through, nil
	}
	if from < base {
		from = base
	}
	slot := session.IndexWithin(from-base, n)
	end := n
	if limit > 0 && uint64(limit) < uint64(n-slot) { //nolint:gosec // slot is bounded by n; the difference is nonnegative
		end = slot + int(limit)
	}
	commits, err := readIndexedCommits(id, dir, x, slot, end)
	return commits, x.idx.Through, err
}

// readIndexedCommits reads exactly the indexed range [start, end), without
// reparsing the rest of the log or reloading the index for each stream match.
func readIndexedCommits(id session.SegmentID, dir string, x *segIndex, start, end int) ([]ledger.Commit, error) {
	f, err := os.Open(filepath.Join(dir, logFile))
	if err != nil {
		return nil, segerr("read", id, err.Error())
	}
	defer f.Close()
	return indexedCommitsFromFile(id, f, x, start, end)
}

func indexedCommitsFromFile(id session.SegmentID, f *os.File, x *segIndex, start, end int) ([]ledger.Commit, error) {
	spanStart, spanEnd := x.spans[start].start, x.spans[end-1].end
	data, err := io.ReadAll(io.NewSectionReader(f, spanStart, spanEnd-spanStart))
	if err != nil {
		return nil, segerr("read", id, err.Error())
	}
	commits, _, _, torn, err := parseLog(data, "", "read")
	if err != nil {
		return nil, err
	}
	if torn || len(commits) != end-start {
		return nil, segerr("read", id, "log does not match its index")
	}
	for i := range commits {
		e := &x.idx.Entries[start+i]
		if commits[i].Seq != e.Seq || commits[i].CommitID != e.CommitID {
			return nil, segerr("read", id, "log does not match its index")
		}
	}
	return commits, nil
}

// CutIndex keeps only the first keep lines of the tip segment's index.jsonl
// while log.jsonl stays, so conformance can prove that Open detects a lagging
// or absent index and repairs it; production code never calls
// it. A negative keep removes the file.
func (s *Store) CutIndex(sid session.SessionID, keep int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	header, dir, err := s.tip(sid, "cut_index")
	if err != nil {
		return err
	}
	s.dropIndex(header.ID)
	path := filepath.Join(dir, indexFile)
	if keep < 0 {
		return os.RemoveAll(path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	off := 0
	for i := 0; i < keep && off < len(data); i++ {
		nl := bytes.IndexByte(data[off:], '\n')
		if nl < 0 {
			off = len(data)
			break
		}
		off += nl + 1
	}
	return writeAtomic(path, data[:off])
}
