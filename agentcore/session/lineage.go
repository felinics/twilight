package session

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// The Session lineage is a tree (agent-session.md section 8, SES-LIN-1):
// immutable commit segments are its nodes, a segment's single parent anchor
// is an edge, and a Session is a root that names the segment it appends to.
// A segment has at most one parent, so the segments under one root segment
// form a tree and all of them a forest; no operation gives an existing
// segment a second parent. These are the domain types; the Ledger implements
// every operation over them and the adapters store them.

// SegmentID identifies one commit segment independently of any Session: 128
// random bits the kernel draws when the segment is created (SES-WIR-4).
type SegmentID string

// LedgerRef names one position in the lineage tree: a commit of a segment, by
// its place in the stitched sequence. As SegmentHeader.Parent it is the edge
// from a child segment to the last commit it inherits: the child's own
// commits are numbered from Seq+1 and readers see the prefix [0, Seq]
// followed by them. History is append-only, so (Segment, Seq) names one
// commit for good and the edge is a stable reference (SES-FRK-1).
type LedgerRef struct {
	Segment SegmentID `json:"segment"`
	Seq     CommitSeq `json:"seq"`
}

// Segment is a node: an immutable creation record whose own commits start
// at LedgerSeed(Header). Header.Parent is the edge to the parent segment; a
// root segment has none. ID equals Header.ID.
type Segment struct {
	ID     SegmentID
	Header SegmentHeader
}

// NewSegmentID returns a fresh segment identity: 128 random bits, hex encoded.
func NewSegmentID() (SegmentID, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("session: segment id: %w", err)
	}
	return SegmentID(hex.EncodeToString(b[:])), nil
}

// Parent returns the edge to the parent segment, or nil for a root.
func (s Segment) Parent() *LedgerRef {
	if s.Header.Parent == nil {
		return nil
	}
	edge := *s.Header.Parent
	return &edge
}

// Seed is the head of the segment while it holds no commits of its own.
func (s Segment) Seed() Head { return LedgerSeed(s.Header) }

// SessionRecord is a root: a Session's identity, the segment it appends to
// (its tip) and the Session's own metadata. Dropping the record is deleting
// the Session; the tip stays a lineage node for as long as any root
// reaches it. Two roots never share a tip (SES-FRK-4): a fork gets a new
// child segment, so writers of different Sessions never append to one node.
type SessionRecord struct {
	ID                 SessionID `json:"sessionId"`
	Tip                SegmentID `json:"tip"`
	CreatedAtUnixMilli int64     `json:"createdAtUnixMilli"`
}

// Lease is writer ownership of one Session root (SES-OWN-1/2): the adapter
// fences every append with its Epoch. Owner names the holder; UntilUnixMilli
// is when the lease stops being live (zero: never), after which another
// Open may supersede it without Takeover.
type Lease struct {
	Session        SessionID
	Epoch          Epoch
	Owner          string
	UntilUnixMilli int64
}

// LedgerStore is the adapter port for nodes: independent, append-only
// segments. It knows nothing of Sessions, forks or reachability.
type LedgerStore interface {
	// Segment returns a node; ErrNotFound when absent.
	Segment(context.Context, SegmentID) (Segment, error)
	// ListSegments returns every node.
	ListSegments(context.Context) ([]SegmentID, error)
	// ReadSegment returns the segment's own commits from CommitSeq from
	// (absolute), at most limit (0 = unlimited), its head, and whether more
	// own commits follow. A torn tail is never returned.
	ReadSegment(ctx context.Context, id SegmentID, from CommitSeq, limit uint32) ([]Commit, Head, bool, error)
	// Locate reports whether the segment holds CommitID as its own commit,
	// and at which Seq, from the segment's CommitIndex alone (SES-REP-3/5);
	// LookupCommit reads the commit (SES-REP-4).
	Locate(context.Context, SegmentID, CommitID) (CommitSeq, bool, error)
	LookupCommit(context.Context, SegmentID, CommitID) (Commit, bool, error)
	// StreamHead returns the number of events the segment's own commits
	// with Seq below before wrote to one logical stream, from the segment's
	// CommitIndex alone (SES-REP-3): the StreamSeq the stream's next event
	// takes as of that head, 0 when none of those commits wrote to it.
	StreamHead(ctx context.Context, id SegmentID, stream StreamRef, before CommitSeq) (StreamSeq, error)
	// Summarize returns the summary of the segment's CommitIndex as the
	// adapter keeps it, and the segment's current head (SES-REP-5): what
	// Open checks the index by, without the entries. After a crash the
	// index may lag the commits, which the kernel detects with
	// IndexSummary.Valid and repairs through PutIndex.
	Summarize(context.Context, SegmentID) (IndexSummary, Head, error)
	// Index returns the segment's whole CommitIndex and its head
	// (SES-REP-5); Collect reads it to name the commits a truncation drops.
	Index(context.Context, SegmentID) (CommitIndex, Head, error)
	// PutIndex replaces the segment's CommitIndex with one the kernel rebuilt
	// from the commits.
	PutIndex(context.Context, SegmentID, CommitIndex) error
	// Append persists a commit whose Seq is the segment head, under a Lease
	// the adapter checks atomically with the write: the Lease must be
	// current for its Session and that Session's Tip must be the segment
	// (SES-OWN-2). A superseded Lease gets ErrOwnershipLost and writes
	// nothing. The adapter only ever inserts: no operation of this port
	// rewrites or removes a commit a root still reaches (SES-APP-5).
	Append(context.Context, Lease, SegmentID, Commit) error
	// TruncateSegment drops the segment's own commits after through and
	// returns the new head; RemoveSegment deletes the node.
	TruncateSegment(ctx context.Context, id SegmentID, through CommitSeq) (Head, error)
	RemoveSegment(context.Context, SegmentID) error
}

// SessionStore is the adapter port for roots: Session records and their
// writer ownership.
type SessionStore interface {
	// Record returns a root; ErrNotFound when absent.
	Record(context.Context, SessionID) (SessionRecord, error)
	// ListRecords returns every root.
	ListRecords(context.Context) ([]SessionRecord, error)
	// Acquire takes writer ownership of a root (SES-OWN-1): ErrOwned while a
	// Lease is live unless Takeover, which supersedes it with the next Epoch.
	// Repair of a torn tail in the root's segment happens here.
	Acquire(context.Context, SessionID, OpenOptions) (Lease, error)
	// Renew moves the Lease's expiry to untilUnixMilli when the Lease is
	// still the root's current one (SES-OWN-1); a superseded Lease is
	// ErrOwnershipLost and nothing changes.
	Renew(context.Context, Lease, int64) error
	// Release ends a Lease; a superseded Lease is a no-op.
	Release(context.Context, Lease) error
	// LeaseOf returns the root's current Lease (SES-OWN-5): ok is false
	// when the root has no holder (never opened, or released); a root that
	// does not exist is ErrNotFound. A read: nothing changes, and an expired
	// Lease is returned as it is for the caller to judge against its clock.
	LeaseOf(context.Context, SessionID) (Lease, bool, error)
	// ListLeases returns the Lease of every root that has a holder
	// (SES-OWN-5), expired ones included.
	ListLeases(context.Context) ([]Lease, error)
	// ExpiredLeases returns held Leases whose expiry is at or before
	// beforeUnixMilli, soonest expired first, at most limit of them (0 for
	// all); never-expiring Leases are never returned (SES-OWN-5). It is the
	// read a replica pool recovers dead owners' Sessions by (APP-ACT-3),
	// and an adapter indexes it.
	ExpiredLeases(ctx context.Context, beforeUnixMilli int64, limit int) ([]Lease, error)
	// DeleteRecord drops a root (SES-GC-1); ErrOwned while a Lease is live.
	DeleteRecord(context.Context, SessionID) error
}

// Backend is what an adapter implements: both ports, sharing one
// consistency domain so Append can check a Lease atomically, plus the one
// write that spans them.
type Backend interface {
	LedgerStore
	SessionStore
	// CreateSession persists a new node and the root that names it as one
	// durable step (SES-FRK-1): never a root without its segment, never a
	// segment a Collect could see without its root. Both must be new:
	// ErrConflict when the SessionID or the SegmentID exists, so no two
	// roots ever name one writable tip (SES-FRK-4).
	CreateSession(context.Context, Segment, SessionRecord) error
}

// Ancestry is the unique path of a Session through the lineage tree: its
// segments from the root down to the tip, each with the range of the
// stitched sequence it contributes. Every read, lookup and reachability
// question is answered on this value; nothing walks the store recursively.
type Ancestry struct {
	// Segments are ordered root first; the last is the tip a Session
	// appends to.
	Segments []AncestrySegment
}

// AncestrySegment is one node on the path with the stitched positions it
// contributes: its own commits from Seed up to and including Through (the
// next segment's anchor), or up to its head for the tip.
type AncestrySegment struct {
	Segment Segment
	// From is the first stitched CommitSeq the segment contributes.
	From CommitSeq
	// Through is the last stitched CommitSeq it contributes; the tip
	// contributes through its head, reported as ^CommitSeq(0).
	Through CommitSeq
}

// Tip is the segment the Session appends to.
func (a *Ancestry) Tip() Segment { return a.Segments[len(a.Segments)-1].Segment }

// Header is the tip's creation record: the Session's public header.
func (a *Ancestry) Header() SegmentHeader { return a.Tip().Header }

// Owner returns the segment that contributes the commit at seq.
func (a *Ancestry) Owner(seq CommitSeq) (AncestrySegment, bool) {
	for i := range a.Segments {
		if s := &a.Segments[i]; seq >= s.From && seq <= s.Through {
			return *s, true
		}
	}
	return AncestrySegment{}, false
}

// Load resolves the Ancestry of the segment tip: it follows parent edges to
// the root through the LedgerStore, loading each node once.
func LoadAncestry(ctx context.Context, store LedgerStore, tip SegmentID) (*Ancestry, error) {
	var chain []Segment
	seen := map[SegmentID]bool{}
	for id := tip; ; {
		if seen[id] {
			return nil, &Error{Code: ErrCorrupt, Operation: "ancestry", Detail: fmt.Sprintf("segment %s is its own ancestor", id)}
		}
		seen[id] = true
		seg, err := store.Segment(ctx, id)
		if err != nil {
			return nil, err
		}
		chain = append(chain, seg)
		parent := seg.Parent()
		if parent == nil {
			break
		}
		id = parent.Segment
	}
	a := &Ancestry{Segments: make([]AncestrySegment, len(chain))}
	for i := range chain {
		seg := chain[len(chain)-1-i] // root first
		s := AncestrySegment{Segment: seg, From: seg.Seed().Next, Through: ^CommitSeq(0)}
		if i+1 < len(chain) {
			s.Through = chain[len(chain)-2-i].Parent().Seq
		}
		a.Segments[i] = s
	}
	return a, nil
}

// Read returns the stitched commits of the Ancestry from from (inclusive),
// at most limit (0 = unlimited), the tip's head and whether more follow. It
// iterates the explicit path; each segment is read once for its range.
func (a *Ancestry) Read(ctx context.Context, store LedgerStore, from CommitSeq, limit uint32) ([]Commit, Head, bool, error) {
	var out []Commit
	var head Head
	tip := len(a.Segments) - 1
	for i := range a.Segments {
		s := &a.Segments[i]
		start := from
		if start < s.From {
			start = s.From
		}
		if i < tip && start > s.Through {
			continue
		}
		var want uint32
		if limit > 0 {
			remaining := int(limit) - len(out)
			if remaining <= 0 {
				head, err := a.tipHead(ctx, store)
				return out, head, true, err
			}
			want = Limit32(uint64(remaining))
			if i < tip && CommitSeq(want) > s.Through-start+1 {
				want = Limit32(uint64(s.Through - start + 1))
			}
		} else if i < tip {
			want = Limit32(uint64(s.Through - start + 1))
		}
		commits, segHead, more, err := store.ReadSegment(ctx, s.Segment.ID, start, want)
		if err != nil {
			return nil, Head{}, false, err
		}
		if i == tip {
			head = segHead
			out = append(out, commits...)
			return out, head, more, nil
		}
		for _, c := range commits {
			if c.Seq > s.Through {
				break
			}
			out = append(out, c)
		}
		if AtLimit(len(out), limit) {
			// Anything after the last returned commit exists by construction:
			// either the rest of this segment's range or the segments below.
			last := out[len(out)-1].Seq
			head, err := a.tipHead(ctx, store)
			return out, head, last < s.Through || i < tip, err
		}
	}
	return out, head, false, nil
}

// tipHead reads the tip's head without reading commits.
func (a *Ancestry) tipHead(ctx context.Context, store LedgerStore) (Head, error) {
	_, head, _, err := store.ReadSegment(ctx, a.Tip().ID, ^CommitSeq(0), 1)
	return head, err
}

// Contains reports whether id is a commit of the Ancestry: one of the tip's
// own commits or an inherited one within its anchor range (SES-FRK-3). It
// consults the segments' indexes only (SES-REP-5).
func (a *Ancestry) Contains(ctx context.Context, store LedgerStore, id CommitID) (bool, error) {
	return locateIn(ctx, store, a.Segments, id)
}

// ContainsInherited is Contains over the Ancestry without its tip.
func (a *Ancestry) ContainsInherited(ctx context.Context, store LedgerStore, id CommitID) (bool, error) {
	return locateIn(ctx, store, a.Segments[:len(a.Segments)-1], id)
}

// Lookup reads a commit of the Ancestry by CommitID.
func (a *Ancestry) Lookup(ctx context.Context, store LedgerStore, id CommitID) (Commit, bool, error) {
	return lookupIn(ctx, store, a.Segments, id)
}

// LookupInherited reads an inherited commit by CommitID: the Ancestry without
// its tip. The inherited prefix is immutable, so reading it from storage at
// any time gives the same answer.
func (a *Ancestry) LookupInherited(ctx context.Context, store LedgerStore, id CommitID) (Commit, bool, error) {
	return lookupIn(ctx, store, a.Segments[:len(a.Segments)-1], id)
}

func lookupIn(ctx context.Context, store LedgerStore, segments []AncestrySegment, id CommitID) (Commit, bool, error) {
	for i := len(segments) - 1; i >= 0; i-- {
		s := segments[i]
		c, ok, err := store.LookupCommit(ctx, s.Segment.ID, id)
		if err != nil {
			return Commit{}, false, err
		}
		if ok && c.Seq >= s.From && c.Seq <= s.Through {
			return c, true, nil
		}
	}
	return Commit{}, false, nil
}

// locateIn reports whether id is a commit of segments within their anchor
// ranges, from the segments' indexes alone.
func locateIn(ctx context.Context, store LedgerStore, segments []AncestrySegment, id CommitID) (bool, error) {
	for i := len(segments) - 1; i >= 0; i-- {
		s := &segments[i]
		seq, ok, err := store.Locate(ctx, s.Segment.ID, id)
		if err != nil {
			return false, err
		}
		if ok && seq >= s.From && seq <= s.Through {
			return true, nil
		}
	}
	return false, nil
}

// Reachable computes, from every node and the roots that are live, the last
// stitched CommitSeq each node must keep (SES-GC-2): a node that is some
// root's tip keeps everything; a node reached only through edges keeps up to
// the largest anchor Seq any reaching edge carries. Nodes absent from the
// result are unreachable. Edges are followed transitively, so a node
// referenced only by unreachable nodes is unreachable.
func Reachable(nodes map[SegmentID]Segment, roots []SessionRecord) map[SegmentID]CommitSeq {
	const all = ^CommitSeq(0)
	need := make(map[SegmentID]CommitSeq, len(nodes))
	for _, r := range roots {
		id := r.Tip
		seg, ok := nodes[id]
		if !ok {
			continue
		}
		need[id] = all
		for edge := seg.Parent(); edge != nil; {
			if cur, ok := need[edge.Segment]; !ok || edge.Seq > cur {
				need[edge.Segment] = edge.Seq
			}
			parent, ok := nodes[edge.Segment]
			if !ok {
				break
			}
			edge = parent.Parent()
		}
	}
	return need
}
