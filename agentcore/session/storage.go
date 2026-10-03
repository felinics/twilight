package session

import (
	"context"
	"github.com/felinics/twilight/agentcore/ledger"
	"time"
)

// SegmentStore is the adapter port for nodes: independent, append-only
// segments and their indexes. It knows nothing of Sessions, forks or
// reachability.
type SegmentStore interface {
	// Segment returns a node; ErrNotFound when absent.
	Segment(context.Context, SegmentID) (Segment, error)
	// ReadSegment returns the segment's own commits from CommitSeq from
	// (absolute), at most limit (0 = unlimited), its head, and whether more
	// own commits follow. A torn tail is never returned.
	ReadSegment(ctx context.Context, id SegmentID, from ledger.CommitSeq, limit uint32) ([]ledger.Commit, ledger.Head, bool, error)
	// ReadSegmentStream returns, from the segment's own commits with Seq at
	// or past from, those that carry a batch of stream, in Seq order, at
	// most limit of them (0 = unlimited), and whether more follow
	// . The adapter narrows by the CommitIndex's stream counts
	ReadSegmentStream(ctx context.Context, id SegmentID, stream ledger.Domain, from ledger.CommitSeq, limit uint32) ([]ledger.Commit, bool, error)
	// Locate reports whether the segment holds CommitID as its own commit,
	// and at which Seq, from the CommitIndex alone;
	// LookupCommit reads the commit.
	Locate(context.Context, SegmentID, ledger.CommitID) (ledger.CommitSeq, bool, error)
	LookupCommit(context.Context, SegmentID, ledger.CommitID) (ledger.Commit, bool, error)
	// StreamHead returns the number of events the segment's own commits
	// with Seq below before wrote to stream, from the CommitIndex alone
	StreamHead(ctx context.Context, id SegmentID, stream ledger.Domain, before ledger.CommitSeq) (ledger.StreamSeq, error)
	// Summarize returns the summary of the segment's CommitIndex as the
	// adapter keeps it, and the segment's head: what Open
	// checks the index by, without the entries.
	Summarize(context.Context, SegmentID) (IndexSummary, ledger.Head, error)
	// Index returns the segment's whole CommitIndex and its head
	Index(context.Context, SegmentID) (CommitIndex, ledger.Head, error)
	// PutIndex replaces the segment's CommitIndex with one the kernel
	// rebuilt from the commits.
	PutIndex(context.Context, SegmentID, CommitIndex) error
	// Append persists a commit whose Seq is the segment head, checking the
	// Lease atomically with the write: the Lease must be current for its
	// Session and that Session's Tip must be the segment. A
	// superseded Lease gets ErrOwnershipLost and writes nothing. The port
	// only inserts: nothing here rewrites or removes a commit a root still
	// reaches.
	Append(context.Context, Lease, SegmentID, ledger.Commit) error
}

// RootStore is the adapter port for roots: a Session's record and its
// writer ownership.
type RootStore interface {
	// Record returns a root; ErrNotFound when absent.
	Record(context.Context, SessionID) (SessionRecord, error)
	// Acquire takes writer ownership of a root: ErrOwned while
	// a Lease is live unless Takeover, which supersedes it with the next
	// Epoch. Lease expiry is judged by the adapter's clock.
	// Repair of a torn tail in the root's segment happens here.
	Acquire(context.Context, SessionID, OpenOptions) (Lease, error)
	// Renew moves the Lease's expiry to the adapter's now plus duration
	// while the Lease is still the root's current one; a
	// superseded Lease is ErrOwnershipLost and nothing changes. A
	// non-positive duration is a lease that never expires.
	Renew(context.Context, Lease, time.Duration) (int64, error)
	// Release ends a Lease; a superseded Lease is a no-op.
	Release(context.Context, Lease) error
	// LeaseOf returns the root's current Lease; ok is false
	// when the root has no holder. A read: nothing changes, and an expired
	// Lease is returned as it is.
	LeaseOf(context.Context, SessionID) (Lease, bool, error)
	// ListLeases returns the Lease of every root that has a holder,
	// expired ones included.
	ListLeases(context.Context) ([]Lease, error)
	// ExpiredLeases returns held Leases expired by the adapter's clock
	// , soonest first, at most limit of them (0 for all);
	// never-expiring Leases are never returned. An adapter
	// indexes it.
	ExpiredLeases(ctx context.Context, limit int) ([]Lease, error)
}

// MaintenanceStore is the adapter port for the operations that change the
// set of nodes, path spans and roots: enumeration, node creation
// with its root, the greatest span still naming a segment, truncation and
// removal, root deletion. They share one consistency domain with the other
// two ports.
type MaintenanceStore interface {
	// ListSegments returns every node.
	ListSegments(context.Context) ([]SegmentID, error)
	// ListRecords returns every root.
	ListRecords(context.Context) ([]SessionRecord, error)
	// CreateSession persists a new segment, the root's path spans and the
	// root, the root last; the whole lands or nothing does.
	// Both the SessionID and the SegmentID must be new: ErrConflict when
	// either exists, ErrDeleted when the SessionID was deleted.
	// When the segment's header names a Parent, the parent segment must
	// exist at the moment of the write and the adapter must keep it from
	// being removed while the edge stands: a vanished parent is
	// ErrNotFound. Each closed span's Through commit and the parent edge's
	// commit must also still exist; a missing one is ErrNotFound and
	// nothing is written. The span insert holds the same per-segment lock
	// as TruncateSegment.
	CreateSession(context.Context, Segment, SessionRecord) error
	// SpanBound is the greatest end among the path spans that still name
	// the segment; ok is false when none do. An open end retains every
	// commit the segment has. DropOrphanSpans deletes path spans whose
	// session is not a live root.
	SpanBound(ctx context.Context, id SegmentID) (Bound, bool, error)
	DropOrphanSpans(context.Context) error
	// TruncateSegment drops the segment's own commits after through and
	// returns the head afterwards plus the CommitIDs it actually removed.
	// Under the same per-segment lock CreateSession holds, it re-reads the
	// greatest span still naming the segment: an open span deletes nothing
	// and a greater Through raises the cut. Nothing removed yields the
	// unchanged head and an empty list.
	TruncateSegment(ctx context.Context, id SegmentID, through ledger.CommitSeq) (ledger.Head, []ledger.CommitID, error)
	// RemoveSegment deletes the node when nothing references it, atomically
	// with the check: a root whose Tip is the segment, a segment
	// whose Parent edge names it, or a path span still naming it makes the
	// removal ErrReferenced and nothing is removed. The adapter enforces
	// the check with its own consistency (a foreign key, a check under the
	// store lock).
	RemoveSegment(context.Context, SegmentID) error
	// DeleteRecord marks a root deleted and drops its path spans in the
	// same write, returning the record as it was, including the
	// path. ErrOwned while a Lease is live, and then nothing is written. A
	// nonempty path that does not validate is ErrCorrupt and nothing is
	// written. Record and ListRecords do not return deleted roots.
	DeleteRecord(context.Context, SessionID) (SessionRecord, error)
}

// Storage is what an adapter implements: the three ports over one
// consistency domain.
type Storage interface {
	SegmentStore
	RootStore
	MaintenanceStore
}
