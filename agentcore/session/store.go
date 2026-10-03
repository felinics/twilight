package session

import (
	"context"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
)

// CreateRequest establishes a Session: a root naming a new segment. A
// repeat for an existing SessionID whose resolved parent edge, CausationID
// and Ext match the existing Session is idempotent and returns the existing
// header; any difference is a Conflict. CreatedAtUnixMilli is recorded from
// the first successful Create and does not take part in that judgement. Fork makes the new segment a child of another
// Session's history: that Session must be live in the same
// Store and its path must hold commit Seq; otherwise Create fails and
// writes nothing. The segment's ID is always the kernel's to draw: a caller never
// names a writable node, so no two roots can be made to share one
type CreateRequest struct {
	SessionID          SessionID
	CreatedAtUnixMilli int64
	Fork               *ForkOrigin
	CausationID        ledger.CausationID
	// Ext are the module extension slots stored as SegmentHeader.Ext
	// a module records what it needs about the segment's
	// creation under its own key.
	Ext module.Extensions
}

// ForkOrigin names the point a fork inherits: a Session and a CommitSeq of
// its stitched history. The Ledger resolves it to the segment that
// contributes that commit and records the edge as SegmentHeader.Parent.
type ForkOrigin struct {
	Session SessionID
	Seq     ledger.CommitSeq
}

// CommitReadRequest reads whole commits from From (inclusive). Limit counts
// commits and never truncates inside one. On a fork the sequence
// read is the inherited parent prefix followed by the Session's own commits
type CommitReadRequest struct {
	SessionID SessionID
	From      ledger.CommitSeq
	Limit     uint32 // 0 = unlimited
}

// CommitPage is the result of one ReadCommits. Header is the tip segment's;
// Head is the ledger head at read time; HasMore reports whether commits
// beyond the returned ones exist.
type CommitPage struct {
	Header  SegmentHeader
	Commits []ledger.Commit
	Head    ledger.Head
	HasMore bool
}

// StreamReadRequest reads the events of one logical stream in CommitSeq
// order. Lineage selects how the read crosses the tip segment's edges
//
//	and is the mode the stream's owning module declared for its
//
// domain; a zero Lineage is ErrInvalid. From counts events within the
// stream as the chosen lineage sees it, starting at 0 for the first event.
type StreamReadRequest struct {
	SessionID   SessionID
	Domain      ledger.Domain
	Inheritance module.Inheritance
	From        ledger.StreamSeq
	Limit       uint32 // 0 = unlimited
}

// StreamPage is the result of one ReadStream. Head is the ledger head at
// read time; HasMore reports whether events beyond the returned ones exist.
type StreamPage struct {
	Header  SegmentHeader
	Domain  ledger.Domain
	Events  []ledger.Event
	Head    ledger.Head
	HasMore bool
}

// CollectReport is what one Delete or Collect reclaimed: the
// segments it removed entirely and, for segments some session still covers,
// the new Head.Next after their unneeded suffix was dropped.
type CollectReport struct {
	Removed   []SegmentID
	Truncated map[SegmentID]ledger.CommitSeq
	// Dropped lists, per truncated segment, the CommitIDs of the commits the
	// truncation removed, so the layer that owns their retention claims can
	// release them; a removed segment's claims are released by
	// segment.
	Dropped map[SegmentID][]ledger.CommitID
}

// Store is the kernel port. A Session is a root into the
// lineage tree: it names the segment it appends to, and reads the stitched
// history of its path. Store is one Session's face;
// LeaseDirectory reads leases across Sessions; Maintenance changes the set
// of roots and reclaims nodes. session.Ledger implements all three.
type Store interface {
	// Create establishes a root and its tip segment and returns the tip's
	// header.
	Create(context.Context, CreateRequest) (SegmentHeader, error)
	// Header returns the header of the Session's tip segment.
	Header(context.Context, SessionID) (SegmentHeader, error)
	// Record returns the Session's root.
	Record(context.Context, SessionID) (SessionRecord, error)
	Open(context.Context, SessionID, OpenOptions) (Handle, error)
	ReadCommits(context.Context, CommitReadRequest) (CommitPage, error)
	ReadStream(context.Context, StreamReadRequest) (StreamPage, error)
}

// LeaseDirectory is the fleet's read of writer leases: what a
// controller, a gateway or an owner pool's activation scan consults. Every
// method is a read and changes nothing.
type LeaseDirectory interface {
	// LeaseOf returns the Session's current writer Lease, if any:
	// a read for controllers and routers, which changes nothing and may be
	// stale by the time it is acted on.
	LeaseOf(context.Context, SessionID) (Lease, bool, error)
	// ListLeases returns the Lease of every held Session, expired ones
	// included.
	ListLeases(context.Context) ([]Lease, error)
	// ExpiredLeases returns held Leases expired by the store's clock,
	// soonest first, at most limit (0 for all).
	ExpiredLeases(ctx context.Context, limit int) ([]Lease, error)
}

// Maintenance changes the set of roots and reclaims nodes.
type Maintenance interface {
	// Delete tombstones the root, drops its path spans, and reclaims along
	// that path. A segment with no remaining span is removed.
	// A closed maximum truncates the segment to that commit. A prefix
	// another live session still names stays. The Session is no longer
	// found, opened, read or forked, and its SessionID is never reused (a
	// Create under it is ErrDeleted). An owned Session is ErrOwned and
	// nothing is reclaimed. The report lists the segments and commits this
	// call removed.
	Delete(context.Context, SessionID) (CollectReport, error)
	// Collect reclaims from the spans that still name each segment
	// it truncates to the greatest remaining end and removes
	// segments no path names. It is idempotent and safe while Sessions are
	// open. An open tip stays open, so the commits under it stay.
	Collect(context.Context) (CollectReport, error)
}

// Stores is every face of a Session store at once: what an adapter's
// session.Ledger provides and what an owner process, which opens Sessions,
// scans leases and collects, requires.
type Stores interface {
	Store
	LeaseDirectory
	Maintenance
}

// Handle is the kernel's ownership handle returned by Store.Open. Append
// carries its Epoch; a Handle whose Epoch has been superseded gets
// ErrOwnershipLost and writes nothing.
type Handle interface {
	SessionID() SessionID
	Epoch() ledger.Epoch
	// Lease is the ownership this handle holds: its Epoch, Owner and expiry.
	Lease() Lease
	// Renew extends the lease by LeaseDuration from now; a
	// superseded handle gets ErrOwnershipLost. A handle whose lease never
	// expires renews to no effect.
	Renew(context.Context) error
	Head() ledger.Head
	// Header is the tip segment's creation record. It does not change for
	// the life of the handle: a Session's tip segment is fixed at Create.
	Header() SegmentHeader
	// Append persists one commit atomically and returns it as stored.
	// It rejects malformed CommitIDs, duplicate CommitIDs, malformed stream
	// refs and events, and a stale Epoch.
	Append(context.Context, ledger.Proposal) (ledger.Commit, error)
	// Committed reports whether CommitID is already in the ledger. Append must
	// reject a duplicate CommitID, so the kernel answers this from
	// the index it already keeps. A failed handle, and a failed
	// index read, return the error instead of a false negative.
	Committed(ledger.CommitID) (bool, error)
	// LookupCommit returns a committed group. It reads it from storage when
	// the handle does not already hold it, so a caller that needs the commit
	// pays for it only on a hit.
	LookupCommit(ledger.CommitID) (ledger.Commit, bool, error)
	// StreamHead reports whether the tip segment holds any event of a
	// logical stream and, if so, the StreamSeq the next one takes. It is
	// answered from the same index Committed uses: which streams this
	// ledger has written is a ledger fact, so a module that must refuse a
	// second creation of a stream asks here instead of remembering every
	// stream it ever closed in a projection. Inherited segments are not
	// counted whatever lineage the stream's domain declared: the index is
	// the tip segment's own.
	StreamHead(ledger.Domain) (ledger.StreamSeq, bool)
	Close(context.Context) error
}
