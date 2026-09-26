package session

import (
	"context"
	"math"
	"time"

	"github.com/felinics/twilight/agentcore/es"
)

// CreateRequest establishes a Session: a root naming a new segment. A
// repeat for an existing SessionID whose resolved parent edge, CausationID
// and Ext match the existing Session is idempotent and returns the existing
// header; any difference is a Conflict. CreatedAtUnixMilli is recorded from
// the first successful Create and does not take part in that judgement. Fork makes the new segment a child of another
// Session's history (SES-FRK-1): that Session must be live in the same
// Store and its ancestry must hold commit Seq; otherwise Create fails and
// writes nothing. The segment's ID is always the kernel's to draw: a caller never
// names a writable node, so no two roots can be made to share one
// (SES-FRK-4).
type CreateRequest struct {
	SessionID          SessionID
	CreatedAtUnixMilli int64
	Fork               *ForkOrigin
	CausationID        es.CausationID
	// Ext are the module extension slots stored as SegmentHeader.Ext
	// (SES-WIR-5): a module records what it needs about the segment's
	// creation under its own key.
	Ext Extensions
}

// ForkOrigin names the point a fork inherits: a Session and a CommitSeq of
// its stitched history. The Ledger resolves it to the segment that
// contributes that commit and records the edge as SegmentHeader.Parent.
type ForkOrigin struct {
	Session SessionID
	Seq     CommitSeq
}

// OpenOptions configures writer ownership (SES-OWN-1). A Handle holds a
// lease: while the lease is live, an Open without Takeover fails with
// ErrOwned; once it has expired (LeaseDuration elapsed since the last Renew)
// an Open supersedes it without Takeover, and an Open with Takeover
// supersedes a live lease as well. Safety rests on Epoch fencing
// (SES-OWN-2) in every case; the lease only decides when a supersession is
// allowed without an operator's say. A zero LeaseDuration never expires,
// which is the setting of a process that cannot crash without releasing.
type OpenOptions struct {
	Takeover bool
	// Owner identifies the opening process in the lease, for diagnostics; it
	// is not an authorization.
	Owner string
	// LeaseDuration is how long the lease is live after Acquire and after
	// each Renew; zero means the lease lives until Release.
	LeaseDuration time.Duration
	// Clock supplies the time the lease is measured against; nil is
	// time.Now. Fixtures inject one to age a lease.
	Clock func() time.Time
}

// Now returns the time by the configured clock.
func (o OpenOptions) Now() time.Time {
	if o.Clock != nil {
		return o.Clock()
	}
	return time.Now()
}

// LeaseUntil is the expiry of a lease taken or renewed at now: zero when
// the lease never expires.
func (o OpenOptions) LeaseUntil(now time.Time) int64 {
	if o.LeaseDuration <= 0 {
		return 0
	}
	return now.Add(o.LeaseDuration).UnixMilli()
}

// Head is the ledger head after the last commit: the next CommitSeq to
// assign. A segment with no commits of its own has head LedgerSeed(header):
// 0 for a root segment, Parent.Seq+1 for a child.
type Head struct {
	Next CommitSeq
}

// Proposal is one atomic append: the batches of a single commit under one
// CommitID. The commit may span several streams; the store lands every batch
// or none (SES-APP-1).
type Proposal struct {
	CommitID CommitID
	Batches  []StreamBatch
}

// Handle is the kernel's ownership handle returned by Store.Open. Append
// carries its Epoch; a Handle whose Epoch has been superseded gets
// ErrOwnershipLost and writes nothing (SES-OWN-2).
type Handle interface {
	SessionID() SessionID
	Epoch() Epoch
	// Lease is the ownership this handle holds: its Epoch, Owner and expiry.
	Lease() Lease
	// Renew extends the lease by LeaseDuration from now (SES-OWN-1); a
	// superseded handle gets ErrOwnershipLost. A handle whose lease never
	// expires renews to no effect.
	Renew(context.Context) error
	Head() Head
	// Append persists one commit atomically and returns it as stored (SES-APP-1).
	// It rejects malformed CommitIDs, duplicate CommitIDs, malformed stream
	// refs and events, and a stale Epoch (SES-APP-3).
	Append(context.Context, Proposal) (Commit, error)
	// Committed reports whether CommitID is already in the ledger. Append must
	// reject a duplicate CommitID (SES-APP-3), so the kernel answers this from
	// the index it already keeps, without touching storage (SES-REP-3).
	Committed(CommitID) bool
	// LookupCommit returns a committed group. It reads it from storage when
	// the handle does not already hold it, so a caller that needs the commit
	// pays for it only on a hit (SES-REP-4).
	LookupCommit(CommitID) (Commit, bool, error)
	// StreamHead reports whether the tip segment holds any event of a
	// logical stream and, if so, the StreamSeq the next one takes. It is
	// answered from the same index Committed uses: which streams this
	// ledger has written is a ledger fact, so a module that must refuse a
	// second creation of a stream asks here instead of remembering every
	// stream it ever closed in a projection. Inherited segments are not
	// counted whatever lineage the stream's domain declared: the index is
	// the tip segment's own (SES-FRK-5).
	StreamHead(StreamRef) (StreamSeq, bool)
	Close(context.Context) error
}

// Limit32 converts a count or sequence to the uint32 a page limit takes,
// saturating instead of wrapping.
func Limit32(n uint64) uint32 {
	if n > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(n)
}

// IndexWithin converts a commit offset to an index into a slice of n items,
// clamping to n so a caller can slice from it without checking bounds.
func IndexWithin(off CommitSeq, n int) int {
	if n <= 0 || uint64(off) >= uint64(n) {
		return max(n, 0)
	}
	return int(off) //nolint:gosec // off < n <= MaxInt
}

// AtLimit reports whether a page of n items has reached limit; a zero limit
// is no limit.
func AtLimit(n int, limit uint32) bool {
	return limit > 0 && n >= 0 && uint64(n) >= uint64(limit)
}

// StreamSeq is the position of an event inside its logical stream: the first
// event a stream ever receives is 0. It is derived from the ledger by
// counting a stream's events in CommitSeq order and is a read optimization
// only; CommitSeq is the canonical order.
type StreamSeq uint64

// CommitReadRequest reads whole commits from From (inclusive). Limit counts
// commits and never truncates inside one (SES-REP-1). On a fork the sequence
// read is the inherited parent prefix followed by the Session's own commits
// (SES-FRK-2).
type CommitReadRequest struct {
	SessionID SessionID
	From      CommitSeq
	Limit     uint32 // 0 = unlimited
}

// CommitPage is the result of one ReadCommits. Header is the tip segment's;
// Head is the ledger head at read time; HasMore reports whether commits
// beyond the returned ones exist.
type CommitPage struct {
	Header  SegmentHeader
	Commits []Commit
	Head    Head
	HasMore bool
}

// StreamReadRequest reads the events of one logical stream in CommitSeq
// order. Lineage selects how the read crosses the tip segment's edges
// (SES-FRK-5) and is the mode the stream's owning module declared for its
// domain; a zero Lineage is ErrInvalid. From counts events within the
// stream as the chosen lineage sees it, starting at 0 for the first event.
type StreamReadRequest struct {
	SessionID SessionID
	Stream    StreamRef
	Lineage   StreamLineage
	From      StreamSeq
	Limit     uint32 // 0 = unlimited
}

// StreamPage is the result of one ReadStream. Head is the ledger head at
// read time; HasMore reports whether events beyond the returned ones exist.
type StreamPage struct {
	Header  SegmentHeader
	Stream  StreamRef
	Events  []Event
	Head    Head
	HasMore bool
}

// CollectReport is what one Collect reclaimed (SES-GC-2): the segments it
// removed entirely and, for segments some root still reaches, the new
// Head.Next after their unreachable suffix was dropped.
type CollectReport struct {
	Removed   []SegmentID
	Truncated map[SegmentID]CommitSeq
	// Dropped lists, per truncated segment, the CommitIDs of the commits the
	// truncation removed, so the layer that owns their retention claims can
	// release them (SES-GC-3); a removed segment's claims are released by
	// segment.
	Dropped map[SegmentID][]CommitID
}

// Store is the kernel port (SES 4 to 6, 8, 9). A Session is a root into the
// lineage tree: it names the segment it appends to, and reads the stitched
// history of that segment's ancestry. Delete drops the root; Collect
// reclaims the nodes no root reaches.
type Store interface {
	// Create establishes a root and its tip segment and returns the tip's
	// header.
	Create(context.Context, CreateRequest) (SegmentHeader, error)
	// Header returns the header of the Session's tip segment.
	Header(context.Context, SessionID) (SegmentHeader, error)
	// Record returns the Session's root.
	Record(context.Context, SessionID) (SessionRecord, error)
	// LeaseOf returns the Session's current writer Lease, if any (SES-OWN-5):
	// a read for controllers and routers, which changes nothing and may be
	// stale by the time it is acted on.
	LeaseOf(context.Context, SessionID) (Lease, bool, error)
	// ListLeases returns the Lease of every held Session, expired ones
	// included (SES-OWN-5).
	ListLeases(context.Context) ([]Lease, error)
	// ExpiredLeases returns held Leases expired at or before
	// beforeUnixMilli, soonest first, at most limit (0 for all)
	// (SES-OWN-5).
	ExpiredLeases(ctx context.Context, beforeUnixMilli int64, limit int) ([]Lease, error)
	Open(context.Context, SessionID, OpenOptions) (Handle, error)
	ReadCommits(context.Context, CommitReadRequest) (CommitPage, error)
	ReadStream(context.Context, StreamReadRequest) (StreamPage, error)
	// Delete drops the Session's root (SES-GC-1): the Session is no longer
	// found, opened, read or forked; its SessionID is free again at once.
	// The segments it reached stay lineage nodes for as long as another
	// root reaches them. An owned Session is ErrOwned.
	Delete(context.Context, SessionID) error
	// Collect reclaims every node and suffix no root reaches (SES-GC-2). It
	// is idempotent and safe while Sessions are open: nothing a root reaches
	// is touched.
	Collect(context.Context) (CollectReport, error)
}

// HasTypePrefix reports whether typ matches one of the prefixes (empty list
// matches everything).
func HasTypePrefix(typ EventType, prefixes []EventType) bool {
	if len(prefixes) == 0 {
		return true
	}
	for _, p := range prefixes {
		if len(typ) >= len(p) && typ[:len(p)] == p {
			return true
		}
	}
	return false
}
