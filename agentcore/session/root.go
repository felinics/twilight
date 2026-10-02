package session

import (
	"github.com/felinics/twilight/agentcore/ledger"
	"time"
)

// SessionRecord is a root: a Session's identity, the segment it appends to
// (its tip), the path of spans it reads, and the Session's own metadata.
// Tip is the last span's segment. An empty Path is a root written before
// paths were stored; readers assemble one by walking parent edges. Deleting
// the record tombstones the Session. Two roots never share a tip
// (SES-FRK-4): a fork gets a new child segment, so writers of different
// Sessions never append to one node.
type SessionRecord struct {
	ID                 SessionID `json:"sessionId"`
	Tip                SegmentID `json:"tip"`
	CreatedAtUnixMilli int64     `json:"createdAtUnixMilli"`
	Path               Path      `json:"path,omitempty"`
}

// Lease is writer ownership of one Session root (SES-OWN-1/2): the adapter
// fences every append with its Epoch. Owner names the holder; UntilUnixMilli
// is when the lease stops being live (zero: never), after which another
// Open may supersede it without Takeover.
type Lease struct {
	Session        SessionID
	Epoch          ledger.Epoch
	Owner          string
	UntilUnixMilli int64
}

// OpenOptions configures writer ownership (SES-OWN-1). A Handle holds a
// lease: while the lease is live, an Open without Takeover fails with
// ErrOwned; once it has expired (LeaseDuration elapsed since the last Renew)
// an Open supersedes it without Takeover, and an Open with Takeover
// supersedes a live lease as well. Safety rests on Epoch fencing
// (SES-OWN-2) in every case; the lease only decides when a supersession is
// allowed without an operator's say. A zero LeaseDuration never expires,
// which is the setting of a process that cannot crash without releasing.
//
// Time is the store's (SES-OWN-6): the adapter judges expiry and stamps
// the new expiry by its own clock, so replicas over one database agree
// whatever their process clocks say. Fixtures age a lease through the
// adapter's clock, not through these options.
type OpenOptions struct {
	Takeover bool
	// Owner identifies the opening process in the lease, for diagnostics; it
	// is not an authorization.
	Owner string
	// LeaseDuration is how long the lease is live after Acquire and after
	// each Renew; zero means the lease lives until Release.
	LeaseDuration time.Duration
}
