package session

import (
	"errors"
	"fmt"
	"strings"

	"github.com/felinics/twilight/agentcore/jsonstable"
)

// CommitSeq is the position of one Commit in the ledger and the canonical
// total order of the authority. Per-stream local positions are read
// optimizations derived from the ledger, never a second ordering.
type CommitSeq uint64

// StreamRef names the logical stream one batch belongs to: a Domain and,
// for a keyed stream, the ID of the aggregate within it (a singleton stream
// has an empty ID). Domains belong to Session modules: which domains exist,
// whether a domain is keyed and how its streams cross a segment edge are the
// owning module's declarations (EXT-STR-1). The kernel fixes the shape here,
// the order and atomicity of commits across streams, and offers both lineage
// read modes (StreamLineage); it names no domain of its own.
type StreamRef struct {
	Domain string `json:"domain"`
	ID     string `json:"id,omitempty"`
}

// String renders the stream for diagnostics and indexes.
func (r StreamRef) String() string {
	if r.ID == "" {
		return r.Domain
	}
	return r.Domain + "/" + r.ID
}

// StreamLineage is how a stream read crosses segment edges (SES-FRK-5). A
// fork inherits the commits of its
// ancestry; a stream's owning module declares which of the two histories its
// streams are, and a read names that mode. The kernel applies the mode it is
// given and does not know which one a domain declared.
type StreamLineage string

const (
	// LineageSession reads the stream as the Session's semantic history: the
	// inherited prefix stitched before the tip segment's own commits, so a
	// fork or a new tip continues the stream where its ancestry left it.
	LineageSession StreamLineage = "session"
	// LineageSegment reads the stream as execution history of the segment
	// that wrote it: the tip segment's own commits only, so a fork or a new
	// tip starts the stream empty.
	LineageSegment StreamLineage = "segment"
)

// ValidateStreamLineage checks that a read names one of the two modes.
func ValidateStreamLineage(l StreamLineage) error {
	switch l {
	case LineageSession, LineageSegment:
		return nil
	case "":
		return errors.New("stream lineage is empty")
	default:
		return fmt.Errorf("unknown stream lineage %q", l)
	}
}

// Event is one committed payload. Unlike the v1 row it carries no transaction
// metadata: canonical order comes from CommitSeq plus the event's position
// inside its batch.
type Event struct {
	Type                EventType        `json:"type"`
	RecordedAtUnixMilli int64            `json:"recordedAtUnixMilli"`
	Payload             jsonstable.Value `json:"payload"`
}

// Position is the ledger position of one event: the commit it landed in and
// its index among that commit's events in batch order. Positions order every
// event of a Session totally, so a projection that needs to order what it
// derives records the position of the event that produced it instead of
// keeping a counter of its own.
type Position struct {
	Commit CommitSeq `json:"commit"`
	Index  uint32    `json:"index"`
}

// Less reports whether p precedes q in the ledger.
func (p Position) Less(q Position) bool {
	if p.Commit != q.Commit {
		return p.Commit < q.Commit
	}
	return p.Index < q.Index
}

// StreamBatch is the ordered slice of one commit that belongs to one stream.
type StreamBatch struct {
	Stream StreamRef `json:"stream"`
	Events []Event   `json:"events"`
}

// Commit is one atomic unit of the ledger. One Append persists exactly one
// Commit; the Commit may span several streams, and the store either lands
// every batch or none (SES-APP-1). Seq is its position, CommitID the
// identity of the operation that produced it (SES-APP-4); the store never
// rewrites or removes a commit, so the two name it for good.
type Commit struct {
	Seq      CommitSeq     `json:"seq"`
	CommitID CommitID      `json:"commitId"`
	Batches  []StreamBatch `json:"batches"`
}

// ValidateStreamRef checks the shape of a batch's stream attribution: a
// non-empty domain without the separator String uses, and an ID that is
// empty or a valid identity. Which domains a module owns, whether a domain
// is keyed and which ID an event binds to are Module Framework checks
// (EXT-STR-1), not the kernel's.
func ValidateStreamRef(r StreamRef) error {
	if err := validIdentity("stream domain", r.Domain); err != nil {
		return err
	}
	if strings.Contains(r.Domain, "/") {
		return fmt.Errorf("stream domain %q contains %q", r.Domain, "/")
	}
	if r.ID == "" {
		return nil
	}
	return validIdentity("stream ID", r.ID)
}

// ValidateEvent checks one event before it is stored.
func ValidateEvent(e *Event) error {
	return validateEventShape(e.Type, e.Payload)
}

// ValidateBatches checks the shape of one proposed commit before it is stored:
// non-empty batches, unique streams within the commit, valid attribution and
// canonical payloads. Duplicate CommitIDs and epoch fencing are store duties.
func ValidateBatches(batches []StreamBatch) error {
	if len(batches) == 0 {
		return errors.New("commit without batches")
	}
	seen := make(map[StreamRef]struct{}, len(batches))
	for i := range batches {
		b := &batches[i]
		if err := ValidateStreamRef(b.Stream); err != nil {
			return fmt.Errorf("batch %d: %w", i, err)
		}
		if _, dup := seen[b.Stream]; dup {
			return fmt.Errorf("batch %d: stream %s appears twice in one commit", i, b.Stream)
		}
		seen[b.Stream] = struct{}{}
		if len(b.Events) == 0 {
			return fmt.Errorf("batch %d: no events", i)
		}
		for j := range b.Events {
			if err := ValidateEvent(&b.Events[j]); err != nil {
				return fmt.Errorf("batch %d event %d: %w", i, j, err)
			}
		}
	}
	return nil
}

// ValidateHeader checks the shape of a segment's creation record: a
// non-empty ID, a well-formed edge and a well-formed Ext. Whether the
// version is one the Ledger serves is the Ledger's check.
func ValidateHeader(h SegmentHeader) error {
	if err := validIdentity("segment ID", string(h.ID)); err != nil {
		return newError(ErrInvalid, "header", "", err.Error())
	}
	if err := ValidateEdge(h.Parent); err != nil {
		return newError(ErrInvalid, "header", "", err.Error())
	}
	if err := ValidateExtensions(h.Ext); err != nil {
		return newError(ErrInvalid, "header", "", err.Error())
	}
	return nil
}

// ValidateCommit checks the shape of a commit before it is stored: a valid
// CommitID and well-formed batches. Seq and duplicate CommitIDs are the
// store's checks (SES-APP-3).
func ValidateCommit(c *Commit) error {
	if err := validIdentity("CommitID", string(c.CommitID)); err != nil {
		return err
	}
	return ValidateBatches(c.Batches)
}

// ValidateEdge checks the shape of a parent edge: nil is a root segment;
// otherwise it names a parent segment. Whether the parent holds the commit
// is the Ledger's check at Create (SES-FRK-1).
func ValidateEdge(edge *LedgerRef) error {
	if edge == nil {
		return nil
	}
	return validIdentity("Parent.Segment", string(edge.Segment))
}

// LedgerSeed is the head of a segment that holds no commits of its own: a
// root segment starts at 0, a child continues its parent's numbering at
// Parent.Seq+1 (SES-FRK-2).
func LedgerSeed(h SegmentHeader) Head {
	if h.Parent != nil {
		return Head{Next: h.Parent.Seq + 1}
	}
	return Head{}
}
