// Package ledger is the commit vocabulary every event-sourced authority of
// the agent core shares below the Session kernel: an execution's ledger
// (agentcore/executor/store) and an effect process's ledger
// (agentcore/process) are built from it, in the Session's own terms. A
// ledger is a sequence of commits; a commit has a Seq, a CommitID naming the
// operation, and events with a type and a canonical payload. Who may write
// is the store's concern, judged from the writer's lease or epoch at Append
// and never recorded on the commit: provenance, where a domain needs it, is
// a fact of its own. Three rules follow the Session kernel (SES-APP-4): a
// commit's Seq is the ledger's Head.Next or the append conflicts; a CommitID
// seen before is the same operation and is not written again; a commit is
// folded before it is written, so a ledger never holds an illegal step. The
// store only inserts: a commit is never rewritten or removed.
package ledger

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/jsonstable"
)

var (
	// ErrConflict: the commit's Seq is not the ledger's Head.Next.
	ErrConflict = errors.New("ledger: commit sequence conflict")
	// ErrAlreadyApplied: a commit with the same CommitID exists.
	ErrAlreadyApplied = errors.New("ledger: commit already applied")
	// ErrStateConflict: the commit's events are not legal from the fold.
	ErrStateConflict = errors.New("ledger: state conflict")
	// ErrFenced: the writer's epoch is behind the ledger's; it was superseded.
	ErrFenced = errors.New("ledger: writer fenced by a later epoch")
)

// CommitSeq is the position of a commit in one ledger.
type CommitSeq uint64

// CommitID names the operation a commit records; replaying it is recognised.
// How an operation is named is the writing domain's rule, never the
// ledger's: it only enforces that one CommitID lands once.
type CommitID string

// Epoch is a writer's fencing epoch, judged at Append.
type Epoch uint64

// EventType names a fact.
type EventType string

// Event is one fact, the shape of a Session event.
type Event struct {
	Type                EventType        `json:"type"`
	RecordedAtUnixMilli int64            `json:"recordedAtUnixMilli"`
	Payload             jsonstable.Value `json:"payload"`
}

// NewEvent renders payload as the event's canonical JSON; nil is {}.
func NewEvent(typ EventType, recordedAtUnixMilli int64, payload any) (Event, error) {
	if payload == nil {
		payload = struct{}{}
	}
	v, err := jsonstable.FromValue(payload)
	if err != nil {
		return Event{}, fmt.Errorf("ledger: %s payload: %w", typ, err)
	}
	return Event{Type: typ, RecordedAtUnixMilli: recordedAtUnixMilli, Payload: v}, nil
}

// Decode decodes the payload into dst.
func (e *Event) Decode(dst any) error { return e.Payload.Decode(dst) }

// Commit is one atomic step of a ledger.
type Commit struct {
	Seq      CommitSeq `json:"seq"`
	CommitID CommitID  `json:"commitId"`
	Events   []Event   `json:"events"`
}

// Head is a ledger's tip: the next Seq to assign.
type Head struct {
	Next CommitSeq `json:"next"`
}
