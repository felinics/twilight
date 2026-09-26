// Package store is the Executor's authority over one execution: the commit
// ledger of an effect's attempts (RUN-EXE-3, RUN-EXE-9) and the lease that
// fences its Worker. It speaks the vocabulary of the Session kernel — a
// ledger of commits, each with a Seq and a CommitID, carrying events with a
// type and a canonical payload — so that what is true
// of a Session ledger is true here: the ledger is the only fact authority,
// ExecutionState is its fold, a replayed command is recognised by its
// CommitID, and a fenced writer's commit is refused by its Epoch. The Run
// never reads this ledger; the two authorities meet only at AssignmentKey.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/executor/protocol"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/run/effect"
)

var (
	// ErrAssignmentConflict: the ledger was accepted for a different
	// Assignment of the same key (RUN-EXE-3).
	ErrAssignmentConflict = errors.New("executor/store: assignment conflict")
	// ErrLeaseLost: the lease the commit was made under is no longer the
	// key's lease — another Worker holds a later Epoch, or it expired.
	ErrLeaseLost = errors.New("executor/store: execution lease lost")
	// The commit rules are the kernel's (agentcore/ledger).
	ErrStateConflict  = ledger.ErrStateConflict
	ErrConflict       = ledger.ErrConflict
	ErrAlreadyApplied = ledger.ErrAlreadyApplied
)

// The commit vocabulary is the kernel's (agentcore/ledger).
type (
	CommitSeq = ledger.CommitSeq
	CommitID  = ledger.CommitID
	Epoch     = ledger.Epoch
	EventType = ledger.EventType
	Event     = ledger.Event
	Commit    = ledger.Commit
	Head      = ledger.Head
)

// NewEvent renders payload as the event's canonical JSON.
func NewEvent(typ EventType, recordedAtUnixMilli int64, payload any) (Event, error) {
	return ledger.NewEvent(typ, recordedAtUnixMilli, payload)
}

const (
	// EventExecutionAccepted: the Assignment was accepted into this ledger,
	// before anything started (RUN-EXE-3). The first event of a ledger that
	// was opened by a Dispatch.
	EventExecutionAccepted EventType = "execution_accepted"
	// EventExecutionAborted: the key was closed before any acceptance
	// (RUN-EXE-16). The first and only event of a ledger opened by Abort:
	// it and execution_accepted contend for Seq 0, so a ledger holds one or
	// the other, never both.
	EventExecutionAborted EventType = "execution_aborted"
	// EventExecutionBound: the attempt's physical binding, Provider and Ref,
	// was chosen (RUN-EXE-9, RUN-EXE-10).
	EventExecutionBound EventType = "execution_bound"
	// EventExecutionClaimed: a Worker took the key under a new Epoch. Lease
	// renewals are not events; the lease row is the fence's authority.
	EventExecutionClaimed EventType = "execution_claimed"
	// EventExecutionStarted: Backend.Start is about to be called; the state
	// is Dispatching, which may already have crossed the effect boundary.
	EventExecutionStarted EventType = "execution_started"
	// EventExecutionRunning: the backend accepted the start.
	EventExecutionRunning EventType = "execution_running"
	// EventCancelRequested: cancellation was asked of the backend.
	EventCancelRequested EventType = "cancel_requested"
	// EventExecutionRestarted: the attempt moved to a new Ref, the previous
	// one joining the audit trail (RUN-EXE-9, RUN-EXE-11).
	EventExecutionRestarted EventType = "execution_restarted"
	// EventExecutionSettled: the terminal state and the Outcome.
	EventExecutionSettled EventType = "execution_settled"
	// EventOutcomeAcknowledged: the Owner reported the Outcome settled as a
	// Session fact; the Executor no longer serves it (RUN-EXE-13).
	EventOutcomeAcknowledged EventType = "outcome_acknowledged"
)

// Lease is a Worker's fenced hold on one execution (RUN-EXE-6). The store
// is its authority: Acquire creates it under a new Epoch, Renew extends it,
// and Append under it is refused once the key has a later Epoch.
type Lease struct {
	Key            effect.AssignmentKey
	Owner          string
	Epoch          Epoch
	UntilUnixMilli int64
}

// IsZero reports the absence of a lease: an unfenced Append.
func (l Lease) IsZero() bool { return l.Owner == "" && l.Epoch == 0 }

// ExecutionRef is the Executor's physical binding of the current attempt it
// makes for one effect (RUN-EXE-9): the provider (backend) the execution was
// handed to and that backend's opaque handle. It never leaves the Executor:
// Agent Core addresses executions by AssignmentKey, and the Run records the
// EffectID alone.
type ExecutionRef struct {
	Provider string `json:"provider"`
	Ref      string `json:"ref"`
}

// --- payloads ---

// Accepted is the payload of execution_accepted.
type Accepted struct {
	Assignment effect.Assignment `json:"assignment"`
}

// Aborted is the payload of execution_aborted.
type Aborted struct {
	Reason string `json:"reason,omitempty"`
}

// Bound is the payload of execution_bound.
type Bound struct {
	Ref ExecutionRef `json:"ref"`
}

// Claimed is the payload of execution_claimed.
type Claimed struct {
	Owner string `json:"owner"`
	Epoch Epoch  `json:"epoch"`
}

// Restarted is the payload of execution_restarted: the Ref the attempt
// leaves and the one it continues under, on the same Provider.
type Restarted struct {
	Superseded ExecutionRef `json:"superseded"`
	Ref        string       `json:"ref"`
}

// Settled is the payload of execution_settled.
type Settled struct {
	State   effect.ExecutionStatus   `json:"state"`
	Outcome protocol.OutcomeEnvelope `json:"outcome"`
}

// --- identity ---

// DeriveCommitID names a command on one key. Commands that happen once per
// ledger (acceptance, settlement, acknowledgement) take no discriminator;
// those that recur (a claim per Epoch, a restart per generation) take one,
// so their identity is the command and the occasion, never the wall clock.
func DeriveCommitID(key effect.AssignmentKey, command, discriminator string) CommitID {
	d, err := es.DigestCanonical(struct {
		Key           effect.AssignmentKey `json:"scope"`
		Command       string               `json:"command"`
		Discriminator string               `json:"discriminator,omitempty"`
	}{key, command, discriminator})
	if err != nil {
		panic(err) // AssignmentKey is three strings; canonical encoding cannot fail
	}
	return CommitID(d)
}

// AcceptCommitID, SettleCommitID and AcknowledgeCommitID name the three
// commands that happen once per ledger: opening it, ending the execution,
// and the Owner's acknowledgement. A settlement by the lease holder and one
// by a controller's Dispose share SettleCommitID, so the second reads the
// first's Outcome instead of writing a second ending.
func AcceptCommitID(key effect.AssignmentKey) CommitID { return DeriveCommitID(key, "accept", "") }

// AbortCommitID names the one tombstone of a key (RUN-EXE-16). It differs
// from AcceptCommitID, so the two commands are told apart by Seq, not by
// replay: whichever reaches Seq 0 first stands and the other is ErrConflict.
func AbortCommitID(key effect.AssignmentKey) CommitID  { return DeriveCommitID(key, "abort", "") }
func SettleCommitID(key effect.AssignmentKey) CommitID { return DeriveCommitID(key, "settle", "") }
func AcknowledgeCommitID(key effect.AssignmentKey) CommitID {
	return DeriveCommitID(key, "acknowledge", "")
}

// --- fold ---

// ExecutionState is the fold of one execution's ledger: the facts alone,
// the execution plane's side of the effect. Who holds the key's lease is
// not a fact of the execution and lives in the lease row (Execution.Lease).
type ExecutionState struct {
	Assignment   effect.Assignment `json:"assignment"`
	ExecutionRef ExecutionRef      `json:"executionRef"`
	// Superseded lists the ExecutionRefs of the earlier attempts made for
	// this effect, oldest first (RUN-EXE-9).
	Superseded []ExecutionRef            `json:"superseded,omitempty"`
	State      effect.ExecutionStatus    `json:"state"`
	Outcome    *protocol.OutcomeEnvelope `json:"outcome,omitempty"`
	// Acknowledged records that the Owner settled the Outcome into its
	// Session (RUN-EXE-13): the Outcome is no longer served and the record
	// may be reclaimed by retention.
	Acknowledged bool `json:"acknowledged,omitempty"`
}

// Terminal reports whether the execution has settled, or was aborted before
// anything was accepted.
func (s *ExecutionState) Terminal() bool { return protocol.StatusTerminal(s.State) }

// Aborted reports the tombstone: the ledger was opened by Abort and holds
// no Assignment.
func (s *ExecutionState) Aborted() bool { return s.State == effect.ExecutionAborted }

// Execution is what Load returns: the fold of the ledger and the key's
// current lease, read from their two sources in one transaction. Lease is
// zero when the key was never claimed.
type Execution struct {
	ExecutionState
	Lease Lease
}

// LegalTransition is the execution state machine (RUN-EXE-3).
func LegalTransition(from, to effect.ExecutionStatus) bool {
	if to == effect.ExecutionCancelRequested {
		return from == effect.ExecutionAccepted || from == effect.ExecutionDispatching || from == effect.ExecutionRunning
	}
	switch from {
	case effect.ExecutionAccepted:
		return to == effect.ExecutionDispatching
	case effect.ExecutionDispatching:
		return to == effect.ExecutionRunning
	case effect.ExecutionRunning:
		// A restart after the backend proved the execution missing, or after
		// a retryable failure (RUN-EXE-9, RUN-EXE-11).
		return to == effect.ExecutionDispatching
	default:
		return false
	}
}

// Fenced reports whether an event may only be committed under the key's
// lease. Acceptance and the abort tombstone precede any lease; settlement
// by Dispose and the Owner's acknowledgement come from outside the Worker
// (RUN-EXE-6, RUN-EXE-13, RUN-EXE-16). Everything else is the lease
// holder's.
func Fenced(typ EventType) bool {
	switch typ {
	case EventExecutionAccepted, EventExecutionAborted, EventExecutionBound, EventExecutionSettled, EventOutcomeAcknowledged:
		return false
	default:
		return true
	}
}

// Fold applies one commit to the state; an event that is not legal from the
// current state is ErrStateConflict. The store folds every commit before it
// is appended, so the ledger never holds an illegal step.
func Fold(state ExecutionState, c *Commit) (ExecutionState, error) { //nolint:gocritic // hugeParam: a fold takes and returns the state by value
	for i := range c.Events {
		e := &c.Events[i]
		var err error
		state, err = apply(state, e)
		if err != nil {
			return ExecutionState{}, fmt.Errorf("%w: commit %d event %d (%s): %w", ErrStateConflict, c.Seq, i, e.Type, err)
		}
	}
	return state, nil
}

func apply(s ExecutionState, e *Event) (ExecutionState, error) { //nolint:gocritic,gocyclo // hugeParam: value fold; gocyclo: one case per event type
	switch e.Type {
	case EventExecutionAccepted:
		if s.State != "" {
			return s, fmt.Errorf("accepted into a ledger already %s", s.State)
		}
		var p Accepted
		if err := e.Decode(&p); err != nil {
			return s, err
		}
		s.Assignment, s.State = p.Assignment, effect.ExecutionAccepted
	case EventExecutionAborted:
		if s.State != "" {
			return s, fmt.Errorf("aborted a ledger already %s", s.State)
		}
		var p Aborted
		if err := e.Decode(&p); err != nil {
			return s, err
		}
		s.State = effect.ExecutionAborted
	case EventExecutionBound:
		if s.State == "" || s.Terminal() {
			return s, errors.New("bound outside an accepted, unsettled execution")
		}
		if s.ExecutionRef.Provider != "" {
			return s, errors.New("bound twice; a new attempt is a restart")
		}
		var p Bound
		if err := e.Decode(&p); err != nil {
			return s, err
		}
		s.ExecutionRef = p.Ref
	case EventExecutionClaimed:
		// The claim is provenance: who took the key and under which Epoch.
		// The fence itself is the lease row, which Acquire advances in the
		// same transaction that records this fact.
		if s.State == "" || s.Terminal() {
			return s, errors.New("claimed outside an accepted, unsettled execution")
		}
	case EventExecutionStarted:
		if !LegalTransition(s.State, effect.ExecutionDispatching) {
			return s, fmt.Errorf("start from %s", s.State)
		}
		s.State = effect.ExecutionDispatching
	case EventExecutionRunning:
		if !LegalTransition(s.State, effect.ExecutionRunning) {
			return s, fmt.Errorf("running from %s", s.State)
		}
		s.State = effect.ExecutionRunning
	case EventCancelRequested:
		if !LegalTransition(s.State, effect.ExecutionCancelRequested) {
			return s, fmt.Errorf("cancel from %s", s.State)
		}
		s.State = effect.ExecutionCancelRequested
	case EventExecutionRestarted:
		if s.State == "" || s.Terminal() {
			return s, errors.New("restart outside an unsettled execution")
		}
		var p Restarted
		if err := e.Decode(&p); err != nil {
			return s, err
		}
		if p.Superseded != s.ExecutionRef {
			return s, fmt.Errorf("restart supersedes %+v, current is %+v", p.Superseded, s.ExecutionRef)
		}
		s.Superseded = append(append([]ExecutionRef(nil), s.Superseded...), s.ExecutionRef)
		s.ExecutionRef = ExecutionRef{Provider: s.ExecutionRef.Provider, Ref: p.Ref}
	case EventExecutionSettled:
		if s.State == "" || s.Terminal() {
			return s, errors.New("settled outside an unsettled execution")
		}
		var p Settled
		if err := e.Decode(&p); err != nil {
			return s, err
		}
		if !protocol.StatusTerminal(p.State) {
			return s, fmt.Errorf("settled to non-terminal %s", p.State)
		}
		out := p.Outcome
		s.State, s.Outcome = p.State, &out
	case EventOutcomeAcknowledged:
		if !s.Terminal() || s.Aborted() {
			return s, fmt.Errorf("acknowledged in state %s", s.State)
		}
		if s.Acknowledged {
			return s, errors.New("acknowledged twice")
		}
		s.Acknowledged = true
	default:
		return s, fmt.Errorf("unknown event type %q", e.Type)
	}
	return s, nil
}

// Store is the ledger and lease authority of executions, keyed by
// AssignmentKey. Every write is one transaction; the store is the fence for
// any number of Workers over the same file (RUN-EXE-6).
type Store interface {
	// Load folds the key's ledger and reads its lease row in one
	// transaction; ok is false for a key with no ledger, which is a proven
	// absence (RUN-EXE-3).
	Load(context.Context, effect.AssignmentKey) (Execution, Head, bool, error)
	// Read returns the key's commits from Seq from, in order, and the Head.
	Read(context.Context, effect.AssignmentKey, CommitSeq) ([]Commit, Head, error)
	// Append commits c to the key's ledger. c.Seq must be Head.Next
	// (ErrConflict). A commit whose CommitID already exists is
	// ErrAlreadyApplied and nothing is written: the CommitID names the
	// operation, so the Worker that must tell two Assignments apart reads
	// the ledger back (ErrAssignmentConflict). A zero lease is an unfenced
	// append and may carry only events Fenced reports false for; otherwise
	// the lease must be the key's current, unexpired lease (ErrLeaseLost).
	// The commit is folded before it is written (ErrStateConflict).
	Append(context.Context, Lease, effect.AssignmentKey, Commit) error
	// Acquire takes the key's lease for owner under a new Epoch and records
	// execution_claimed in the same transaction. ok is false while another
	// owner's lease is live or the execution is terminal; a key without a
	// ledger is effect.ErrExecutionNotFound.
	Acquire(context.Context, effect.AssignmentKey, string, time.Duration) (Lease, bool, error)
	// Renew moves the lease's expiry when it is still the key's current
	// lease (same owner and Epoch, execution unsettled); otherwise
	// ErrLeaseLost. Expiry alone does not end ownership.
	Renew(context.Context, Lease, time.Duration) error
	// LeaseOf returns the key's current lease row; ok is false when the key
	// was never claimed. A key without a ledger is effect.ErrExecutionNotFound.
	LeaseOf(context.Context, effect.AssignmentKey) (Lease, bool, error)
	// ListOwned returns the keys whose lease row names owner: what a
	// restarted incarnation resumes. No other listing is part of the data
	// plane (RUN-EXE-6).
	ListOwned(context.Context, string) ([]effect.AssignmentKey, error)
}
