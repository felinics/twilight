// Package redispatch is the dispatch ledger of one effect: the process
// manager's own decisions. Whether an effect is outstanding is the Run's
// state; whether a destination holds an attempt is answered by attaching
// there; neither is repeated here. What only the process manager knows, and
// what must survive its crash, is which redispatch it decided to make,
// whether that dispatch reached the destination, and whether it gave up
// (RUN-EXE-15). Each record is written before the act it decides, so a
// decision made and a dispatch sent cannot drift apart.
package redispatch

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/run/effect"
)

const (
	// EventDispatchPlanned: the process manager decided to hand the effect
	// to the Executor again; Attempt numbers the redispatches from 1. It is
	// written before the Dispatch and stays pending until dispatched.
	EventDispatchPlanned ledger.EventType = "dispatch_planned"
	// EventDispatched: the Dispatch of the planned attempt reached the
	// Executor (accepted, or already held under the same key). Only now may
	// the next attempt be planned.
	EventDispatched ledger.EventType = "dispatched"
	// EventGivenUp: the process manager stopped redispatching; the reason is
	// recorded and the Run disposes the effect (RUN-CMT-7).
	EventGivenUp ledger.EventType = "given_up"
)

// Planned is the payload of dispatch_planned.
type Planned struct {
	Attempt int `json:"attempt"`
}

// Dispatched is the payload of dispatched.
type Dispatched struct {
	Attempt int `json:"attempt"`
}

// GivenUp is the payload of given_up.
type GivenUp struct {
	Reason string `json:"reason"`
}

// --- identity ---

// deriveCommitID names a command on one key; the naming rule is this
// domain's, the ledger only enforces uniqueness.
func deriveCommitID(key effect.AssignmentKey, command, discriminator string) ledger.CommitID {
	d, err := jsonstable.DigestCanonical(struct {
		Key           effect.AssignmentKey `json:"scope"`
		Command       string               `json:"command"`
		Discriminator string               `json:"discriminator,omitempty"`
	}{key, command, discriminator})
	if err != nil {
		panic(err) // AssignmentKey is three strings; canonical encoding cannot fail
	}
	return ledger.CommitID(d)
}

// PlannedCommitID names the decision to make the n-th redispatch.
func PlannedCommitID(key effect.AssignmentKey, attempt int) ledger.CommitID {
	return deriveCommitID(key, "process/dispatch_planned", fmt.Sprint(attempt))
}

// DispatchedCommitID names the completion of the n-th redispatch.
func DispatchedCommitID(key effect.AssignmentKey, attempt int) ledger.CommitID {
	return deriveCommitID(key, "process/dispatched", fmt.Sprint(attempt))
}

// GivenUpCommitID names the one decision to stop.
func GivenUpCommitID(key effect.AssignmentKey) ledger.CommitID {
	return deriveCommitID(key, "process/given_up", "")
}

// --- fold ---

// State is the fold of one effect's dispatch ledger. Planned is the number
// of redispatches decided, Dispatched the number that reached the Executor;
// Planned is Dispatched or Dispatched+1, and the difference is the one
// attempt still owed to the Executor.
type State struct {
	Key        effect.AssignmentKey `json:"key"`
	Planned    int                  `json:"planned"`
	Dispatched int                  `json:"dispatched"`
	GivenUp    bool                 `json:"givenUp,omitempty"`
	Reason     string               `json:"reason,omitempty"`
}

// Pending reports the planned attempt whose Dispatch has not been recorded,
// or 0 when none is owed.
func (s State) Pending() int {
	if s.Planned > s.Dispatched {
		return s.Planned
	}
	return 0
}

// Fold applies one commit; an event not legal from the state is
// ledger.ErrStateConflict.
func Fold(state State, c *ledger.Commit) (State, error) { //nolint:gocritic // hugeParam: a fold takes and returns the state by value
	k := 0
	for b := range c.Batches {
		for i := range c.Batches[b].Events {
			e := &c.Batches[b].Events[i]
			var err error
			state, err = apply(state, e)
			if err != nil {
				return State{}, fmt.Errorf("%w: commit %d event %d (%s): %w", ledger.ErrStateConflict, c.Seq, k, e.Type, err)
			}
			k++
		}
	}
	return state, nil
}

func apply(s State, e *ledger.Event) (State, error) { //nolint:gocritic // hugeParam: value fold
	if s.GivenUp {
		return s, errors.New("decision after given_up")
	}
	switch e.Type {
	case EventDispatchPlanned:
		var p Planned
		if err := e.Decode(&p); err != nil {
			return s, err
		}
		if s.Planned != s.Dispatched {
			return s, fmt.Errorf("attempt %d planned while %d is pending", p.Attempt, s.Planned)
		}
		if p.Attempt != s.Planned+1 {
			return s, fmt.Errorf("attempt %d planned after %d", p.Attempt, s.Planned)
		}
		s.Planned = p.Attempt
	case EventDispatched:
		var p Dispatched
		if err := e.Decode(&p); err != nil {
			return s, err
		}
		if p.Attempt != s.Planned || s.Dispatched != s.Planned-1 {
			return s, fmt.Errorf("attempt %d dispatched with %d planned and %d dispatched", p.Attempt, s.Planned, s.Dispatched)
		}
		s.Dispatched = p.Attempt
	case EventGivenUp:
		var p GivenUp
		if err := e.Decode(&p); err != nil {
			return s, err
		}
		s.GivenUp, s.Reason = true, p.Reason
	default:
		return s, fmt.Errorf("unknown event type %q", e.Type)
	}
	return s, nil
}

// --- store ---

// Store is the dispatch ledger authority, keyed by AssignmentKey.
type Store interface {
	// Load folds the key's ledger; ok is false for a key with no ledger.
	Load(ctx context.Context, key effect.AssignmentKey) (State, ledger.Head, bool, error)
	// Read returns the key's commits from Seq from, in order, and the Head.
	Read(ctx context.Context, key effect.AssignmentKey, from ledger.CommitSeq) ([]ledger.Commit, ledger.Head, error)
	// Append commits c under the kernel's rules (agentcore/ledger): Seq is
	// Head.Next or ErrConflict; a known CommitID is ErrAlreadyApplied and
	// nothing is written; the commit is folded before it is written
	// (ErrStateConflict). epoch is the Session Epoch of the owner whose
	// reconciler writes: one below the highest the ledger has seen is
	// ErrFenced, so a superseded owner's reconciler writes nothing.
	Append(ctx context.Context, epoch ledger.Epoch, key effect.AssignmentKey, c ledger.Commit) error
}

// Plan returns the attempt the process manager is to make now: the planned
// attempt still owed to the Executor, or the next one, recorded before the
// Dispatch it announces (RUN-EXE-15). A crash between the record and the
// Dispatch leaves the attempt pending, and the next reconciliation makes
// the same attempt instead of a new one.
func Plan(ctx context.Context, s Store, epoch ledger.Epoch, key effect.AssignmentKey, now int64) (int, error) {
	state, head, _, err := s.Load(ctx, key)
	if err != nil {
		return 0, err
	}
	if n := state.Pending(); n != 0 {
		return n, nil
	}
	n := state.Planned + 1
	if err := append1(ctx, s, epoch, key, head, PlannedCommitID(key, n), EventDispatchPlanned, Planned{Attempt: n}, now); err != nil {
		return 0, err
	}
	return n, nil
}

// MarkDispatched records that the Dispatch of the planned attempt reached
// the Executor; the attempt is no longer owed.
func MarkDispatched(ctx context.Context, s Store, epoch ledger.Epoch, key effect.AssignmentKey, attempt int, now int64) error {
	state, head, _, err := s.Load(ctx, key)
	if err != nil {
		return err
	}
	if state.Dispatched >= attempt {
		return nil
	}
	return append1(ctx, s, epoch, key, head, DispatchedCommitID(key, attempt), EventDispatched, Dispatched{Attempt: attempt}, now)
}

// GiveUp records that the process manager stops redispatching key.
func GiveUp(ctx context.Context, s Store, epoch ledger.Epoch, key effect.AssignmentKey, reason string, now int64) error {
	state, head, _, err := s.Load(ctx, key)
	if err != nil {
		return err
	}
	if state.GivenUp {
		return nil
	}
	return append1(ctx, s, epoch, key, head, GivenUpCommitID(key), EventGivenUp, GivenUp{Reason: reason}, now)
}

func append1(ctx context.Context, s Store, epoch ledger.Epoch, key effect.AssignmentKey, head ledger.Head, id ledger.CommitID, typ ledger.EventType, payload any, now int64) error {
	ev, err := ledger.NewEvent(typ, now, payload)
	if err != nil {
		return err
	}
	err = s.Append(ctx, epoch, key, ledger.Commit{Seq: head.Next, CommitID: id, Batches: []ledger.EventBatch{{Events: []ledger.Event{ev}}}})
	if err == nil || errors.Is(err, ledger.ErrAlreadyApplied) {
		return nil
	}
	return err
}
