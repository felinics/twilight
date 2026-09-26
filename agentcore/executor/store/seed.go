package store

import (
	"fmt"

	"github.com/felinics/twilight/agentcore/executor/protocol"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// SeedCommits plans the ledger an execution goes through to reach state:
// one commit per event, folded as it is planned so a shape Fold rejects is
// never produced, every event recorded at time at. The commit identities
// are the ones a live execution leaves (AcceptCommitID, AbortCommitID), so
// a Dispatch replayed against a seeded ledger is already applied. It is
// the shared half of every adapter's Seed, which writes the commits and
// the lease row; it is not part of Store.
func SeedCommits(state Execution, at int64) ([]Commit, error) { //nolint:gocritic,gocyclo // hugeParam: plans from the value; gocyclo: one branch per reachable state
	key := state.Assignment.Key()
	var events [][]Event
	add := func(typ EventType, payload any) error {
		ev, err := NewEvent(typ, at, payload)
		if err != nil {
			return err
		}
		events = append(events, []Event{ev})
		return nil
	}
	switch state.State {
	case effect.ExecutionAborted:
		// A tombstone is the whole ledger of an aborted key: the abort stood
		// at Seq 0, so no acceptance, binding or lease ever existed
		// (RUN-EXE-16).
		if err := add(EventExecutionAborted, Aborted{Reason: "seeded"}); err != nil {
			return nil, err
		}
	default:
		if err := add(EventExecutionAccepted, Accepted{Assignment: state.Assignment}); err != nil {
			return nil, err
		}
		refs := append(append([]ExecutionRef(nil), state.Superseded...), state.ExecutionRef)
		if refs[0].Provider != "" {
			if err := add(EventExecutionBound, Bound{Ref: refs[0]}); err != nil {
				return nil, err
			}
		}
		if state.Lease.Owner != "" && state.Lease.Epoch > 0 {
			if err := add(EventExecutionClaimed, Claimed{Owner: state.Lease.Owner, Epoch: state.Lease.Epoch}); err != nil {
				return nil, err
			}
		}
		for i := 1; i < len(refs); i++ {
			for _, step := range []struct {
				typ     EventType
				payload any
			}{{EventExecutionStarted, nil}, {EventExecutionRunning, nil}, {EventExecutionRestarted, Restarted{Superseded: refs[i-1], Ref: refs[i].Ref}}} {
				if err := add(step.typ, step.payload); err != nil {
					return nil, err
				}
			}
		}
		if err := seedTail(state, key, add); err != nil {
			return nil, err
		}
	}
	var folded ExecutionState
	head := Head{}
	commits := make([]Commit, 0, len(events))
	for i, evs := range events {
		c := Commit{Seq: head.Next, CommitID: DeriveCommitID(key, "seed", fmt.Sprint(i)), Events: evs}
		switch evs[0].Type {
		case EventExecutionAccepted:
			c.CommitID = AcceptCommitID(key)
		case EventExecutionAborted:
			c.CommitID = AbortCommitID(key)
		}
		var err error
		if folded, err = Fold(folded, &c); err != nil {
			return nil, err
		}
		commits = append(commits, c)
		head = Head{Next: c.Seq + 1}
	}
	return commits, nil
}

// seedTail adds the events that take an accepted, bound, claimed execution
// to state.State.
func seedTail(state Execution, key effect.AssignmentKey, add func(EventType, any) error) error { //nolint:gocritic // hugeParam: plans from the value
	switch state.State {
	case effect.ExecutionAccepted:
		return nil
	case effect.ExecutionDispatching:
		return add(EventExecutionStarted, nil)
	case effect.ExecutionRunning:
		if err := add(EventExecutionStarted, nil); err != nil {
			return err
		}
		return add(EventExecutionRunning, nil)
	case effect.ExecutionCancelRequested:
		return add(EventCancelRequested, nil)
	default:
		if !protocol.StatusTerminal(state.State) {
			return fmt.Errorf("executor/store: seed: unknown state %q", state.State)
		}
		if err := add(EventExecutionStarted, nil); err != nil {
			return err
		}
		if err := add(EventExecutionRunning, nil); err != nil {
			return err
		}
		out := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key}
		if state.Outcome != nil {
			out = *state.Outcome
		}
		if err := add(EventExecutionSettled, Settled{State: state.State, Outcome: out}); err != nil {
			return err
		}
		if state.Acknowledged {
			return add(EventOutcomeAcknowledged, nil)
		}
		return nil
	}
}
