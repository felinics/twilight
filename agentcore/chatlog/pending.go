package chatlog

import (
	"context"
	"fmt"
	"slices"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
)

const PendingProjectionID module.ProjectionID = "twilight/chatlog/pending"

// PendingInput is the routing information for an input still awaiting delivery.
// Content and settled input history remain in Surface and Context.
type PendingInput struct {
	ID        InputID           `json:"id"`
	Digest    jsonstable.Digest `json:"digest"`
	Placement Placement         `json:"placement"`
	Position  ledger.Position   `json:"position"`
}

// Pending is a derived scheduling view. Its size depends on the queue, not the
// conversation history. Surface remains the authority for input lifecycle
// validation, including resubmissions of already settled input identities.
type Pending struct {
	Inputs Table[InputID, PendingInput] `json:"inputs"`
}

// SubmittedInputs returns routing information in ledger submission order.
func (p Pending) SubmittedInputs() []PendingInput {
	inputs := make([]PendingInput, 0, p.Inputs.Len())
	p.Inputs.Range(func(_ InputID, in PendingInput) bool {
		inputs = append(inputs, in)
		return true
	})
	slices.SortFunc(inputs, func(a, b PendingInput) int {
		if a.Position.Less(b.Position) {
			return -1
		}
		if b.Position.Less(a.Position) {
			return 1
		}
		return 0
	})
	return inputs
}

// PendingProjection consumes only input lifecycle facts. Like Surface it
// follows each domain's inheritance policy, including accepted inputs in forks.
// RequiredForCommit keeps it in lockstep with accepted facts and requires
// checkpoint integrity verification before it can hide pending scheduling work.
var PendingProjection = module.ProjectionDefinition{
	ID: PendingProjectionID, Version: 1,
	RequiredForCommit: true,
	Consumes:          []ledger.EventType{TypeInputSubmitted, TypeInputWithdrawn, TypeInputRejected, sessionstore.Type("input_accepted")},
	Initial:           func() (any, error) { return Pending{}, nil },
	Apply:             applyPending,
	StateCodec:        module.JSONStateCodec[Pending]{},
}

func applyPending(state any, event module.DecodedEvent) (any, error) {
	pending, ok := state.(Pending)
	if !ok {
		return nil, fmt.Errorf("chatlog pending: state is %T", state)
	}
	var remove InputID
	switch value := event.Value.(type) {
	case InputSubmittedPayload:
		if pending.Inputs.Has(value.InputID) {
			return nil, fmt.Errorf("input %s submitted twice", value.InputID)
		}
		digest, err := DigestInput(value.InputID, value.Content)
		if err != nil {
			return nil, err
		}
		pending.Inputs = pending.Inputs.Set(value.InputID, PendingInput{
			ID: value.InputID, Digest: digest, Placement: value.Placement.Effective(), Position: event.Position,
		})
		return pending, nil
	case InputWithdrawnPayload:
		remove = value.InputID
	case InputRejectedPayload:
		remove = value.InputID
	case sessionstore.Event:
		accepted, ok := value.Fact.(turn.InputAccepted)
		if !ok {
			return nil, fmt.Errorf("chatlog pending: unexpected fact %T", value.Fact)
		}
		remove = InputID(accepted.Input.ID)
		input, ok := pending.Inputs.Get(remove)
		if !ok || input.Digest != accepted.Input.Digest {
			return nil, fmt.Errorf("input %s accepted without matching submission", remove)
		}
	default:
		return nil, fmt.Errorf("chatlog pending: unexpected %T", event.Value)
	}
	if !pending.Inputs.Has(remove) {
		return nil, fmt.Errorf("input %s terminated without submission", remove)
	}
	pending.Inputs = pending.Inputs.Delete(remove)
	return pending, nil
}

// ReadPending loads routing information without materializing chat history.
func ReadPending(ctx context.Context, reader session.ProjectionReader, sid session.SessionID) (Pending, error) {
	state, _, err := reader.Load(ctx, sid, PendingProjectionID, PendingProjection.Version)
	if err != nil {
		return Pending{}, err
	}
	pending, ok := state.(Pending)
	if !ok {
		return Pending{}, fmt.Errorf("chatlog: pending projection is %T", state)
	}
	return pending, nil
}
