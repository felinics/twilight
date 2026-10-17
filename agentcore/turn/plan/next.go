package plan

import (
	"fmt"

	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/turn"
)

// Action is the at-most-one pending action Next derives from the current
// state: what the Loop does next to move the Turn. Actions are
// never persisted; the Loop re-derives them after every Load. An action is
// not an effect: a start action requests an effect (turn.EffectID) only once
// its start fact is committed, and the other actions request none.
type Action interface{ action() }

type NeedModelRequest struct {
	Hint prompt.Input
}

func (NeedModelRequest) action() {}

type StartModelCall struct {
	StepID turn.StepID
}

func (StartModelCall) action() {}

// WithdrawPrepared asks the Loop to commit WithdrawPreparedStep: inputs were
// accepted after this step was Prepared, so its frozen request is incomplete
// and the Turn should replan.
type WithdrawPrepared struct {
	StepID turn.StepID
}

func (WithdrawPrepared) action() {}

type StartToolCalls struct {
	StepID  turn.StepID
	CallIDs []turn.CallID
}

func (StartToolCalls) action() {}

// Idle means the Turn is still active and Next has no executable action.
// Application inspects MachineState with Turn's WaitingCalls,
// ExecutingCalls and NeedsRecovery queries. Loop does not interpret them.
type Idle struct{}

func (Idle) action() {}

// Next derives the pending action from the current state.
// Terminal states return ErrTurnTerminal; callers check Status first.
//
//nolint:gocritic // hugeParam: Next is a pure value-state interpreter and must not mutate MachineState.
func Next(s turn.MachineState) (Action, error) {
	if s.Status.Terminal() {
		return nil, turn.ErrTurnTerminal
	}
	switch cur := s.Current.(type) {
	case turn.Open:
		var source turn.StepID
		if s.LastToolStep != nil {
			source = s.LastToolStep.RefValue.ID
		}
		return NeedModelRequest{Hint: prompt.Input{
			TurnID:     s.TurnID,
			SourceStep: source,
			Inputs:     append([]turn.AgentInput(nil), s.PendingInputs...),
		}}, nil
	case turn.ModelStep:
		if cur.Status == turn.ModelPrepared {
			if len(s.PendingInputs) > 0 {
				return WithdrawPrepared{StepID: cur.RefValue.ID}, nil
			}
			return StartModelCall{StepID: cur.RefValue.ID}, nil
		}
		return Idle{}, nil
	case turn.ToolStep:
		var pending []turn.CallID
		live := false
		for _, c := range cur.Calls {
			switch c.Status {
			case turn.ToolPending:
				pending = append(pending, c.CallID)
				live = true
			case turn.ToolWaiting, turn.ToolExecuting:
				live = true
			}
		}
		if len(pending) > 0 {
			return StartToolCalls{StepID: cur.RefValue.ID, CallIDs: pending}, nil
		}
		if live {
			return Idle{}, nil
		}
		return nil, fmt.Errorf("agent: next: tool step %q has no live calls but was not closed", cur.RefValue.ID)
	default:
		return nil, fmt.Errorf("agent: next: unknown current variant %T", s.Current)
	}
}
