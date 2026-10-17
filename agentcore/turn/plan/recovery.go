package plan

import (
	"github.com/felinics/twilight/agentcore/turn"
)

// RecoveryDisposition is one takeover disposition command with its derived identity.
type RecoveryDisposition struct {
	Command turn.AgentCommand
	ID      turn.CommandID
}

// RecoveryTarget is one Executing target a takeover has to decide about: the
// model step or tool call, and the effect it requested (from the started
// fact). The reconciler (agentcore/turn/reconcile) asks the executor whether it
// still holds an attempt for that effect; only if not is the recovery command
// issued.
type RecoveryTarget struct {
	TurnID turn.TurnID
	StepID turn.StepID
	CallID turn.CallID // empty for a model step
	Effect turn.EffectID
	Model  *turn.ModelStep     // set for a model target
	Call   *turn.ToolCallState // set for a tool target
}

// RecoveryTargets lists the Executing targets of state in the order RecoveryCommands
// disposes them.
func RecoveryTargets(state *turn.MachineState) []RecoveryTarget {
	if state.Status.Terminal() {
		return nil
	}
	switch cur := state.Current.(type) {
	case turn.ModelStep:
		if cur.Status != turn.ModelExecuting {
			return nil
		}
		ms := cur
		return []RecoveryTarget{{TurnID: state.TurnID, StepID: cur.RefValue.ID, Effect: cur.Effect, Model: &ms}}
	case turn.ToolStep:
		var out []RecoveryTarget
		for i := range cur.Calls {
			call := cur.Calls[i]
			if call.Status != turn.ToolExecuting {
				continue
			}
			out = append(out, RecoveryTarget{TurnID: state.TurnID, StepID: cur.RefValue.ID, CallID: call.CallID, Effect: call.Effect, Call: &call})
		}
		return out
	default:
		return nil
	}
}

// RecoveryCommand is the disposition of one target whose effect is lost: an
// Executing model step is withdrawn to Open (the next Prepare plans again);
// an Executing tool call settles as Unknown. The command's identity derives
// from the effect alone, so the same disposition issued by any
// owner, or issued twice, replays instead of applying again.
func RecoveryCommand(id turn.Identity, target RecoveryTarget) RecoveryDisposition {
	if target.Call == nil {
		return RecoveryDisposition{
			Command: turn.RecoverModelExecution{StepID: target.StepID, Effect: target.Effect},
			ID:      id.DeriveRecoveryCommandID(target.Effect),
		}
	}
	return RecoveryDisposition{
		Command: turn.SubmitToolFailure{
			StepID:  target.StepID,
			CallID:  target.CallID,
			Effect:  target.Effect,
			Failure: turn.ToolFailure{Class: turn.FailureEffectUnknown, Message: "owner process lost before settlement"},
			Outcome: turn.ToolOutcomeUnknown,
		},
		ID: id.DeriveRecoveryCommandID(target.Effect),
	}
}

// RecoveryCommands lists the takeover dispositions of every Executing target
// in state. Pending and Waiting calls are left alone.
func RecoveryCommands(id turn.Identity, state *turn.MachineState) []RecoveryDisposition {
	targets := RecoveryTargets(state)
	if len(targets) == 0 {
		return nil
	}
	out := make([]RecoveryDisposition, len(targets))
	for i, t := range targets {
		out[i] = RecoveryCommand(id, t)
	}
	return out
}
