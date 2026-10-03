package plan

import (
	"github.com/felinics/twilight/agentcore/run"
)

// RecoveryDisposition is one takeover disposition command with its derived identity.
type RecoveryDisposition struct {
	Command run.AgentCommand
	ID      run.CommandID
}

// RecoveryTarget is one Executing target a takeover has to decide about: the
// model step or tool call, and the effect it requested (from the started
// fact). The reconciler (agent/run/reconcile) asks the executor whether it
// still holds an attempt for that effect; only if not is the recovery command
// issued (RUN-CMT-7).
type RecoveryTarget struct {
	RunID  run.RunID
	StepID run.StepID
	CallID run.CallID // empty for a model step
	Effect run.EffectID
	Model  *run.ModelStep     // set for a model target
	Call   *run.ToolCallState // set for a tool target
}

// RecoveryTargets lists the Executing targets of state in the order RecoveryCommands
// disposes them.
func RecoveryTargets(state *run.MachineState) []RecoveryTarget {
	if state.Status.Terminal() {
		return nil
	}
	switch cur := state.Current.(type) {
	case run.ModelStep:
		if cur.Status != run.ModelExecuting {
			return nil
		}
		ms := cur
		return []RecoveryTarget{{RunID: state.RunID, StepID: cur.RefValue.ID, Effect: cur.Effect, Model: &ms}}
	case run.ToolStep:
		var out []RecoveryTarget
		for i := range cur.Calls {
			call := cur.Calls[i]
			if call.Status != run.ToolExecuting {
				continue
			}
			out = append(out, RecoveryTarget{RunID: state.RunID, StepID: cur.RefValue.ID, CallID: call.CallID, Effect: call.Effect, Call: &call})
		}
		return out
	default:
		return nil
	}
}

// RecoveryCommand is the disposition of one target whose effect is lost: an
// Executing model step is withdrawn to Open (the next Prepare plans again);
// an Executing tool call settles as Unknown. The command's identity derives
// from the effect alone (RUN-WIR-1), so the same disposition issued by any
// owner, or issued twice, replays instead of applying again.
func RecoveryCommand(id run.Identity, target RecoveryTarget) RecoveryDisposition {
	if target.Call == nil {
		return RecoveryDisposition{
			Command: run.RecoverModelExecution{StepID: target.StepID, Effect: target.Effect},
			ID:      id.DeriveRecoveryCommandID(target.Effect),
		}
	}
	return RecoveryDisposition{
		Command: run.SubmitToolFailure{
			StepID:  target.StepID,
			CallID:  target.CallID,
			Effect:  target.Effect,
			Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: "owner process lost before settlement"},
			Outcome: run.ToolOutcomeUnknown,
		},
		ID: id.DeriveRecoveryCommandID(target.Effect),
	}
}

// RecoveryCommands lists the takeover dispositions of every Executing target
// in state (RUN-CMT-7). Pending and Waiting calls are left alone.
func RecoveryCommands(id run.Identity, state *run.MachineState) []RecoveryDisposition {
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
