package loop

import (
	"context"
	"errors"
	"fmt"

	run "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/runtime"
)

// ErrEffectNotExecuting reports a Redispatch of an effect the Run is not
// Executing under: it settled, was recovered, or was never started.
var ErrEffectNotExecuting = errors.New("agent: loop: effect is not executing")

// Redispatch rebuilds the Assignment of an Executing effect from the Run's
// state and hands it to the Executor again. It is the effect process's
// dispatch step (RUN-EXE-15): the Run recorded the start barrier, so the
// effect is owed to the Executor, and a Dispatch the Loop's own drive did
// not complete (a crash between the start fact and the call) is completed
// here. The Executor recognises a replayed Assignment by its key, so a
// Redispatch of an effect it already holds changes nothing (RUN-EXE-3).
func (l *Loop) Redispatch(ctx context.Context, rt runtime.RunStore, key AssignmentKey) error {
	if err := l.checkArgs(ctx, rt, key.RunID); err != nil {
		return err
	}
	snapshot, err := rt.Load(ctx, key.RunID)
	if err != nil {
		return err
	}
	if snapshot.State.Status.Terminal() {
		return ErrEffectNotExecuting
	}
	scope := rt.Scope()
	switch cur := snapshot.State.Current.(type) {
	case run.ModelStep:
		if cur.Status != run.ModelExecuting || key.Effect == "" || cur.Effect != key.Effect {
			return ErrEffectNotExecuting
		}
		stepID := cur.RefValue.ID
		target, err := l.targetFor(ctx, EffectContext{Session: scope, RunID: key.RunID, StepID: stepID, Effect: key.Effect, Kind: AssignmentModel})
		if err != nil {
			return err
		}
		request, err := rt.FrozenRequest(ctx, cur.RequestDigest)
		if err != nil {
			return fmt.Errorf("agent: loop: redispatch: load frozen model request: %w", err)
		}
		return l.dispatch(ctx, Assignment{Session: scope, RunID: key.RunID, StepID: stepID, Effect: key.Effect, Target: target,
			Body: ModelAssignment{Model: cur.Model, RequestDigest: cur.RequestDigest, Request: &request}})
	case run.ToolStep:
		call, ok := executingCall(&cur, key.Effect)
		if !ok {
			return ErrEffectNotExecuting
		}
		stepID := cur.RefValue.ID
		target, err := l.targetFor(ctx, EffectContext{Session: scope, RunID: key.RunID, StepID: stepID, CallID: call.CallID, Effect: key.Effect, Kind: AssignmentTool, Tool: call.ToolRef, Placement: call.Placement})
		if err != nil {
			return err
		}
		return l.dispatch(ctx, Assignment{Session: scope, RunID: key.RunID, StepID: stepID, CallID: call.CallID, Effect: key.Effect, Target: target,
			Body: ToolAssignment{ToolRef: call.ToolRef, DefinitionDigest: call.DefinitionDigest, Arguments: call.Arguments, Policy: call.Policy, Replay: call.Replay, Placement: call.Placement}})
	default:
		return ErrEffectNotExecuting
	}
}
