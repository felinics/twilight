package loop

import (
	"context"
	"fmt"

	turn "github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/plan"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/store"
)

func toolCallIndex(step turn.ToolStep, callID turn.CallID) int {
	for i := range step.Calls {
		if step.Calls[i].CallID == callID {
			return i
		}
	}
	return -1
}

// startToolCalls validates, starts and dispatches the Pending calls the
// frozen Scheduling allows. Validation happens before the start
// barrier through the Executor and a failed call is declined without a start
// (DeclineToolCall); a validated call is started under its tool
// effect and handed to the Executor. It returns the dispatched keys; an empty
// list with no error means nothing is executing on this Loop's behalf and the
// reload decides.
func (l *Loop) startToolCalls(ctx context.Context, rt store.TurnStore, events EventSink, snapshot *store.Snapshot, act plan.StartToolCalls) ([]AssignmentKey, error) {
	turnID := snapshot.State.TurnID
	ts, ok := snapshot.State.Current.(turn.ToolStep)
	if !ok || ts.RefValue.ID != act.StepID {
		return nil, fmt.Errorf("agent: loop: tool step %q is not current", act.StepID)
	}
	limit := len(act.CallIDs)
	if ts.Scheduling.Mode == turn.ToolScheduleSequential {
		limit = 1
	}
	if ts.Scheduling.MaxParallel > 0 && ts.Scheduling.MaxParallel < limit {
		limit = ts.Scheduling.MaxParallel
	}
	var dispatched []AssignmentKey
	for _, callID := range act.CallIDs {
		if len(dispatched) >= limit {
			break
		}
		// Outer ctx cancelled: stop starting new calls; what was dispatched
		// settles through its Outcome.
		if ctx.Err() != nil {
			break
		}
		i := toolCallIndex(ts, callID)
		if i < 0 {
			continue
		}
		call := ts.Calls[i]
		if call.Status != turn.ToolPending {
			// Executing calls settle through the Outcome of the effect they
			// started or the owner's takeover disposition; never re-run
			continue
		}
		ref := toolEffect(turnID, act.StepID, callID)
		// The target is resolved per tool effect before the
		// pre-start check, so the Validate probe carries what the Assignment
		// will carry.
		target, err := l.targetFor(ctx, EffectContext{Session: rt.Scope(), TurnID: turnID, StepID: act.StepID, CallID: callID,
			Effect: ref.id, Kind: AssignmentTool, Tool: call.ToolRef, Placement: call.Placement})
		if err != nil {
			return dispatched, err
		}
		binding := ToolAssignment{ToolRef: call.ToolRef, DefinitionDigest: call.DefinitionDigest, Arguments: call.Arguments, Policy: call.Policy, Replay: call.Replay, Placement: call.Placement}
		probe := Assignment{Session: rt.Scope(), OwnerKind: OwnerTurn, OwnerID: string(turnID), StepID: act.StepID, CallID: callID, Target: target, Body: binding}
		known, err := l.Executor.Validate(ctx, probe)
		if err != nil {
			return dispatched, err
		}
		if known != nil {
			// The call fails before its effect is requested: no start
			// barrier, no effect, no attempt. A call is declined at most
			// once, so the decline is identified by the call alone and a
			// retry of the same rejection is idempotent.
			res, err := l.commit(ctx, rt, turnID, schema.Identity().DeriveDeclineCommandID(turnID, act.StepID, callID), snapshot.Position,
				turn.DeclineToolCall{StepID: act.StepID, CallID: callID, Failure: *known})
			if err != nil {
				if retriable(err) {
					return dispatched, nil // another actor moved the call; reload decides
				}
				return dispatched, err
			}
			l.emitCommitted(ctx, events, rt.Scope(), turnID, res.Facts)
			continue
		}

		start, err := l.commit(ctx, rt, turnID, ref.startID(), snapshot.Position,
			turn.StartToolCall{StepID: act.StepID, CallID: callID, Effect: ref.id})
		if err != nil {
			if retriable(err) {
				return dispatched, nil // another actor moved the call; reload decides
			}
			return dispatched, err
		}
		if startedCall, ok := toolCallFromSnapshot(&start.Snapshot.State, act.StepID, callID); !ok || startedCall.Status != turn.ToolExecuting || startedCall.Effect != ref.id {
			// The one-shot replay may land after the call was settled, or the
			// call is Executing under another effect. Never invoke a call the
			// The Turn did not start under this effect.
			continue
		}
		l.emitCommitted(ctx, events, rt.Scope(), turnID, start.Facts)
		if events != nil {
			_ = events.Emit(ctx, Event{Session: rt.Scope(), TurnID: turnID, StepID: act.StepID, CallID: callID,
				Kind: EventToolStarted, Durability: EventCommitted})
		}
		assignment := probe
		assignment.Effect = ref.id
		if err := l.dispatch(ctx, assignment); err != nil {
			if recoverableDispatch(ctx, err) {
				// Acceptance may precede a temporary refusal or lost response. Keep the
				// call Executing for explicit recovery rather than claiming a
				// known failure or dispatching a duplicate.
				return dispatched, fmt.Errorf("agent: loop: tool dispatch outcome: %w", err)
			}
			// The effect never started: settle it as a Known execution
			// failure so the call does not stay Executing.
			failure := turn.ToolFailure{Class: turn.FailureExecution, Message: "dispatch: " + err.Error()}
			if _, serr := l.settle(ctx, rt, events, &ref, start.Snapshot.Position,
				turn.SubmitToolFailure{StepID: act.StepID, CallID: callID, Effect: ref.id, Failure: failure, Outcome: turn.ToolOutcomeKnown}); serr != nil {
				return dispatched, serr
			}
			continue
		}
		dispatched = append(dispatched, assignment.Key())
	}
	return dispatched, nil
}

func toolCallFromSnapshot(state *turn.MachineState, stepID turn.StepID, callID turn.CallID) (turn.ToolCallState, bool) {
	step, ok := state.Current.(turn.ToolStep)
	if !ok || step.RefValue.ID != stepID {
		return turn.ToolCallState{}, false
	}
	for i := range step.Calls {
		if step.Calls[i].CallID == callID {
			return step.Calls[i], true
		}
	}
	return turn.ToolCallState{}, false
}

// executingCall finds the call of step that is Executing under the effect id.
func executingCall(step *turn.ToolStep, id turn.EffectID) (turn.ToolCallState, bool) {
	if id == "" {
		return turn.ToolCallState{}, false
	}
	for i := range step.Calls {
		if step.Calls[i].Status == turn.ToolExecuting && step.Calls[i].Effect == id {
			return step.Calls[i], true
		}
	}
	return turn.ToolCallState{}, false
}

// toolCompletion maps a tool Outcome to the settlement of the call's tool
// effect. A sealed outcome maps directly; a missing outcome or a transport
// error is Unknown, because the effect may have happened.
func toolCompletion(stepID turn.StepID, callID turn.CallID, eff turn.EffectID, out Outcome) turn.AgentCommand {
	switch o := out.Result.(type) {
	case ToolExecutionSucceeded:
		return turn.SubmitToolResult{StepID: stepID, CallID: callID, Effect: eff, Result: o.Result}
	case ToolExecutionFailed:
		failure := o.Failure
		if failure.Class == "" || failure.Class == turn.FailureEffectUnknown {
			failure.Class = turn.FailureExecution
		}
		return turn.SubmitToolFailure{StepID: stepID, CallID: callID, Effect: eff, Failure: failure, Outcome: turn.ToolOutcomeKnown}
	case ToolExecutionUnknown:
		failure := o.Failure
		if failure.Class != "" && failure.Class != turn.FailureEffectUnknown && failure.Message == "" {
			failure.Message = "tool reported " + failure.Class
		}
		failure.Class = turn.FailureEffectUnknown
		return turn.SubmitToolFailure{StepID: stepID, CallID: callID, Effect: eff, Failure: failure, Outcome: turn.ToolOutcomeUnknown}
	case effect.Cancelled:
		return turn.SubmitToolFailure{StepID: stepID, CallID: callID, Effect: eff,
			Failure: turn.ToolFailure{Class: turn.FailureEffectUnknown, Message: "cancelled: " + o.Message}, Outcome: turn.ToolOutcomeUnknown}
	case effect.Unknown:
		msg := "tool returned no outcome"
		if o.Message != "" {
			msg = "executor: " + o.Message
		}
		return turn.SubmitToolFailure{StepID: stepID, CallID: callID, Effect: eff,
			Failure: turn.ToolFailure{Class: turn.FailureEffectUnknown, Message: msg}, Outcome: turn.ToolOutcomeUnknown}
	}
	// A model result or no result for a tool call: the effect may have
	// happened, so it is Unknown.
	return turn.SubmitToolFailure{StepID: stepID, CallID: callID, Effect: eff,
		Failure: turn.ToolFailure{Class: turn.FailureEffectUnknown, Message: fmt.Sprintf("executor delivered %T for a tool call", out.Result)}, Outcome: turn.ToolOutcomeUnknown}
}
