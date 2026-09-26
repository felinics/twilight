package loop

import (
	"context"
	"errors"
	"fmt"

	run "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
)

func toolCallIndex(step run.ToolStep, callID run.CallID) int {
	for i := range step.Calls {
		if step.Calls[i].CallID == callID {
			return i
		}
	}
	return -1
}

// startToolCalls validates, starts and dispatches the Pending calls the
// frozen Scheduling allows (RUN-LOP-4). Validation happens before the start
// barrier through the Executor and a failed call is declined without a start
// (DeclineToolCall, RUN-EXE-5); a validated call is started under its tool
// effect and handed to the Executor. It returns the dispatched keys; an empty
// list with no error means nothing is executing on this Loop's behalf and the
// reload decides.
func (l *Loop) startToolCalls(ctx context.Context, rt runtime.RunStore, events EventSink, snapshot *runtime.Snapshot, act plan.StartToolCalls) ([]AssignmentKey, error) {
	runID := snapshot.State.RunID
	ts, ok := snapshot.State.Current.(run.ToolStep)
	if !ok || ts.RefValue.ID != act.StepID {
		return nil, fmt.Errorf("agent: loop: tool step %q is not current", act.StepID)
	}
	limit := len(act.CallIDs)
	if ts.Scheduling.Mode == run.ToolScheduleSequential {
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
		if call.Status != run.ToolPending {
			// Executing calls settle through the Outcome of the effect they
			// started or the owner's takeover disposition; never re-run
			// (TRN-DUR-4).
			continue
		}
		ref := toolEffect(runID, act.StepID, callID)
		// The target is resolved per tool effect (RUN-LOP-9) before the
		// pre-start check, so the Validate probe carries what the Assignment
		// will carry.
		target, err := l.targetFor(ctx, EffectContext{Session: rt.Scope(), RunID: runID, StepID: act.StepID, CallID: callID,
			Effect: ref.id, Kind: AssignmentTool, Tool: call.ToolRef, Placement: call.Placement})
		if err != nil {
			return dispatched, err
		}
		binding := ToolAssignment{ToolRef: call.ToolRef, DefinitionDigest: call.DefinitionDigest, Arguments: call.Arguments, Policy: call.Policy, Replay: call.Replay, Placement: call.Placement}
		probe := Assignment{Session: rt.Scope(), RunID: runID, StepID: act.StepID, CallID: callID, Target: target, Body: binding}
		known, err := l.Executor.Validate(ctx, probe)
		if err != nil {
			return dispatched, err
		}
		if known != nil {
			// The call fails before its effect is requested: no start
			// barrier, no effect, no attempt. A call is declined at most
			// once, so the decline is identified by the call alone and a
			// retry of the same rejection is idempotent (RUN-EXE-5).
			res, err := l.commit(ctx, rt, runID, schema.Identity().DeriveDeclineCommandID(runID, act.StepID, callID), snapshot.Position,
				run.DeclineToolCall{StepID: act.StepID, CallID: callID, Failure: *known})
			if err != nil {
				if retriable(err) {
					return dispatched, nil // another actor moved the call; reload decides
				}
				return dispatched, err
			}
			l.emitCommitted(ctx, events, rt.Scope(), runID, res.Facts)
			continue
		}

		start, err := l.commit(ctx, rt, runID, ref.startID(), snapshot.Position,
			run.StartToolCall{StepID: act.StepID, CallID: callID, Effect: ref.id})
		if err != nil {
			if retriable(err) {
				return dispatched, nil // another actor moved the call; reload decides
			}
			return dispatched, err
		}
		if startedCall, ok := toolCallFromSnapshot(&start.Snapshot.State, act.StepID, callID); !ok || startedCall.Status != run.ToolExecuting || startedCall.Effect != ref.id {
			// The one-shot replay may land after the call was settled, or the
			// call is Executing under another effect. Never invoke a call the
			// Run did not start under this effect.
			continue
		}
		l.emitCommitted(ctx, events, rt.Scope(), runID, start.Facts)
		if events != nil {
			_ = events.Emit(ctx, Event{Session: rt.Scope(), RunID: runID, StepID: act.StepID, CallID: callID,
				Kind: EventToolStarted, Durability: EventCommitted})
		}
		assignment := probe
		assignment.Effect = ref.id
		if err := l.dispatch(ctx, assignment); err != nil {
			if errors.Is(err, effect.ErrDispatchUnknown) {
				// The request may have crossed the external boundary. Keep the
				// call Executing for explicit recovery rather than claiming a
				// known failure or dispatching a duplicate.
				return dispatched, fmt.Errorf("agent: loop: tool dispatch outcome: %w", err)
			}
			// The effect never started: settle it as a Known execution
			// failure so the call does not stay Executing.
			failure := run.ToolFailure{Class: run.FailureExecution, Message: "dispatch: " + err.Error()}
			if _, serr := l.settle(context.WithoutCancel(ctx), rt, events, &ref, start.Snapshot.Position,
				run.SubmitToolFailure{StepID: act.StepID, CallID: callID, Effect: ref.id, Failure: failure, Outcome: run.ToolOutcomeKnown}); serr != nil {
				return dispatched, serr
			}
			continue
		}
		dispatched = append(dispatched, assignment.Key())
	}
	return dispatched, nil
}

func toolCallFromSnapshot(state *run.MachineState, stepID run.StepID, callID run.CallID) (run.ToolCallState, bool) {
	step, ok := state.Current.(run.ToolStep)
	if !ok || step.RefValue.ID != stepID {
		return run.ToolCallState{}, false
	}
	for i := range step.Calls {
		if step.Calls[i].CallID == callID {
			return step.Calls[i], true
		}
	}
	return run.ToolCallState{}, false
}

// executingCall finds the call of step that is Executing under the effect id.
func executingCall(step *run.ToolStep, id run.EffectID) (run.ToolCallState, bool) {
	if id == "" {
		return run.ToolCallState{}, false
	}
	for i := range step.Calls {
		if step.Calls[i].Status == run.ToolExecuting && step.Calls[i].Effect == id {
			return step.Calls[i], true
		}
	}
	return run.ToolCallState{}, false
}

// toolCompletion maps a tool Outcome to the settlement of the call's tool
// effect. A sealed outcome maps directly; a missing outcome or a transport
// error is Unknown, because the effect may have happened (RUN-LOP-5).
func toolCompletion(stepID run.StepID, callID run.CallID, eff run.EffectID, out Outcome) run.AgentCommand {
	switch o := out.Result.(type) {
	case ToolExecutionSucceeded:
		return run.SubmitToolResult{StepID: stepID, CallID: callID, Effect: eff, Result: o.Result}
	case ToolExecutionFailed:
		failure := o.Failure
		if failure.Class == "" || failure.Class == run.FailureEffectUnknown {
			failure.Class = run.FailureExecution
		}
		return run.SubmitToolFailure{StepID: stepID, CallID: callID, Effect: eff, Failure: failure, Outcome: run.ToolOutcomeKnown}
	case ToolExecutionUnknown:
		failure := o.Failure
		if failure.Class != "" && failure.Class != run.FailureEffectUnknown && failure.Message == "" {
			failure.Message = "tool reported " + failure.Class
		}
		failure.Class = run.FailureEffectUnknown
		return run.SubmitToolFailure{StepID: stepID, CallID: callID, Effect: eff, Failure: failure, Outcome: run.ToolOutcomeUnknown}
	case effect.Cancelled:
		return run.SubmitToolFailure{StepID: stepID, CallID: callID, Effect: eff,
			Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: "cancelled: " + o.Message}, Outcome: run.ToolOutcomeUnknown}
	case effect.Unknown:
		msg := "tool returned no outcome"
		if o.Message != "" {
			msg = "executor: " + o.Message
		}
		return run.SubmitToolFailure{StepID: stepID, CallID: callID, Effect: eff,
			Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: msg}, Outcome: run.ToolOutcomeUnknown}
	}
	// A model result or no result for a tool call: the effect may have
	// happened, so it is Unknown (RUN-LOP-5).
	return run.SubmitToolFailure{StepID: stepID, CallID: callID, Effect: eff,
		Failure: run.ToolFailure{Class: run.FailureEffectUnknown, Message: fmt.Sprintf("executor delivered %T for a tool call", out.Result)}, Outcome: run.ToolOutcomeUnknown}
}
