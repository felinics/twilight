package turn_test

import (
	"testing"

	"github.com/felinics/twilight/agentcore/testutil/sdkconv"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/sdk"
)

func TestRegressionZeroBindingsWithToolCallsRejected(t *testing.T) {
	s := newTurn(t)
	s, stepID := advanceToExecuting(t, s, testRequest(), nil)
	result := modelResultWithCalls("c1")
	if _, err := schema.Machine().Decide(s, settling(s, turn.SubmitModelResult{StepID: stepID, Result: result, Calls: nil})); err == nil {
		t.Fatal("result with tool calls and no bindings completed the run")
	}
}

func TestRegressionBindingMustMatchModelResult(t *testing.T) {
	safe := testToolDef("safe")
	danger := testToolDef("danger")
	specSafe := makeSpec(t, safe, turn.DirectExecution)
	specDanger := makeSpec(t, danger, turn.DirectExecution)
	s := newTurn(t)
	s, stepID := advanceToExecuting(t, s, testRequest(safe, danger), []turn.ToolSpec{specSafe, specDanger})

	evil := makeBinding(t, stepID, 0, "c1", specDanger, `{"rm":"-rf"}`)
	result := modelResultWithNamedCalls("safe", `{"a":1}`, "c1")
	if _, err := schema.Machine().Decide(s, settling(s, turn.SubmitModelResult{StepID: stepID, Result: result, Calls: []turn.ToolCallBinding{evil}})); err == nil {
		t.Fatal("binding for a tool the model never called was accepted")
	}

	tampered := makeBinding(t, stepID, 0, "c1", specSafe, `{"a":999}`)
	if _, err := schema.Machine().Decide(s, settling(s, turn.SubmitModelResult{StepID: stepID, Result: result, Calls: []turn.ToolCallBinding{tampered}})); err == nil {
		t.Fatal("binding with tampered arguments was accepted")
	}
}

func TestRegressionToolStepIDReproducible(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, turn.ApprovalRequired)
	s := newTurn(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []turn.ToolSpec{spec})
	b := makeBinding(t, stepID, 0, "c1", spec, `{}`)
	facts := mustDecide(t, s, turn.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []turn.ToolCallBinding{b}})
	opened := facts[1].(turn.ToolStepOpened)
	if schema.Identity().DeriveToolStepID(opened.Source) != opened.StepID {
		t.Fatal("ToolStepOpened source does not reproduce its StepID")
	}
	s = fold(t, s, facts)
	ts := s.Current.(turn.ToolStep)
	if schema.Identity().DeriveToolStepID(ts.Source) != ts.RefValue.ID {
		t.Fatal("persisted ToolStep source does not reproduce the step ID")
	}
}

func TestRegressionEvolveRejectsIllegalCallState(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, turn.DirectExecution)
	s := newTurn(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []turn.ToolSpec{spec})
	b := makeBinding(t, stepID, 0, "c1", spec, `{}`)
	facts := mustDecide(t, s, turn.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []turn.ToolCallBinding{b}})
	opened := facts[1].(turn.ToolStepOpened)
	s = fold(t, s, facts)
	s = fold(t, s, mustDecide(t, s, startTool(s, opened.StepID, cid(stepID, 0))))

	_, err := schema.Machine().Evolve(s, turn.ToolCallFailed{
		StepID:  opened.StepID,
		CallID:  cid(stepID, 0),
		Failure: turn.ToolFailure{Class: turn.FailureExecution},
		Outcome: turn.ToolOutcomeUnknown,
	})
	if err == nil {
		t.Fatal("Evolve accepted an illegal unknown-outcome class")
	}
}

func TestRegressionCancelReasonFixed(t *testing.T) {
	s := newTurn(t)
	if _, err := schema.Machine().Decide(s, turn.CancelTurn{Reason: turn.TurnReason("other")}); err == nil {
		t.Fatal("CancelTurn accepted a non-cancellation reason")
	}
	facts := mustDecide(t, s, turn.CancelTurn{})
	end, ok := facts[0].(turn.TurnEnded).End.(turn.TurnStoppedEnd)
	if !ok || end.Reason != turn.ReasonCancelled {
		t.Fatal("cancel reason not fixed to cancelled")
	}
}

func TestCancelSettlesEveryUnfinishedToolCall(t *testing.T) {
	for _, kind := range []turn.ResponseKind{turn.ResponseApproval, turn.ResponseExternal} {
		t.Run(string(kind), func(t *testing.T) {
			policy := turn.ApprovalRequired
			if kind == turn.ResponseExternal {
				policy = turn.ExternalResponse
			}
			def, waitDef := testToolDef("t"), testToolDef("wait")
			spec, waitSpec := makeSpec(t, def, turn.DirectExecution), makeSpec(t, waitDef, policy)
			current := newTurn(t)
			current, modelStep := advanceToExecuting(t, current, testRequest(def, waitDef), []turn.ToolSpec{spec, waitSpec})
			bindings := make([]turn.ToolCallBinding, 5)
			result := sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls}
			for i := range bindings {
				callSpec := spec
				if i == 2 {
					callSpec = waitSpec
				}
				providerID := "c" + string(rune('1'+i))
				bindings[i] = makeBinding(t, modelStep, i, providerID, callSpec, `{}`)
				result.ToolCalls = append(result.ToolCalls, sdk.ToolCall{ToolCallID: providerID, ToolName: callSpec.Name, Input: sdk.ParseToolArguments(`{}`)})
			}
			frozen, err := sdkconv.FreezeModelResult(result)
			if err != nil {
				t.Fatal(err)
			}
			current = fold(t, current, mustDecide(t, current, turn.SubmitModelResult{StepID: modelStep, Result: frozen, Calls: bindings}))
			stepID := current.Current.(turn.ToolStep).RefValue.ID
			// Retain one executing call, one completed result and one known failure.
			for _, i := range []int{0, 3} {
				current = fold(t, current, mustDecide(t, current, startTool(current, stepID, bindings[i].CallID)))
			}
			current = fold(t, current, mustDecide(t, current, turn.SubmitToolResult{StepID: stepID, CallID: bindings[3].CallID,
				Result: turn.ToolExecutionResult{Output: cj(`"done"`)}}))
			current = fold(t, current, mustDecide(t, current, turn.DeclineToolCall{StepID: stepID, CallID: bindings[4].CallID,
				Failure: turn.ToolFailure{Class: turn.FailureExecution, Message: "failed"}}))
			facts := mustDecide(t, current, turn.CancelTurn{})
			if len(facts) != 4 {
				t.Fatalf("cancel facts = %d, want three failures and TurnEnded", len(facts))
			}
			for i := 0; i < 3; i++ {
				failure, ok := facts[i].(turn.ToolCallFailed)
				if !ok || failure.CallID != bindings[i].CallID {
					t.Fatalf("fact %d = %+v", i, facts[i])
				}
				outcome, class := turn.ToolOutcomeKnown, turn.FailureCancelled
				if i == 0 {
					outcome, class = turn.ToolOutcomeUnknown, turn.FailureEffectUnknown
				}
				if failure.Outcome != outcome || failure.Failure.Class != class {
					t.Fatalf("call %d failure = %+v", i, failure)
				}
			}
			ended := fold(t, current, facts)
			if ended.Status != turn.TurnStopped || ended.LastToolStep == nil ||
				len(ended.Result.UncertainCalls) != 1 || ended.Result.UncertainCalls[0] != bindings[0].CallID {
				t.Fatalf("cancel result = %+v", ended)
			}
			calls := ended.LastToolStep.Calls
			if calls[3].Status != turn.ToolCompleted || calls[4].Failure.Failure.Class != turn.FailureExecution {
				t.Fatalf("settled calls changed: %+v", calls[3:])
			}
		})
	}
}
