package run_test

import (
	"testing"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/sdk"
)

func TestRegressionZeroBindingsWithToolCallsRejected(t *testing.T) {
	s := newRun(t)
	s, stepID := advanceToExecuting(t, s, testRequest(), nil)
	result := modelResultWithCalls("c1")
	if _, err := schema.Machine().Decide(s, settling(s, run.SubmitModelResult{StepID: stepID, Result: result, Calls: nil})); err == nil {
		t.Fatal("result with tool calls and no bindings completed the run")
	}
}

func TestRegressionBindingMustMatchModelResult(t *testing.T) {
	safe := testToolDef("safe")
	danger := testToolDef("danger")
	specSafe := makeSpec(t, safe, run.DirectExecution)
	specDanger := makeSpec(t, danger, run.DirectExecution)
	s := newRun(t)
	s, stepID := advanceToExecuting(t, s, testRequest(safe, danger), []run.ToolSpec{specSafe, specDanger})

	evil := makeBinding(t, stepID, 0, "c1", specDanger, `{"rm":"-rf"}`)
	result := modelResultWithNamedCalls("safe", `{"a":1}`, "c1")
	if _, err := schema.Machine().Decide(s, settling(s, run.SubmitModelResult{StepID: stepID, Result: result, Calls: []run.ToolCallBinding{evil}})); err == nil {
		t.Fatal("binding for a tool the model never called was accepted")
	}

	tampered := makeBinding(t, stepID, 0, "c1", specSafe, `{"a":999}`)
	if _, err := schema.Machine().Decide(s, settling(s, run.SubmitModelResult{StepID: stepID, Result: result, Calls: []run.ToolCallBinding{tampered}})); err == nil {
		t.Fatal("binding with tampered arguments was accepted")
	}
}

func TestRegressionToolStepIDReproducible(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, run.ApprovalRequired)
	s := newRun(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []run.ToolSpec{spec})
	b := makeBinding(t, stepID, 0, "c1", spec, `{}`)
	facts := mustDecide(t, s, run.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []run.ToolCallBinding{b}})
	opened := facts[1].(run.ToolStepOpened)
	if schema.Identity().DeriveToolStepID(opened.Source) != opened.StepID {
		t.Fatal("ToolStepOpened source does not reproduce its StepID")
	}
	s = fold(t, s, facts)
	ts := s.Current.(run.ToolStep)
	if schema.Identity().DeriveToolStepID(ts.Source) != ts.RefValue.ID {
		t.Fatal("persisted ToolStep source does not reproduce the step ID")
	}
}

func TestRegressionEvolveRejectsIllegalCallState(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, run.DirectExecution)
	s := newRun(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []run.ToolSpec{spec})
	b := makeBinding(t, stepID, 0, "c1", spec, `{}`)
	facts := mustDecide(t, s, run.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []run.ToolCallBinding{b}})
	opened := facts[1].(run.ToolStepOpened)
	s = fold(t, s, facts)
	s = fold(t, s, mustDecide(t, s, startTool(s, opened.StepID, cid(stepID, 0))))

	_, err := schema.Machine().Evolve(s, run.ToolCallFailed{
		StepID:  opened.StepID,
		CallID:  cid(stepID, 0),
		Failure: run.ToolFailure{Class: run.FailureExecution},
		Outcome: run.ToolOutcomeUnknown,
	})
	if err == nil {
		t.Fatal("Evolve accepted an illegal unknown-outcome class")
	}
}

func TestRegressionCancelReasonFixed(t *testing.T) {
	s := newRun(t)
	if _, err := schema.Machine().Decide(s, run.CancelRun{Reason: run.RunReason("other")}); err == nil {
		t.Fatal("CancelRun accepted a non-cancellation reason")
	}
	facts := mustDecide(t, s, run.CancelRun{})
	end, ok := facts[0].(run.RunEnded).End.(run.RunStoppedEnd)
	if !ok || end.Reason != run.ReasonCancelled {
		t.Fatal("cancel reason not fixed to cancelled")
	}
}

func TestCancelSettlesEveryUnfinishedToolCall(t *testing.T) {
	for _, kind := range []run.ResponseKind{run.ResponseApproval, run.ResponseExternal} {
		t.Run(string(kind), func(t *testing.T) {
			policy := run.ApprovalRequired
			if kind == run.ResponseExternal {
				policy = run.ExternalResponse
			}
			def, waitDef := testToolDef("t"), testToolDef("wait")
			spec, waitSpec := makeSpec(t, def, run.DirectExecution), makeSpec(t, waitDef, policy)
			current := newRun(t)
			current, modelStep := advanceToExecuting(t, current, testRequest(def, waitDef), []run.ToolSpec{spec, waitSpec})
			bindings := make([]run.ToolCallBinding, 5)
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
			current = fold(t, current, mustDecide(t, current, run.SubmitModelResult{StepID: modelStep, Result: frozen, Calls: bindings}))
			stepID := current.Current.(run.ToolStep).RefValue.ID
			// Retain one executing call, one completed result and one known failure.
			for _, i := range []int{0, 3} {
				current = fold(t, current, mustDecide(t, current, startTool(current, stepID, bindings[i].CallID)))
			}
			current = fold(t, current, mustDecide(t, current, run.SubmitToolResult{StepID: stepID, CallID: bindings[3].CallID,
				Result: run.ToolExecutionResult{Output: cj(`"done"`)}}))
			current = fold(t, current, mustDecide(t, current, run.DeclineToolCall{StepID: stepID, CallID: bindings[4].CallID,
				Failure: run.ToolFailure{Class: run.FailureExecution, Message: "failed"}}))
			facts := mustDecide(t, current, run.CancelRun{})
			if len(facts) != 4 {
				t.Fatalf("cancel facts = %d, want three failures and RunEnded", len(facts))
			}
			for i := 0; i < 3; i++ {
				failure, ok := facts[i].(run.ToolCallFailed)
				if !ok || failure.CallID != bindings[i].CallID {
					t.Fatalf("fact %d = %+v", i, facts[i])
				}
				outcome, class := run.ToolOutcomeKnown, run.FailureCancelled
				if i == 0 {
					outcome, class = run.ToolOutcomeUnknown, run.FailureEffectUnknown
				}
				if failure.Outcome != outcome || failure.Failure.Class != class {
					t.Fatalf("call %d failure = %+v", i, failure)
				}
			}
			ended := fold(t, current, facts)
			if ended.Status != run.RunStopped || ended.LastToolStep == nil ||
				len(ended.Result.UncertainCalls) != 1 || ended.Result.UncertainCalls[0] != bindings[0].CallID {
				t.Fatalf("cancel result = %+v", ended)
			}
			calls := ended.LastToolStep.Calls
			if calls[3].Status != run.ToolCompleted || calls[4].Failure.Failure.Class != run.FailureExecution {
				t.Fatalf("settled calls changed: %+v", calls[3:])
			}
		})
	}
}
