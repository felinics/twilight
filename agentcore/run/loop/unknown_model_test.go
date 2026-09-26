package loop

import (
	"testing"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// A model outcome without an answer withdraws the step to Open, the same
// disposition as a cancelled effect and as the recovery path's missing
// execution; only a provider failure ends the Run (RUN-CMT-7, RUN-EXE-6).
func TestModelCompletionWithdrawsOnUnknown(t *testing.T) {
	step := &run.ModelStep{RefValue: run.StepRef{RunID: "r", ID: "s1"}, Status: run.ModelExecuting, Effect: "e1"}
	cases := []struct {
		name string
		out  OutcomeResult
		want run.AgentCommand
	}{
		{"unknown", effect.Unknown{Message: "disposed"}, run.RecoverModelExecution{StepID: "s1", Effect: "e1"}},
		{"cancelled", effect.Cancelled{}, run.RecoverModelExecution{StepID: "s1", Effect: "e1"}},
		{"provider failure", effect.ModelFailed{Code: effect.FailureExecutor, Message: "boom"},
			run.SubmitModelFailure{StepID: "s1", Effect: "e1", Failure: run.StepFailure{Class: run.FailureProvider, Message: "boom"}}},
	}
	l := &Loop{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := l.modelCompletion(step, Outcome{Result: tc.out})
			if err != nil || got != tc.want {
				t.Fatalf("completion = %#v, %v, want %#v", got, err, tc.want)
			}
		})
	}
}
