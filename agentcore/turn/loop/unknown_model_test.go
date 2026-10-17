package loop

import (
	"testing"

	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
)

// A model outcome without an answer withdraws the step to Open, the same
// disposition as a cancelled effect and as the recovery path's missing
// execution; only a provider failure ends the Turn.
func TestModelCompletionWithdrawsOnUnknown(t *testing.T) {
	step := &turn.ModelStep{RefValue: turn.StepRef{TurnID: "r", ID: "s1"}, Status: turn.ModelExecuting, Effect: "e1"}
	cases := []struct {
		name string
		out  OutcomeResult
		want turn.AgentCommand
	}{
		{"unknown", effect.Unknown{Message: "disposed"}, turn.RecoverModelExecution{StepID: "s1", Effect: "e1"}},
		{"cancelled", effect.Cancelled{}, turn.RecoverModelExecution{StepID: "s1", Effect: "e1"}},
		{"provider failure", effect.ModelFailed{Code: effect.FailureExecutor, Message: "boom"},
			turn.SubmitModelFailure{StepID: "s1", Effect: "e1", Failure: turn.StepFailure{Class: turn.FailureProvider, Message: "boom"}}},
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
