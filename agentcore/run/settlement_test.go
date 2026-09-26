package run_test

import (
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/schema"
)

// RUN-WIR-1: every fact that closes an effect names it, and RunEnded closes
// none. A model failure and a cancellation settle the executing model effect
// with ModelStepFailed before the Run ends; a rejection names the effect it
// settles; Evolve refuses a RunEnded over an Executing target and a
// settlement naming another effect.
func TestEveryEffectEndsUnderItsOwnSettlement(t *testing.T) {
	s := newRun(t)
	model, stepID := advanceToExecuting(t, s, testRequest(), nil)
	eff := model.Current.(run.ModelStep).Effect
	other := schema.Identity().DeriveEffectID(model.RunID, stepID, "", 7)
	failure := run.StepFailure{Class: run.FailureProvider, Message: "down"}

	t.Run("model failure", func(t *testing.T) {
		facts := mustDecide(t, model, run.SubmitModelFailure{StepID: stepID, Effect: eff, Failure: failure})
		if len(facts) != 2 {
			t.Fatalf("facts = %d, want [ModelStepFailed, RunEnded]", len(facts))
		}
		failed, ok := facts[0].(run.ModelStepFailed)
		if !ok || failed.Effect != eff || failed.Failure != failure {
			t.Fatalf("facts[0] = %+v", facts[0])
		}
		if end, ok := facts[1].(run.RunEnded); !ok || end.End.(run.RunFailedEnd).Reason != run.ReasonProviderFailure {
			t.Fatalf("facts[1] = %+v", facts[1])
		}
		if after := fold(t, model, facts); after.Status != run.RunFailed || after.ModelSteps != 1 {
			t.Fatalf("after = %+v", after)
		}
	})
	t.Run("cancel while executing", func(t *testing.T) {
		facts := mustDecide(t, model, run.CancelRun{})
		if len(facts) != 2 {
			t.Fatalf("facts = %d, want [ModelStepFailed, RunEnded]", len(facts))
		}
		failed, ok := facts[0].(run.ModelStepFailed)
		if !ok || failed.Effect != eff || failed.Failure.Class != run.FailureEffectUnknown {
			t.Fatalf("facts[0] = %+v", facts[0])
		}
		if end := facts[1].(run.RunEnded).End.(run.RunStoppedEnd); end.UncertainModel != stepID {
			t.Fatalf("end = %+v", end)
		}
		fold(t, model, facts)
	})
	t.Run("rejection names the effect", func(t *testing.T) {
		facts := mustDecide(t, model, run.RejectModelResult{StepID: stepID, Effect: eff, Failure: failure, Disposition: run.ModelRejectRetry})
		if rejected, ok := facts[0].(run.ModelStepRejected); !ok || rejected.Effect != eff {
			t.Fatalf("facts[0] = %+v", facts[0])
		}
	})

	evolveRejects := []struct {
		name string
		fact run.Fact
		want string
	}{
		{"run ended over an executing model step", run.RunEnded{End: run.RunFailedEnd{Reason: run.ReasonProviderFailure, Failure: run.RunFailure{Class: run.FailureProvider}}}, "executes effect"},
		{"failure of another effect", run.ModelStepFailed{StepID: stepID, Effect: other, Failure: failure}, "settles effect"},
		{"failure without effect", run.ModelStepFailed{StepID: stepID, Failure: failure}, "without effect"},
		{"rejection of another effect", run.ModelStepRejected{StepID: stepID, Effect: other, Failure: failure}, "settles effect"},
	}
	for _, tc := range evolveRejects {
		t.Run(tc.name, func(t *testing.T) {
			_, err := schema.Machine().Evolve(model, tc.fact)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Evolve(%T) = %v, want error containing %q", tc.fact, err, tc.want)
			}
		})
	}
}
