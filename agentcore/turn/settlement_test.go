package turn_test

import (
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/schema"
)

// every fact that closes an effect names it, and TurnEnded closes
// none. A model failure and a cancellation settle the executing model effect
// with ModelStepFailed before the Turn ends; a rejection names the effect it
// settles; Evolve refuses a TurnEnded over an Executing target and a
// settlement naming another effect.
func TestEveryEffectEndsUnderItsOwnSettlement(t *testing.T) {
	s := newTurn(t)
	model, stepID := advanceToExecuting(t, s, testRequest(), nil)
	eff := model.Current.(turn.ModelStep).Effect
	other := schema.Identity().DeriveEffectID(model.TurnID, stepID, "", 7)
	failure := turn.StepFailure{Class: turn.FailureProvider, Message: "down"}

	t.Run("model failure", func(t *testing.T) {
		facts := mustDecide(t, model, turn.SubmitModelFailure{StepID: stepID, Effect: eff, Failure: failure})
		if len(facts) != 2 {
			t.Fatalf("facts = %d, want [ModelStepFailed, TurnEnded]", len(facts))
		}
		failed, ok := facts[0].(turn.ModelStepFailed)
		if !ok || failed.Effect != eff || failed.Failure != failure {
			t.Fatalf("facts[0] = %+v", facts[0])
		}
		if end, ok := facts[1].(turn.TurnEnded); !ok || end.End.(turn.TurnFailedEnd).Reason != turn.ReasonProviderFailure {
			t.Fatalf("facts[1] = %+v", facts[1])
		}
		if after := fold(t, model, facts); after.Status != turn.TurnFailed || after.ModelSteps != 1 {
			t.Fatalf("after = %+v", after)
		}
	})
	t.Run("cancel while executing", func(t *testing.T) {
		facts := mustDecide(t, model, turn.CancelTurn{})
		if len(facts) != 2 {
			t.Fatalf("facts = %d, want [ModelStepFailed, TurnEnded]", len(facts))
		}
		failed, ok := facts[0].(turn.ModelStepFailed)
		if !ok || failed.Effect != eff || failed.Failure.Class != turn.FailureEffectUnknown {
			t.Fatalf("facts[0] = %+v", facts[0])
		}
		if end := facts[1].(turn.TurnEnded).End.(turn.TurnStoppedEnd); end.UncertainModel != stepID {
			t.Fatalf("end = %+v", end)
		}
		fold(t, model, facts)
	})
	t.Run("rejection names the effect", func(t *testing.T) {
		facts := mustDecide(t, model, turn.RejectModelResult{StepID: stepID, Effect: eff, Failure: failure, Disposition: turn.ModelRejectRetry})
		if rejected, ok := facts[0].(turn.ModelStepRejected); !ok || rejected.Effect != eff {
			t.Fatalf("facts[0] = %+v", facts[0])
		}
	})

	evolveRejects := []struct {
		name string
		fact turn.Fact
		want string
	}{
		{"Turn ended over an executing model step", turn.TurnEnded{End: turn.TurnFailedEnd{Reason: turn.ReasonProviderFailure, Failure: turn.TurnFailure{Class: turn.FailureProvider}}}, "executes effect"},
		{"failure of another effect", turn.ModelStepFailed{StepID: stepID, Effect: other, Failure: failure}, "settles effect"},
		{"failure without effect", turn.ModelStepFailed{StepID: stepID, Failure: failure}, "without effect"},
		{"rejection of another effect", turn.ModelStepRejected{StepID: stepID, Effect: other, Failure: failure}, "settles effect"},
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
