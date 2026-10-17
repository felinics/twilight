package driver_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/model"
)

func TestBudgetPolicyUsesCompleteCandidateAndOutputCap(t *testing.T) {
	ctx := context.Background()
	outputCap := 30
	candidate := prompt.Prompt{Request: model.ModelRequest{
		Model: "logical-model", System: "system", MaxTokens: &outputCap,
		Messages: []model.Message{{Role: model.MessageRoleUser, Content: []model.MessagePart{{Type: model.MessagePartTypeText, Text: "input"}}}},
		Tools:    []model.ToolDefinition{{Name: "tool"}},
	}}
	policy := &driver.BudgetPolicy{
		Budget: func(_ context.Context, ref turn.ModelRef) (prompt.TokenBudget, error) {
			if ref != "logical-model" {
				t.Fatalf("model = %q", ref)
			}
			return prompt.TokenBudget{ContextWindow: 100, OutputReserve: 10, SafetyMargin: 5}, nil
		},
		Estimate: func(_ context.Context, request *model.ModelRequest) (int, error) {
			if request.System != "system" || len(request.Tools) != 1 || len(request.Messages) != 1 {
				t.Fatalf("incomplete request: %+v", request)
			}
			return 66, nil
		},
		RetainEntries: 2,
		Summary: func(context.Context, *driver.CompactionContext) (driver.SummaryRequest, error) {
			return driver.SummaryRequest{}, nil
		},
	}
	decision, err := policy.Decide(ctx, prompt.Input{}, &candidate)
	if err != nil || !decision.Compact || decision.RetainEntries != 2 {
		t.Fatalf("decision = %+v, %v", decision, err)
	}
	outputCap = 20
	decision, err = policy.Decide(ctx, prompt.Input{}, &candidate)
	if err != nil || decision.Compact {
		t.Fatalf("smaller output cap = %+v, %v", decision, err)
	}
	// Do not silently ignore a negative explicit limit or overwrite an
	// invalid configured reserve with a larger request cap.
	outputCap = -1
	if _, err := policy.Decide(ctx, prompt.Input{}, &candidate); err == nil {
		t.Fatal("negative output cap accepted")
	}
	outputCap = 30
	policy.Budget = func(context.Context, turn.ModelRef) (prompt.TokenBudget, error) {
		return prompt.TokenBudget{ContextWindow: 100, OutputReserve: -1}, nil
	}
	if _, err := policy.Decide(ctx, prompt.Input{}, &candidate); err == nil {
		t.Fatal("negative reserve was hidden")
	}
}

func TestBudgetPolicyPropagatesConfigurationAndEstimatorFailures(t *testing.T) {
	ctx := context.Background()
	candidate := &prompt.Prompt{Model: "model", Request: model.ModelRequest{Model: "model"}}
	policy := &driver.BudgetPolicy{}
	if _, err := policy.Decide(ctx, prompt.Input{}, candidate); err == nil {
		t.Fatal("incomplete policy accepted")
	}
	if _, err := policy.BuildSummary(ctx, &driver.CompactionContext{}); err == nil {
		t.Fatal("missing summary builder accepted")
	}
	failure := errors.New("estimator unavailable")
	policy.Budget = func(context.Context, turn.ModelRef) (prompt.TokenBudget, error) {
		return prompt.TokenBudget{ContextWindow: 100}, nil
	}
	policy.Estimate = func(context.Context, *model.ModelRequest) (int, error) { return 0, failure }
	policy.Summary = func(context.Context, *driver.CompactionContext) (driver.SummaryRequest, error) {
		return driver.SummaryRequest{}, failure
	}
	if _, err := policy.Decide(ctx, prompt.Input{}, candidate); !errors.Is(err, failure) {
		t.Fatalf("estimator failure = %v", err)
	}
	if _, err := policy.BuildSummary(ctx, &driver.CompactionContext{}); !errors.Is(err, failure) {
		t.Fatalf("summary failure = %v", err)
	}
	policy.Budget = func(context.Context, turn.ModelRef) (prompt.TokenBudget, error) { return prompt.TokenBudget{}, failure }
	if _, err := policy.Decide(ctx, prompt.Input{}, candidate); !errors.Is(err, failure) {
		t.Fatalf("limits failure = %v", err)
	}
}
