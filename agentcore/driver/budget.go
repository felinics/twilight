package driver

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/model"
)

// BudgetPolicy is the default candidate-budget policy. The product supplies
// model metadata, a tokenizer/estimator and the summary prompt; the core applies
// the output reserve, safety margin and optional lower input threshold to the
// entire candidate, including system messages and tool definitions. This is an
// estimate, not a guarantee about a provider's eventual usage accounting.
type BudgetPolicy struct {
	Budget        func(context.Context, turn.ModelRef) (prompt.TokenBudget, error)
	Estimate      func(context.Context, *model.ModelRequest) (int, error)
	RetainEntries int
	Summary       func(context.Context, *CompactionContext) (SummaryRequest, error)
}

func (p *BudgetPolicy) Decide(ctx context.Context, _ prompt.Input, candidate *prompt.Prompt) (ContextDecision, error) {
	if p.Budget == nil || p.Estimate == nil || p.Summary == nil {
		return ContextDecision{}, errors.New("driver: budget policy requires limits, estimator and summary builder")
	}
	if p.RetainEntries < 0 {
		return ContextDecision{}, errors.New("driver: negative context retention")
	}
	modelRef := candidate.Model
	if modelRef == "" {
		modelRef = turn.ModelRef(candidate.Request.Model)
	}
	budget, err := p.Budget(ctx, modelRef)
	if err != nil {
		return ContextDecision{}, err
	}
	if _, err := budget.Exceeded(0); err != nil {
		return ContextDecision{}, err
	}
	if candidate.Request.MaxTokens != nil && *candidate.Request.MaxTokens < 0 {
		return ContextDecision{}, errors.New("driver: negative candidate output limit")
	}
	// The request's explicit output cap participates even when a product did
	// not reserve it separately. A product may reserve more conservatively.
	if candidate.Request.MaxTokens != nil && *candidate.Request.MaxTokens > budget.OutputReserve {
		budget.OutputReserve = *candidate.Request.MaxTokens
	}
	tokens, err := p.Estimate(ctx, &candidate.Request)
	if err != nil {
		return ContextDecision{}, err
	}
	compact, err := budget.Exceeded(tokens)
	return ContextDecision{Compact: compact, RetainEntries: p.RetainEntries}, err
}

func (p *BudgetPolicy) BuildSummary(ctx context.Context, snapshot *CompactionContext) (SummaryRequest, error) {
	if p.Summary == nil {
		return SummaryRequest{}, errors.New("driver: missing summary builder")
	}
	return p.Summary(ctx, snapshot)
}
