package prompt

import "fmt"

// TokenBudget compares an estimated input token count against model capacity,
// reserving room for both output and a safety margin. The estimator and model
// capacity are supplied by the caller; this package does not tokenize requests.
// An opaque turn.PromptToken is a freshness token, not a token count.
type TokenBudget struct {
	ContextWindow int
	OutputReserve int
	SafetyMargin  int
	// InputLimit optionally imposes a tighter input limit; zero disables it.
	InputLimit int
}

// Exceeded reports whether inputTokens is strictly greater than the available
// input budget. Invalid counts or budgets return an error, even if another
// field would already make the request exceed the limit.
func (b TokenBudget) Exceeded(inputTokens int) (bool, error) {
	if b.ContextWindow <= 0 {
		return false, fmt.Errorf("prompt: context window must be positive")
	}
	if b.OutputReserve < 0 || b.SafetyMargin < 0 || b.InputLimit < 0 || inputTokens < 0 {
		return false, fmt.Errorf("prompt: negative token budget or input count")
	}
	// Subtract sequentially rather than summing reserves: even MaxInt values
	// must be validated without overflowing.
	limit := b.ContextWindow
	if b.OutputReserve > limit {
		return false, fmt.Errorf("prompt: output reserve exceeds context window")
	}
	limit -= b.OutputReserve
	if b.SafetyMargin > limit {
		return false, fmt.Errorf("prompt: safety margin exceeds remaining context window")
	}
	limit -= b.SafetyMargin
	if b.InputLimit > 0 && b.InputLimit < limit {
		limit = b.InputLimit
	}
	return inputTokens > limit, nil
}
