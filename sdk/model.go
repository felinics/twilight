package sdk

import "context"

// Model is a chat model served by a provider; embedding models are
// EmbeddingModel. ID and Provider identify it. The remaining fields describe
// what it can do; they are optional, and a zero value means not stated.
// Providers fill what their API reports, and a caller fills the rest from its
// own model catalog, for example to configure replay with the model's
// Reasoning.Replay.
type Model struct {
	ID          string
	DisplayName string
	Provider    Provider

	Description string
	Modalities  Modalities
	// ContextWindow is the context window in tokens, input and output together.
	ContextWindow int
	// MaxOutputTokens is the most tokens one response can produce.
	MaxOutputTokens int
	ToolCall        bool
	// StructuredOutput reports support for a JSON schema response format.
	StructuredOutput bool
	// Reasoning is nil when the model does not reason or it is not stated.
	Reasoning *ModelReasoning
}

// Test checks whether this model is supported by its provider.
func (m *Model) Test(ctx context.Context) (*ModelTestResult, error) {
	return m.Provider.TestModel(ctx, m.ID)
}

// Accepts reports whether the model takes the modality as input.
func (m *Model) Accepts(modality Modality) bool {
	return containsModality(m.Modalities.Input, modality)
}

// Produces reports whether the model returns the modality as output.
func (m *Model) Produces(modality Modality) bool {
	return containsModality(m.Modalities.Output, modality)
}
