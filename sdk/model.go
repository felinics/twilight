package sdk

import "context"

// Model is a chat model served by a provider; embedding models are
// EmbeddingModel.
type Model struct {
	ID          string
	DisplayName string
	Provider    Provider
}

// Test checks whether this model is supported by its provider.
func (m *Model) Test(ctx context.Context) (*ModelTestResult, error) {
	return m.Provider.TestModel(ctx, m.ID)
}
