package sdk

import "context"

// EmbeddingProvider is the interface that embedding backends must implement.
type EmbeddingProvider interface {
	DoEmbed(ctx context.Context, params EmbedParams) (*EmbedResult, error)
}

// EmbeddingModel is an embedding model served by a provider; chat models are
// Model. ID and Provider identify it, and Embed and EmbedMany call it through
// WithEmbeddingModel. The remaining fields describe it; they are optional, and
// a zero value means not stated.
type EmbeddingModel struct {
	ID          string
	DisplayName string
	Provider    EmbeddingProvider

	Description string
	// Dimensions is the size of the vectors the model returns by default.
	Dimensions int
	// MaxInputTokens is the most tokens one input value can hold.
	MaxInputTokens int
	// MaxEmbeddingsPerCall is the most values one request can embed.
	MaxEmbeddingsPerCall int
}

// EmbedParams holds the parameters for an embedding request.
type EmbedParams struct {
	Model      *EmbeddingModel
	Values     []string
	Dimensions *int
}

// EmbedResult holds the result of an embedding request.
type EmbedResult struct {
	Embeddings [][]float64
	Usage      EmbeddingUsage
}

// EmbeddingUsage tracks token usage for embedding requests.
type EmbeddingUsage struct {
	Tokens int
}
