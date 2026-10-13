package embedding

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/felinics/twilight/internal/errorformat"
	"github.com/felinics/twilight/internal/utils"
	"github.com/felinics/twilight/sdk"
)

const (
	defaultBaseURL = "https://generativelanguage.googleapis.com/v1beta"

	// providerName identifies this package in APIError.Provider.
	providerName = "google-embedding"
)

type Provider struct {
	apiKey     string
	baseURL    string
	httpClient *http.Client
	taskType   string
}

type Option func(*Provider)

func WithAPIKey(apiKey string) Option {
	return func(p *Provider) { p.apiKey = apiKey }
}

func WithBaseURL(baseURL string) Option {
	return func(p *Provider) { p.baseURL = baseURL }
}

func WithHTTPClient(client *http.Client) Option {
	return func(p *Provider) { p.httpClient = client }
}

// WithTaskType sets the default task type for all embedding requests.
// Supported values: SEMANTIC_SIMILARITY, CLASSIFICATION, CLUSTERING,
// RETRIEVAL_DOCUMENT, RETRIEVAL_QUERY, QUESTION_ANSWERING,
// FACT_VERIFICATION, CODE_RETRIEVAL_QUERY.
func WithTaskType(taskType string) Option {
	return func(p *Provider) { p.taskType = taskType }
}

func New(options ...Option) *Provider {
	p := &Provider{
		baseURL:    defaultBaseURL,
		httpClient: &http.Client{},
	}
	for _, opt := range options {
		opt(p)
	}
	return p
}

// EmbeddingModel creates an EmbeddingModel bound to this provider.
func (p *Provider) EmbeddingModel(id string) *sdk.EmbeddingModel {
	return &sdk.EmbeddingModel{
		ID:                   id,
		Provider:             p,
		MaxEmbeddingsPerCall: 2048,
	}
}

// ListModels lists the models that support embedContent.
func (p *Provider) ListModels(ctx context.Context) ([]sdk.EmbeddingModel, error) {
	resp, err := utils.FetchJSON[modelsListResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:      http.MethodGet,
		BaseURL:     p.baseURL,
		Path:        "/models",
		Headers:     p.authHeaders(),
		Provider:    providerName,
		DecodeError: errorformat.DecodeGoogle,
	})
	if err != nil {
		return nil, fmt.Errorf("google: list embedding models request failed: %w", err)
	}
	models := make([]sdk.EmbeddingModel, 0, len(resp.Models))
	for _, m := range resp.Models {
		if !supportsEmbed(m.SupportedGenerationMethods) {
			continue
		}
		model := p.EmbeddingModel(strings.TrimPrefix(m.Name, "models/"))
		model.DisplayName = m.DisplayName
		model.Description = m.Description
		model.MaxInputTokens = m.InputTokenLimit
		models = append(models, *model)
	}
	return models, nil
}

func supportsEmbed(methods []string) bool {
	for _, method := range methods {
		if method == "embedContent" {
			return true
		}
	}
	return false
}

// DoEmbed implements sdk.EmbeddingProvider.
// For a single value it calls the embedContent endpoint;
// for multiple values it calls batchEmbedContents.
func (p *Provider) DoEmbed(ctx context.Context, params sdk.EmbedParams) (*sdk.EmbedResult, error) {
	if params.Model == nil {
		return nil, fmt.Errorf("google: embedding model is required")
	}

	modelPath := getModelPath(params.Model.ID)

	if len(params.Values) == 1 {
		return p.doEmbedSingle(ctx, params, modelPath)
	}
	return p.doEmbedBatch(ctx, params, modelPath)
}

func (p *Provider) doEmbedSingle(ctx context.Context, params sdk.EmbedParams, modelPath string) (*sdk.EmbedResult, error) {
	req := &embedContentRequest{
		Model: modelPath,
		Content: content{
			Parts: []contentPart{{Text: params.Values[0]}},
		},
		OutputDimensionality: params.Dimensions,
		TaskType:             p.taskType,
	}

	resp, err := utils.FetchJSON[embedContentResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:      http.MethodPost,
		BaseURL:     p.baseURL,
		Path:        "/" + modelPath + ":embedContent",
		Headers:     p.authHeaders(),
		Body:        req,
		Provider:    providerName,
		DecodeError: errorformat.DecodeGoogle,
	})
	if err != nil {
		return nil, fmt.Errorf("google: embedContent request failed: %w", err)
	}

	return &sdk.EmbedResult{
		Embeddings: [][]float64{resp.Embedding.Values},
	}, nil
}

func (p *Provider) doEmbedBatch(ctx context.Context, params sdk.EmbedParams, modelPath string) (*sdk.EmbedResult, error) {
	requests := make([]embedContentRequest, len(params.Values))
	for i, v := range params.Values {
		requests[i] = embedContentRequest{
			Model: modelPath,
			Content: content{
				Role:  "user",
				Parts: []contentPart{{Text: v}},
			},
			OutputDimensionality: params.Dimensions,
			TaskType:             p.taskType,
		}
	}

	resp, err := utils.FetchJSON[batchEmbedContentsResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:      http.MethodPost,
		BaseURL:     p.baseURL,
		Path:        "/" + modelPath + ":batchEmbedContents",
		Headers:     p.authHeaders(),
		Body:        &batchEmbedContentsRequest{Requests: requests},
		Provider:    providerName,
		DecodeError: errorformat.DecodeGoogle,
	})
	if err != nil {
		return nil, fmt.Errorf("google: batchEmbedContents request failed: %w", err)
	}

	embeddings := make([][]float64, len(resp.Embeddings))
	for i, e := range resp.Embeddings {
		embeddings[i] = e.Values
	}

	return &sdk.EmbedResult{
		Embeddings: embeddings,
	}, nil
}

func (p *Provider) authHeaders() map[string]string {
	return map[string]string{
		"x-goog-api-key": p.apiKey,
	}
}

func getModelPath(modelID string) string {
	if strings.Contains(modelID, "/") {
		return modelID
	}
	return "models/" + modelID
}
