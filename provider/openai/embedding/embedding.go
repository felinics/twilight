package embedding

import (
	"context"
	"fmt"
	"net/http"

	"github.com/felinics/twilight/internal/utils"
	"github.com/felinics/twilight/sdk"
)

const defaultBaseURL = "https://api.openai.com/v1"

type Provider struct {
	headers        map[string]string
	apiKey         string
	baseURL        string
	httpClient     *http.Client
	prepareRequest func(*http.Request) error
}

type Option func(*Provider)

// WithHeaders sets provider-wide HTTP headers, overriding defaults. The map is
// copied when the option is created. Use sdk.WithRequestHeaders for call-scoped
// values such as session IDs; those take precedence over these headers.
func WithHeaders(headers map[string]string) Option {
	headers = utils.MergeHeaders(headers)
	return func(p *Provider) { p.headers = headers }
}

func WithAPIKey(apiKey string) Option {
	return func(p *Provider) { p.apiKey = apiKey }
}

// WithBedrockRegion enables AWS SigV4 authentication for Amazon Bedrock's
// OpenAI-compatible endpoint using the default AWS credential chain.
func WithBedrockRegion(region string) Option {
	return func(p *Provider) {
		p.prepareRequest = utils.NewBedrockDefaultCredentialsPreparer(region)
	}
}

// WithBedrockCredentials enables AWS SigV4 authentication for Amazon Bedrock's
// OpenAI-compatible endpoint using static credentials.
func WithBedrockCredentials(region, accessKeyID, secretAccessKey, sessionToken string) Option {
	return func(p *Provider) {
		p.prepareRequest = utils.NewBedrockStaticCredentialsPreparer(region, accessKeyID, secretAccessKey, sessionToken)
	}
}

func WithBaseURL(baseURL string) Option {
	return func(p *Provider) { p.baseURL = baseURL }
}

func WithHTTPClient(client *http.Client) Option {
	return func(p *Provider) { p.httpClient = client }
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

// DoEmbed implements sdk.EmbeddingProvider.
func (p *Provider) DoEmbed(ctx context.Context, params sdk.EmbedParams) (*sdk.EmbedResult, error) {
	if params.Model == nil {
		return nil, fmt.Errorf("openai: embedding model is required")
	}

	req := &embeddingRequest{
		Model:          params.Model.ID,
		Input:          params.Values,
		EncodingFormat: "float",
		Dimensions:     params.Dimensions,
	}

	resp, err := utils.FetchJSON[embeddingResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    "/embeddings",
		Headers: p.requestHeaders(ctx),
		Prepare: p.prepareRequest,
		Body:    req,
	})
	if err != nil {
		return nil, fmt.Errorf("openai: embeddings request failed: %w", err)
	}

	embeddings := make([][]float64, len(resp.Data))
	for i, d := range resp.Data {
		embeddings[i] = d.Embedding
	}

	return &sdk.EmbedResult{
		Embeddings: embeddings,
		Usage: sdk.EmbeddingUsage{
			Tokens: resp.Usage.PromptTokens,
		},
	}, nil
}

func (p *Provider) requestHeaders(ctx context.Context) map[string]string {
	var defaults map[string]string
	if p.prepareRequest == nil && p.apiKey != "" {
		defaults = utils.AuthHeader(p.apiKey)
	}
	return utils.RequestHeaders(ctx, defaults, p.headers)
}
