package images

import (
	"context"
	"fmt"
	"net/http"

	"github.com/felinics/twilight/internal/utils"
	"github.com/felinics/twilight/sdk"
)

const defaultBaseURL = "https://api.openai.com/v1"

// Provider implements sdk.ImageGenerationProvider and sdk.ImageEditProvider
// for the OpenAI Images API.
type Provider struct {
	headers    map[string]string
	apiKey     string
	baseURL    string
	httpClient *http.Client
}

// Option configures the Provider.
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

func WithBaseURL(baseURL string) Option {
	return func(p *Provider) { p.baseURL = baseURL }
}

func WithHTTPClient(client *http.Client) Option {
	return func(p *Provider) { p.httpClient = client }
}

// New creates a new OpenAI Images provider.
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

// GenerationModel creates an ImageGenerationModel bound to this provider.
func (p *Provider) GenerationModel(id string) *sdk.ImageGenerationModel {
	return &sdk.ImageGenerationModel{
		ID:       id,
		Provider: p,
	}
}

// EditModel creates an ImageEditModel bound to this provider.
func (p *Provider) EditModel(id string) *sdk.ImageEditModel {
	return &sdk.ImageEditModel{
		ID:       id,
		Provider: p,
	}
}

// DoGenerate implements sdk.ImageGenerationProvider.
func (p *Provider) DoGenerate(ctx context.Context, params *sdk.ImageGenerationParams) (*sdk.ImageResult, error) {
	if params.Model == nil {
		return nil, fmt.Errorf("openai images: generation model is required")
	}

	req := &generationRequest{
		Model:             params.Model.ID,
		Prompt:            params.Prompt,
		N:                 params.N,
		Size:              params.Size,
		Quality:           params.Quality,
		Style:             params.Style,
		ResponseFormat:    params.ResponseFormat,
		Background:        params.Background,
		OutputFormat:      params.OutputFormat,
		OutputCompression: params.OutputCompression,
		Moderation:        params.Moderation,
		User:              params.User,
	}

	resp, err := utils.FetchJSON[imagesResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    "/images/generations",
		Headers: p.requestHeaders(ctx),
		Body:    req,
	})
	if err != nil {
		return nil, fmt.Errorf("openai images: generation request failed: %w", err)
	}

	return toImageResult(resp), nil
}

// DoEdit implements sdk.ImageEditProvider.
// It uses multipart/form-data when any ImageInput carries raw Data bytes,
// and falls back to a JSON request body when inputs use URL/FileID references.
func (p *Provider) DoEdit(ctx context.Context, params *sdk.ImageEditParams) (*sdk.ImageResult, error) {
	if params.Model == nil {
		return nil, fmt.Errorf("openai images: edit model is required")
	}

	if needsMultipart(params) {
		return p.doEditMultipart(ctx, params)
	}
	return p.doEditJSON(ctx, params)
}

func (p *Provider) doEditJSON(ctx context.Context, params *sdk.ImageEditParams) (*sdk.ImageResult, error) {
	req := &editRequest{
		Model:             params.Model.ID,
		Prompt:            params.Prompt,
		N:                 params.N,
		Size:              params.Size,
		Quality:           params.Quality,
		Background:        params.Background,
		OutputFormat:      params.OutputFormat,
		OutputCompression: params.OutputCompression,
		InputFidelity:     params.InputFidelity,
		Moderation:        params.Moderation,
		ResponseFormat:    params.ResponseFormat,
		User:              params.User,
	}

	for i := range params.Images {
		req.Images = append(req.Images, toImageRef(&params.Images[i]))
	}
	if params.Mask != nil {
		ref := toImageRef(params.Mask)
		req.Mask = &ref
	}

	resp, err := utils.FetchJSON[imagesResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    "/images/edits",
		Headers: p.requestHeaders(ctx),
		Body:    req,
	})
	if err != nil {
		return nil, fmt.Errorf("openai images: edit request failed: %w", err)
	}

	return toImageResult(resp), nil
}

func toImageRef(input *sdk.ImageInput) imageRef {
	return imageRef{
		URL:    input.URL,
		FileID: input.FileID,
	}
}

func toImageResult(resp *imagesResponse) *sdk.ImageResult {
	result := &sdk.ImageResult{
		Created: resp.Created,
		Data:    make([]sdk.ImageData, len(resp.Data)),
	}
	for i, d := range resp.Data {
		result.Data[i] = sdk.ImageData{
			B64JSON:       d.B64JSON,
			URL:           d.URL,
			RevisedPrompt: d.RevisedPrompt,
		}
	}
	if resp.Usage != nil {
		result.Usage = sdk.ImageUsage{
			TotalTokens:  resp.Usage.TotalTokens,
			InputTokens:  resp.Usage.InputTokens,
			OutputTokens: resp.Usage.OutputTokens,
		}
		if resp.Usage.InputTokenDetails != nil {
			result.Usage.InputTokenDetails = &sdk.ImageInputTokenDetails{
				TextTokens:  resp.Usage.InputTokenDetails.TextTokens,
				ImageTokens: resp.Usage.InputTokenDetails.ImageTokens,
			}
		}
	}
	return result
}

// needsMultipart returns true if any image input carries raw bytes.
func needsMultipart(params *sdk.ImageEditParams) bool {
	for _, img := range params.Images {
		if len(img.Data) > 0 {
			return true
		}
	}
	if params.Mask != nil && len(params.Mask.Data) > 0 {
		return true
	}
	return false
}

func (p *Provider) requestHeaders(ctx context.Context) map[string]string {
	return utils.RequestHeaders(ctx, utils.AuthHeader(p.apiKey), p.headers)
}
