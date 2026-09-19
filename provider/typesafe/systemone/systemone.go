// Package systemone is a provider for System One models, which evaluate a
// state against typed questions and return structured answers instead of text.
//
// It speaks two dialects of the same API. By default it talks to TypeSafe
// directly; [WithVercelAIGateway] reaches the same model, Jev, through Vercel
// AI Gateway. Questions and answers are identical either way.
package systemone

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/felinics/twilight/internal/utils"
	"github.com/felinics/twilight/sdk"
)

const (
	// DefaultBaseURL is TypeSafe's own API root.
	DefaultBaseURL = "https://api.typesafe.ai/v1"
	// DefaultModel is TypeSafe's flagship System One model. It is an alias, so
	// the versioned ID that actually answered is reported on the result.
	DefaultModel = "jev-latest"

	// statusOverloaded is TypeSafe's non-standard "try again shortly".
	statusOverloaded = 529
)

// Provider is a System One backend. It implements [sdk.EvaluationProvider].
type Provider struct {
	apiKey     string
	baseURL    string
	baseURLSet bool
	gateway    bool
	httpClient *http.Client
	retry      *utils.RetryPolicy
	wire       wire
}

// Option configures a Provider.
type Option func(*Provider)

// WithAPIKey sets the credential. Against TypeSafe this is a TypeSafe API key;
// through Vercel AI Gateway it is a gateway key.
func WithAPIKey(apiKey string) Option {
	return func(p *Provider) { p.apiKey = apiKey }
}

// WithBaseURL overrides the API root. It wins over the default that
// [WithVercelAIGateway] would otherwise select, whatever order they are given
// in.
func WithBaseURL(baseURL string) Option {
	return func(p *Provider) {
		p.baseURL = baseURL
		p.baseURLSet = true
	}
}

func WithHTTPClient(client *http.Client) Option {
	return func(p *Provider) { p.httpClient = client }
}

// WithVercelAIGateway sends requests to Vercel AI Gateway in the AI SDK's
// evaluation-model dialect instead of to TypeSafe. Use [GatewayModel] as the
// model ID; model listing is not available through the gateway.
func WithVercelAIGateway() Option {
	return func(p *Provider) { p.gateway = true }
}

// WithMaxRetries sets how many times a request is retried after its first
// attempt. Zero disables retrying. Rate limits (429) and overload (529) are
// retried with exponential backoff, honouring Retry-After.
func WithMaxRetries(retries int) Option {
	return func(p *Provider) {
		if retries < 0 {
			retries = 0
		}
		p.retry.MaxAttempts = retries + 1
	}
}

func New(options ...Option) *Provider {
	p := &Provider{
		httpClient: &http.Client{},
		retry: &utils.RetryPolicy{
			MaxAttempts: 3,
			BaseDelay:   500 * time.Millisecond,
			MaxDelay:    8 * time.Second,
			RetryStatus: func(status int) bool {
				return status == http.StatusTooManyRequests ||
					status == statusOverloaded ||
					status == http.StatusServiceUnavailable
			},
		},
	}
	for _, opt := range options {
		opt(p)
	}

	p.wire = wire(typesafeWire{})
	if p.gateway {
		p.wire = gatewayWire{}
	}
	if !p.baseURLSet {
		p.baseURL = p.wire.defaultBaseURL()
	}
	return p
}

// Name returns a human-readable provider identifier.
func (p *Provider) Name() string {
	if p.gateway {
		return "typesafe-systemone-gateway"
	}
	return "typesafe-systemone"
}

// EvaluationModel creates an [sdk.EvaluationModel] bound to this provider.
// Use [DefaultModel] against TypeSafe and [GatewayModel] through the gateway.
func (p *Provider) EvaluationModel(id string) *sdk.EvaluationModel {
	return &sdk.EvaluationModel{ID: id, Provider: p}
}

// DoEvaluate implements [sdk.EvaluationProvider].
func (p *Provider) DoEvaluate(ctx context.Context, params sdk.EvaluateParams) (*sdk.EvaluateResult, error) {
	if err := params.Validate(); err != nil {
		return nil, err
	}

	body, err := p.wire.encodeRequest(params)
	if err != nil {
		return nil, err
	}

	raw, err := utils.FetchJSON[json.RawMessage](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    p.wire.evaluatePath(),
		Headers: p.requestHeaders(params.Model.ID),
		Body:    body,
		Retry:   p.retry,
	})
	if err != nil {
		return nil, fmt.Errorf("typesafe: evaluation request failed: %w", err)
	}

	return p.wire.decodeResponse(*raw, params.Model.ID)
}

// ListModels implements [sdk.EvaluationProvider]. Versioned IDs such as
// jev-1.13.0 are accepted by the API whether or not they appear here, so this
// list is a starting point rather than the set of valid model IDs.
func (p *Provider) ListModels(ctx context.Context) ([]*sdk.EvaluationModel, error) {
	if !p.wire.listsModels() {
		return nil, fmt.Errorf("typesafe: %s does not list models", p.Name())
	}

	resp, err := utils.FetchJSON[typesafeModelList](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodGet,
		BaseURL: p.baseURL,
		Path:    "/models",
		Headers: p.requestHeaders(""),
		Retry:   p.retry,
	})
	if err != nil {
		return nil, fmt.Errorf("typesafe: list models failed: %w", err)
	}

	models := make([]*sdk.EvaluationModel, 0, len(resp.Models))
	for _, m := range resp.Models {
		if m.Name != "" {
			models = append(models, p.EvaluationModel(m.Name))
		}
	}
	return models, nil
}

func (p *Provider) requestHeaders(modelID string) map[string]string {
	headers := map[string]string{}
	for k, v := range p.wire.headers(modelID) {
		headers[k] = v
	}
	if p.apiKey != "" {
		headers["Authorization"] = utils.BearerToken(p.apiKey)
	}
	return headers
}

var _ sdk.EvaluationProvider = (*Provider)(nil)
