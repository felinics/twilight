// Package opencodego provides OpenCode Go using its documented per-model
// Completions, Responses and Anthropic Messages endpoints.
package opencodego

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"

	"github.com/felinics/twilight/internal/utils"
	"github.com/felinics/twilight/provider/anthropic/messages"
	"github.com/felinics/twilight/provider/openai/completions"
	"github.com/felinics/twilight/provider/openai/responses"
	"github.com/felinics/twilight/sdk"
)

const (
	defaultBaseURL = "https://opencode.ai/zen/go/v1"
	// SessionHeader carries a caller-owned ID that stays stable across a
	// conversation, including tool-result replays and retries.
	SessionHeader = "x-opencode-session"
)

// Provider routes requests to the existing protocol implementations. After
// construction it may be shared across conversations. Supply session headers
// using sdk.WithRequestHeaders on each conversation's context, and identify
// your application with a User-Agent through WithHeaders or your HTTP client.
type Provider struct {
	apiKey         string
	baseURL        string
	httpClient     *http.Client
	headers        map[string]string
	modelProtocols map[string]Protocol
	displayNames   map[string]string
	delegates      map[Protocol]sdk.Provider
}

var _ sdk.Provider = (*Provider)(nil)

type Option func(*Provider)

func WithAPIKey(apiKey string) Option {
	return func(p *Provider) { p.apiKey = apiKey }
}

// WithBaseURL replaces the base URL, including its /v1 prefix.
func WithBaseURL(baseURL string) Option {
	return func(p *Provider) { p.baseURL = baseURL }
}

func WithHTTPClient(client *http.Client) Option {
	return func(p *Provider) { p.httpClient = client }
}

// WithHeaders snapshots provider-wide headers. sdk.WithRequestHeaders takes
// precedence and should be used for session IDs on shared providers.
func WithHeaders(headers map[string]string) Option {
	headers = utils.MergeHeaders(headers)
	return func(p *Provider) { p.headers = headers }
}

// WithModelProtocols adds or overrides explicit model routes. The map is copied
// when this option is created. Unknown models and invalid protocols produce an
// error before generation; there is no prefix-based or default-protocol guess.
func WithModelProtocols(protocols map[string]Protocol) Option {
	protocols = maps.Clone(protocols)
	return func(p *Provider) { maps.Copy(p.modelProtocols, protocols) }
}

func New(options ...Option) *Provider {
	p := &Provider{
		baseURL:        defaultBaseURL,
		httpClient:     &http.Client{},
		modelProtocols: make(map[string]Protocol),
		displayNames:   make(map[string]string),
	}
	for _, model := range Catalog() {
		p.modelProtocols[model.ID] = model.Protocol
		p.displayNames[model.ID] = model.DisplayName
	}
	for _, option := range options {
		option(p)
	}
	p.delegates = map[Protocol]sdk.Provider{
		ProtocolCompletions: completions.New(
			completions.WithAPIKey(p.apiKey), completions.WithBaseURL(p.baseURL),
			completions.WithHTTPClient(p.httpClient), completions.WithHeaders(p.headers),
		),
		ProtocolResponses: responses.New(
			responses.WithAPIKey(p.apiKey), responses.WithBaseURL(p.baseURL),
			responses.WithHTTPClient(p.httpClient), responses.WithHeaders(p.headers),
		),
		ProtocolMessages: messages.New(
			messages.WithAPIKey(p.apiKey), messages.WithBaseURL(p.baseURL),
			messages.WithHTTPClient(p.httpClient), messages.WithHeaders(p.headers),
		),
	}
	return p
}

func (p *Provider) Name() string { return "opencode-go" }

func (p *Provider) ChatModel(id string) *sdk.Model {
	return &sdk.Model{ID: id, DisplayName: p.displayNames[id], Provider: p, Type: sdk.ModelTypeChat}
}

// ProtocolForModel exposes the same routing decision used by generation and
// probes, so applications do not need a second model-to-protocol catalog.
func (p *Provider) ProtocolForModel(id string) (Protocol, error) {
	protocol, ok := p.modelProtocols[id]
	if !ok {
		return "", fmt.Errorf("opencode-go: no protocol registered for model %q; use WithModelProtocols", id)
	}
	switch protocol {
	case ProtocolCompletions, ProtocolResponses, ProtocolMessages:
		return protocol, nil
	default:
		return "", fmt.Errorf("opencode-go: unsupported protocol %q for model %q", protocol, id)
	}
}

// ListModels queries the live endpoint. It also returns newly published models
// whose protocol is not yet known locally. Check ProtocolForModel before using
// those models and register a documented route with WithModelProtocols.
func (p *Provider) ListModels(ctx context.Context) ([]sdk.Model, error) {
	result, err := utils.FetchJSON[struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodGet,
		BaseURL: p.baseURL,
		Path:    "/models",
		Headers: p.requestHeaders(ctx),
	})
	if err != nil {
		return nil, fmt.Errorf("opencode-go: list models: %w", err)
	}
	models := make([]sdk.Model, 0, len(result.Data))
	for _, model := range result.Data {
		models = append(models, *p.ChatModel(model.ID))
	}
	return models, nil
}

// Test checks reachability through the public models endpoint. Use TestModel
// with a session context to validate credentials and actual generation.
func (p *Provider) Test(ctx context.Context) *sdk.ProviderTestResult {
	_, err := p.ListModels(ctx)
	if err == nil {
		return &sdk.ProviderTestResult{
			Status:  sdk.ProviderStatusOK,
			Message: "models endpoint reachable; use TestModel to verify authentication and generation",
		}
	}
	status := sdk.ProviderStatusUnreachable
	var apiErr *utils.APIError
	if errors.As(err, &apiErr) {
		status = sdk.ProviderStatusUnhealthy
	}
	return &sdk.ProviderTestResult{Status: status, Message: err.Error(), Error: err}
}

// TestModel makes a small generation request using the model's actual protocol.
// It does not interpret a rejected request (including a missing session header)
// as proof that generation works. This probe can incur upstream usage charges.
func (p *Provider) TestModel(ctx context.Context, modelID string) (*sdk.ModelTestResult, error) {
	maxTokens := 16
	_, err := p.DoGenerate(ctx, sdk.Request{
		Model:     modelID,
		Messages:  []sdk.Message{sdk.UserMessage("hi")},
		MaxTokens: &maxTokens,
	})
	if err != nil {
		return nil, fmt.Errorf("opencode-go: test model: %w", err)
	}
	return &sdk.ModelTestResult{Supported: true, Message: "supported"}, nil
}

func (p *Provider) DoGenerate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) { //nolint:gocritic // interface method
	if req.Model == "" {
		return sdk.ModelResult{}, fmt.Errorf("opencode-go: model is required")
	}
	delegate, err := p.providerForModel(req.Model)
	if err != nil {
		return sdk.ModelResult{}, err
	}
	return delegate.DoGenerate(ctx, p.requestForModel(delegate, req))
}

func (p *Provider) DoStream(ctx context.Context, req sdk.Request) (<-chan sdk.StreamPart, error) { //nolint:gocritic // interface method
	if req.Model == "" {
		return nil, fmt.Errorf("opencode-go: model is required")
	}
	delegate, err := p.providerForModel(req.Model)
	if err != nil {
		return nil, err
	}
	return delegate.DoStream(ctx, p.requestForModel(delegate, req))
}

func (p *Provider) providerForModel(id string) (sdk.Provider, error) {
	protocol, err := p.ProtocolForModel(id)
	if err != nil {
		return nil, err
	}
	return p.delegates[protocol], nil
}

// requestForModel hands the caller's "opencode-go" provider options to the
// delegate under its own namespace, the only one it reads; options keyed by
// any other namespace are not for this provider and are dropped.
//
// It also adapts a request to how the service's Completions routes behave, as
// observed against the live service rather than inferred from the model
// family:
//
//   - Developer messages are sent as system messages. The routes accept the
//     developer role, but several models silently ignore its content, while
//     every model honors system messages, including mid-conversation ones.
//   - A replayed assistant tool call always carries reasoning_content. Some
//     routes reject the request otherwise, which happens when persisted history
//     dropped the reasoning or the model emitted none; an empty value is
//     accepted by every route.
func (p *Provider) requestForModel(delegate sdk.Provider, req sdk.Request) sdk.Request { //nolint:gocritic // mirrors interface methods
	options := req.ProviderOptions[p.Name()]
	req.ProviderOptions = nil
	if len(options) > 0 {
		req.ProviderOptions = map[string]json.RawMessage{delegate.Name(): options}
	}
	if p.modelProtocols[req.Model] != ProtocolCompletions {
		return req
	}
	converted := slices.Clone(req.Messages)
	for i := range converted {
		switch converted[i].Role {
		case sdk.MessageRoleDeveloper:
			converted[i].Role = sdk.MessageRoleSystem
		case sdk.MessageRoleAssistant:
			converted[i].Content = padToolCallReasoning(converted[i].Content)
		}
	}
	req.Messages = converted
	return req
}

// padToolCallReasoning appends an empty Chat Completions reasoning block to a
// tool-call message that has none, without modifying the caller's parts.
func padToolCallReasoning(parts []sdk.MessagePart) []sdk.MessagePart {
	hasToolCall := false
	for _, part := range parts {
		switch part := part.(type) {
		case sdk.ToolCallPart:
			hasToolCall = true
		case sdk.ReasoningPart:
			if part.Format == sdk.ReasoningFormatOpenAIChat {
				return parts
			}
		}
	}
	if !hasToolCall {
		return parts
	}
	return append(slices.Clone(parts), sdk.ReasoningPart{Format: sdk.ReasoningFormatOpenAIChat})
}

// requestHeaders omits Authorization when no key is configured, so the public
// models endpoint is not sent an empty bearer token.
func (p *Provider) requestHeaders(ctx context.Context) map[string]string {
	var defaults map[string]string
	if p.apiKey != "" {
		defaults = utils.AuthHeader(p.apiKey)
	}
	return utils.RequestHeaders(ctx, defaults, p.headers)
}
