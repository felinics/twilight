// Package opencodego provides OpenCode Go through its Completions endpoint,
// and through Responses or Anthropic Messages for the models documented there.
package opencodego

import (
	"context"
	"encoding/json"
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

// WithModelProtocols adds or overrides per-model routes; models without one use
// Completions. The map is copied when this option is created. An invalid
// protocol produces an error before generation.
func WithModelProtocols(protocols map[string]Protocol) Option {
	protocols = maps.Clone(protocols)
	return func(p *Provider) { maps.Copy(p.modelProtocols, protocols) }
}

func New(options ...Option) *Provider {
	p := &Provider{
		baseURL:        defaultBaseURL,
		httpClient:     &http.Client{},
		modelProtocols: maps.Clone(protocolExceptions),
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
	return &sdk.Model{ID: id, Provider: p}
}

// ProtocolForModel exposes the same routing decision used by generation and
// probes, so applications do not need a second model-to-protocol catalog.
func (p *Provider) ProtocolForModel(id string) (Protocol, error) {
	protocol, ok := p.modelProtocols[id]
	if !ok {
		return ProtocolCompletions, nil
	}
	switch protocol {
	case ProtocolCompletions, ProtocolResponses, ProtocolMessages:
		return protocol, nil
	default:
		return "", fmt.Errorf("opencode-go: unsupported protocol %q for model %q", protocol, id)
	}
}

// ListModels queries the live endpoint, which carries no protocol metadata.
// Newly published models are routed to Completions unless registered with
// WithModelProtocols.
func (p *Provider) ListModels(ctx context.Context) ([]sdk.Model, error) {
	models, err := p.delegates[ProtocolCompletions].ListModels(ctx)
	if err != nil {
		return nil, fmt.Errorf("opencode-go: list models: %w", err)
	}
	for i := range models {
		models[i].Provider = p
	}
	return models, nil
}

// Test checks reachability through the public models endpoint, so nil does not
// validate credentials. Use TestModel with a session context to validate
// credentials and actual generation.
func (p *Provider) Test(ctx context.Context) error {
	if _, err := p.ListModels(ctx); err != nil {
		return fmt.Errorf("opencode-go: test: %w", err)
	}
	return nil
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
	protocol, err := p.ProtocolForModel(req.Model)
	if err != nil {
		return sdk.ModelResult{}, err
	}
	return p.delegates[protocol].DoGenerate(ctx, p.requestForModel(protocol, req))
}

func (p *Provider) DoStream(ctx context.Context, req sdk.Request) (<-chan sdk.StreamPart, error) { //nolint:gocritic // interface method
	if req.Model == "" {
		return nil, fmt.Errorf("opencode-go: model is required")
	}
	protocol, err := p.ProtocolForModel(req.Model)
	if err != nil {
		return nil, err
	}
	return p.delegates[protocol].DoStream(ctx, p.requestForModel(protocol, req))
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
func (p *Provider) requestForModel(protocol Protocol, req sdk.Request) sdk.Request { //nolint:gocritic // mirrors interface methods
	options := req.ProviderOptions[p.Name()]
	req.ProviderOptions = nil
	if len(options) > 0 {
		req.ProviderOptions = map[string]json.RawMessage{p.delegates[protocol].Name(): options}
	}
	if protocol != ProtocolCompletions {
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
