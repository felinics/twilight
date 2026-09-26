package copilot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/felinics/twilight/internal/messagecompat"
	"github.com/felinics/twilight/internal/utils"
	"github.com/felinics/twilight/sdk"
)

const defaultBaseURL = "https://api.githubcopilot.com"

type Provider struct {
	githubToken  string
	baseURL      string
	httpClient   *http.Client
	messageRoles sdk.MessageRoleCapabilities
}

type Option func(*Provider)

// WithGitHubToken configures the bearer token issued by GitHub Copilot for an agent request.
func WithGitHubToken(token string) Option {
	return func(p *Provider) {
		p.githubToken = token
	}
}

// WithAPIKey is an alias for WithGitHubToken so callers can swap providers with minimal call-site changes.
func WithAPIKey(token string) Option {
	return WithGitHubToken(token)
}

func WithBaseURL(baseURL string) Option {
	return func(p *Provider) {
		p.baseURL = baseURL
	}
}

func WithHTTPClient(client *http.Client) Option {
	return func(p *Provider) {
		p.httpClient = client
	}
}

// WithMessageRoleCapabilities enables instruction roles supported by the
// configured Copilot endpoint. The default conservatively falls back developer
// and mid-conversation system messages to user messages.
func WithMessageRoleCapabilities(capabilities sdk.MessageRoleCapabilities) Option {
	return func(p *Provider) {
		p.messageRoles = capabilities
	}
}

func New(options ...Option) *Provider {
	provider := &Provider{
		baseURL:    defaultBaseURL,
		httpClient: &http.Client{},
	}
	for _, option := range options {
		option(provider)
	}
	return provider
}

func (p *Provider) Name() string {
	return "github-copilot"
}

func (p *Provider) ListModels(context.Context) ([]sdk.Model, error) {
	models := Catalog()
	out := make([]sdk.Model, 0, len(models))
	for _, m := range models {
		out = append(out, sdk.Model{
			ID:          m.ID,
			DisplayName: m.DisplayName,
			Provider:    p,
			Type:        sdk.ModelTypeChat,
		})
	}
	return out, nil
}

func (p *Provider) Test(ctx context.Context) *sdk.ProviderTestResult {
	_, err := p.TestModel(ctx, AutoModel)
	if err != nil {
		return classifyError(err)
	}
	return &sdk.ProviderTestResult{Status: sdk.ProviderStatusOK, Message: "ok"}
}

func (p *Provider) TestModel(ctx context.Context, modelID string) (*sdk.ModelTestResult, error) {
	req, err := p.buildRequest(&sdk.Request{
		Model:    p.ChatModel(modelID).ID,
		Messages: []sdk.Message{sdk.UserMessage("ping")},
	})
	if err != nil {
		return nil, fmt.Errorf("github-copilot: build probe request: %w", err)
	}

	status, err := utils.ProbeStatus(ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    "/chat/completions",
		Headers: p.authHeaders(),
		Body:    req,
	})
	if err != nil {
		return nil, fmt.Errorf("github-copilot: probe model request failed: %w", err)
	}
	return sdk.ClassifyProbeStatus(status)
}

func (p *Provider) ChatModel(id string) *sdk.Model {
	if id == "" {
		id = AutoModel
	}
	return &sdk.Model{
		ID:       id,
		Provider: p,
		Type:     sdk.ModelTypeChat,
	}
}

func (p *Provider) DoGenerate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) { //nolint:gocritic // interface method
	if req.Model == "" {
		return sdk.ModelResult{}, fmt.Errorf("github-copilot: model is required")
	}

	wire, err := p.buildRequest(&req)
	if err != nil {
		return sdk.ModelResult{}, fmt.Errorf("github-copilot: build request: %w", err)
	}

	resp, err := utils.FetchJSON[chatResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    "/chat/completions",
		Headers: p.authHeaders(),
		Body:    wire,
	})
	if err != nil {
		var apiErr *utils.APIError
		if errors.As(err, &apiErr) {
			return sdk.ModelResult{}, fmt.Errorf("github-copilot: chat completions request failed: %s", apiErr.Detail())
		}
		return sdk.ModelResult{}, fmt.Errorf("github-copilot: chat completions request failed: %w", err)
	}

	return p.parseResponse(resp)
}

func (p *Provider) buildRequest(request *sdk.Request) (*chatRequest, error) {
	messages, err := messagecompat.Normalize(request.Messages, p.messageRoles)
	if err != nil {
		return nil, err
	}
	req := &chatRequest{
		Model:            request.Model,
		Messages:         convertMessages(request.System, messages),
		Temperature:      request.Temperature,
		TopP:             request.TopP,
		MaxTokens:        request.MaxTokens,
		FrequencyPenalty: request.FrequencyPenalty,
		PresencePenalty:  request.PresencePenalty,
		Seed:             request.Seed,
		ReasoningEffort:  request.ReasoningEffort,
	}
	if req.Model == AutoModel {
		req.Model = ""
	}
	if len(request.StopSequences) > 0 {
		req.Stop = request.StopSequences
	}
	if len(request.Tools) > 0 {
		req.Tools = convertTools(request.Tools)
		req.ToolChoice = convertToolChoice(request.ToolChoice)
	}
	if request.ResponseFormat != nil {
		req.ResponseFormat = &chatRespFormat{
			Type:       string(request.ResponseFormat.Type),
			JSONSchema: request.ResponseFormat.JSONSchema,
		}
	}
	if err := sdk.ApplyProviderOptions(p.Name(), request.ProviderOptions, req); err != nil {
		return nil, fmt.Errorf("copilot: %w", err)
	}
	return req, nil
}

// convertToolChoice maps the SDK's closed ToolChoice onto the OpenAI wire form:
// an unset mode leaves tool_choice unset, the three scalar modes are the string
// itself, and a named tool is the function object.
func convertToolChoice(choice sdk.ToolChoice) any {
	switch choice.Mode {
	case "":
		return nil
	case sdk.ToolChoiceTool:
		return chatToolChoiceFunction{
			Type:     "function",
			Function: chatToolChoiceName{Name: choice.Tool},
		}
	default:
		return string(choice.Mode)
	}
}

func convertTools(tools []sdk.ToolDefinition) []chatTool {
	out := make([]chatTool, 0, len(tools))
	for _, t := range tools {
		out = append(out, chatTool{
			Type: "function",
			Function: chatFunction{
				Name:        t.Name,
				Description: t.Description,
				Parameters:  t.Parameters,
			},
		})
	}
	return out
}

func convertMessages(system string, messages []sdk.Message) []chatMessage {
	var out []chatMessage

	if system != "" {
		out = append(out, chatMessage{
			Role:    "system",
			Content: system,
		})
	}

	for _, msg := range messages {
		out = append(out, convertMessage(msg)...)
	}
	return out
}

func convertMessage(msg sdk.Message) []chatMessage {
	switch msg.Role {
	case sdk.MessageRoleTool:
		return convertToolResultMessages(msg)
	case sdk.MessageRoleAssistant:
		return []chatMessage{convertAssistantMessage(msg)}
	default:
		return []chatMessage{{
			Role:    string(msg.Role),
			Content: convertContent(msg.Content),
		}}
	}
}

func convertAssistantMessage(msg sdk.Message) chatMessage {
	cm := chatMessage{Role: "assistant"}

	var contentParts []sdk.MessagePart
	var toolCalls []chatToolCall
	var reasoning, reasoningOpaque string

	for _, part := range msg.Content {
		switch p := part.(type) {
		case sdk.ToolCallPart:
			id := p.ToolCallID
			if id == "" {
				id = generateID()
			}
			toolCalls = append(toolCalls, chatToolCall{
				ID:   id,
				Type: "function",
				Function: chatFunctionCall{
					Name:      p.ToolName,
					Arguments: string(p.Input.Object()),
				},
			})
		case sdk.ReasoningPart:
			if p.Format != sdk.ReasoningFormatCopilot {
				continue
			}
			if opaque := reasoningOpaqueOf(p.ProviderMetadata); opaque != "" {
				reasoningOpaque = opaque
				reasoning += p.Text
			}
		default:
			contentParts = append(contentParts, part)
		}
	}

	if len(contentParts) > 0 {
		cm.Content = convertContent(contentParts)
	}
	// The API accepts reasoning_text only together with its opaque token.
	if reasoningOpaque != "" {
		cm.ReasoningOpaque = reasoningOpaque
		cm.ReasoningText = reasoning
	}
	if len(toolCalls) > 0 {
		cm.ToolCalls = toolCalls
	}

	return cm
}

func convertToolResultMessages(msg sdk.Message) []chatMessage {
	var out []chatMessage
	for _, part := range msg.Content {
		if trp, ok := part.(sdk.ToolResultPart); ok {
			out = append(out, chatMessage{
				Role:       "tool",
				ToolCallID: trp.ToolCallID,
				Content:    trp.Result.String(),
			})
		}
	}
	return out
}

func convertContent(parts []sdk.MessagePart) any {
	if len(parts) == 1 {
		if tp, ok := parts[0].(sdk.TextPart); ok {
			return tp.Text
		}
	}

	out := make([]any, 0, len(parts))
	for _, part := range parts {
		switch p := part.(type) {
		case sdk.TextPart:
			out = append(out, chatContentPartText{Type: "text", Text: p.Text})
		case sdk.ImagePart:
			out = append(out, chatContentPartImage{
				Type:     "image_url",
				ImageURL: chatImageURL{URL: p.Image},
			})
		case sdk.FilePart:
			// Copilot has no confirmed native file input. Emit an explicit
			// marker instead of the raw payload: silently sending megabytes of
			// base64 as "text" is exactly the failure mode file parts exist to
			// fix. Swap this for a native part once upstream support lands.
			out = append(out, chatContentPartText{Type: "text", Text: utils.OmittedFileNotice(p.Filename, p.MediaType)})
		}
	}
	return out
}

func (p *Provider) parseResponse(resp *chatResponse) (sdk.ModelResult, error) {
	result := sdk.ModelResult{
		Usage: convertUsage(&resp.Usage),
		Response: sdk.ResponseMetadata{
			ID:        resp.ID,
			ModelID:   resp.Model,
			Timestamp: sdk.TimestampFromUnix(resp.Created),
		},
	}

	if len(resp.Choices) > 0 {
		choice := resp.Choices[0]
		result.Text = choice.Message.Content
		result.Reasoning = reasoningFromMessage(&choice.Message)
		if result.Reasoning != "" || choice.Message.ReasoningOpaque != "" {
			result.ReasoningParts = []sdk.ReasoningPart{{
				Text:             result.Reasoning,
				Format:           sdk.ReasoningFormatCopilot,
				Model:            resp.Model,
				ProviderMetadata: reasoningOpaqueMetadata(choice.Message.ReasoningOpaque),
			}}
		}
		result.FinishReason = mapFinishReason(choice.FinishReason)
		result.RawFinishReason = choice.FinishReason

		for _, tc := range choice.Message.ToolCalls {
			input := sdk.ParseToolArguments(tc.Function.Arguments)
			id := tc.ID
			if id == "" {
				id = generateID()
			}
			result.ToolCalls = append(result.ToolCalls, sdk.ToolCall{
				ToolCallID: id,
				ToolName:   tc.Function.Name,
				Input:      input,
			})
		}

		for _, img := range choice.Message.Images {
			url := img.ImageURL.URL
			if url == "" {
				continue
			}
			mediaType, data := parseDataURL(url)
			result.Files = append(result.Files, sdk.GeneratedFile{
				Data:      data,
				MediaType: mediaType,
			})
		}
	}

	return result, nil
}

func (p *Provider) DoStream(ctx context.Context, req sdk.Request) (<-chan sdk.StreamPart, error) { //nolint:gocritic // interface method
	if req.Model == "" {
		return nil, fmt.Errorf("github-copilot: model is required")
	}

	wire, err := p.buildRequest(&req)
	if err != nil {
		return nil, fmt.Errorf("github-copilot: build request: %w", err)
	}
	wire.Stream = true
	wire.StreamOptions = &chatStreamOptions{IncludeUsage: true}

	ch := make(chan sdk.StreamPart, 64)

	go func() {
		defer close(ch)

		sp := &streamProcessor{
			ctx:              ctx,
			ch:               ch,
			pendingToolCalls: map[int]*streamingToolCall{},
		}

		if !sp.send(&sdk.StartPart{}) {
			return
		}
		if !sp.send(&sdk.StartStepPart{}) {
			return
		}

		err := utils.FetchSSE(ctx, p.httpClient, &utils.RequestOptions{
			Method:  http.MethodPost,
			BaseURL: p.baseURL,
			Path:    "/chat/completions",
			Headers: p.authHeaders(),
			Body:    wire,
		}, func(ev *utils.SSEEvent) error {
			if ev.Data == "[DONE]" {
				return utils.ErrStreamDone
			}

			var chunk chatChunkResponse
			if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
				sp.send(&sdk.ErrorPart{Error: fmt.Errorf("github-copilot: unmarshal chunk: %w", err)})
				return err
			}

			return sp.processChunk(&chunk)
		})

		if err != nil {
			var apiErr *utils.APIError
			if errors.As(err, &apiErr) {
				sp.send(&sdk.ErrorPart{Error: fmt.Errorf("github-copilot: stream failed: %s", apiErr.Detail())})
			} else {
				sp.send(&sdk.ErrorPart{Error: fmt.Errorf("github-copilot: stream failed: %w", err)})
			}
		}

		sp.flush()

		sp.send(&sdk.FinishPart{
			FinishReason:    sp.finishReason,
			RawFinishReason: sp.rawFinishReason,
			TotalUsage:      sp.usage,
		})
	}()

	return ch, nil
}

// streamingToolCall accumulates one function call's argument deltas. args
// uses strings.Builder because it grows by one small delta per SSE event;
// instances are always held by pointer (pendingToolCalls map).
type streamingToolCall struct {
	id       string
	name     string
	args     strings.Builder
	finished bool
}

func reasoningFromMessage(m *chatRespMessage) string {
	switch {
	case m.ReasoningText != "":
		return m.ReasoningText
	case m.ReasoningContent != "":
		return m.ReasoningContent
	default:
		return m.Reasoning
	}
}

func reasoningFromDelta(d *chatChunkDelta) string {
	switch {
	case d.ReasoningText != "":
		return d.ReasoningText
	case d.ReasoningContent != "":
		return d.ReasoningContent
	default:
		return d.Reasoning
	}
}

const (
	metadataNamespace = "copilot"
	// metadataKeyOpaque holds Copilot's reasoning_opaque: the upstream model's
	// signature, wrapped uniformly regardless of which model produced it.
	metadataKeyOpaque = "reasoningOpaque"
)

func reasoningOpaqueMetadata(opaque string) sdk.ProviderMetadata {
	return sdk.NewProviderMetadata(metadataNamespace, map[string]string{metadataKeyOpaque: opaque})
}

func reasoningOpaqueOf(meta sdk.ProviderMetadata) string {
	return meta.Get(metadataNamespace, metadataKeyOpaque)
}

func convertUsage(u *chatUsage) sdk.Usage {
	usage := sdk.Usage{
		InputTokens:  u.PromptTokens,
		OutputTokens: u.CompletionTokens,
		TotalTokens:  u.TotalTokens,
	}
	if u.PromptTokensDetails != nil {
		usage.CachedInputTokens = u.PromptTokensDetails.CachedTokens
		usage.InputTokenDetails.CacheReadTokens = u.PromptTokensDetails.CachedTokens
	}
	if u.CompletionTokensDetails != nil {
		usage.ReasoningTokens = u.CompletionTokensDetails.ReasoningTokens
		usage.OutputTokenDetails.ReasoningTokens = u.CompletionTokensDetails.ReasoningTokens
		usage.OutputTokenDetails.TextTokens = u.CompletionTokensDetails.TextTokens
	}
	return usage
}

func mapFinishReason(reason string) sdk.FinishReason {
	switch reason {
	case "stop":
		return sdk.FinishReasonStop
	case "length":
		return sdk.FinishReasonLength
	case "content_filter":
		return sdk.FinishReasonContentFilter
	case "tool_calls":
		return sdk.FinishReasonToolCalls
	default:
		return sdk.FinishReasonUnknown
	}
}

func classifyError(err error) *sdk.ProviderTestResult {
	var apiErr *utils.APIError
	if errors.As(err, &apiErr) {
		if apiErr.StatusCode == http.StatusUnauthorized || apiErr.StatusCode == http.StatusForbidden {
			return &sdk.ProviderTestResult{
				Status:  sdk.ProviderStatusUnhealthy,
				Message: fmt.Sprintf("authentication failed: %s", apiErr.Message),
				Error:   err,
			}
		}
		return &sdk.ProviderTestResult{
			Status:  sdk.ProviderStatusUnhealthy,
			Message: fmt.Sprintf("service error (%d): %s", apiErr.StatusCode, apiErr.Message),
			Error:   err,
		}
	}
	return &sdk.ProviderTestResult{
		Status:  sdk.ProviderStatusUnreachable,
		Message: fmt.Sprintf("connection failed: %s", err.Error()),
		Error:   err,
	}
}

func (p *Provider) authHeaders() map[string]string {
	return map[string]string{
		"Authorization": utils.BearerToken(p.githubToken),
	}
}
