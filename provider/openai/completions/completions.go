package completions

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

const (
	pathChatCompletions = "/chat/completions"
	thinkingDisabled    = "disabled"
	toolTypeFunction    = "function"
	keyType             = "type"
	roleAssistant       = "assistant"
)

const defaultBaseURL = "https://api.openai.com/v1"

type Provider struct {
	apiKey         string
	baseURL        string
	httpClient     *http.Client
	prepareRequest func(*http.Request) error
	compat         chatCompletionsCompat
	messageRoles   sdk.MessageRoleCapabilities
}

type Option func(*Provider)

type chatCompletionsCompat string

const (
	chatCompletionsCompatDeepSeek chatCompletionsCompat = "deepseek"
	chatCompletionsCompatMiniMax  chatCompletionsCompat = "minimax"
	chatCompletionsCompatKimi     chatCompletionsCompat = "kimi"
)

func WithAPIKey(apiKey string) Option {
	return func(p *Provider) {
		p.apiKey = apiKey
	}
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
	return func(p *Provider) {
		p.baseURL = baseURL
	}
}

func WithHTTPClient(client *http.Client) Option {
	return func(p *Provider) {
		p.httpClient = client
	}
}

// WithMessageRoleCapabilities overrides the instruction roles supported by an
// OpenAI-compatible Chat Completions endpoint. The OpenAI default supports
// developer messages and system messages anywhere in the conversation.
func WithMessageRoleCapabilities(capabilities sdk.MessageRoleCapabilities) Option {
	return func(p *Provider) {
		p.messageRoles = capabilities
	}
}

// WithDeepSeekChatCompletionsCompat maps reasoning_effort "none" to DeepSeek's
// thinking disable toggle while keeping the generic Chat Completions provider.
func WithDeepSeekChatCompletionsCompat() Option {
	return func(p *Provider) {
		p.compat = chatCompletionsCompatDeepSeek
	}
}

// WithMiniMaxChatCompletionsCompat adapts MiniMax's OpenAI-compatible Chat
// Completions transport: it sends reasoning_split=true so thinking is returned
// in reasoning_details instead of inline <think> tags, and maps reasoning
// efforts onto MiniMax's thinking toggle (none -> disabled, otherwise adaptive).
func WithMiniMaxChatCompletionsCompat() Option {
	return func(p *Provider) {
		p.compat = chatCompletionsCompatMiniMax
	}
}

// WithKimiChatCompletionsCompat adapts Moonshot/Kimi's OpenAI-compatible Chat
// Completions transport. Kimi enforces a stricter "Moonshot flavored" JSON
// Schema than the standard: when a schema uses anyOf, every anyOf branch must
// declare its own "type" rather than inheriting one from the parent schema.
// This rewrites outgoing tool parameter schemas to satisfy that constraint.
func WithKimiChatCompletionsCompat() Option {
	return func(p *Provider) {
		p.compat = chatCompletionsCompatKimi
	}
}

func New(options ...Option) *Provider {
	provider := &Provider{
		baseURL:    defaultBaseURL,
		httpClient: &http.Client{},
		messageRoles: sdk.MessageRoleCapabilities{
			Developer:             true,
			MidConversationSystem: true,
		},
	}
	for _, option := range options {
		option(provider)
	}
	return provider
}

func (p *Provider) Name() string {
	return "openai-completions"
}

func (p *Provider) ListModels(ctx context.Context) ([]sdk.Model, error) {
	resp, err := utils.FetchJSON[modelsListResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodGet,
		BaseURL: p.baseURL,
		Path:    "/models",
		Headers: p.authHeaders(),
		Prepare: p.prepareRequest,
	})
	if err != nil {
		return nil, fmt.Errorf("openai: list models request failed: %w", err)
	}

	models := make([]sdk.Model, 0, len(resp.Data))
	for _, m := range resp.Data {
		models = append(models, sdk.Model{
			ID:       m.ID,
			Provider: p,
			Type:     sdk.ModelTypeChat,
		})
	}
	return models, nil
}

func (p *Provider) Test(ctx context.Context) *sdk.ProviderTestResult {
	_, err := utils.FetchJSON[modelsListResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodGet,
		BaseURL: p.baseURL,
		Path:    "/models",
		Query:   map[string]string{"limit": "1"},
		Headers: p.authHeaders(),
		Prepare: p.prepareRequest,
	})
	if err != nil {
		return classifyError(err)
	}
	return &sdk.ProviderTestResult{Status: sdk.ProviderStatusOK, Message: "ok"}
}

func (p *Provider) TestModel(ctx context.Context, modelID string) (*sdk.ModelTestResult, error) {
	_, err := utils.FetchJSON[modelObject](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodGet,
		BaseURL: p.baseURL,
		Path:    "/models/" + modelID,
		Headers: p.authHeaders(),
		Prepare: p.prepareRequest,
	})
	if err == nil {
		return &sdk.ModelTestResult{Supported: true, Message: "supported"}, nil
	}
	var apiErr *utils.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
		return nil, fmt.Errorf("openai: test model request failed: %w", err)
	}

	// GET /models/{id} returned 404 — fall back to a minimal generation
	// request for providers that don't implement the models listing API.
	status, probeErr := utils.ProbeStatus(ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    pathChatCompletions,
		Headers: p.authHeaders(),
		Prepare: p.prepareRequest,
		Body: map[string]any{
			"model":      modelID,
			"messages":   []map[string]string{{"role": "user", "content": "hi"}},
			"max_tokens": 1,
		},
	})
	if probeErr != nil {
		return nil, fmt.Errorf("openai: probe model request failed: %w", probeErr)
	}
	return sdk.ClassifyProbeStatus(status)
}

// ChatModel creates a Model bound to this provider.
func (p *Provider) ChatModel(id string) *sdk.Model {
	return &sdk.Model{
		ID:       id,
		Provider: p,
		Type:     sdk.ModelTypeChat,
	}
}

// ---------- DoGenerate ----------

func (p *Provider) DoGenerate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) { //nolint:gocritic // interface method
	if req.Model == "" {
		return sdk.ModelResult{}, fmt.Errorf("openai: model is required")
	}

	chatReq, err := p.buildRequest(&req)
	if err != nil {
		return sdk.ModelResult{}, fmt.Errorf("openai: build request: %w", err)
	}

	resp, err := utils.FetchJSON[chatResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    pathChatCompletions,
		Headers: p.authHeaders(),
		Prepare: p.prepareRequest,
		Body:    chatReq,
	})
	if err != nil {
		var apiErr *utils.APIError
		if errors.As(err, &apiErr) {
			// Keep the structured error in the chain: the executor classifies
			// the failure by its HTTP status (sdk.HTTPStatusError).
			return sdk.ModelResult{}, fmt.Errorf("openai: chat completions request failed: %s: %w", apiErr.Detail(), apiErr)
		}
		return sdk.ModelResult{}, fmt.Errorf("openai: chat completions request failed: %w", err)
	}

	return p.parseResponse(resp)
}

// ---------- buildRequest ----------

func (p *Provider) buildRequest(req *sdk.Request) (*chatRequest, error) {
	messages, err := messagecompat.Normalize(req.Messages, p.messageRoles)
	if err != nil {
		return nil, err
	}
	chatReq := &chatRequest{
		Model:               req.Model,
		Messages:            convertMessages(req.System, messages),
		Temperature:         req.Temperature,
		TopP:                req.TopP,
		MaxCompletionTokens: req.MaxTokens,
		FrequencyPenalty:    req.FrequencyPenalty,
		PresencePenalty:     req.PresencePenalty,
		Seed:                req.Seed,
		ReasoningEffort:     req.ReasoningEffort,
		PromptCacheKey:      req.PromptCacheKey,
	}
	if len(req.StopSequences) > 0 {
		chatReq.Stop = req.StopSequences
	}
	if len(req.Tools) > 0 {
		chatReq.Tools = convertTools(req.Tools)
		chatReq.ToolChoice = toolChoiceForWire(req.ToolChoice)
	}
	if req.ResponseFormat != nil {
		chatReq.ResponseFormat = &chatRespFormat{
			Type:       string(req.ResponseFormat.Type),
			JSONSchema: req.ResponseFormat.JSONSchema,
		}
	}
	if err := p.applyChatCompletionsCompat(chatReq); err != nil {
		return nil, err
	}
	padThinkingReplay(chatReq.Messages, p.compat)
	if err := sdk.ApplyProviderOptions(p.Name(), req.ProviderOptions, chatReq); err != nil {
		return nil, fmt.Errorf("openai: %w", err)
	}
	return chatReq, nil
}

func (p *Provider) applyChatCompletionsCompat(req *chatRequest) error {
	switch p.compat {
	case chatCompletionsCompatDeepSeek:
		if req.ReasoningEffort == nil {
			return nil
		}
		effort := strings.TrimSpace(*req.ReasoningEffort)
		if effort == "" {
			return nil
		}
		switch strings.ToLower(effort) {
		case "none", "disable", thinkingDisabled:
			req.ReasoningEffort = nil
			req.Thinking = &chatThinking{Type: thinkingDisabled}
		}
	case chatCompletionsCompatMiniMax:
		// MiniMax does not honor reasoning_effort; it gates thinking via the
		// thinking toggle and separates reasoning into reasoning_details when
		// reasoning_split is set.
		req.ReasoningSplit = true
		if req.ReasoningEffort == nil {
			return nil
		}
		effort := strings.ToLower(strings.TrimSpace(*req.ReasoningEffort))
		req.ReasoningEffort = nil
		switch effort {
		case "":
			// no explicit effort: leave thinking at MiniMax's default.
		case "none", "disable", thinkingDisabled:
			req.Thinking = &chatThinking{Type: thinkingDisabled}
		default:
			req.Thinking = &chatThinking{Type: "adaptive"}
		}
	case chatCompletionsCompatKimi:
		for i := range req.Tools {
			parameters, err := normalizeSchemaForKimi(req.Tools[i].Function.Parameters)
			if err != nil {
				return fmt.Errorf("kimi tool %q schema: %w", req.Tools[i].Function.Name, err)
			}
			req.Tools[i].Function.Parameters = parameters
		}
	}
	return nil
}

func convertTools(tools []sdk.ToolDefinition) []chatTool {
	out := make([]chatTool, 0, len(tools))
	for _, t := range tools {
		out = append(out, chatTool{
			Type: toolTypeFunction,
			Function: chatFunction{
				Name:        t.Name,
				Description: t.Description,
				Parameters:  t.Parameters,
			},
		})
	}
	return out
}

// toolChoiceForWire maps the closed provider-neutral ToolChoice onto the
// OpenAI Chat Completions wire shape. An empty mode leaves the field unset, a
// mode maps to its own string, and a named tool becomes the function object.
func toolChoiceForWire(choice sdk.ToolChoice) any {
	switch choice.Mode {
	case sdk.ToolChoiceAuto, sdk.ToolChoiceNone, sdk.ToolChoiceRequired:
		return string(choice.Mode)
	case sdk.ToolChoiceTool:
		return map[string]any{
			keyType:          toolTypeFunction,
			toolTypeFunction: map[string]any{"name": choice.Tool},
		}
	default:
		return nil
	}
}

// ---------- message conversion ----------

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
	cm := chatMessage{Role: roleAssistant}

	var contentParts []sdk.MessagePart
	var toolCalls []chatToolCall
	var reasoning string
	var hasReasoning bool
	var reasoningDetails []chatReasoningDetail

	for _, part := range msg.Content {
		switch p := part.(type) {
		case sdk.ToolCallPart:
			id := p.ToolCallID
			if id == "" {
				id = generateID()
			}
			toolCalls = append(toolCalls, chatToolCall{
				ID:   id,
				Type: toolTypeFunction,
				Function: chatFunctionCall{
					Name:      p.ToolName,
					Arguments: p.Input.String(),
				},
			})
		case sdk.ReasoningPart:
			if p.Format != sdk.ReasoningFormatOpenAIChat {
				continue
			}
			reasoning += p.Text
			hasReasoning = true
			if details := extractMiniMaxReasoningDetails(p.ProviderMetadata); len(details) > 0 {
				reasoningDetails = details
			}
		default:
			contentParts = append(contentParts, part)
		}
	}

	if len(contentParts) > 0 {
		cm.Content = convertContent(contentParts)
	}
	if len(reasoningDetails) > 0 {
		cm.ReasoningDetails = reasoningDetails
	} else if hasReasoning {
		// An empty block is still a block the model emitted: keep the key so
		// thinking-mode endpoints see the step's reasoning was passed back.
		cm.ReasoningContent = &reasoning
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
			data, mediaType := utils.NormalizeFileData(p.Data, p.MediaType)
			out = append(out, chatContentPartFile{
				Type: "file",
				File: chatFile{
					Filename: strings.TrimSpace(p.Filename),
					FileData: utils.FileDataURL(data, mediaType),
				},
			})
		}
	}
	return out
}

// ---------- parseResponse ----------

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
		// A present but empty reasoning_content is still a reasoning block: the
		// model was in thinking mode and produced nothing for this step. Record
		// it so the step replays with the key DeepSeek and Kimi validate.
		if result.Reasoning != "" || len(choice.Message.ReasoningDetails) > 0 || choice.Message.ReasoningContent != nil {
			result.ReasoningParts = []sdk.ReasoningPart{{
				Text:             result.Reasoning,
				Format:           sdk.ReasoningFormatOpenAIChat,
				Model:            resp.Model,
				ProviderMetadata: minimaxReasoningMetadata(choice.Message.ReasoningDetails),
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

// ---------- DoStream ----------

func (p *Provider) DoStream(ctx context.Context, req sdk.Request) (<-chan sdk.StreamPart, error) { //nolint:gocritic // interface method
	if req.Model == "" {
		return nil, fmt.Errorf("openai: model is required")
	}

	out, err := p.buildRequest(&req)
	if err != nil {
		return nil, fmt.Errorf("openai: build request: %w", err)
	}
	out.Stream = true
	out.StreamOptions = &chatStreamOptions{IncludeUsage: true}

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
			Path:    pathChatCompletions,
			Headers: p.authHeaders(),
			Prepare: p.prepareRequest,
			Body:    out,
		}, func(ev *utils.SSEEvent) error {
			if ev.Data == "[DONE]" {
				return utils.ErrStreamDone
			}

			var chunk chatChunkResponse
			if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
				sp.send(&sdk.ErrorPart{Error: fmt.Errorf("openai: unmarshal chunk: %w", err)})
				return err
			}

			return sp.processChunk(&chunk)
		})

		if err != nil {
			var apiErr *utils.APIError
			if errors.As(err, &apiErr) {
				sp.send(&sdk.ErrorPart{Error: fmt.Errorf("openai: stream failed: %s: %w", apiErr.Detail(), apiErr)})
			} else {
				sp.send(&sdk.ErrorPart{Error: fmt.Errorf("openai: stream failed: %w", err)})
			}
		}

		sp.flush()
		sp.emitFinishStep()

		sp.send(&sdk.FinishPart{
			FinishReason:    sp.finishReason,
			RawFinishReason: sp.rawFinishReason,
			TotalUsage:      sp.usage,
		})
	}()

	return ch, nil
}

func (p *Provider) authHeaders() map[string]string {
	if p.prepareRequest != nil {
		return nil
	}
	if p.apiKey == "" {
		return nil
	}
	return utils.AuthHeader(p.apiKey)
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

// ---------- helpers ----------

func reasoningFromMessage(m *chatRespMessage) string {
	if len(m.ReasoningDetails) > 0 {
		if text := reasoningTextFromDetails(m.ReasoningDetails); text != "" {
			return text
		}
	}
	if m.ReasoningContent != nil && *m.ReasoningContent != "" {
		return *m.ReasoningContent
	}
	return m.Reasoning
}

func reasoningFromDelta(d *chatChunkDelta) string {
	if len(d.ReasoningDetails) > 0 {
		if text := reasoningTextFromDetails(d.ReasoningDetails); text != "" {
			return text
		}
	}
	if d.ReasoningContent != nil && *d.ReasoningContent != "" {
		return *d.ReasoningContent
	}
	return d.Reasoning
}

func reasoningTextFromDetails(details []chatReasoningDetail) string {
	for i := len(details) - 1; i >= 0; i-- {
		if text, _ := details[i]["text"].(string); text != "" {
			return text
		}
	}
	return ""
}

// minimaxReasoningMetadata carries MiniMax's reasoning_details on the part as
// their JSON encoding, the opaque token the replay puts back on the wire.
func minimaxReasoningMetadata(details []chatReasoningDetail) sdk.ProviderMetadata {
	if len(details) == 0 {
		return nil
	}
	raw, err := json.Marshal(details)
	if err != nil {
		return nil
	}
	return sdk.NewProviderMetadata(minimaxNamespace, map[string]string{minimaxKeyReasoningDetails: string(raw)})
}

func extractMiniMaxReasoningDetails(meta sdk.ProviderMetadata) []chatReasoningDetail {
	raw := meta.Get(minimaxNamespace, minimaxKeyReasoningDetails)
	if raw == "" {
		return nil
	}
	var details []chatReasoningDetail
	if err := json.Unmarshal([]byte(raw), &details); err != nil {
		return nil
	}
	return details
}

const (
	minimaxNamespace           = "minimax"
	minimaxKeyReasoningDetails = "reasoning_details"
)

func copyReasoningDetails(details []chatReasoningDetail) []chatReasoningDetail {
	out := make([]chatReasoningDetail, 0, len(details))
	for _, detail := range details {
		out = append(out, copyReasoningDetail(detail))
	}
	return out
}

func copyReasoningDetail(detail map[string]any) chatReasoningDetail {
	out := make(chatReasoningDetail, len(detail))
	for k, v := range detail {
		out[k] = v
	}
	return out
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
