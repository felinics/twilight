package messages

import (
	"context"
	"crypto/rand"
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
	defaultBaseURL      = "https://api.anthropic.com/v1"
	defaultAnthropicVer = "2023-06-01"
	defaultMaxTokens    = 4096
	// defaultReasoningMaxTokens is the fallback output cap when reasoning is
	// active without an explicit budget (adaptive / output_config.effort). The
	// plain 4096 default would truncate reasoning + answer; modern Claude models
	// support far larger outputs.
	defaultReasoningMaxTokens = 32000

	// Content block types for Anthropic API
	blockTypeText     = "text"
	blockTypeThinking = "thinking"
	blockTypeToolUse  = "tool_use"
	// blockTypeRedactedThinking carries encrypted reasoning with no readable
	// text. It must be replayed verbatim alongside ordinary thinking blocks.
	blockTypeRedactedThinking = "redacted_thinking"

	// Keys under the provider's metadata namespace. A thinking block is
	// identified by its signature and a redacted one by its encrypted data; the
	// two are different kinds of token and never interchangeable.
	metadataNamespace       = "anthropic"
	metadataKeySignature    = "signature"
	metadataKeyRedactedData = "redactedData"

	thinkingTypeDisabled = "disabled"
)

// ThinkingConfig controls extended thinking for Anthropic models.
//
// This flat shape can express combinations the API rejects, such as
// {Type: "adaptive", BudgetTokens: 8000}. Prefer the constructor options
// WithThinkingEnabled, WithThinkingAdaptive and WithThinkingDisabled, which
// model the same wire object as a union so the illegal combination cannot be
// built. This type remains fully supported for existing callers.
type ThinkingConfig struct {
	Type         string // "enabled", "adaptive", or "disabled"
	BudgetTokens int    // required when Type is "enabled"
}

// thinkingConfigUnion mirrors Anthropic's ThinkingConfigParamUnion: exactly one
// variant is set, and the adaptive variant deliberately has no budget field so
// adaptive-plus-budget is unrepresentable rather than merely discouraged.
//
// A fourth "passthrough" variant carries type strings the SDK does not know
// about. The SDK executes what a caller declared; it does not validate it, so an
// unrecognised type reaches the wire untouched and the server decides.
type thinkingConfigUnion struct {
	enabled     *thinkingEnabled
	adaptive    *thinkingAdaptive
	disabled    *thinkingDisabled
	passthrough *thinkingPassthrough
}

// thinkingEnabled is the budget-based mode used by Claude 4.5 and earlier.
type thinkingEnabled struct {
	budgetTokens int
}

// thinkingAdaptive is the mode used by Claude 4.6 and later. It has no budget
// field: budget_tokens is deprecated on 4.6 and rejected outright by 4.7+, and
// effort is carried per request through output_config.effort instead.
type thinkingAdaptive struct{}

// thinkingDisabled turns extended thinking off.
type thinkingDisabled struct{}

// thinkingPassthrough carries an unrecognised ThinkingConfig from the legacy
// flat API verbatim. It is unreachable from the union constructors.
type thinkingPassthrough struct {
	typ          string
	budgetTokens int
}

// active reports whether the union asks for thinking at all. Disabled and unset
// are both inactive.
func (t *thinkingConfigUnion) active() bool {
	if t == nil {
		return false
	}
	return t.enabled != nil || t.adaptive != nil || t.passthrough != nil
}

// explicitBudget returns the thinking token budget the caller asked for, and
// whether one was set. Adaptive and disabled never carry a budget.
func (t *thinkingConfigUnion) explicitBudget() (int, bool) {
	if t == nil {
		return 0, false
	}
	switch {
	case t.enabled != nil && t.enabled.budgetTokens > 0:
		return t.enabled.budgetTokens, true
	case t.passthrough != nil && t.passthrough.budgetTokens > 0:
		return t.passthrough.budgetTokens, true
	default:
		return 0, false
	}
}

// wire renders the union into the request object, or nil when no thinking block
// should be sent.
func (t *thinkingConfigUnion) wire() *anthropicThinking {
	if t == nil {
		return nil
	}
	switch {
	case t.enabled != nil:
		return &anthropicThinking{Type: "enabled", BudgetTokens: t.enabled.budgetTokens}
	case t.adaptive != nil:
		// No budget field exists on this variant, so none can be sent.
		return &anthropicThinking{Type: "adaptive"}
	case t.passthrough != nil:
		return &anthropicThinking{Type: t.passthrough.typ, BudgetTokens: t.passthrough.budgetTokens}
	default:
		// Disabled is expressed by omitting the block entirely.
		return nil
	}
}

type Provider struct {
	apiKey                        string
	authToken                     string
	baseURL                       string
	httpClient                    *http.Client
	headers                       map[string]string
	thinking                      *thinkingConfigUnion
	supportsMidConversationSystem bool
}

type Option func(*Provider)

func WithAPIKey(apiKey string) Option {
	return func(p *Provider) {
		p.apiKey = apiKey
	}
}

// WithAuthToken sets a Bearer token for authentication instead of x-api-key.
// Useful for proxies like OpenRouter that require Authorization: Bearer.
func WithAuthToken(token string) Option {
	return func(p *Provider) {
		p.authToken = token
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

// WithHeaders sets provider-wide HTTP headers, overriding defaults. The map is
// copied when the option is created. Use sdk.WithRequestHeaders for call-scoped
// values such as session IDs; those take precedence over these headers.
func WithHeaders(headers map[string]string) Option {
	headers = utils.MergeHeaders(headers)
	return func(p *Provider) { p.headers = headers }
}

// WithThinking enables extended thinking for all requests made by this provider
// using the flat ThinkingConfig shape.
//
// It is retained for existing callers and produces exactly the same wire output
// as before. New code should prefer WithThinkingEnabled, WithThinkingAdaptive or
// WithThinkingDisabled, which cannot express a mode/budget combination the API
// rejects. A ThinkingConfig with an empty Type configures no thinking at all.
func WithThinking(cfg ThinkingConfig) Option {
	return func(p *Provider) {
		p.thinking = thinkingFromLegacyConfig(cfg)
	}
}

// thinkingFromLegacyConfig lowers the flat ThinkingConfig onto the union.
// Unrecognised types become a passthrough variant so this conversion never
// changes what an existing caller puts on the wire.
func thinkingFromLegacyConfig(cfg ThinkingConfig) *thinkingConfigUnion {
	switch cfg.Type {
	case "":
		return nil
	case "enabled":
		return &thinkingConfigUnion{enabled: &thinkingEnabled{budgetTokens: cfg.BudgetTokens}}
	case "adaptive":
		// The legacy struct allows a budget here, but adaptive takes none and
		// the pre-union code never sent one, so it is dropped exactly as before.
		return &thinkingConfigUnion{adaptive: &thinkingAdaptive{}}
	case thinkingTypeDisabled:
		return &thinkingConfigUnion{disabled: &thinkingDisabled{}}
	default:
		return &thinkingConfigUnion{passthrough: &thinkingPassthrough{
			typ:          cfg.Type,
			budgetTokens: cfg.BudgetTokens,
		}}
	}
}

// WithThinkingEnabled turns on budget-based extended thinking, sending
// thinking{type:"enabled", budget_tokens:N}.
//
// This is the shape accepted by Claude 4.5 and earlier. On those models
// output_config.effort alone does not turn thinking on, so this option is how a
// caller enables it. budget_tokens is deprecated on Claude 4.6 and rejected with
// a 400 by 4.7+; use WithThinkingAdaptive for those. The SDK never infers the
// model generation, so choosing the right option is the caller's decision.
//
// When no explicit MaxTokens is supplied, the budget is added on top of the
// default answer allowance so thinking does not consume the answer's room.
func WithThinkingEnabled(budgetTokens int) Option {
	return func(p *Provider) {
		p.thinking = &thinkingConfigUnion{enabled: &thinkingEnabled{budgetTokens: budgetTokens}}
	}
}

// WithThinkingAdaptive turns on adaptive extended thinking, sending
// thinking{type:"adaptive"} with no budget.
//
// This is the shape used by Claude 4.6 and later, where the depth of thinking is
// steered per request through output_config.effort (see
// sdk.Request.ReasoningEffort) rather than a token budget. There is
// deliberately no budget parameter: adaptive thinking takes none.
func WithThinkingAdaptive() Option {
	return func(p *Provider) {
		p.thinking = &thinkingConfigUnion{adaptive: &thinkingAdaptive{}}
	}
}

// WithThinkingDisabled turns extended thinking off by omitting the thinking
// block from requests.
//
// Note that a ReasoningEffort on the request still sends output_config.effort;
// this option only controls the thinking block.
func WithThinkingDisabled() Option {
	return func(p *Provider) {
		p.thinking = &thinkingConfigUnion{disabled: &thinkingDisabled{}}
	}
}

// WithMidConversationSystemMessages enables native system messages inside the
// Anthropic message timeline. Keep this disabled unless the selected model is
// documented to support interleaved system messages.
func WithMidConversationSystemMessages(enabled bool) Option {
	return func(p *Provider) {
		p.supportsMidConversationSystem = enabled
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
	return "anthropic-messages"
}

func (p *Provider) ListModels(ctx context.Context) ([]sdk.Model, error) {
	resp, err := utils.FetchJSON[modelsListResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodGet,
		BaseURL: p.baseURL,
		Path:    "/models",
		Headers: p.requestHeaders(ctx),
	})
	if err != nil {
		return nil, fmt.Errorf("anthropic: list models request failed: %w", err)
	}

	models := make([]sdk.Model, 0, len(resp.Data))
	for _, m := range resp.Data {
		models = append(models, sdk.Model{
			ID:          m.ID,
			DisplayName: m.DisplayName,
			Provider:    p,
			Type:        sdk.ModelTypeChat,
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
		Headers: p.requestHeaders(ctx),
	})
	if err != nil {
		return classifyError(err)
	}
	return &sdk.ProviderTestResult{Status: sdk.ProviderStatusOK, Message: "ok"}
}

func (p *Provider) TestModel(ctx context.Context, modelID string) (*sdk.ModelTestResult, error) {
	_, err := utils.FetchJSON[anthropicModelObject](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodGet,
		BaseURL: p.baseURL,
		Path:    "/models/" + modelID,
		Headers: p.requestHeaders(ctx),
	})
	if err == nil {
		return &sdk.ModelTestResult{Supported: true, Message: "supported"}, nil
	}
	var apiErr *utils.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
		return nil, fmt.Errorf("anthropic: test model request failed: %w", err)
	}

	status, probeErr := utils.ProbeStatus(ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    "/messages",
		Headers: p.requestHeaders(ctx),
		Body: map[string]any{
			"model":      modelID,
			"messages":   []map[string]string{{"role": "user", "content": "hi"}},
			"max_tokens": 1,
		},
	})
	if probeErr != nil {
		return nil, fmt.Errorf("anthropic: probe model request failed: %w", probeErr)
	}
	return sdk.ClassifyProbeStatus(status)
}

func (p *Provider) ChatModel(id string) *sdk.Model {
	return &sdk.Model{
		ID:       id,
		Provider: p,
		Type:     sdk.ModelTypeChat,
	}
}

func (p *Provider) requestHeaders(ctx context.Context) map[string]string {
	h := map[string]string{
		"anthropic-version": defaultAnthropicVer,
		"Content-Type":      "application/json",
	}
	if p.authToken != "" {
		h["Authorization"] = "Bearer " + p.authToken
	} else if p.apiKey != "" {
		h["x-api-key"] = p.apiKey
	}
	return utils.RequestHeaders(ctx, h, p.headers)
}

// ---------- DoGenerate ----------

func (p *Provider) DoGenerate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) { //nolint:gocritic // interface method
	if req.Model == "" {
		return sdk.ModelResult{}, fmt.Errorf("anthropic: model is required")
	}

	body, err := p.buildRequest(&req)
	if err != nil {
		return sdk.ModelResult{}, fmt.Errorf("anthropic: build request: %w", err)
	}

	resp, err := utils.FetchJSON[messagesResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    "/messages",
		Headers: p.requestHeaders(ctx),
		Body:    body,
	})
	if err != nil {
		var apiErr *utils.APIError
		if errors.As(err, &apiErr) {
			return sdk.ModelResult{}, fmt.Errorf("anthropic: messages request failed: %s", apiErr.Detail())
		}
		return sdk.ModelResult{}, fmt.Errorf("anthropic: messages request failed: %w", err)
	}

	return p.parseResponse(resp)
}

// ---------- buildRequest ----------

func (p *Provider) buildRequest(params *sdk.Request) (*messagesRequest, error) {
	normalized, err := messagecompat.Normalize(params.Messages, sdk.MessageRoleCapabilities{
		MidConversationSystem: p.supportsMidConversationSystem,
	})
	if err != nil {
		return nil, err
	}
	system, messages := convertMessages(params.System, params.Model, normalized)

	req := &messagesRequest{
		Model:       params.Model,
		System:      system,
		Messages:    messages,
		MaxTokens:   resolveMaxTokens(params, p.thinking),
		Temperature: params.Temperature,
		TopP:        params.TopP,
	}

	if len(params.StopSequences) > 0 {
		req.StopSequences = params.StopSequences
	}
	if len(params.Tools) > 0 {
		req.Tools = convertTools(params.Tools)
		req.ToolChoice = convertToolChoice(params.ToolChoice)
	}

	req.Thinking = p.thinking.wire()

	// Reasoning effort is carried via output_config.effort. On modern Claude
	// models (>= 4.6) this is the supported control; budget_tokens is deprecated
	// (4.6) or rejected (4.7+). The caller is responsible for only sending an
	// effort the target model accepts; errors surface as-is.
	if params.ReasoningEffort != nil {
		if effort := strings.TrimSpace(*params.ReasoningEffort); effort != "" {
			req.OutputConfig = &anthropicOutputConfig{Effort: effort}
		}
	}

	if err := sdk.ApplyProviderOptions(p.Name(), params.ProviderOptions, req); err != nil {
		return nil, fmt.Errorf("anthropic: %w", err)
	}
	return req, nil
}

// resolveMaxTokens derives the output cap for a request.
//
// The three outcomes below are behaviour-critical: an explicit thinking budget
// must be added to the answer allowance, and budget-less reasoning must lift the
// cap off the low default. Collapsing either case back to defaultMaxTokens
// silently truncates thinking output rather than failing, so the full matrix is
// pinned by TestResolveMaxTokens_Matrix.
func resolveMaxTokens(params *sdk.Request, thinking *thinkingConfigUnion) *int {
	if params.MaxTokens != nil {
		return params.MaxTokens
	}

	maxTokens := defaultMaxTokens
	switch budget, hasBudget := thinking.explicitBudget(); {
	case hasBudget:
		// Explicit budget thinking: reserve room for the thinking budget on top
		// of the answer budget.
		maxTokens += budget
	case reasoningActive(params, thinking):
		// Effort-based or adaptive thinking carries no explicit budget, but the
		// model still needs generous headroom (reasoning + answer). The low 4096
		// default would truncate; use a reasoning-aware default instead.
		maxTokens = defaultReasoningMaxTokens
	}

	return &maxTokens
}

// reasoningActive reports whether the request enables reasoning without an
// explicit token budget (adaptive thinking and/or output_config.effort).
func reasoningActive(params *sdk.Request, thinking *thinkingConfigUnion) bool {
	if thinking.active() {
		return true
	}
	if params.ReasoningEffort != nil && strings.TrimSpace(*params.ReasoningEffort) != "" {
		return true
	}
	return false
}

func convertTools(tools []sdk.ToolDefinition) []anthropicTool {
	out := make([]anthropicTool, 0, len(tools))
	for _, t := range tools {
		at := anthropicTool{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: t.Parameters,
		}
		if t.CacheControl != nil {
			at.CacheControl = &cacheControl{Type: t.CacheControl.Type, TTL: t.CacheControl.TTL}
		}
		out = append(out, at)
	}
	return out
}

// convertToolChoice lowers the provider-neutral ToolChoice onto Anthropic's
// tool_choice object. An unset Mode leaves the field off the wire entirely.
func convertToolChoice(choice sdk.ToolChoice) *anthropicToolChoice {
	switch choice.Mode {
	case sdk.ToolChoiceAuto:
		return &anthropicToolChoice{Type: "auto"}
	case sdk.ToolChoiceNone:
		return &anthropicToolChoice{Type: "none"}
	case sdk.ToolChoiceRequired:
		return &anthropicToolChoice{Type: "any"}
	case sdk.ToolChoiceTool:
		return &anthropicToolChoice{Type: "tool", Name: choice.Tool}
	default:
		return nil
	}
}

// ---------- message conversion ----------

// convertMessages splits SDK messages into Anthropic's system blocks and
// alternating user/assistant messages. Tool result messages are merged into
// user messages, as required by the Anthropic API.
//
// Note: Request.System (plain string) is converted to a single system
// block without cache_control. To attach cache_control to a system prompt,
// pass it as a MessageRoleSystem message with a TextPart that has CacheControl
// set instead of using the System field.
func convertMessages(systemPrompt, targetModel string, messages []sdk.Message) ([]contentBlock, []anthropicMessage) {
	var system []contentBlock
	var out []anthropicMessage
	conversationStarted := false

	if systemPrompt != "" {
		system = append(system, contentBlock{Type: blockTypeText, Text: systemPrompt})
	}

	for _, msg := range messages {
		switch msg.Role {
		case sdk.MessageRoleSystem:
			blocks := convertSystemContent(msg.Content)
			if conversationStarted {
				out = append(out, anthropicMessage{Role: "system", Content: blocks})
			} else {
				system = append(system, blocks...)
			}

		case sdk.MessageRoleUser:
			conversationStarted = true
			blocks := convertUserContent(msg.Content)
			out = appendUserBlocks(out, blocks)

		case sdk.MessageRoleAssistant:
			conversationStarted = true
			out = append(out, convertAssistantMessage(msg, targetModel))

		case sdk.MessageRoleTool:
			conversationStarted = true
			blocks := convertToolResults(msg.Content)
			out = appendUserBlocks(out, blocks)
		}
	}

	return system, out
}

func convertSystemContent(parts []sdk.MessagePart) []contentBlock {
	blocks := make([]contentBlock, 0, len(parts))
	for _, part := range parts {
		tp, ok := part.(sdk.TextPart)
		if !ok {
			continue
		}
		block := contentBlock{Type: blockTypeText, Text: tp.Text}
		if tp.CacheControl != nil {
			block.CacheControl = &cacheControl{Type: tp.CacheControl.Type, TTL: tp.CacheControl.TTL}
		}
		blocks = append(blocks, block)
	}
	return blocks
}

// appendUserBlocks appends content blocks to the last user message if it exists,
// or creates a new user message.
func appendUserBlocks(messages []anthropicMessage, blocks []contentBlock) []anthropicMessage {
	if len(messages) > 0 && messages[len(messages)-1].Role == "user" {
		messages[len(messages)-1].Content = append(messages[len(messages)-1].Content, blocks...)
		return messages
	}
	return append(messages, anthropicMessage{
		Role:    "user",
		Content: blocks,
	})
}

func convertUserContent(parts []sdk.MessagePart) []contentBlock {
	var blocks []contentBlock
	for _, part := range parts {
		switch p := part.(type) {
		case sdk.TextPart:
			block := contentBlock{Type: blockTypeText, Text: p.Text}
			if p.CacheControl != nil {
				block.CacheControl = &cacheControl{Type: p.CacheControl.Type, TTL: p.CacheControl.TTL}
			}
			blocks = append(blocks, block)
		case sdk.ImagePart:
			blocks = append(blocks, convertImagePart(p))
		case sdk.FilePart:
			blocks = append(blocks, convertFilePart(p))
		}
	}
	return blocks
}

// convertFilePart converts an sdk.FilePart into an Anthropic document content
// block for native file input (text layer + per-page rendering happen on the
// provider side). FilePart data is bare base64 by convention; data URLs are
// tolerated and stripped.
func convertFilePart(p sdk.FilePart) contentBlock {
	data, mediaType := utils.NormalizeFileData(p.Data, p.MediaType)

	var cc *cacheControl
	if p.CacheControl != nil {
		cc = &cacheControl{Type: p.CacheControl.Type, TTL: p.CacheControl.TTL}
	}

	return contentBlock{
		Type: "document",
		Source: &imageSource{
			Type:      "base64",
			MediaType: mediaType,
			Data:      data,
		},
		Title:        strings.TrimSpace(p.Filename),
		CacheControl: cc,
	}
}

// convertImagePart converts an sdk.ImagePart into an Anthropic content block,
// handling both data URLs and public URLs as image sources. For data URLs the
// prefix is stripped and the embedded MIME type is used when the caller did not
// provide one. For public URLs the "url" source type is used instead.
func convertImagePart(p sdk.ImagePart) contentBlock {
	image := strings.TrimSpace(p.Image)
	mediaType := strings.TrimSpace(p.MediaType)

	var cc *cacheControl
	if p.CacheControl != nil {
		cc = &cacheControl{Type: p.CacheControl.Type, TTL: p.CacheControl.TTL}
	}

	if strings.HasPrefix(strings.ToLower(image), "http://") || strings.HasPrefix(strings.ToLower(image), "https://") {
		return contentBlock{
			Type: "image",
			Source: &imageSource{
				Type:      "url",
				MediaType: mediaType,
				URL:       image,
			},
			CacheControl: cc,
		}
	}

	if lower := strings.ToLower(image); strings.HasPrefix(lower, "data:") {
		if idx := strings.Index(image, ","); idx >= 0 {
			header := image[len("data:"):idx]
			image = image[idx+1:]

			if mediaType == "" || !isAnthropicSupportedMediaType(mediaType) {
				if semi := strings.Index(header, ";"); semi >= 0 {
					mediaType = strings.TrimSpace(header[:semi])
				} else {
					mediaType = strings.TrimSpace(header)
				}
			}
		}
	}

	if !isAnthropicSupportedMediaType(mediaType) {
		mediaType = "image/png"
	}

	return contentBlock{
		Type: "image",
		Source: &imageSource{
			Type:      "base64",
			MediaType: mediaType,
			Data:      image,
		},
		CacheControl: cc,
	}
}

func isAnthropicSupportedMediaType(mt string) bool {
	switch strings.ToLower(strings.TrimSpace(mt)) {
	case "image/jpeg", "image/png", "image/gif", "image/webp":
		return true
	default:
		return false
	}
}

func convertAssistantMessage(msg sdk.Message, targetModel string) anthropicMessage {
	var blocks []contentBlock

	for _, part := range msg.Content {
		switch p := part.(type) {
		case sdk.TextPart:
			block := contentBlock{Type: blockTypeText, Text: p.Text}
			if p.CacheControl != nil {
				block.CacheControl = &cacheControl{Type: p.CacheControl.Type, TTL: p.CacheControl.TTL}
			}
			blocks = append(blocks, block)
		case sdk.ReasoningPart:
			// Only this dialect's blocks can be replayed: the API verifies the
			// sequence and rejects anything it did not produce. Foreign or
			// unmarked reasoning is dropped rather than re-sent as text,
			// because feeding a model its own inner monologue back as ordinary
			// content teaches it to imitate the form in user-visible answers.
			if p.Format != sdk.ReasoningFormatAnthropic {
				continue
			}
			// A signature is verified by the model that issued it. After a
			// mid-conversation model switch the old blocks cannot pass the new
			// model's check — replaying them is a hard 400 — so they are
			// dropped, exactly as foreign-dialect reasoning is.
			if !sdk.SameReasoningModel(p.Model, targetModel) {
				continue
			}
			if data := redactedDataOf(p.ProviderMetadata); data != "" {
				blocks = append(blocks, contentBlock{
					Type: blockTypeRedactedThinking,
					Data: data,
				})
				continue
			}
			if sig := signatureOf(p.ProviderMetadata); sig != "" {
				thinking := p.Text
				blocks = append(blocks, contentBlock{
					Type:      blockTypeThinking,
					Thinking:  &thinking,
					Signature: sig,
				})
			}
		case sdk.ToolCallPart:
			id := p.ToolCallID
			if id == "" {
				id = generateID()
			}
			// The API requires tool_use blocks to always carry an input
			// object; Object substitutes the empty object for arguments the
			// model got wrong.
			block := contentBlock{
				Type:  blockTypeToolUse,
				ID:    id,
				Name:  p.ToolName,
				Input: p.Input.Object(),
			}
			if p.CacheControl != nil {
				block.CacheControl = &cacheControl{Type: p.CacheControl.Type, TTL: p.CacheControl.TTL}
			}
			blocks = append(blocks, block)
		}
	}

	return anthropicMessage{Role: "assistant", Content: blocks}
}

func convertToolResults(parts []sdk.MessagePart) []contentBlock {
	var blocks []contentBlock
	for _, part := range parts {
		if trp, ok := part.(sdk.ToolResultPart); ok {
			block := contentBlock{
				Type:      "tool_result",
				ToolUseID: trp.ToolCallID,
				Content:   trp.Result.String(),
				IsError:   trp.IsError,
			}
			if trp.CacheControl != nil {
				block.CacheControl = &cacheControl{Type: trp.CacheControl.Type, TTL: trp.CacheControl.TTL}
			}
			blocks = append(blocks, block)
		}
	}
	return blocks
}

// ---------- parseResponse ----------

func (p *Provider) parseResponse(resp *messagesResponse) (sdk.ModelResult, error) {
	result := sdk.ModelResult{
		Usage:           convertUsage(&resp.Usage),
		FinishReason:    mapFinishReason(resp.StopReason),
		RawFinishReason: resp.StopReason,
	}
	result.Response = sdk.ResponseMetadata{ID: resp.ID, ModelID: resp.Model}

	for i := range resp.Content {
		block := &resp.Content[i]
		switch block.Type {
		case blockTypeText:
			result.Text += block.Text
		case blockTypeThinking:
			result.ReasoningParts = append(result.ReasoningParts, sdk.ReasoningPart{
				Text:             block.Thinking,
				Format:           sdk.ReasoningFormatAnthropic,
				Model:            resp.Model,
				ProviderMetadata: signatureMetadata(block.Signature),
			})
		case blockTypeRedactedThinking:
			// No readable text, but the encrypted payload has to be replayed
			// or the next request is rejected.
			result.ReasoningParts = append(result.ReasoningParts, sdk.ReasoningPart{
				Format:           sdk.ReasoningFormatAnthropic,
				Model:            resp.Model,
				ProviderMetadata: redactedMetadata(block.Data),
			})
		case blockTypeToolUse:
			result.ToolCalls = append(result.ToolCalls, sdk.ToolCall{
				ToolCallID: block.ID,
				ToolName:   block.Name,
				Input:      sdk.ParseToolArguments(string(block.Input)),
			})
		}
	}

	result.Reasoning = sdk.ReasoningText(result.ReasoningParts)

	return result, nil
}

// ---------- DoStream ----------

func (p *Provider) DoStream(ctx context.Context, req sdk.Request) (<-chan sdk.StreamPart, error) { //nolint:gocritic // interface method
	if req.Model == "" {
		return nil, fmt.Errorf("anthropic: model is required")
	}

	body, err := p.buildRequest(&req)
	if err != nil {
		return nil, fmt.Errorf("anthropic: build request: %w", err)
	}
	body.Stream = true

	ch := make(chan sdk.StreamPart, 64)

	go func() {
		defer close(ch)

		h := &streamHandler{
			ch:           ch,
			ctx:          ctx,
			activeBlocks: map[int]*streamingBlock{},
		}

		if !h.send(&sdk.StartPart{}) {
			return
		}
		if !h.send(&sdk.StartStepPart{}) {
			return
		}

		err := utils.FetchSSE(ctx, p.httpClient, &utils.RequestOptions{
			Method:  http.MethodPost,
			BaseURL: p.baseURL,
			Path:    "/messages",
			Headers: p.requestHeaders(ctx),
			Body:    body,
		}, h.handleEvent)

		if err != nil {
			var apiErr *utils.APIError
			if errors.As(err, &apiErr) {
				h.send(&sdk.ErrorPart{Error: fmt.Errorf("anthropic: stream failed: %s", apiErr.Detail())})
			} else {
				h.send(&sdk.ErrorPart{Error: fmt.Errorf("anthropic: stream failed: %w", err)})
			}
		}

		h.send(&sdk.FinishPart{
			FinishReason:    h.finishReason,
			RawFinishReason: h.rawFinishReason,
			TotalUsage:      h.usage,
		})
	}()

	return ch, nil
}

type streamHandler struct {
	ch           chan sdk.StreamPart
	ctx          context.Context
	activeBlocks map[int]*streamingBlock

	rawFinishReason string
	finishReason    sdk.FinishReason
	usage           sdk.Usage
	messageID       string
	messageModel    string
}

func (h *streamHandler) send(part sdk.StreamPart) bool {
	select {
	case h.ch <- part:
		return true
	case <-h.ctx.Done():
		return false
	}
}

func (h *streamHandler) handleEvent(ev *utils.SSEEvent) error {
	var event streamEvent
	if err := json.Unmarshal([]byte(ev.Data), &event); err != nil {
		h.send(&sdk.ErrorPart{Error: fmt.Errorf("anthropic: unmarshal event: %w", err)})
		return err
	}

	switch event.Type {
	case "message_start":
		h.onMessageStart(&event)
	case "content_block_start":
		h.onBlockStart(&event)
	case "content_block_delta":
		h.onBlockDelta(&event)
	case "content_block_stop":
		h.onBlockStop(&event)
	case "message_delta":
		h.onMessageDelta(&event)
	case "message_stop":
		return utils.ErrStreamDone
	case "ping":
		// ignore
	case "error":
		h.onError(&event)
	}
	return nil
}

func (h *streamHandler) onMessageStart(event *streamEvent) {
	if event.Message == nil {
		return
	}
	h.messageID = event.Message.ID
	h.messageModel = event.Message.Model
	h.usage = convertUsage(&event.Message.Usage)
}

func (h *streamHandler) onBlockStart(event *streamEvent) {
	if event.Index == nil || event.ContentBlock == nil {
		return
	}
	idx := *event.Index
	cb := event.ContentBlock
	switch cb.Type {
	case blockTypeText:
		h.activeBlocks[idx] = &streamingBlock{blockType: blockTypeText}
		h.send(&sdk.TextStartPart{ID: h.messageID})
	case blockTypeThinking:
		id := h.reasoningBlockID(idx)
		h.activeBlocks[idx] = &streamingBlock{blockType: blockTypeThinking, reasoningID: id}
		h.send(&sdk.ReasoningStartPart{ID: id, Format: sdk.ReasoningFormatAnthropic, Model: h.messageModel})
	case blockTypeRedactedThinking:
		// Delivered whole: no deltas follow, and the payload is already here.
		id := h.reasoningBlockID(idx)
		h.activeBlocks[idx] = &streamingBlock{
			blockType:    blockTypeRedactedThinking,
			reasoningID:  id,
			redactedData: cb.Data,
		}
		h.send(&sdk.ReasoningStartPart{ID: id, Format: sdk.ReasoningFormatAnthropic, Model: h.messageModel})
	case blockTypeToolUse:
		h.activeBlocks[idx] = &streamingBlock{
			blockType: blockTypeToolUse,
			toolID:    cb.ID,
			toolName:  cb.Name,
		}
		h.send(&sdk.ToolInputStartPart{
			ID:       cb.ID,
			ToolName: cb.Name,
		})
	}
}

func (h *streamHandler) onBlockDelta(event *streamEvent) {
	if event.Index == nil || event.Delta == nil {
		return
	}
	idx := *event.Index
	delta := event.Delta
	sb := h.activeBlocks[idx]

	switch delta.Type {
	case "text_delta":
		h.send(&sdk.TextDeltaPart{ID: h.messageID, Text: delta.Text})
	case "thinking_delta":
		id := h.messageID
		if sb != nil {
			id = sb.reasoningID
		}
		h.send(&sdk.ReasoningDeltaPart{ID: id, Text: delta.Thinking, Format: sdk.ReasoningFormatAnthropic, Model: h.messageModel})
	case "input_json_delta":
		if sb != nil {
			sb.args.WriteString(delta.PartialJSON)
			h.send(&sdk.ToolInputDeltaPart{
				ID:    sb.toolID,
				Delta: delta.PartialJSON,
			})
		}
	case "signature_delta":
		if sb != nil {
			sb.signature.WriteString(delta.Signature)
		}
	}
}

func (h *streamHandler) onBlockStop(event *streamEvent) {
	if event.Index == nil {
		return
	}
	idx := *event.Index
	sb, ok := h.activeBlocks[idx]
	if !ok {
		return
	}
	delete(h.activeBlocks, idx)

	switch sb.blockType {
	case blockTypeText:
		h.send(&sdk.TextEndPart{ID: h.messageID})
	case blockTypeThinking:
		h.send(&sdk.ReasoningEndPart{
			ID:               sb.reasoningID,
			Format:           sdk.ReasoningFormatAnthropic,
			Model:            h.messageModel,
			ProviderMetadata: signatureMetadata(sb.signature.String()),
		})
	case blockTypeRedactedThinking:
		h.send(&sdk.ReasoningEndPart{
			ID:               sb.reasoningID,
			Format:           sdk.ReasoningFormatAnthropic,
			Model:            h.messageModel,
			ProviderMetadata: redactedMetadata(sb.redactedData),
		})
	case blockTypeToolUse:
		h.send(&sdk.ToolInputEndPart{ID: sb.toolID})
		// An empty buffer is a no-argument call; text that is not a JSON
		// document is kept as the call's Text, so the caller can answer the
		// model instead of running the tool on it (sdk.ToolArguments).
		h.send(&sdk.StreamToolCallPart{
			ToolCallID: sb.toolID,
			ToolName:   sb.toolName,
			Input:      sdk.ParseToolArguments(sb.args.String()),
		})
	}
}

func (h *streamHandler) onMessageDelta(event *streamEvent) {
	if event.Delta != nil {
		h.rawFinishReason = event.Delta.StopReason
		h.finishReason = mapFinishReason(h.rawFinishReason)
	}
	if event.Usage != nil {
		h.usage.OutputTokens = event.Usage.OutputTokens
		h.usage.TotalTokens = h.usage.InputTokens + h.usage.OutputTokens
	}
	h.send(&sdk.FinishStepPart{
		FinishReason:    h.finishReason,
		RawFinishReason: h.rawFinishReason,
		Usage:           h.usage,
		Response: sdk.ResponseMetadata{
			ID:      h.messageID,
			ModelID: h.messageModel,
		},
	})
}

func (h *streamHandler) onError(event *streamEvent) {
	errMsg := "unknown error"
	if event.Delta != nil && event.Delta.Text != "" {
		errMsg = event.Delta.Text
	}
	h.send(&sdk.ErrorPart{Error: fmt.Errorf("anthropic: stream error: %s", errMsg)})
}

// streamingBlock accumulates one content block's deltas. args and signature
// grow by one small delta per SSE event, so they use strings.Builder — plain
// string concatenation would copy the accumulated prefix on every event,
// quadratic in the payload size. Blocks are always held by pointer
// (activeBlocks map), which the Builders' no-copy requirement relies on.
type streamingBlock struct {
	blockType string
	toolID    string
	toolName  string
	args      strings.Builder
	signature strings.Builder
	// reasoningID identifies this reasoning block in the part stream. Blocks
	// need distinct IDs so each signature stays paired with its own text.
	reasoningID  string
	redactedData string
}

// reasoningBlockID derives a stable per-block stream ID from the message ID and
// the block's index in the response.
func (h *streamHandler) reasoningBlockID(idx int) string {
	return fmt.Sprintf("%s:%d", h.messageID, idx)
}

// ---------- helpers ----------

func generateID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic("anthropic: generateID entropy failure: " + err.Error())
	}
	return fmt.Sprintf("toolu_%x", b)
}

func convertUsage(u *messagesUsage) sdk.Usage {
	total := u.InputTokens + u.OutputTokens
	detail := sdk.InputTokenDetail{
		CacheReadTokens:  u.CacheReadInputTokens,
		CacheWriteTokens: u.CacheCreationInputTokens,
	}
	if u.CacheCreation != nil {
		detail.CacheWrite5mTokens = u.CacheCreation.Ephemeral5mInputTokens
		detail.CacheWrite1hTokens = u.CacheCreation.Ephemeral1hInputTokens
	}
	return sdk.Usage{
		InputTokens:       u.InputTokens,
		OutputTokens:      u.OutputTokens,
		TotalTokens:       total,
		CachedInputTokens: u.CacheReadInputTokens,
		InputTokenDetails: detail,
	}
}

func mapFinishReason(reason string) sdk.FinishReason {
	switch reason {
	case "end_turn", "stop_sequence":
		return sdk.FinishReasonStop
	case "tool_use":
		return sdk.FinishReasonToolCalls
	case "max_tokens":
		return sdk.FinishReasonLength
	default:
		return sdk.FinishReasonUnknown
	}
}

func signatureMetadata(signature string) sdk.ProviderMetadata {
	return sdk.NewProviderMetadata(metadataNamespace, map[string]string{metadataKeySignature: signature})
}

func redactedMetadata(data string) sdk.ProviderMetadata {
	return sdk.NewProviderMetadata(metadataNamespace, map[string]string{metadataKeyRedactedData: data})
}

func signatureOf(meta sdk.ProviderMetadata) string {
	return meta.Get(metadataNamespace, metadataKeySignature)
}

// redactedDataOf returns the encrypted payload of a redacted thinking block.
func redactedDataOf(meta sdk.ProviderMetadata) string {
	return meta.Get(metadataNamespace, metadataKeyRedactedData)
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
