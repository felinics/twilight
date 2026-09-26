package generativeai

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
	"github.com/google/jsonschema-go/jsonschema"
)

const defaultBaseURL = "https://generativelanguage.googleapis.com/v1beta"

// ThinkingBudgetDynamic instructs the model to choose its own thinking budget
// (thinkingBudget: -1, AUTOMATIC). Supported on 2.5-generation models.
const ThinkingBudgetDynamic = -1

// ThinkingBudgetDisabled disables thinking (thinkingBudget: 0, DISABLED).
// Legal on Flash/Lite models; the API rejects it for 2.5 Pro.
const ThinkingBudgetDisabled = 0

// ThinkingConfig holds provider-level thinking parameters for Google Generative
// AI models. The caller is responsible for choosing the right fields for the
// target model generation:
//   - ThinkingBudget is used by 2.5-generation models (e.g. gemini-2.5-flash).
//   - ThinkingLevel is used by 3.x-generation models (e.g. gemini-3.1-pro-preview).
//
// Both fields are passed through as-is; the SDK does not validate mutual
// exclusivity or budget ranges — misuse results in a 400 from the API.
type ThinkingConfig struct {
	// ThinkingBudget sets the token budget for 2.5-generation models.
	// Use ThinkingBudgetDynamic (-1) for automatic, ThinkingBudgetDisabled (0) to
	// disable (only valid on Flash/Lite). Nil means this field is not sent.
	ThinkingBudget *int
	// ThinkingLevel sets the effort tier for 3.x-generation models. The wire
	// format is an uppercase proto enum: "MINIMAL", "LOW", "MEDIUM", "HIGH".
	// Values are upper-cased before being sent, so "high" and "HIGH" are
	// equivalent here. Empty means not sent.
	ThinkingLevel string
	// IncludeThoughts controls whether thought content is returned. Nil means not
	// sent. Note that the API only emits thought parts when this is true, so
	// reasoning stream parts stay empty unless it is explicitly enabled.
	IncludeThoughts *bool
}

// isEmpty reports whether the config carries no fields at all. An empty config
// is treated as "not configured" so it falls through to req.ReasoningEffort
// rather than sending a bare `thinkingConfig: {}` and swallowing the request's
// effort.
func (c *ThinkingConfig) isEmpty() bool {
	return c.ThinkingBudget == nil && c.ThinkingLevel == "" && c.IncludeThoughts == nil
}

// normalizeThinkingLevel converts an effort tier to the uppercase form the
// thinkingLevel proto enum requires, trimming surrounding space. It is a
// dialect translation and deliberately not a validation: a tier Google does not
// define (e.g. "xhigh") is upper-cased and sent, surfacing as a 400 from the
// API rather than being silently downgraded to a supported neighbour.
func normalizeThinkingLevel(level string) string {
	return strings.ToUpper(strings.TrimSpace(level))
}

type Provider struct {
	headers    map[string]string
	apiKey     string
	baseURL    string
	httpClient *http.Client
	thinking   *ThinkingConfig
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
	return func(p *Provider) {
		p.apiKey = apiKey
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

// WithThinking injects provider-level thinking configuration. When set, it
// takes precedence over the generic req.ReasoningEffort from a Generate
// call — the more expressive provider option wins. See ThinkingConfig for
// field semantics and generation-specific guidance.
func WithThinking(cfg ThinkingConfig) Option {
	return func(p *Provider) {
		p.thinking = &cfg
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
	return "google-generative-ai"
}

func (p *Provider) ListModels(ctx context.Context) ([]sdk.Model, error) {
	resp, err := utils.FetchJSON[googleModelsListResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodGet,
		BaseURL: p.baseURL,
		Path:    "/models",
		Headers: p.requestHeaders(ctx),
	})
	if err != nil {
		return nil, fmt.Errorf("google: list models request failed: %w", err)
	}

	models := make([]sdk.Model, 0, len(resp.Models))
	for _, m := range resp.Models {
		id := strings.TrimPrefix(m.Name, "models/")
		models = append(models, sdk.Model{
			ID:          id,
			DisplayName: m.DisplayName,
			Provider:    p,
			Type:        googleModelType(m.SupportedGenerationMethods),
		})
	}
	return models, nil
}

func (p *Provider) Test(ctx context.Context) *sdk.ProviderTestResult {
	_, err := utils.FetchJSON[googleModelsListResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodGet,
		BaseURL: p.baseURL,
		Path:    "/models",
		Query:   map[string]string{"pageSize": "1"},
		Headers: p.requestHeaders(ctx),
	})
	if err != nil {
		return classifyError(err)
	}
	return &sdk.ProviderTestResult{Status: sdk.ProviderStatusOK, Message: "ok"}
}

func (p *Provider) TestModel(ctx context.Context, modelID string) (*sdk.ModelTestResult, error) {
	modelPath := getModelPath(modelID)
	_, err := utils.FetchJSON[googleModelObject](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodGet,
		BaseURL: p.baseURL,
		Path:    "/" + modelPath,
		Headers: p.requestHeaders(ctx),
	})
	if err == nil {
		return &sdk.ModelTestResult{Supported: true, Message: "supported"}, nil
	}
	var apiErr *utils.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusNotFound {
		return nil, fmt.Errorf("google: test model request failed: %w", err)
	}

	status, probeErr := utils.ProbeStatus(ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    "/" + modelPath + ":generateContent",
		Headers: p.requestHeaders(ctx),
		Body: map[string]any{
			"contents":         []map[string]any{{"parts": []map[string]string{{"text": "hi"}}}},
			"generationConfig": map[string]int{"maxOutputTokens": 1},
		},
	})
	if probeErr != nil {
		return nil, fmt.Errorf("google: probe model request failed: %w", probeErr)
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

func googleModelType(methods []string) sdk.ModelType {
	hasGenerate := false
	hasEmbed := false
	for _, m := range methods {
		switch m {
		case "generateContent":
			hasGenerate = true
		case "embedContent":
			hasEmbed = true
		}
	}
	if hasEmbed && !hasGenerate {
		return sdk.ModelTypeEmbedding
	}
	return sdk.ModelTypeChat
}

// ---------- DoGenerate ----------

func (p *Provider) DoGenerate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) { //nolint:gocritic // interface method
	if req.Model == "" {
		return sdk.ModelResult{}, fmt.Errorf("google: model is required")
	}

	body, err := p.buildRequest(&req)
	if err != nil {
		return sdk.ModelResult{}, fmt.Errorf("google: build request: %w", err)
	}
	modelPath := getModelPath(req.Model)

	resp, err := utils.FetchJSON[generateResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    "/" + modelPath + ":generateContent",
		Headers: p.requestHeaders(ctx),
		Body:    body,
	})
	if err != nil {
		var apiErr *utils.APIError
		if errors.As(err, &apiErr) {
			return sdk.ModelResult{}, fmt.Errorf("google: generateContent request failed: %s", apiErr.Detail())
		}
		return sdk.ModelResult{}, fmt.Errorf("google: generateContent request failed: %w", err)
	}

	return p.parseResponse(resp)
}

// ---------- buildRequest ----------

func (p *Provider) buildRequest(req *sdk.Request) (*generateRequest, error) {
	messages, err := messagecompat.Normalize(req.Messages, sdk.MessageRoleCapabilities{})
	if err != nil {
		return nil, err
	}
	contents, sysInstruction := convertMessages(req.System, messages)

	body := &generateRequest{
		Contents:          contents,
		SystemInstruction: sysInstruction,
	}

	genCfg := &generationConfig{
		Temperature:      req.Temperature,
		TopP:             req.TopP,
		MaxOutputTokens:  req.MaxTokens,
		FrequencyPenalty: req.FrequencyPenalty,
		PresencePenalty:  req.PresencePenalty,
		Seed:             req.Seed,
	}
	if len(req.StopSequences) > 0 {
		genCfg.StopSequences = req.StopSequences
	}
	if req.ResponseFormat != nil {
		switch req.ResponseFormat.Type {
		case sdk.ResponseFormatJSONObject, sdk.ResponseFormatJSONSchema:
			genCfg.ResponseMimeType = "application/json"
			if req.ResponseFormat.JSONSchema != nil {
				genCfg.ResponseSchema = req.ResponseFormat.JSONSchema
			}
		}
	}

	// Thinking configuration: provider-level WithThinking takes precedence over
	// the generic req.ReasoningEffort. When both are present the provider
	// option wins — it is more expressive and its intent is unambiguous. An
	// empty WithThinking carries no intent, so it falls through to
	// ReasoningEffort instead of suppressing it. When only ReasoningEffort is
	// set it is treated as a thinkingLevel tier, which is structurally
	// equivalent (both are named effort tiers). When neither is set
	// thinkingConfig is omitted entirely (omitempty on generationConfig).
	//
	// thinkingLevel is a proto enum and only accepts uppercase members
	// ("MINIMAL", "LOW", "MEDIUM", "HIGH"), while effort tiers elsewhere in this
	// SDK are lowercase. Upper-casing here is dialect translation, not
	// validation: tiers outside Google's enum (e.g. "xhigh") are still passed
	// through and surface as a 400 rather than being silently remapped.
	//
	// No mutual-exclusion check and no budget-range validation are performed;
	// both would require the SDK to encode knowledge about which model
	// generation accepts which fields — that is the caller's responsibility.
	// Illegal combinations surface as 400 errors from the API.
	switch {
	case p.thinking != nil && !p.thinking.isEmpty():
		tc := &thinkingConfig{
			ThinkingBudget:  p.thinking.ThinkingBudget,
			ThinkingLevel:   normalizeThinkingLevel(p.thinking.ThinkingLevel),
			IncludeThoughts: p.thinking.IncludeThoughts,
		}
		genCfg.ThinkingConfig = tc
	case req.ReasoningEffort != nil:
		if level := normalizeThinkingLevel(*req.ReasoningEffort); level != "" {
			genCfg.ThinkingConfig = &thinkingConfig{ThinkingLevel: level}
		}
	}

	body.GenerationConfig = genCfg

	if len(req.Tools) > 0 {
		tools, toolCfg := convertTools(req.Tools, req.ToolChoice)
		body.Tools = tools
		body.ToolConfig = toolCfg
	}

	if err := sdk.ApplyProviderOptions(p.Name(), req.ProviderOptions, body); err != nil {
		return nil, fmt.Errorf("google: %w", err)
	}
	return body, nil
}

// ---------- message conversion ----------

func convertMessages(systemPrompt string, messages []sdk.Message) ([]content, *content) { //nolint:gocritic // unnamed results are clear in context
	contents := make([]content, 0, len(messages))
	var sysInstruction *content

	if systemPrompt != "" {
		sysInstruction = &content{
			Parts: []contentPart{{Text: systemPrompt}},
		}
	}

	for _, msg := range messages {
		if msg.Role == sdk.MessageRoleSystem {
			if sysInstruction == nil {
				sysInstruction = &content{}
			}
			sysInstruction.Parts = append(sysInstruction.Parts, contentPart{Text: messagecompat.InstructionText(msg)})
			continue
		}
		contents = append(contents, convertMessage(msg)...)
	}
	return contents, sysInstruction
}

func convertMessage(msg sdk.Message) []content {
	switch msg.Role {
	case sdk.MessageRoleSystem:
		return nil
	case sdk.MessageRoleAssistant:
		return []content{convertAssistantMessage(msg)}
	case sdk.MessageRoleTool:
		return []content{convertToolResultMessage(msg)}
	default:
		return []content{convertUserMessage(msg)}
	}
}

func convertUserMessage(msg sdk.Message) content {
	var parts []contentPart
	for _, part := range msg.Content {
		switch p := part.(type) {
		case sdk.TextPart:
			parts = append(parts, contentPart{Text: p.Text})
		case sdk.ImagePart:
			if strings.HasPrefix(p.Image, "data:") || strings.Contains(p.Image, ";base64,") {
				mediaType, data := parseDataURI(p.Image)
				if mediaType == "" {
					mediaType = p.MediaType
				}
				if mediaType == "" {
					mediaType = "image/jpeg"
				}
				parts = append(parts, contentPart{
					InlineData: &inlineData{MimeType: mediaType, Data: data},
				})
			} else {
				mediaType := p.MediaType
				if mediaType == "" {
					mediaType = "image/jpeg"
				}
				parts = append(parts, contentPart{
					FileData: &fileData{MimeType: mediaType, FileURI: p.Image},
				})
			}
		case sdk.FilePart:
			data, mediaType := utils.NormalizeFileData(p.Data, p.MediaType)
			parts = append(parts, contentPart{
				InlineData: &inlineData{MimeType: mediaType, Data: data},
			})
		}
	}
	return content{Role: "user", Parts: parts}
}

func convertAssistantMessage(msg sdk.Message) content {
	var parts []contentPart
	for _, part := range msg.Content {
		switch p := part.(type) {
		case sdk.TextPart:
			if p.Text != "" {
				cp := contentPart{Text: p.Text}
				if sig := extractGoogleThoughtSignature(p.ProviderMetadata); sig != "" {
					cp.ThoughtSignature = sig
				}
				parts = append(parts, cp)
			}
		case sdk.ReasoningPart:
			// Only this dialect's parts can be replayed: a signature is bound to
			// the exact part it arrived on, and foreign reasoning re-sent as
			// text teaches the model to imitate it in user-visible answers.
			if p.Format != sdk.ReasoningFormatGoogle {
				continue
			}
			sig := extractGoogleThoughtSignature(p.ProviderMetadata)
			// A signature may arrive on a part with no text; dropping it would
			// break the positional context the signature is bound to.
			if p.Text == "" && sig == "" {
				continue
			}
			thought := true
			cp := contentPart{
				Text:             p.Text,
				Thought:          &thought,
				ThoughtSignature: sig,
			}
			parts = append(parts, cp)
		case sdk.ToolCallPart:
			cp := contentPart{
				FunctionCall: &functionCall{
					Name: p.ToolName,
					Args: p.Input.Object(),
				},
			}
			if sig := extractGoogleThoughtSignature(p.ProviderMetadata); sig != "" {
				cp.ThoughtSignature = sig
			}
			parts = append(parts, cp)
		}
	}
	return content{Role: "model", Parts: parts}
}

func convertToolResultMessage(msg sdk.Message) content {
	var parts []contentPart
	for _, part := range msg.Content {
		if trp, ok := part.(sdk.ToolResultPart); ok {
			parts = append(parts, contentPart{
				FunctionResponse: &functionResponse{
					Name: trp.ToolName,
					Response: functionResponseVal{
						Name:    trp.ToolName,
						Content: functionResponseContent(trp.Result),
					},
				},
			})
		}
	}
	return content{Role: "user", Parts: parts}
}

// ---------- tool conversion ----------

func convertTools(tools []sdk.ToolDefinition, choice sdk.ToolChoice) ([]toolGroup, *toolConfig) {
	decls := make([]functionDeclaration, 0, len(tools))
	for _, t := range tools {
		decls = append(decls, functionDeclaration{
			Name:                 t.Name,
			Description:          t.Description,
			ParametersJSONSchema: schemaJSON(t.Parameters),
		})
	}

	return []toolGroup{{FunctionDeclarations: decls}}, convertToolChoice(choice)
}

// convertToolChoice maps the provider-neutral ToolChoice onto Google's
// toolConfig.functionCallingConfig form. The zero ToolChoice carries no intent,
// so it emits no toolConfig at all — matching the legacy open-ended field where
// an absent value left the API default alone. A tool-scoped choice becomes ANY
// restricted to that one function name; Google expresses "only this tool" that
// way rather than with a distinct mode.
func convertToolChoice(choice sdk.ToolChoice) *toolConfig {
	switch choice.Mode {
	case sdk.ToolChoiceAuto:
		return &toolConfig{FunctionCallingConfig: &functionCallingConfig{Mode: "AUTO"}}
	case sdk.ToolChoiceNone:
		return &toolConfig{FunctionCallingConfig: &functionCallingConfig{Mode: "NONE"}}
	case sdk.ToolChoiceRequired:
		return &toolConfig{FunctionCallingConfig: &functionCallingConfig{Mode: "ANY"}}
	case sdk.ToolChoiceTool:
		fcc := &functionCallingConfig{Mode: "ANY"}
		if choice.Tool != "" {
			fcc.AllowedFunctionNames = []string{choice.Tool}
		}
		return &toolConfig{FunctionCallingConfig: fcc}
	default:
		return nil
	}
}

// ---------- parseResponse ----------

func (p *Provider) parseResponse(resp *generateResponse) (sdk.ModelResult, error) {
	var result sdk.ModelResult

	if resp.UsageMetadata != nil {
		result.Usage = convertUsage(resp.UsageMetadata)
	}

	if len(resp.Candidates) == 0 {
		result.FinishReason = sdk.FinishReasonOther
		result.RawFinishReason = ""
		return result, nil
	}

	candidate := resp.Candidates[0]
	hasToolCalls := false

	if candidate.Content != nil {
		for _, part := range candidate.Content.Parts {
			switch {
			case part.FunctionCall != nil:
				hasToolCalls = true
				id := generateID()
				result.ToolCalls = append(result.ToolCalls, sdk.ToolCall{
					ToolCallID:       id,
					ToolName:         part.FunctionCall.Name,
					Input:            sdk.ParseToolArguments(string(part.FunctionCall.Args)),
					ProviderMetadata: googleThoughtSignatureMetadata(part.ThoughtSignature),
				})
			case part.Text != "":
				isThought := part.Thought != nil && *part.Thought
				if isThought {
					// Each thought part is its own block: a signature is bound
					// to the exact part it arrived on and cannot be merged with
					// another part's. Parts without a signature are normal —
					// Google signs only the first of several parallel calls.
					result.ReasoningParts = append(result.ReasoningParts, sdk.ReasoningPart{
						Text:             part.Text,
						Format:           sdk.ReasoningFormatGoogle,
						Model:            resp.ModelVersion,
						ProviderMetadata: googleThoughtSignatureMetadata(part.ThoughtSignature),
					})
				} else {
					result.Text += part.Text
					// Gemini binds a thought signature to the exact part that
					// carries it, and a response without a function call signs
					// an ordinary text part — usually the last one. That token
					// has to survive even though the text itself is flattened.
					if part.ThoughtSignature != "" {
						result.TextProviderMetadata = googleThoughtSignatureMetadata(part.ThoughtSignature)
					}
				}
			case part.InlineData != nil:
				result.Files = append(result.Files, sdk.GeneratedFile{
					Data:      part.InlineData.Data,
					MediaType: part.InlineData.MimeType,
				})
			}
		}
	}

	result.FinishReason = mapFinishReason(candidate.FinishReason, hasToolCalls)
	result.RawFinishReason = candidate.FinishReason
	result.Reasoning = sdk.ReasoningText(result.ReasoningParts)

	return result, nil
}

// ---------- DoStream ----------

func (p *Provider) DoStream(ctx context.Context, req sdk.Request) (<-chan sdk.StreamPart, error) { //nolint:gocritic // interface method
	if req.Model == "" {
		return nil, fmt.Errorf("google: model is required")
	}

	body, err := p.buildRequest(&req)
	if err != nil {
		return nil, fmt.Errorf("google: build request: %w", err)
	}
	modelPath := getModelPath(req.Model)

	ch := make(chan sdk.StreamPart, 64)

	go func() {
		defer close(ch)

		var (
			textStartSent      bool
			reasoningStartSent bool
			rawFinishReason    string
			finishReason       sdk.FinishReason
			usage              sdk.Usage
			flushed            bool
			hasToolCalls       bool
			blockCounter       int
			currentTextID      string
			currentReasoningID string
			lastThoughtSig     string
			lastTextSig        string
			streamModel        string
		)

		send := func(part sdk.StreamPart) bool {
			select {
			case ch <- part:
				return true
			case <-ctx.Done():
				return false
			}
		}

		reasoningEndMeta := func() sdk.ProviderMetadata {
			return googleThoughtSignatureMetadata(lastThoughtSig)
		}

		textEndMeta := func() sdk.ProviderMetadata {
			if lastTextSig == "" {
				return nil
			}
			meta := googleThoughtSignatureMetadata(lastTextSig)
			lastTextSig = ""
			return meta
		}

		flush := func() {
			if flushed {
				return
			}
			flushed = true
			if reasoningStartSent {
				send(&sdk.ReasoningEndPart{ID: currentReasoningID, Format: sdk.ReasoningFormatGoogle, Model: streamModel, ProviderMetadata: reasoningEndMeta()})
				reasoningStartSent = false
			}
			if textStartSent {
				send(&sdk.TextEndPart{ID: currentTextID, ProviderMetadata: textEndMeta()})
				textStartSent = false
			}
		}

		if !send(&sdk.StartPart{}) {
			return
		}
		if !send(&sdk.StartStepPart{}) {
			return
		}

		err := utils.FetchSSE(ctx, p.httpClient, &utils.RequestOptions{
			Method:  http.MethodPost,
			BaseURL: p.baseURL,
			Path:    "/" + modelPath + ":streamGenerateContent",
			Query:   map[string]string{"alt": "sse"},
			Headers: p.requestHeaders(ctx),
			Body:    body,
		}, func(ev *utils.SSEEvent) error {
			var chunk generateResponse
			if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
				send(&sdk.ErrorPart{Error: fmt.Errorf("google: unmarshal chunk: %w", err)})
				return err
			}

			if chunk.ModelVersion != "" {
				streamModel = chunk.ModelVersion
			}
			if chunk.UsageMetadata != nil {
				usage = convertUsage(chunk.UsageMetadata)
			}

			if len(chunk.Candidates) == 0 {
				return nil
			}
			candidate := chunk.Candidates[0]

			if candidate.Content != nil {
				for _, part := range candidate.Content.Parts {
					switch {
					case part.FunctionCall != nil:
						if reasoningStartSent {
							send(&sdk.ReasoningEndPart{ID: currentReasoningID, Format: sdk.ReasoningFormatGoogle, Model: streamModel, ProviderMetadata: reasoningEndMeta()})
							reasoningStartSent = false
						}
						if textStartSent {
							send(&sdk.TextEndPart{ID: currentTextID})
							textStartSent = false
						}

						hasToolCalls = true
						toolCallID := generateID()
						argsStr := string(part.FunctionCall.Args)

						send(&sdk.ToolInputStartPart{
							ID:       toolCallID,
							ToolName: part.FunctionCall.Name,
						})
						send(&sdk.ToolInputDeltaPart{
							ID:    toolCallID,
							Delta: argsStr,
						})
						send(&sdk.ToolInputEndPart{ID: toolCallID})

						send(&sdk.StreamToolCallPart{
							ToolCallID:       toolCallID,
							ToolName:         part.FunctionCall.Name,
							Input:            sdk.ParseToolArguments(argsStr),
							ProviderMetadata: googleThoughtSignatureMetadata(part.ThoughtSignature),
						})
					case part.Text != "":
						isThought := part.Thought != nil && *part.Thought
						if isThought {
							if part.ThoughtSignature != "" {
								lastThoughtSig = part.ThoughtSignature
							}
							if textStartSent {
								send(&sdk.TextEndPart{ID: currentTextID})
								textStartSent = false
							}
							if !reasoningStartSent {
								currentReasoningID = fmt.Sprintf("%d", blockCounter)
								blockCounter++
								send(&sdk.ReasoningStartPart{ID: currentReasoningID, Format: sdk.ReasoningFormatGoogle, Model: streamModel})
								reasoningStartSent = true
							}
							send(&sdk.ReasoningDeltaPart{ID: currentReasoningID, Text: part.Text, Format: sdk.ReasoningFormatGoogle, Model: streamModel})
						} else {
							if reasoningStartSent {
								send(&sdk.ReasoningEndPart{ID: currentReasoningID, Format: sdk.ReasoningFormatGoogle, Model: streamModel, ProviderMetadata: reasoningEndMeta()})
								reasoningStartSent = false
							}
							if !textStartSent {
								currentTextID = fmt.Sprintf("%d", blockCounter)
								blockCounter++
								send(&sdk.TextStartPart{ID: currentTextID})
								textStartSent = true
							}
							// A signature can ride on an ordinary text part —
							// possibly one with empty text — and belongs to the
							// block, delivered when it closes.
							if part.ThoughtSignature != "" {
								lastTextSig = part.ThoughtSignature
							}
							send(&sdk.TextDeltaPart{ID: currentTextID, Text: part.Text})
						}
					case part.InlineData != nil:
						if textStartSent {
							send(&sdk.TextEndPart{ID: currentTextID, ProviderMetadata: textEndMeta()})
							textStartSent = false
						}
						if reasoningStartSent {
							send(&sdk.ReasoningEndPart{ID: currentReasoningID, Format: sdk.ReasoningFormatGoogle, Model: streamModel, ProviderMetadata: reasoningEndMeta()})
							reasoningStartSent = false
						}
						send(&sdk.StreamFilePart{
							File: sdk.GeneratedFile{
								Data:      part.InlineData.Data,
								MediaType: part.InlineData.MimeType,
							},
						})
					}
				}
			}

			if candidate.FinishReason != "" {
				rawFinishReason = candidate.FinishReason
				finishReason = mapFinishReason(rawFinishReason, hasToolCalls)

				flush()

				send(&sdk.FinishStepPart{
					FinishReason:    finishReason,
					RawFinishReason: rawFinishReason,
					Usage:           usage,
					Response:        sdk.ResponseMetadata{},
				})
			}

			return nil
		})

		if err != nil {
			var apiErr *utils.APIError
			if errors.As(err, &apiErr) {
				send(&sdk.ErrorPart{Error: fmt.Errorf("google: stream failed: %s", apiErr.Detail())})
			} else {
				send(&sdk.ErrorPart{Error: fmt.Errorf("google: stream failed: %w", err)})
			}
		}

		flush()

		send(&sdk.FinishPart{
			FinishReason:    finishReason,
			RawFinishReason: rawFinishReason,
			TotalUsage:      usage,
		})
	}()

	return ch, nil
}

// ---------- helpers ----------

func getModelPath(modelID string) string {
	if strings.Contains(modelID, "/") {
		return modelID
	}
	return "models/" + modelID
}

func (p *Provider) requestHeaders(ctx context.Context) map[string]string {
	return utils.RequestHeaders(ctx, map[string]string{"x-goog-api-key": p.apiKey}, p.headers)
}

func generateID() string {
	b := make([]byte, 12)
	if _, err := rand.Read(b); err != nil {
		panic("generativeai: generateID entropy failure: " + err.Error())
	}
	return fmt.Sprintf("call_%x", b)
}

func convertUsage(u *usageMetadata) sdk.Usage {
	candidateTokens := u.CandidatesTokenCount
	thoughtTokens := u.ThoughtsTokenCount
	cachedTokens := u.CachedContentTokenCount

	return sdk.Usage{
		InputTokens:       u.PromptTokenCount,
		OutputTokens:      candidateTokens + thoughtTokens,
		TotalTokens:       u.TotalTokenCount,
		ReasoningTokens:   thoughtTokens,
		CachedInputTokens: cachedTokens,
		InputTokenDetails: sdk.InputTokenDetail{
			NoCacheTokens:   u.PromptTokenCount - cachedTokens,
			CacheReadTokens: cachedTokens,
		},
		OutputTokenDetails: sdk.OutputTokenDetail{
			TextTokens:      candidateTokens,
			ReasoningTokens: thoughtTokens,
		},
	}
}

func mapFinishReason(reason string, hasToolCalls bool) sdk.FinishReason {
	switch reason {
	case "STOP":
		if hasToolCalls {
			return sdk.FinishReasonToolCalls
		}
		return sdk.FinishReasonStop
	case "MAX_TOKENS":
		return sdk.FinishReasonLength
	case "SAFETY", "RECITATION", "IMAGE_SAFETY", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII":
		return sdk.FinishReasonContentFilter
	case "MALFORMED_FUNCTION_CALL":
		return sdk.FinishReasonError
	default:
		return sdk.FinishReasonOther
	}
}

const (
	metadataNamespace           = "google"
	metadataKeyThoughtSignature = "thoughtSignature"
)

func extractGoogleThoughtSignature(meta sdk.ProviderMetadata) string {
	return meta.Get(metadataNamespace, metadataKeyThoughtSignature)
}

func googleThoughtSignatureMetadata(sig string) sdk.ProviderMetadata {
	return sdk.NewProviderMetadata(metadataNamespace, map[string]string{metadataKeyThoughtSignature: sig})
}

// schemaJSON encodes a tool schema for the parametersJsonSchema field; a tool
// without parameters sends none.
func schemaJSON(s *jsonschema.Schema) json.RawMessage {
	if s == nil {
		return nil
	}
	raw, err := json.Marshal(s)
	if err != nil {
		return nil
	}
	return raw
}

// functionResponseContent is the tool output as the functionResponse value:
// a JSON document as itself, text as a string.
func functionResponseContent(out sdk.ToolOutput) any {
	if out.IsJSON() {
		return out.JSON
	}
	return out.Text
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

func parseDataURI(uri string) (mediaType, data string) {
	idx := strings.Index(uri, ",")
	if idx < 0 {
		return "", uri
	}
	header := uri[:idx]
	data = uri[idx+1:]
	header = strings.TrimPrefix(header, "data:")
	header = strings.TrimSuffix(header, ";base64")
	return header, data
}
