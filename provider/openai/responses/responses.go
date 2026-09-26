package responses

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/felinics/twilight/internal/messagecompat"
	"github.com/felinics/twilight/internal/utils"
	openaiutil "github.com/felinics/twilight/provider/openai"
	"github.com/felinics/twilight/sdk"
)

const (
	defaultBaseURL = "https://api.openai.com/v1"

	// Output item types for OpenAI Responses API
	outputTypeMessage      = "message"
	outputTypeReasoning    = "reasoning"
	outputTypeFunctionCall = "function_call"
)

type Provider struct {
	apiKey         string
	baseURL        string
	httpClient     *http.Client
	prepareRequest func(*http.Request) error
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

// WithBedrockRegion enables AWS SigV4 authentication for Amazon Bedrock's
// OpenAI-compatible Responses endpoint using the default AWS credential chain.
func WithBedrockRegion(region string) Option {
	return func(p *Provider) {
		p.prepareRequest = utils.NewBedrockDefaultCredentialsPreparer(region)
	}
}

// WithBedrockCredentials enables AWS SigV4 authentication for Amazon Bedrock's
// OpenAI-compatible Responses endpoint using static credentials.
func WithBedrockCredentials(region, accessKeyID, secretAccessKey, sessionToken string) Option {
	return func(p *Provider) {
		p.prepareRequest = utils.NewBedrockStaticCredentialsPreparer(region, accessKeyID, secretAccessKey, sessionToken)
	}
}

func New(options ...Option) *Provider {
	p := &Provider{
		baseURL:    defaultBaseURL,
		httpClient: &http.Client{},
	}
	for _, o := range options {
		o(p)
	}
	return p
}

func (p *Provider) Name() string { return "openai-responses" }

func (p *Provider) ListModels(ctx context.Context) ([]sdk.Model, error) {
	resp, err := utils.FetchJSON[modelsListResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodGet,
		BaseURL: p.baseURL,
		Path:    "/models",
		Headers: p.authHeaders(),
		Prepare: p.prepareRequest,
	})
	if err != nil {
		return nil, fmt.Errorf("openai-responses: list models request failed: %w", err)
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
		return nil, fmt.Errorf("openai-responses: test model request failed: %w", err)
	}

	status, probeErr := utils.ProbeStatus(ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    "/responses",
		Headers: p.authHeaders(),
		Prepare: p.prepareRequest,
		Body: map[string]any{
			"model":             modelID,
			"input":             "hi",
			"max_output_tokens": 1,
		},
	})
	if probeErr != nil {
		return nil, fmt.Errorf("openai-responses: probe model request failed: %w", probeErr)
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

func (p *Provider) authHeaders() map[string]string {
	if p.prepareRequest != nil {
		return nil
	}
	if p.apiKey == "" {
		return nil
	}
	return utils.AuthHeader(p.apiKey)
}

// ---------- DoGenerate ----------

func (p *Provider) DoGenerate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) { //nolint:gocritic // interface method
	if req.Model == "" {
		return sdk.ModelResult{}, fmt.Errorf("openai-responses: model is required")
	}

	wireReq, err := p.buildRequest(&req)
	if err != nil {
		return sdk.ModelResult{}, fmt.Errorf("openai-responses: build request: %w", err)
	}

	resp, err := utils.FetchJSON[responsesResponse](ctx, p.httpClient, &utils.RequestOptions{
		Method:  http.MethodPost,
		BaseURL: p.baseURL,
		Path:    "/responses",
		Headers: p.authHeaders(),
		Prepare: p.prepareRequest,
		Body:    wireReq,
	})
	if err != nil {
		var apiErr *utils.APIError
		if errors.As(err, &apiErr) {
			return sdk.ModelResult{}, fmt.Errorf("openai-responses: request failed: %s", apiErr.Detail())
		}
		return sdk.ModelResult{}, fmt.Errorf("openai-responses: request failed: %w", err)
	}

	if resp.Error != nil {
		return sdk.ModelResult{}, fmt.Errorf("openai-responses: api error [%s]: %s", resp.Error.Code, resp.Error.Message)
	}

	return p.parseResponse(resp)
}

// ---------- buildRequest ----------

func (p *Provider) buildRequest(params *sdk.Request) (*responsesRequest, error) {
	messages, err := messagecompat.Normalize(params.Messages, sdk.MessageRoleCapabilities{
		Developer:             true,
		MidConversationSystem: true,
	})
	if err != nil {
		return nil, err
	}
	// The SDK carries conversation state in its own message list, so the server
	// must not store it. That makes encrypted_content the only channel for
	// reasoning state across turns, and it is returned only when requested.
	store := false
	req := &responsesRequest{
		Model:           params.Model,
		Instructions:    params.System,
		Input:           convertToResponsesInput(messages),
		Temperature:     params.Temperature,
		TopP:            params.TopP,
		MaxOutputTokens: params.MaxTokens,
		PromptCacheKey:  params.PromptCacheKey,
		Include:         []string{openaiutil.IncludeReasoningEncryptedContent},
		Store:           &store,
	}

	if len(params.Tools) > 0 {
		req.Tools = convertResponsesTools(params.Tools)
		req.ToolChoice = convertToolChoice(params.ToolChoice)
	}

	if params.ResponseFormat != nil {
		tf := &responsesTextFmt{}
		switch params.ResponseFormat.Type {
		case sdk.ResponseFormatJSONObject:
			tf.Format = &responsesTextFormat{Type: "json_object"}
		case sdk.ResponseFormatJSONSchema:
			tf.Format = &responsesTextFormat{
				Type:   "json_schema",
				Name:   "response",
				Schema: params.ResponseFormat.JSONSchema,
			}
		}
		if tf.Format != nil {
			req.Text = tf
		}
	}

	if (params.ReasoningEffort != nil && *params.ReasoningEffort != "") ||
		(params.ReasoningSummary != nil && *params.ReasoningSummary != "") {
		req.Reasoning = &responsesReasoning{}
		if params.ReasoningEffort != nil {
			req.Reasoning.Effort = *params.ReasoningEffort
		}
		if params.ReasoningSummary != nil {
			req.Reasoning.Summary = *params.ReasoningSummary
		}
	}

	if err := sdk.ApplyProviderOptions(p.Name(), params.ProviderOptions, req); err != nil {
		return nil, fmt.Errorf("openai: %w", err)
	}
	return req, nil
}

func convertResponsesTools(tools []sdk.ToolDefinition) []responsesTool {
	out := make([]responsesTool, 0, len(tools))
	for _, t := range tools {
		out = append(out, responsesTool{
			Type:        "function",
			Name:        t.Name,
			Description: t.Description,
			Parameters:  t.Parameters,
		})
	}
	return out
}

// convertToolChoice maps the frozen tool choice onto this wire format. An empty
// Mode means no choice was requested, so the field stays unset; every other
// mode keeps the shape the endpoint already accepted before the frozen
// ToolChoice type replaced the open `any`.
func convertToolChoice(choice sdk.ToolChoice) any {
	switch choice.Mode {
	case sdk.ToolChoiceAuto, sdk.ToolChoiceNone, sdk.ToolChoiceRequired:
		return string(choice.Mode)
	case sdk.ToolChoiceTool:
		return map[string]any{
			"type":     "function",
			"function": map[string]any{"name": choice.Tool},
		}
	default:
		return nil
	}
}

// ---------- input conversion ----------

func convertToResponsesInput(messages []sdk.Message) []json.RawMessage {
	items := make([]json.RawMessage, 0, len(messages))

	for _, msg := range messages {
		items = append(items, convertResponsesMessage(msg)...)
	}
	return items
}

func convertResponsesMessage(msg sdk.Message) []json.RawMessage {
	switch msg.Role {
	case sdk.MessageRoleSystem:
		return []json.RawMessage{marshalRaw(responsesSystemMessage{
			Role:    "system",
			Content: textFromParts(msg.Content),
		})}

	case sdk.MessageRoleDeveloper:
		return []json.RawMessage{marshalRaw(responsesSystemMessage{
			Role:    "developer",
			Content: textFromParts(msg.Content),
		})}

	case sdk.MessageRoleUser:
		return convertResponsesUserMessage(msg)

	case sdk.MessageRoleAssistant:
		return convertResponsesAssistantMessage(msg)

	case sdk.MessageRoleTool:
		return convertResponsesToolResults(msg)

	default:
		return nil
	}
}

func convertResponsesUserMessage(msg sdk.Message) []json.RawMessage {
	var parts []responsesUserContentPart
	for _, part := range msg.Content {
		switch p := part.(type) {
		case sdk.TextPart:
			parts = append(parts, responsesUserContentPart{Type: "input_text", Text: p.Text})
		case sdk.ImagePart:
			parts = append(parts, responsesUserContentPart{Type: "input_image", ImageURL: p.Image})
		case sdk.FilePart:
			data, mediaType := utils.NormalizeFileData(p.Data, p.MediaType)
			parts = append(parts, responsesUserContentPart{
				Type:     "input_file",
				Filename: strings.TrimSpace(p.Filename),
				FileData: utils.FileDataURL(data, mediaType),
			})
		}
	}
	return []json.RawMessage{marshalRaw(responsesUserMessage{
		Role:    "user",
		Content: parts,
	})}
}

func convertResponsesAssistantMessage(msg sdk.Message) []json.RawMessage {
	var items []json.RawMessage
	var textParts []responsesOutputTextPart
	var reasoningItems []responsesReasoningItem
	reasoningIndexByID := map[string]int{}

	for _, part := range msg.Content {
		switch p := part.(type) {
		case sdk.TextPart:
			textParts = append(textParts, responsesOutputTextPart{
				Type: "output_text",
				Text: p.Text,
			})

		case sdk.ReasoningPart:
			// Only this dialect's blocks can be replayed; anything else the API
			// cannot verify, and re-sending reasoning as ordinary text teaches
			// the model to imitate it in user-visible answers.
			if p.Format != sdk.ReasoningFormatOpenAIResponses {
				continue
			}
			id := openaiutil.ReasoningItemID(p.ProviderMetadata)
			if id == "" {
				id = p.ID
			}
			idx, ok := reasoningIndexByID[id]
			if !ok || id == "" {
				reasoningItems = append(reasoningItems, responsesReasoningItem{
					Type:             "reasoning",
					ID:               id,
					EncryptedContent: openaiutil.ReasoningEncryptedContent(p.ProviderMetadata),
				})
				idx = len(reasoningItems) - 1
				if id != "" {
					reasoningIndexByID[id] = idx
				}
			}
			if p.Text != "" {
				reasoningItems[idx].Summary = append(reasoningItems[idx].Summary,
					responsesReasoningSummaryText{Type: "summary_text", Text: p.Text})
			}

		case sdk.ToolCallPart:
			id := p.ToolCallID
			if id == "" {
				id = generateID()
			}
			items = appendRaw(items, responsesFunctionCall{
				Type:      "function_call",
				CallID:    id,
				Name:      p.ToolName,
				Arguments: string(p.Input.Object()),
			})
		}
	}

	var prefix []json.RawMessage
	for i := range reasoningItems {
		// Summary is schema-required, so an item that carried only encrypted
		// content still needs the key present.
		if reasoningItems[i].Summary == nil {
			reasoningItems[i].Summary = []responsesReasoningSummaryText{}
		}
		prefix = append(prefix, marshalRaw(reasoningItems[i]))
	}
	if len(textParts) > 0 {
		prefix = append(prefix, marshalRaw(responsesAssistantMessage{
			Role:    "assistant",
			Content: textParts,
		}))
	}
	items = append(prefix, items...)

	return items
}

func convertResponsesToolResults(msg sdk.Message) []json.RawMessage {
	var items []json.RawMessage
	for _, part := range msg.Content {
		if trp, ok := part.(sdk.ToolResultPart); ok {
			items = appendRaw(items, responsesFunctionCallOutput{
				Type:   "function_call_output",
				CallID: trp.ToolCallID,
				Output: trp.Result.String(),
			})
		}
	}
	return items
}

// ---------- parseResponse ----------

func (p *Provider) parseResponse(resp *responsesResponse) (sdk.ModelResult, error) {
	result := sdk.ModelResult{
		Response: sdk.ResponseMetadata{
			ID:        resp.ID,
			ModelID:   resp.Model,
			Timestamp: sdk.TimestampFromUnix(resp.CreatedAt),
		},
	}

	if resp.Usage != nil {
		result.Usage = convertResponsesUsage(resp.Usage)
	}

	hasFunctionCall := false
	var incompleteReason string
	if resp.IncompleteDetails != nil {
		incompleteReason = resp.IncompleteDetails.Reason
	}

	for i := range resp.Output {
		item := &resp.Output[i]
		switch item.Type {
		case outputTypeMessage:
			for _, c := range item.Content {
				if c.Type == "output_text" {
					result.Text += c.Text
				}
				for _, ann := range c.Annotations {
					if ann.Type == "url_citation" {
						result.Sources = append(result.Sources, sdk.Source{
							SourceType: "url",
							ID:         generateID(),
							URL:        ann.URL,
							Title:      ann.Title,
						})
					}
				}
			}

		case outputTypeReasoning:
			// One reasoning item may carry several summary entries; each becomes
			// its own block, all sharing the item's identity so the item can be
			// reassembled on replay. An item with no summary still has to be
			// replayed when it carries encrypted content, so it yields a block
			// with empty text rather than none.
			meta := openaiutil.ReasoningItemMetadata(item.ID, item.EncryptedContent)
			if len(item.Summary) == 0 {
				result.ReasoningParts = append(result.ReasoningParts, sdk.ReasoningPart{
					ID:               item.ID,
					Format:           sdk.ReasoningFormatOpenAIResponses,
					Model:            resp.Model,
					ProviderMetadata: meta,
				})
			}
			for _, s := range item.Summary {
				if s.Type != "summary_text" {
					continue
				}
				result.ReasoningParts = append(result.ReasoningParts, sdk.ReasoningPart{
					ID:               item.ID,
					Text:             s.Text,
					Format:           sdk.ReasoningFormatOpenAIResponses,
					Model:            resp.Model,
					ProviderMetadata: meta,
				})
			}

		case outputTypeFunctionCall:
			hasFunctionCall = true
			input := sdk.ParseToolArguments(item.Arguments)
			callID := item.CallID
			if callID == "" {
				callID = generateID()
			}
			result.ToolCalls = append(result.ToolCalls, sdk.ToolCall{
				ToolCallID: callID,
				ToolName:   item.Name,
				Input:      input,
			})
		}
	}

	result.FinishReason = mapResponsesFinishReason(incompleteReason, hasFunctionCall)
	if incompleteReason != "" {
		result.RawFinishReason = incompleteReason
	}
	result.Reasoning = sdk.ReasoningText(result.ReasoningParts)

	return result, nil
}

// ---------- DoStream ----------

func (p *Provider) DoStream(ctx context.Context, req sdk.Request) (<-chan sdk.StreamPart, error) { //nolint:gocritic,gocyclo // interface method
	if req.Model == "" {
		return nil, fmt.Errorf("openai-responses: model is required")
	}

	wireReq, err := p.buildRequest(&req)
	if err != nil {
		return nil, fmt.Errorf("openai-responses: build request: %w", err)
	}
	wireReq.Stream = true

	ch := make(chan sdk.StreamPart, 64)

	go func() {
		defer close(ch)

		var (
			responseID       string
			responseModel    string
			responseCreated  int64
			usage            sdk.Usage
			incompleteReason string
			hasFunctionCall  bool

			textStartSent     bool
			activeReasoningID string

			// Track ongoing function calls by output_index
			pendingToolCalls = map[int]*streamingToolCall{}
		)

		send := func(part sdk.StreamPart) bool {
			select {
			case ch <- part:
				return true
			case <-ctx.Done():
				return false
			}
		}

		// endReasoning closes the active reasoning block. meta carries the
		// block's final metadata: encrypted_content is populated only on
		// response.output_item.done, so the closing part is the only chance to
		// deliver it. Every other close path passes nil.
		endReasoning := func(meta sdk.ProviderMetadata) {
			if activeReasoningID == "" {
				return
			}
			send(&sdk.ReasoningEndPart{
				ID:               activeReasoningID,
				Format:           sdk.ReasoningFormatOpenAIResponses,
				ProviderMetadata: meta,
			})
			activeReasoningID = ""
		}

		startReasoning := func(id string, meta sdk.ProviderMetadata) {
			if activeReasoningID == id {
				return
			}
			endReasoning(nil)
			send(&sdk.ReasoningStartPart{
				ID:               id,
				Format:           sdk.ReasoningFormatOpenAIResponses,
				ProviderMetadata: meta,
			})
			activeReasoningID = id
		}

		flush := func() {
			endReasoning(nil)
			if textStartSent {
				send(&sdk.TextEndPart{ID: responseID})
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
			Path:    "/responses",
			Headers: p.authHeaders(),
			Prepare: p.prepareRequest,
			Body:    wireReq,
		}, func(ev *utils.SSEEvent) error {
			eventType := ev.Event
			if eventType == "" {
				// Try to infer event type from the data if event field is missing
				var probe struct {
					Type string `json:"type"`
				}
				if json.Unmarshal([]byte(ev.Data), &probe) == nil && probe.Type != "" {
					eventType = probe.Type
				}
			}

			switch eventType {
			case "response.created":
				var chunk responsesCreatedChunk
				if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
					return nil
				}
				responseID = chunk.Response.ID
				responseModel = chunk.Response.Model
				responseCreated = chunk.Response.CreatedAt

			case "response.output_item.added":
				var chunk responsesOutputItemAddedChunk
				if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
					return nil
				}
				switch chunk.Item.Type {
				case outputTypeMessage:
					if !textStartSent {
						send(&sdk.TextStartPart{ID: chunk.Item.ID})
						textStartSent = true
					}
				case outputTypeReasoning:
					startReasoning(chunk.Item.ID, openaiutil.ReasoningItemMetadata(chunk.Item.ID, chunk.Item.EncryptedContent))
				case outputTypeFunctionCall:
					endReasoning(nil)
					if textStartSent {
						send(&sdk.TextEndPart{ID: responseID})
						textStartSent = false
					}
					callID := chunk.Item.CallID
					if callID == "" {
						callID = generateID()
					}
					pendingToolCalls[chunk.OutputIndex] = &streamingToolCall{
						id:   callID,
						name: chunk.Item.Name,
					}
					send(&sdk.ToolInputStartPart{
						ID:       callID,
						ToolName: chunk.Item.Name,
					})
				}

			case "response.output_text.delta":
				var chunk responsesTextDeltaChunk
				if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
					return nil
				}
				endReasoning(nil)
				if !textStartSent {
					send(&sdk.TextStartPart{ID: chunk.ItemID})
					textStartSent = true
				}
				send(&sdk.TextDeltaPart{ID: chunk.ItemID, Text: chunk.Delta})

			case "response.reasoning_text.delta", "response.reasoning_summary_text.delta":
				var chunk responsesReasoningDeltaChunk
				if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
					return nil
				}
				startReasoning(chunk.ItemID, nil)
				send(&sdk.ReasoningDeltaPart{ID: chunk.ItemID, Text: chunk.Delta, Format: sdk.ReasoningFormatOpenAIResponses, Model: responseModel})

			case "response.function_call_arguments.delta":
				var chunk responsesFuncArgsDeltaChunk
				if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
					return nil
				}
				stc := pendingToolCalls[chunk.OutputIndex]
				if stc == nil {
					return nil
				}
				stc.args.WriteString(chunk.Delta)
				send(&sdk.ToolInputDeltaPart{
					ID:    stc.id,
					Delta: chunk.Delta,
				})

			case "response.output_item.done":
				var chunk responsesOutputItemDoneChunk
				if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
					return nil
				}
				switch chunk.Item.Type {
				case outputTypeMessage:
					if textStartSent {
						send(&sdk.TextEndPart{ID: chunk.Item.ID})
						textStartSent = false
					}
				case outputTypeReasoning:
					// The done event is where encrypted_content arrives; the
					// added event fires before it is populated.
					meta := openaiutil.ReasoningItemMetadata(chunk.Item.ID, chunk.Item.EncryptedContent)
					switch {
					case activeReasoningID == chunk.Item.ID:
						endReasoning(meta)
					case chunk.Item.EncryptedContent != "":
						// The block was already closed by an interleaved event.
						// Send another end part for it: the accumulator merges
						// metadata by block ID, so the payload still lands on
						// the right block instead of being lost.
						send(&sdk.ReasoningEndPart{
							ID:               chunk.Item.ID,
							Format:           sdk.ReasoningFormatOpenAIResponses,
							ProviderMetadata: meta,
						})
					}
				case outputTypeFunctionCall:
					hasFunctionCall = true
					stc := pendingToolCalls[chunk.OutputIndex]
					if stc != nil && !stc.finished {
						send(&sdk.ToolInputEndPart{ID: stc.id})
						args := chunk.Item.Arguments
						if args == "" {
							args = stc.args.String()
						}
						send(&sdk.StreamToolCallPart{
							ToolCallID: stc.id,
							ToolName:   stc.name,
							Input:      sdk.ParseToolArguments(args),
						})
						stc.finished = true
					}
				}

			case "response.output_text.annotation.added":
				var chunk responsesAnnotationAddedChunk
				if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
					return nil
				}
				if chunk.Annotation.Type == "url_citation" {
					send(&sdk.StreamSourcePart{
						Source: sdk.Source{
							SourceType: "url",
							ID:         generateID(),
							URL:        chunk.Annotation.URL,
							Title:      chunk.Annotation.Title,
						},
					})
				}

			case "response.completed", "response.incomplete":
				var chunk responsesCompletedChunk
				if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
					return nil
				}
				if chunk.Response.IncompleteDetails != nil {
					incompleteReason = chunk.Response.IncompleteDetails.Reason
				}
				if chunk.Response.Usage != nil {
					usage = convertResponsesUsage(chunk.Response.Usage)
				}

				flush()

				finishReason := mapResponsesFinishReason(incompleteReason, hasFunctionCall)
				send(&sdk.FinishStepPart{
					FinishReason:    finishReason,
					RawFinishReason: incompleteReason,
					Usage:           usage,
					Response: sdk.ResponseMetadata{
						ID:        responseID,
						ModelID:   responseModel,
						Timestamp: sdk.TimestampFromUnix(responseCreated),
					},
				})

				return utils.ErrStreamDone

			case "response.failed":
				var chunk responsesFailedChunk
				if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
					return nil
				}
				if chunk.Response.Usage != nil {
					usage = convertResponsesUsage(chunk.Response.Usage)
				}
				if chunk.Response.Error == nil {
					send(&sdk.ErrorPart{Error: fmt.Errorf("openai-responses: response failed")})
				} else {
					send(&sdk.ErrorPart{Error: fmt.Errorf(
						"openai-responses: %s: %s",
						chunk.Response.Error.Code,
						chunk.Response.Error.Message,
					)})
				}
				return utils.ErrStreamDone

			case "error":
				var chunk responsesErrorChunk
				if err := json.Unmarshal([]byte(ev.Data), &chunk); err != nil {
					return nil
				}
				send(&sdk.ErrorPart{Error: fmt.Errorf("openai-responses: %s: %s", chunk.Error.Code, chunk.Error.Message)})
				return utils.ErrStreamDone
			}

			return nil
		})

		if err != nil {
			var apiErr *utils.APIError
			if errors.As(err, &apiErr) {
				send(&sdk.ErrorPart{Error: fmt.Errorf("openai-responses: stream failed: %s", apiErr.Detail())})
			} else {
				send(&sdk.ErrorPart{Error: fmt.Errorf("openai-responses: stream failed: %w", err)})
			}
		}

		flush()

		finishReason := mapResponsesFinishReason(incompleteReason, hasFunctionCall)
		send(&sdk.FinishPart{
			FinishReason:    finishReason,
			RawFinishReason: incompleteReason,
			TotalUsage:      usage,
		})
	}()

	return ch, nil
}

// ---------- helpers ----------

func mapResponsesFinishReason(incompleteReason string, hasFunctionCall bool) sdk.FinishReason {
	switch incompleteReason {
	case "max_output_tokens":
		return sdk.FinishReasonLength
	case "content_filter":
		return sdk.FinishReasonContentFilter
	case "":
		if hasFunctionCall {
			return sdk.FinishReasonToolCalls
		}
		return sdk.FinishReasonStop
	default:
		if hasFunctionCall {
			return sdk.FinishReasonToolCalls
		}
		return sdk.FinishReasonOther
	}
}

func convertResponsesUsage(u *responsesUsage) sdk.Usage {
	inputTokens := u.InputTokens
	outputTokens := u.OutputTokens
	cachedTokens := 0
	reasoningTokens := 0

	if u.InputTokensDetails != nil {
		cachedTokens = u.InputTokensDetails.CachedTokens
	}
	if u.OutputTokensDetails != nil {
		reasoningTokens = u.OutputTokensDetails.ReasoningTokens
	}

	return sdk.Usage{
		InputTokens:       inputTokens,
		OutputTokens:      outputTokens,
		TotalTokens:       inputTokens + outputTokens,
		ReasoningTokens:   reasoningTokens,
		CachedInputTokens: cachedTokens,
		InputTokenDetails: sdk.InputTokenDetail{
			CacheReadTokens: cachedTokens,
			NoCacheTokens:   inputTokens - cachedTokens,
		},
		OutputTokenDetails: sdk.OutputTokenDetail{
			ReasoningTokens: reasoningTokens,
			TextTokens:      outputTokens - reasoningTokens,
		},
	}
}

func textFromParts(parts []sdk.MessagePart) string {
	var text string
	for _, p := range parts {
		if tp, ok := p.(sdk.TextPart); ok {
			text += tp.Text
		}
	}
	return text
}

func marshalRaw(v any) json.RawMessage {
	data, _ := json.Marshal(v)
	return data
}

func appendRaw(items []json.RawMessage, v any) []json.RawMessage {
	return append(items, marshalRaw(v))
}
