package responses

import "encoding/json"

// --- Request types ---

type responsesRequest struct {
	Model           string              `json:"model"`
	Instructions    string              `json:"instructions,omitempty"`
	Input           []json.RawMessage   `json:"input"`
	Temperature     *float64            `json:"temperature,omitempty"`
	TopP            *float64            `json:"top_p,omitempty"`
	MaxOutputTokens *int                `json:"max_output_tokens,omitempty"`
	PromptCacheKey  *string             `json:"prompt_cache_key,omitempty"`
	Tools           []responsesTool     `json:"tools,omitempty"`
	ToolChoice      any                 `json:"tool_choice,omitempty"`
	Text            *responsesTextFmt   `json:"text,omitempty"`
	Reasoning       *responsesReasoning `json:"reasoning,omitempty"`
	Include         []string            `json:"include,omitempty"`
	Store           *bool               `json:"store,omitempty"`
	Stream          bool                `json:"stream,omitempty"`
}

type responsesTool struct {
	Type        string `json:"type"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters,omitempty"`
}

type responsesTextFmt struct {
	Format *responsesTextFormat `json:"format,omitempty"`
}

type responsesTextFormat struct {
	Type   string `json:"type"`
	Name   string `json:"name,omitempty"`
	Schema any    `json:"schema,omitempty"`
}

type responsesReasoning struct {
	Effort  string `json:"effort,omitempty"`
	Summary string `json:"summary,omitempty"`
}

// --- Input item types ---

type responsesSystemMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type responsesUserContentPart struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	ImageURL string `json:"image_url,omitempty"`
	Filename string `json:"filename,omitempty"`
	FileData string `json:"file_data,omitempty"`
}

type responsesUserMessage struct {
	Role    string                     `json:"role"`
	Content []responsesUserContentPart `json:"content"`
}

type responsesOutputTextPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type responsesAssistantMessage struct {
	Role    string                    `json:"role"`
	Content []responsesOutputTextPart `json:"content"`
}

type responsesFunctionCall struct {
	Type      string `json:"type"`
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type responsesFunctionCallOutput struct {
	Type   string `json:"type"`
	CallID string `json:"call_id"`
	Output string `json:"output"`
}

type responsesReasoningSummaryText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type responsesReasoningItem struct {
	Type string `json:"type"`
	// ID is schema-required by the API and must be the id the item was issued
	// under.
	ID               string                          `json:"id,omitempty"`
	Summary          []responsesReasoningSummaryText `json:"summary"`
	EncryptedContent string                          `json:"encrypted_content,omitempty"`
}

// --- Response types ---

type responsesResponse struct {
	ID                string                `json:"id"`
	CreatedAt         int64                 `json:"created_at"`
	Model             string                `json:"model"`
	Output            []responsesOutputItem `json:"output"`
	Usage             *responsesUsage       `json:"usage,omitempty"`
	IncompleteDetails *incompleteDetails    `json:"incomplete_details,omitempty"`
	Error             *responsesError       `json:"error,omitempty"`
}

type responsesError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

type incompleteDetails struct {
	Reason string `json:"reason"`
}

type responsesOutputItem struct {
	Type string `json:"type"`

	// type: "message"
	ID      string                   `json:"id,omitempty"`
	Role    string                   `json:"role,omitempty"`
	Content []responsesOutputContent `json:"content,omitempty"`

	// type: "reasoning"
	Summary          []responsesReasoningSummaryText `json:"summary,omitempty"`
	EncryptedContent string                          `json:"encrypted_content,omitempty"`

	// type: "function_call"
	CallID    string `json:"call_id,omitempty"`
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}

type responsesOutputContent struct {
	Type        string                `json:"type"`
	Text        string                `json:"text"`
	Annotations []responsesAnnotation `json:"annotations,omitempty"`
}

type responsesAnnotation struct {
	Type       string `json:"type"`
	URL        string `json:"url,omitempty"`
	Title      string `json:"title,omitempty"`
	StartIndex int    `json:"start_index,omitempty"`
	EndIndex   int    `json:"end_index,omitempty"`
}

type responsesUsage struct {
	InputTokens         int                          `json:"input_tokens"`
	OutputTokens        int                          `json:"output_tokens"`
	InputTokensDetails  *responsesInputTokenDetails  `json:"input_tokens_details,omitempty"`
	OutputTokensDetails *responsesOutputTokenDetails `json:"output_tokens_details,omitempty"`
}

type responsesInputTokenDetails struct {
	CachedTokens int `json:"cached_tokens"`
}

type responsesOutputTokenDetails struct {
	ReasoningTokens int `json:"reasoning_tokens"`
}

// --- Streaming chunk types ---
// The Responses API uses typed SSE events (the event: field) with JSON data.

// responsesCreatedChunk is sent for event: response.created
type responsesCreatedChunk struct {
	Type     string `json:"type"`
	Response struct {
		ID        string `json:"id"`
		CreatedAt int64  `json:"created_at"`
		Model     string `json:"model"`
	} `json:"response"`
}

// responsesOutputItemAddedChunk is sent for event: response.output_item.added
type responsesOutputItemAddedChunk struct {
	Type        string `json:"type"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		Type string `json:"type"`
		ID   string `json:"id"`
		// function_call fields
		CallID string `json:"call_id,omitempty"`
		Name   string `json:"name,omitempty"`
		// reasoning fields
		EncryptedContent string `json:"encrypted_content,omitempty"`
	} `json:"item"`
}

// responsesOutputItemDoneChunk is sent for event: response.output_item.done
type responsesOutputItemDoneChunk struct {
	Type        string `json:"type"`
	OutputIndex int    `json:"output_index"`
	Item        struct {
		Type string `json:"type"`
		ID   string `json:"id"`
		// function_call fields
		CallID    string `json:"call_id,omitempty"`
		Name      string `json:"name,omitempty"`
		Arguments string `json:"arguments,omitempty"`
		// reasoning fields. encrypted_content is populated only on this event,
		// not on output_item.added, where the item has just been created.
		EncryptedContent string `json:"encrypted_content,omitempty"`
	} `json:"item"`
}

// responsesTextDeltaChunk is sent for event: response.output_text.delta
type responsesTextDeltaChunk struct {
	Type   string `json:"type"`
	ItemID string `json:"item_id"`
	Delta  string `json:"delta"`
}

// responsesFuncArgsDeltaChunk is sent for event: response.function_call_arguments.delta
type responsesFuncArgsDeltaChunk struct {
	Type        string `json:"type"`
	ItemID      string `json:"item_id"`
	OutputIndex int    `json:"output_index"`
	Delta       string `json:"delta"`
}

// responsesReasoningDeltaChunk is sent for reasoning delta events.
type responsesReasoningDeltaChunk struct {
	Type   string `json:"type"`
	ItemID string `json:"item_id"`
	Delta  string `json:"delta"`
}

// responsesAnnotationAddedChunk is sent for event: response.output_text.annotation.added
type responsesAnnotationAddedChunk struct {
	Type       string              `json:"type"`
	Annotation responsesAnnotation `json:"annotation"`
}

// responsesCompletedChunk is sent for event: response.completed / response.incomplete
type responsesCompletedChunk struct {
	Type     string `json:"type"`
	Response struct {
		IncompleteDetails *incompleteDetails `json:"incomplete_details,omitempty"`
		Usage             *responsesUsage    `json:"usage,omitempty"`
	} `json:"response"`
}

// responsesFailedChunk is sent for event: response.failed. Its error object is
// decoded by errorformat.DecodeOpenAIFailedEvent.
type responsesFailedChunk struct {
	Type     string `json:"type"`
	Response struct {
		Usage *responsesUsage `json:"usage,omitempty"`
	} `json:"response"`
}

// --- Models API response types ---

type modelsListResponse struct {
	Data []modelObject `json:"data"`
}

type modelObject struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}
