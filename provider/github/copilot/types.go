package copilot

// --- Request types ---

type chatRequest struct {
	Model            string             `json:"model,omitempty"`
	Messages         []chatMessage      `json:"messages"`
	Tools            []chatTool         `json:"tools,omitempty"`
	ToolChoice       any                `json:"tool_choice,omitempty"`
	ResponseFormat   *chatRespFormat    `json:"response_format,omitempty"`
	Temperature      *float64           `json:"temperature,omitempty"`
	TopP             *float64           `json:"top_p,omitempty"`
	MaxTokens        *int               `json:"max_tokens,omitempty"`
	Stop             []string           `json:"stop,omitempty"`
	FrequencyPenalty *float64           `json:"frequency_penalty,omitempty"`
	PresencePenalty  *float64           `json:"presence_penalty,omitempty"`
	Seed             *int               `json:"seed,omitempty"`
	ReasoningEffort  *string            `json:"reasoning_effort,omitempty"`
	Stream           bool               `json:"stream,omitempty"`
	StreamOptions    *chatStreamOptions `json:"stream_options,omitempty"`
}

type chatStreamOptions struct {
	IncludeUsage bool `json:"include_usage"`
}

type chatRespFormat struct {
	Type       string `json:"type"`
	JSONSchema any    `json:"json_schema,omitempty"`
}

type chatTool struct {
	Type     string       `json:"type"`
	Function chatFunction `json:"function"`
}

// chatToolChoiceFunction is the OpenAI wire form of a named tool choice,
// {"type":"function","function":{"name":...}}.
type chatToolChoiceFunction struct {
	Type     string             `json:"type"`
	Function chatToolChoiceName `json:"function"`
}

type chatToolChoiceName struct {
	Name string `json:"name"`
}

type chatFunction struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
	// ReasoningText and ReasoningOpaque are Copilot's own reasoning fields. The
	// opaque token wraps the upstream model's signature; the text is only
	// accepted alongside it.
	ReasoningText   string         `json:"reasoning_text,omitempty"`
	ReasoningOpaque string         `json:"reasoning_opaque,omitempty"`
	ToolCalls       []chatToolCall `json:"tool_calls,omitempty"`
	ToolCallID      string         `json:"tool_call_id,omitempty"`
}

type chatContentPartText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type chatContentPartImage struct {
	Type     string       `json:"type"`
	ImageURL chatImageURL `json:"image_url"`
}

type chatImageURL struct {
	URL    string `json:"url"`
	Detail string `json:"detail,omitempty"`
}

// --- Response types ---

type chatResponse struct {
	ID      string       `json:"id"`
	Object  string       `json:"object"`
	Created int64        `json:"created"`
	Model   string       `json:"model"`
	Choices []chatChoice `json:"choices"`
	Usage   chatUsage    `json:"usage"`
}

type chatChoice struct {
	Index        int             `json:"index"`
	Message      chatRespMessage `json:"message"`
	FinishReason string          `json:"finish_reason"`
}

type chatRespMessage struct {
	Role            string `json:"role"`
	Content         string `json:"content"`
	ReasoningText   string `json:"reasoning_text,omitempty"`
	ReasoningOpaque string `json:"reasoning_opaque,omitempty"`
	// ReasoningContent and Reasoning are other vendors' field names, accepted
	// because Copilot proxies models that emit them.
	ReasoningContent string          `json:"reasoning_content,omitempty"`
	Reasoning        string          `json:"reasoning,omitempty"`
	Refusal          string          `json:"refusal,omitempty"`
	ToolCalls        []chatToolCall  `json:"tool_calls,omitempty"`
	Images           []chatImagePart `json:"images,omitempty"`
}

type chatToolCall struct {
	ID       string           `json:"id"`
	Type     string           `json:"type"`
	Function chatFunctionCall `json:"function"`
}

type chatFunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type chatUsage struct {
	PromptTokens            int                         `json:"prompt_tokens"`
	CompletionTokens        int                         `json:"completion_tokens"`
	TotalTokens             int                         `json:"total_tokens"`
	PromptTokensDetails     *chatPromptTokenDetails     `json:"prompt_tokens_details,omitempty"`
	CompletionTokensDetails *chatCompletionTokenDetails `json:"completion_tokens_details,omitempty"`
}

type chatPromptTokenDetails struct {
	CachedTokens *int `json:"cached_tokens,omitempty"`
}

type chatCompletionTokenDetails struct {
	ReasoningTokens int `json:"reasoning_tokens"`
	TextTokens      int `json:"text_tokens"`
}

// --- Streaming chunk types (chat.completion.chunk) ---

type chatChunkResponse struct {
	ID      string            `json:"id"`
	Object  string            `json:"object"`
	Created int64             `json:"created"`
	Model   string            `json:"model"`
	Choices []chatChunkChoice `json:"choices"`
	Usage   *chatUsage        `json:"usage,omitempty"`
}

type chatChunkChoice struct {
	Index        int            `json:"index"`
	Delta        chatChunkDelta `json:"delta"`
	FinishReason *string        `json:"finish_reason"`
}

type chatChunkDelta struct {
	Role            string `json:"role,omitempty"`
	Content         string `json:"content,omitempty"`
	ReasoningText   string `json:"reasoning_text,omitempty"`
	ReasoningOpaque string `json:"reasoning_opaque,omitempty"`
	// ReasoningContent and Reasoning are other vendors' field names, accepted
	// because Copilot proxies models that emit them.
	ReasoningContent string              `json:"reasoning_content,omitempty"`
	Reasoning        string              `json:"reasoning,omitempty"`
	Refusal          string              `json:"refusal,omitempty"`
	ToolCalls        []chatToolCallChunk `json:"tool_calls,omitempty"`
	Images           []chatImagePart     `json:"images,omitempty"`
}

type chatImagePart struct {
	Type     string       `json:"type"`
	ImageURL chatImageURL `json:"image_url"`
}

type chatToolCallChunk struct {
	Index    int               `json:"index"`
	ID       string            `json:"id,omitempty"`
	Type     string            `json:"type,omitempty"`
	Function chatFunctionDelta `json:"function,omitempty"`
}

type chatFunctionDelta struct {
	Name      string `json:"name,omitempty"`
	Arguments string `json:"arguments,omitempty"`
}
