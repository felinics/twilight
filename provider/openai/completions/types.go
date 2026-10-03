package completions

// --- Request types ---

type chatRequest struct {
	Model               string             `json:"model"`
	Messages            []chatMessage      `json:"messages"`
	Tools               []chatTool         `json:"tools,omitempty"`
	ToolChoice          any                `json:"tool_choice,omitempty"`
	ResponseFormat      *chatRespFormat    `json:"response_format,omitempty"`
	Temperature         *float64           `json:"temperature,omitempty"`
	TopP                *float64           `json:"top_p,omitempty"`
	MaxCompletionTokens *int               `json:"max_completion_tokens,omitempty"`
	Stop                []string           `json:"stop,omitempty"`
	FrequencyPenalty    *float64           `json:"frequency_penalty,omitempty"`
	PresencePenalty     *float64           `json:"presence_penalty,omitempty"`
	Seed                *int               `json:"seed,omitempty"`
	ReasoningEffort     *string            `json:"reasoning_effort,omitempty"`
	PromptCacheKey      *string            `json:"prompt_cache_key,omitempty"`
	Thinking            *chatThinking      `json:"thinking,omitempty"`
	ReasoningSplit      bool               `json:"reasoning_split,omitempty"`
	Stream              bool               `json:"stream,omitempty"`
	StreamOptions       *chatStreamOptions `json:"stream_options,omitempty"`
}

type chatThinking struct {
	Type string `json:"type"`
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

type chatFunction struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters,omitempty"`
}

type chatMessage struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
	// ReasoningContent is a pointer so an empty reasoning block can be sent as
	// "" instead of being dropped: DeepSeek and Kimi thinking modes validate
	// the key's presence on replayed tool-call messages, not its length.
	ReasoningContent *string               `json:"reasoning_content,omitempty"`
	ReasoningDetails []chatReasoningDetail `json:"reasoning_details,omitempty"`
	ToolCalls        []chatToolCall        `json:"tool_calls,omitempty"`
	ToolCallID       string                `json:"tool_call_id,omitempty"`
}

type chatReasoningDetail map[string]any

type chatContentPartText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type chatContentPartImage struct {
	Type     string       `json:"type"`
	ImageURL chatImageURL `json:"image_url"`
}

type chatContentPartFile struct {
	Type string   `json:"type"`
	File chatFile `json:"file"`
}

type chatFile struct {
	Filename string `json:"filename,omitempty"`
	FileData string `json:"file_data"`
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
	Role    string `json:"role"`
	Content string `json:"content"`
	// ReasoningContent is a pointer because the key's presence is itself a
	// signal: DeepSeek returns "" for a thinking-mode step that produced no
	// reasoning and omits the key when thinking is disabled.
	ReasoningContent *string               `json:"reasoning_content,omitempty"`
	ReasoningDetails []chatReasoningDetail `json:"reasoning_details,omitempty"`
	Reasoning        string                `json:"reasoning,omitempty"`
	Refusal          string                `json:"refusal,omitempty"`
	ToolCalls        []chatToolCall        `json:"tool_calls,omitempty"`
	Images           []chatImagePart       `json:"images,omitempty"`
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
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
	// ReasoningContent is a pointer for the same reason as in chatRespMessage:
	// DeepSeek's first thinking-mode delta carries "" and later deltas null.
	ReasoningContent *string               `json:"reasoning_content,omitempty"`
	ReasoningDetails []chatReasoningDetail `json:"reasoning_details,omitempty"`
	Reasoning        string                `json:"reasoning,omitempty"`
	Refusal          string                `json:"refusal,omitempty"`
	ToolCalls        []chatToolCallChunk   `json:"tool_calls,omitempty"`
	Images           []chatImagePart       `json:"images,omitempty"`
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
