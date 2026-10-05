package messages

import "encoding/json"

// --- Request types ---

type messagesRequest struct {
	Model         string                 `json:"model"`
	MaxTokens     *int                   `json:"max_tokens,omitempty"`
	System        []contentBlock         `json:"system,omitempty"`
	Messages      []anthropicMessage     `json:"messages"`
	Tools         []anthropicTool        `json:"tools,omitempty"`
	ToolChoice    *anthropicToolChoice   `json:"tool_choice,omitempty"`
	Temperature   *float64               `json:"temperature,omitempty"`
	TopP          *float64               `json:"top_p,omitempty"`
	TopK          *int                   `json:"top_k,omitempty"`
	StopSequences []string               `json:"stop_sequences,omitempty"`
	Stream        bool                   `json:"stream,omitempty"`
	Thinking      *anthropicThinking     `json:"thinking,omitempty"`
	OutputConfig  *anthropicOutputConfig `json:"output_config,omitempty"`
}

type anthropicThinking struct {
	Type         string `json:"type"`
	BudgetTokens int    `json:"budget_tokens,omitempty"`
}

// anthropicOutputConfig maps to Anthropic's output_config object. Effort is the
// reasoning-effort tier ("low"/"medium"/"high"/"xhigh"/"max"); it controls total
// token output and supersedes thinking.budget_tokens on modern Claude models.
type anthropicOutputConfig struct {
	Effort string `json:"effort,omitempty"`
}

type anthropicMessage struct {
	Role    string         `json:"role"`
	Content []contentBlock `json:"content"`
}

// cacheControl maps to Anthropic's cache_control object.
// Type is always "ephemeral". TTL is optional: "" means 5-minute cache
// (Anthropic default), "1h" means 1-hour cache (billed at a higher rate).
type cacheControl struct {
	Type string `json:"type"`
	TTL  string `json:"ttl,omitempty"`
}

type contentBlock struct {
	Type string `json:"type"`

	// text block
	Text string `json:"text,omitempty"`

	// image / document block
	Source *imageSource `json:"source,omitempty"`
	Title  string       `json:"title,omitempty"`

	// thinking block. Thinking is a pointer because the empty string is a
	// legal value that must reach the wire: a summarized thinking block
	// carries a signature over empty text, and the API rejects the block as
	// invalid when the thinking key is missing entirely.
	Thinking  *string `json:"thinking,omitempty"`
	Signature string  `json:"signature,omitempty"`

	// redacted_thinking block: an encrypted payload replayed verbatim.
	Data string `json:"data,omitempty"`

	// tool_use block
	ID    string `json:"id,omitempty"`
	Name  string `json:"name,omitempty"`
	Input any    `json:"input,omitempty"`

	// tool_result block
	ToolUseID string `json:"tool_use_id,omitempty"`
	Content   any    `json:"content,omitempty"`
	IsError   bool   `json:"is_error,omitempty"`

	// prompt caching
	CacheControl *cacheControl `json:"cache_control,omitempty"`
}

type imageSource struct {
	Type      string `json:"type"`
	MediaType string `json:"media_type,omitempty"`
	Data      string `json:"data,omitempty"`
	URL       string `json:"url,omitempty"`
}

type anthropicTool struct {
	Name         string        `json:"name"`
	Description  string        `json:"description,omitempty"`
	InputSchema  any           `json:"input_schema"`
	CacheControl *cacheControl `json:"cache_control,omitempty"`
}

type anthropicToolChoice struct {
	Type                   string `json:"type"`
	Name                   string `json:"name,omitempty"`
	DisableParallelToolUse *bool  `json:"disable_parallel_tool_use,omitempty"`
}

// --- Response types ---

type messagesResponse struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	Model        string          `json:"model"`
	Role         string          `json:"role"`
	Content      []responseBlock `json:"content"`
	StopReason   string          `json:"stop_reason"`
	StopSequence string          `json:"stop_sequence"`
	Usage        messagesUsage   `json:"usage"`
}

type responseBlock struct {
	Type string `json:"type"`

	// text
	Text string `json:"text,omitempty"`

	// thinking
	Thinking  string `json:"thinking,omitempty"`
	Signature string `json:"signature,omitempty"`

	// redacted_thinking
	Data string `json:"data,omitempty"`

	// tool_use
	ID    string          `json:"id,omitempty"`
	Name  string          `json:"name,omitempty"`
	Input json.RawMessage `json:"input,omitempty"`
}

// cacheCreationDetail holds the per-TTL breakdown of cache-write tokens.
// Populated by Anthropic when at least one cache_control block is present.
type cacheCreationDetail struct {
	Ephemeral5mInputTokens int `json:"ephemeral_5m_input_tokens,omitempty"`
	Ephemeral1hInputTokens int `json:"ephemeral_1h_input_tokens,omitempty"`
}

type messagesUsage struct {
	InputTokens              int                  `json:"input_tokens"`
	OutputTokens             int                  `json:"output_tokens"`
	CacheCreationInputTokens int                  `json:"cache_creation_input_tokens,omitempty"`
	CacheReadInputTokens     *int                 `json:"cache_read_input_tokens,omitempty"`
	CacheCreation            *cacheCreationDetail `json:"cache_creation,omitempty"`
}

type messagesUsageDelta struct {
	InputTokens              *int `json:"input_tokens"`
	OutputTokens             *int `json:"output_tokens"`
	CacheCreationInputTokens *int `json:"cache_creation_input_tokens"`
	CacheReadInputTokens     *int `json:"cache_read_input_tokens"`
	CacheCreation            *struct {
		Ephemeral5mInputTokens *int `json:"ephemeral_5m_input_tokens"`
		Ephemeral1hInputTokens *int `json:"ephemeral_1h_input_tokens"`
	} `json:"cache_creation"`
}

func (u *messagesUsage) applyDelta(delta *messagesUsageDelta) {
	if delta.InputTokens != nil {
		u.InputTokens = *delta.InputTokens
	}
	if delta.OutputTokens != nil {
		u.OutputTokens = *delta.OutputTokens
	}
	if delta.CacheReadInputTokens != nil {
		u.CacheReadInputTokens = delta.CacheReadInputTokens
	}
	if delta.CacheCreationInputTokens != nil {
		if *delta.CacheCreationInputTokens == 0 || (*delta.CacheCreationInputTokens != u.CacheCreationInputTokens && delta.CacheCreation == nil) {
			u.CacheCreation = nil
		}
		u.CacheCreationInputTokens = *delta.CacheCreationInputTokens
	}
	if delta.CacheCreation != nil {
		if u.CacheCreation == nil {
			u.CacheCreation = &cacheCreationDetail{}
		}
		if delta.CacheCreation.Ephemeral5mInputTokens != nil {
			u.CacheCreation.Ephemeral5mInputTokens = *delta.CacheCreation.Ephemeral5mInputTokens
		}
		if delta.CacheCreation.Ephemeral1hInputTokens != nil {
			u.CacheCreation.Ephemeral1hInputTokens = *delta.CacheCreation.Ephemeral1hInputTokens
		}
		if u.CacheCreation.Ephemeral5mInputTokens+u.CacheCreation.Ephemeral1hInputTokens > u.CacheCreationInputTokens {
			u.CacheCreation = nil
		}
	}
}

// --- Streaming event types ---

// streamEvent is the top-level SSE data payload for all Anthropic stream events.
type streamEvent struct {
	Type string `json:"type"`

	// message_start
	Message *messagesResponse `json:"message,omitempty"`

	// content_block_start
	Index        *int           `json:"index,omitempty"`
	ContentBlock *responseBlock `json:"content_block,omitempty"`

	// content_block_delta
	Delta *streamDelta `json:"delta,omitempty"`

	// message_delta
	Usage *messagesUsageDelta `json:"usage,omitempty"`
}

type streamDelta struct {
	Type string `json:"type"`

	// text_delta
	Text string `json:"text,omitempty"`

	// thinking_delta
	Thinking string `json:"thinking,omitempty"`

	// signature_delta
	Signature string `json:"signature,omitempty"`

	// input_json_delta
	PartialJSON string `json:"partial_json,omitempty"`

	// message_delta fields
	StopReason   string `json:"stop_reason,omitempty"`
	StopSequence string `json:"stop_sequence,omitempty"`
}

// --- Models API response types ---

type modelsListResponse struct {
	Data    []anthropicModelObject `json:"data"`
	HasMore bool                   `json:"has_more"`
}

type anthropicModelObject struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	DisplayName string `json:"display_name"`
	CreatedAt   string `json:"created_at"`
}
