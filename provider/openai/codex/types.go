package codex

import "encoding/json"

type codexRequest struct {
	Model        string            `json:"model"`
	Instructions string            `json:"instructions"`
	Input        []json.RawMessage `json:"input"`
	Tools        []codexTool       `json:"tools,omitempty"`
	ToolChoice   any               `json:"tool_choice,omitempty"`
	Text         *codexTextFmt     `json:"text,omitempty"`
	Reasoning    *codexReasoning   `json:"reasoning,omitempty"`
	Include      []string          `json:"include,omitempty"`
	Store        bool              `json:"store"`
	Stream       bool              `json:"stream,omitempty"`
}

type codexTool struct {
	Type        string `json:"type"`
	Name        string `json:"name,omitempty"`
	Description string `json:"description,omitempty"`
	Parameters  any    `json:"parameters,omitempty"`
}

type codexTextFmt struct {
	Format *codexTextFormat `json:"format,omitempty"`
}

type codexTextFormat struct {
	Type   string `json:"type"`
	Name   string `json:"name,omitempty"`
	Schema any    `json:"schema,omitempty"`
}

type codexReasoning struct {
	Effort string `json:"effort,omitempty"`
}

type codexUserContentPart struct {
	Type     string `json:"type"`
	Text     string `json:"text,omitempty"`
	ImageURL string `json:"image_url,omitempty"`
}

type codexUserMessage struct {
	Role    string                 `json:"role"`
	Content []codexUserContentPart `json:"content"`
}

type codexOutputTextPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type codexAssistantMessage struct {
	Role    string                `json:"role"`
	Content []codexOutputTextPart `json:"content"`
}

type codexFunctionCall struct {
	Type      string `json:"type"`
	CallID    string `json:"call_id"`
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type codexFunctionCallOutput struct {
	Type   string `json:"type"`
	CallID string `json:"call_id"`
	Output string `json:"output"`
}

type codexReasoningSummaryText struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type codexReasoningItem struct {
	Type string `json:"type"`
	// ID is schema-required by the API and must be the id the item was issued
	// under.
	ID               string                      `json:"id,omitempty"`
	Summary          []codexReasoningSummaryText `json:"summary"`
	EncryptedContent string                      `json:"encrypted_content,omitempty"`
}

type codexCreatedChunk struct {
	Response struct {
		ID        string `json:"id"`
		CreatedAt int64  `json:"created_at"`
		Model     string `json:"model"`
	} `json:"response"`
}

type codexOutputItemAddedChunk struct {
	OutputIndex int `json:"output_index"`
	Item        struct {
		Type             string `json:"type"`
		ID               string `json:"id"`
		CallID           string `json:"call_id,omitempty"`
		Name             string `json:"name,omitempty"`
		EncryptedContent string `json:"encrypted_content,omitempty"`
	} `json:"item"`
}

type codexOutputItemDoneChunk struct {
	OutputIndex int `json:"output_index"`
	Item        struct {
		Type      string `json:"type"`
		ID        string `json:"id"`
		CallID    string `json:"call_id,omitempty"`
		Name      string `json:"name,omitempty"`
		Arguments string `json:"arguments,omitempty"`
		// reasoning fields. encrypted_content is populated only on this event,
		// not on output_item.added, where the item has just been created.
		EncryptedContent string `json:"encrypted_content,omitempty"`
	} `json:"item"`
}

type codexTextDeltaChunk struct {
	ItemID string `json:"item_id"`
	Delta  string `json:"delta"`
}

type codexFuncArgsDeltaChunk struct {
	OutputIndex int    `json:"output_index"`
	Delta       string `json:"delta"`
}

type codexReasoningSummaryDeltaChunk struct {
	ItemID string `json:"item_id"`
	Delta  string `json:"delta"`
}

type codexCompletedChunk struct {
	Response struct {
		IncompleteDetails *codexIncompleteDetails `json:"incomplete_details,omitempty"`
		Usage             *codexUsage             `json:"usage,omitempty"`
	} `json:"response"`
}

type codexIncompleteDetails struct {
	Reason string `json:"reason"`
}

type codexUsage struct {
	InputTokens         int                      `json:"input_tokens"`
	OutputTokens        int                      `json:"output_tokens"`
	InputTokensDetails  *codexInputTokenDetails  `json:"input_tokens_details,omitempty"`
	OutputTokensDetails *codexOutputTokenDetails `json:"output_tokens_details,omitempty"`
}

type codexInputTokenDetails struct {
	CachedTokens *int `json:"cached_tokens,omitempty"`
}

type codexOutputTokenDetails struct {
	ReasoningTokens int `json:"reasoning_tokens"`
}
