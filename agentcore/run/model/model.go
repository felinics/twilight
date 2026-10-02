package model

import (
	"github.com/felinics/twilight/agentcore/jsonstable"
)

// ProviderMetadata is the agent's persisted copy of provider-owned opaque
// tokens: namespace, then token name, then the token as a string. It has the
// shape of sdk.ProviderMetadata; runtime events and state own their copy.
type ProviderMetadata map[string]map[string]string

type CacheControl struct {
	Type string `json:"type"`
	TTL  string `json:"ttl,omitempty"`
}

type MessageRole string

const (
	MessageRoleUser      MessageRole = "user"
	MessageRoleAssistant MessageRole = "assistant"
	MessageRoleSystem    MessageRole = "system"
	MessageRoleTool      MessageRole = "tool"
	MessageRoleDeveloper MessageRole = "developer"
)

type MessagePartType string

const (
	MessagePartTypeText       MessagePartType = "text"
	MessagePartTypeReasoning  MessagePartType = "reasoning"
	MessagePartTypeImage      MessagePartType = "image"
	MessagePartTypeFile       MessagePartType = "file"
	MessagePartTypeToolCall   MessagePartType = "tool-call"
	MessagePartTypeToolResult MessagePartType = "tool-result"
)

type ReasoningFormat string

const (
	ReasoningFormatUnknown         ReasoningFormat = ""
	ReasoningFormatAnthropic       ReasoningFormat = "anthropic-v1"
	ReasoningFormatOpenAIResponses ReasoningFormat = "openai-responses-v1"
	ReasoningFormatGoogle          ReasoningFormat = "google-v1"
	ReasoningFormatCopilot         ReasoningFormat = "copilot-v1"
	ReasoningFormatOpenAIChat      ReasoningFormat = "openai-chat-v1"
)

// MessagePart is a closed, JSON-stable persisted content block. SDK message
// parts are interface values; the Runtime never stores that open interface.
type MessagePart struct {
	Type MessagePartType `json:"type"`

	// Text / reasoning.
	Text string `json:"text,omitempty"`

	// Reasoning-only identity/provenance.
	ID     string          `json:"id,omitempty"`
	Format ReasoningFormat `json:"format,omitempty"`
	Model  string          `json:"model,omitempty"`

	// Image / file.
	Image     string `json:"image,omitempty"`
	Data      string `json:"data,omitempty"`
	MediaType string `json:"mediaType,omitempty"`
	Filename  string `json:"filename,omitempty"`

	// Tool call / result.
	ToolCallID string        `json:"toolCallId,omitempty"`
	ToolName   string        `json:"toolName,omitempty"`
	Input      ToolArguments `json:"input,omitzero"`
	Result     ToolOutput    `json:"result,omitzero"`
	IsError    bool          `json:"isError,omitempty"`

	CacheControl     *CacheControl    `json:"cacheControl,omitempty"`
	ProviderMetadata ProviderMetadata `json:"providerMetadata,omitempty"`
}

type Message struct {
	Role    MessageRole   `json:"role"`
	Content []MessagePart `json:"content"`
	Usage   *Usage        `json:"usage,omitempty"`
}

type ResponseFormatType string

const (
	ResponseFormatText       ResponseFormatType = "text"
	ResponseFormatJSONObject ResponseFormatType = "json_object"
	ResponseFormatJSONSchema ResponseFormatType = "json_schema"
)

type ResponseFormat struct {
	Type       ResponseFormatType `json:"type"`
	JSONSchema jsonstable.Value   `json:"jsonSchema,omitzero"`
}

type ToolChoiceMode string

const (
	ToolChoiceAuto     ToolChoiceMode = "auto"
	ToolChoiceNone     ToolChoiceMode = "none"
	ToolChoiceRequired ToolChoiceMode = "required"
	ToolChoiceTool     ToolChoiceMode = "tool"
)

type ToolChoice struct {
	Mode ToolChoiceMode `json:"mode,omitempty"`
	Tool string         `json:"tool,omitempty"`
}

type ToolDefinition struct {
	Name         string           `json:"name"`
	Description  string           `json:"description,omitempty"`
	Parameters   jsonstable.Value `json:"parameters"`
	CacheControl *CacheControl    `json:"cacheControl,omitempty"`
}

// ModelRequest is the complete persisted input of one model call. It is the
// agent-owned mirror of sdk.Request with no open SDK interfaces or any fields.
type ModelRequest struct {
	Model    string    `json:"model"`
	System   string    `json:"system,omitempty"`
	Messages []Message `json:"messages,omitempty"`

	Tools      []ToolDefinition `json:"tools,omitempty"`
	ToolChoice ToolChoice       `json:"toolChoice,omitzero"`

	ResponseFormat *ResponseFormat `json:"responseFormat,omitempty"`

	Temperature      *float64 `json:"temperature,omitempty"`
	TopP             *float64 `json:"topP,omitempty"`
	MaxTokens        *int     `json:"maxTokens,omitempty"`
	StopSequences    []string `json:"stopSequences,omitempty"`
	FrequencyPenalty *float64 `json:"frequencyPenalty,omitempty"`
	PresencePenalty  *float64 `json:"presencePenalty,omitempty"`
	Seed             *int     `json:"seed,omitempty"`
	ReasoningEffort  *string  `json:"reasoningEffort,omitempty"`
	ReasoningSummary *string  `json:"reasoningSummary,omitempty"`
	PromptCacheKey   *string  `json:"promptCacheKey,omitempty"`

	ProviderOptions map[string]jsonstable.Value `json:"providerOptions,omitempty"`
}

type FinishReason string

const (
	FinishReasonStop          FinishReason = "stop"
	FinishReasonLength        FinishReason = "length"
	FinishReasonContentFilter FinishReason = "content-filter"
	FinishReasonToolCalls     FinishReason = "tool-calls"
	FinishReasonError         FinishReason = "error"
	FinishReasonOther         FinishReason = "other"
	FinishReasonUnknown       FinishReason = "unknown"
)

type InputTokenDetail struct {
	NoCacheTokens      int `json:"noCacheTokens"`
	CacheReadTokens    int `json:"cacheReadTokens"`
	CacheWriteTokens   int `json:"cacheWriteTokens"`
	CacheWrite5mTokens int `json:"cacheWrite5mTokens,omitempty"`
	CacheWrite1hTokens int `json:"cacheWrite1hTokens,omitempty"`
}

type OutputTokenDetail struct {
	TextTokens      int `json:"textTokens"`
	ReasoningTokens int `json:"reasoningTokens"`
}

type Usage struct {
	InputTokens        int               `json:"inputTokens"`
	OutputTokens       int               `json:"outputTokens"`
	TotalTokens        int               `json:"totalTokens"`
	ReasoningTokens    int               `json:"reasoningTokens,omitempty"`
	CachedInputTokens  int               `json:"cachedInputTokens,omitempty"`
	InputTokenDetails  InputTokenDetail  `json:"inputTokenDetails,omitempty"`
	OutputTokenDetails OutputTokenDetail `json:"outputTokenDetails,omitempty"`
}

//nolint:gocritic // hugeParam: Add is a pure value operation and must not mutate caller-owned Usage.
func (u Usage) Add(other Usage) Usage {
	u.InputTokens += other.InputTokens
	u.OutputTokens += other.OutputTokens
	u.TotalTokens += other.TotalTokens
	u.ReasoningTokens += other.ReasoningTokens
	u.CachedInputTokens += other.CachedInputTokens
	u.InputTokenDetails.NoCacheTokens += other.InputTokenDetails.NoCacheTokens
	u.InputTokenDetails.CacheReadTokens += other.InputTokenDetails.CacheReadTokens
	u.InputTokenDetails.CacheWriteTokens += other.InputTokenDetails.CacheWriteTokens
	u.InputTokenDetails.CacheWrite5mTokens += other.InputTokenDetails.CacheWrite5mTokens
	u.InputTokenDetails.CacheWrite1hTokens += other.InputTokenDetails.CacheWrite1hTokens
	u.OutputTokenDetails.TextTokens += other.OutputTokenDetails.TextTokens
	u.OutputTokenDetails.ReasoningTokens += other.OutputTokenDetails.ReasoningTokens
	return u
}

type ReasoningPart struct {
	ID               string           `json:"id,omitempty"`
	Text             string           `json:"text"`
	Format           ReasoningFormat  `json:"format,omitempty"`
	Model            string           `json:"model,omitempty"`
	ProviderMetadata ProviderMetadata `json:"providerMetadata,omitempty"`
}

type Source struct {
	SourceType       string           `json:"sourceType"`
	ID               string           `json:"id"`
	URL              string           `json:"url"`
	Title            string           `json:"title,omitempty"`
	ProviderMetadata ProviderMetadata `json:"providerMetadata,omitempty"`
}

type GeneratedFile struct {
	Data      string `json:"data"`
	MediaType string `json:"mediaType"`
}

type ModelToolCall struct {
	ToolCallID       string           `json:"toolCallId"`
	ToolName         string           `json:"toolName"`
	Input            ToolArguments    `json:"input"`
	ProviderMetadata ProviderMetadata `json:"providerMetadata,omitempty"`
}

// ResponseMetadata mirrors sdk.ResponseMetadata with the timestamp as an
// RFC 3339 UTC string. It is always present on a ModelResult; a field the
// provider did not report stays empty, and an all-empty value is omitted.
type ResponseMetadata struct {
	ID        string            `json:"id,omitempty"`
	ModelID   string            `json:"modelId,omitempty"`
	Timestamp string            `json:"timestamp,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// IsZero reports metadata with no field set; encoding/json's omitzero uses it.
func (m ResponseMetadata) IsZero() bool {
	return m.ID == "" && m.ModelID == "" && m.Timestamp == "" && len(m.Headers) == 0
}

// ModelResult is the persisted output of one model call. It mirrors
// sdk.ModelResult as agent-owned JSON-stable value types.
type ModelResult struct {
	Text                 string           `json:"text"`
	Reasoning            string           `json:"reasoning,omitempty"`
	ReasoningParts       []ReasoningPart  `json:"reasoningParts,omitempty"`
	TextProviderMetadata ProviderMetadata `json:"textProviderMetadata,omitempty"`

	FinishReason    FinishReason `json:"finishReason"`
	RawFinishReason string       `json:"rawFinishReason,omitempty"`
	Usage           Usage        `json:"usage"`

	Sources   []Source        `json:"sources,omitempty"`
	Files     []GeneratedFile `json:"files,omitempty"`
	ToolCalls []ModelToolCall `json:"toolCalls,omitempty"`

	Response ResponseMetadata `json:"response,omitzero"`
}
