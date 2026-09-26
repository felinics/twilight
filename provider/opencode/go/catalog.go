package opencodego

// Protocol identifies the wire protocol used by an OpenCode Go model.
type Protocol string

const (
	ProtocolCompletions Protocol = "openai-completions"
	ProtocolResponses   Protocol = "openai-responses"
	ProtocolMessages    Protocol = "anthropic-messages"
)

// ModelDescriptor describes a documented model's wire protocol. Applications
// can use Protocol to select protocol-specific reasoning and cache settings.
type ModelDescriptor struct {
	ID          string
	DisplayName string
	Protocol    Protocol
}

// Catalog returns the model routes documented at https://opencode.ai/docs/go/
// on 2026-09-21. It is a routing directory, not a promise of account availability.
// ListModels queries the live catalog; WithModelProtocols adds or overrides
// routes when the service introduces models or changes an endpoint.
func Catalog() []ModelDescriptor {
	return []ModelDescriptor{
		{ID: "grok-4.7", DisplayName: "Grok 4.7", Protocol: ProtocolResponses},
		{ID: "grok-4.6", DisplayName: "Grok 4.6", Protocol: ProtocolResponses},
		{ID: "gpt-5.6-luna", DisplayName: "GPT 5.6 Luna", Protocol: ProtocolResponses},
		{ID: "glm-5.3-flash", DisplayName: "GLM-5.3-Flash", Protocol: ProtocolCompletions},
		{ID: "glm-5.3", DisplayName: "GLM-5.3", Protocol: ProtocolCompletions},
		{ID: "glm-5.2", DisplayName: "GLM-5.2", Protocol: ProtocolCompletions},
		{ID: "glm-5.1", DisplayName: "GLM-5.1", Protocol: ProtocolCompletions},
		{ID: "kimi-k3", DisplayName: "Kimi K3", Protocol: ProtocolCompletions},
		{ID: "kimi-k2.7-code", DisplayName: "Kimi K2.7 Code", Protocol: ProtocolCompletions},
		{ID: "kimi-k2.6", DisplayName: "Kimi K2.6", Protocol: ProtocolCompletions},
		{ID: "longcat-2.0", DisplayName: "LongCat-2.0", Protocol: ProtocolCompletions},
		// Not in the endpoint table: listed by the live /models endpoint and
		// verified against the service on 2026-09-20.
		{ID: "deepseek-flash", DisplayName: "DeepSeek Flash", Protocol: ProtocolCompletions},
		{ID: "deepseek-v4.1-flash", DisplayName: "DeepSeek V4.1 Flash", Protocol: ProtocolCompletions},
		{ID: "deepseek-v4-pro", DisplayName: "DeepSeek V4 Pro", Protocol: ProtocolCompletions},
		{ID: "deepseek-v4-flash", DisplayName: "DeepSeek V4 Flash", Protocol: ProtocolCompletions},
		{ID: "deepseek-v4-flash-vision-exp", DisplayName: "DeepSeek V4 Flash Vision Exp", Protocol: ProtocolCompletions},
		{ID: "mimo-v2.5", DisplayName: "MiMo-V2.5", Protocol: ProtocolCompletions},
		{ID: "mimo-v2.5-pro", DisplayName: "MiMo-V2.5-Pro", Protocol: ProtocolCompletions},
		{ID: "minimax-m3", DisplayName: "MiniMax M3", Protocol: ProtocolMessages},
		{ID: "minimax-m2.7", DisplayName: "MiniMax M2.7", Protocol: ProtocolMessages},
		{ID: "minimax-m2.5", DisplayName: "MiniMax M2.5", Protocol: ProtocolMessages},
		{ID: "muse-spark-1.3-contributor", DisplayName: "Muse Spark 1.3 Contributor", Protocol: ProtocolResponses},
		{ID: "muse-spark-1.2-contributor", DisplayName: "Muse Spark 1.2 Contributor", Protocol: ProtocolResponses},
		{ID: "qwen3.8-max", DisplayName: "Qwen3.8 Max", Protocol: ProtocolMessages},
		{ID: "qwen3.8-flash", DisplayName: "Qwen3.8 Flash", Protocol: ProtocolMessages},
		{ID: "qwen3.7-max", DisplayName: "Qwen3.7 Max", Protocol: ProtocolMessages},
		{ID: "qwen3.7-plus", DisplayName: "Qwen3.7 Plus", Protocol: ProtocolMessages},
		{ID: "qwen3.6-plus", DisplayName: "Qwen3.6 Plus", Protocol: ProtocolMessages},
		{ID: "hy4-preview", DisplayName: "Hy4 preview", Protocol: ProtocolCompletions},
		{ID: "hy3", DisplayName: "Hy3", Protocol: ProtocolCompletions},
	}
}
