package codex

// ModelDescriptor describes a Codex chat model and its capabilities.
type ModelDescriptor struct {
	ID                string
	DisplayName       string
	SupportsToolCall  bool
	SupportsReasoning bool
	ReasoningEfforts  []string
}

// Catalog returns the static model directory currently supported by the
// Codex backend integration.
func Catalog() []ModelDescriptor {
	return []ModelDescriptor{
		{
			ID:                "gpt-5.2",
			DisplayName:       "gpt-5.2",
			SupportsToolCall:  true,
			SupportsReasoning: true,
			ReasoningEfforts:  []string{"none", effortLow, effortMedium, effortHigh, effortXHigh},
		},
		{
			ID:                "gpt-5.2-codex",
			DisplayName:       "gpt-5.2-codex",
			SupportsToolCall:  true,
			SupportsReasoning: true,
			ReasoningEfforts:  []string{effortLow, effortMedium, effortHigh, effortXHigh},
		},
		{
			ID:                "gpt-5.1-codex-max",
			DisplayName:       "gpt-5.1-codex-max",
			SupportsToolCall:  true,
			SupportsReasoning: true,
			ReasoningEfforts:  []string{effortLow, effortMedium, effortHigh, effortXHigh},
		},
		{
			ID:                "gpt-5.1-codex",
			DisplayName:       "gpt-5.1-codex",
			SupportsToolCall:  true,
			SupportsReasoning: true,
			ReasoningEfforts:  []string{effortLow, effortMedium, effortHigh},
		},
		{
			ID:                "gpt-5.1-codex-mini",
			DisplayName:       "gpt-5.1-codex-mini",
			SupportsToolCall:  true,
			SupportsReasoning: true,
			ReasoningEfforts:  []string{effortMedium, effortHigh},
		},
		{
			ID:                "gpt-5.1",
			DisplayName:       "gpt-5.1",
			SupportsToolCall:  true,
			SupportsReasoning: true,
			ReasoningEfforts:  []string{"none", effortLow, effortMedium, effortHigh},
		},
	}
}
