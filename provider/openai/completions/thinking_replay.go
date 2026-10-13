package completions

// padThinkingReplay makes sure every assistant tool-call message in the
// request carries a reasoning_content key when the endpoint validates
// thinking-mode replays.
//
// DeepSeek (thinking mode, request carries tools) and Moonshot/Kimi (thinking
// enabled) reject a request in which an assistant message with tool_calls has
// no reasoning_content key; an empty string satisfies the check. DeepSeek fills
// the gap from its own side while the tool_call id is fresh, so the failure
// surfaces on replays of persisted history: the model emitted no reasoning for
// that step, or the stored row dropped it. Both endpoints ignore the value on
// messages that were never tool calls.
//
// The key is added when the provider runs in DeepSeek or Kimi compat, or when
// some message in the request already carries reasoning_content: the endpoint
// then demonstrably accepts the field, so an empty one is safe. Plain OpenAI
// rejects unknown message fields, so nothing is added otherwise. Messages that
// carry reasoning_details (MiniMax) already replay their reasoning and are left
// alone, as is the whole request in MiniMax compat.
func padThinkingReplay(messages []chatMessage, compat chatCompletionsCompat) {
	if compat == chatCompletionsCompatMiniMax {
		return
	}
	if !thinkingReplayValidated(messages, compat) {
		return
	}
	for i := range messages {
		m := &messages[i]
		if m.Role != "assistant" || len(m.ToolCalls) == 0 {
			continue
		}
		if m.ReasoningContent != nil || len(m.ReasoningDetails) > 0 {
			continue
		}
		empty := ""
		m.ReasoningContent = &empty
	}
}

// thinkingReplayValidated reports whether the endpoint is expected to check
// reasoning_content on replayed tool-call messages.
func thinkingReplayValidated(messages []chatMessage, compat chatCompletionsCompat) bool {
	switch compat {
	case chatCompletionsCompatDeepSeek, chatCompletionsCompatKimi:
		return true
	}
	for i := range messages {
		if messages[i].Role == "assistant" && messages[i].ReasoningContent != nil {
			return true
		}
	}
	return false
}
