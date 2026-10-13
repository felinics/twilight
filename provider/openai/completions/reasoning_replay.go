package completions

import (
	"fmt"

	"github.com/felinics/twilight/sdk"
)

// roleAssistant is the Chat Completions role of a model turn.
const roleAssistant = "assistant"

// WithReasoningReplay sets how assistant reasoning is replayed: the field that
// carries it and whether every tool-call message must carry it. A model's
// Reasoning.Replay can be passed as is.
//
// Without it the provider replays reasoning text as reasoning_content, and
// adds the key to bare tool calls only once the conversation already carries
// it. DeepSeek and Kimi in thinking mode need Required; MiniMax and
// OpenRouter need sdk.ReasoningReplayDetails.
func WithReasoningReplay(replay sdk.ReasoningReplay) Option {
	return func(p *Provider) {
		p.replay = replay
	}
}

func replayField(replay sdk.ReasoningReplay) sdk.ReasoningReplayField {
	if replay.Field == "" {
		return sdk.ReasoningReplayContent
	}
	return replay.Field
}

func validateReplay(replay sdk.ReasoningReplay) error {
	switch replayField(replay) {
	case sdk.ReasoningReplayContent, sdk.ReasoningReplayDetails, sdk.ReasoningReplayReasoning:
		return nil
	default:
		return fmt.Errorf("unknown reasoning replay field %q", replay.Field)
	}
}

// applyReasoningReplay moves each assistant message's reasoning onto the
// configured field. convertAssistantMessage fills ReasoningContent with the
// text and ReasoningDetails with the preserved array; exactly one of them, or
// reasoning, leaves the process.
func applyReasoningReplay(messages []chatMessage, replay sdk.ReasoningReplay) {
	field := replayField(replay)
	for i := range messages {
		m := &messages[i]
		if m.Role != roleAssistant {
			continue
		}
		switch field {
		case sdk.ReasoningReplayDetails:
			m.ReasoningContent = nil
		case sdk.ReasoningReplayReasoning:
			m.Reasoning = m.ReasoningContent
			m.ReasoningContent = nil
			m.ReasoningDetails = nil
		default:
			m.ReasoningDetails = nil
		}
	}
	if field == sdk.ReasoningReplayDetails {
		return
	}
	if !replay.Required && !carriesReasoningField(messages, field) {
		return
	}
	for i := range messages {
		m := &messages[i]
		if m.Role != roleAssistant || len(m.ToolCalls) == 0 {
			continue
		}
		empty := ""
		switch field {
		case sdk.ReasoningReplayReasoning:
			if m.Reasoning == nil {
				m.Reasoning = &empty
			}
		default:
			if m.ReasoningContent == nil {
				m.ReasoningContent = &empty
			}
		}
	}
}

// carriesReasoningField reports whether some assistant message already carries
// the text field.
func carriesReasoningField(messages []chatMessage, field sdk.ReasoningReplayField) bool {
	for i := range messages {
		m := &messages[i]
		if m.Role != roleAssistant {
			continue
		}
		if field == sdk.ReasoningReplayReasoning && m.Reasoning != nil {
			return true
		}
		if field == sdk.ReasoningReplayContent && m.ReasoningContent != nil {
			return true
		}
	}
	return false
}
