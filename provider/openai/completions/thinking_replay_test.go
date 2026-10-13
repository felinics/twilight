package completions_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/provider/openai/completions"
	"github.com/felinics/twilight/sdk"
)

// captureMessages serves one canned completion and records the messages the
// provider sent, so tests can assert on reasoning_content key presence.
func captureMessages(t *testing.T) (*httptest.Server, *[]map[string]json.RawMessage) {
	t.Helper()
	var messages []map[string]json.RawMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []map[string]json.RawMessage `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		messages = body.Messages
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "chatcmpl-replay", "model": "m",
			"choices": []map[string]any{{
				"index": 0, "finish_reason": "stop",
				"message": map[string]any{"role": "assistant", "content": "ok"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	t.Cleanup(srv.Close)
	return srv, &messages
}

func toolCallMessage(id string, reasoning ...sdk.ReasoningPart) sdk.Message {
	parts := make([]sdk.MessagePart, 0, len(reasoning)+1)
	for _, r := range reasoning {
		parts = append(parts, r)
	}
	parts = append(parts, sdk.ToolCallPart{
		ToolCallID: id,
		ToolName:   "get_weather",
		Input:      sdk.ParseToolArguments(`{"city":"Paris"}`),
	})
	return sdk.Message{Role: sdk.MessageRoleAssistant, Content: parts}
}

func weatherTool() sdk.ToolDefinition {
	return sdk.ToolDefinition{
		Name:        "get_weather",
		Description: "Get weather",
		Parameters: mustSchema(map[string]any{
			"type":       "object",
			"properties": map[string]any{"city": map[string]any{"type": "string"}},
		}),
	}
}

// assertReasoningContent checks messages[index] against want: a nil want
// means the key must be absent, otherwise it must equal *want.
func assertReasoningContent(t *testing.T, msgs []map[string]json.RawMessage, index int, want *string) {
	t.Helper()
	raw, ok := msgs[index]["reasoning_content"]
	if !ok {
		if want != nil {
			t.Fatalf("messages[%d]: reasoning_content key missing, want %q", index, *want)
		}
		return
	}
	var got string
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("messages[%d]: reasoning_content is not a string: %s", index, raw)
	}
	if want == nil {
		t.Fatalf("messages[%d]: reasoning_content should be absent, got %q", index, got)
	}
	if got != *want {
		t.Fatalf("messages[%d]: reasoning_content = %q, want %q", index, got, *want)
	}
}

func str(s string) *string { return &s }

// TestDoGenerate_ThinkingReplayPadding drives padThinkingReplay through one
// history under each provider configuration: a user turn, a tool call whose
// reasoning varies per case, its result, a second tool call with no
// reasoning, its result, a text answer and a follow-up user turn. Only
// messages 1 and 3 are assistant tool calls; everything else must stay bare.
func TestDoGenerate_ThinkingReplayPadding(t *testing.T) {
	empty := str("")
	cases := []struct {
		name      string
		options   []completions.Option
		step1     []sdk.ReasoningPart // reasoning on the first tool call
		wantStep1 *string             // nil: key absent
		wantStep2 *string
	}{
		{
			name: "required replay pads bare tool calls",
			options: []completions.Option{
				completions.WithDeepSeekChatCompletionsCompat(),
				completions.WithReasoningReplay(sdk.ReasoningReplay{Required: true}),
			},
			wantStep1: empty, wantStep2: empty,
		},
		{
			// The compat options no longer imply a replay policy: the caller
			// sets it with WithReasoningReplay.
			name:    "deepseek compat alone does not pad",
			options: []completions.Option{completions.WithDeepSeekChatCompletionsCompat()},
		},
		{
			name:      "plain endpoint pads once the conversation carries reasoning_content",
			step1:     []sdk.ReasoningPart{{Text: "Paris first.", Format: sdk.ReasoningFormatOpenAIChat}},
			wantStep1: str("Paris first."), wantStep2: empty,
		},
		{
			name: "plain endpoint sends nothing without thinking evidence",
		},
		{
			// reasoning_details is an array; there is no empty value to pad.
			name: "details replay never pads",
			options: []completions.Option{
				completions.WithMiniMaxChatCompletionsCompat(),
				completions.WithReasoningReplay(sdk.ReasoningReplay{Field: sdk.ReasoningReplayDetails, Required: true}),
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, sent := captureMessages(t)
			opts := append([]completions.Option{completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL)}, tc.options...)
			p := completions.New(opts...)

			history := []sdk.Message{
				sdk.UserMessage("Weather in Paris, then Tokyo."),
				toolCallMessage("c1", tc.step1...),
				sdk.ToolMessage(sdk.ToolResultPart{ToolCallID: "c1", ToolName: "get_weather", Result: sdk.TextOutput("18C")}),
				toolCallMessage("c2"),
				sdk.ToolMessage(sdk.ToolResultPart{ToolCallID: "c2", ToolName: "get_weather", Result: sdk.TextOutput("25C")}),
				sdk.AssistantMessage("Paris 18C, Tokyo 25C."),
				sdk.UserMessage("Thanks."),
			}
			_, err := p.DoGenerate(context.Background(), sdk.Request{
				Model:    "m",
				Messages: history,
				Tools:    []sdk.ToolDefinition{weatherTool()},
			})
			if err != nil {
				t.Fatalf("DoGenerate: %v", err)
			}

			msgs := *sent
			if len(msgs) != len(history) {
				t.Fatalf("expected %d messages, got %d", len(history), len(msgs))
			}
			for i := range msgs {
				var want *string
				switch i {
				case 1:
					want = tc.wantStep1
				case 3:
					want = tc.wantStep2
				}
				assertReasoningContent(t, msgs, i, want)
			}
		})
	}
}
