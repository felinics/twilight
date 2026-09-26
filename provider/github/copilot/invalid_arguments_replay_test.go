package copilot_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/provider/github/copilot"
	"github.com/felinics/twilight/sdk"
)

// A historical tool call whose arguments were not a JSON document replays as
// the empty object on the tool call's string-typed arguments.
func TestInvalidToolArgumentsReplayAsEmptyObject(t *testing.T) {
	var messages []map[string]json.RawMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []map[string]json.RawMessage `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		messages = body.Messages
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"object":"chat.completion","choices":[{"index":0,"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]}`))
	}))
	defer srv.Close()

	p := copilot.New(copilot.WithAPIKey("k"), copilot.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: "gpt-4o",
		Messages: []sdk.Message{
			sdk.UserMessage("delete something"),
			{Role: sdk.MessageRoleAssistant, Content: []sdk.MessagePart{sdk.ToolCallPart{
				ToolCallID: "call_1", ToolName: "delete_file", Input: sdk.ParseToolArguments(`{"path": "/etc`),
			}}},
			sdk.ToolMessage(sdk.ToolResultPart{ToolCallID: "call_1", ToolName: "delete_file", Result: sdk.TextOutput("invalid arguments"), IsError: true}),
		},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	if len(messages) < 2 {
		t.Fatalf("messages sent = %d, want at least 2", len(messages))
	}
	var assistant struct {
		ToolCalls []struct {
			Function struct {
				Arguments string `json:"arguments"`
			} `json:"function"`
		} `json:"tool_calls"`
	}
	raw, _ := json.Marshal(messages[1])
	if err := json.Unmarshal(raw, &assistant); err != nil {
		t.Fatal(err)
	}
	if len(assistant.ToolCalls) != 1 || assistant.ToolCalls[0].Function.Arguments != "{}" {
		t.Fatalf("replayed arguments = %#v, want {}", assistant.ToolCalls)
	}
}
