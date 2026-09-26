package responses_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/provider/openai/responses"
	"github.com/felinics/twilight/sdk"
)

// A historical tool call whose arguments were not a JSON document replays as
// the empty object on the function_call item's string-typed arguments.
func TestInvalidToolArgumentsReplayAsEmptyObject(t *testing.T) {
	var input []json.RawMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input []json.RawMessage `json:"input"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode body: %v", err)
		}
		input = body.Input
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "resp_replay", "created_at": 1700000000, "model": "gpt-4o-mini",
			"output": []map[string]any{{
				"type": "message", "id": "msg_1", "role": "assistant",
				"content": []map[string]any{{"type": "output_text", "text": "ok", "annotations": []any{}}},
			}},
			"usage": map[string]any{"input_tokens": 3, "output_tokens": 1},
		})
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: "gpt-4o-mini",
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
	assertFunctionCallArguments(t, input, "{}")
}

func assertFunctionCallArguments(t *testing.T, items []json.RawMessage, want string) {
	t.Helper()
	for _, item := range items {
		var call struct {
			Type      string `json:"type"`
			Arguments string `json:"arguments"`
		}
		if err := json.Unmarshal(item, &call); err != nil {
			t.Fatal(err)
		}
		if call.Type == "function_call" {
			if call.Arguments != want {
				t.Fatalf("replayed arguments = %q, want %q", call.Arguments, want)
			}
			return
		}
	}
	t.Fatalf("no function_call item in %s", items)
}
