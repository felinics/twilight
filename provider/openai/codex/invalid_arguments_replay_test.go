package codex_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/provider/openai/codex"
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
		openCodexStream(w)
		codexSSE(w, "response.created", codexCreated("resp_replay"))
		codexSSE(w, "response.output_item.added", `{"output_index":0,"item":{"type":"message","id":"msg_1"}}`)
		codexSSE(w, "response.output_text.delta", `{"item_id":"msg_1","delta":"ok"}`)
		codexSSE(w, "response.output_item.done", `{"output_index":0,"item":{"type":"message","id":"msg_1"}}`)
		codexSSE(w, "response.completed", `{"response":{"usage":{"input_tokens":3,"output_tokens":1}}}`)
	}))
	defer srv.Close()

	p := codex.New(codex.WithAccessToken("token"), codex.WithAccountID("acct_1"), codex.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: conformanceModelID,
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
	for _, item := range input {
		var call struct {
			Type      string `json:"type"`
			Arguments string `json:"arguments"`
		}
		if err := json.Unmarshal(item, &call); err != nil {
			t.Fatal(err)
		}
		if call.Type == "function_call" {
			if call.Arguments != "{}" {
				t.Fatalf("replayed arguments = %q, want {}", call.Arguments)
			}
			return
		}
	}
	t.Fatalf("no function_call item in %s", input)
}
