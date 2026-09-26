package completions_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/felinics/twilight/provider/openai/completions"
	"github.com/felinics/twilight/sdk"
)

// A historical tool call whose arguments were not a JSON document replays as
// the empty object: a backend that parses the arguments of earlier calls
// would otherwise reject the request, and keep rejecting it on every later
// call. The text the model produced reaches it through the error result.
func TestInvalidToolArgumentsReplayAsEmptyObject(t *testing.T) {
	srv, messages := captureMessages(t)
	p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "m",
		Messages: invalidArgumentsExchange(),
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	var assistant struct {
		ToolCalls []struct {
			Function struct {
				Arguments string `json:"arguments"`
			} `json:"function"`
		} `json:"tool_calls"`
	}
	if len(*messages) < 2 {
		t.Fatalf("messages sent = %d, want at least 2", len(*messages))
	}
	raw, _ := json.Marshal((*messages)[1])
	if err := json.Unmarshal(raw, &assistant); err != nil {
		t.Fatal(err)
	}
	if len(assistant.ToolCalls) != 1 || assistant.ToolCalls[0].Function.Arguments != "{}" {
		t.Fatalf("replayed arguments = %#v, want {}", assistant.ToolCalls)
	}
}

func invalidArgumentsExchange() []sdk.Message {
	return []sdk.Message{
		sdk.UserMessage("delete something"),
		{Role: sdk.MessageRoleAssistant, Content: []sdk.MessagePart{sdk.ToolCallPart{
			ToolCallID: "call_1",
			ToolName:   "delete_file",
			Input:      sdk.ParseToolArguments(`{"path": "/etc`),
		}}},
		sdk.ToolMessage(sdk.ToolResultPart{
			ToolCallID: "call_1",
			ToolName:   "delete_file",
			Result:     sdk.TextOutput(`invalid tool arguments: {"path": "/etc`),
			IsError:    true,
		}),
	}
}
