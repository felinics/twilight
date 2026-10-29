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

func captureCodexInput(t *testing.T, messages []sdk.Message) []map[string]any {
	t.Helper()
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
	if _, err := p.DoGenerate(context.Background(), sdk.Request{Model: conformanceModelID, Messages: messages}); err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	items := make([]map[string]any, 0, len(input))
	for _, raw := range input {
		var item map[string]any
		if err := json.Unmarshal(raw, &item); err != nil {
			t.Fatalf("unmarshal item: %v", err)
		}
		items = append(items, item)
	}
	return items
}

func reasoningItems(items []map[string]any) []map[string]any {
	var out []map[string]any
	for _, item := range items {
		if item["type"] == "reasoning" {
			out = append(out, item)
		}
	}
	return out
}

func assistantTexts(items []map[string]any) []string {
	var out []string
	for _, item := range items {
		if item["role"] != "assistant" {
			continue
		}
		content, _ := item["content"].([]any)
		for _, part := range content {
			if p, ok := part.(map[string]any); ok {
				if text, ok := p["text"].(string); ok {
					out = append(out, text)
				}
			}
		}
	}
	return out
}

func replayedAssistant(parts ...sdk.MessagePart) []sdk.Message {
	return []sdk.Message{
		sdk.UserMessage("hi"),
		{Role: sdk.MessageRoleAssistant, Content: append(parts, sdk.TextPart{Text: "answer"})},
		sdk.UserMessage("again"),
	}
}

// Reasoning the backend issued replays as one item per id, with its summary
// entries and encrypted content, so a continuing conversation keeps its
// reasoning state.
func TestReplayKeepsIssuedReasoningItem(t *testing.T) {
	meta := sdk.ProviderMetadata{"openai": {"itemId": "rs_1", "reasoningEncryptedContent": "EC"}}
	items := captureCodexInput(t, replayedAssistant(
		sdk.ReasoningPart{ID: "rs_1", Text: "first", Format: sdk.ReasoningFormatOpenAIResponses, ProviderMetadata: meta},
		sdk.ReasoningPart{ID: "rs_1", Text: "second", Format: sdk.ReasoningFormatOpenAIResponses, ProviderMetadata: meta},
	))

	reasoning := reasoningItems(items)
	if len(reasoning) != 1 {
		t.Fatalf("reasoning items: got %d, want 1: %+v", len(reasoning), items)
	}
	if reasoning[0]["id"] != "rs_1" || reasoning[0]["encrypted_content"] != "EC" {
		t.Fatalf("reasoning item: got %+v, want id rs_1 with its encrypted content", reasoning[0])
	}
	if summary, _ := reasoning[0]["summary"].([]any); len(summary) != 2 {
		t.Fatalf("summary entries: got %d, want 2: %+v", len(summary), reasoning[0])
	}
}

// A Responses-compatible relay shares the dialect but issues its own item ids.
// The backend rejects the whole request for an id it did not issue, so such
// items are dropped while the rest of the turn still replays.
func TestReplayDropsReasoningItemsTheBackendDidNotIssue(t *testing.T) {
	cases := map[string]sdk.ReasoningPart{
		"relay item id in metadata": {
			ID: "0f8e2c1a-5b7d-4e3f-9a6b-2c4d8e1f3a5b", Text: "relay thought", Format: sdk.ReasoningFormatOpenAIResponses,
			ProviderMetadata: sdk.ProviderMetadata{"openai": {"itemId": "0f8e2c1a-5b7d-4e3f-9a6b-2c4d8e1f3a5b", "reasoningEncryptedContent": "RELAY"}},
		},
		"relay block id without metadata": {
			ID: "0f8e2c1a-5b7d-4e3f-9a6b-2c4d8e1f3a5b", Text: "relay thought", Format: sdk.ReasoningFormatOpenAIResponses,
		},
		"no id": {
			Text: "unidentified thought", Format: sdk.ReasoningFormatOpenAIResponses,
		},
	}
	for name, part := range cases {
		t.Run(name, func(t *testing.T) {
			items := captureCodexInput(t, replayedAssistant(part))
			if reasoning := reasoningItems(items); len(reasoning) != 0 {
				t.Fatalf("replayed reasoning the backend did not issue: %+v", reasoning)
			}
			if texts := assistantTexts(items); len(texts) != 1 || texts[0] != "answer" {
				t.Fatalf("assistant text: got %v, want [answer]", texts)
			}
		})
	}
}

// Dropping one foreign item leaves the issued ones in the same message intact.
func TestReplayKeepsIssuedItemsBesideForeignOnes(t *testing.T) {
	items := captureCodexInput(t, replayedAssistant(
		sdk.ReasoningPart{ID: "relay-1", Text: "relay", Format: sdk.ReasoningFormatOpenAIResponses},
		sdk.ReasoningPart{Text: "issued", Format: sdk.ReasoningFormatOpenAIResponses,
			ProviderMetadata: sdk.ProviderMetadata{"openai": {"itemId": "rs_2"}}},
	))

	reasoning := reasoningItems(items)
	if len(reasoning) != 1 || reasoning[0]["id"] != "rs_2" {
		t.Fatalf("reasoning items: got %+v, want only rs_2", reasoning)
	}
}
