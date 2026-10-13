package completions_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/felinics/twilight/provider/openai/completions"
	"github.com/felinics/twilight/sdk"
)

// openRouterDetails is a reasoning_details array as OpenRouter returns it for a
// hosted model: a signed text entry and an encrypted entry, both indexed.
const openRouterDetails = `[{"type":"reasoning.text","text":"Paris first.","signature":"SIG","format":"anthropic-claude-v1","index":0},{"type":"reasoning.encrypted","data":"ENC","format":"anthropic-claude-v1","index":1}]`

func detailsPart(namespace, details string) sdk.ReasoningPart {
	return sdk.ReasoningPart{
		Text:             "Paris first.",
		Format:           sdk.ReasoningFormatOpenAIChat,
		ProviderMetadata: sdk.NewProviderMetadata(namespace, map[string]string{"reasoning_details": details}),
	}
}

// replayRequest sends a two-tool-call history and returns the two assistant
// messages as decoded JSON objects.
func replayRequest(t *testing.T, options []completions.Option, step1 ...sdk.ReasoningPart) (map[string]json.RawMessage, map[string]json.RawMessage) {
	t.Helper()
	srv, sent := captureMessages(t)
	opts := append([]completions.Option{completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL)}, options...)
	_, err := completions.New(opts...).DoGenerate(context.Background(), sdk.Request{
		Model: "m",
		Messages: []sdk.Message{
			sdk.UserMessage("Weather in Paris, then Tokyo."),
			toolCallMessage("c1", step1...),
			sdk.ToolMessage(sdk.ToolResultPart{ToolCallID: "c1", ToolName: "get_weather", Result: sdk.TextOutput("18C")}),
			toolCallMessage("c2"),
			sdk.ToolMessage(sdk.ToolResultPart{ToolCallID: "c2", ToolName: "get_weather", Result: sdk.TextOutput("25C")}),
		},
		Tools: []sdk.ToolDefinition{weatherTool()},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	return (*sent)[1], (*sent)[3]
}

func stringField(t *testing.T, msg map[string]json.RawMessage, key string) *string {
	t.Helper()
	raw, ok := msg[key]
	if !ok {
		return nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("%s is not a string: %s", key, raw)
	}
	return &s
}

func TestReasoningReplay_ReasoningField(t *testing.T) {
	step1, step2 := replayRequest(t,
		[]completions.Option{completions.WithReasoningReplay(sdk.ReasoningReplay{
			Field: sdk.ReasoningReplayReasoning, Required: true,
		})},
		sdk.ReasoningPart{Text: "Paris first.", Format: sdk.ReasoningFormatOpenAIChat},
	)
	if got := stringField(t, step1, "reasoning"); got == nil || *got != "Paris first." {
		t.Errorf("step 1 reasoning: got %v, want Paris first.", got)
	}
	if got := stringField(t, step2, "reasoning"); got == nil || *got != "" {
		t.Errorf("step 2 reasoning: got %v, want an empty padded value", got)
	}
	for i, msg := range []map[string]json.RawMessage{step1, step2} {
		if _, ok := msg["reasoning_content"]; ok {
			t.Errorf("step %d: reasoning_content must not be sent alongside reasoning", i+1)
		}
	}
}

func TestReasoningReplay_DetailsFieldReplaysArrayUnchanged(t *testing.T) {
	for _, namespace := range []string{"openai-chat", "minimax"} {
		t.Run(namespace, func(t *testing.T) {
			step1, step2 := replayRequest(t,
				[]completions.Option{completions.WithReasoningReplay(sdk.ReasoningReplay{Field: sdk.ReasoningReplayDetails})},
				detailsPart(namespace, openRouterDetails),
			)
			var got, want []map[string]any
			if err := json.Unmarshal(step1["reasoning_details"], &got); err != nil {
				t.Fatalf("reasoning_details: %v (%s)", err, step1["reasoning_details"])
			}
			if err := json.Unmarshal([]byte(openRouterDetails), &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("reasoning_details changed on replay:\n got  %v\n want %v", got, want)
			}
			if _, ok := step1["reasoning_content"]; ok {
				t.Error("reasoning_content must not be sent alongside reasoning_details")
			}
			if _, ok := step2["reasoning_details"]; ok {
				t.Error("a step without preserved details must send none")
			}
		})
	}
}

func TestReasoningReplay_DefaultSendsContentOnly(t *testing.T) {
	step1, _ := replayRequest(t, nil, detailsPart("openai-chat", openRouterDetails))
	if got := stringField(t, step1, "reasoning_content"); got == nil || *got != "Paris first." {
		t.Errorf("reasoning_content: got %v, want Paris first.", got)
	}
	if _, ok := step1["reasoning_details"]; ok {
		t.Error("the default replay must not send reasoning_details")
	}
}

func TestReasoningReplay_UnknownFieldFailsBeforeSending(t *testing.T) {
	sent := false
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { sent = true }))
	defer srv.Close()
	p := completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL(srv.URL),
		completions.WithReasoningReplay(sdk.ReasoningReplay{Field: "thoughts"}),
	)
	_, err := p.DoGenerate(context.Background(), sdk.Request{Model: "m", Messages: []sdk.Message{sdk.UserMessage("hi")}})
	if err == nil {
		t.Fatal("DoGenerate: want an error for an unknown replay field")
	}
	if sent {
		t.Error("the request must fail before it is sent")
	}
}

// OpenRouter streams reasoning_details in wire order: text arrives in pieces,
// the signature on a later piece, and an encrypted entry with no text. The
// block's metadata must hold the merged entries, every field kept.
func TestDoStream_ReasoningDetailsMergedInWireOrder(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		chunks := []string{
			`{"id":"c1","model":"m","choices":[{"index":0,"delta":{"role":"assistant","reasoning":"Paris","reasoning_details":[{"type":"reasoning.text","text":"Paris","format":"anthropic-claude-v1","index":0}]}}]}`,
			`{"id":"c1","model":"m","choices":[{"index":0,"delta":{"reasoning":" first.","reasoning_details":[{"type":"reasoning.text","text":" first.","format":"anthropic-claude-v1","index":0}]}}]}`,
			`{"id":"c1","model":"m","choices":[{"index":0,"delta":{"reasoning_details":[{"type":"reasoning.text","text":"","signature":"SIG","format":"anthropic-claude-v1","index":0}]}}]}`,
			`{"id":"c1","model":"m","choices":[{"index":0,"delta":{"reasoning_details":[{"type":"reasoning.encrypted","data":"ENC","format":"anthropic-claude-v1","index":1}]}}]}`,
			`{"id":"c1","model":"m","choices":[{"index":0,"delta":{"content":"18C."}}]}`,
			`{"id":"c1","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL))
	stream, err := p.DoStream(context.Background(), sdk.Request{Model: "m", Messages: []sdk.Message{sdk.UserMessage("hi")}})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	var reasoning string
	var meta sdk.ProviderMetadata
	for part := range stream {
		switch v := part.(type) {
		case *sdk.ReasoningDeltaPart:
			reasoning += v.Text
		case *sdk.ReasoningEndPart:
			meta = v.ProviderMetadata
		case *sdk.ErrorPart:
			t.Fatalf("stream error: %v", v.Error)
		}
	}
	if reasoning != "Paris first." {
		t.Errorf("reasoning text: got %q", reasoning)
	}
	var got, want []map[string]any
	if err := json.Unmarshal([]byte(meta.Get("openai-chat", "reasoning_details")), &got); err != nil {
		t.Fatalf("metadata: %v (%#v)", err, meta)
	}
	if err := json.Unmarshal([]byte(openRouterDetails), &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merged reasoning_details:\n got  %v\n want %v", got, want)
	}
}
