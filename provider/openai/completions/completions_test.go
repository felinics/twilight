package completions_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"

	"github.com/felinics/twilight/internal/testutil"
	"github.com/felinics/twilight/provider/openai/completions"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// mustJSON is the test-side half of the seam change: the SDK resolves a tool's
// Parameters into JSON Schema before a provider sees it, so a test that used to
// hand the provider a Go schema value now hands it the resolved JSON.
// mustSchema builds a schema from its JSON shape, for fixtures written as maps.
func mustSchema(v any) *jsonschema.Schema {
	var s jsonschema.Schema
	if err := json.Unmarshal(mustJSON(v), &s); err != nil {
		panic(err)
	}
	return &s
}

func mustJSON(v any) json.RawMessage {
	encoded, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return encoded
}

// ---------- unit tests (mock server) ----------

func TestDoGenerate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("unexpected auth header: %s", r.Header.Get("Authorization"))
		}

		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)
		if body["model"] != "gpt-4o-mini" {
			t.Errorf("unexpected model: %v", body["model"])
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":      "chatcmpl-test",
			"object":  "chat.completion",
			"created": 1700000000,
			"model":   "gpt-4o-mini",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "Hello!"},
			}},
			"usage": map[string]any{
				"prompt_tokens":     5,
				"completion_tokens": 2,
				"total_tokens":      7,
			},
		})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("test-key"),
		completions.WithBaseURL(srv.URL),
	)

	model := &sdk.Model{ID: "gpt-4o-mini"}
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: model.ID,
		Messages: []sdk.Message{{
			Role:    sdk.MessageRoleUser,
			Content: []sdk.MessagePart{sdk.TextPart{Text: "Hi"}},
		}},
	})
	if err != nil {
		t.Fatalf("DoGenerate failed: %v", err)
	}

	if result.Text != "Hello!" {
		t.Errorf("expected 'Hello!', got %q", result.Text)
	}
	if result.FinishReason != sdk.FinishReasonStop {
		t.Errorf("expected finish reason 'stop', got %q", result.FinishReason)
	}
	if result.Usage.InputTokens != 5 {
		t.Errorf("expected 5 input tokens, got %d", result.Usage.InputTokens)
	}
	if result.Usage.OutputTokens != 2 {
		t.Errorf("expected 2 output tokens, got %d", result.Usage.OutputTokens)
	}
}

func TestDoGenerate_PromptCacheKey(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&body)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":      "chatcmpl-test",
			"object":  "chat.completion",
			"created": 1700000000,
			"model":   "gpt-4o-mini",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "Hello!"},
			}},
			"usage": map[string]any{
				"prompt_tokens":     5,
				"completion_tokens": 2,
				"total_tokens":      7,
			},
		})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("test-key"),
		completions.WithBaseURL(srv.URL),
	)

	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:          "gpt-4o-mini",
		Messages:       []sdk.Message{sdk.UserMessage("Hi")},
		PromptCacheKey: stringPtr("some-key"),
	})
	if err != nil {
		t.Fatalf("DoGenerate failed: %v", err)
	}

	if body["prompt_cache_key"] != "some-key" {
		t.Errorf("expected prompt_cache_key %q, got %v", "some-key", body["prompt_cache_key"])
	}
}

func TestDoGenerate_PromptCacheKeyOmittedWhenUnset(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&body)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":      "chatcmpl-test",
			"object":  "chat.completion",
			"created": 1700000000,
			"model":   "gpt-4o-mini",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "Hello!"},
			}},
			"usage": map[string]any{
				"prompt_tokens":     5,
				"completion_tokens": 2,
				"total_tokens":      7,
			},
		})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("test-key"),
		completions.WithBaseURL(srv.URL),
	)

	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "gpt-4o-mini",
		Messages: []sdk.Message{sdk.UserMessage("Hi")},
	})
	if err != nil {
		t.Fatalf("DoGenerate failed: %v", err)
	}

	if _, ok := body["prompt_cache_key"]; ok {
		t.Errorf("expected prompt_cache_key to be omitted, got %v", body["prompt_cache_key"])
	}
}

func TestDoGenerate_WithBedrockCredentials(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got == "" || got[:16] != "AWS4-HMAC-SHA256" {
			t.Fatalf("expected SigV4 auth header, got %q", got)
		}
		if got := r.Header.Get("X-Amz-Date"); got == "" {
			t.Fatal("expected X-Amz-Date header")
		}
		if got := r.Header.Get("X-Amz-Security-Token"); got != "test-session" {
			t.Fatalf("expected session token header, got %q", got)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":      "chatcmpl-test",
			"object":  "chat.completion",
			"created": 1700000000,
			"model":   "openai.gpt-oss-120b",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "Hello from Bedrock"},
			}},
			"usage": map[string]any{
				"prompt_tokens":     5,
				"completion_tokens": 2,
				"total_tokens":      7,
			},
		})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithBaseURL(srv.URL),
		completions.WithBedrockCredentials("us-east-1", "AKIDEXAMPLE", "secret", "test-session"),
	)

	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "openai.gpt-oss-120b",
		Messages: []sdk.Message{sdk.UserMessage("Hi")},
	})
	if err != nil {
		t.Fatalf("DoGenerate failed: %v", err)
	}
	if result.Text != "Hello from Bedrock" {
		t.Fatalf("expected Bedrock response, got %q", result.Text)
	}
}

func TestDoStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("server does not support flushing")
		}

		chunks := []string{
			`{"id":"chunk-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"Hello"},"finish_reason":null}]}`,
			`{"id":"chunk-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":" world"},"finish_reason":null}]}`,
			`{"id":"chunk-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("test-key"),
		completions.WithBaseURL(srv.URL),
	)

	model := &sdk.Model{ID: "gpt-4o-mini"}
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model: model.ID,
		Messages: []sdk.Message{{
			Role:    sdk.MessageRoleUser,
			Content: []sdk.MessagePart{sdk.TextPart{Text: "Hi"}},
		}},
	})
	if err != nil {
		t.Fatalf("DoStream failed: %v", err)
	}

	var collected string
	var gotStart, gotFinish bool
	for part := range sr {
		switch p := part.(type) {
		case *sdk.StartPart:
			gotStart = true
		case *sdk.TextDeltaPart:
			collected += p.Text
		case *sdk.FinishPart:
			gotFinish = true
			if p.FinishReason != sdk.FinishReasonStop {
				t.Errorf("expected stop, got %q", p.FinishReason)
			}
		}
	}

	if !gotStart {
		t.Error("missing StartPart")
	}
	if !gotFinish {
		t.Error("missing FinishPart")
	}
	if collected != "Hello world" {
		t.Errorf("expected 'Hello world', got %q", collected)
	}
}

// TestDoStream_UsageInTrailingChunk guards against a regression where the
// OpenAI Chat Completions streaming protocol delivers the `usage` block in a
// SEPARATE chunk AFTER the chunk carrying `finish_reason` (with `choices: []`),
// as permitted by the official spec and emitted by llama.cpp's server
// implementation when stream_options.include_usage is enabled.
//
// The previous streamProcessor emitted FinishStepPart synchronously inside
// processFinishReason, capturing sp.usage at that moment. Because the trailing
// usage chunk arrives later, FinishStepPart.Usage ended up empty. The fix
// defers FinishStepPart until the stream completes.
//
// See: https://github.com/felinics/twilight/issues/7
func TestDoStream_UsageInTrailingChunk(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("server does not support flushing")
		}

		// Mimic llama.cpp / OpenAI spec when include_usage=true:
		// 1) content chunk
		// 2) chunk with finish_reason and NO usage
		// 3) trailing chunk with choices:[] and the usage payload
		chunks := []string{
			`{"id":"chunk-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"Hello"},"finish_reason":null}]}`,
			`{"id":"chunk-1","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			`{"id":"chunk-1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":5,"completion_tokens":1,"total_tokens":6}}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("test-key"),
		completions.WithBaseURL(srv.URL),
	)

	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model: "gpt-4o-mini",
		Messages: []sdk.Message{{
			Role:    sdk.MessageRoleUser,
			Content: []sdk.MessagePart{sdk.TextPart{Text: "Hi"}},
		}},
	})
	if err != nil {
		t.Fatalf("DoStream failed: %v", err)
	}

	var (
		finishStep *sdk.FinishStepPart
		finishAll  *sdk.FinishPart
	)
	for part := range sr {
		switch p := part.(type) {
		case *sdk.FinishStepPart:
			finishStep = p
		case *sdk.FinishPart:
			finishAll = p
		case *sdk.ErrorPart:
			t.Fatalf("unexpected error part: %v", p.Error)
		}
	}

	if finishStep == nil {
		t.Fatal("missing FinishStepPart")
	}
	if finishAll == nil {
		t.Fatal("missing FinishPart")
	}

	// Both the per-step usage and the total usage must reflect the trailing
	// usage chunk. The pre-fix implementation left FinishStepPart.Usage empty
	// because it was sent before the trailing chunk was processed.
	if finishStep.Usage.InputTokens != 5 {
		t.Errorf("FinishStepPart.Usage.InputTokens: got %d, want 5", finishStep.Usage.InputTokens)
	}
	if finishStep.Usage.OutputTokens != 1 {
		t.Errorf("FinishStepPart.Usage.OutputTokens: got %d, want 1", finishStep.Usage.OutputTokens)
	}
	if finishStep.Usage.TotalTokens != 6 {
		t.Errorf("FinishStepPart.Usage.TotalTokens: got %d, want 6", finishStep.Usage.TotalTokens)
	}

	// FinishPart.TotalUsage already works today because DoStream sends it
	// after the SSE loop finishes — but assert it explicitly so a regression
	// either way is caught.
	if finishAll.TotalUsage.InputTokens != 5 {
		t.Errorf("FinishPart.TotalUsage.InputTokens: got %d, want 5", finishAll.TotalUsage.InputTokens)
	}
}

func TestDoGenerate_WithImage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []struct {
				Role    string `json:"role"`
				Content any    `json:"content"`
			} `json:"messages"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		if len(body.Messages) != 1 {
			t.Fatalf("expected 1 message, got %d", len(body.Messages))
		}

		parts, ok := body.Messages[0].Content.([]any)
		if !ok {
			t.Fatalf("expected array content, got %T", body.Messages[0].Content)
		}
		if len(parts) != 2 {
			t.Fatalf("expected 2 content parts, got %d", len(parts))
		}

		textPart := parts[0].(map[string]any)
		if textPart["type"] != "text" || textPart["text"] != "What is in this image?" {
			t.Errorf("unexpected text part: %v", textPart)
		}

		imgPart := parts[1].(map[string]any)
		if imgPart["type"] != "image_url" {
			t.Errorf("expected image_url type, got %v", imgPart["type"])
		}
		imgURL := imgPart["image_url"].(map[string]any)
		if imgURL["url"] != "https://example.com/cat.png" {
			t.Errorf("unexpected image url: %v", imgURL["url"])
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":      "chatcmpl-img",
			"object":  "chat.completion",
			"created": 1700000000,
			"model":   "gpt-4o-mini",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "A cat."},
			}},
			"usage": map[string]any{
				"prompt_tokens":     20,
				"completion_tokens": 3,
				"total_tokens":      23,
			},
		})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("test-key"),
		completions.WithBaseURL(srv.URL),
	)

	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: "gpt-4o-mini",
		Messages: []sdk.Message{{
			Role: sdk.MessageRoleUser,
			Content: []sdk.MessagePart{
				sdk.TextPart{Text: "What is in this image?"},
				sdk.ImagePart{Image: "https://example.com/cat.png", MediaType: "image/png"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("DoGenerate failed: %v", err)
	}

	if result.Text != "A cat." {
		t.Errorf("expected 'A cat.', got %q", result.Text)
	}
	if result.Usage.InputTokens != 20 {
		t.Errorf("expected 20 input tokens, got %d", result.Usage.InputTokens)
	}
}

func TestDoGenerate_ToolCall(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Tools []struct {
				Type     string `json:"type"`
				Function struct {
					Name        string `json:"name"`
					Description string `json:"description"`
					Parameters  any    `json:"parameters"`
				} `json:"function"`
			} `json:"tools"`
			ToolChoice string `json:"tool_choice"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		if len(body.Tools) != 1 {
			t.Fatalf("expected 1 tool, got %d", len(body.Tools))
		}
		if body.Tools[0].Function.Name != "get_weather" {
			t.Errorf("tool name: got %q", body.Tools[0].Function.Name)
		}
		if body.ToolChoice != "auto" {
			t.Errorf("tool_choice: got %q", body.ToolChoice)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "chatcmpl-tool", "object": "chat.completion", "model": "gpt-4o-mini",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "tool_calls",
				"message": map[string]any{
					"role":    "assistant",
					"content": "",
					"tool_calls": []map[string]any{{
						"id":   "call_abc123",
						"type": "function",
						"function": map[string]any{
							"name":      "get_weather",
							"arguments": `{"location":"Beijing"}`,
						},
					}},
				},
			}},
			"usage": map[string]any{"prompt_tokens": 20, "completion_tokens": 10, "total_tokens": 30},
		})
	}))
	defer srv.Close()

	p := completions.New(completions.WithAPIKey("test-key"), completions.WithBaseURL(srv.URL))

	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: "gpt-4o-mini",
		Messages: []sdk.Message{{
			Role:    sdk.MessageRoleUser,
			Content: []sdk.MessagePart{sdk.TextPart{Text: "What's the weather in Beijing?"}},
		}},
		Tools: []sdk.ToolDefinition{{
			Name:        "get_weather",
			Description: "Get the weather for a location",
			Parameters: &jsonschema.Schema{
				Type: "object",
				Properties: map[string]*jsonschema.Schema{
					"location": {Type: "string"},
				},
				Required: []string{"location"},
			},
		}},
		ToolChoice: sdk.ToolChoice{Mode: sdk.ToolChoiceAuto},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}

	if result.FinishReason != sdk.FinishReasonToolCalls {
		t.Errorf("finish: got %q, want %q", result.FinishReason, sdk.FinishReasonToolCalls)
	}
	if len(result.ToolCalls) != 1 {
		t.Fatalf("tool calls: got %d, want 1", len(result.ToolCalls))
	}
	tc := result.ToolCalls[0]
	if tc.ToolCallID != "call_abc123" {
		t.Errorf("tool call id: got %q", tc.ToolCallID)
	}
	if tc.ToolName != "get_weather" {
		t.Errorf("tool name: got %q", tc.ToolName)
	}
	var input map[string]any
	if err := tc.Input.Unmarshal(&input); err != nil {
		t.Fatalf("decode input: %v", err)
	}
	if input["location"] != "Beijing" {
		t.Errorf("location: got %v", input["location"])
	}
}

func TestDoGenerate_ToolCallMultiTurn(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []json.RawMessage `json:"messages"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		if len(body.Messages) != 3 {
			t.Fatalf("expected 3 messages, got %d", len(body.Messages))
		}

		// verify assistant message has tool_calls
		var assistantMsg struct {
			Role      string `json:"role"`
			ToolCalls []struct {
				ID       string `json:"id"`
				Function struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				} `json:"function"`
			} `json:"tool_calls"`
		}
		json.Unmarshal(body.Messages[1], &assistantMsg)
		if assistantMsg.Role != "assistant" {
			t.Errorf("msg[1] role: got %q", assistantMsg.Role)
		}
		if len(assistantMsg.ToolCalls) != 1 || assistantMsg.ToolCalls[0].ID != "call_abc" {
			t.Errorf("msg[1] tool_calls: %+v", assistantMsg.ToolCalls)
		}

		// verify tool result message
		var toolMsg struct {
			Role       string `json:"role"`
			ToolCallID string `json:"tool_call_id"`
			Content    string `json:"content"`
		}
		json.Unmarshal(body.Messages[2], &toolMsg)
		if toolMsg.Role != "tool" {
			t.Errorf("msg[2] role: got %q", toolMsg.Role)
		}
		if toolMsg.ToolCallID != "call_abc" {
			t.Errorf("msg[2] tool_call_id: got %q", toolMsg.ToolCallID)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "chatcmpl-2", "object": "chat.completion", "model": "gpt-4o-mini",
			"choices": []map[string]any{{
				"index": 0, "finish_reason": "stop",
				"message": map[string]any{"role": "assistant", "content": "It's sunny in Beijing."},
			}},
			"usage": map[string]any{"prompt_tokens": 30, "completion_tokens": 8, "total_tokens": 38},
		})
	}))
	defer srv.Close()

	p := completions.New(completions.WithAPIKey("test-key"), completions.WithBaseURL(srv.URL))

	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: "gpt-4o-mini",
		Messages: []sdk.Message{
			{
				Role:    sdk.MessageRoleUser,
				Content: []sdk.MessagePart{sdk.TextPart{Text: "Weather?"}},
			},
			{
				Role: sdk.MessageRoleAssistant,
				Content: []sdk.MessagePart{sdk.ToolCallPart{
					ToolCallID: "call_abc",
					ToolName:   "get_weather",
					Input:      sdk.ParseToolArguments(`{"location":"Beijing"}`),
				}},
			},
			{
				Role: sdk.MessageRoleTool,
				Content: []sdk.MessagePart{sdk.ToolResultPart{
					ToolCallID: "call_abc",
					ToolName:   "get_weather",
					Result:     sdk.ToolOutput{JSON: json.RawMessage(`{"condition":"sunny","temp":25}`)},
				}},
			},
		},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}

	if result.Text != "It's sunny in Beijing." {
		t.Errorf("text: got %q", result.Text)
	}
}

func TestDoStream_ToolCall(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		chunks := []string{
			// first chunk: tool call start with id and name
			`{"id":"chunk-1","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_xyz","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null}]}`,
			// second chunk: arguments delta
			`{"id":"chunk-1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"location\""}}]},"finish_reason":null}]}`,
			// third chunk: arguments continued
			`{"id":"chunk-1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":":\"Tokyo\"}"}}]},"finish_reason":null}]}`,
			// finish
			`{"id":"chunk-1","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	p := completions.New(completions.WithAPIKey("test-key"), completions.WithBaseURL(srv.URL))

	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model: "gpt-4o-mini",
		Messages: []sdk.Message{{
			Role:    sdk.MessageRoleUser,
			Content: []sdk.MessagePart{sdk.TextPart{Text: "Weather in Tokyo?"}},
		}},
		Tools: []sdk.ToolDefinition{{Name: "get_weather", Parameters: &jsonschema.Schema{Type: "object"}}},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	var (
		gotInputStart bool
		gotInputEnd   bool
		argsDelta     string
		gotToolCall   *sdk.StreamToolCallPart
		gotFinishStep bool
		gotFinish     bool
	)

	for part := range sr {
		switch p := part.(type) {
		case *sdk.ToolInputStartPart:
			gotInputStart = true
			if p.ToolName != "get_weather" {
				t.Errorf("input start tool name: got %q", p.ToolName)
			}
		case *sdk.ToolInputDeltaPart:
			argsDelta += p.Delta
		case *sdk.ToolInputEndPart:
			gotInputEnd = true
		case *sdk.StreamToolCallPart:
			gotToolCall = p
		case *sdk.FinishStepPart:
			gotFinishStep = true
			if p.FinishReason != sdk.FinishReasonToolCalls {
				t.Errorf("finish step reason: got %q", p.FinishReason)
			}
		case *sdk.FinishPart:
			gotFinish = true
		case *sdk.ErrorPart:
			t.Fatalf("error: %v", p.Error)
		}
	}

	if !gotInputStart {
		t.Error("missing ToolInputStartPart")
	}
	if !gotInputEnd {
		t.Error("missing ToolInputEndPart")
	}
	if argsDelta != `{"location":"Tokyo"}` {
		t.Errorf("args delta: got %q", argsDelta)
	}
	if gotToolCall == nil {
		t.Fatal("missing StreamToolCallPart")
	} else if gotToolCall.ToolCallID != "call_xyz" || gotToolCall.ToolName != "get_weather" {
		t.Errorf("tool call: %+v", gotToolCall)
	}
	var input map[string]any
	if err := gotToolCall.Input.Unmarshal(&input); err != nil || input["location"] != "Tokyo" {
		t.Errorf("tool call input: %+v", gotToolCall.Input)
	}
	if !gotFinishStep {
		t.Error("missing FinishStepPart")
	}
	if !gotFinish {
		t.Error("missing FinishPart")
	}
}

func TestDoGenerate_NoModel(t *testing.T) {
	p := completions.New(completions.WithAPIKey("k"))
	_, err := p.DoGenerate(context.Background(), sdk.Request{})
	if err == nil {
		t.Fatal("expected error for nil model")
	}
}

func TestDoStream_NoModel(t *testing.T) {
	p := completions.New(completions.WithAPIKey("k"))
	_, err := p.DoStream(context.Background(), sdk.Request{})
	if err == nil {
		t.Fatal("expected error for nil model")
	}
}

// ---------- reasoning tests ----------

func TestDoGenerate_ReasoningContent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "chatcmpl-r", "model": "deepseek-r1",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message": map[string]any{
					"role":              "assistant",
					"content":           "The answer is 4.",
					"reasoning_content": "Let me think... 2+2=4",
				},
			}},
			"usage": map[string]any{"prompt_tokens": 5, "completion_tokens": 10, "total_tokens": 15},
		})
	}))
	defer srv.Close()

	p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL))
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "deepseek-r1",
		Messages: []sdk.Message{sdk.UserMessage("2+2?")},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	if result.Text != "The answer is 4." {
		t.Errorf("text: got %q", result.Text)
	}
	if result.Reasoning != "Let me think... 2+2=4" {
		t.Errorf("reasoning: got %q", result.Reasoning)
	}
}

func TestDoGenerate_ReasoningFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "chatcmpl-rf", "model": "gpt-oss",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message": map[string]any{
					"role":      "assistant",
					"content":   "42",
					"reasoning": "Thinking via reasoning field...",
				},
			}},
			"usage": map[string]any{"prompt_tokens": 5, "completion_tokens": 3, "total_tokens": 8},
		})
	}))
	defer srv.Close()

	p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL))
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "gpt-oss",
		Messages: []sdk.Message{sdk.UserMessage("answer")},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	if result.Reasoning != "Thinking via reasoning field..." {
		t.Errorf("reasoning fallback: got %q", result.Reasoning)
	}
}

func TestDoGenerate_DeepSeekCompatDisablesThinking(t *testing.T) {
	var body struct {
		ReasoningEffort *string `json:"reasoning_effort"`
		Thinking        *struct {
			Type string `json:"type"`
		} `json:"thinking"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":    "chatcmpl-deepseek",
			"model": "deepseek-v4-flash",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "ok"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL(srv.URL),
		completions.WithDeepSeekChatCompletionsCompat(),
	)
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:           "deepseek-v4-flash",
		Messages:        []sdk.Message{sdk.UserMessage("hi")},
		ReasoningEffort: stringPtr("none"),
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	if body.ReasoningEffort != nil {
		t.Fatalf("reasoning_effort should be omitted, got %q", *body.ReasoningEffort)
	}
	if body.Thinking == nil || body.Thinking.Type != "disabled" {
		t.Fatalf("thinking: got %#v, want disabled", body.Thinking)
	}
}

func TestDoGenerate_DeepSeekCompatLeavesOtherReasoningEffortAlone(t *testing.T) {
	var body struct {
		ReasoningEffort *string `json:"reasoning_effort"`
		Thinking        *struct {
			Type string `json:"type"`
		} `json:"thinking"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":    "chatcmpl-deepseek",
			"model": "deepseek-v4-pro",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "ok"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL(srv.URL),
		completions.WithDeepSeekChatCompletionsCompat(),
	)
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:           "deepseek-v4-pro",
		Messages:        []sdk.Message{sdk.UserMessage("hi")},
		ReasoningEffort: stringPtr("xhigh"),
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	if body.ReasoningEffort == nil || *body.ReasoningEffort != "xhigh" {
		t.Fatalf("reasoning_effort: got %v, want xhigh", body.ReasoningEffort)
	}
	if body.Thinking != nil {
		t.Fatalf("thinking should be omitted, got %#v", body.Thinking)
	}
}

// TestDoGenerate_ForwardsMaxReasoningEffortVerbatim pins that the SDK no longer
// rewrites "max" into "xhigh". Effort is an open enum, so an unsupported tier is
// the endpoint's 400 to return rather than the SDK's to silently downgrade.
func TestDoGenerate_ForwardsMaxReasoningEffortVerbatim(t *testing.T) {
	var body struct {
		ReasoningEffort *string `json:"reasoning_effort"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":    "chatcmpl-reasoning",
			"model": "gpt-5.2",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "ok"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	defer srv.Close()

	p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:           "gpt-5.2",
		Messages:        []sdk.Message{sdk.UserMessage("hi")},
		ReasoningEffort: stringPtr("max"),
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	if body.ReasoningEffort == nil || *body.ReasoningEffort != "max" {
		t.Fatalf("reasoning_effort: got %v, want max (forwarded verbatim)", body.ReasoningEffort)
	}
}

func TestDoStream_ReasoningFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		chunks := []string{
			`{"id":"c1","choices":[{"index":0,"delta":{"role":"assistant","reasoning":"Think"},"finish_reason":null}]}`,
			`{"id":"c1","choices":[{"index":0,"delta":{"reasoning":"ing..."},"finish_reason":null}]}`,
			`{"id":"c1","choices":[{"index":0,"delta":{"content":"Done"},"finish_reason":null}]}`,
			`{"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "gpt-oss",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	var reasoning, text string
	for part := range sr {
		switch p := part.(type) {
		case *sdk.ReasoningDeltaPart:
			reasoning += p.Text
		case *sdk.TextDeltaPart:
			text += p.Text
		case *sdk.ErrorPart:
			t.Fatalf("error: %v", p.Error)
		}
	}
	if reasoning != "Thinking..." {
		t.Errorf("reasoning fallback: got %q", reasoning)
	}
	if text != "Done" {
		t.Errorf("text: got %q", text)
	}
}

func TestDoStream_ReasoningClosedBeforeToolCall(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		chunks := []string{
			`{"id":"c1","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"Let me think..."},"finish_reason":null}]}`,
			`{"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"search","arguments":""}}]},"finish_reason":null}]}`,
			`{"id":"c1","choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"q\":\"test\"}"}}]},"finish_reason":null}]}`,
			`{"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":5,"completion_tokens":10,"total_tokens":15}}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "deepseek-r1",
		Messages: []sdk.Message{sdk.UserMessage("search")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	events := make([]sdk.StreamPartType, 0, 8)
	for part := range sr {
		events = append(events, part.Type())
	}

	reasoningEndIdx := -1
	toolInputStartIdx := -1
	for i, ev := range events {
		if ev == sdk.StreamPartTypeReasoningEnd && reasoningEndIdx == -1 {
			reasoningEndIdx = i
		}
		if ev == sdk.StreamPartTypeToolInputStart && toolInputStartIdx == -1 {
			toolInputStartIdx = i
		}
	}

	if reasoningEndIdx == -1 {
		t.Fatal("missing reasoning-end event")
	}
	if toolInputStartIdx == -1 {
		t.Fatal("missing tool-input-start event")
	}
	if reasoningEndIdx >= toolInputStartIdx {
		t.Errorf("reasoning-end (idx %d) should come before tool-input-start (idx %d)", reasoningEndIdx, toolInputStartIdx)
	}
}

func TestDoStream_FlushOnAbruptEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		chunks := []string{
			`{"id":"c1","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"Thinking..."},"finish_reason":null}]}`,
			`{"id":"c1","choices":[{"index":0,"delta":{"content":"partial"},"finish_reason":null}]}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
	}))
	defer srv.Close()

	p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "m",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	var gotReasoningEnd, gotTextEnd, gotFinish bool
	for part := range sr {
		switch part.(type) {
		case *sdk.ReasoningEndPart:
			gotReasoningEnd = true
		case *sdk.TextEndPart:
			gotTextEnd = true
		case *sdk.FinishPart:
			gotFinish = true
		}
	}

	if !gotReasoningEnd {
		t.Error("missing ReasoningEndPart on abrupt stream end")
	}
	if !gotTextEnd {
		t.Error("missing TextEndPart on abrupt stream end")
	}
	if !gotFinish {
		t.Error("missing FinishPart on abrupt stream end")
	}
}

func TestDoGenerate_AssistantReasoningInRequest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []json.RawMessage `json:"messages"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		if len(body.Messages) != 2 {
			t.Fatalf("expected 2 messages, got %d", len(body.Messages))
		}

		var assistantMsg struct {
			Role             string `json:"role"`
			Content          string `json:"content"`
			ReasoningContent string `json:"reasoning_content"`
		}
		json.Unmarshal(body.Messages[1], &assistantMsg)
		if assistantMsg.Role != "assistant" {
			t.Errorf("msg[1] role: got %q", assistantMsg.Role)
		}
		if assistantMsg.ReasoningContent != "I thought about it" {
			t.Errorf("msg[1] reasoning_content: got %q", assistantMsg.ReasoningContent)
		}
		if assistantMsg.Content != "The answer" {
			t.Errorf("msg[1] content: got %q", assistantMsg.Content)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "chatcmpl-rr", "model": "m",
			"choices": []map[string]any{{
				"index": 0, "finish_reason": "stop",
				"message": map[string]any{"role": "assistant", "content": "OK"},
			}},
			"usage": map[string]any{"prompt_tokens": 10, "completion_tokens": 1, "total_tokens": 11},
		})
	}))
	defer srv.Close()

	p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: "m",
		Messages: []sdk.Message{
			sdk.UserMessage("question"),
			{
				Role: sdk.MessageRoleAssistant,
				Content: []sdk.MessagePart{
					sdk.TextPart{Text: "The answer"},
					sdk.ReasoningPart{Text: "I thought about it", Format: sdk.ReasoningFormatOpenAIChat},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
}

func TestDoStream_EarlyToolCallDetection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		chunks := []string{
			`{"id":"c1","choices":[{"index":0,"delta":{"role":"assistant","tool_calls":[{"index":0,"id":"call_full","type":"function","function":{"name":"get_time","arguments":"{}"}}]},"finish_reason":null}]}`,
			`{"id":"c1","choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}],"usage":{"prompt_tokens":3,"completion_tokens":5,"total_tokens":8}}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	p := completions.New(completions.WithAPIKey("k"), completions.WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "m",
		Messages: []sdk.Message{sdk.UserMessage("time?")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	events := make([]sdk.StreamPartType, 0, 8)
	var toolCallCount int
	for part := range sr {
		events = append(events, part.Type())
		if part.Type() == sdk.StreamPartTypeToolCall {
			toolCallCount++
		}
	}

	if toolCallCount != 1 {
		t.Errorf("expected exactly 1 tool-call event, got %d", toolCallCount)
	}

	inputEndCount := 0
	for _, ev := range events {
		if ev == sdk.StreamPartTypeToolInputEnd {
			inputEndCount++
		}
	}
	if inputEndCount != 1 {
		t.Errorf("expected exactly 1 tool-input-end event, got %d", inputEndCount)
	}
}

// ---------- integration tests (real API, skipped without env) ----------

func envOrSkip(t *testing.T, key string) string {
	t.Helper()
	v := os.Getenv(key)
	if v == "" {
		t.Skipf("skipping: %s not set", key)
	}
	return v
}

func newIntegrationProvider(t *testing.T) *completions.Provider {
	t.Helper()
	apiKey := envOrSkip(t, "OPENAI_API_KEY")
	opts := []completions.Option{completions.WithAPIKey(apiKey)}
	if base := os.Getenv("OPENAI_BASE_URL"); base != "" {
		opts = append(opts, completions.WithBaseURL(base))
	}
	return completions.New(opts...)
}

func integrationModel(t *testing.T) *sdk.Model {
	t.Helper()
	m := os.Getenv("OPENAI_MODEL")
	if m == "" {
		m = "gpt-4o-mini"
	}
	return &sdk.Model{ID: m}
}

func TestIntegration_DoGenerate(t *testing.T) {
	p := newIntegrationProvider(t)
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: integrationModel(t).ID,
		Messages: []sdk.Message{{
			Role:    sdk.MessageRoleUser,
			Content: []sdk.MessagePart{sdk.TextPart{Text: "Say hello in one word."}},
		}},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	t.Logf("text=%q finish=%s tokens=%d/%d", result.Text, result.FinishReason,
		result.Usage.InputTokens, result.Usage.OutputTokens)

	if result.Text == "" {
		t.Error("expected non-empty text")
	}
}

func TestIntegration_DoStream(t *testing.T) {
	p := newIntegrationProvider(t)
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model: integrationModel(t).ID,
		Messages: []sdk.Message{{
			Role:    sdk.MessageRoleUser,
			Content: []sdk.MessagePart{sdk.TextPart{Text: "Count from 1 to 5."}},
		}},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	var text string
	for part := range sr {
		switch p := part.(type) {
		case *sdk.TextDeltaPart:
			text += p.Text
			t.Logf("text delta: %q", p.Text)
		case *sdk.ErrorPart:
			t.Fatalf("stream error: %v", p.Error)
		case *sdk.FinishPart:
			t.Logf("finish=%s", p.FinishReason)
		}
	}
	t.Logf("streamed text: %q", text)
	if text == "" {
		t.Error("expected non-empty streamed text")
	}
}

// ---------- multi-model integration tests (OpenRouter) ----------

const (
	openRouterGoogleIntegrationModel = "google/gemini-3.6-flash"
	// The proxy's plain-chat DeepSeek; "deepseek/deepseek-chat" is no longer
	// routed there (404 model_not_found).
	openRouterDeepSeekChatIntegrationModel = "deepseek/deepseek-v4-flash"
)

func TestIntegration_MultiModel(t *testing.T) {
	p := newIntegrationProvider(t)

	models := []struct {
		id           string
		hasReasoning bool
	}{
		{openRouterGoogleIntegrationModel, false},
		{"deepseek/deepseek-r1", true},
		{openRouterDeepSeekChatIntegrationModel, false},
	}

	for _, m := range models {
		t.Run(m.id, func(t *testing.T) {
			model := &sdk.Model{ID: m.id}
			result, err := p.DoGenerate(context.Background(), sdk.Request{
				Model:    model.ID,
				Messages: []sdk.Message{sdk.UserMessage("What is 2+3? Answer with just the number.")},
			})
			if err != nil {
				t.Fatalf("DoGenerate: %v", err)
			}
			t.Logf("text=%q reasoning=%q finish=%s tokens=in:%d/out:%d/reasoning:%d",
				result.Text, truncate(result.Reasoning, 80), result.FinishReason,
				result.Usage.InputTokens, result.Usage.OutputTokens, result.Usage.ReasoningTokens)

			if result.Text == "" {
				t.Error("expected non-empty text")
			}
			if m.hasReasoning && result.Reasoning == "" {
				t.Error("expected non-empty reasoning for reasoning model")
			}
		})
	}
}

func TestIntegration_MultiModel_Stream(t *testing.T) {
	p := newIntegrationProvider(t)

	models := []struct {
		id           string
		hasReasoning bool
	}{
		{openRouterGoogleIntegrationModel, false},
		{"deepseek/deepseek-r1", true},
	}

	for _, m := range models {
		t.Run(m.id, func(t *testing.T) {
			model := &sdk.Model{ID: m.id}
			sr, err := p.DoStream(context.Background(), sdk.Request{
				Model:    model.ID,
				Messages: []sdk.Message{sdk.UserMessage("What is 2+3? Answer with just the number.")},
			})
			if err != nil {
				t.Fatalf("DoStream: %v", err)
			}

			var text, reasoning string
			var gotReasoningStart, gotReasoningEnd bool
			for part := range sr {
				switch p := part.(type) {
				case *sdk.ReasoningStartPart:
					gotReasoningStart = true
				case *sdk.ReasoningDeltaPart:
					reasoning += p.Text
				case *sdk.ReasoningEndPart:
					gotReasoningEnd = true
				case *sdk.TextDeltaPart:
					text += p.Text
				case *sdk.ErrorPart:
					t.Fatalf("stream error: %v", p.Error)
				case *sdk.FinishPart:
					t.Logf("finish=%s", p.FinishReason)
				}
			}
			t.Logf("text=%q reasoning=%q (len=%d)", text, truncate(reasoning, 80), len(reasoning))

			if text == "" {
				t.Error("expected non-empty text")
			}
			if m.hasReasoning {
				if reasoning == "" {
					t.Error("expected non-empty reasoning")
				}
				if !gotReasoningStart {
					t.Error("missing ReasoningStartPart")
				}
				if !gotReasoningEnd {
					t.Error("missing ReasoningEndPart")
				}
			}
		})
	}
}

func TestIntegration_Reasoning_ToolCall(t *testing.T) {
	p := newIntegrationProvider(t)
	// The reasoning assertions need a model that emits reasoning, so this case
	// reads OPENAI_MODEL like every other integration case: point it at a
	// reasoning model (deepseek-reasoner, o4-mini, ...) to exercise them, and
	// the tool-call assertions run against whatever the endpoint serves.
	model := integrationModel(t)

	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    model.ID,
		Messages: []sdk.Message{sdk.UserMessage("What's the weather in Tokyo right now?")},
		Tools: []sdk.ToolDefinition{{
			Name:        "get_weather",
			Description: "Get the current weather for a city",
			Parameters: &jsonschema.Schema{
				Type: "object",
				Properties: map[string]*jsonschema.Schema{
					"city": {Type: "string", Description: "City name"},
				},
				Required: []string{"city"},
			},
		}},
		ToolChoice: sdk.ToolChoice{Mode: sdk.ToolChoiceAuto},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}

	t.Logf("text=%q reasoning=%q (len=%d) finish=%s toolCalls=%d",
		truncate(result.Text, 80), truncate(result.Reasoning, 80),
		len(result.Reasoning), result.FinishReason, len(result.ToolCalls))

	if result.Reasoning == "" {
		t.Log("warning: no reasoning returned (model may not emit reasoning with tool calls)")
	}
	if len(result.ToolCalls) > 0 {
		for _, tc := range result.ToolCalls {
			t.Logf("  tool=%q id=%s input=%v", tc.ToolName, tc.ToolCallID, tc.Input)
		}
	} else if result.Text == "" {
		t.Error("expected either tool calls or text response")
	}
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// ---------- ListModels / Test / TestModel unit tests ----------

func TestListModels(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Method != http.MethodGet {
			t.Errorf("expected GET, got %s", r.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"id": "gpt-4o", "object": "model", "owned_by": "openai"},
				{"id": "gpt-4o-mini", "object": "model", "owned_by": "openai"},
			},
		})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("test-key"),
		completions.WithBaseURL(srv.URL),
	)

	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels failed: %v", err)
	}
	if len(models) != 2 {
		t.Fatalf("expected 2 models, got %d", len(models))
	}
	if models[0].ID != "gpt-4o" {
		t.Errorf("expected first model 'gpt-4o', got %q", models[0].ID)
	}
	if models[1].ID != "gpt-4o-mini" {
		t.Errorf("expected second model 'gpt-4o-mini', got %q", models[1].ID)
	}
}

func TestProviderTest_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{},
		})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("test-key"),
		completions.WithBaseURL(srv.URL),
	)

	result := p.Test(context.Background())
	if result.Status != sdk.ProviderStatusOK {
		t.Errorf("expected status OK, got %q", result.Status)
	}
}

func TestProviderTest_Unhealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "invalid api key"}})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("bad-key"),
		completions.WithBaseURL(srv.URL),
	)

	result := p.Test(context.Background())
	if result.Status != sdk.ProviderStatusUnhealthy {
		t.Errorf("expected status Unhealthy, got %q", result.Status)
	}
}

func TestProviderTest_Unreachable(t *testing.T) {
	p := completions.New(
		completions.WithAPIKey("test-key"),
		completions.WithBaseURL("http://127.0.0.1:1"),
	)

	result := p.Test(context.Background())
	if result.Status != sdk.ProviderStatusUnreachable {
		t.Errorf("expected status Unreachable, got %q", result.Status)
	}
}

func TestTestModel_Supported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models/gpt-4o" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "gpt-4o", "object": "model",
		})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("test-key"),
		completions.WithBaseURL(srv.URL),
	)

	result, err := p.TestModel(context.Background(), "gpt-4o")
	if err != nil {
		t.Fatalf("TestModel failed: %v", err)
	}
	if !result.Supported {
		t.Error("expected model to be supported")
	}
}

func TestTestModel_NotSupported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "model not found"}})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("test-key"),
		completions.WithBaseURL(srv.URL),
	)

	result, err := p.TestModel(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("TestModel failed: %v", err)
	}
	if result.Supported {
		t.Error("expected model to not be supported")
	}
}

func TestDoGenerate_MiniMaxCompatDisablesThinkingAndSplitsReasoning(t *testing.T) {
	var body struct {
		ReasoningEffort *string `json:"reasoning_effort"`
		ReasoningSplit  bool    `json:"reasoning_split"`
		Thinking        *struct {
			Type string `json:"type"`
		} `json:"thinking"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":    "chatcmpl-minimax",
			"model": "MiniMax-M3",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "ok"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL(srv.URL),
		completions.WithMiniMaxChatCompletionsCompat(),
	)
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:           "MiniMax-M3",
		Messages:        []sdk.Message{sdk.UserMessage("hi")},
		ReasoningEffort: stringPtr("none"),
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	if !body.ReasoningSplit {
		t.Fatal("expected reasoning_split=true")
	}
	if body.ReasoningEffort != nil {
		t.Fatalf("reasoning_effort should be omitted, got %q", *body.ReasoningEffort)
	}
	if body.Thinking == nil || body.Thinking.Type != "disabled" {
		t.Fatalf("thinking: got %#v, want disabled", body.Thinking)
	}
}

func TestDoGenerate_MiniMaxCompatMapsReasoningEffortToAdaptiveThinking(t *testing.T) {
	var body struct {
		ReasoningEffort *string `json:"reasoning_effort"`
		ReasoningSplit  bool    `json:"reasoning_split"`
		Thinking        *struct {
			Type string `json:"type"`
		} `json:"thinking"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":    "chatcmpl-minimax",
			"model": "MiniMax-M3",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "ok"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL(srv.URL),
		completions.WithMiniMaxChatCompletionsCompat(),
	)
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:           "MiniMax-M3",
		Messages:        []sdk.Message{sdk.UserMessage("hi")},
		ReasoningEffort: stringPtr("high"),
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	if !body.ReasoningSplit {
		t.Fatal("expected reasoning_split=true")
	}
	if body.ReasoningEffort != nil {
		t.Fatalf("reasoning_effort should be omitted, got %q", *body.ReasoningEffort)
	}
	if body.Thinking == nil || body.Thinking.Type != "adaptive" {
		t.Fatalf("thinking: got %#v, want adaptive", body.Thinking)
	}
}

func TestGenerate_MiniMaxReasoningDetailsPreserved(t *testing.T) {
	var call int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call++
		if call == 2 {
			var body struct {
				Messages []json.RawMessage `json:"messages"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode request body: %v", err)
			}
			if len(body.Messages) < 2 {
				t.Fatalf("expected assistant history in second request, got %d messages", len(body.Messages))
			}
			var assistantMsg struct {
				ReasoningContent string           `json:"reasoning_content"`
				ReasoningDetails []map[string]any `json:"reasoning_details"`
			}
			if err := json.Unmarshal(body.Messages[1], &assistantMsg); err != nil {
				t.Fatalf("decode assistant history: %v", err)
			}
			if assistantMsg.ReasoningContent != "" {
				t.Fatalf("reasoning_content should be omitted for MiniMax history, got %q", assistantMsg.ReasoningContent)
			}
			if len(assistantMsg.ReasoningDetails) != 1 || assistantMsg.ReasoningDetails[0]["text"] != "Let me think" {
				t.Fatalf("reasoning_details: got %#v", assistantMsg.ReasoningDetails)
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":    "chatcmpl-minimax",
			"model": "MiniMax-M3",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message": map[string]any{
					"role":              "assistant",
					"content":           "ok",
					"reasoning_content": "legacy reasoning",
					"reasoning_details": []map[string]any{{
						"type":   "reasoning.text",
						"id":     "reasoning-text-1",
						"format": "MiniMax-response-v1",
						"index":  0,
						"text":   "Let me think",
					}},
				},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL(srv.URL),
		completions.WithMiniMaxChatCompletionsCompat(),
	)
	model := p.ChatModel("MiniMax-M3")
	result, err := model.Generate(context.Background(), sdk.Request{Messages: []sdk.Message{sdk.UserMessage("hi")}})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if result.Reasoning != "Let me think" {
		t.Fatalf("reasoning: got %q", result.Reasoning)
	}
	// The caller assembles the step's messages and replays them: the reasoning
	// details must come back on the assistant message of the second request.
	history := make([]sdk.Message, 0, 4)
	history = append(history, sdk.UserMessage("hi"))
	history = append(history, assistantStep(&result))
	history = append(history, sdk.UserMessage("continue"))
	if _, err := model.Generate(context.Background(), sdk.Request{Messages: history}); err != nil {
		t.Fatalf("second Generate: %v", err)
	}
}

func TestDoStream_MiniMaxReasoningDetails(t *testing.T) {
	var reasoningSplit bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ReasoningSplit bool `json:"reasoning_split"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		reasoningSplit = body.ReasoningSplit

		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		chunks := []string{
			`{"id":"c1","model":"MiniMax-M3","choices":[{"index":0,"delta":{"role":"assistant","reasoning_content":"legacy","reasoning_details":[{"type":"reasoning.text","id":"reasoning-text-1","format":"MiniMax-response-v1","index":0,"text":"Let"}]},"finish_reason":null}]}`,
			`{"id":"c1","model":"MiniMax-M3","choices":[{"index":0,"delta":{"reasoning_content":" fallback","reasoning_details":[{"type":"reasoning.text","id":"reasoning-text-1","format":"MiniMax-response-v1","index":0,"text":" me think"}]},"finish_reason":null}]}`,
			`{"id":"c1","model":"MiniMax-M3","choices":[{"index":0,"delta":{"content":"The answer"},"finish_reason":null}]}`,
			`{"id":"c1","model":"MiniMax-M3","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":4,"total_tokens":7}}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
		fmt.Fprintf(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL(srv.URL),
		completions.WithMiniMaxChatCompletionsCompat(),
	)
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "MiniMax-M3",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	var reasoning, text string
	var reasoningMeta sdk.ProviderMetadata
	for part := range sr {
		switch p := part.(type) {
		case *sdk.ReasoningDeltaPart:
			reasoning += p.Text
		case *sdk.ReasoningEndPart:
			reasoningMeta = p.ProviderMetadata
		case *sdk.TextDeltaPart:
			text += p.Text
		case *sdk.ErrorPart:
			t.Fatalf("error: %v", p.Error)
		}
	}
	if !reasoningSplit {
		t.Fatal("expected reasoning_split=true on the streaming request")
	}
	if reasoning != "Let me think" {
		t.Errorf("reasoning: got %q", reasoning)
	}
	details := minimaxReasoningDetailsFromTestMetadata(t, reasoningMeta)
	if len(details) != 1 || details[0]["text"] != "Let me think" {
		t.Errorf("reasoning metadata details: got %#v", details)
	}
	if text != "The answer" {
		t.Errorf("text: got %q", text)
	}
}

func minimaxReasoningDetailsFromTestMetadata(t *testing.T, meta sdk.ProviderMetadata) []map[string]any {
	t.Helper()
	raw := meta.Get("minimax", "reasoning_details")
	if raw == "" {
		t.Fatalf("expected minimax metadata, got %#v", meta)
	}
	var details []map[string]any
	if err := json.Unmarshal([]byte(raw), &details); err != nil {
		t.Fatalf("unmarshal reasoning_details metadata: %v", err)
	}
	return details
}

func stringPtr(s string) *string { return &s }

// ---------- Kimi/Moonshot compat ----------

// kimiAnyOfTool is a tool whose Parameters schema puts "type" on the parent
// of an anyOf, the pattern standard JSON Schema allows but Moonshot/Kimi
// rejects with "tools.function.parameters is not a valid moonshot flavored
// json schema".
func kimiAnyOfTool() sdk.ToolDefinition {
	return sdk.ToolDefinition{
		Name:        "attach_file",
		Description: "Attach a file",
		Parameters: &jsonschema.Schema{
			Type: "object",
			Properties: map[string]*jsonschema.Schema{
				"attachments": {
					Type: "array",
					Items: &jsonschema.Schema{
						Type: "object",
						AnyOf: []*jsonschema.Schema{
							{Properties: map[string]*jsonschema.Schema{"url": {Type: "string"}}},
							{Properties: map[string]*jsonschema.Schema{"data": {Type: "string"}}},
						},
					},
				},
			},
			Required: []string{"attachments"},
		},
	}
}

func kimiMemohAttachmentTool() sdk.ToolDefinition {
	return sdk.ToolDefinition{
		Name:        "send_message",
		Description: "Send attachments",
		Parameters: mustSchema(map[string]any{
			"type": "object",
			"properties": map[string]any{
				"attachments": map[string]any{
					"type": "array",
					"items": map[string]any{
						"anyOf": []any{
							map[string]any{"type": "string"},
							map[string]any{
								"type":                 "object",
								"additionalProperties": false,
								"anyOf": []any{
									map[string]any{"required": []string{"path"}},
									map[string]any{"required": []string{"url"}},
									map[string]any{"required": []string{"base64"}},
									map[string]any{"required": []string{"content_hash"}},
									map[string]any{"required": []string{"platform_key"}},
								},
								"properties": map[string]any{
									"path":         map[string]any{"type": "string"},
									"url":          map[string]any{"type": "string"},
									"base64":       map[string]any{"type": "string"},
									"content_hash": map[string]any{"type": "string"},
									"platform_key": map[string]any{"type": "string"},
									"metadata":     map[string]any{"type": "object"},
								},
							},
						},
					},
				},
			},
			"required": []string{"attachments"},
		}),
	}
}

func decodeToolParameters(t *testing.T, r *http.Request) (map[string]any, bool) {
	t.Helper()
	var body struct {
		Stream bool `json:"stream"`
		Tools  []struct {
			Function struct {
				Parameters map[string]any `json:"parameters"`
			} `json:"function"`
		} `json:"tools"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	if len(body.Tools) != 1 {
		t.Fatalf("expected 1 tool, got %d", len(body.Tools))
	}
	return body.Tools[0].Function.Parameters, body.Stream
}

// assertKimiSanitizedAnyOf checks that the "attachments.items" schema no
// longer has "type" alongside "anyOf", and that each anyOf branch has its
// own "type": "object".
func assertKimiSanitizedAnyOf(t *testing.T, params map[string]any) {
	t.Helper()
	items := schemaPath(t, params, "properties", "attachments", "items")
	if _, hasType := items["type"]; hasType {
		t.Fatalf("items schema should not have a parent \"type\" alongside anyOf, got %#v", items)
	}
	anyOf, ok := items["anyOf"].([]any)
	if !ok || len(anyOf) != 2 {
		t.Fatalf("expected anyOf with 2 branches, got %#v", items["anyOf"])
	}
	for i, branch := range anyOf {
		branchMap, ok := branch.(map[string]any)
		if !ok {
			t.Fatalf("anyOf[%d] is not an object: %#v", i, branch)
		}
		if branchMap["type"] != "object" {
			t.Errorf("anyOf[%d].type: got %v, want %q", i, branchMap["type"], "object")
		}
	}
}

func assertKimiNormalizedMemohAttachments(t *testing.T, params map[string]any) {
	t.Helper()
	items := schemaPath(t, params, "properties", "attachments", "items")
	outerAnyOf, ok := items["anyOf"].([]any)
	if !ok || len(outerAnyOf) != 2 {
		t.Fatalf("attachments.items.anyOf = %#v, want two branches", items["anyOf"])
	}
	objectBranch, ok := outerAnyOf[1].(map[string]any)
	if !ok {
		t.Fatalf("attachments object branch = %T, want map[string]any", outerAnyOf[1])
	}
	for _, key := range []string{"type", "properties", "required", "additionalProperties"} {
		if _, exists := objectBranch[key]; exists {
			t.Fatalf("attachments object parent still contains %q: %#v", key, objectBranch)
		}
	}
	innerAnyOf, ok := objectBranch["anyOf"].([]any)
	if !ok || len(innerAnyOf) != 5 {
		t.Fatalf("attachments object anyOf = %#v, want five branches", objectBranch["anyOf"])
	}
	for index, rawBranch := range innerAnyOf {
		branch, ok := rawBranch.(map[string]any)
		if !ok {
			t.Fatalf("attachments object anyOf[%d] = %T, want map[string]any", index, rawBranch)
		}
		if branch["type"] != "object" || branch["additionalProperties"] != false {
			t.Fatalf("attachments object anyOf[%d] lacks object constraints: %#v", index, branch)
		}
		properties, ok := branch["properties"].(map[string]any)
		if !ok {
			t.Fatalf("attachments object anyOf[%d].properties = %T", index, branch["properties"])
		}
		required, ok := branch["required"].([]any)
		if !ok || len(required) != 1 {
			t.Fatalf("attachments object anyOf[%d].required = %#v", index, branch["required"])
		}
		name, ok := required[0].(string)
		if !ok {
			t.Fatalf("attachments object anyOf[%d].required[0] = %#v", index, required[0])
		}
		if _, exists := properties[name]; !exists {
			t.Fatalf("attachments object anyOf[%d] requires undefined property %q", index, name)
		}
	}
}

func schemaPath(t *testing.T, m map[string]any, path ...string) map[string]any {
	t.Helper()
	cur := m
	for _, key := range path {
		next, ok := cur[key].(map[string]any)
		if !ok {
			t.Fatalf("schema path %v: %q not found or not an object in %#v", path, key, cur)
		}
		cur = next
	}
	return cur
}

func newEchoToolsServer(t *testing.T, capture *map[string]any) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var stream bool
		*capture, stream = decodeToolParameters(t, r)
		if stream {
			w.Header().Set("Content-Type", "text/event-stream")
			fmt.Fprint(w, "data: [DONE]\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":    "chatcmpl-kimi",
			"model": "kimi-k2",
			"choices": []map[string]any{{
				"index":         0,
				"finish_reason": "stop",
				"message":       map[string]any{"role": "assistant", "content": "ok"},
			}},
			"usage": map[string]any{"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
		})
	}))
}

func TestDoGenerate_KimiCompatSanitizesAnyOfSchema(t *testing.T) {
	var params map[string]any
	srv := newEchoToolsServer(t, &params)
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL(srv.URL),
		completions.WithKimiChatCompletionsCompat(),
	)
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "kimi-k2",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
		Tools:    []sdk.ToolDefinition{kimiAnyOfTool()},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	assertKimiSanitizedAnyOf(t, params)
}

func TestDoGenerate_KimiCompatNormalizesMemohAttachmentSchema(t *testing.T) {
	var params map[string]any
	srv := newEchoToolsServer(t, &params)
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL(srv.URL),
		completions.WithKimiChatCompletionsCompat(),
	)
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "kimi-k2",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
		Tools:    []sdk.ToolDefinition{kimiMemohAttachmentTool()},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	assertKimiNormalizedMemohAttachments(t, params)
}

func TestDoStream_KimiCompatNormalizesMemohAttachmentSchema(t *testing.T) {
	var params map[string]any
	srv := newEchoToolsServer(t, &params)
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL(srv.URL),
		completions.WithKimiChatCompletionsCompat(),
	)
	result, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "kimi-k2",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
		Tools:    []sdk.ToolDefinition{kimiMemohAttachmentTool()},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	for part := range result {
		if errorPart, ok := part.(*sdk.ErrorPart); ok {
			t.Fatalf("stream error: %v", errorPart.Error)
		}
	}
	assertKimiNormalizedMemohAttachments(t, params)
}

// redirectingTransport rewrites every outgoing request's scheme/host to
// point at a local test server while leaving the rest of the request (path,
// body, etc.) untouched. This lets a test configure WithBaseURL with a real
// Moonshot-looking host (so Kimi auto-detection can key off it) while still
// serving the request from an in-process httptest.Server.
type redirectingTransport struct {
	targetURL *url.URL
}

func (rt *redirectingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme = rt.targetURL.Scheme
	req.URL.Host = rt.targetURL.Host
	req.Host = rt.targetURL.Host
	return http.DefaultTransport.RoundTrip(req)
}

// TestKimiCompatRequiresExplicitOption verifies that a Moonshot-looking base
// URL alone does not select a wire dialect. Callers must opt into Kimi/MFJS
// behavior explicitly.
func TestKimiCompatRequiresExplicitOption(t *testing.T) {
	var params map[string]any
	srv := newEchoToolsServer(t, &params)
	defer srv.Close()

	target, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}

	p := completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL("https://api.moonshot.cn/v1"),
		completions.WithHTTPClient(&http.Client{Transport: &redirectingTransport{targetURL: target}}),
	)
	_, err = p.DoGenerate(context.Background(), sdk.Request{
		Model:    "kimi-k2",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
		Tools:    []sdk.ToolDefinition{kimiAnyOfTool()},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}

	items := schemaPath(t, params, "properties", "attachments", "items")
	if items["type"] != "object" {
		t.Fatalf("Moonshot base URL unexpectedly enabled Kimi compat: %#v", items)
	}
}

// TestKimiCompatExplicitOptionOverridesNonMoonshotBaseURL confirms that
// calling WithKimiChatCompletionsCompat explicitly still sanitizes tool
// schemas even when the base URL doesn't look like Moonshot's (e.g. a proxy
// or self-hosted gateway in front of Kimi).
func TestKimiCompatExplicitOptionOverridesNonMoonshotBaseURL(t *testing.T) {
	var params map[string]any
	srv := newEchoToolsServer(t, &params)
	defer srv.Close()

	p := completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL(srv.URL), // not a Moonshot host
		completions.WithKimiChatCompletionsCompat(),
	)
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "kimi-k2",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
		Tools:    []sdk.ToolDefinition{kimiAnyOfTool()},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	assertKimiSanitizedAnyOf(t, params)
}

func TestMain(m *testing.M) {
	testutil.LoadEnv()
	os.Exit(m.Run())
}
