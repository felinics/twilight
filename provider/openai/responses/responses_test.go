package responses_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/felinics/twilight/provider/openai/responses"
	"github.com/felinics/twilight/provider/providertest"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// mustJSON is the test-side half of the seam change: the SDK resolves a tool's
// Parameters into JSON Schema before a provider sees it, so a test that used to
// hand the provider a Go schema value now hands it the resolved JSON.
// ---------- unit tests (mock server) ----------

func TestResponsesDoGenerate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/responses" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("unexpected auth: %s", r.Header.Get("Authorization"))
		}

		var body struct {
			Model string            `json:"model"`
			Input []json.RawMessage `json:"input"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		if body.Model != "gpt-4o-mini" {
			t.Errorf("model: got %q", body.Model)
		}

		// Expect system + user input items
		if len(body.Input) < 1 {
			t.Fatalf("expected at least 1 input item, got %d", len(body.Input))
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":         "resp_test123",
			"created_at": 1700000000,
			"model":      "gpt-4o-mini",
			"output": []map[string]any{{
				"type": "message",
				"id":   "msg_001",
				"role": "assistant",
				"content": []map[string]any{{
					"type":        "output_text",
					"text":        "Hello!",
					"annotations": []any{},
				}},
			}},
			"usage": map[string]any{
				"input_tokens":  5,
				"output_tokens": 2,
			},
		})
	}))
	defer srv.Close()

	p := responses.New(
		responses.WithAPIKey("test-key"),
		responses.WithBaseURL(srv.URL),
	)

	model := p.ChatModel("gpt-4o-mini")
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    model.ID,
		System:   "You are helpful.",
		Messages: []sdk.Message{sdk.UserMessage("Hi")},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}

	if result.Text != "Hello!" {
		t.Errorf("text: got %q, want %q", result.Text, "Hello!")
	}
	if result.FinishReason != sdk.FinishReasonStop {
		t.Errorf("finish: got %q, want %q", result.FinishReason, sdk.FinishReasonStop)
	}
	if result.Usage.InputTokens != 5 {
		t.Errorf("input tokens: got %d, want 5", result.Usage.InputTokens)
	}
	if result.Usage.OutputTokens != 2 {
		t.Errorf("output tokens: got %d, want 2", result.Usage.OutputTokens)
	}
	if result.Response.ID != "resp_test123" {
		t.Errorf("response id: got %q", result.Response.ID)
	}
}

func TestResponsesDoGenerate_PromptCacheKey(t *testing.T) {
	var body struct {
		PromptCacheKey *string `json:"prompt_cache_key"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&body)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":         "resp_test123",
			"created_at": 1700000000,
			"model":      "gpt-4o-mini",
			"output": []map[string]any{{
				"type": "message",
				"id":   "msg_001",
				"role": "assistant",
				"content": []map[string]any{{
					"type":        "output_text",
					"text":        "Hello!",
					"annotations": []any{},
				}},
			}},
			"usage": map[string]any{
				"input_tokens":  5,
				"output_tokens": 2,
			},
		})
	}))
	defer srv.Close()

	p := responses.New(
		responses.WithAPIKey("test-key"),
		responses.WithBaseURL(srv.URL),
	)

	key := "some-key"
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:          "gpt-4o-mini",
		Messages:       []sdk.Message{sdk.UserMessage("Hi")},
		PromptCacheKey: &key,
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}

	if body.PromptCacheKey == nil || *body.PromptCacheKey != "some-key" {
		t.Errorf("prompt_cache_key: got %v, want %q", body.PromptCacheKey, "some-key")
	}
}

func TestResponsesDoGenerate_PromptCacheKeyOmittedWhenUnset(t *testing.T) {
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewDecoder(r.Body).Decode(&body)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":         "resp_test123",
			"created_at": 1700000000,
			"model":      "gpt-4o-mini",
			"output": []map[string]any{{
				"type": "message",
				"id":   "msg_001",
				"role": "assistant",
				"content": []map[string]any{{
					"type":        "output_text",
					"text":        "Hello!",
					"annotations": []any{},
				}},
			}},
			"usage": map[string]any{
				"input_tokens":  5,
				"output_tokens": 2,
			},
		})
	}))
	defer srv.Close()

	p := responses.New(
		responses.WithAPIKey("test-key"),
		responses.WithBaseURL(srv.URL),
	)

	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "gpt-4o-mini",
		Messages: []sdk.Message{sdk.UserMessage("Hi")},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}

	if _, ok := body["prompt_cache_key"]; ok {
		t.Errorf("expected prompt_cache_key to be omitted, got %v", body["prompt_cache_key"])
	}
}

// TestResponsesDoGenerate_ForwardsMaxReasoningEffortVerbatim pins that the SDK
// no longer rewrites "max" into "xhigh". Effort is an open enum, so an
// unsupported tier is the endpoint's 400 to return rather than the SDK's to
// silently downgrade.
func TestResponsesDoGenerate_ForwardsMaxReasoningEffortVerbatim(t *testing.T) {
	var body struct {
		Reasoning *struct {
			Effort  string  `json:"effort"`
			Summary *string `json:"summary"`
		} `json:"reasoning"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":         "resp_reasoning",
			"created_at": 1700000000,
			"model":      "gpt-5.2",
			"output": []map[string]any{{
				"type": "message",
				"id":   "msg_001",
				"role": "assistant",
				"content": []map[string]any{{
					"type": "output_text",
					"text": "ok",
				}},
			}},
			"usage": map[string]any{"input_tokens": 1, "output_tokens": 1},
		})
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("test-key"), responses.WithBaseURL(srv.URL))
	effort := "max"
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:           "gpt-5.2",
		Messages:        []sdk.Message{sdk.UserMessage("hi")},
		ReasoningEffort: &effort,
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	if body.Reasoning == nil || body.Reasoning.Effort != "max" {
		t.Fatalf("reasoning.effort: got %#v, want max (forwarded verbatim)", body.Reasoning)
	}
	if body.Reasoning.Summary != nil {
		t.Fatalf("reasoning.summary should be omitted unless explicitly requested, got %q", *body.Reasoning.Summary)
	}
}

func TestResponsesDoGenerate_ReasoningSummaryIsExplicitAndIndependent(t *testing.T) {
	var body struct {
		Reasoning *struct {
			Effort  *string `json:"effort"`
			Summary *string `json:"summary"`
		} `json:"reasoning"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":         "resp_summary",
			"created_at": 1700000000,
			"model":      "gpt-5.2",
			"output": []map[string]any{{
				"type": "message",
				"id":   "msg_001",
				"role": "assistant",
				"content": []map[string]any{{
					"type": "output_text",
					"text": "ok",
				}},
			}},
			"usage": map[string]any{"input_tokens": 1, "output_tokens": 1},
		})
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("test-key"), responses.WithBaseURL(srv.URL))
	summary := "auto"
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:            "gpt-5.2",
		Messages:         []sdk.Message{sdk.UserMessage("hi")},
		ReasoningSummary: &summary,
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	if body.Reasoning == nil || body.Reasoning.Summary == nil || *body.Reasoning.Summary != "auto" {
		t.Fatalf("reasoning.summary: got %#v, want auto", body.Reasoning)
	}
	if body.Reasoning.Effort != nil {
		t.Fatalf("reasoning.effort should be omitted when unset, got %q", *body.Reasoning.Effort)
	}
}

func TestResponsesDoGenerate_WithBedrockCredentials(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got == "" || got[:16] != "AWS4-HMAC-SHA256" {
			t.Fatalf("expected SigV4 auth header, got %q", got)
		}
		if got := r.Header.Get("X-Amz-Date"); got == "" {
			t.Fatal("expected X-Amz-Date header")
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id":         "resp_test123",
			"created_at": 1700000000,
			"model":      "openai.gpt-oss-120b",
			"output": []map[string]any{{
				"type": "message",
				"id":   "msg_001",
				"role": "assistant",
				"content": []map[string]any{{
					"type":        "output_text",
					"text":        "Hello from Bedrock",
					"annotations": []any{},
				}},
			}},
			"usage": map[string]any{
				"input_tokens":  5,
				"output_tokens": 2,
			},
		})
	}))
	defer srv.Close()

	p := responses.New(
		responses.WithBaseURL(srv.URL),
		responses.WithBedrockCredentials("us-east-1", "AKIDEXAMPLE", "secret", ""),
	)

	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "openai.gpt-oss-120b",
		Messages: []sdk.Message{sdk.UserMessage("Hi")},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	if result.Text != "Hello from Bedrock" {
		t.Fatalf("text: got %q", result.Text)
	}
}

func TestResponsesDoGenerate_ToolCall(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Tools []struct {
				Type       string `json:"type"`
				Name       string `json:"name"`
				Parameters any    `json:"parameters"`
			} `json:"tools"`
			ToolChoice string `json:"tool_choice"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		if len(body.Tools) != 1 {
			t.Fatalf("tools: got %d, want 1", len(body.Tools))
		}
		if body.Tools[0].Type != "function" {
			t.Errorf("tool type: got %q", body.Tools[0].Type)
		}
		if body.Tools[0].Name != "get_weather" {
			t.Errorf("tool name: got %q", body.Tools[0].Name)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "resp_tool", "created_at": 1700000000, "model": "gpt-4o-mini",
			"output": []map[string]any{{
				"type":      "function_call",
				"id":        "fc_001",
				"call_id":   "call_abc123",
				"name":      "get_weather",
				"arguments": `{"location":"Beijing"}`,
			}},
			"usage": map[string]any{
				"input_tokens":  20,
				"output_tokens": 10,
			},
		})
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))

	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "gpt-4o-mini",
		Messages: []sdk.Message{sdk.UserMessage("Weather in Beijing?")},
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
		t.Errorf("call_id: got %q", tc.ToolCallID)
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

func TestResponsesDoGenerate_ToolCallMultiTurn(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input []json.RawMessage `json:"input"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		// Expect: user message, function_call, function_call_output
		if len(body.Input) < 3 {
			t.Fatalf("expected at least 3 input items, got %d", len(body.Input))
		}

		var fc struct {
			Type   string `json:"type"`
			CallID string `json:"call_id"`
			Name   string `json:"name"`
		}
		json.Unmarshal(body.Input[1], &fc)
		if fc.Type != "function_call" {
			t.Errorf("input[1] type: got %q, want function_call", fc.Type)
		}
		if fc.CallID != "call_abc" {
			t.Errorf("input[1] call_id: got %q", fc.CallID)
		}

		var fco struct {
			Type   string `json:"type"`
			CallID string `json:"call_id"`
			Output string `json:"output"`
		}
		json.Unmarshal(body.Input[2], &fco)
		if fco.Type != "function_call_output" {
			t.Errorf("input[2] type: got %q, want function_call_output", fco.Type)
		}
		if fco.CallID != "call_abc" {
			t.Errorf("input[2] call_id: got %q", fco.CallID)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "resp_mt", "created_at": 1700000000, "model": "gpt-4o-mini",
			"output": []map[string]any{{
				"type": "message", "id": "msg_002", "role": "assistant",
				"content": []map[string]any{{
					"type": "output_text", "text": "It's sunny in Beijing.",
					"annotations": []any{},
				}},
			}},
			"usage": map[string]any{"input_tokens": 30, "output_tokens": 8},
		})
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))

	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: "gpt-4o-mini",
		Messages: []sdk.Message{
			sdk.UserMessage("Weather?"),
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

func TestResponsesDoGenerate_Reasoning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "resp_r", "created_at": 1700000000, "model": "o3-mini",
			"output": []map[string]any{
				{
					"type": "reasoning",
					"id":   "rs_001",
					"summary": []map[string]any{
						{"type": "summary_text", "text": "Let me think... 2+2=4"},
					},
				},
				{
					"type": "message", "id": "msg_003", "role": "assistant",
					"content": []map[string]any{{
						"type": "output_text", "text": "The answer is 4.",
						"annotations": []any{},
					}},
				},
			},
			"usage": map[string]any{"input_tokens": 5, "output_tokens": 10},
		})
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "o3-mini",
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

func TestResponsesDoGenerate_WithAnnotations(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "resp_ann", "created_at": 1700000000, "model": "gpt-4o-mini",
			"output": []map[string]any{{
				"type": "message", "id": "msg_004", "role": "assistant",
				"content": []map[string]any{{
					"type": "output_text",
					"text": "According to [1], the answer is yes.",
					"annotations": []map[string]any{{
						"type":        "url_citation",
						"url":         "https://example.com/source",
						"title":       "Example Source",
						"start_index": 14,
						"end_index":   17,
					}},
				}},
			}},
			"usage": map[string]any{"input_tokens": 10, "output_tokens": 8},
		})
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "gpt-4o-mini",
		Messages: []sdk.Message{sdk.UserMessage("Is water wet?")},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}

	if len(result.Sources) != 1 {
		t.Fatalf("sources: got %d, want 1", len(result.Sources))
	}
	src := result.Sources[0]
	if src.SourceType != "url" {
		t.Errorf("source type: got %q", src.SourceType)
	}
	if src.URL != "https://example.com/source" {
		t.Errorf("source url: got %q", src.URL)
	}
	if src.Title != "Example Source" {
		t.Errorf("source title: got %q", src.Title)
	}
}

func TestResponsesDoGenerate_IncompleteLength(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "resp_inc", "created_at": 1700000000, "model": "gpt-4o-mini",
			"output": []map[string]any{{
				"type": "message", "id": "msg_005", "role": "assistant",
				"content": []map[string]any{{
					"type": "output_text", "text": "Truncated...",
					"annotations": []any{},
				}},
			}},
			"incomplete_details": map[string]any{"reason": "max_output_tokens"},
			"usage":              map[string]any{"input_tokens": 10, "output_tokens": 100},
		})
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "gpt-4o-mini",
		Messages: []sdk.Message{sdk.UserMessage("Write a long essay")},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}

	if result.FinishReason != sdk.FinishReasonLength {
		t.Errorf("finish: got %q, want %q", result.FinishReason, sdk.FinishReasonLength)
	}
	if result.RawFinishReason != "max_output_tokens" {
		t.Errorf("raw finish: got %q", result.RawFinishReason)
	}
}

func TestResponsesDoGenerate_NoModel(t *testing.T) {
	p := responses.New(responses.WithAPIKey("k"))
	_, err := p.DoGenerate(context.Background(), sdk.Request{})
	if err == nil {
		t.Fatal("expected error for nil model")
	}
}

// ---------- streaming tests ----------

func TestResponsesDoStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		events := []struct{ event, data string }{
			{
				"response.created",
				`{"type":"response.created","response":{"id":"resp_s1","created_at":1700000000,"model":"gpt-4o-mini"}}`,
			},
			{
				"response.output_item.added",
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_010"}}`,
			},
			{
				"response.output_text.delta",
				`{"type":"response.output_text.delta","item_id":"msg_010","delta":"Hello"}`,
			},
			{
				"response.output_text.delta",
				`{"type":"response.output_text.delta","item_id":"msg_010","delta":" world"}`,
			},
			{
				"response.output_item.done",
				`{"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"msg_010"}}`,
			},
			{
				"response.completed",
				`{"type":"response.completed","response":{"usage":{"input_tokens":3,"output_tokens":2}}}`,
			},
		}

		for _, e := range events {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.event, e.data)
			flusher.Flush()
		}
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "gpt-4o-mini",
		Messages: []sdk.Message{sdk.UserMessage("Hi")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	var collected string
	var gotStart, gotTextStart, gotTextEnd, gotFinishStep, gotFinish bool
	for part := range sr {
		switch p := part.(type) {
		case *sdk.StartPart:
			gotStart = true
		case *sdk.TextStartPart:
			gotTextStart = true
		case *sdk.TextDeltaPart:
			collected += p.Text
		case *sdk.TextEndPart:
			gotTextEnd = true
		case *sdk.FinishStepPart:
			gotFinishStep = true
			if p.FinishReason != sdk.FinishReasonStop {
				t.Errorf("finish step reason: got %q", p.FinishReason)
			}
			if p.Usage.InputTokens != 3 {
				t.Errorf("usage input tokens: got %d", p.Usage.InputTokens)
			}
		case *sdk.FinishPart:
			gotFinish = true
			if p.FinishReason != sdk.FinishReasonStop {
				t.Errorf("finish reason: got %q", p.FinishReason)
			}
		case *sdk.ErrorPart:
			t.Fatalf("error: %v", p.Error)
		}
	}

	if !gotStart {
		t.Error("missing StartPart")
	}
	if !gotTextStart {
		t.Error("missing TextStartPart")
	}
	if !gotTextEnd {
		t.Error("missing TextEndPart")
	}
	if !gotFinishStep {
		t.Error("missing FinishStepPart")
	}
	if !gotFinish {
		t.Error("missing FinishPart")
	}
	if collected != "Hello world" {
		t.Errorf("text: got %q, want %q", collected, "Hello world")
	}
}

func TestResponsesDoStream_ToolCall(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		events := []struct{ event, data string }{
			{
				"response.created",
				`{"type":"response.created","response":{"id":"resp_st","created_at":1700000000,"model":"gpt-4o-mini"}}`,
			},
			{
				"response.output_item.added",
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_010","call_id":"call_xyz","name":"get_weather"}}`,
			},
			{
				"response.function_call_arguments.delta",
				`{"type":"response.function_call_arguments.delta","item_id":"fc_010","output_index":0,"delta":"{\"location\""}`,
			},
			{
				"response.function_call_arguments.delta",
				`{"type":"response.function_call_arguments.delta","item_id":"fc_010","output_index":0,"delta":":\"Tokyo\"}"}`,
			},
			{
				"response.output_item.done",
				`{"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"fc_010","call_id":"call_xyz","name":"get_weather","arguments":"{\"location\":\"Tokyo\"}"}}`,
			},
			{
				"response.completed",
				`{"type":"response.completed","response":{"usage":{"input_tokens":10,"output_tokens":5}}}`,
			},
		}

		for _, e := range events {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.event, e.data)
			flusher.Flush()
		}
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "gpt-4o-mini",
		Messages: []sdk.Message{sdk.UserMessage("Weather in Tokyo?")},
		Tools:    []sdk.ToolDefinition{{Name: "get_weather", Parameters: &jsonschema.Schema{Type: "object"}}},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	var (
		gotInputStart bool
		gotInputEnd   bool
		argsDelta     string
		gotToolCall   *sdk.StreamToolCallPart
		gotFinish     bool
	)

	for part := range sr {
		switch p := part.(type) {
		case *sdk.ToolInputStartPart:
			gotInputStart = true
			if p.ToolName != "get_weather" {
				t.Errorf("input start tool: got %q", p.ToolName)
			}
			if p.ID != "call_xyz" {
				t.Errorf("input start id: got %q", p.ID)
			}
		case *sdk.ToolInputDeltaPart:
			argsDelta += p.Delta
		case *sdk.ToolInputEndPart:
			gotInputEnd = true
		case *sdk.StreamToolCallPart:
			gotToolCall = p
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
	if !gotFinish {
		t.Error("missing FinishPart")
	}
}

func TestResponsesDoStream_Reasoning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		events := []struct{ event, data string }{
			{
				"response.created",
				`{"type":"response.created","response":{"id":"resp_sr","created_at":1700000000,"model":"o3-mini"}}`,
			},
			{
				"response.output_item.added",
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"reasoning","id":"rs_010"}}`,
			},
			{
				"response.reasoning_summary_text.delta",
				`{"type":"response.reasoning_summary_text.delta","item_id":"rs_010","summary_index":0,"delta":"Summary"}`,
			},
			{
				"response.reasoning_text.delta",
				`{"type":"response.reasoning_text.delta","item_id":"rs_010","delta":"Raw"}`,
			},
			{
				"response.output_item.done",
				`{"type":"response.output_item.done","output_index":0,"item":{"type":"reasoning","id":"rs_010"}}`,
			},
			{
				"response.output_item.added",
				`{"type":"response.output_item.added","output_index":1,"item":{"type":"reasoning","id":"rs_011"}}`,
			},
			{
				"response.reasoning_summary_text.delta",
				`{"type":"response.reasoning_summary_text.delta","item_id":"rs_011","summary_index":0,"delta":"SecondSummary"}`,
			},
			{
				"response.output_item.done",
				`{"type":"response.output_item.done","output_index":1,"item":{"type":"reasoning","id":"rs_011"}}`,
			},
			{
				"response.output_item.added",
				`{"type":"response.output_item.added","output_index":2,"item":{"type":"message","id":"msg_020"}}`,
			},
			{
				"response.output_text.delta",
				`{"type":"response.output_text.delta","item_id":"msg_020","delta":"The answer is 4."}`,
			},
			{
				"response.output_item.done",
				`{"type":"response.output_item.done","output_index":2,"item":{"type":"message","id":"msg_020"}}`,
			},
			{
				"response.completed",
				`{"type":"response.completed","response":{"usage":{"input_tokens":5,"output_tokens":15,"output_tokens_details":{"reasoning_tokens":10}}}}`,
			},
		}

		for _, e := range events {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.event, e.data)
			flusher.Flush()
		}
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "o3-mini",
		Messages: []sdk.Message{sdk.UserMessage("2+2?")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	var reasoning, text string
	var reasoningStartIDs, reasoningEndIDs []string
	events := make([]sdk.StreamPartType, 0, 8)
	for part := range sr {
		events = append(events, part.Type())
		switch p := part.(type) {
		case *sdk.ReasoningStartPart:
			reasoningStartIDs = append(reasoningStartIDs, p.ID)
		case *sdk.ReasoningDeltaPart:
			reasoning += p.Text
		case *sdk.ReasoningEndPart:
			reasoningEndIDs = append(reasoningEndIDs, p.ID)
		case *sdk.TextDeltaPart:
			text += p.Text
		case *sdk.FinishPart:
			if p.TotalUsage.ReasoningTokens != 10 {
				t.Errorf("reasoning tokens: got %d, want 10", p.TotalUsage.ReasoningTokens)
			}
		case *sdk.ErrorPart:
			t.Fatalf("error: %v", p.Error)
		}
	}

	if got := strings.Join(reasoningStartIDs, ","); got != "rs_010,rs_011" {
		t.Errorf("reasoning start IDs: got %q, want %q", got, "rs_010,rs_011")
	}
	if got := strings.Join(reasoningEndIDs, ","); got != "rs_010,rs_011" {
		t.Errorf("reasoning end IDs: got %q, want %q", got, "rs_010,rs_011")
	}
	if reasoning != "SummaryRawSecondSummary" {
		t.Errorf("reasoning: got %q, want %q", reasoning, "SummaryRawSecondSummary")
	}
	if text != "The answer is 4." {
		t.Errorf("text: got %q", text)
	}

	// Verify reasoning ends before text starts
	reasoningEndIdx := -1
	textStartIdx := -1
	for i, ev := range events {
		if ev == sdk.StreamPartTypeReasoningEnd && reasoningEndIdx == -1 {
			reasoningEndIdx = i
		}
		if ev == sdk.StreamPartTypeTextStart && textStartIdx == -1 {
			textStartIdx = i
		}
	}
	if reasoningEndIdx >= textStartIdx {
		t.Errorf("reasoning-end (idx %d) should come before text-start (idx %d)", reasoningEndIdx, textStartIdx)
	}
}

func TestResponsesDoStream_ReasoningKeepsItemIDOnFinalFlush(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_flush\",\"created_at\":1700000000,\"model\":\"grok-4\"}}\n\n")
		fmt.Fprint(w, "event: response.output_item.added\ndata: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"type\":\"reasoning\",\"id\":\"rs_flush\"}}\n\n")
		fmt.Fprint(w, "event: response.reasoning_text.delta\ndata: {\"type\":\"response.reasoning_text.delta\",\"item_id\":\"rs_flush\",\"delta\":\"Thinking\"}\n\n")
		fmt.Fprint(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":1,\"output_tokens\":1}}}\n\n")
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "grok-4",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	var endID string
	for part := range sr {
		switch part := part.(type) {
		case *sdk.ReasoningEndPart:
			endID = part.ID
		case *sdk.ErrorPart:
			t.Fatalf("error: %v", part.Error)
		}
	}
	if endID != "rs_flush" {
		t.Fatalf("reasoning end ID: got %q, want %q", endID, "rs_flush")
	}
}

func TestResponsesDoStream_WithAnnotations(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		events := []struct{ event, data string }{
			{
				"response.created",
				`{"type":"response.created","response":{"id":"resp_sa","created_at":1700000000,"model":"gpt-4o-mini"}}`,
			},
			{
				"response.output_item.added",
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_030"}}`,
			},
			{
				"response.output_text.delta",
				`{"type":"response.output_text.delta","item_id":"msg_030","delta":"See [1]."}`,
			},
			{
				"response.output_text.annotation.added",
				`{"type":"response.output_text.annotation.added","annotation":{"type":"url_citation","url":"https://example.com","title":"Example","start_index":4,"end_index":7}}`,
			},
			{
				"response.output_item.done",
				`{"type":"response.output_item.done","output_index":0,"item":{"type":"message","id":"msg_030"}}`,
			},
			{
				"response.completed",
				`{"type":"response.completed","response":{"usage":{"input_tokens":5,"output_tokens":3}}}`,
			},
		}

		for _, e := range events {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.event, e.data)
			flusher.Flush()
		}
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "gpt-4o-mini",
		Messages: []sdk.Message{sdk.UserMessage("search")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	var sources []sdk.Source
	for part := range sr {
		switch p := part.(type) {
		case *sdk.StreamSourcePart:
			sources = append(sources, p.Source)
		case *sdk.ErrorPart:
			t.Fatalf("error: %v", p.Error)
		}
	}

	if len(sources) != 1 {
		t.Fatalf("sources: got %d, want 1", len(sources))
	}
	if sources[0].URL != "https://example.com" {
		t.Errorf("source url: got %q", sources[0].URL)
	}
	if sources[0].Title != "Example" {
		t.Errorf("source title: got %q", sources[0].Title)
	}
}

func TestResponsesDoStream_Incomplete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		events := []struct{ event, data string }{
			{
				"response.created",
				`{"type":"response.created","response":{"id":"resp_inc","created_at":1700000000,"model":"gpt-4o-mini"}}`,
			},
			{
				"response.output_item.added",
				`{"type":"response.output_item.added","output_index":0,"item":{"type":"message","id":"msg_040"}}`,
			},
			{
				"response.output_text.delta",
				`{"type":"response.output_text.delta","item_id":"msg_040","delta":"Partial..."}`,
			},
			{
				"response.incomplete",
				`{"type":"response.incomplete","response":{"incomplete_details":{"reason":"max_output_tokens"},"usage":{"input_tokens":10,"output_tokens":100}}}`,
			},
		}

		for _, e := range events {
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", e.event, e.data)
			flusher.Flush()
		}
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "gpt-4o-mini",
		Messages: []sdk.Message{sdk.UserMessage("long essay")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	var finishReason sdk.FinishReason
	for part := range sr {
		switch p := part.(type) {
		case *sdk.FinishPart:
			finishReason = p.FinishReason
		case *sdk.ErrorPart:
			t.Fatalf("error: %v", p.Error)
		}
	}

	if finishReason != sdk.FinishReasonLength {
		t.Errorf("finish: got %q, want %q", finishReason, sdk.FinishReasonLength)
	}
}

func TestResponsesDoStream_NoModel(t *testing.T) {
	p := responses.New(responses.WithAPIKey("k"))
	_, err := p.DoStream(context.Background(), sdk.Request{})
	if err == nil {
		t.Fatal("expected error for nil model")
	}
}

// streamFailure streams one event and returns the stream's FinishPart and its
// single error.
func streamFailure(t *testing.T, event, data string) (*sdk.FinishPart, error) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("x-request-id", "req_stream")
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
		w.(http.Flusher).Flush()
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "gpt-5.6",
		Messages: []sdk.Message{sdk.UserMessage("test")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}
	var (
		errorsSeen []error
		finish     *sdk.FinishPart
	)
	for part := range sr {
		switch part := part.(type) {
		case *sdk.ErrorPart:
			errorsSeen = append(errorsSeen, part.Error)
		case *sdk.FinishPart:
			finish = part
		}
	}
	if len(errorsSeen) != 1 {
		t.Fatalf("ErrorPart count = %d, want 1 (%v)", len(errorsSeen), errorsSeen)
	}
	if finish == nil {
		t.Fatal("expected a FinishPart after the failure")
	}
	if finish.FinishReason != sdk.FinishReasonError {
		t.Errorf("FinishReason = %q, want %q", finish.FinishReason, sdk.FinishReasonError)
	}
	return finish, errorsSeen[0]
}

// The API reference puts the error event's code and message at the top level
// (openai-go's ResponseErrorEvent,
// https://github.com/openai/openai-go/blob/d7fd0c65cc247957d5b247ad42283fc8e4061868/responses/response.go);
// the Codex CLI reads them from a nested error object
// (https://github.com/openai/codex/blob/1b1835f751ebdc0cfc50b3fe55d4571dbb294563/codex-rs/codex-api/src/sse/responses.rs).
func TestResponsesDoStream_ErrorEvent(t *testing.T) {
	tests := []struct {
		name, data string
		wantType   string
	}{
		{
			name: "top-level",
			data: `{"type":"error","code":"server_error","message":"Internal error","param":null,"sequence_number":0}`,
		},
		{
			name:     "nested",
			data:     `{"type":"error","sequence_number":0,"error":{"type":"server_error","code":"server_error","message":"Internal error"}}`,
			wantType: "server_error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := streamFailure(t, "error", tt.data)
			apiErr := providertest.WantAPIError(t, err, "openai-responses", 0, sdk.KindServerError)
			if apiErr.Type != tt.wantType || apiErr.Code != "server_error" || apiErr.Message != "Internal error" || apiErr.RequestID != "req_stream" {
				t.Errorf("Type, Code, Message, RequestID = %q, %q, %q, %q", apiErr.Type, apiErr.Code, apiErr.Message, apiErr.RequestID)
			}
			if string(apiErr.Body) != tt.data {
				t.Errorf("Body = %q, want the event data", apiErr.Body)
			}
		})
	}
}

func TestResponsesDoStream_ResponseFailed(t *testing.T) {
	tests := []struct {
		name        string
		response    string
		wantCode    string
		wantMessage string
		wantKind    sdk.ErrorKind
		wantUsage   sdk.Usage
	}{
		{
			name: "structured error and usage",
			response: `{"type":"response.failed","response":{"status":"failed","error":{` +
				`"type":"server_error","code":"server_error","message":"generation failed"},` +
				`"usage":{"input_tokens":7,"output_tokens":2,"input_tokens_details":{"cached_tokens":3},` +
				`"output_tokens_details":{"reasoning_tokens":1}}}}`,
			wantCode:    "server_error",
			wantMessage: "generation failed",
			wantKind:    sdk.KindServerError,
			wantUsage: sdk.Usage{
				InputTokens:       7,
				OutputTokens:      2,
				TotalTokens:       9,
				ReasoningTokens:   1,
				CachedInputTokens: 3,
			},
		},
		{
			name:     "missing error payload",
			response: `{"type":"response.failed","response":{"status":"failed"}}`,
			wantKind: sdk.KindUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			finish, err := streamFailure(t, "response.failed", tt.response)
			apiErr := providertest.WantAPIError(t, err, "openai-responses", 0, tt.wantKind)
			if apiErr.Code != tt.wantCode || apiErr.Message != tt.wantMessage {
				t.Errorf("Code, Message = %q, %q; want %q, %q", apiErr.Code, apiErr.Message, tt.wantCode, tt.wantMessage)
			}
			if string(apiErr.Body) != tt.response {
				t.Errorf("Body = %q, want the event data", apiErr.Body)
			}
			if finish.TotalUsage.InputTokens != tt.wantUsage.InputTokens ||
				finish.TotalUsage.OutputTokens != tt.wantUsage.OutputTokens ||
				finish.TotalUsage.TotalTokens != tt.wantUsage.TotalTokens ||
				finish.TotalUsage.ReasoningTokens != tt.wantUsage.ReasoningTokens ||
				finish.TotalUsage.CachedInputTokens != tt.wantUsage.CachedInputTokens {
				t.Errorf("usage = %+v, want selected fields from %+v", finish.TotalUsage, tt.wantUsage)
			}
		})
	}
}

// ---------- input conversion tests ----------

func TestResponsesInputConversion_SystemMessage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Instructions string            `json:"instructions"`
			Input        []json.RawMessage `json:"input"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		if body.Instructions != "Be helpful" {
			t.Fatalf("instructions: got %q", body.Instructions)
		}
		if len(body.Input) != 1 {
			t.Fatalf("expected 1 input item, got %d", len(body.Input))
		}

		var user struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		}
		json.Unmarshal(body.Input[0], &user)
		if user.Role != "user" {
			t.Errorf("user role: got %q", user.Role)
		}
		if len(user.Content) != 1 || user.Content[0].Type != "input_text" {
			t.Errorf("user content: %+v", user.Content)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "resp_sys", "created_at": 1700000000, "model": "gpt-4o-mini",
			"output": []map[string]any{{
				"type": "message", "id": "msg_050", "role": "assistant",
				"content": []map[string]any{{"type": "output_text", "text": "OK", "annotations": []any{}}},
			}},
			"usage": map[string]any{"input_tokens": 5, "output_tokens": 1},
		})
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "gpt-4o-mini",
		System:   "Be helpful",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
}

func TestResponsesInputConversion_ImagePart(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input []json.RawMessage `json:"input"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		var user struct {
			Role    string `json:"role"`
			Content []struct {
				Type     string `json:"type"`
				Text     string `json:"text,omitempty"`
				ImageURL string `json:"image_url,omitempty"`
			} `json:"content"`
		}
		json.Unmarshal(body.Input[0], &user)

		if len(user.Content) != 2 {
			t.Fatalf("user content parts: got %d, want 2", len(user.Content))
		}
		if user.Content[0].Type != "input_text" {
			t.Errorf("part[0] type: got %q", user.Content[0].Type)
		}
		if user.Content[1].Type != "input_image" {
			t.Errorf("part[1] type: got %q", user.Content[1].Type)
		}
		if user.Content[1].ImageURL != "https://example.com/cat.png" {
			t.Errorf("image url: got %q", user.Content[1].ImageURL)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "resp_img", "created_at": 1700000000, "model": "gpt-4o-mini",
			"output": []map[string]any{{
				"type": "message", "id": "msg_060", "role": "assistant",
				"content": []map[string]any{{"type": "output_text", "text": "A cat", "annotations": []any{}}},
			}},
			"usage": map[string]any{"input_tokens": 20, "output_tokens": 2},
		})
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: "gpt-4o-mini",
		Messages: []sdk.Message{{
			Role: sdk.MessageRoleUser,
			Content: []sdk.MessagePart{
				sdk.TextPart{Text: "What is this?"},
				sdk.ImagePart{Image: "https://example.com/cat.png"},
			},
		}},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
}

func TestResponsesInputConversion_AssistantReasoning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Input []json.RawMessage `json:"input"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		// Expect: user, reasoning, assistant
		if len(body.Input) < 3 {
			t.Fatalf("expected at least 3 input items, got %d", len(body.Input))
		}

		var reasoning struct {
			Type    string `json:"type"`
			Summary []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"summary"`
		}
		json.Unmarshal(body.Input[1], &reasoning)
		if reasoning.Type != "reasoning" {
			t.Errorf("input[1] type: got %q, want reasoning", reasoning.Type)
		}
		if len(reasoning.Summary) != 1 || reasoning.Summary[0].Text != "I thought carefully" {
			t.Errorf("reasoning summary: %+v", reasoning.Summary)
		}

		var assistant struct {
			Role    string `json:"role"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		}
		json.Unmarshal(body.Input[2], &assistant)
		if assistant.Role != "assistant" {
			t.Errorf("input[2] role: got %q", assistant.Role)
		}
		if len(assistant.Content) != 1 || assistant.Content[0].Text != "The answer" {
			t.Errorf("assistant content: %+v", assistant.Content)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"id": "resp_ar", "created_at": 1700000000, "model": "o3-mini",
			"output": []map[string]any{{
				"type": "message", "id": "msg_070", "role": "assistant",
				"content": []map[string]any{{"type": "output_text", "text": "OK", "annotations": []any{}}},
			}},
			"usage": map[string]any{"input_tokens": 10, "output_tokens": 1},
		})
	}))
	defer srv.Close()

	p := responses.New(responses.WithAPIKey("k"), responses.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: "o3-mini",
		Messages: []sdk.Message{
			sdk.UserMessage("question"),
			{
				Role: sdk.MessageRoleAssistant,
				Content: []sdk.MessagePart{
					sdk.ReasoningPart{Text: "I thought carefully", Format: sdk.ReasoningFormatOpenAIResponses},
					sdk.TextPart{Text: "The answer"},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
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
				{"id": "o3-mini", "object": "model", "owned_by": "openai"},
			},
		})
	}))
	defer srv.Close()

	p := responses.New(
		responses.WithAPIKey("test-key"),
		responses.WithBaseURL(srv.URL),
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
}

func TestProviderTest_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{},
		})
	}))
	defer srv.Close()

	p := responses.New(
		responses.WithAPIKey("test-key"),
		responses.WithBaseURL(srv.URL),
	)

	if err := p.Test(context.Background()); err != nil {
		t.Errorf("Test() = %v, want nil", err)
	}
}

func TestProviderTest_Unhealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "invalid api key"}})
	}))
	defer srv.Close()

	p := responses.New(
		responses.WithAPIKey("bad-key"),
		responses.WithBaseURL(srv.URL),
	)

	err := p.Test(context.Background())
	if kind := sdk.KindOf(err); kind != sdk.KindAuthentication {
		t.Errorf("KindOf(Test()) = %q, want %q (err %v)", kind, sdk.KindAuthentication, err)
	}
}

func TestProviderTest_Unreachable(t *testing.T) {
	p := responses.New(
		responses.WithAPIKey("test-key"),
		responses.WithBaseURL("http://127.0.0.1:1"),
	)

	err := p.Test(context.Background())
	if err == nil || sdk.KindOf(err) != sdk.KindUnknown {
		t.Errorf("Test() = %v, want a transport error with no APIError Kind", err)
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

	p := responses.New(
		responses.WithAPIKey("test-key"),
		responses.WithBaseURL(srv.URL),
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

	p := responses.New(
		responses.WithAPIKey("test-key"),
		responses.WithBaseURL(srv.URL),
	)

	result, err := p.TestModel(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("TestModel failed: %v", err)
	}
	if result.Supported {
		t.Error("expected model to not be supported")
	}
}
