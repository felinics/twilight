package generativeai_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/felinics/twilight/internal/testutil"
	"github.com/felinics/twilight/provider/google/generativeai"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// mustJSON is the test-side half of the seam change: the SDK resolves a tool's
// Parameters into JSON Schema before a provider sees it, so a test that used to
// hand the provider a Go schema value now hands it the resolved JSON.
// ---------- unit tests (mock server) ----------

func TestDoGenerate(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models/gemini-2.0-flash:generateContent" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("x-goog-api-key") != "test-key" {
			t.Errorf("unexpected api key header: %s", r.Header.Get("x-goog-api-key"))
		}

		var body map[string]any
		json.NewDecoder(r.Body).Decode(&body)

		if body["systemInstruction"] == nil {
			t.Error("expected systemInstruction to be set")
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"candidates": []map[string]any{{
				"content": map[string]any{
					"role":  "model",
					"parts": []map[string]any{{"text": "Hello!"}},
				},
				"finishReason": "STOP",
			}},
			"usageMetadata": map[string]any{
				"promptTokenCount":     5,
				"candidatesTokenCount": 2,
				"totalTokenCount":      7,
			},
		})
	}))
	defer srv.Close()

	p := generativeai.New(
		generativeai.WithAPIKey("test-key"),
		generativeai.WithBaseURL(srv.URL),
	)

	model := p.ChatModel("gemini-2.0-flash")
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:  model.ID,
		System: "You are helpful.",
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

func TestDoGenerate_ToolCall(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var rawBody map[string]any
		if err := json.NewDecoder(r.Body).Decode(&rawBody); err != nil {
			t.Fatalf("decode request body: %v", err)
		}
		data, err := json.Marshal(rawBody)
		if err != nil {
			t.Fatalf("marshal request body: %v", err)
		}

		var body struct {
			Tools []struct {
				FunctionDeclarations []struct {
					Name                 string `json:"name"`
					Description          string `json:"description"`
					Parameters           any    `json:"parameters"`
					ParametersJSONSchema any    `json:"parametersJsonSchema"`
				} `json:"functionDeclarations"`
			} `json:"tools"`
			ToolConfig *struct {
				FunctionCallingConfig struct {
					Mode string `json:"mode"`
				} `json:"functionCallingConfig"`
			} `json:"toolConfig"`
		}
		if err := json.Unmarshal(data, &body); err != nil {
			t.Fatalf("unmarshal request body: %v", err)
		}

		if len(body.Tools) != 1 {
			t.Fatalf("expected 1 tool group, got %d", len(body.Tools))
		}
		if len(body.Tools[0].FunctionDeclarations) != 1 {
			t.Fatalf("expected 1 function declaration, got %d", len(body.Tools[0].FunctionDeclarations))
		}
		if body.Tools[0].FunctionDeclarations[0].Name != "get_weather" {
			t.Errorf("tool name: got %q", body.Tools[0].FunctionDeclarations[0].Name)
		}
		if body.Tools[0].FunctionDeclarations[0].Parameters != nil {
			t.Errorf("expected parameters to be omitted")
		}
		if body.Tools[0].FunctionDeclarations[0].ParametersJSONSchema == nil {
			t.Errorf("expected parametersJsonSchema to be set")
		}

		toolsRaw, ok := rawBody["tools"].([]any)
		if !ok || len(toolsRaw) != 1 {
			t.Fatalf("raw tools: got %T %v", rawBody["tools"], rawBody["tools"])
		}
		toolRaw, ok := toolsRaw[0].(map[string]any)
		if !ok {
			t.Fatalf("raw tool type: got %T", toolsRaw[0])
		}
		declsRaw, ok := toolRaw["functionDeclarations"].([]any)
		if !ok || len(declsRaw) != 1 {
			t.Fatalf("raw functionDeclarations: got %T %v", toolRaw["functionDeclarations"], toolRaw["functionDeclarations"])
		}
		declRaw, ok := declsRaw[0].(map[string]any)
		if !ok {
			t.Fatalf("raw function declaration type: got %T", declsRaw[0])
		}
		if _, ok := declRaw["parameters"]; ok {
			t.Errorf("expected raw parameters key to be omitted")
		}
		schema, ok := declRaw["parametersJsonSchema"].(map[string]any)
		if !ok {
			t.Fatalf("raw parametersJsonSchema type: got %T", declRaw["parametersJsonSchema"])
		}
		if schema["additionalProperties"] != false {
			t.Errorf("additionalProperties: got %v, want false", schema["additionalProperties"])
		}
		props, ok := schema["properties"].(map[string]any)
		if !ok {
			t.Fatalf("properties type: got %T", schema["properties"])
		}
		location, ok := props["location"].(map[string]any)
		if !ok {
			t.Fatalf("location property type: got %T", props["location"])
		}
		if location["type"] != "string" {
			t.Errorf("location type: got %v, want string", location["type"])
		}
		required, ok := schema["required"].([]any)
		if !ok || len(required) != 1 || required[0] != "location" {
			t.Errorf("required: got %v, want [location]", schema["required"])
		}
		if body.ToolConfig == nil || body.ToolConfig.FunctionCallingConfig.Mode != "AUTO" {
			t.Errorf("expected AUTO tool config mode")
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"candidates": []map[string]any{{
				"content": map[string]any{
					"role": "model",
					"parts": []map[string]any{{
						"functionCall": map[string]any{
							"name": "get_weather",
							"args": map[string]any{"location": "Beijing"},
						},
						"thoughtSignature": "sig-generate",
					}},
				},
				"finishReason": "STOP",
			}},
			"usageMetadata": map[string]any{
				"promptTokenCount":     20,
				"candidatesTokenCount": 10,
				"totalTokenCount":      30,
			},
		})
	}))
	defer srv.Close()

	p := generativeai.New(generativeai.WithAPIKey("test-key"), generativeai.WithBaseURL(srv.URL))

	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: "gemini-2.0-flash",
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
				Required:             []string{"location"},
				AdditionalProperties: &jsonschema.Schema{Not: &jsonschema.Schema{}},
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
	if got := tc.ProviderMetadata.Get("google", "thoughtSignature"); got != "sig-generate" {
		t.Fatalf("thoughtSignature = %q, want sig-generate", got)
	}
}

func TestDoGenerate_ToolCallMultiTurn(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Contents []json.RawMessage `json:"contents"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		if len(body.Contents) != 3 {
			t.Fatalf("expected 3 contents, got %d", len(body.Contents))
		}

		// verify model message has functionCall
		var modelMsg struct {
			Role  string `json:"role"`
			Parts []struct {
				FunctionCall *struct {
					Name string `json:"name"`
					Args any    `json:"args"`
				} `json:"functionCall,omitempty"`
				ThoughtSignature string `json:"thoughtSignature,omitempty"`
			} `json:"parts"`
		}
		json.Unmarshal(body.Contents[1], &modelMsg)
		if modelMsg.Role != "model" {
			t.Errorf("msg[1] role: got %q", modelMsg.Role)
		}
		if len(modelMsg.Parts) != 1 || modelMsg.Parts[0].FunctionCall == nil {
			t.Errorf("msg[1] should have functionCall part")
		}
		if modelMsg.Parts[0].ThoughtSignature != "sig-multiturn" {
			t.Errorf("msg[1] thoughtSignature: got %q, want sig-multiturn", modelMsg.Parts[0].ThoughtSignature)
		}

		// verify tool result message has functionResponse
		var toolMsg struct {
			Role  string `json:"role"`
			Parts []struct {
				FunctionResponse *struct {
					Name     string `json:"name"`
					Response struct {
						Name    string `json:"name"`
						Content any    `json:"content"`
					} `json:"response"`
				} `json:"functionResponse,omitempty"`
			} `json:"parts"`
		}
		json.Unmarshal(body.Contents[2], &toolMsg)
		if toolMsg.Role != "user" {
			t.Errorf("msg[2] role: got %q, want 'user'", toolMsg.Role)
		}
		if len(toolMsg.Parts) != 1 || toolMsg.Parts[0].FunctionResponse == nil {
			t.Errorf("msg[2] should have functionResponse part")
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"candidates": []map[string]any{{
				"content": map[string]any{
					"role":  "model",
					"parts": []map[string]any{{"text": "It's sunny in Beijing."}},
				},
				"finishReason": "STOP",
			}},
			"usageMetadata": map[string]any{
				"promptTokenCount": 30, "candidatesTokenCount": 8, "totalTokenCount": 38,
			},
		})
	}))
	defer srv.Close()

	p := generativeai.New(generativeai.WithAPIKey("test-key"), generativeai.WithBaseURL(srv.URL))

	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: "gemini-2.0-flash",
		Messages: []sdk.Message{
			{
				Role:    sdk.MessageRoleUser,
				Content: []sdk.MessagePart{sdk.TextPart{Text: "Weather?"}},
			},
			{
				Role: sdk.MessageRoleAssistant,
				Content: []sdk.MessagePart{sdk.ToolCallPart{
					ToolCallID:       "call_abc",
					ToolName:         "get_weather",
					Input:            sdk.ParseToolArguments(`{"location":"Beijing"}`),
					ProviderMetadata: sdk.ProviderMetadata{"google": {"thoughtSignature": "sig-multiturn"}},
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

func TestDoGenerate_Reasoning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		thought := true
		_ = thought
		json.NewEncoder(w).Encode(map[string]any{
			"candidates": []map[string]any{{
				"content": map[string]any{
					"role": "model",
					"parts": []map[string]any{
						{"text": "Let me think... 2+2=4", "thought": true},
						{"text": "The answer is 4."},
					},
				},
				"finishReason": "STOP",
			}},
			"usageMetadata": map[string]any{
				"promptTokenCount":     5,
				"candidatesTokenCount": 5,
				"totalTokenCount":      20,
				"thoughtsTokenCount":   10,
			},
		})
	}))
	defer srv.Close()

	p := generativeai.New(generativeai.WithAPIKey("k"), generativeai.WithBaseURL(srv.URL))
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "gemini-2.5-flash",
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
	if result.Usage.ReasoningTokens != 10 {
		t.Errorf("reasoning tokens: got %d", result.Usage.ReasoningTokens)
	}
}

func TestDoGenerate_SystemInstruction(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			SystemInstruction *struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"systemInstruction"`
			Contents []struct {
				Role  string `json:"role"`
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"contents"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		if body.SystemInstruction == nil {
			t.Fatal("expected systemInstruction")
		}
		if len(body.SystemInstruction.Parts) != 1 || body.SystemInstruction.Parts[0].Text != "Be concise." {
			t.Errorf("unexpected systemInstruction: %+v", body.SystemInstruction)
		}

		// system messages should not appear in contents
		for _, c := range body.Contents {
			if c.Role == "system" {
				t.Error("system role should not appear in contents")
			}
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"candidates": []map[string]any{{
				"content": map[string]any{
					"role":  "model",
					"parts": []map[string]any{{"text": "OK"}},
				},
				"finishReason": "STOP",
			}},
			"usageMetadata": map[string]any{
				"promptTokenCount": 3, "candidatesTokenCount": 1, "totalTokenCount": 4,
			},
		})
	}))
	defer srv.Close()

	p := generativeai.New(generativeai.WithAPIKey("k"), generativeai.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "gemini-2.0-flash",
		System:   "Be concise.",
		Messages: []sdk.Message{sdk.UserMessage("Hi")},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
}

func TestDoGenerate_ModelPathWithSlash(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/publishers/google/models/gemini-2.0-flash:generateContent" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"candidates": []map[string]any{{
				"content":      map[string]any{"role": "model", "parts": []map[string]any{{"text": "OK"}}},
				"finishReason": "STOP",
			}},
			"usageMetadata": map[string]any{"promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2},
		})
	}))
	defer srv.Close()

	p := generativeai.New(generativeai.WithAPIKey("k"), generativeai.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "publishers/google/models/gemini-2.0-flash",
		Messages: []sdk.Message{sdk.UserMessage("Hi")},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
}

func TestDoGenerate_FinishReasonMapping(t *testing.T) {
	tests := []struct {
		googleReason string
		hasToolCalls bool
		expected     sdk.FinishReason
	}{
		{"STOP", false, sdk.FinishReasonStop},
		{"STOP", true, sdk.FinishReasonToolCalls},
		{"MAX_TOKENS", false, sdk.FinishReasonLength},
		{"SAFETY", false, sdk.FinishReasonContentFilter},
		{"RECITATION", false, sdk.FinishReasonContentFilter},
		{"IMAGE_SAFETY", false, sdk.FinishReasonContentFilter},
		{"BLOCKLIST", false, sdk.FinishReasonContentFilter},
		{"PROHIBITED_CONTENT", false, sdk.FinishReasonContentFilter},
		{"SPII", false, sdk.FinishReasonContentFilter},
		{"MALFORMED_FUNCTION_CALL", false, sdk.FinishReasonError},
		{"OTHER", false, sdk.FinishReasonOther},
		{"FINISH_REASON_UNSPECIFIED", false, sdk.FinishReasonOther},
	}

	for _, tt := range tests {
		t.Run(tt.googleReason, func(t *testing.T) {
			parts := []map[string]any{}
			if tt.hasToolCalls {
				parts = append(parts, map[string]any{
					"functionCall": map[string]any{"name": "test", "args": map[string]any{}},
				})
			} else {
				parts = append(parts, map[string]any{"text": "ok"})
			}

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{
					"candidates": []map[string]any{{
						"content":      map[string]any{"role": "model", "parts": parts},
						"finishReason": tt.googleReason,
					}},
					"usageMetadata": map[string]any{
						"promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2,
					},
				})
			}))
			defer srv.Close()

			p := generativeai.New(generativeai.WithAPIKey("k"), generativeai.WithBaseURL(srv.URL))
			result, err := p.DoGenerate(context.Background(), sdk.Request{
				Model:    "gemini-2.0-flash",
				Messages: []sdk.Message{sdk.UserMessage("test")},
			})
			if err != nil {
				t.Fatalf("DoGenerate: %v", err)
			}
			if result.FinishReason != tt.expected {
				t.Errorf("finish reason: got %q, want %q", result.FinishReason, tt.expected)
			}
			if result.RawFinishReason != tt.googleReason {
				t.Errorf("raw finish reason: got %q, want %q", result.RawFinishReason, tt.googleReason)
			}
		})
	}
}

func TestDoGenerate_NoModel(t *testing.T) {
	p := generativeai.New(generativeai.WithAPIKey("k"))
	_, err := p.DoGenerate(context.Background(), sdk.Request{})
	if err == nil {
		t.Fatal("expected error for nil model")
	}
}

func TestDoStream_NoModel(t *testing.T) {
	p := generativeai.New(generativeai.WithAPIKey("k"))
	_, err := p.DoStream(context.Background(), sdk.Request{})
	if err == nil {
		t.Fatal("expected error for nil model")
	}
}

// ---------- streaming tests ----------

func TestDoStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models/gemini-2.0-flash:streamGenerateContent" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.URL.Query().Get("alt") != "sse" {
			t.Errorf("expected alt=sse query param, got %q", r.URL.Query().Get("alt"))
		}

		w.Header().Set("Content-Type", "text/event-stream")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("server does not support flushing")
		}

		chunks := []string{
			`{"candidates":[{"content":{"role":"model","parts":[{"text":"Hello"}]}}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":1,"totalTokenCount":4}}`,
			`{"candidates":[{"content":{"role":"model","parts":[{"text":" world"}]}}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2,"totalTokenCount":5}}`,
			`{"candidates":[{"content":{"role":"model","parts":[{"text":""}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":3,"candidatesTokenCount":2,"totalTokenCount":5}}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
	}))
	defer srv.Close()

	p := generativeai.New(
		generativeai.WithAPIKey("test-key"),
		generativeai.WithBaseURL(srv.URL),
	)

	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model: "gemini-2.0-flash",
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

func TestDoStream_ToolCall(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		chunks := []string{
			`{"candidates":[{"content":{"role":"model","parts":[{"functionCall":{"name":"get_weather","args":{"location":"Tokyo"}},"thoughtSignature":"sig-tool"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":5,"totalTokenCount":15}}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
	}))
	defer srv.Close()

	p := generativeai.New(generativeai.WithAPIKey("test-key"), generativeai.WithBaseURL(srv.URL))

	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model: "gemini-2.0-flash",
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
		case *sdk.FinishPart:
			gotFinish = true
			if p.FinishReason != sdk.FinishReasonToolCalls {
				t.Errorf("finish reason: got %q", p.FinishReason)
			}
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
	} else if gotToolCall.ToolName != "get_weather" {
		t.Errorf("tool call name: got %q", gotToolCall.ToolName)
	}
	var input map[string]any
	if err := gotToolCall.Input.Unmarshal(&input); err != nil || input["location"] != "Tokyo" {
		t.Errorf("tool call input: %+v", gotToolCall.Input)
	}
	if !gotFinish {
		t.Error("missing FinishPart")
	}
	if got := gotToolCall.ProviderMetadata.Get("google", "thoughtSignature"); got != "sig-tool" {
		t.Fatalf("thoughtSignature = %q, want %s", got, "sig-tool")
	}
}

func TestDoStream_Reasoning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		chunks := []string{
			`{"candidates":[{"content":{"role":"model","parts":[{"text":"Let me think...","thought":true}]}}],"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":0,"totalTokenCount":5,"thoughtsTokenCount":3}}`,
			`{"candidates":[{"content":{"role":"model","parts":[{"text":" 2+2=4","thought":true}]}}],"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":0,"totalTokenCount":5,"thoughtsTokenCount":5}}`,
			`{"candidates":[{"content":{"role":"model","parts":[{"text":"The answer is 4."}]}}],"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":4,"totalTokenCount":14,"thoughtsTokenCount":5}}`,
			`{"candidates":[{"content":{"role":"model","parts":[]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":5,"candidatesTokenCount":4,"totalTokenCount":14,"thoughtsTokenCount":5}}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
	}))
	defer srv.Close()

	p := generativeai.New(generativeai.WithAPIKey("k"), generativeai.WithBaseURL(srv.URL))
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "gemini-2.5-flash",
		Messages: []sdk.Message{sdk.UserMessage("2+2?")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	var reasoning, text string
	var gotReasoningStart, gotReasoningEnd, gotTextStart, gotTextEnd bool
	for part := range sr {
		switch p := part.(type) {
		case *sdk.ReasoningStartPart:
			gotReasoningStart = true
		case *sdk.ReasoningDeltaPart:
			reasoning += p.Text
		case *sdk.ReasoningEndPart:
			gotReasoningEnd = true
		case *sdk.TextStartPart:
			gotTextStart = true
		case *sdk.TextDeltaPart:
			text += p.Text
		case *sdk.TextEndPart:
			gotTextEnd = true
		case *sdk.ErrorPart:
			t.Fatalf("error: %v", p.Error)
		}
	}

	if !gotReasoningStart {
		t.Error("missing ReasoningStartPart")
	}
	if !gotReasoningEnd {
		t.Error("missing ReasoningEndPart")
	}
	if !gotTextStart {
		t.Error("missing TextStartPart")
	}
	if !gotTextEnd {
		t.Error("missing TextEndPart")
	}
	if reasoning != "Let me think... 2+2=4" {
		t.Errorf("reasoning: got %q", reasoning)
	}
	if text != "The answer is 4." {
		t.Errorf("text: got %q", text)
	}
}

func TestDoStream_FlushOnAbruptEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)

		chunks := []string{
			`{"candidates":[{"content":{"role":"model","parts":[{"text":"Thinking...","thought":true}]}}]}`,
			`{"candidates":[{"content":{"role":"model","parts":[{"text":"partial"}]}}]}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
	}))
	defer srv.Close()

	p := generativeai.New(generativeai.WithAPIKey("k"), generativeai.WithBaseURL(srv.URL))
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

func TestDoGenerate_ToolChoiceNone(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ToolConfig *struct {
				FunctionCallingConfig struct {
					Mode string `json:"mode"`
				} `json:"functionCallingConfig"`
			} `json:"toolConfig"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		if body.ToolConfig == nil || body.ToolConfig.FunctionCallingConfig.Mode != "NONE" {
			t.Errorf("expected NONE mode, got %+v", body.ToolConfig)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"candidates": []map[string]any{{
				"content":      map[string]any{"role": "model", "parts": []map[string]any{{"text": "OK"}}},
				"finishReason": "STOP",
			}},
			"usageMetadata": map[string]any{"promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2},
		})
	}))
	defer srv.Close()

	p := generativeai.New(generativeai.WithAPIKey("k"), generativeai.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "gemini-2.0-flash",
		Messages: []sdk.Message{sdk.UserMessage("test")},
		Tools: []sdk.ToolDefinition{{
			Name:       "tool1",
			Parameters: &jsonschema.Schema{Type: "object"},
		}},
		ToolChoice: sdk.ToolChoice{Mode: sdk.ToolChoiceNone},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
}

func TestDoGenerate_ToolChoiceRequired(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			ToolConfig *struct {
				FunctionCallingConfig struct {
					Mode string `json:"mode"`
				} `json:"functionCallingConfig"`
			} `json:"toolConfig"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		if body.ToolConfig == nil || body.ToolConfig.FunctionCallingConfig.Mode != "ANY" {
			t.Errorf("expected ANY mode, got %+v", body.ToolConfig)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"candidates": []map[string]any{{
				"content": map[string]any{
					"role":  "model",
					"parts": []map[string]any{{"functionCall": map[string]any{"name": "tool1", "args": map[string]any{}}}},
				},
				"finishReason": "STOP",
			}},
			"usageMetadata": map[string]any{"promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2},
		})
	}))
	defer srv.Close()

	p := generativeai.New(generativeai.WithAPIKey("k"), generativeai.WithBaseURL(srv.URL))
	_, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "gemini-2.0-flash",
		Messages: []sdk.Message{sdk.UserMessage("test")},
		Tools: []sdk.ToolDefinition{{
			Name:       "tool1",
			Parameters: &jsonschema.Schema{Type: "object"},
		}},
		ToolChoice: sdk.ToolChoice{Mode: sdk.ToolChoiceRequired},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
}

func TestDoGenerate_ResponseFormatJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			GenerationConfig struct {
				ResponseMimeType string `json:"responseMimeType"`
			} `json:"generationConfig"`
		}
		json.NewDecoder(r.Body).Decode(&body)

		if body.GenerationConfig.ResponseMimeType != "application/json" {
			t.Errorf("expected application/json, got %q", body.GenerationConfig.ResponseMimeType)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"candidates": []map[string]any{{
				"content":      map[string]any{"role": "model", "parts": []map[string]any{{"text": `{"answer": 42}`}}},
				"finishReason": "STOP",
			}},
			"usageMetadata": map[string]any{"promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2},
		})
	}))
	defer srv.Close()

	p := generativeai.New(generativeai.WithAPIKey("k"), generativeai.WithBaseURL(srv.URL))
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:          "gemini-2.0-flash",
		Messages:       []sdk.Message{sdk.UserMessage("test")},
		ResponseFormat: &sdk.ResponseFormat{Type: sdk.ResponseFormatJSONObject},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	if result.Text != `{"answer": 42}` {
		t.Errorf("text: got %q", result.Text)
	}
}

func TestDoGenerate_Usage(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"candidates": []map[string]any{{
				"content":      map[string]any{"role": "model", "parts": []map[string]any{{"text": "OK"}}},
				"finishReason": "STOP",
			}},
			"usageMetadata": map[string]any{
				"promptTokenCount":        100,
				"candidatesTokenCount":    50,
				"totalTokenCount":         170,
				"cachedContentTokenCount": 20,
				"thoughtsTokenCount":      30,
			},
		})
	}))
	defer srv.Close()

	p := generativeai.New(generativeai.WithAPIKey("k"), generativeai.WithBaseURL(srv.URL))
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model:    "m",
		Messages: []sdk.Message{sdk.UserMessage("test")},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}

	u := result.Usage
	if u.InputTokens != 100 {
		t.Errorf("input tokens: got %d", u.InputTokens)
	}
	if u.OutputTokens != 80 {
		t.Errorf("output tokens: got %d, want 80 (50+30)", u.OutputTokens)
	}
	if u.CachedInputTokens != 20 {
		t.Errorf("cached input tokens: got %d", u.CachedInputTokens)
	}
	if u.ReasoningTokens != 30 {
		t.Errorf("reasoning tokens: got %d", u.ReasoningTokens)
	}
	if u.InputTokenDetails.CacheReadTokens != 20 {
		t.Errorf("input cache read: got %d", u.InputTokenDetails.CacheReadTokens)
	}
	if u.InputTokenDetails.NoCacheTokens != 80 {
		t.Errorf("input no cache: got %d, want 80 (100-20)", u.InputTokenDetails.NoCacheTokens)
	}
	if u.OutputTokenDetails.TextTokens != 50 {
		t.Errorf("output text tokens: got %d", u.OutputTokenDetails.TextTokens)
	}
	if u.OutputTokenDetails.ReasoningTokens != 30 {
		t.Errorf("output reasoning tokens: got %d", u.OutputTokenDetails.ReasoningTokens)
	}
}

func TestProviderName(t *testing.T) {
	p := generativeai.New()
	if p.Name() != "google-generative-ai" {
		t.Errorf("name: got %q", p.Name())
	}
}

// ---------- thinking config tests ----------

// capturedThinkingConfig decodes the generationConfig.thinkingConfig field from
// the request body, returning nil when the field is absent.
func capturedThinkingConfig(t *testing.T, r *http.Request) map[string]any {
	t.Helper()
	var body struct {
		GenerationConfig map[string]any `json:"generationConfig"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	if body.GenerationConfig == nil {
		return nil
	}
	tc, _ := body.GenerationConfig["thinkingConfig"].(map[string]any)
	return tc
}

func okResponse() map[string]any {
	return map[string]any{
		"candidates": []map[string]any{{
			"content":      map[string]any{"role": "model", "parts": []map[string]any{{"text": "ok"}}},
			"finishReason": "STOP",
		}},
		"usageMetadata": map[string]any{
			"promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2,
		},
	}
}

func TestDoGenerate_ThinkingConfig(t *testing.T) {
	budget := 1024
	budgetDynamic := generativeai.ThinkingBudgetDynamic
	budgetDisabled := generativeai.ThinkingBudgetDisabled
	includeTrue := true
	effortHigh := "high"
	effortMedium := "medium"
	effortXHigh := "xhigh"
	effortBlank := "   "

	tests := []struct {
		name            string
		providerOpts    []generativeai.Option
		reasoningEffort *string
		// wantAbsent is true when thinkingConfig must not appear in the request.
		wantAbsent bool
		// wantBudget is non-nil when thinkingBudget must be present and equal.
		wantBudget *int
		// wantLevel is non-empty when thinkingLevel must be present and equal.
		wantLevel string
		// wantIncludeThoughts is non-nil when includeThoughts must be present and equal.
		wantIncludeThoughts *bool
	}{
		{
			name: "WithThinking budget only",
			providerOpts: []generativeai.Option{
				generativeai.WithThinking(generativeai.ThinkingConfig{ThinkingBudget: &budget}),
			},
			wantBudget: &budget,
		},
		{
			name: "WithThinking dynamic budget constant",
			providerOpts: []generativeai.Option{
				generativeai.WithThinking(generativeai.ThinkingConfig{ThinkingBudget: &budgetDynamic}),
			},
			wantBudget: &budgetDynamic,
		},
		{
			name: "WithThinking disabled budget constant",
			providerOpts: []generativeai.Option{
				generativeai.WithThinking(generativeai.ThinkingConfig{ThinkingBudget: &budgetDisabled}),
			},
			wantBudget: &budgetDisabled,
		},
		{
			name: "WithThinking level only",
			providerOpts: []generativeai.Option{
				generativeai.WithThinking(generativeai.ThinkingConfig{ThinkingLevel: "high"}),
			},
			wantLevel: "HIGH",
		},
		{
			name: "WithThinking with IncludeThoughts",
			providerOpts: []generativeai.Option{
				generativeai.WithThinking(generativeai.ThinkingConfig{
					ThinkingLevel:   "medium",
					IncludeThoughts: &includeTrue,
				}),
			},
			wantLevel:           "MEDIUM",
			wantIncludeThoughts: &includeTrue,
		},
		{
			name:            "only ReasoningEffort maps to uppercase thinkingLevel",
			reasoningEffort: &effortHigh,
			wantLevel:       "HIGH",
		},
		{
			name:            "ReasoningEffort medium maps to thinkingLevel MEDIUM",
			reasoningEffort: &effortMedium,
			wantLevel:       "MEDIUM",
		},
		{
			name: "WithThinking overrides ReasoningEffort (provider option wins)",
			providerOpts: []generativeai.Option{
				generativeai.WithThinking(generativeai.ThinkingConfig{ThinkingBudget: &budget}),
			},
			reasoningEffort: &effortHigh, // should be ignored
			wantBudget:      &budget,
			wantLevel:       "",
		},
		{
			name:       "no thinking config when neither set",
			wantAbsent: true,
		},
		{
			// An empty WithThinking carries no intent, so it must not suppress
			// the request-level effort or emit a bare thinkingConfig: {}.
			name: "empty WithThinking falls through to ReasoningEffort",
			providerOpts: []generativeai.Option{
				generativeai.WithThinking(generativeai.ThinkingConfig{}),
			},
			reasoningEffort: &effortHigh,
			wantLevel:       "HIGH",
		},
		{
			name: "empty WithThinking with no effort omits thinkingConfig",
			providerOpts: []generativeai.Option{
				generativeai.WithThinking(generativeai.ThinkingConfig{}),
			},
			wantAbsent: true,
		},
		{
			// Already-uppercase input must survive unchanged.
			name: "WithThinking uppercase level passes through",
			providerOpts: []generativeai.Option{
				generativeai.WithThinking(generativeai.ThinkingConfig{ThinkingLevel: "LOW"}),
			},
			wantLevel: "LOW",
		},
		{
			// Tiers outside Google's enum are upper-cased and sent as-is rather
			// than being remapped to a supported neighbour; the API returns 400.
			name:            "unsupported tier is passed through uppercased",
			reasoningEffort: &effortXHigh,
			wantLevel:       "XHIGH",
		},
		{
			name:            "whitespace-only effort omits thinkingConfig",
			reasoningEffort: &effortBlank,
			wantAbsent:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var capturedTC map[string]any
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				capturedTC = capturedThinkingConfig(t, r)
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(okResponse())
			}))
			defer srv.Close()

			opts := append([]generativeai.Option{
				generativeai.WithAPIKey("k"),
				generativeai.WithBaseURL(srv.URL),
			}, tt.providerOpts...)
			p := generativeai.New(opts...)

			params := sdk.Request{
				Model:           "gemini-test",
				Messages:        []sdk.Message{sdk.UserMessage("hi")},
				ReasoningEffort: tt.reasoningEffort,
			}
			if _, err := p.DoGenerate(context.Background(), params); err != nil {
				t.Fatalf("DoGenerate: %v", err)
			}

			if tt.wantAbsent {
				if capturedTC != nil {
					t.Errorf("expected thinkingConfig to be absent, got %v", capturedTC)
				}
				return
			}

			if capturedTC == nil {
				t.Fatal("expected thinkingConfig to be present, got nil")
			}

			if tt.wantBudget != nil {
				// JSON numbers decode to float64 by default.
				got, ok := capturedTC["thinkingBudget"].(float64)
				if !ok {
					t.Fatalf("thinkingBudget: got %T %v, want float64", capturedTC["thinkingBudget"], capturedTC["thinkingBudget"])
				}
				if int(got) != *tt.wantBudget {
					t.Errorf("thinkingBudget: got %d, want %d", int(got), *tt.wantBudget)
				}
			} else {
				if _, present := capturedTC["thinkingBudget"]; present {
					t.Errorf("thinkingBudget: expected absent, got %v", capturedTC["thinkingBudget"])
				}
			}

			if tt.wantLevel != "" {
				if got, _ := capturedTC["thinkingLevel"].(string); got != tt.wantLevel {
					t.Errorf("thinkingLevel: got %q, want %q", got, tt.wantLevel)
				}
			} else {
				if v, present := capturedTC["thinkingLevel"]; present && v != "" {
					t.Errorf("thinkingLevel: expected absent or empty, got %v", v)
				}
			}

			if tt.wantIncludeThoughts != nil {
				got, ok := capturedTC["includeThoughts"].(bool)
				if !ok {
					t.Fatalf("includeThoughts: got %T %v, want bool", capturedTC["includeThoughts"], capturedTC["includeThoughts"])
				}
				if got != *tt.wantIncludeThoughts {
					t.Errorf("includeThoughts: got %v, want %v", got, *tt.wantIncludeThoughts)
				}
			} else {
				if _, present := capturedTC["includeThoughts"]; present {
					t.Errorf("includeThoughts: expected absent, got %v", capturedTC["includeThoughts"])
				}
			}
		})
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

func newIntegrationProvider(t *testing.T) *generativeai.Provider {
	t.Helper()
	apiKey := envOrSkip(t, "GOOGLE_GENERATIVE_AI_API_KEY")
	opts := []generativeai.Option{generativeai.WithAPIKey(apiKey)}
	if base := os.Getenv("GOOGLE_GENERATIVE_AI_BASE_URL"); base != "" {
		opts = append(opts, generativeai.WithBaseURL(base))
	}
	return generativeai.New(opts...)
}

func integrationModel(t *testing.T) *sdk.Model {
	t.Helper()
	m := os.Getenv("GOOGLE_GENERATIVE_AI_MODEL")
	if m == "" {
		m = "gemini-2.5-flash"
	}
	return &sdk.Model{ID: m}
}

func TestIntegration_DoGenerate(t *testing.T) {
	p := newIntegrationProvider(t)
	model := integrationModel(t)
	model.Provider = p
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: model.ID,
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
	model := integrationModel(t)
	model.Provider = p
	sr, err := p.DoStream(context.Background(), sdk.Request{
		Model: model.ID,
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

func TestIntegration_ToolCall(t *testing.T) {
	p := newIntegrationProvider(t)
	model := integrationModel(t)
	model.Provider = p
	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: model.ID,
		Messages: []sdk.Message{{
			Role:    sdk.MessageRoleUser,
			Content: []sdk.MessagePart{sdk.TextPart{Text: "What's the weather in San Francisco?"}},
		}},
		Tools: []sdk.ToolDefinition{{
			Name:        "get_weather",
			Description: "Get the weather for a location",
			Parameters: &jsonschema.Schema{
				Type: "object",
				Properties: map[string]*jsonschema.Schema{
					"location": {Type: "string", Description: "City name"},
				},
				Required: []string{"location"},
			},
		}},
		ToolChoice: sdk.ToolChoice{Mode: sdk.ToolChoiceAuto},
	})
	if err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	t.Logf("finish=%s toolCalls=%d text=%q", result.FinishReason, len(result.ToolCalls), result.Text)

	if len(result.ToolCalls) == 0 {
		t.Error("expected at least one tool call")
	}
	for _, tc := range result.ToolCalls {
		t.Logf("  tool=%q id=%s input=%v", tc.ToolName, tc.ToolCallID, tc.Input)
	}
}

func TestIntegration_ToolCallWithAdditionalPropertiesSchema(t *testing.T) {
	p := newIntegrationProvider(t)
	model := integrationModel(t)
	model.Provider = p

	result, err := p.DoGenerate(context.Background(), sdk.Request{
		Model: model.ID,
		Messages: []sdk.Message{{
			Role: sdk.MessageRoleUser,
			Content: []sdk.MessagePart{sdk.TextPart{
				Text: "Call get_weather with location San Francisco.",
			}},
		}},
		Tools: []sdk.ToolDefinition{{
			Name:        "get_weather",
			Description: "Get the weather for a location.",
			Parameters: &jsonschema.Schema{
				Type: "object",
				Properties: map[string]*jsonschema.Schema{
					"location": {Type: "string", Description: "City name"},
				},
				Required:             []string{"location"},
				AdditionalProperties: &jsonschema.Schema{Not: &jsonschema.Schema{}},
			},
		}},
		ToolChoice: sdk.ToolChoice{Mode: sdk.ToolChoiceRequired},
	})
	if err != nil {
		t.Fatalf("DoGenerate with additionalProperties in tool schema: %v", err)
	}
	t.Logf("finish=%s toolCalls=%d text=%q", result.FinishReason, len(result.ToolCalls), result.Text)

	if result.FinishReason != sdk.FinishReasonToolCalls {
		t.Errorf("finish: got %q, want %q", result.FinishReason, sdk.FinishReasonToolCalls)
	}
	if len(result.ToolCalls) == 0 {
		t.Fatal("expected at least one tool call")
	}
	tc := result.ToolCalls[0]
	t.Logf("  tool=%q id=%s input=%v", tc.ToolName, tc.ToolCallID, tc.Input)
	if tc.ToolName != "get_weather" {
		t.Errorf("tool name: got %q, want get_weather", tc.ToolName)
	}
	var input map[string]any
	if err := tc.Input.Unmarshal(&input); err != nil {
		t.Fatalf("decode input: %v", err)
	}
	if location, ok := input["location"].(string); !ok || location == "" {
		t.Errorf("location input: got %v", input["location"])
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
			"models": []map[string]any{
				{"name": "models/gemini-2.5-pro", "displayName": "Gemini 2.5 Pro"},
				{"name": "models/gemini-2.5-flash", "displayName": "Gemini 2.5 Flash"},
			},
		})
	}))
	defer srv.Close()

	p := generativeai.New(
		generativeai.WithAPIKey("test-key"),
		generativeai.WithBaseURL(srv.URL),
	)

	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels failed: %v", err)
	}
	if len(models) != 2 {
		t.Fatalf("expected 2 models, got %d", len(models))
	}
	if models[0].ID != "gemini-2.5-pro" {
		t.Errorf("expected first model 'gemini-2.5-pro', got %q", models[0].ID)
	}
	if models[0].DisplayName != "Gemini 2.5 Pro" {
		t.Errorf("expected display name 'Gemini 2.5 Pro', got %q", models[0].DisplayName)
	}
	if models[0].Type != sdk.ModelTypeChat {
		t.Errorf("expected chat type for model without supportedGenerationMethods, got %q", models[0].Type)
	}
}

func TestListModels_EmbeddingType(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"models": []map[string]any{
				{
					"name":                       "models/gemini-2.5-flash",
					"displayName":                "Gemini 2.5 Flash",
					"supportedGenerationMethods": []string{"generateContent", "countTokens"},
				},
				{
					"name":                       "models/gemini-embedding-001",
					"displayName":                "Gemini Embedding 001",
					"supportedGenerationMethods": []string{"embedContent", "countTokens"},
				},
			},
		})
	}))
	defer srv.Close()

	p := generativeai.New(
		generativeai.WithAPIKey("test-key"),
		generativeai.WithBaseURL(srv.URL),
	)

	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels failed: %v", err)
	}
	if len(models) != 2 {
		t.Fatalf("expected 2 models, got %d", len(models))
	}
	if models[0].Type != sdk.ModelTypeChat {
		t.Errorf("expected chat type for generateContent model, got %q", models[0].Type)
	}
	if models[1].Type != sdk.ModelTypeEmbedding {
		t.Errorf("expected embedding type for embedContent model, got %q", models[1].Type)
	}
}

func TestProviderTest_OK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"models": []map[string]any{},
		})
	}))
	defer srv.Close()

	p := generativeai.New(
		generativeai.WithAPIKey("test-key"),
		generativeai.WithBaseURL(srv.URL),
	)

	result := p.Test(context.Background())
	if result.Status != sdk.ProviderStatusOK {
		t.Errorf("expected status OK, got %q", result.Status)
	}
}

func TestProviderTest_Unhealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "API key not valid"}})
	}))
	defer srv.Close()

	p := generativeai.New(
		generativeai.WithAPIKey("bad-key"),
		generativeai.WithBaseURL(srv.URL),
	)

	result := p.Test(context.Background())
	if result.Status != sdk.ProviderStatusUnhealthy {
		t.Errorf("expected status Unhealthy, got %q", result.Status)
	}
}

func TestProviderTest_Unreachable(t *testing.T) {
	p := generativeai.New(
		generativeai.WithAPIKey("test-key"),
		generativeai.WithBaseURL("http://127.0.0.1:1"),
	)

	result := p.Test(context.Background())
	if result.Status != sdk.ProviderStatusUnreachable {
		t.Errorf("expected status Unreachable, got %q", result.Status)
	}
}

func TestTestModel_Supported(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models/gemini-2.5-pro" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"name": "models/gemini-2.5-pro", "displayName": "Gemini 2.5 Pro",
		})
	}))
	defer srv.Close()

	p := generativeai.New(
		generativeai.WithAPIKey("test-key"),
		generativeai.WithBaseURL(srv.URL),
	)

	result, err := p.TestModel(context.Background(), "gemini-2.5-pro")
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

	p := generativeai.New(
		generativeai.WithAPIKey("test-key"),
		generativeai.WithBaseURL(srv.URL),
	)

	result, err := p.TestModel(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("TestModel failed: %v", err)
	}
	if result.Supported {
		t.Error("expected model to not be supported")
	}
}

func TestMain(m *testing.M) {
	testutil.LoadEnv()
	os.Exit(m.Run())
}
