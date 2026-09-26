package opencodego_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	opencodego "github.com/felinics/twilight/provider/opencode/go"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

var routes = []struct {
	model    string
	protocol opencodego.Protocol
	path     string
}{
	{"glm-5.2", opencodego.ProtocolCompletions, "/chat/completions"},
	{"gpt-5.6-luna", opencodego.ProtocolResponses, "/responses"},
	{"minimax-m2.7", opencodego.ProtocolMessages, "/messages"},
}

func TestGenerationAndToolContinuations(t *testing.T) {
	for _, route := range routes {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", route.protocol, stream), func(t *testing.T) {
				var requests atomic.Int32
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					step := requests.Add(1)
					if r.URL.Path != "/zen/go/v1"+route.path || r.Method != http.MethodPost {
						t.Errorf("request = %s %s", r.Method, r.URL.Path)
					}
					if r.Header.Get(opencodego.SessionHeader) != "conversation-1" || r.Header.Get("User-Agent") != "test-agent/1.0" || r.Header.Get("X-Provider") != "kept" {
						t.Errorf("headers = %v", r.Header)
					}
					if route.protocol == opencodego.ProtocolMessages {
						if r.Header.Get("x-api-key") != "key" || r.Header.Get("anthropic-version") == "" {
							t.Errorf("Anthropic auth = %v", r.Header)
						}
					} else if r.Header.Get("Authorization") != "Bearer key" {
						t.Errorf("OpenAI auth = %v", r.Header)
					}
					var body map[string]json.RawMessage
					if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
						t.Error(err)
						http.Error(w, "bad request", http.StatusBadRequest)
						return
					}
					if string(body["model"]) != fmt.Sprintf("%q", route.model) {
						t.Errorf("model = %s", body["model"])
					}
					if stream && string(body["stream"]) != "true" {
						t.Error("missing stream flag")
					}
					if len(body["tools"]) == 0 {
						t.Error("tools were dropped")
					}
					if string(body["top_p"]) != "0.5" {
						t.Errorf("provider options were not applied: top_p = %s", body["top_p"])
					}
					checkProtocolRequest(t, route.protocol, step, body)
					reply(w, route.protocol, route.model, stream, step == 1)
				}))
				defer srv.Close()
				p := opencodego.New(opencodego.WithAPIKey("key"), opencodego.WithBaseURL(srv.URL+"/zen/go/v1"), opencodego.WithHeaders(map[string]string{"User-Agent": "test-agent/1.0", "X-Provider": "kept"}))
				model := p.ChatModel(route.model)
				ctx := sdk.WithRequestHeaders(context.Background(), map[string]string{opencodego.SessionHeader: "conversation-1"})
				effort := "high"
				req := sdk.Request{
					Messages: []sdk.Message{sdk.UserMessage("hi")}, ReasoningEffort: &effort,
					Tools: []sdk.ToolDefinition{{Name: "lookup", Parameters: &jsonschema.Schema{Type: "object"}}},
					// Only this provider's namespace reaches the delegate; the
					// delegates' own namespaces would fail on the unknown member.
					ProviderOptions: map[string]json.RawMessage{
						"opencode-go":        json.RawMessage(`{"top_p":0.5}`),
						"openai-completions": json.RawMessage(`{"unknown":true}`),
						"openai-responses":   json.RawMessage(`{"unknown":true}`),
						"anthropic-messages": json.RawMessage(`{"unknown":true}`),
					},
				}
				call := func() *sdk.ModelResult {
					t.Helper()
					if !stream {
						result, err := model.Generate(ctx, req)
						if err != nil {
							t.Fatal(err)
						}
						return &result
					}
					sr, err := model.Stream(ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					for range sr.Parts {
					}
					result, err := sr.Result()
					if err != nil {
						t.Fatal(err)
					}
					return result
				}
				first := call()
				if len(first.ToolCalls) != 1 {
					t.Fatalf("first step = %+v", first)
				}
				req.Messages = append(req.Messages, stepMessages(first, "found")...)
				result := call()
				if result.Text != "done" || result.FinishReason != sdk.FinishReasonStop {
					t.Fatalf("result = %+v", result)
				}
				if requests.Load() != 2 {
					t.Errorf("requests = %d, want 2", requests.Load())
				}
				if model.Provider != p || model.ID != route.model {
					t.Error("caller model was mutated")
				}
			})
		}
	}
}

func TestConcurrentSessionIsolation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			http.Error(w, "read", http.StatusInternalServerError)
			return
		}
		var payload struct {
			Model string `json:"model"`
		}
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Error(err)
			http.Error(w, "decode", http.StatusBadRequest)
			return
		}
		session := r.Header.Get(opencodego.SessionHeader)
		if session == "" || !strings.Contains(string(body), fmt.Sprintf("%q", session)) {
			t.Errorf("session %q does not belong to request %s", session, body)
		}
		for _, route := range routes {
			if route.model == payload.Model {
				reply(w, route.protocol, payload.Model, false, false)
				return
			}
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	p := opencodego.New(opencodego.WithBaseURL(srv.URL), opencodego.WithHeaders(map[string]string{"User-Agent": "test-agent/1.0"}))
	var wg sync.WaitGroup
	for i := range 30 {
		wg.Go(func() {
			session := fmt.Sprintf("session-%d", i)
			headers := map[string]string{opencodego.SessionHeader: session}
			ctx := sdk.WithRequestHeaders(context.Background(), headers)
			headers[opencodego.SessionHeader] = "mutated"
			_, err := p.ChatModel(routes[i%len(routes)].model).Generate(ctx, sdk.Request{Messages: []sdk.Message{sdk.UserMessage(session)}})
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

func TestDiscoveryAndProbes(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path == "/models" {
			if values, ok := r.Header["Authorization"]; ok {
				t.Errorf("keyless models request sent Authorization %q", values)
			}
			io.WriteString(w, `{"data":[{"id":"glm-5.2"},{"id":"future-model"}]}`)
			return
		}
		if r.Header.Get(opencodego.SessionHeader) == "" {
			http.Error(w, "missing session", http.StatusBadRequest)
			return
		}
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
			http.Error(w, "decode", http.StatusBadRequest)
			return
		}
		for _, route := range routes {
			if body.Model == route.model {
				if r.URL.Path != route.path {
					t.Errorf("probe path = %s, want %s", r.URL.Path, route.path)
				}
				reply(w, route.protocol, route.model, false, false)
				return
			}
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	p := opencodego.New(opencodego.WithBaseURL(srv.URL))
	models, err := p.ListModels(context.Background())
	if err != nil || len(models) != 2 {
		t.Fatalf("models = %v, err = %v", models, err)
	}
	for _, model := range models {
		if model.Provider != p {
			t.Error("discovered model not bound to Go provider")
		}
	}
	if models[0].DisplayName != "GLM-5.2" || models[1].DisplayName != "" {
		t.Errorf("display names = %q, %q", models[0].DisplayName, models[1].DisplayName)
	}
	status := p.Test(context.Background())
	if status.Status != sdk.ProviderStatusOK || !strings.Contains(status.Message, "TestModel") {
		t.Fatalf("public catalog test = %+v", status)
	}
	if _, err := p.TestModel(context.Background(), "glm-5.2"); err == nil {
		t.Fatal("rejected session was treated as a working model")
	}
	ctx := sdk.WithRequestHeaders(context.Background(), map[string]string{opencodego.SessionHeader: "probe-session"})
	for _, route := range routes {
		result, err := p.TestModel(ctx, route.model)
		if err != nil || !result.Supported {
			t.Fatalf("probe %s = %+v, %v", route.model, result, err)
		}
	}
	if requests.Load() != 6 {
		t.Fatalf("requests = %d, want 6", requests.Load())
	}
}

func TestExplicitRoutesAndInputValidation(t *testing.T) {
	var requests atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		reply(w, opencodego.ProtocolMessages, "new-model", false, false)
	}))
	defer srv.Close()
	overrides := map[string]opencodego.Protocol{"new-model": opencodego.ProtocolMessages, "glm-5.2": opencodego.ProtocolMessages, "bad-model": "invalid"}
	option := opencodego.WithModelProtocols(overrides)
	overrides["new-model"] = opencodego.ProtocolResponses
	p := opencodego.New(opencodego.WithBaseURL(srv.URL), option)
	other := opencodego.New(option, opencodego.WithModelProtocols(map[string]opencodego.Protocol{"new-model": opencodego.ProtocolResponses}))
	if protocol, _ := p.ProtocolForModel("new-model"); protocol != opencodego.ProtocolMessages {
		t.Fatal("route map was not copied")
	}
	if protocol, _ := other.ProtocolForModel("new-model"); protocol != opencodego.ProtocolResponses {
		t.Fatal("override not applied")
	}
	if protocol, _ := p.ProtocolForModel("glm-5.2"); protocol != opencodego.ProtocolMessages {
		t.Fatal("built-in route not overridden")
	}
	for _, id := range []string{"", "unknown", "glm-future", "bad-model", "opencode-go/glm-5.2"} {
		req := sdk.Request{Model: id}
		if _, err := p.DoGenerate(context.Background(), req); err == nil {
			t.Errorf("generated unknown model %q", id)
		}
		if _, err := p.DoStream(context.Background(), req); err == nil {
			t.Errorf("streamed unknown model %q", id)
		}
		if _, err := p.TestModel(context.Background(), id); err == nil {
			t.Errorf("probed unknown model %q", id)
		}
	}
	if requests.Load() != 0 {
		t.Error("invalid inputs reached server")
	}
	_, err := p.DoGenerate(context.Background(), sdk.Request{Model: "new-model", Messages: []sdk.Message{sdk.UserMessage("hi")}})
	if err != nil || requests.Load() != 1 {
		t.Fatalf("explicit model: requests = %d, err = %v", requests.Load(), err)
	}
}

// stepMessages replays one step: the assistant message that made the calls,
// with its reasoning and metadata, and output as the answer to each call.
func stepMessages(r *sdk.ModelResult, output string) []sdk.Message {
	var parts []sdk.MessagePart
	for _, rp := range r.ReasoningParts {
		parts = append(parts, rp)
	}
	if r.Text != "" {
		parts = append(parts, sdk.TextPart{Text: r.Text, ProviderMetadata: r.TextProviderMetadata})
	}
	results := make([]sdk.ToolResultPart, 0, len(r.ToolCalls))
	for _, c := range r.ToolCalls {
		parts = append(parts, sdk.ToolCallPart{ToolCallID: c.ToolCallID, ToolName: c.ToolName, Input: c.Input, ProviderMetadata: c.ProviderMetadata})
		results = append(results, sdk.ToolResultPart{ToolCallID: c.ToolCallID, ToolName: c.ToolName, Result: sdk.TextOutput(output)})
	}
	return []sdk.Message{{Role: sdk.MessageRoleAssistant, Content: parts}, sdk.ToolMessage(results...)}
}

// Replies exercise real protocol parsers and real tool-replay serialization.
func reply(w http.ResponseWriter, protocol opencodego.Protocol, model string, stream, tool bool) {
	if stream {
		w.Header().Set("Content-Type", "text/event-stream")
	} else {
		w.Header().Set("Content-Type", "application/json")
	}
	event := func(name, data string) {
		var body map[string]any
		_ = json.Unmarshal([]byte(data), &body)
		body["type"] = name
		encoded, _ := json.Marshal(body)
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, encoded)
	}
	switch protocol {
	case opencodego.ProtocolCompletions:
		finish := "stop"
		message := `{"role":"assistant","content":"done"}`
		if tool {
			finish = "tool_calls"
			message = `{"role":"assistant","tool_calls":[{"index":0,"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}`
		}
		if stream {
			fmt.Fprintf(w, "data: {\"id\":\"resp-1\",\"model\":%q,\"choices\":[{\"delta\":%s,\"finish_reason\":%q}],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n", model, message, finish)
		} else {
			fmt.Fprintf(w, `{"id":"resp-1","model":%q,"choices":[{"message":%s,"finish_reason":%q}],"usage":{"prompt_tokens":4,"completion_tokens":2}}`, model, message, finish)
		}
	case opencodego.ProtocolResponses:
		output := `{"type":"message","id":"msg-1","role":"assistant","content":[{"type":"output_text","text":"done"}]}`
		if tool {
			output = `{"type":"function_call","id":"item-1","call_id":"call-1","name":"lookup","arguments":"{}"}`
		}
		if stream {
			event("response.created", fmt.Sprintf(`{"response":{"id":"resp-1","model":%q}}`, model))
			event("response.output_item.added", fmt.Sprintf(`{"output_index":0,"item":%s}`, output))
			if !tool {
				event("response.output_text.delta", `{"item_id":"msg-1","delta":"done"}`)
			}
			event("response.output_item.done", fmt.Sprintf(`{"output_index":0,"item":%s}`, output))
			event("response.completed", `{"response":{"status":"completed","usage":{"input_tokens":4,"output_tokens":2}}}`)
		} else {
			fmt.Fprintf(w, `{"id":"resp-1","model":%q,"status":"completed","output":[%s],"usage":{"input_tokens":4,"output_tokens":2}}`, model, output)
		}
	case opencodego.ProtocolMessages:
		finish := "end_turn"
		content := `{"type":"text","text":"done"}`
		if tool {
			finish = "tool_use"
			content = `{"type":"tool_use","id":"call-1","name":"lookup","input":{}}`
		}
		if stream {
			event("message_start", fmt.Sprintf(`{"message":{"id":"msg-1","model":%q,"role":"assistant","content":[],"usage":{"input_tokens":4,"output_tokens":0}}}`, model))
			if tool {
				event("content_block_start", `{"index":0,"content_block":{"type":"tool_use","id":"call-1","name":"lookup"}}`)
				event("content_block_delta", `{"index":0,"delta":{"type":"input_json_delta","partial_json":"{}"}}`)
			} else {
				event("content_block_start", `{"index":0,"content_block":{"type":"text","text":""}}`)
				event("content_block_delta", `{"index":0,"delta":{"type":"text_delta","text":"done"}}`)
			}
			event("content_block_stop", `{"index":0}`)
			event("message_delta", fmt.Sprintf(`{"delta":{"stop_reason":%q},"usage":{"output_tokens":2}}`, finish))
			event("message_stop", `{}`)
		} else {
			fmt.Fprintf(w, `{"id":"msg-1","type":"message","model":%q,"role":"assistant","content":[%s],"stop_reason":%q,"usage":{"input_tokens":4,"output_tokens":2}}`, model, content, finish)
		}
	}
}

func TestUpstreamErrorsAndCancellation(t *testing.T) {
	for _, route := range routes {
		for _, status := range []int{http.StatusUnauthorized, http.StatusTooManyRequests} {
			t.Run(fmt.Sprintf("%s/%d", route.protocol, status), func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "upstream rejected request", status) }))
				defer srv.Close()
				p := opencodego.New(opencodego.WithBaseURL(srv.URL))
				model := p.ChatModel(route.model)
				req := sdk.Request{Messages: []sdk.Message{sdk.UserMessage("hi")}}
				if _, err := model.Generate(context.Background(), req); err == nil {
					t.Error("generation ignored upstream error")
				}
				if sr, err := model.Stream(context.Background(), req); err == nil {
					for range sr.Parts {
					}
					if _, err := sr.Result(); err == nil {
						t.Error("stream ignored upstream error")
					}
				}
				if _, err := p.TestModel(context.Background(), route.model); err == nil {
					t.Error("probe ignored upstream error")
				}
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := model.Generate(ctx, req); err == nil {
					t.Error("generation ignored cancellation")
				}
			})
		}
	}
}

func checkProtocolRequest(t *testing.T, protocol opencodego.Protocol, step int32, body map[string]json.RawMessage) {
	t.Helper()
	switch protocol {
	case opencodego.ProtocolCompletions:
		if len(body["messages"]) == 0 || string(body["reasoning_effort"]) != `"high"` {
			t.Errorf("completions body = %s", body)
		}
		if step == 2 && !strings.Contains(string(body["messages"]), `"role":"tool"`) {
			t.Error("missing tool result")
		}
	case opencodego.ProtocolResponses:
		if len(body["input"]) == 0 || len(body["reasoning"]) == 0 {
			t.Errorf("responses body = %s", body)
		}
		if step == 2 && !strings.Contains(string(body["input"]), "function_call_output") {
			t.Error("missing tool result")
		}
	case opencodego.ProtocolMessages:
		if len(body["messages"]) == 0 || len(body["max_tokens"]) == 0 || len(body["output_config"]) == 0 {
			t.Errorf("messages body = %s", body)
		}
		if step == 2 && !strings.Contains(string(body["messages"]), "tool_result") {
			t.Error("missing tool result")
		}
	}
}

// Both behaviors were confirmed against the live service: some Completions
// routes reject a replayed tool call without reasoning_content, and several
// models silently ignore the developer role. Neither depends on the model name.
func TestCompletionsCompat(t *testing.T) {
	type wireMessage struct {
		Role             string  `json:"role"`
		ReasoningContent *string `json:"reasoning_content"`
	}
	seen := make(chan []wireMessage, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []wireMessage `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		seen <- body.Messages
		reply(w, opencodego.ProtocolCompletions, "model", false, false)
	}))
	defer srv.Close()
	p := opencodego.New(opencodego.WithBaseURL(srv.URL))
	history := []sdk.Message{
		sdk.DeveloperMessage("be brief"),
		sdk.UserMessage("hi"),
		{Role: sdk.MessageRoleAssistant, Content: []sdk.MessagePart{sdk.ToolCallPart{ToolCallID: "call-1", ToolName: "lookup", Input: sdk.ParseToolArguments("{}")}}},
		sdk.ToolMessage(sdk.ToolResultPart{ToolCallID: "call-1", ToolName: "lookup", Result: sdk.TextOutput("found")}),
	}
	for _, model := range []string{"deepseek-v4-pro", "glm-5.2"} {
		if _, err := p.DoGenerate(context.Background(), sdk.Request{Model: model, Messages: history}); err != nil {
			t.Fatal(err)
		}
		if history[0].Role != sdk.MessageRoleDeveloper {
			t.Fatal("caller messages were mutated")
		}
		messages := <-seen
		if len(messages) != 4 || messages[0].Role != "system" {
			t.Fatalf("%s: developer message sent as %+v", model, messages)
		}
		if messages[2].ReasoningContent == nil {
			t.Errorf("%s: replayed tool call has no reasoning_content", model)
		}
		if len(history[2].Content) != 1 {
			t.Fatal("caller message parts were mutated")
		}
	}
}

// Every catalog model must reach its documented endpoint with the request
// shape of its protocol.
func TestCatalogRoutes(t *testing.T) {
	paths := map[opencodego.Protocol]string{
		opencodego.ProtocolCompletions: "/chat/completions",
		opencodego.ProtocolResponses:   "/responses",
		opencodego.ProtocolMessages:    "/messages",
	}
	type request struct {
		path  string
		model string
	}
	seen := make(chan request, 1)
	var protocol atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		seen <- request{r.URL.Path, body.Model}
		reply(w, protocol.Load().(opencodego.Protocol), body.Model, false, false)
	}))
	defer srv.Close()
	p := opencodego.New(opencodego.WithBaseURL(srv.URL))
	ids := make(map[string]bool)
	for _, entry := range opencodego.Catalog() {
		if ids[entry.ID] {
			t.Errorf("duplicate catalog entry %q", entry.ID)
		}
		ids[entry.ID] = true
		if entry.DisplayName == "" || paths[entry.Protocol] == "" {
			t.Errorf("incomplete catalog entry %+v", entry)
			continue
		}
		protocol.Store(entry.Protocol)
		model := p.ChatModel(entry.ID)
		if model.DisplayName != entry.DisplayName {
			t.Errorf("%s: display name = %q", entry.ID, model.DisplayName)
		}
		result, err := model.Generate(context.Background(), sdk.Request{Messages: []sdk.Message{sdk.UserMessage("hi")}})
		if err != nil || result.Text != "done" {
			t.Errorf("%s: result = %+v, err = %v", entry.ID, result, err)
			continue
		}
		if got := <-seen; got.path != paths[entry.Protocol] || got.model != entry.ID {
			t.Errorf("%s: request = %+v, want path %s", entry.ID, got, paths[entry.Protocol])
		}
	}
}
