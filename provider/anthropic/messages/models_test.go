package messages_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"

	"github.com/felinics/twilight/provider/anthropic/messages"
	"github.com/felinics/twilight/sdk"
)

// openRouterModelPages are three pages of OpenRouter's Anthropic-compatible
// GET /v1/models, keyed by the after_id that asks for them. OpenRouter reports
// limits but no capabilities.
var openRouterModelPages = map[string]string{
	"": `{"data":[
		{"id":"anthropic/claude-sonnet-5.5[1m]","type":"model","display_name":"Anthropic: Claude Sonnet 5.5","created_at":"2026-09-28T18:04:46.586+00:00","max_input_tokens":1000000,"max_tokens":128000,"capabilities":null},
		{"id":"anthropic/deepseek/deepseek-v4.1-flash[1m]","type":"model","display_name":"DeepSeek: DeepSeek V4.1 Flash","created_at":"2026-09-10T06:21:25.18+00:00","max_input_tokens":1048576,"max_tokens":1048576,"capabilities":null}
	],"has_more":true,"first_id":"anthropic/claude-sonnet-5.5[1m]","last_id":"anthropic/deepseek/deepseek-v4.1-flash[1m]"}`,
	"anthropic/deepseek/deepseek-v4.1-flash[1m]": `{"data":[
		{"id":"anthropic/openai/gpt-5.5","type":"model","display_name":"OpenAI: GPT-5.5","created_at":"2026-04-24T17:31:33.253+00:00","max_input_tokens":922000,"max_tokens":128000,"capabilities":null},
		{"id":"anthropic/rekaai/reka-edge","type":"model","display_name":"Reka Edge","created_at":"2026-03-20T17:16:05.541274+00:00","max_input_tokens":16384,"max_tokens":16384,"capabilities":null}
	],"has_more":true,"first_id":"anthropic/openai/gpt-5.5","last_id":"anthropic/rekaai/reka-edge"}`,
	"anthropic/rekaai/reka-edge": `{"data":[
		{"id":"anthropic/openai/gpt-4o","type":"model","display_name":"OpenAI: GPT-4o","created_at":"2024-05-13T00:00:00+00:00","max_input_tokens":128000,"max_tokens":16384,"capabilities":null}
	],"has_more":false,"first_id":"anthropic/openai/gpt-4o","last_id":"anthropic/openai/gpt-4o"}`,
}

func TestListModels_Paginates(t *testing.T) {
	var (
		mu       sync.Mutex
		afterIDs []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/models" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if got := r.URL.Query().Get("limit"); got != "1000" {
			t.Errorf("limit = %q, want 1000", got)
		}
		afterID := r.URL.Query().Get("after_id")
		mu.Lock()
		afterIDs = append(afterIDs, afterID)
		mu.Unlock()
		page, ok := openRouterModelPages[afterID]
		if !ok {
			t.Errorf("unexpected after_id %q", afterID)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(page))
	}))
	defer srv.Close()

	p := messages.New(messages.WithAuthToken("test-token"), messages.WithBaseURL(srv.URL))
	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels failed: %v", err)
	}

	wantAfterIDs := []string{"", "anthropic/deepseek/deepseek-v4.1-flash[1m]", "anthropic/rekaai/reka-edge"}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(afterIDs, wantAfterIDs) {
		t.Errorf("after_id sequence = %q, want %q", afterIDs, wantAfterIDs)
	}
	want := []sdk.Model{
		{ID: "anthropic/claude-sonnet-5.5[1m]", DisplayName: "Anthropic: Claude Sonnet 5.5", Provider: p, ContextWindow: 1000000, MaxOutputTokens: 128000},
		{ID: "anthropic/deepseek/deepseek-v4.1-flash[1m]", DisplayName: "DeepSeek: DeepSeek V4.1 Flash", Provider: p, ContextWindow: 1048576, MaxOutputTokens: 1048576},
		{ID: "anthropic/openai/gpt-5.5", DisplayName: "OpenAI: GPT-5.5", Provider: p, ContextWindow: 922000, MaxOutputTokens: 128000},
		{ID: "anthropic/rekaai/reka-edge", DisplayName: "Reka Edge", Provider: p, ContextWindow: 16384, MaxOutputTokens: 16384},
		{ID: "anthropic/openai/gpt-4o", DisplayName: "OpenAI: GPT-4o", Provider: p, ContextWindow: 128000, MaxOutputTokens: 16384},
	}
	if !reflect.DeepEqual(models, want) {
		t.Errorf("models:\n got %#v\nwant %#v", models, want)
	}
}

// TestListModels_CursorDoesNotAdvance covers a host that ignores after_id and
// keeps answering has_more with the same page.
func TestListModels_CursorDoesNotAdvance(t *testing.T) {
	var (
		mu       sync.Mutex
		requests int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openRouterModelPages[""]))
	}))
	defer srv.Close()

	p := messages.New(messages.WithAPIKey("test-key"), messages.WithBaseURL(srv.URL))
	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels failed: %v", err)
	}
	if len(models) != 2 {
		t.Errorf("got %d models, want the 2 of the repeated page", len(models))
	}
	mu.Lock()
	defer mu.Unlock()
	if requests != 2 {
		t.Errorf("got %d requests, want 2", requests)
	}
}

// TestListModels_MissingCursor covers has_more without last_id: the last
// item's ID is the cursor.
func TestListModels_MissingCursor(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("after_id") == "" {
			_, _ = w.Write([]byte(`{"data":[{"id":"a","type":"model"}],"has_more":true}`))
			return
		}
		if got := r.URL.Query().Get("after_id"); got != "a" {
			t.Errorf("after_id = %q, want a", got)
		}
		_, _ = w.Write([]byte(`{"data":[{"id":"b","type":"model"}],"has_more":false}`))
	}))
	defer srv.Close()

	p := messages.New(messages.WithAPIKey("test-key"), messages.WithBaseURL(srv.URL))
	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels failed: %v", err)
	}
	if len(models) != 2 || models[0].ID != "a" || models[1].ID != "b" {
		t.Errorf("models = %#v, want a and b", models)
	}
}

// officialModelsResponse follows the Claude API ModelInfo schema with
// capabilities, trimmed to the fields the provider reads plus a few others.
const officialModelsResponse = `{"data":[
	{"type":"model","id":"claude-opus-5-5","display_name":"Claude Opus 5.5",
	 "created_at":"2026-09-21T00:00:00Z","lifecycle":"active","line":"opus",
	 "deprecated_at":null,"retires_at":null,
	 "max_input_tokens":1000000,"max_tokens":128000,
	 "capabilities":{
	  "batch":{"supported":true},"citations":{"supported":true},"code_execution":{"supported":true},
	  "context_management":{"clear_thinking_20251015":{"supported":true},"clear_tool_uses_20250919":{"supported":true},"compact_20260112":{"supported":true},"supported":true},
	  "effort":{"low":{"supported":true},"medium":{"supported":true},"high":{"supported":true},"xhigh":{"supported":true},"max":{"supported":true},"supported":true},
	  "image_input":{"supported":true},"pdf_input":{"supported":true},
	  "server_tools":{"code_execution":{"supported":true},"web_search":{"supported":true},"supported":true},
	  "structured_outputs":{"supported":true},
	  "thinking":{"supported":true,"types":{"adaptive":{"supported":true},"disabled":{"supported":false},"enabled":{"supported":false}}}
	 }},
	{"type":"model","id":"claude-sonnet-4-6","display_name":"Claude Sonnet 4.6",
	 "created_at":"2026-02-17T00:00:00Z","lifecycle":"active","line":"sonnet",
	 "max_input_tokens":1000000,"max_tokens":64000,
	 "capabilities":{
	  "effort":{"low":{"supported":true},"medium":{"supported":true},"high":{"supported":true},"xhigh":null,"max":{"supported":true},"supported":true},
	  "image_input":{"supported":true},"pdf_input":{"supported":true},
	  "structured_outputs":{"supported":true},
	  "thinking":{"supported":true,"types":{"adaptive":{"supported":true},"disabled":{"supported":true},"enabled":{"supported":true}}}
	 }},
	{"type":"model","id":"claude-haiku-4-5-20251001","display_name":"Claude Haiku 4.5",
	 "created_at":"2025-10-15T00:00:00Z","lifecycle":"active","line":"haiku",
	 "max_input_tokens":200000,"max_tokens":64000,
	 "capabilities":{
	  "effort":{"low":{"supported":false},"medium":{"supported":false},"high":{"supported":false},"xhigh":null,"max":{"supported":false},"supported":false},
	  "image_input":{"supported":true},"pdf_input":{"supported":true},
	  "structured_outputs":{"supported":true},
	  "thinking":{"supported":true,"types":{"adaptive":{"supported":false},"disabled":{"supported":true},"enabled":{"supported":true}}}
	 }},
	{"type":"model","id":"claude-3-haiku-20240307","display_name":"Claude Haiku 3",
	 "created_at":"2024-03-07T00:00:00Z","lifecycle":"deprecated",
	 "max_input_tokens":200000,"max_tokens":4096,
	 "capabilities":{
	  "effort":{"supported":false},
	  "image_input":{"supported":true},"pdf_input":{"supported":false},
	  "structured_outputs":{"supported":false},
	  "thinking":{"supported":false,"types":{"adaptive":{"supported":false},"disabled":{"supported":true},"enabled":{"supported":false}}}
	 }},
	{"type":"model","id":"odd-host-model","display_name":"Odd","max_input_tokens":"lots","capabilities":[]}
],"has_more":false,"first_id":"claude-opus-5-5","last_id":"odd-host-model"}`

func TestListModels_Capabilities(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(officialModelsResponse))
	}))
	defer srv.Close()

	p := messages.New(messages.WithAPIKey("test-key"), messages.WithBaseURL(srv.URL))
	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels failed: %v", err)
	}

	text := []sdk.Modality{sdk.ModalityText}
	all := sdk.Modalities{Input: []sdk.Modality{sdk.ModalityText, sdk.ModalityImage, sdk.ModalityPDF}, Output: text}
	want := []sdk.Model{
		{
			ID: "claude-opus-5-5", DisplayName: "Claude Opus 5.5", Provider: p,
			Modalities: all, ContextWindow: 1000000, MaxOutputTokens: 128000, StructuredOutput: true,
			Reasoning: &sdk.ModelReasoning{
				Efforts: []string{"low", "medium", "high", "xhigh", "max"},
				Off:     sdk.ReasoningOffRejected,
				Dialect: sdk.ReasoningDialectAdaptive,
			},
		},
		{
			ID: "claude-sonnet-4-6", DisplayName: "Claude Sonnet 4.6", Provider: p,
			Modalities: all, ContextWindow: 1000000, MaxOutputTokens: 64000, StructuredOutput: true,
			Reasoning: &sdk.ModelReasoning{
				Toggle:  true,
				Efforts: []string{"low", "medium", "high", "max"},
				Budget:  &sdk.ReasoningBudget{},
				Off:     sdk.ReasoningOffAccepted,
				Dialect: sdk.ReasoningDialectAdaptive,
			},
		},
		{
			ID: "claude-haiku-4-5-20251001", DisplayName: "Claude Haiku 4.5", Provider: p,
			Modalities: all, ContextWindow: 200000, MaxOutputTokens: 64000, StructuredOutput: true,
			Reasoning: &sdk.ModelReasoning{
				Toggle:  true,
				Budget:  &sdk.ReasoningBudget{},
				Off:     sdk.ReasoningOffAccepted,
				Dialect: sdk.ReasoningDialectBudget,
			},
		},
		{
			ID: "claude-3-haiku-20240307", DisplayName: "Claude Haiku 3", Provider: p,
			Modalities:    sdk.Modalities{Input: []sdk.Modality{sdk.ModalityText, sdk.ModalityImage}, Output: text},
			ContextWindow: 200000, MaxOutputTokens: 4096,
		},
		// Limits and capabilities in a shape that does not decode are dropped.
		{ID: "odd-host-model", DisplayName: "Odd", Provider: p},
	}
	if len(models) != len(want) {
		t.Fatalf("got %d models, want %d", len(models), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(models[i], want[i]) {
			t.Errorf("model %d:\n got %#v\nwant %#v", i, models[i], want[i])
			if !reflect.DeepEqual(models[i].Reasoning, want[i].Reasoning) {
				t.Errorf("reasoning:\n got %#v\nwant %#v", models[i].Reasoning, want[i].Reasoning)
			}
		}
	}
}

// A compatible host whose page cap is below 1000 rejects limit=1000; the
// listing is retried once without limit and pages at the host's default size.
func TestListModels_RetriesWithoutLimitWhenRejected(t *testing.T) {
	var (
		mu     sync.Mutex
		limits []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		limit := r.URL.Query().Get("limit")
		mu.Lock()
		limits = append(limits, limit)
		mu.Unlock()
		if limit != "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"limit: must be at most 100"}}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(openRouterModelPages[r.URL.Query().Get("after_id")]))
	}))
	defer srv.Close()

	p := messages.New(messages.WithAuthToken("test-token"), messages.WithBaseURL(srv.URL))
	models, err := p.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels: %v", err)
	}
	if len(models) != 5 {
		t.Fatalf("got %d models, want 5", len(models))
	}
	want := []string{"1000", "", "", ""}
	if !reflect.DeepEqual(limits, want) {
		t.Errorf("limit sequence = %q, want %q", limits, want)
	}
}

// Only a rejected first page is retried: a later page failing says nothing
// about the limit, so the error is returned.
func TestListModels_LaterPageErrorIsNotRetried(t *testing.T) {
	var (
		mu       sync.Mutex
		requests int
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("after_id") != "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"type":"error","error":{"type":"invalid_request_error","message":"bad cursor"}}`))
			return
		}
		_, _ = w.Write([]byte(openRouterModelPages[""]))
	}))
	defer srv.Close()

	p := messages.New(messages.WithAuthToken("test-token"), messages.WithBaseURL(srv.URL))
	_, err := p.ListModels(context.Background())
	var apiErr *sdk.APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusBadRequest {
		t.Fatalf("err = %v, want a 400 APIError", err)
	}
	if requests != 2 {
		t.Errorf("requests = %d, want 2", requests)
	}
}
