package messages_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/provider/anthropic/messages"
	"github.com/felinics/twilight/sdk"
)

// capturedThinking mirrors the wire shape of the request fields this file cares
// about, so assertions run against the JSON that actually leaves the process
// rather than against internal state.
type capturedThinking struct {
	MaxTokens int `json:"max_tokens"`
	Thinking  *struct {
		Type         string `json:"type"`
		BudgetTokens *int   `json:"budget_tokens"`
		Display      string `json:"display"`
	} `json:"thinking"`
	OutputConfig *struct {
		Effort string `json:"effort"`
	} `json:"output_config"`
}

// captureRequest runs one DoGenerate against a stub server and returns the
// decoded request body.
func captureRequest(t *testing.T, opts []messages.Option, params sdk.Request) capturedThinking {
	t.Helper()

	var captured capturedThinking
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&captured); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "msg_capture", "type": "message", "model": "claude-test", "role": "assistant",
			"content":     []map[string]any{{"type": "text", "text": "OK"}},
			"stop_reason": "end_turn",
			"usage":       map[string]any{"input_tokens": 1, "output_tokens": 1},
		})
	}))
	defer srv.Close()

	all := append([]messages.Option{
		messages.WithAPIKey("test-key"),
		messages.WithBaseURL(srv.URL),
	}, opts...)

	if params.Model == "" {
		params.Model = "claude-test"
	}
	if params.Messages == nil {
		params.Messages = []sdk.Message{sdk.UserMessage("Hi")}
	}

	if _, err := messages.New(all...).DoGenerate(context.Background(), params); err != nil {
		t.Fatalf("DoGenerate: %v", err)
	}
	return captured
}

func effortPtr(s string) *string { return &s }

func intPtr(i int) *int { return &i }

// TestResolveMaxTokens_Matrix pins the max_tokens arithmetic across every
// combination of thinking mode and reasoning effort.
//
// This is the regression surface flagged in the design doc: max_tokens is
// derived from whether thinking is active and whether it carries an explicit
// budget. Getting it wrong silently truncates thinking output on legacy Claude
// models, which no other assertion in this package would catch. The expected
// values here were captured from the pre-union implementation and must not
// change.
func TestResolveMaxTokens_Matrix(t *testing.T) {
	const (
		defaultMax   = 4096  // no reasoning at all
		reasoningMax = 32000 // reasoning active without an explicit budget
	)

	tests := []struct {
		name          string
		opts          []messages.Option
		effort        *string
		explicitMax   *int
		wantMaxTokens int
	}{
		// --- enabled: budget is added on top of the answer budget ---
		{
			name:          "enabled/budget 1024",
			opts:          []messages.Option{messages.WithThinkingEnabled(1024)},
			wantMaxTokens: defaultMax + 1024,
		},
		{
			name:          "enabled/budget 5000",
			opts:          []messages.Option{messages.WithThinkingEnabled(5000)},
			wantMaxTokens: defaultMax + 5000,
		},
		{
			name:          "enabled/budget 50000",
			opts:          []messages.Option{messages.WithThinkingEnabled(50000)},
			wantMaxTokens: defaultMax + 50000,
		},
		{
			// A zero budget is not an explicit budget, so this falls through to
			// the reasoning-aware default rather than staying at 4096.
			name:          "enabled/budget 0",
			opts:          []messages.Option{messages.WithThinkingEnabled(0)},
			wantMaxTokens: reasoningMax,
		},
		{
			name:          "enabled/budget 8000 + effort",
			opts:          []messages.Option{messages.WithThinkingEnabled(8000)},
			effort:        effortPtr("high"),
			wantMaxTokens: defaultMax + 8000,
		},

		// --- adaptive: never carries a budget ---
		{
			name:          "adaptive",
			opts:          []messages.Option{messages.WithThinkingAdaptive()},
			wantMaxTokens: reasoningMax,
		},
		{
			name:          "adaptive + effort",
			opts:          []messages.Option{messages.WithThinkingAdaptive()},
			effort:        effortPtr("xhigh"),
			wantMaxTokens: reasoningMax,
		},

		// --- disabled: reasoning is off, so the plain default applies ---
		{
			name:          "disabled",
			opts:          []messages.Option{messages.WithThinkingDisabled()},
			wantMaxTokens: defaultMax,
		},
		{
			// output_config.effort still counts as reasoning even when the
			// thinking block is explicitly disabled.
			name:          "disabled + effort",
			opts:          []messages.Option{messages.WithThinkingDisabled()},
			effort:        effortPtr("medium"),
			wantMaxTokens: reasoningMax,
		},

		// --- effort only, no thinking option ---
		{
			name:          "effort only",
			effort:        effortPtr("high"),
			wantMaxTokens: reasoningMax,
		},
		{
			name:          "effort blank is not reasoning",
			effort:        effortPtr("   "),
			wantMaxTokens: defaultMax,
		},

		// --- neither ---
		{
			name:          "no thinking, no effort",
			wantMaxTokens: defaultMax,
		},

		// --- explicit MaxTokens always wins ---
		{
			name:          "explicit max_tokens beats enabled budget",
			opts:          []messages.Option{messages.WithThinkingEnabled(5000)},
			explicitMax:   intPtr(777),
			wantMaxTokens: 777,
		},
		{
			name:          "explicit max_tokens beats adaptive",
			opts:          []messages.Option{messages.WithThinkingAdaptive()},
			explicitMax:   intPtr(888),
			wantMaxTokens: 888,
		},
		{
			name:          "explicit max_tokens beats effort",
			effort:        effortPtr("high"),
			explicitMax:   intPtr(999),
			wantMaxTokens: 999,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := captureRequest(t, tc.opts, sdk.Request{
				ReasoningEffort: tc.effort,
				MaxTokens:       tc.explicitMax,
			})
			if got.MaxTokens != tc.wantMaxTokens {
				t.Errorf("max_tokens: got %d, want %d", got.MaxTokens, tc.wantMaxTokens)
			}
		})
	}
}

// TestThinkingWireShape checks what each option actually serializes to,
// independent of the max_tokens arithmetic.
func TestThinkingWireShape(t *testing.T) {
	tests := []struct {
		name           string
		opts           []messages.Option
		wantAbsent     bool
		wantType       string
		wantBudget     *int
		wantBudgetSent bool
	}{
		{
			name:           "enabled sends type and budget",
			opts:           []messages.Option{messages.WithThinkingEnabled(5000)},
			wantType:       "enabled",
			wantBudget:     intPtr(5000),
			wantBudgetSent: true,
		},
		{
			name:     "adaptive sends type only",
			opts:     []messages.Option{messages.WithThinkingAdaptive()},
			wantType: "adaptive",
		},
		{
			// Off is sent explicitly: Claude 5 models think when the block is
			// absent, so omitting it cannot turn thinking off there.
			name:     "disabled sends an explicit disabled block",
			opts:     []messages.Option{messages.WithThinkingDisabled()},
			wantType: "disabled",
		},
		{
			name:     "between tools sends its own type",
			opts:     []messages.Option{messages.WithThinkingBetweenTools()},
			wantType: "between_tools",
		},
		{
			name:       "no option omits the thinking block",
			wantAbsent: true,
		},
		{
			name:     "last thinking option wins",
			opts:     []messages.Option{messages.WithThinkingEnabled(5000), messages.WithThinkingAdaptive()},
			wantType: "adaptive",
		},
		{
			name:     "adaptive then disabled sends disabled",
			opts:     []messages.Option{messages.WithThinkingAdaptive(), messages.WithThinkingDisabled()},
			wantType: "disabled",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := captureRequest(t, tc.opts, sdk.Request{})

			if tc.wantAbsent {
				if got.Thinking != nil {
					t.Fatalf("thinking should be absent, got %+v", *got.Thinking)
				}
				return
			}

			if got.Thinking == nil {
				t.Fatalf("thinking should be present, got nil")
			}
			if got.Thinking.Type != tc.wantType {
				t.Errorf("thinking.type: got %q, want %q", got.Thinking.Type, tc.wantType)
			}

			switch {
			case tc.wantBudgetSent:
				if got.Thinking.BudgetTokens == nil {
					t.Errorf("budget_tokens: got absent, want %d", *tc.wantBudget)
				} else if *got.Thinking.BudgetTokens != *tc.wantBudget {
					t.Errorf("budget_tokens: got %d, want %d", *got.Thinking.BudgetTokens, *tc.wantBudget)
				}
			default:
				if got.Thinking.BudgetTokens != nil {
					t.Errorf("budget_tokens must be omitted for %s, got %d", tc.wantType, *got.Thinking.BudgetTokens)
				}
			}
		})
	}
}

// TestWithThinking_BackwardCompatibility proves the legacy flat-struct API
// still produces byte-identical wire output after the union refactor.
//
// ThinkingConfig and WithThinking are exported and in use downstream (Memoh
// constructs them by generation), so the union is additive: the old path must
// keep working, not merely compile.
func TestWithThinking_BackwardCompatibility(t *testing.T) {
	tests := []struct {
		name   string
		legacy messages.ThinkingConfig
		modern []messages.Option
	}{
		{
			name:   "enabled with budget",
			legacy: messages.ThinkingConfig{Type: "enabled", BudgetTokens: 5000},
			modern: []messages.Option{messages.WithThinkingEnabled(5000)},
		},
		{
			name:   "adaptive",
			legacy: messages.ThinkingConfig{Type: "adaptive"},
			modern: []messages.Option{messages.WithThinkingAdaptive()},
		},
		{
			name:   "disabled",
			legacy: messages.ThinkingConfig{Type: "disabled"},
			modern: []messages.Option{messages.WithThinkingDisabled()},
		},
		{
			name:   "between tools",
			legacy: messages.ThinkingConfig{Type: "between_tools"},
			modern: []messages.Option{messages.WithThinkingBetweenTools()},
		},
		{
			name:   "adaptive with display",
			legacy: messages.ThinkingConfig{Type: "adaptive", Display: messages.ThinkingDisplaySummarized},
			modern: []messages.Option{messages.WithThinkingAdaptive(), messages.WithThinkingDisplay(messages.ThinkingDisplaySummarized)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			legacy := captureRequest(t, []messages.Option{messages.WithThinking(tc.legacy)}, sdk.Request{})
			modern := captureRequest(t, tc.modern, sdk.Request{})

			legacyJSON, err := json.Marshal(legacy)
			if err != nil {
				t.Fatalf("marshal legacy: %v", err)
			}
			modernJSON, err := json.Marshal(modern)
			if err != nil {
				t.Fatalf("marshal modern: %v", err)
			}
			if string(legacyJSON) != string(modernJSON) {
				t.Errorf("wire output diverged:\n legacy = %s\n modern = %s", legacyJSON, modernJSON)
			}
		})
	}
}

// TestWithThinking_LegacyExactWire pins the exact bytes the legacy call path
// produced before the union existed, so a refactor cannot quietly change what
// downstream callers send.
func TestWithThinking_LegacyExactWire(t *testing.T) {
	got := captureRequest(t,
		[]messages.Option{messages.WithThinking(messages.ThinkingConfig{Type: "enabled", BudgetTokens: 5000})},
		sdk.Request{},
	)

	if got.Thinking == nil {
		t.Fatal("thinking block missing")
	}
	if got.Thinking.Type != "enabled" {
		t.Errorf("thinking.type: got %q, want %q", got.Thinking.Type, "enabled")
	}
	if got.Thinking.BudgetTokens == nil || *got.Thinking.BudgetTokens != 5000 {
		t.Errorf("budget_tokens: got %v, want 5000", got.Thinking.BudgetTokens)
	}
	if got.MaxTokens != 4096+5000 {
		t.Errorf("max_tokens: got %d, want %d", got.MaxTokens, 4096+5000)
	}
	if got.OutputConfig != nil {
		t.Errorf("output_config should be absent, got %+v", *got.OutputConfig)
	}
}

// TestWithThinking_LegacyUnknownType keeps the pre-union passthrough behaviour
// for type strings the SDK does not recognise: they are forwarded verbatim and
// the server decides. The SDK does not validate what a caller declared.
func TestWithThinking_LegacyUnknownType(t *testing.T) {
	got := captureRequest(t,
		[]messages.Option{messages.WithThinking(messages.ThinkingConfig{Type: "future_mode", BudgetTokens: 1234})},
		sdk.Request{},
	)

	if got.Thinking == nil {
		t.Fatal("thinking block missing")
	}
	if got.Thinking.Type != "future_mode" {
		t.Errorf("thinking.type: got %q, want %q", got.Thinking.Type, "future_mode")
	}
	if got.Thinking.BudgetTokens == nil || *got.Thinking.BudgetTokens != 1234 {
		t.Errorf("budget_tokens: got %v, want 1234", got.Thinking.BudgetTokens)
	}
	if got.MaxTokens != 4096+1234 {
		t.Errorf("max_tokens: got %d, want %d", got.MaxTokens, 4096+1234)
	}
}

// TestWithThinking_LegacyEmptyType covers the zero-value ThinkingConfig, which
// the pre-union code treated as "no thinking configured".
func TestWithThinking_LegacyEmptyType(t *testing.T) {
	got := captureRequest(t,
		[]messages.Option{messages.WithThinking(messages.ThinkingConfig{})},
		sdk.Request{},
	)

	if got.Thinking != nil {
		t.Errorf("thinking should be absent for a zero-value config, got %+v", *got.Thinking)
	}
	if got.MaxTokens != 4096 {
		t.Errorf("max_tokens: got %d, want 4096", got.MaxTokens)
	}
}

// TestThinkingDisplay checks that display rides on the thinking modes that
// produce thinking text and is rejected before sending on the modes that take
// no other field.
func TestThinkingDisplay(t *testing.T) {
	t.Run("adaptive summarized", func(t *testing.T) {
		got := captureRequest(t, []messages.Option{
			messages.WithThinkingAdaptive(),
			messages.WithThinkingDisplay(messages.ThinkingDisplaySummarized),
		}, sdk.Request{})
		if got.Thinking == nil || got.Thinking.Type != "adaptive" || got.Thinking.Display != "summarized" {
			t.Fatalf("thinking: got %+v, want adaptive with display summarized", got.Thinking)
		}
	})
	t.Run("display survives a later mode option", func(t *testing.T) {
		got := captureRequest(t, []messages.Option{
			messages.WithThinkingDisplay(messages.ThinkingDisplayOmitted),
			messages.WithThinkingEnabled(4000),
		}, sdk.Request{})
		if got.Thinking == nil || got.Thinking.Type != "enabled" || got.Thinking.Display != "omitted" {
			t.Fatalf("thinking: got %+v, want enabled with display omitted", got.Thinking)
		}
	})
	t.Run("no display sends none", func(t *testing.T) {
		got := captureRequest(t, []messages.Option{messages.WithThinkingAdaptive()}, sdk.Request{})
		if got.Thinking == nil || got.Thinking.Display != "" {
			t.Fatalf("thinking: got %+v, want no display", got.Thinking)
		}
	})

	for _, tc := range []struct {
		name string
		opts []messages.Option
	}{
		{"with disabled", []messages.Option{messages.WithThinkingDisabled(), messages.WithThinkingDisplay(messages.ThinkingDisplaySummarized)}},
		{"with between tools", []messages.Option{messages.WithThinkingBetweenTools(), messages.WithThinkingDisplay(messages.ThinkingDisplaySummarized)}},
		{"without a thinking mode", []messages.Option{messages.WithThinkingDisplay(messages.ThinkingDisplaySummarized)}},
	} {
		t.Run("rejected "+tc.name, func(t *testing.T) {
			sent := false
			srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { sent = true }))
			defer srv.Close()
			opts := append([]messages.Option{messages.WithAPIKey("k"), messages.WithBaseURL(srv.URL)}, tc.opts...)
			_, err := messages.New(opts...).DoGenerate(context.Background(), sdk.Request{
				Model:    "claude-test",
				Messages: []sdk.Message{sdk.UserMessage("Hi")},
			})
			if err == nil {
				t.Fatal("DoGenerate: want an error for display without a thinking mode that shows text")
			}
			if sent {
				t.Error("the request must fail before it is sent")
			}
		})
	}
}

// TestInterleavedThinkingHeader checks the beta is added on opt-in only and
// merged into betas set elsewhere instead of replacing them.
func TestInterleavedThinkingHeader(t *testing.T) {
	capture := func(t *testing.T, opts ...messages.Option) string {
		t.Helper()
		var beta string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			beta = r.Header.Get("anthropic-beta")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"id": "msg", "type": "message", "model": "claude-test", "role": "assistant",
				"content":     []map[string]any{{"type": "text", "text": "OK"}},
				"stop_reason": "end_turn",
				"usage":       map[string]any{"input_tokens": 1, "output_tokens": 1},
			})
		}))
		defer srv.Close()
		all := append([]messages.Option{messages.WithAPIKey("k"), messages.WithBaseURL(srv.URL)}, opts...)
		if _, err := messages.New(all...).DoGenerate(context.Background(), sdk.Request{
			Model:    "claude-test",
			Messages: []sdk.Message{sdk.UserMessage("Hi")},
		}); err != nil {
			t.Fatalf("DoGenerate: %v", err)
		}
		return beta
	}

	if got := capture(t, messages.WithThinkingEnabled(4000)); got != "" {
		t.Errorf("default: got anthropic-beta %q, want none", got)
	}
	if got := capture(t, messages.WithThinkingEnabled(4000), messages.WithInterleavedThinking()); got != "interleaved-thinking-2025-05-14" {
		t.Errorf("opt-in: got anthropic-beta %q", got)
	}
	got := capture(t,
		messages.WithHeaders(map[string]string{"anthropic-beta": "context-1m-2025-08-07"}),
		messages.WithInterleavedThinking(),
	)
	if got != "context-1m-2025-08-07,interleaved-thinking-2025-05-14" {
		t.Errorf("merged: got anthropic-beta %q, want the existing beta kept", got)
	}
	got = capture(t,
		messages.WithHeaders(map[string]string{"Anthropic-Beta": "interleaved-thinking-2025-05-14"}),
		messages.WithInterleavedThinking(),
	)
	if got != "interleaved-thinking-2025-05-14" {
		t.Errorf("deduplicated: got anthropic-beta %q", got)
	}
}
