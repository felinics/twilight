package modeldiscovery

import (
	"encoding/json"
	"testing"
)

func TestIsCandidate_MistralCompletionChatCapability(t *testing.T) {
	tests := []struct {
		name         string
		id           string
		capabilities map[string]any
		want         bool
	}{
		{
			name:         "embedding capability is false",
			id:           "mistral-embed",
			capabilities: map[string]any{"completion_chat": false},
			want:         false,
		},
		{
			name:         "moderation capability is false",
			id:           "mistral-moderation-latest",
			capabilities: map[string]any{"completion_chat": false},
			want:         false,
		},
		{
			name:         "false capability overrides chat-looking id",
			id:           "mistral-small-latest",
			capabilities: map[string]any{"completion_chat": false},
			want:         false,
		},
		{
			name:         "chat capability is true",
			id:           "mistral-large-latest",
			capabilities: map[string]any{"completion_chat": true},
			want:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := Model{ID: tt.id, Capabilities: marshalRaw(t, tt.capabilities)}
			if got := IsCandidate(&model, ChatCompletions); got != tt.want {
				t.Fatalf("IsCandidate(%q, ChatCompletions) = %v, want %v", tt.id, got, tt.want)
			}
		})
	}
}

func TestIsCandidate_KnownNonChatFamilies(t *testing.T) {
	ids := []string{
		"chatgpt-image-latest",
		"gpt-transcribe",
		"gpt-transcribe-2026-01-01",
		"gpt-live-1",
		"gpt-live-transcribe",
		"sora-2",
		"provider/mistral-embed",
		"mistral-embed-2312",
		"codestral-embed-latest",
		"mistral-moderation-latest",
		"ft:babbage-002:org:project:suffix",
		"ft:davinci-002:org:project:suffix",
		"ft:gpt-3.5-turbo-instruct:org:project:suffix",
	}
	for _, endpoint := range []Endpoint{ChatCompletions, Responses} {
		for _, id := range ids {
			t.Run(string(endpoint)+"/"+id, func(t *testing.T) {
				if IsCandidate(&Model{ID: id}, endpoint) {
					t.Fatalf("IsCandidate(%q, %q) = true, want false", id, endpoint)
				}
			})
		}
	}
}

func TestIsCandidate_ResponsesOnlyFamilies(t *testing.T) {
	ids := []string{
		"computer-use-preview",
		"o1-pro",
		"o1-pro-2025-03-19",
		"o3-pro",
		"o3-deep-research",
		"o4-mini-deep-research-2025-06-26",
		"gpt-5-codex",
		"gpt-5.1-codex-max",
		"gpt-5.2-codex",
		"gpt-5-pro",
		"gpt-5.2-pro",
	}
	for _, id := range ids {
		t.Run(id, func(t *testing.T) {
			model := Model{ID: id}
			if IsCandidate(&model, ChatCompletions) {
				t.Errorf("IsCandidate(%q, ChatCompletions) = true, want false", id)
			}
			if !IsCandidate(&model, Responses) {
				t.Errorf("IsCandidate(%q, Responses) = false, want true", id)
			}
		})
	}
}

func TestIsCandidate_SupportedEndpoints(t *testing.T) {
	tests := []struct {
		name string
		raw  json.RawMessage
		want bool
	}{
		{name: "missing is unknown", want: true},
		{name: "null is unknown", raw: json.RawMessage("null"), want: true},
		{name: "malformed is unknown", raw: json.RawMessage(`{"not":"an array"}`), want: true},
		{name: "empty is explicitly unsupported", raw: json.RawMessage("[]"), want: false},
		{name: "other endpoint is unsupported", raw: marshalRaw(t, []string{"/v1/embeddings"}), want: false},
		{name: "matching endpoint is supported", raw: marshalRaw(t, []string{"/v1/chat/completions"}), want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			model := Model{ID: "unknown-model", SupportedEndpoints: tt.raw}
			if got := IsCandidate(&model, ChatCompletions); got != tt.want {
				t.Fatalf("IsCandidate() = %v, want %v", got, tt.want)
			}
		})
	}
}

func marshalRaw(t *testing.T, value any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal(%T): %v", value, err)
	}
	return raw
}
