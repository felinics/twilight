package generativeai

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	sdk "github.com/felinics/twilight/sdk"
)

// Gemini binds a thought signature to the exact part that carries it — not
// only to thought parts. A response without a function call signs an ordinary
// text part (usually the last one), and Gemini 3 rejects a later request that
// fails to return it. See ai.google.dev/gemini-api/docs/generate-content/thought-signatures.

func TestParseResponseKeepsSignatureOnPlainTextPart(t *testing.T) {
	thought := true
	resp := &generateResponse{
		Candidates: []candidate{{
			Content: &content{Parts: []contentPart{
				{Text: "thinking...", Thought: &thought, ThoughtSignature: "SIG_THOUGHT"},
				{Text: "the answer", ThoughtSignature: "SIG_TEXT"},
			}},
			FinishReason: "STOP",
		}},
	}

	result, err := (&Provider{}).parseResponse(resp)
	if err != nil {
		t.Fatalf("parseResponse: %v", err)
	}

	if got := extractGoogleThoughtSignature(result.TextProviderMetadata); got != "SIG_TEXT" {
		t.Errorf("text signature: got %q, want SIG_TEXT — a signature on a plain part was dropped", got)
	}
	if len(result.ReasoningParts) != 1 {
		t.Fatalf("reasoning parts: got %d, want 1", len(result.ReasoningParts))
	}
	if got := extractGoogleThoughtSignature(result.ReasoningParts[0].ProviderMetadata); got != "SIG_THOUGHT" {
		t.Errorf("thought signature: got %q, want SIG_THOUGHT", got)
	}
}

// The signed text part must come back out on the wire with its signature in
// place. TextPart already carries ProviderMetadata on replay; this closes the
// loop from parse to request.
func TestAssistantTextSignatureRoundTrips(t *testing.T) {
	msg := sdk.Message{
		Role: sdk.MessageRoleAssistant,
		Content: []sdk.MessagePart{
			sdk.TextPart{
				Text:             "the answer",
				ProviderMetadata: googleThoughtSignatureMetadata("SIG_TEXT"),
			},
		},
	}

	converted := convertAssistantMessage(msg)
	if len(converted.Parts) != 1 {
		t.Fatalf("parts: got %d, want 1", len(converted.Parts))
	}
	if converted.Parts[0].ThoughtSignature != "SIG_TEXT" {
		t.Errorf("replayed signature: got %q, want SIG_TEXT", converted.Parts[0].ThoughtSignature)
	}
	if converted.Parts[0].Thought != nil && *converted.Parts[0].Thought {
		t.Error("a signed text part must stay an ordinary part, not become a thought")
	}
}

// Gemini can sign a part whose text is empty. The signature is the only thing
// such a part carries and it must survive parsing so it can be replayed.
func TestParseResponseKeepsSignatureOnlyParts(t *testing.T) {
	thought := true
	resp := &generateResponse{
		Candidates: []candidate{{
			Content: &content{Parts: []contentPart{
				{Text: "", Thought: &thought, ThoughtSignature: "SIG_THOUGHT_ONLY"},
				{Text: "the answer"},
				{Text: "", ThoughtSignature: "SIG_TEXT_ONLY"},
			}},
			FinishReason: "STOP",
		}},
	}

	result, err := (&Provider{}).parseResponse(resp)
	if err != nil {
		t.Fatalf("parseResponse: %v", err)
	}

	if len(result.ReasoningParts) != 1 {
		t.Fatalf("reasoning parts: got %d, want 1 for the signature-only thought part", len(result.ReasoningParts))
	}
	if got := extractGoogleThoughtSignature(result.ReasoningParts[0].ProviderMetadata); got != "SIG_THOUGHT_ONLY" {
		t.Errorf("thought signature: got %q, want SIG_THOUGHT_ONLY", got)
	}
	if result.Text != "the answer" {
		t.Errorf("text: got %q, want the answer", result.Text)
	}
	if got := extractGoogleThoughtSignature(result.TextProviderMetadata); got != "SIG_TEXT_ONLY" {
		t.Errorf("text signature: got %q, want SIG_TEXT_ONLY", got)
	}
}

// In a stream a signature closes the thought block it arrived on, as each
// thought part is its own block in a non-streamed response, and the next block
// must not inherit it. A signature-only text part still signs its text block.
func TestDoStreamSignatureClosesThoughtBlock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		chunks := []string{
			`{"candidates":[{"content":{"role":"model","parts":[{"text":"Plan A","thought":true}]}}]}`,
			`{"candidates":[{"content":{"role":"model","parts":[{"text":"","thought":true,"thoughtSignature":"SIG_A"}]}}]}`,
			`{"candidates":[{"content":{"role":"model","parts":[{"text":"Plan B","thought":true}]}}]}`,
			`{"candidates":[{"content":{"role":"model","parts":[{"text":"answer"}]}}]}`,
			`{"candidates":[{"content":{"role":"model","parts":[{"text":"","thoughtSignature":"SIG_TEXT"}]},"finishReason":"STOP"}]}`,
		}
		for _, c := range chunks {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
	}))
	defer srv.Close()

	p := New(WithAPIKey("k"), WithBaseURL(srv.URL))
	stream, err := p.DoStream(context.Background(), sdk.Request{
		Model:    "gemini-3-pro-preview",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
	})
	if err != nil {
		t.Fatalf("DoStream: %v", err)
	}

	var reasoningEnds []string
	reasoningText := map[string]string{}
	var textSig, text string
	for part := range stream {
		switch v := part.(type) {
		case *sdk.ReasoningDeltaPart:
			reasoningText[v.ID] += v.Text
		case *sdk.ReasoningEndPart:
			reasoningEnds = append(reasoningEnds, v.ID+"="+reasoningText[v.ID]+"|"+extractGoogleThoughtSignature(v.ProviderMetadata))
		case *sdk.TextDeltaPart:
			text += v.Text
		case *sdk.TextEndPart:
			textSig = extractGoogleThoughtSignature(v.ProviderMetadata)
		case *sdk.ErrorPart:
			t.Fatalf("stream error: %v", v.Error)
		}
	}

	if len(reasoningEnds) != 2 {
		t.Fatalf("reasoning blocks: got %v, want two blocks", reasoningEnds)
	}
	if reasoningEnds[0] != "0=Plan A|SIG_A" {
		t.Errorf("first block: got %q, want Plan A signed SIG_A", reasoningEnds[0])
	}
	if reasoningEnds[1] != "1=Plan B|" {
		t.Errorf("second block: got %q, want Plan B with no inherited signature", reasoningEnds[1])
	}
	if text != "answer" || textSig != "SIG_TEXT" {
		t.Errorf("text block: got %q signed %q, want answer signed SIG_TEXT", text, textSig)
	}
}
