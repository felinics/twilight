package completions_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/felinics/twilight/provider/openai/completions"
	"github.com/felinics/twilight/sdk"
)

// The chunks below were recorded from OpenRouter on 2026-10-13: streamed steps
// that reason and then call a tool, from Gemini (Google Vertex), GPT (OpenAI)
// and Claude (Anthropic), and a Gemini step that answers in text. Opaque values (data, signature) are cut to
// their first 24 characters, long reasoning texts to their first words, and
// GPT's summary keeps four of its 91 pieces; everything else is as the
// endpoint sent it.

// Gemini: text pieces at index 0, then the function call's thought signature
// as a reasoning.encrypted item that also sits at index 0, with the tool
// call's id.
var geminiToolCallChunks = []string{
	`{"id":"gen-1791891805-CpoMkkLsvH5EhAaNMih4","object":"chat.completion.chunk","created":1791891805,"model":"google/gemini-3.8-flash","provider":"Google","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":"**Solving the four-digit number puzzle**\n\n","reasoning_details":[{"type":"reasoning.text","text":"**Solving the four-digit number puzzle**\n\n","format":"google-gemini-v1","index":0}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891805-CpoMkkLsvH5EhAaNMih4","object":"chat.completion.chunk","created":1791891805,"model":"google/gemini-3.8-flash","provider":"Google","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":"**Determining the leading digit**\n\n","reasoning_details":[{"type":"reasoning.text","text":"**Determining the leading digit**\n\n","format":"google-gemini-v1","index":0}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891805-CpoMkkLsvH5EhAaNMih4","object":"chat.completion.chunk","created":1791891805,"model":"google/gemini-3.8-flash","provider":"Google","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":null,"reasoning_details":[{"type":"reasoning.encrypted","data":"AY89a19NjHDETYGOfpG8ryHu","format":"google-gemini-v1","id":"call_99412","index":0}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891805-CpoMkkLsvH5EhAaNMih4","object":"chat.completion.chunk","created":1791891805,"model":"google/gemini-3.8-flash","provider":"Google","choices":[{"index":0,"delta":{"content":null,"role":"assistant","tool_calls":[{"index":0,"id":"call_99412","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891805-CpoMkkLsvH5EhAaNMih4","object":"chat.completion.chunk","created":1791891805,"model":"google/gemini-3.8-flash","provider":"Google","choices":[{"index":0,"delta":{"content":null,"role":"assistant","tool_calls":[{"index":0,"function":{"arguments":"{\"city\":\"2178\"}"}}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891805-CpoMkkLsvH5EhAaNMih4","object":"chat.completion.chunk","created":1791891805,"model":"google/gemini-3.8-flash","provider":"Google","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":null},"finish_reason":"tool_calls","native_finish_reason":"STOP"}]}`,
	`{"id":"gen-1791891805-CpoMkkLsvH5EhAaNMih4","object":"chat.completion.chunk","created":1791891805,"model":"google/gemini-3.8-flash","provider":"Google","service_tier":"default","choices":[{"index":0,"delta":{"content":"","role":"assistant"},"finish_reason":"tool_calls","native_finish_reason":"STOP"}],"usage":{"prompt_tokens":61,"completion_tokens":728,"total_tokens":789}}`,
}

// GPT: summary pieces at index 0, then the encrypted reasoning item at index
// 1 with the Responses item id.
var openAIToolCallChunks = []string{
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":"**Calculating multiplication**\n\nI","reasoning_details":[{"type":"reasoning.summary","summary":"**Calculating multiplication**\n\nI","format":"openai-responses-v1","index":0}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":" need","reasoning_details":[{"type":"reasoning.summary","summary":" need","format":"openai-responses-v1","index":0}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":" to","reasoning_details":[{"type":"reasoning.summary","summary":" to","format":"openai-responses-v1","index":0}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":" solve","reasoning_details":[{"type":"reasoning.summary","summary":" solve","format":"openai-responses-v1","index":0}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":null,"reasoning_details":[{"type":"reasoning.encrypted","data":"gAAAAABqzhmrLgaNgoqd-quY","format":"openai-responses-v1","id":"rs_0ff0d18852b88860016ace19a964a887d1900992abf9056ca6","index":1}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","choices":[{"index":0,"delta":{"content":null,"role":"assistant","tool_calls":[{"index":0,"id":"call_0FCCd3LFkwY8BbYslmRTygJy","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","choices":[{"index":0,"delta":{"content":null,"role":"assistant","tool_calls":[{"index":0,"function":{"arguments":"{\""}}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","choices":[{"index":0,"delta":{"content":null,"role":"assistant","tool_calls":[{"index":0,"function":{"arguments":"city"}}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","choices":[{"index":0,"delta":{"content":null,"role":"assistant","tool_calls":[{"index":0,"function":{"arguments":"\":\""}}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","choices":[{"index":0,"delta":{"content":null,"role":"assistant","tool_calls":[{"index":0,"function":{"arguments":"217"}}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","choices":[{"index":0,"delta":{"content":null,"role":"assistant","tool_calls":[{"index":0,"function":{"arguments":"8"}}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","choices":[{"index":0,"delta":{"content":null,"role":"assistant","tool_calls":[{"index":0,"function":{"arguments":"\"}"}}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":null},"finish_reason":"tool_calls","native_finish_reason":"completed"}]}`,
	`{"id":"gen-1791891874-V1fepvlpswwb18CQaTXK","object":"chat.completion.chunk","created":1791891874,"model":"openai/gpt-6.1-sol","provider":"OpenAI","service_tier":"default","choices":[{"index":0,"delta":{"content":"","role":"assistant"},"finish_reason":"tool_calls","native_finish_reason":"completed"}],"usage":{"prompt_tokens":86,"completion_tokens":47,"total_tokens":133}}`,
}

// Claude: text pieces at index 0, then the block's signature as a piece of
// the same reasoning.text item, with no text and no reasoning key.
var anthropicToolCallChunks = []string{
	`{"id":"gen-1791891933-9XxgWcDHr1gHmVmYJviM","object":"chat.completion.chunk","created":1791891933,"model":"anthropic/claude-haiku-5.5","provider":"Anthropic","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":"I need ","reasoning_details":[{"type":"reasoning.text","text":"I need ","format":"anthropic-claude-v1","index":0}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891933-9XxgWcDHr1gHmVmYJviM","object":"chat.completion.chunk","created":1791891933,"model":"anthropic/claude-haiku-5.5","provider":"Anthropic","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":"ABCD × 4 =","reasoning_details":[{"type":"reasoning.text","text":"ABCD × 4 =","format":"anthropic-claude-v1","index":0}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891933-9XxgWcDHr1gHmVmYJviM","object":"chat.completion.chunk","created":1791891933,"model":"anthropic/claude-haiku-5.5","provider":"Anthropic","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":" DCBA, a class","reasoning_details":[{"type":"reasoning.text","text":" DCBA, a class","format":"anthropic-claude-v1","index":0}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891933-9XxgWcDHr1gHmVmYJviM","object":"chat.completion.chunk","created":1791891933,"model":"anthropic/claude-haiku-5.5","provider":"Anthropic","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":"\n\n","reasoning_details":[{"type":"reasoning.text","text":"\n\n","format":"anthropic-claude-v1","index":0}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891933-9XxgWcDHr1gHmVmYJviM","object":"chat.completion.chunk","created":1791891933,"model":"anthropic/claude-haiku-5.5","provider":"Anthropic","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning_details":[{"type":"reasoning.text","signature":"CAQSlQgKEAgSGAI4AUIIdGhp","format":"anthropic-claude-v1","index":0}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891933-9XxgWcDHr1gHmVmYJviM","object":"chat.completion.chunk","created":1791891933,"model":"anthropic/claude-haiku-5.5","provider":"Anthropic","choices":[{"index":0,"delta":{"content":null,"role":"assistant","tool_calls":[{"index":0,"id":"toolu_01GQsisNYbv2pj5qtKGU3nJH","type":"function","function":{"name":"get_weather","arguments":""}}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891933-9XxgWcDHr1gHmVmYJviM","object":"chat.completion.chunk","created":1791891933,"model":"anthropic/claude-haiku-5.5","provider":"Anthropic","choices":[{"index":0,"delta":{"content":null,"role":"assistant","tool_calls":[{"index":0,"function":{"arguments":""}}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891933-9XxgWcDHr1gHmVmYJviM","object":"chat.completion.chunk","created":1791891933,"model":"anthropic/claude-haiku-5.5","provider":"Anthropic","choices":[{"index":0,"delta":{"content":null,"role":"assistant","tool_calls":[{"index":0,"function":{"arguments":"{\"city\": \"2178"}}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891933-9XxgWcDHr1gHmVmYJviM","object":"chat.completion.chunk","created":1791891933,"model":"anthropic/claude-haiku-5.5","provider":"Anthropic","choices":[{"index":0,"delta":{"content":null,"role":"assistant","tool_calls":[{"index":0,"function":{"arguments":"\"}"}}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791891933-9XxgWcDHr1gHmVmYJviM","object":"chat.completion.chunk","created":1791891933,"model":"anthropic/claude-haiku-5.5","provider":"Anthropic","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":null},"finish_reason":"tool_calls","native_finish_reason":"tool_use"}]}`,
	`{"id":"gen-1791891933-9XxgWcDHr1gHmVmYJviM","object":"chat.completion.chunk","created":1791891933,"model":"anthropic/claude-haiku-5.5","provider":"Anthropic","service_tier":"default","choices":[{"index":0,"delta":{"content":"","role":"assistant"},"finish_reason":"tool_calls","native_finish_reason":"tool_use"}],"usage":{"prompt_tokens":446,"completion_tokens":231,"total_tokens":677}}`,
}

// Gemini answering without reasoning text: the text part's thought signature
// comes as a reasoning.text item after the text has streamed.
var geminiTextSignatureChunks = []string{
	`{"id":"gen-1791888292-7tMGAOxwZLOZh4kbqwWT","object":"chat.completion.chunk","created":1791888292,"model":"google/gemini-3.8-flash","provider":"Google","choices":[{"index":0,"delta":{"content":"新加坡现在","role":"assistant"},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791888292-7tMGAOxwZLOZh4kbqwWT","object":"chat.completion.chunk","created":1791888292,"model":"google/gemini-3.8-flash","provider":"Google","choices":[{"index":0,"delta":{"content":"有雷阵雨，气温约为31摄氏度。","role":"assistant"},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791888292-7tMGAOxwZLOZh4kbqwWT","object":"chat.completion.chunk","created":1791888292,"model":"google/gemini-3.8-flash","provider":"Google","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning_details":[{"type":"reasoning.text","signature":"AY89a1+Db/nFVwzUKszd2nvp","format":"google-gemini-v1","index":0}]},"finish_reason":null,"native_finish_reason":null}]}`,
	`{"id":"gen-1791888292-7tMGAOxwZLOZh4kbqwWT","object":"chat.completion.chunk","created":1791888292,"model":"google/gemini-3.8-flash","provider":"Google","choices":[{"index":0,"delta":{"content":"","role":"assistant","reasoning":null},"finish_reason":"stop","native_finish_reason":"STOP"}]}`,
}

// TestStream_OpenRouterReasoningDetailsRoundTrip streams a recorded step and
// replays it: every reasoning_details item, the opaque ones included, must be
// kept on the reasoning part and go back on the assistant message unchanged.
func TestStream_OpenRouterReasoningDetailsRoundTrip(t *testing.T) {
	cases := []struct {
		name      string
		model     string
		chunks    []string
		reasoning string
		text      string
		toolCall  string
		details   string
	}{
		{
			name:      "gemini text and encrypted thought signature",
			model:     "google/gemini-3.8-flash",
			chunks:    geminiToolCallChunks,
			reasoning: "**Solving the four-digit number puzzle**\n\n**Determining the leading digit**\n\n",
			toolCall:  "call_99412",
			details: `[
				{"type":"reasoning.text","text":"**Solving the four-digit number puzzle**\n\n**Determining the leading digit**\n\n","format":"google-gemini-v1","index":0},
				{"type":"reasoning.encrypted","data":"AY89a19NjHDETYGOfpG8ryHu","format":"google-gemini-v1","id":"call_99412","index":0}
			]`,
		},
		{
			name:      "openai summary and encrypted reasoning",
			model:     "openai/gpt-6.1-sol",
			chunks:    openAIToolCallChunks,
			reasoning: "**Calculating multiplication**\n\nI need to solve",
			toolCall:  "call_0FCCd3LFkwY8BbYslmRTygJy",
			details: `[
				{"type":"reasoning.summary","summary":"**Calculating multiplication**\n\nI need to solve","format":"openai-responses-v1","index":0},
				{"type":"reasoning.encrypted","data":"gAAAAABqzhmrLgaNgoqd-quY","format":"openai-responses-v1","id":"rs_0ff0d18852b88860016ace19a964a887d1900992abf9056ca6","index":1}
			]`,
		},
		{
			name:      "anthropic signed thinking",
			model:     "anthropic/claude-haiku-5.5",
			chunks:    anthropicToolCallChunks,
			reasoning: "I need ABCD × 4 = DCBA, a class\n\n",
			toolCall:  "toolu_01GQsisNYbv2pj5qtKGU3nJH",
			details: `[
				{"type":"reasoning.text","text":"I need ABCD × 4 = DCBA, a class\n\n","signature":"CAQSlQgKEAgSGAI4AUIIdGhp","format":"anthropic-claude-v1","index":0}
			]`,
		},
		{
			name:   "gemini signature after the text",
			model:  "google/gemini-3.8-flash",
			chunks: geminiTextSignatureChunks,
			text:   "新加坡现在有雷阵雨，气温约为31摄氏度。",
			details: `[
				{"type":"reasoning.text","signature":"AY89a1+Db/nFVwzUKszd2nvp","format":"google-gemini-v1","index":0}
			]`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newReplayServer(t, tc.chunks)
			model := openRouter(srv.URL).ChatModel(tc.model)

			history := []sdk.Message{sdk.UserMessage("先求出 ABCD，再查天气")}
			result := streamStep(t, model, history)
			if len(result.ReasoningParts) != 1 {
				t.Fatalf("reasoning parts: got %d, want 1: %#v", len(result.ReasoningParts), result.ReasoningParts)
			}
			part := result.ReasoningParts[0]
			if part.Text != tc.reasoning {
				t.Errorf("reasoning text: got %q, want %q", part.Text, tc.reasoning)
			}
			if part.Format != sdk.ReasoningFormatOpenAIChat || part.Model != tc.model {
				t.Errorf("reasoning provenance: got %q from %q", part.Format, part.Model)
			}
			assertSameJSON(t, "kept details", part.ProviderMetadata.Get("openai-chat", "reasoning_details"), tc.details)
			if result.Text != tc.text {
				t.Errorf("text: got %q, want %q", result.Text, tc.text)
			}

			history = append(history, assistantStep(&result))
			if tc.toolCall != "" {
				if len(result.ToolCalls) != 1 || result.ToolCalls[0].ToolCallID != tc.toolCall {
					t.Fatalf("tool calls: got %#v, want one %s", result.ToolCalls, tc.toolCall)
				}
				history = append(history, sdk.ToolMessage(sdk.ToolResultPart{
					ToolCallID: tc.toolCall, ToolName: "get_weather", Result: sdk.TextOutput(`{"temp_c":27}`),
				}))
			} else {
				history = append(history, sdk.UserMessage("那明天呢？"))
			}
			streamStep(t, model, history)

			replayed := srv.replayedAssistant(t)
			if _, ok := replayed["reasoning_content"]; ok {
				t.Errorf("reasoning_content should be omitted when details replay, got %v", replayed["reasoning_content"])
			}
			sent, err := json.Marshal(replayed["reasoning_details"])
			if err != nil {
				t.Fatalf("marshal replayed details: %v", err)
			}
			assertSameJSON(t, "replayed details", string(sent), tc.details)
		})
	}
}

// TestStream_ReasoningDetailsAfterTextJoinTheirBlock: Gemini sends the answer
// text's thought signature after the text (geminiTextSignatureChunks). When
// reasoning text came first, the text has already closed the reasoning block,
// and the signature must still reach that block: neither dropped nor in a
// second block. The step is assembled from recorded pieces: the first
// reasoning piece of geminiToolCallChunks, moved to the generation of
// geminiTextSignatureChunks, then that recording.
func TestStream_ReasoningDetailsAfterTextJoinTheirBlock(t *testing.T) {
	reasoning := strings.ReplaceAll(geminiToolCallChunks[0], "gen-1791891805-CpoMkkLsvH5EhAaNMih4", "gen-1791888292-7tMGAOxwZLOZh4kbqwWT")
	srv := newReplayServer(t, append([]string{reasoning}, geminiTextSignatureChunks...))
	model := openRouter(srv.URL).ChatModel("google/gemini-3.8-flash")

	history := []sdk.Message{sdk.UserMessage("先求出 ABCD，再查天气")}
	result := streamStep(t, model, history)
	if len(result.ReasoningParts) != 1 {
		t.Fatalf("reasoning parts: got %d, want 1: %#v", len(result.ReasoningParts), result.ReasoningParts)
	}
	if got := result.ReasoningParts[0].Text; got != "**Solving the four-digit number puzzle**\n\n" {
		t.Errorf("reasoning text: got %q", got)
	}
	details := `[{"type":"reasoning.text","text":"**Solving the four-digit number puzzle**\n\n","signature":"AY89a1+Db/nFVwzUKszd2nvp","format":"google-gemini-v1","index":0}]`
	assertSameJSON(t, "kept details", result.ReasoningParts[0].ProviderMetadata.Get("openai-chat", "reasoning_details"), details)
	if result.Text != "新加坡现在有雷阵雨，气温约为31摄氏度。" {
		t.Errorf("text: got %q", result.Text)
	}

	history = append(history, assistantStep(&result), sdk.UserMessage("那明天呢？"))
	streamStep(t, model, history)
	sent, err := json.Marshal(srv.replayedAssistant(t)["reasoning_details"])
	if err != nil {
		t.Fatalf("marshal replayed details: %v", err)
	}
	assertSameJSON(t, "replayed details", string(sent), details)
}

// openRouter is a provider set up as OpenRouter needs: reasoning goes back as
// reasoning_details.
func openRouter(baseURL string) *completions.Provider {
	return completions.New(
		completions.WithAPIKey("k"),
		completions.WithBaseURL(baseURL),
		completions.WithReasoningReplay(sdk.ReasoningReplay{Field: sdk.ReasoningReplayDetails}),
	)
}

// replayServer streams the recorded chunks to the first request and a short
// answer to every later one, keeping the last request's assistant message.
type replayServer struct {
	*httptest.Server
	mu        sync.Mutex
	requests  int
	assistant map[string]any
}

func newReplayServer(t *testing.T, chunks []string) *replayServer {
	t.Helper()
	rs := &replayServer{}
	rs.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Messages []map[string]any `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		rs.mu.Lock()
		rs.requests++
		first := rs.requests == 1 && chunks != nil
		for _, m := range body.Messages {
			if m["role"] == "assistant" {
				rs.assistant = m
			}
		}
		rs.mu.Unlock()

		w.Header().Set("Content-Type", "text/event-stream")
		flusher := w.(http.Flusher)
		send := chunks
		if !first {
			send = []string{
				`{"id":"c2","model":"m","choices":[{"index":0,"delta":{"content":"27°C"},"finish_reason":null}]}`,
				`{"id":"c2","model":"m","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}`,
			}
		}
		fmt.Fprint(w, ": OPENROUTER PROCESSING\n\n")
		for _, c := range send {
			fmt.Fprintf(w, "data: %s\n\n", c)
			flusher.Flush()
		}
		fmt.Fprint(w, "data: [DONE]\n\n")
		flusher.Flush()
	}))
	t.Cleanup(rs.Close)
	return rs
}

func (rs *replayServer) replayedAssistant(t *testing.T) map[string]any {
	t.Helper()
	rs.mu.Lock()
	defer rs.mu.Unlock()
	if rs.assistant == nil {
		t.Fatal("no assistant message was replayed")
	}
	return rs.assistant
}

func streamStep(t *testing.T, model *sdk.Model, messages []sdk.Message) sdk.ModelResult {
	t.Helper()
	s, err := model.Stream(context.Background(), sdk.Request{Messages: messages, Tools: []sdk.ToolDefinition{{Name: "get_weather"}}})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	for part := range s.Parts {
		if e, ok := part.(*sdk.ErrorPart); ok {
			t.Fatalf("stream error: %v", e.Error)
		}
	}
	result, err := s.Result()
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	return *result
}

func assertSameJSON(t *testing.T, what, got, want string) {
	t.Helper()
	var g, w any
	if err := json.Unmarshal([]byte(got), &g); err != nil {
		t.Fatalf("%s: decode %q: %v", what, got, err)
	}
	if err := json.Unmarshal([]byte(want), &w); err != nil {
		t.Fatalf("%s: decode want: %v", what, err)
	}
	if !reflect.DeepEqual(g, w) {
		t.Errorf("%s:\n got %s\nwant %s", what, got, want)
	}
}
