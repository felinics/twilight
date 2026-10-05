package providertest_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/provider/anthropic/messages"
	"github.com/felinics/twilight/provider/github/copilot"
	"github.com/felinics/twilight/provider/google/generativeai"
	"github.com/felinics/twilight/provider/openai/codex"
	"github.com/felinics/twilight/provider/openai/completions"
	"github.com/felinics/twilight/provider/openai/responses"
	"github.com/felinics/twilight/sdk"
)

func TestInputUsageContract(t *testing.T) {
	providers := []struct {
		name   string
		format string
		new    func(string) sdk.Provider
	}{
		{"anthropic", "anthropic", func(url string) sdk.Provider {
			return messages.New(messages.WithAPIKey("test-key"), messages.WithBaseURL(url))
		}},
		{"openai-chat", "chat", func(url string) sdk.Provider {
			return completions.New(completions.WithAPIKey("test-key"), completions.WithBaseURL(url))
		}},
		{"copilot", "chat", func(url string) sdk.Provider {
			return copilot.New(copilot.WithGitHubToken("test-token"), copilot.WithBaseURL(url))
		}},
		{"openai-responses", "responses", func(url string) sdk.Provider {
			return responses.New(responses.WithAPIKey("test-key"), responses.WithBaseURL(url))
		}},
		{"codex", "responses", func(url string) sdk.Provider {
			return codex.New(codex.WithAccessToken("test-token"), codex.WithBaseURL(url))
		}},
		{"google", "google", func(url string) sdk.Provider {
			return generativeai.New(generativeai.WithAPIKey("test-key"), generativeai.WithBaseURL(url))
		}},
	}
	cases := []struct {
		name     string
		wire     map[string]string
		input    int
		output   int
		detail   sdk.InputTokenDetail
		reported bool
	}{
		{name: "missing usage"},
		{
			name: "zero usage",
			wire: map[string]string{
				"anthropic": `{"input_tokens":0,"output_tokens":0}`,
				"chat":      `{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}`,
				"responses": `{"input_tokens":0,"output_tokens":0,"total_tokens":0}`,
				"google":    `{"promptTokenCount":0,"candidatesTokenCount":0,"totalTokenCount":0}`,
			},
		},
		{
			name: "no cache details",
			wire: map[string]string{
				"anthropic": `{"input_tokens":100,"output_tokens":5}`,
				"chat":      `{"prompt_tokens":100,"completion_tokens":5,"total_tokens":105}`,
				"responses": `{"input_tokens":100,"output_tokens":5,"total_tokens":105}`,
				"google":    `{"promptTokenCount":100,"candidatesTokenCount":5,"totalTokenCount":105}`,
			},
			input: 100, output: 5, detail: sdk.InputTokenDetail{NoCacheTokens: 100},
		},
		{
			name: "null usage",
			wire: map[string]string{"anthropic": "null", "chat": "null", "responses": "null", "google": "null"},
		},
		{
			name: "null cache field",
			wire: map[string]string{
				"anthropic": `{"input_tokens":100,"output_tokens":5,"cache_read_input_tokens":null}`,
				"chat":      `{"prompt_tokens":100,"completion_tokens":5,"total_tokens":105,"prompt_tokens_details":{"cached_tokens":null}}`,
				"responses": `{"input_tokens":100,"output_tokens":5,"input_tokens_details":{"cached_tokens":null}}`,
				"google":    `{"promptTokenCount":100,"candidatesTokenCount":5,"totalTokenCount":105,"cachedContentTokenCount":null}`,
			},
			input: 100, output: 5, detail: sdk.InputTokenDetail{NoCacheTokens: 100},
		},
		{
			name: "null cache details",
			wire: map[string]string{
				"chat":      `{"prompt_tokens":100,"completion_tokens":5,"total_tokens":105,"prompt_tokens_details":null}`,
				"responses": `{"input_tokens":100,"output_tokens":5,"input_tokens_details":null}`,
			},
			input: 100, output: 5, detail: sdk.InputTokenDetail{NoCacheTokens: 100},
		},
		{
			name: "details without cache read",
			wire: map[string]string{
				"chat":      `{"prompt_tokens":100,"completion_tokens":5,"total_tokens":105,"prompt_tokens_details":{}}`,
				"responses": `{"input_tokens":100,"output_tokens":5,"input_tokens_details":{}}`,
			},
			input: 100, output: 5, detail: sdk.InputTokenDetail{NoCacheTokens: 100},
		},
		{
			name:     "zero cache details",
			reported: true,
			wire: map[string]string{
				"anthropic": `{"input_tokens":100,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}`,
				"chat":      `{"prompt_tokens":100,"completion_tokens":5,"total_tokens":105,"prompt_tokens_details":{"cached_tokens":0}}`,
				"responses": `{"input_tokens":100,"output_tokens":5,"input_tokens_details":{"cached_tokens":0}}`,
				"google":    `{"promptTokenCount":100,"candidatesTokenCount":5,"totalTokenCount":105,"cachedContentTokenCount":0}`,
			},
			input: 100, output: 5, detail: sdk.InputTokenDetail{NoCacheTokens: 100},
		},
		{
			name:     "partial cache read",
			reported: true,
			wire: map[string]string{
				"anthropic": `{"input_tokens":80,"output_tokens":5,"cache_read_input_tokens":20}`,
				"chat":      `{"prompt_tokens":100,"completion_tokens":5,"total_tokens":105,"prompt_tokens_details":{"cached_tokens":20}}`,
				"responses": `{"input_tokens":100,"output_tokens":5,"input_tokens_details":{"cached_tokens":20}}`,
				"google":    `{"promptTokenCount":100,"candidatesTokenCount":5,"totalTokenCount":105,"cachedContentTokenCount":20}`,
			},
			input: 100, output: 5, detail: sdk.InputTokenDetail{NoCacheTokens: 80, CacheReadTokens: 20},
		},
		{
			name:     "full cache read",
			reported: true,
			wire: map[string]string{
				"anthropic": `{"input_tokens":0,"output_tokens":5,"cache_read_input_tokens":100}`,
				"chat":      `{"prompt_tokens":100,"completion_tokens":5,"total_tokens":105,"prompt_tokens_details":{"cached_tokens":100}}`,
				"responses": `{"input_tokens":100,"output_tokens":5,"input_tokens_details":{"cached_tokens":100}}`,
				"google":    `{"promptTokenCount":100,"candidatesTokenCount":5,"totalTokenCount":105,"cachedContentTokenCount":100}`,
			},
			input: 100, output: 5, detail: sdk.InputTokenDetail{CacheReadTokens: 100},
		},
		{
			name:     "mixed cache lifetimes",
			reported: true,
			wire: map[string]string{
				"anthropic": `{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":20,"cache_creation_input_tokens":70,"cache_creation":{"ephemeral_5m_input_tokens":30,"ephemeral_1h_input_tokens":40}}`,
			},
			input: 100, output: 5, detail: sdk.InputTokenDetail{NoCacheTokens: 10, CacheReadTokens: 20, CacheWriteTokens: 70, CacheWrite5mTokens: 30, CacheWrite1hTokens: 40},
		},
	}
	for _, p := range providers {
		for _, tc := range cases {
			wire, supported := tc.wire[p.format]
			if tc.wire != nil && !supported {
				continue
			}
			for _, stream := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/stream=%t", p.name, tc.name, stream), func(t *testing.T) {
					check := func(t *testing.T, label string, got *sdk.Usage) {
						t.Helper()
						if got.InputTokens != tc.input || got.OutputTokens != tc.output || got.TotalTokens != tc.input+tc.output ||
							got.InputTokenDetails != tc.detail || got.CachedInputTokens != tc.detail.CacheReadTokens {
							t.Errorf("%s: usage = %+v, want input=%d output=%d details=%+v", label, got, tc.input, tc.output, tc.detail)
						}
						if got.CacheReadTokensReported != tc.reported {
							t.Errorf("%s: cache reporting = %t, want %t", label, got.CacheReadTokensReported, tc.reported)
						}
						d := got.InputTokenDetails
						if got.InputTokens != d.NoCacheTokens+d.CacheReadTokens+d.CacheWriteTokens {
							t.Errorf("%s: input tokens do not equal uncached + cache read + cache write: %+v", label, got)
						}
					}
					srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
						writeUsageReply(w, p.format, wire, tc.output, stream || p.name == "codex")
					}))
					defer srv.Close()
					ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
					defer cancel()
					model := &sdk.Model{ID: "test-model", Provider: p.new(srv.URL), Type: sdk.ModelTypeChat}
					req := sdk.Request{Messages: []sdk.Message{sdk.UserMessage("hi")}}
					if !stream {
						result, err := model.Generate(ctx, req)
						if err != nil {
							t.Fatal(err)
						}
						check(t, "generate", &result.Usage)
						return
					}
					result, err := model.Stream(ctx, req)
					if err != nil {
						t.Fatal(err)
					}
					var steps, finishes int
					for part := range result.Parts {
						switch part := part.(type) {
						case *sdk.ErrorPart:
							t.Errorf("stream: %v", part.Error)
						case *sdk.FinishStepPart:
							steps++
							check(t, "finish step", &part.Usage)
						case *sdk.FinishPart:
							finishes++
							check(t, "finish", &part.TotalUsage)
						}
					}
					if steps != 1 || finishes != 1 {
						t.Errorf("finish events: steps=%d finishes=%d, want one each", steps, finishes)
					}
					assembled, err := result.Result()
					if err != nil {
						t.Fatal(err)
					}
					check(t, "assembled result", &assembled.Usage)
				})
			}
		}
	}
}

func writeUsageReply(w http.ResponseWriter, format, usage string, output int, stream bool) {
	field := ""
	if usage != "" {
		key := "usage"
		if format == "google" {
			key = "usageMetadata"
		}
		field = fmt.Sprintf(",%q:%s", key, usage)
	}
	w.Header().Set("Content-Type", "application/json")
	if stream {
		w.Header().Set("Content-Type", "text/event-stream")
	}
	switch format {
	case "anthropic":
		if !stream {
			fmt.Fprintf(w, `{"id":"msg_test","type":"message","role":"assistant","content":[],"stop_reason":"end_turn"%s}`, field)
			return
		}
		start := strings.Replace(field, fmt.Sprintf(`"output_tokens":%d`, output), `"output_tokens":0`, 1)
		delta := ""
		if usage != "" {
			delta = fmt.Sprintf(`,"usage":{"output_tokens":%d}`, output)
		}
		fmt.Fprintf(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_test\",\"role\":\"assistant\",\"content\":[]%s}}\n\n", start)
		fmt.Fprintf(w, "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"}%s}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n", delta)
	case "chat":
		if !stream {
			fmt.Fprintf(w, `{"id":"chat_test","choices":[{"message":{"role":"assistant","content":"ok"},"finish_reason":"stop"}]%s}`, field)
			return
		}
		fmt.Fprint(w, "data: {\"id\":\"chat_test\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n")
		fmt.Fprintf(w, "data: {\"id\":\"chat_test\",\"choices\":[]%s}\n\ndata: [DONE]\n\n", field)
	case "responses":
		reply := fmt.Sprintf(`{"id":"resp_test","status":"completed","output":[]%s}`, field)
		if !stream {
			fmt.Fprint(w, reply)
			return
		}
		fmt.Fprintf(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":%s}\n\n", reply)
	case "google":
		reply := fmt.Sprintf(`{"candidates":[{"content":{"role":"model","parts":[{"text":"ok"}]},"finishReason":"STOP"}]%s}`, field)
		if !stream {
			fmt.Fprint(w, reply)
			return
		}
		fmt.Fprintf(w, "data: %s\n\n", reply)
	}
}
