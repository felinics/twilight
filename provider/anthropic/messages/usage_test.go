package messages_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/provider/anthropic/messages"
	"github.com/felinics/twilight/sdk"
)

func TestUsageIncludesCachedInput(t *testing.T) {
	cases := []struct {
		name  string
		usage string
		want  sdk.Usage
	}{
		{
			name:  "uncached",
			usage: `{"input_tokens":10,"output_tokens":5}`,
			want: sdk.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10}},
		},
		{
			name:  "cache read",
			usage: `{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":200}`,
			want: sdk.Usage{InputTokens: 210, OutputTokens: 5, TotalTokens: 215, CachedInputTokens: 200,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10, CacheReadTokens: 200}},
		},
		{
			name:  "cache write",
			usage: `{"input_tokens":10,"output_tokens":5,"cache_creation_input_tokens":100}`,
			want: sdk.Usage{InputTokens: 110, OutputTokens: 5, TotalTokens: 115,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10, CacheWriteTokens: 100}},
		},
		{
			name:  "mixed cache lifetimes",
			usage: `{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":200,"cache_creation_input_tokens":556,"cache_creation":{"ephemeral_5m_input_tokens":456,"ephemeral_1h_input_tokens":100}}`,
			want: sdk.Usage{InputTokens: 766, OutputTokens: 5, TotalTokens: 771, CachedInputTokens: 200,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10, CacheReadTokens: 200, CacheWriteTokens: 556, CacheWrite5mTokens: 456, CacheWrite1hTokens: 100}},
		},
		{
			name:  "fully cached",
			usage: `{"input_tokens":0,"output_tokens":5,"cache_read_input_tokens":200}`,
			want: sdk.Usage{InputTokens: 200, OutputTokens: 5, TotalTokens: 205, CachedInputTokens: 200,
				InputTokenDetails: sdk.InputTokenDetail{CacheReadTokens: 200}},
		},
	}
	for _, tc := range cases {
		for _, stream := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.name, stream), func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					if !stream {
						w.Header().Set("Content-Type", "application/json")
						fmt.Fprintf(w, `{"id":"msg_usage","type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":%s}`, tc.usage)
						return
					}
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprintf(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_usage\",\"role\":\"assistant\",\"content\":[],\"usage\":%s}}\n\n", tc.usage)
					fmt.Fprint(w, "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
				}))
				defer srv.Close()
				provider := messages.New(messages.WithAPIKey("test-key"), messages.WithBaseURL(srv.URL))
				params := sdk.Request{Model: "claude-test", Messages: []sdk.Message{sdk.UserMessage("hi")}}
				if !stream {
					result, err := provider.DoGenerate(context.Background(), params)
					if err != nil {
						t.Fatal(err)
					}
					if result.Usage != tc.want {
						t.Fatalf("usage = %+v, want %+v", result.Usage, tc.want)
					}
					return
				}
				result, err := provider.DoStream(context.Background(), params)
				if err != nil {
					t.Fatal(err)
				}
				var stepFinished, finished bool
				for part := range result {
					switch p := part.(type) {
					case *sdk.ErrorPart:
						t.Fatalf("stream error: %+v", p)
					case *sdk.FinishStepPart:
						stepFinished = true
						if p.Usage != tc.want {
							t.Errorf("step usage = %+v, want %+v", p.Usage, tc.want)
						}
					case *sdk.FinishPart:
						finished = true
						if p.TotalUsage != tc.want {
							t.Errorf("total usage = %+v, want %+v", p.TotalUsage, tc.want)
						}
					}
				}
				if !stepFinished || !finished {
					t.Fatal("missing finish events")
				}
			})
		}
	}
}

// Anthropic-compatible relays can report input and cache usage only at the end.
// Delta usage is a cumulative, partial update, not an increment or replacement.
func TestStreamUsageUpdates(t *testing.T) {
	cases := []struct {
		name       string
		initial    string
		deltas     []string
		incomplete bool
		want       sdk.Usage
		wantReason sdk.FinishReason
	}{
		{
			name:    "input and cache arrive at the end",
			initial: `{"input_tokens":0,"output_tokens":0,"cache_read_input_tokens":null}`,
			deltas:  []string{`{"delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":200,"cache_creation_input_tokens":100,"cache_creation":{"ephemeral_5m_input_tokens":80,"ephemeral_1h_input_tokens":20}}}`},
			want: sdk.Usage{InputTokens: 310, OutputTokens: 5, TotalTokens: 315, CachedInputTokens: 200,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10, CacheReadTokens: 200, CacheWriteTokens: 100, CacheWrite5mTokens: 80, CacheWrite1hTokens: 20}},
			wantReason: sdk.FinishReasonStop,
		},
		{
			name:    "output-only delta preserves initial usage",
			initial: `{"input_tokens":10,"output_tokens":0,"cache_read_input_tokens":200,"cache_creation_input_tokens":100}`,
			deltas:  []string{`{"delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":5}}`},
			want: sdk.Usage{InputTokens: 310, OutputTokens: 5, TotalTokens: 315, CachedInputTokens: 200,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10, CacheReadTokens: 200, CacheWriteTokens: 100}},
			wantReason: sdk.FinishReasonStop,
		},
		{
			name:       "explicit zero clears previous counters",
			initial:    `{"input_tokens":10,"output_tokens":3,"cache_read_input_tokens":200,"cache_creation_input_tokens":100,"cache_creation":{"ephemeral_5m_input_tokens":80,"ephemeral_1h_input_tokens":20}}`,
			deltas:     []string{`{"delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":0,"output_tokens":5,"cache_read_input_tokens":0,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}}`},
			want:       sdk.Usage{OutputTokens: 5, TotalTokens: 5},
			wantReason: sdk.FinishReasonStop,
		},
		{
			name:    "cumulative and sparse updates finish once",
			initial: `{"input_tokens":10,"output_tokens":0,"cache_read_input_tokens":200,"cache_creation_input_tokens":100,"cache_creation":{"ephemeral_5m_input_tokens":80,"ephemeral_1h_input_tokens":20}}`,
			deltas: []string{
				`{"delta":{"stop_reason":"tool_use"},"usage":{"input_tokens":10,"output_tokens":3,"cache_read_input_tokens":200,"cache_creation_input_tokens":100}}`,
				`{"delta":{"stop_reason":null},"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":200,"cache_creation_input_tokens":120,"cache_creation":{"ephemeral_5m_input_tokens":100}}}`,
				`{"usage":null}`,
			},
			want: sdk.Usage{InputTokens: 330, OutputTokens: 5, TotalTokens: 335, CachedInputTokens: 200,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10, CacheReadTokens: 200, CacheWriteTokens: 120, CacheWrite5mTokens: 100, CacheWrite1hTokens: 20}},
			wantReason: sdk.FinishReasonToolCalls,
		},
		{
			name:       "incomplete stream retains usage without completing the step",
			initial:    `{"input_tokens":0,"output_tokens":0}`,
			deltas:     []string{`{"delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":200}}`},
			incomplete: true,
			want: sdk.Usage{InputTokens: 210, OutputTokens: 5, TotalTokens: 215, CachedInputTokens: 200,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10, CacheReadTokens: 200}},
			wantReason: sdk.FinishReasonError,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				fmt.Fprintf(w, "data: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_usage\",\"model\":\"claude-test\",\"role\":\"assistant\",\"content\":[],\"usage\":%s}}\n\n", tc.initial)
				for _, delta := range tc.deltas {
					fmt.Fprintf(w, "data: {\"type\":\"message_delta\",%s\n\n", delta[1:])
				}
				if !tc.incomplete {
					fmt.Fprint(w, "data: {\"type\":\"message_stop\"}\n\n")
				}
			}))
			defer srv.Close()
			provider := messages.New(messages.WithBaseURL(srv.URL))
			parts, err := provider.DoStream(context.Background(), sdk.Request{Model: "claude-test", Messages: []sdk.Message{sdk.UserMessage("hi")}})
			if err != nil {
				t.Fatal(err)
			}
			var steps, finishes, failures int
			for part := range parts {
				switch p := part.(type) {
				case *sdk.ErrorPart:
					failures++
					if !tc.incomplete {
						t.Errorf("unexpected stream error: %v", p.Error)
					}
				case *sdk.FinishStepPart:
					steps++
					if p.Usage != tc.want || p.FinishReason != tc.wantReason {
						t.Errorf("step: usage=%+v reason=%q; want %+v %q", p.Usage, p.FinishReason, tc.want, tc.wantReason)
					}
				case *sdk.FinishPart:
					finishes++
					if p.TotalUsage != tc.want || p.FinishReason != tc.wantReason {
						t.Errorf("finish: usage=%+v reason=%q; want %+v %q", p.TotalUsage, p.FinishReason, tc.want, tc.wantReason)
					}
				}
			}
			wantSteps, wantFailures := 1, 0
			if tc.incomplete {
				wantSteps, wantFailures = 0, 1
			}
			if steps != wantSteps || finishes != 1 || failures != wantFailures {
				t.Errorf("events: steps=%d finishes=%d errors=%d; want %d/1/%d", steps, finishes, failures, wantSteps, wantFailures)
			}
		})
	}
}
