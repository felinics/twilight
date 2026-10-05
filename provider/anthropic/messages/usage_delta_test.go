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

func TestStreamUsageUsesCumulativeDeltaFields(t *testing.T) {
	tests := []struct {
		name           string
		start          string
		deltas         []string
		stopBeforeLast bool
		want           sdk.Usage
	}{
		{
			name:  "terminal cache update",
			start: `{"input_tokens":310,"output_tokens":0,"cache_read_input_tokens":0,"cache_creation_input_tokens":0}`,
			deltas: []string{
				`{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":200,"cache_creation_input_tokens":100}`,
			},
			want: sdk.Usage{InputTokens: 310, OutputTokens: 5, TotalTokens: 315, CachedInputTokens: 200, CacheReadTokensReported: true,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10, CacheReadTokens: 200, CacheWriteTokens: 100}},
		},
		{
			name:  "output only retains cache and lifetimes",
			start: `{"input_tokens":10,"output_tokens":0,"cache_read_input_tokens":200,"cache_creation_input_tokens":100,"cache_creation":{"ephemeral_5m_input_tokens":60,"ephemeral_1h_input_tokens":40}}`,
			deltas: []string{
				`{"output_tokens":5}`,
			},
			want: sdk.Usage{InputTokens: 310, OutputTokens: 5, TotalTokens: 315, CachedInputTokens: 200, CacheReadTokensReported: true,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10, CacheReadTokens: 200, CacheWriteTokens: 100, CacheWrite5mTokens: 60, CacheWrite1hTokens: 40}},
		},
		{
			name:  "explicit zero replaces earlier cache",
			start: `{"input_tokens":10,"output_tokens":2,"cache_read_input_tokens":200,"cache_creation_input_tokens":100,"cache_creation":{"ephemeral_5m_input_tokens":60,"ephemeral_1h_input_tokens":40}}`,
			deltas: []string{
				`{"input_tokens":10,"output_tokens":0,"cache_read_input_tokens":0,"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0,"ephemeral_1h_input_tokens":0}}`,
			},
			want: sdk.Usage{InputTokens: 10, TotalTokens: 10, CacheReadTokensReported: true,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10}},
		},
		{
			name:   "zero aggregate with sparse lifetime update",
			start:  `{"input_tokens":10,"output_tokens":0,"cache_creation_input_tokens":100,"cache_creation":{"ephemeral_5m_input_tokens":60,"ephemeral_1h_input_tokens":40}}`,
			deltas: []string{`{"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":0},"output_tokens":5}`},
			want: sdk.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10}},
		},
		{
			name:   "changed aggregate invalidates inconsistent sparse lifetimes",
			start:  `{"input_tokens":10,"output_tokens":0,"cache_creation_input_tokens":100,"cache_creation":{"ephemeral_5m_input_tokens":60,"ephemeral_1h_input_tokens":40}}`,
			deltas: []string{`{"cache_creation_input_tokens":80,"cache_creation":{"ephemeral_5m_input_tokens":60},"output_tokens":5}`},
			want: sdk.Usage{InputTokens: 90, OutputTokens: 5, TotalTokens: 95,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10, CacheWriteTokens: 80}},
		},
		{
			name:   "zero aggregate clears unavailable lifetimes",
			start:  `{"input_tokens":10,"output_tokens":0,"cache_creation_input_tokens":100,"cache_creation":{"ephemeral_5m_input_tokens":60,"ephemeral_1h_input_tokens":40}}`,
			deltas: []string{`{"cache_creation_input_tokens":0,"output_tokens":5}`},
			want: sdk.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10}},
		},
		{
			name:  "cumulative updates are not added",
			start: `{"input_tokens":10,"output_tokens":0,"cache_read_input_tokens":200,"cache_creation_input_tokens":100}`,
			deltas: []string{
				`{"input_tokens":20,"output_tokens":3,"cache_read_input_tokens":400,"cache_creation_input_tokens":200}`,
				`{"input_tokens":20,"output_tokens":3,"cache_read_input_tokens":400,"cache_creation_input_tokens":200}`,
				`{"output_tokens":5}`,
			},
			want: sdk.Usage{InputTokens: 620, OutputTokens: 5, TotalTokens: 625, CachedInputTokens: 400, CacheReadTokensReported: true,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 20, CacheReadTokens: 400, CacheWriteTokens: 200}},
		},
		{
			name:  "usage after stop reason",
			start: `{"input_tokens":310,"output_tokens":0}`,
			deltas: []string{
				`{"output_tokens":3}`,
				`{"input_tokens":10,"output_tokens":5,"cache_read_input_tokens":200,"cache_creation_input_tokens":100}`,
			},
			stopBeforeLast: true,
			want: sdk.Usage{InputTokens: 310, OutputTokens: 5, TotalTokens: 315, CachedInputTokens: 200, CacheReadTokensReported: true,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10, CacheReadTokens: 200, CacheWriteTokens: 100}},
		},
		{
			name:  "partial lifetime update retains other lifetime",
			start: `{"input_tokens":10,"output_tokens":0,"cache_creation_input_tokens":100,"cache_creation":{"ephemeral_5m_input_tokens":60,"ephemeral_1h_input_tokens":40}}`,
			deltas: []string{
				`{"cache_creation_input_tokens":140,"cache_creation":{"ephemeral_5m_input_tokens":100}}`,
				`{"output_tokens":5}`,
			},
			want: sdk.Usage{InputTokens: 150, OutputTokens: 5, TotalTokens: 155,
				InputTokenDetails: sdk.InputTokenDetail{NoCacheTokens: 10, CacheWriteTokens: 140, CacheWrite5mTokens: 100, CacheWrite1hTokens: 40}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = fmt.Fprintf(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_usage\",\"model\":\"claude-test\",\"role\":\"assistant\",\"usage\":%s}}\n\n", tt.start)
				_, _ = fmt.Fprint(w, "event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"ok\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n")
				for i, usage := range tt.deltas {
					delta := `{}`
					if (i == len(tt.deltas)-1 && !tt.stopBeforeLast) || (i == 0 && tt.stopBeforeLast) {
						delta = `{"stop_reason":"end_turn"}`
					}
					_, _ = fmt.Fprintf(w, "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":%s,\"usage\":%s}\n\n", delta, usage)
				}
				_, _ = fmt.Fprint(w, "event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
			}))
			defer server.Close()
			model := messages.New(messages.WithAPIKey("fixture"), messages.WithBaseURL(server.URL)).ChatModel("claude-test")
			stream, err := model.Stream(context.Background(), sdk.Request{Messages: []sdk.Message{sdk.UserMessage("hi")}})
			if err != nil {
				t.Fatal(err)
			}
			var stepUsage sdk.Usage
			var stepEnds, ends int
			for part := range stream.Parts {
				if finish, ok := part.(*sdk.FinishStepPart); ok {
					stepUsage = finish.Usage
					stepEnds++
				}
				if _, ok := part.(*sdk.FinishPart); ok {
					ends++
				}
			}
			result, err := stream.Result()
			if err != nil {
				t.Fatal(err)
			}
			if result.Text != "ok" || result.Usage != tt.want || stepUsage != tt.want || result.FinishReason != sdk.FinishReasonStop || stepEnds != 1 || ends != 1 {
				t.Fatalf("text=%q usage=%+v step=%+v finish=%s stepEnds=%d ends=%d, want %+v and one finish", result.Text, result.Usage, stepUsage, result.FinishReason, stepEnds, ends, tt.want)
			}
		})
	}
}
