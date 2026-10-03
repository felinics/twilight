package responses_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/provider/openai/responses"
	"github.com/felinics/twilight/sdk"
)

func TestResponsesCacheWritesRemainInputSubset(t *testing.T) {
	for _, tt := range []struct {
		name, details   string
		uncached, write int
	}{
		{name: "cache write extension", details: `{"cached_tokens":400,"cache_write_tokens":100}`, uncached: 500, write: 100},
		{name: "standard cached input", details: `{"cached_tokens":400}`, uncached: 600},
	} {
		for _, streaming := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/stream=%t", tt.name, streaming), func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					body := fmt.Sprintf(`{"id":"resp_usage","object":"response","status":"completed","model":"fixture","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":1000,"output_tokens":200,"input_tokens_details":%s}}`, tt.details)
					w.Header().Set("Content-Type", "application/json")
					if streaming {
						w.Header().Set("Content-Type", "text/event-stream")
						_, _ = fmt.Fprintf(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\nevent: response.completed\ndata: {\"type\":\"response.completed\",\"response\":%s}\n\n", body)
						return
					}
					_, _ = fmt.Fprint(w, body)
				}))
				defer server.Close()
				model := responses.New(responses.WithAPIKey("fixture"), responses.WithBaseURL(server.URL)).ChatModel("fixture")
				request := sdk.Request{Messages: []sdk.Message{sdk.UserMessage("hi")}}
				var result sdk.ModelResult
				var err error
				if streaming {
					stream, streamErr := model.Stream(context.Background(), request)
					if streamErr != nil {
						t.Fatal(streamErr)
					}
					for range stream.Parts {
					}
					finished, finishErr := stream.Result()
					if finishErr != nil {
						t.Fatal(finishErr)
					}
					result = *finished
				} else {
					result, err = model.Generate(context.Background(), request)
					if err != nil {
						t.Fatal(err)
					}
				}
				usage := result.Usage
				if result.Text != "ok" || usage.InputTokens != 1000 || usage.OutputTokens != 200 || usage.TotalTokens != 1200 || usage.CachedInputTokens != 400 || usage.InputTokenDetails.CacheReadTokens != 400 || usage.InputTokenDetails.CacheWriteTokens != tt.write || usage.InputTokenDetails.NoCacheTokens != tt.uncached {
					t.Fatalf("text=%q usage=%+v, want input=1000 read=400 write=%d uncached=%d total=1200", result.Text, usage, tt.write, tt.uncached)
				}
			})
		}
	}
}
