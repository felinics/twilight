package messages_test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felinics/twilight/provider/anthropic/messages"
	"github.com/felinics/twilight/sdk"
)

type usageRoundTripper func(*http.Request) (*http.Response, error)

func (f usageRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type usageResponseBody struct {
	io.Reader
	close func()
}

func (b usageResponseBody) Close() error { b.close(); return nil }

func TestStreamFailureDoesNotFinishStep(t *testing.T) {
	start := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_usage\",\"usage\":{\"input_tokens\":10,\"cache_read_input_tokens\":200}}}\n\n"
	stop := "event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":5}}\n\n"
	for _, tt := range []struct {
		name          string
		tail          string
		cancelOnClose bool
	}{
		{name: "error after stop", tail: stop + "event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"fixture\"}}\n\n"},
		{name: "malformed delta", tail: "event: message_delta\ndata: {\n\n"},
		{name: "EOF without stop", tail: "event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":5}}\n\n"},
		{name: "cancellation when response closes", tail: stop, cancelOnClose: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for range 32 {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				var closed atomic.Int32
				client := &http.Client{Transport: usageRoundTripper(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: usageResponseBody{
						Reader: strings.NewReader(start + tt.tail),
						close: func() {
							closed.Add(1)
							if tt.cancelOnClose {
								cancel()
							}
						},
					}}, nil
				})}
				provider := messages.New(messages.WithAPIKey("fixture"), messages.WithHTTPClient(client))
				parts, err := provider.DoStream(ctx, sdk.Request{Model: "claude-test", Messages: []sdk.Message{sdk.UserMessage("hi")}})
				if err != nil {
					cancel()
					t.Fatal(err)
				}
				var stepEnds, failures int
				for part := range parts {
					switch part.(type) {
					case *sdk.FinishStepPart:
						stepEnds++
					case *sdk.ErrorPart:
						failures++
					}
				}
				if stepEnds != 0 || closed.Load() != 1 || (!tt.cancelOnClose && failures != 1) {
					cancel()
					t.Fatalf("stepEnds=%d errors=%d bodyCloses=%d", stepEnds, failures, closed.Load())
				}
				cancel()
			}
		})
	}
}
