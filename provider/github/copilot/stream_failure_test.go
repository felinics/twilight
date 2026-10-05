package copilot_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/felinics/twilight/provider/github/copilot"
	"github.com/felinics/twilight/sdk"
)

func TestDoStreamFailureAfterFinishReasonDoesNotFinishStep(t *testing.T) {
	finish := "data: {\"id\":\"c1\",\"object\":\"chat.completion.chunk\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"ok\"},\"finish_reason\":\"stop\"}]}\n\n"
	for _, tt := range []struct {
		name string
		tail string
		cut  bool
	}{
		{name: "malformed trailing chunk", tail: "data: {\n\n"},
		{name: "connection cut before usage", cut: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tt.cut {
					conn, buf, err := http.NewResponseController(w).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_, _ = fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n%x\r\n%s\r\n", len(finish), finish)
					_ = buf.Flush()
					_ = conn.Close()
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(finish + tt.tail))
			}))
			defer srv.Close()
			p := copilot.New(copilot.WithGitHubToken(testToken()), copilot.WithBaseURL(srv.URL))
			parts, err := p.DoStream(context.Background(), sdk.Request{Model: copilot.AutoModel, Messages: []sdk.Message{sdk.UserMessage("Hi")}})
			if err != nil {
				t.Fatal(err)
			}
			var failures, stepEnds int
			var end *sdk.FinishPart
			for part := range parts {
				switch part := part.(type) {
				case *sdk.ErrorPart:
					failures++
				case *sdk.FinishStepPart:
					stepEnds++
				case *sdk.FinishPart:
					end = part
				}
			}
			if failures != 1 || stepEnds != 0 || end == nil || end.FinishReason != sdk.FinishReasonError {
				t.Fatalf("errors=%d stepEnds=%d finish=%+v", failures, stepEnds, end)
			}
		})
	}
}
