package http_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	executorhttp "github.com/felinics/twilight/agent/executor/http"
	"github.com/felinics/twilight/agentcore/run/effect"
)

type failingTransport struct{}

func (failingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("connection reset")
}

func TestDispatchTransportFailureIsUnknown(t *testing.T) {
	client := &executorhttp.Client{
		BaseURL: "http://executor.invalid",
		HTTP:    &http.Client{Transport: failingTransport{}},
	}
	err := client.Dispatch(context.Background(), effect.Assignment{})
	if !errors.Is(err, effect.ErrDispatchUnknown) {
		t.Fatalf("dispatch error = %v, want ErrDispatchUnknown", err)
	}
}

type responseTransport struct{ status int }

func (t responseTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: t.status,
		Status:     http.StatusText(t.status),
		Body:       io.NopCloser(strings.NewReader("dispatch response")),
		Header:     make(http.Header),
	}, nil
}

// RUN-EXE-3: 400 and 409 are the Server's own Known refusals, 503 is its
// retryable refusal, any other 4xx is an intermediary's refusal and
// retryable, any other 5xx or a lost response is an unknown outcome.
func TestDispatchResponseClassification(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusConflict, http.StatusNotFound, http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusServiceUnavailable, http.StatusInternalServerError, http.StatusBadGateway, http.StatusGatewayTimeout} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			client := &executorhttp.Client{BaseURL: "http://executor.invalid", HTTP: &http.Client{Transport: responseTransport{status: status}}}
			err := client.Dispatch(context.Background(), effect.Assignment{})
			known := status == http.StatusBadRequest || status == http.StatusConflict
			wantRetryable := status == http.StatusServiceUnavailable || (status < http.StatusInternalServerError && !known)
			wantUnknown := status >= http.StatusInternalServerError && status != http.StatusServiceUnavailable
			if err == nil || errors.Is(err, effect.ErrDispatchUnknown) != wantUnknown || errors.Is(err, effect.ErrDispatchRetryable) != wantRetryable {
				t.Fatalf("dispatch status %d = %v", status, err)
			}
		})
	}
}

func TestDispatchInvalidEnvelopeIsRejectedBeforeWorker(t *testing.T) {
	for _, body := range []string{"invalid json", `{"protocolVersion":0}`} {
		t.Run(body, func(t *testing.T) {
			request := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/dispatch", strings.NewReader(body))
			response := httptest.NewRecorder()
			(&executorhttp.Server{}).Handler().ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
		})
	}
}

type cancellingTransport struct {
	cancel context.CancelFunc
	calls  int
}

func (t *cancellingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	t.calls++
	t.cancel()
	return nil, req.Context().Err()
}

func TestDispatchCancellation(t *testing.T) {
	for _, alreadyCancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "after send", true: "before send"}[alreadyCancelled], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			transport := &cancellingTransport{cancel: cancel}
			client := &executorhttp.Client{BaseURL: "http://executor.invalid", HTTP: &http.Client{Transport: transport}}
			if alreadyCancelled {
				cancel()
			}
			err := client.Dispatch(ctx, effect.Assignment{})
			if !errors.Is(err, context.Canceled) || errors.Is(err, effect.ErrDispatchUnknown) == alreadyCancelled {
				t.Fatalf("dispatch error = %v, already cancelled = %v", err, alreadyCancelled)
			}
			wantCalls := 1
			if alreadyCancelled {
				wantCalls = 0
			}
			if transport.calls != wantCalls {
				t.Fatalf("transport calls = %d, want %d", transport.calls, wantCalls)
			}
		})
	}
}
