package http_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	executorhttp "github.com/felinics/twilight/agent/executor/http"
)

// The handler rejects what the Worker must never see: a method other than
// POST, a body past MaxBodyBytes, and a body that is not JSON.
func TestServerRejectsBeforeWorker(t *testing.T) {
	cases := []struct {
		name   string
		method string
		body   string
		want   int
	}{
		{"method", http.MethodGet, `{"key":{}}`, http.StatusMethodNotAllowed},
		{"oversized body", http.MethodPost, `{"key":{"session":"` + strings.Repeat("s", 64) + `"}}`, http.StatusRequestEntityTooLarge},
		{"malformed body", http.MethodPost, `{"key":`, http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequestWithContext(context.Background(), tc.method, "/status", strings.NewReader(tc.body))
			response := httptest.NewRecorder()
			(&executorhttp.Server{MaxBodyBytes: 32}).Handler().ServeHTTP(response, request)
			if response.Code != tc.want {
				t.Fatalf("%s /status with %d body bytes = %d, want %d", tc.method, len(tc.body), response.Code, tc.want)
			}
		})
	}
}
