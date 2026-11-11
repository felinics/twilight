package sdk_test

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/felinics/twilight/sdk"
)

func TestAPIErrorRetryAfter(t *testing.T) {
	future := time.Now().Add(90 * time.Second).UTC().Format(http.TimeFormat)
	past := time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat)
	tests := []struct {
		name    string
		header  http.Header
		want    time.Duration
		wantOK  bool
		between [2]time.Duration
	}{
		{name: "retry-after-ms", header: http.Header{"Retry-After-Ms": {"1500"}}, want: 1500 * time.Millisecond, wantOK: true},
		{name: "fractional retry-after-ms", header: http.Header{"Retry-After-Ms": {"2.5"}}, want: 2500 * time.Microsecond, wantOK: true},
		{name: "seconds", header: http.Header{"Retry-After": {"20"}}, want: 20 * time.Second, wantOK: true},
		{name: "retry-after-ms wins over seconds", header: http.Header{"Retry-After-Ms": {"250"}, "Retry-After": {"20"}}, want: 250 * time.Millisecond, wantOK: true},
		{name: "unparsable retry-after-ms falls back to seconds", header: http.Header{"Retry-After-Ms": {"soon"}, "Retry-After": {"3"}}, want: 3 * time.Second, wantOK: true},
		{name: "http date", header: http.Header{"Retry-After": {future}}, wantOK: true, between: [2]time.Duration{80 * time.Second, 90 * time.Second}},
		{name: "http date in the past", header: http.Header{"Retry-After": {past}}, want: 0, wantOK: true},
		{name: "absent", header: http.Header{}, wantOK: false},
		{name: "nil header", header: nil, wantOK: false},
		{name: "unparsable", header: http.Header{"Retry-After": {"later"}}, wantOK: false},
		{name: "negative seconds", header: http.Header{"Retry-After": {"-5"}}, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := (&sdk.APIError{Header: tt.header}).RetryAfter()
			if ok != tt.wantOK {
				t.Fatalf("RetryAfter() ok = %v, want %v", ok, tt.wantOK)
			}
			if tt.between != [2]time.Duration{} {
				if got < tt.between[0] || got > tt.between[1] {
					t.Fatalf("RetryAfter() = %v, want within %v", got, tt.between)
				}
				return
			}
			if got != tt.want {
				t.Fatalf("RetryAfter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAPIErrorErrorText(t *testing.T) {
	tests := []struct {
		name string
		err  *sdk.APIError
		want string
	}{
		{
			name: "all parts",
			err:  &sdk.APIError{Provider: "anthropic-messages", StatusCode: 429, Type: "rate_limit_error", Message: "slow down", RequestID: "req_1"},
			want: "anthropic-messages: 429 rate_limit_error: slow down (request-id req_1)",
		},
		{
			name: "type and code",
			err:  &sdk.APIError{Provider: "openai-completions", StatusCode: 401, Type: "invalid_request_error", Code: "invalid_api_key", Message: "bad key"},
			want: "openai-completions: 401 invalid_request_error invalid_api_key: bad key",
		},
		{
			name: "type equal to code is written once",
			err:  &sdk.APIError{Provider: "openai-completions", StatusCode: 429, Type: "insufficient_quota", Code: "insufficient_quota"},
			want: "openai-completions: 429 insufficient_quota",
		},
		{
			name: "no status",
			err:  &sdk.APIError{Provider: "anthropic-messages", Type: "overloaded_error", Message: "Overloaded"},
			want: "anthropic-messages: error overloaded_error: Overloaded",
		},
		{
			name: "code repeating the status is written once",
			err:  &sdk.APIError{Provider: "openai-completions", StatusCode: 422, Code: "422", Message: "Provider returned error"},
			want: "openai-completions: 422: Provider returned error",
		},
		{
			name: "numeric code other than the status",
			err:  &sdk.APIError{Provider: "openai-completions", StatusCode: 400, Code: "422", Message: "bad"},
			want: "openai-completions: 400 422: bad",
		},
		{
			name: "numeric code without status",
			err:  &sdk.APIError{Provider: "openai-responses", Code: "502", Message: "boom"},
			want: "openai-responses: error 502: boom",
		},
		{
			name: "status only",
			err:  &sdk.APIError{StatusCode: 502},
			want: "502",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.err.Error(); got != tt.want {
				t.Fatalf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAPIErrorTextExcludesBodyAndHeaders(t *testing.T) {
	const bodySecret = "body-marker-7f3a"
	const headerSecret = "header-marker-7f3a"
	err := &sdk.APIError{
		Provider:   "openai-completions",
		StatusCode: 400,
		Message:    "bad request",
		Header:     http.Header{"X-Echo": {headerSecret}, "Retry-After": {"1"}},
		Body:       []byte(`{"error":{"message":"bad request","prompt":"` + bodySecret + `"}}`),
	}
	wrapped := fmt.Errorf("call failed: %w", err)
	for _, text := range []string{err.Error(), wrapped.Error(), fmt.Sprintf("%v", err), fmt.Sprintf("%+v", wrapped)} {
		if strings.Contains(text, bodySecret) || strings.Contains(text, headerSecret) {
			t.Fatalf("error text %q leaks the body or a header", text)
		}
	}
}

func TestKindOf(t *testing.T) {
	rateLimited := &sdk.APIError{StatusCode: 429, Kind: sdk.KindRateLimited}
	tests := []struct {
		name string
		err  error
		want sdk.ErrorKind
	}{
		{name: "nil", err: nil, want: sdk.KindUnknown},
		{name: "not an APIError", err: errors.New("dial tcp: connection refused"), want: sdk.KindUnknown},
		{name: "direct", err: rateLimited, want: sdk.KindRateLimited},
		{name: "wrapped", err: fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", rateLimited)), want: sdk.KindRateLimited},
		{name: "joined", err: errors.Join(errors.New("other"), rateLimited), want: sdk.KindRateLimited},
		{name: "first in chain wins", err: fmt.Errorf("%w", &sdk.APIError{Kind: sdk.KindAuthentication}), want: sdk.KindAuthentication},
		{name: "unset kind", err: &sdk.APIError{StatusCode: 400}, want: sdk.KindUnknown},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sdk.KindOf(tt.err); got != tt.want {
				t.Fatalf("KindOf() = %q, want %q", got, tt.want)
			}
		})
	}
}
