package utils

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func countingServer(t *testing.T, handler func(attempt int32, w http.ResponseWriter)) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handler(calls.Add(1), w)
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func fastPolicy(maxAttempts int) *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts: maxAttempts,
		BaseDelay:   time.Millisecond,
		MaxDelay:    5 * time.Millisecond,
		RetryStatus: func(status int) bool {
			return status == http.StatusTooManyRequests || status == 529
		},
	}
}

func TestSend_NoPolicySendsOnce(t *testing.T) {
	srv, calls := countingServer(t, func(_ int32, w http.ResponseWriter) {
		w.WriteHeader(http.StatusTooManyRequests)
	})

	resp, err := send(context.Background(), srv.Client(), &RequestOptions{BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	drainAndClose(resp)

	if got := calls.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1", got)
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d, want 429", resp.StatusCode)
	}
}

func TestSend_RetriesRateLimitThenSucceeds(t *testing.T) {
	srv, calls := countingServer(t, func(attempt int32, w http.ResponseWriter) {
		if attempt < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	resp, err := send(context.Background(), srv.Client(), &RequestOptions{BaseURL: srv.URL, Retry: fastPolicy(3)})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	drainAndClose(resp)

	if got := calls.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

// 529 Overloaded is not a standard code, so it is only retried because the
// policy names it.
func TestSend_RetriesOverloaded(t *testing.T) {
	srv, calls := countingServer(t, func(attempt int32, w http.ResponseWriter) {
		if attempt < 2 {
			w.WriteHeader(529)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	resp, err := send(context.Background(), srv.Client(), &RequestOptions{BaseURL: srv.URL, Retry: fastPolicy(3)})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	drainAndClose(resp)

	if got := calls.Load(); got != 2 {
		t.Errorf("attempts = %d, want 2", got)
	}
}

func TestSend_DoesNotRetryClientErrors(t *testing.T) {
	for _, status := range []int{http.StatusUnauthorized, http.StatusUnprocessableEntity} {
		srv, calls := countingServer(t, func(_ int32, w http.ResponseWriter) {
			w.WriteHeader(status)
		})

		resp, err := send(context.Background(), srv.Client(), &RequestOptions{BaseURL: srv.URL, Retry: fastPolicy(3)})
		if err != nil {
			t.Fatalf("send: %v", err)
		}
		drainAndClose(resp)

		if got := calls.Load(); got != 1 {
			t.Errorf("status %d: attempts = %d, want 1", status, got)
		}
	}
}

func TestSend_ReturnsLastResponseWhenAttemptsRunOut(t *testing.T) {
	srv, calls := countingServer(t, func(_ int32, w http.ResponseWriter) {
		w.WriteHeader(http.StatusTooManyRequests)
	})

	resp, err := send(context.Background(), srv.Client(), &RequestOptions{BaseURL: srv.URL, Retry: fastPolicy(3)})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	drainAndClose(resp)

	if got := calls.Load(); got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("status = %d, want the final 429", resp.StatusCode)
	}
}

func TestSend_HonorsRetryAfter(t *testing.T) {
	srv, calls := countingServer(t, func(attempt int32, w http.ResponseWriter) {
		if attempt == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	policy := fastPolicy(3)
	policy.BaseDelay = time.Hour // only a honoured Retry-After can keep this fast
	policy.MaxDelay = time.Hour

	// "0" means retry immediately, which must not read as "no header given".

	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := send(context.Background(), srv.Client(), &RequestOptions{BaseURL: srv.URL, Retry: policy})
		if err != nil {
			t.Errorf("send: %v", err)
			return
		}
		drainAndClose(resp)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Retry-After was ignored; the request is still backing off")
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("attempts = %d, want 2", got)
	}
}

func TestSend_ContextCancelledDuringBackoff(t *testing.T) {
	srv, calls := countingServer(t, func(_ int32, w http.ResponseWriter) {
		w.WriteHeader(http.StatusTooManyRequests)
	})

	policy := fastPolicy(5)
	policy.BaseDelay = time.Hour
	policy.MaxDelay = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	resp, err := send(ctx, srv.Client(), &RequestOptions{BaseURL: srv.URL, Retry: policy})
	if err == nil {
		resp.Body.Close()
		t.Fatal("expected the cancelled context to surface")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 before the cancel landed", got)
	}
}

func TestSend_RetriesTransportError(t *testing.T) {
	srv, _ := countingServer(t, func(_ int32, w http.ResponseWriter) {
		w.WriteHeader(http.StatusOK)
	})
	unreachable := srv.URL
	srv.Close()

	var seen atomic.Int32
	policy := fastPolicy(3)
	policy.RetryTransportError = func(error) bool {
		seen.Add(1)
		return true
	}

	resp, err := send(context.Background(), srv.Client(), &RequestOptions{BaseURL: unreachable, Retry: policy})
	if err == nil {
		resp.Body.Close()
		t.Fatal("expected a transport error")
	}
	if got := seen.Load(); got != 2 {
		t.Errorf("retry decisions = %d, want 2 (the last attempt is not asked)", got)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)

	tests := []struct {
		name    string
		header  string
		want    time.Duration
		wantHas bool
	}{
		{name: "absent", header: "", want: 0, wantHas: false},
		{name: "seconds", header: "12", want: 12 * time.Second, wantHas: true},
		{name: "retry immediately", header: "0", want: 0, wantHas: true},
		{name: "negative seconds", header: "-5", want: 0, wantHas: false},
		{name: "http date", header: now.Add(30 * time.Second).Format(http.TimeFormat), want: 30 * time.Second, wantHas: true},
		{name: "past http date", header: now.Add(-time.Minute).Format(http.TimeFormat), want: 0, wantHas: true},
		{name: "garbage", header: "soon", want: 0, wantHas: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			header := http.Header{}
			if tc.header != "" {
				header.Set("Retry-After", tc.header)
			}
			got, has := parseRetryAfter(header, now)
			if got != tc.want || has != tc.wantHas {
				t.Errorf("got %v, %v; want %v, %v", got, has, tc.want, tc.wantHas)
			}
		})
	}
}

func TestRetryPolicy_BackoffGrowsAndCaps(t *testing.T) {
	policy := &RetryPolicy{MaxAttempts: 10, BaseDelay: time.Second, MaxDelay: 4 * time.Second}

	for attempt, minWant := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 9: 4 * time.Second} {
		got := policy.backoff(attempt, 0, false)
		if got < minWant || got > policy.MaxDelay {
			t.Errorf("backoff(%d) = %v, want between %v and %v", attempt, got, minWant, policy.MaxDelay)
		}
	}

	if got := policy.backoff(1, 30*time.Second, true); got != policy.MaxDelay {
		t.Errorf("a Retry-After past MaxDelay = %v, want it capped at %v", got, policy.MaxDelay)
	}
	if got := policy.backoff(1, 2*time.Second, true); got != 2*time.Second {
		t.Errorf("Retry-After = %v, want it used verbatim", got)
	}
}

func TestRetryPolicy_NilIsSingleAttempt(t *testing.T) {
	var policy *RetryPolicy
	if got := policy.attempts(); got != 1 {
		t.Errorf("attempts = %d, want 1", got)
	}
	if policy.shouldRetryStatus(http.StatusTooManyRequests) {
		t.Error("a nil policy should not retry")
	}
}
