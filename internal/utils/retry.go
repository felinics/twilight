package utils

import (
	"context"
	"net/http"
	"time"
)

// RetryPolicy configures retries for a request. It is opt-in: a nil
// *RetryPolicy on RequestOptions means a single attempt, which is the
// behaviour of every provider that does not set one.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts, including the first. Values
	// below 2 disable retrying.
	MaxAttempts int
	// BaseDelay is the delay before the second attempt. It doubles with each
	// further attempt, up to MaxDelay.
	BaseDelay time.Duration
	// MaxDelay caps the delay between attempts.
	MaxDelay time.Duration
	// RetryStatus reports whether a response status should be retried. When nil,
	// only 429 is retried.
	RetryStatus func(status int) bool
	// RetryTransportError reports whether a transport-level failure should be
	// retried. When nil, transport errors are retried.
	RetryTransportError func(err error) bool
}

// DefaultRetryPolicy returns a policy that retries rate limits and transient
// server errors a few times with exponential backoff.
func DefaultRetryPolicy() *RetryPolicy {
	return &RetryPolicy{
		MaxAttempts: 3,
		BaseDelay:   500 * time.Millisecond,
		MaxDelay:    8 * time.Second,
	}
}

func (p *RetryPolicy) attempts() int {
	if p == nil || p.MaxAttempts < 1 {
		return 1
	}
	return p.MaxAttempts
}

func (p *RetryPolicy) shouldRetryStatus(status int) bool {
	if p == nil {
		return false
	}
	if p.RetryStatus != nil {
		return p.RetryStatus(status)
	}
	return status == http.StatusTooManyRequests
}

func (p *RetryPolicy) shouldRetryTransportError(err error) bool {
	if p == nil {
		return false
	}
	if p.RetryTransportError != nil {
		return p.RetryTransportError(err)
	}
	return true
}

// backoff returns the delay before the given attempt, counted from 1 for the
// attempt after the first. A server-supplied Retry-After always wins, so a
// provider that tells us when to come back is obeyed rather than guessed at.
// hasRetryAfter distinguishes a header asking for an immediate retry from no
// header at all, which would otherwise both read as a zero delay.
func (p *RetryPolicy) backoff(attempt int, retryAfter time.Duration, hasRetryAfter bool) time.Duration {
	base := 500 * time.Millisecond
	maxDelay := 8 * time.Second
	if p != nil {
		if p.BaseDelay > 0 {
			base = p.BaseDelay
		}
		if p.MaxDelay > 0 {
			maxDelay = p.MaxDelay
		}
	}

	if hasRetryAfter {
		if retryAfter > maxDelay {
			return maxDelay
		}
		return retryAfter
	}

	delay := base
	for range attempt - 1 {
		delay *= 2
		if delay >= maxDelay {
			return maxDelay
		}
	}
	// Spread retries that started together, so a burst of callers hitting the
	// same rate limit does not come back in lockstep. The clock is a good enough
	// source of spread for backoff and keeps this free of a PRNG.
	jitter := time.Duration(time.Now().UnixNano() % int64(delay/4+1))
	if delay+jitter > maxDelay {
		return maxDelay
	}
	return delay + jitter
}

// parseRetryAfter reads the Retry-After header, which may hold either a delay
// in seconds or an HTTP date. The second return reports whether a usable value
// was found: a header of "0" asks for an immediate retry, which is not the same
// as no header at all.
func parseRetryAfter(header http.Header, now time.Time) (time.Duration, bool) {
	value := header.Get("Retry-After")
	if value == "" {
		return 0, false
	}

	if seconds, err := time.ParseDuration(value + "s"); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return seconds, true
	}

	if when, err := http.ParseTime(value); err == nil {
		if delay := when.Sub(now); delay > 0 {
			return delay, true
		}
		// The date has already passed, so the server is asking for an immediate
		// retry rather than declining to say.
		return 0, true
	}
	return 0, false
}

// sleep waits for d, or returns early if the context is done.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
