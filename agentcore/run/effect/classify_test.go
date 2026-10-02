package effect_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

type statusError int

func (e statusError) Error() string   { return "status" }
func (e statusError) HTTPStatus() int { return int(e) }

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

var _ net.Error = timeoutError{}

// RUN-EXE-11: provider errors classify by HTTP status, transport errors as
// connection failures, everything else as executor_error; the disposition
// is read off the class, and only the rate limit, provider outage and
// connection classes allow a retry.
func TestClassifyModelError(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		want  effect.FailureCode
		retry run.RetryDisposition
	}{
		{"429", statusError(429), effect.FailureRateLimited, run.RetryAllowed},
		{"503 wrapped", errors.Join(errors.New("openai"), statusError(503)), effect.FailureProviderUnavailable, run.RetryAllowed},
		{"401", statusError(401), effect.FailureAuthentication, run.RetryNever},
		{"402", statusError(402), effect.FailureBilling, run.RetryNever},
		{"400", statusError(400), effect.FailureBadRequest, run.RetryNever},
		{"connection reset", timeoutError{}, effect.FailureConnection, run.RetryAllowed},
		{"unexpected eof", io.ErrUnexpectedEOF, effect.FailureConnection, run.RetryAllowed},
		{"plain error", errors.New("boom"), effect.FailureExecutor, run.RetryNever},
		{"deadline as net error", &net.OpError{Err: context.DeadlineExceeded}, effect.FailureDeadline, run.RetryNever},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := effect.ModelFailed{Code: effect.ClassifyModelError(tc.err), Message: tc.err.Error()}
			if got.Code != tc.want || got.Retry() != tc.retry {
				t.Fatalf("classify = %s (retry %s), want %s (%s)", got.Code, got.Retry(), tc.want, tc.retry)
			}
		})
	}
}
