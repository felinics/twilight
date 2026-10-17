package effect_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"

	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/sdk"
)

type timeoutError struct{}

func (timeoutError) Error() string   { return "timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

var _ net.Error = timeoutError{}

// provider errors classify by HTTP status, transport errors as
// connection failures, everything else as executor_error; the disposition
// is read off the class, and only the rate limit, provider outage and
// connection classes allow a retry.
func TestClassifyModelError(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		want  effect.FailureCode
		retry turn.RetryDisposition
	}{
		{"429", &sdk.APIError{StatusCode: 429}, effect.FailureRateLimited, turn.RetryAllowed},
		{"503 wrapped", errors.Join(errors.New("openai"), &sdk.APIError{StatusCode: 503}), effect.FailureProviderUnavailable, turn.RetryAllowed},
		{"401", &sdk.APIError{StatusCode: 401}, effect.FailureAuthentication, turn.RetryNever},
		{"402", &sdk.APIError{StatusCode: 402}, effect.FailureBilling, turn.RetryNever},
		{"400", &sdk.APIError{StatusCode: 400}, effect.FailureBadRequest, turn.RetryNever},
		{"connection reset", timeoutError{}, effect.FailureConnection, turn.RetryAllowed},
		{"unexpected eof", io.ErrUnexpectedEOF, effect.FailureConnection, turn.RetryAllowed},
		{"plain error", errors.New("boom"), effect.FailureExecutor, turn.RetryNever},
		{"deadline as net error", &net.OpError{Err: context.DeadlineExceeded}, effect.FailureDeadline, turn.RetryNever},
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
