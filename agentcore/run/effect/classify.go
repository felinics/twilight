package effect

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"

	"github.com/felinics/twilight/sdk"
)

// ClassifyModelError maps a model invocation error to a wire-stable
// FailureCode (RUN-EXE-11). Provider errors that expose their HTTP status
// (sdk.HTTPStatusError) classify by status; transport errors classify as
// connection failures; anything else is FailureExecutor, which is not
// transient, so an unclassified error is never retried. Cancellation and the
// effect's own deadline are not failures of this kind and are left to the
// caller.
func ClassifyModelError(err error) FailureCode {
	var status sdk.HTTPStatusError
	if errors.As(err, &status) {
		switch code := status.HTTPStatus(); {
		case code == http.StatusTooManyRequests:
			return FailureRateLimited
		case code == http.StatusUnauthorized, code == http.StatusForbidden:
			return FailureAuthentication
		case code == http.StatusPaymentRequired:
			return FailureBilling
		case code >= 500:
			return FailureProviderUnavailable
		case code >= 400:
			return FailureBadRequest
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		if errors.Is(err, context.DeadlineExceeded) {
			return FailureDeadline
		}
		return FailureConnection
	}
	return FailureExecutor
}
