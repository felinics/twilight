package sdk

import (
	"errors"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// APIError reports a failure that the provider itself reported: a non-2xx
// HTTP response, an error object inside a 2xx body, an error event inside a
// stream, or a failure frame on a WebSocket session.
//
// Transport failures (DNS, TLS, connection resets), context cancellation,
// response decoding failures and local validation are not APIErrors; they are
// returned as ordinary errors that wrap their cause.
//
// Always handle it as a pointer:
//
//	var apiErr *sdk.APIError
//	if errors.As(err, &apiErr) { ... }
//
// The names and meanings of the fields are stable. The contents of Type, Code,
// Message and Body are the provider's own and change when the provider changes
// them; do not depend on them beyond what the provider documents. The text
// returned by Error is for humans and has no stable format.
type APIError struct {
	// Provider identifies the provider that reported the failure. For chat
	// providers it is the Name() of the provider that sent the request, e.g.
	// "anthropic-messages". A provider that delegates requests to another
	// provider reports the delegate's name.
	Provider string

	// StatusCode is the HTTP status that reported this failure. It is 0 when
	// the HTTP status line did not report it: an error event after a 2xx
	// stream started, an error object inside a 2xx body, or a WebSocket frame.
	StatusCode int

	// Type and Code are the provider's own error identifiers, copied verbatim.
	// Which of them a provider fills is provider-specific; either may be empty.
	Type string
	Code string

	// Message is the provider's human-readable message, copied verbatim.
	// When OpenRouter relays the failure of an upstream provider, the
	// upstream provider's name and the start of its message follow in
	// parentheses, e.g. "Provider returned error (DeepInfra: ...)".
	Message string

	// RequestID is the provider's identifier for the failed request, when the
	// provider sends one. Quote it in support requests to the provider.
	RequestID string

	// Kind is the SDK's cross-provider classification of the failure.
	Kind ErrorKind

	// Header holds the HTTP response headers. It is set for stream errors too,
	// since the stream's response headers arrive before the stream starts. Nil
	// when there was no HTTP response.
	Header http.Header

	// Body is the raw response body, event payload or frame, as received. A
	// non-2xx body is cut at 1 MiB. It may echo parts of the request,
	// including end-user input, so do not log it verbatim. It is never part
	// of Error.
	Body []byte
}

// Error describes the failure with the provider, the HTTP status, the
// provider's type and code, its message and the request ID. A code that only
// repeats the HTTP status is left out. It never includes Body or any header.
func (e *APIError) Error() string {
	var b strings.Builder
	if e.Provider != "" {
		b.WriteString(e.Provider)
		b.WriteString(": ")
	}
	if e.StatusCode != 0 {
		b.WriteString(strconv.Itoa(e.StatusCode))
	} else {
		b.WriteString("error")
	}
	if e.Type != "" {
		b.WriteString(" ")
		b.WriteString(e.Type)
	}
	// OpenAI-compatible APIs such as OpenRouter repeat the HTTP status as a
	// numeric code; it is written once.
	if e.Code != "" && e.Code != e.Type && (e.StatusCode == 0 || e.Code != strconv.Itoa(e.StatusCode)) {
		b.WriteString(" ")
		b.WriteString(e.Code)
	}
	if e.Message != "" {
		b.WriteString(": ")
		b.WriteString(e.Message)
	}
	if e.RequestID != "" {
		b.WriteString(" (request-id ")
		b.WriteString(e.RequestID)
		b.WriteString(")")
	}
	return b.String()
}

// RetryAfter reports the delay the provider asked for before retrying, read
// from the retry-after-ms header (milliseconds) or the Retry-After header
// (delta-seconds or an HTTP-date), in that order. A date in the past yields
// zero. ok is false when neither header is present or parsable.
func (e *APIError) RetryAfter() (d time.Duration, ok bool) {
	if e.Header == nil {
		return 0, false
	}
	if ms, err := strconv.ParseFloat(e.Header.Get("retry-after-ms"), 64); err == nil && validDelay(ms) {
		return time.Duration(ms * float64(time.Millisecond)), true
	}
	value := e.Header.Get("Retry-After")
	if value == "" {
		return 0, false
	}
	if secs, err := strconv.ParseFloat(value, 64); err == nil {
		if !validDelay(secs) {
			return 0, false
		}
		return time.Duration(secs * float64(time.Second)), true
	}
	if at, err := http.ParseTime(value); err == nil {
		return max(time.Until(at), 0), true
	}
	return 0, false
}

func validDelay(v float64) bool {
	return v >= 0 && !math.IsInf(v, 0) && !math.IsNaN(v)
}

// ErrorKind classifies an APIError across providers.
//
// The set of values is open: later versions may add values, so a switch on
// ErrorKind must have a default branch. A response the SDK has no mapping for
// is KindUnknown, and a response that was KindUnknown may gain a specific Kind
// in a later version.
type ErrorKind string

const (
	// KindUnknown means the SDK has no classification for the failure.
	// StatusCode, Type and Code are still available.
	KindUnknown ErrorKind = "unknown"
	// KindAuthentication means the credentials are missing, invalid or
	// expired.
	KindAuthentication ErrorKind = "authentication"
	// KindPermissionDenied means the credentials are valid but lack access to
	// the requested resource.
	KindPermissionDenied ErrorKind = "permission_denied"
	// KindQuotaExhausted means the account's credit, billing or spend limit,
	// or a daily quota, is used up; waiting briefly does not help.
	KindQuotaExhausted ErrorKind = "quota_exhausted"
	// KindRateLimited means a request or token rate limit was hit; it is
	// transient.
	KindRateLimited ErrorKind = "rate_limited"
	// KindServerError means the provider failed or is overloaded; it is
	// transient.
	KindServerError ErrorKind = "server_error"
)

// KindOf returns the Kind of the first *APIError in err's chain. It returns
// KindUnknown when err is nil, when the chain holds no *APIError, or when the
// APIError has no Kind set.
func KindOf(err error) ErrorKind {
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Kind == "" {
		return KindUnknown
	}
	return apiErr.Kind
}
