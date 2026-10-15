package messages

import (
	"cmp"
	"encoding/json"

	"github.com/felinics/twilight/internal/errorformat"
	"github.com/felinics/twilight/sdk"
)

// errorBody is the Messages API error body:
// {"type":"error","error":{"type":"...","message":"..."},"request_id":"req_..."}.
// A stream's error event carries the same object as its data
// (https://platform.claude.com/docs/en/api/messages-streaming#error-events).
type errorBody struct {
	Error struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	} `json:"error"`
	RequestID string `json:"request_id"`
}

// decodeError fills e from the Messages API error body. The request ID comes
// from the request-id header, which the body repeats. OpenRouter's Messages
// endpoint adds a top-level metadata object; the upstream provider's failure
// it relays is appended to the message, see
// errorformat.AddOpenRouterUpstream.
func decodeError(e *sdk.APIError) {
	var body errorBody
	_ = json.Unmarshal(e.Body, &body)
	e.Type = body.Error.Type
	e.Message = body.Error.Message
	errorformat.AddOpenRouterUpstream(e)
	e.RequestID = cmp.Or(e.Header.Get("request-id"), body.RequestID)
	if k := kindFor(e.Type); k != sdk.KindUnknown {
		e.Kind = k
	}
}

// kindFor classifies an Anthropic error type. The same types appear in HTTP
// error bodies and in stream error events.
func kindFor(errorType string) sdk.ErrorKind {
	switch errorType {
	case "authentication_error":
		return sdk.KindAuthentication
	case "permission_error":
		return sdk.KindPermissionDenied
	case "billing_error":
		return sdk.KindQuotaExhausted
	case "rate_limit_error":
		return sdk.KindRateLimited
	case "api_error", "timeout_error", "overloaded_error":
		return sdk.KindServerError
	default:
		return sdk.KindUnknown
	}
}
