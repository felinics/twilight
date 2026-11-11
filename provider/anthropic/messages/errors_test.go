package messages

import (
	"net/http"
	"testing"

	"github.com/felinics/twilight/provider/providertest"
	"github.com/felinics/twilight/sdk"
)

// Error types, statuses and the request-id header:
// https://platform.claude.com/docs/en/api/errors
func TestDecodeError(t *testing.T) {
	body := func(typ, msg string) string {
		return `{"type":"error","error":{"type":"` + typ + `","message":"` + msg + `"},"request_id":"req_body"}`
	}
	header := http.Header{"Request-Id": {"req_018EeWyXxfu5pfWkrYcMdjWG"}}
	cases := []providertest.ErrorCase{
		{Name: "authentication", Status: 401, Header: header, Body: body("authentication_error", "invalid x-api-key"),
			Want: sdk.APIError{Type: "authentication_error", Message: "invalid x-api-key", RequestID: "req_018EeWyXxfu5pfWkrYcMdjWG", Kind: sdk.KindAuthentication}},
		{Name: "billing", Status: 402, Body: body("billing_error", "billing"),
			Want: sdk.APIError{Type: "billing_error", Message: "billing", RequestID: "req_body", Kind: sdk.KindQuotaExhausted}},
		{Name: "permission", Status: 403, Body: body("permission_error", "denied"),
			Want: sdk.APIError{Type: "permission_error", Message: "denied", RequestID: "req_body", Kind: sdk.KindPermissionDenied}},
		{Name: "rate limit", Status: 429, Body: body("rate_limit_error", "slow down"),
			Want: sdk.APIError{Type: "rate_limit_error", Message: "slow down", RequestID: "req_body", Kind: sdk.KindRateLimited}},
		{Name: "api error", Status: 500, Body: body("api_error", "internal"),
			Want: sdk.APIError{Type: "api_error", Message: "internal", RequestID: "req_body", Kind: sdk.KindServerError}},
		{Name: "overloaded", Status: 529, Body: body("overloaded_error", "Overloaded"),
			Want: sdk.APIError{Type: "overloaded_error", Message: "Overloaded", RequestID: "req_body", Kind: sdk.KindServerError}},
		// A spend limit arrives as 400 invalid_request_error and has no
		// structured marker, so it stays unknown rather than read from text.
		{Name: "invalid request", Status: 400, Body: body("invalid_request_error", "spend limit"),
			Want: sdk.APIError{Type: "invalid_request_error", Message: "spend limit", RequestID: "req_body", Kind: sdk.KindUnknown}},
		{Name: "unknown type falls back to status", Status: 503, Body: body("new_error", "x"),
			Want: sdk.APIError{Type: "new_error", Message: "x", RequestID: "req_body", Kind: sdk.KindServerError}},
		{Name: "proxy html", Status: 502, Header: header, Body: `<html>Bad Gateway</html>`,
			Want: sdk.APIError{RequestID: "req_018EeWyXxfu5pfWkrYcMdjWG", Kind: sdk.KindServerError}},
	}
	providertest.RunErrorCases(t, "anthropic-messages", decodeError, cases)
}

// Every error type of the ErrorResponse schema, with the schema's default
// message, in
// https://github.com/anthropics/anthropic-sdk-go/blob/ad865dfa3d1a8d2f4a7ad0d072011e811e9957a9/scripts/mock-spec.json.gz
// (MIT, Copyright 2023 Anthropic, PBC.), and the two types that only
// https://platform.claude.com/docs/en/api/errors lists, conflict_error and
// request_too_large. The statuses are the documentation's. The documentation
// gives no message for those two, so theirs are placeholders.
func TestDecodeErrorTypes(t *testing.T) {
	types := []struct {
		status       int
		typ, message string
		kind         sdk.ErrorKind
	}{
		{400, "invalid_request_error", "Invalid request", sdk.KindUnknown},
		{401, "authentication_error", "Authentication error", sdk.KindAuthentication},
		{402, "billing_error", "Billing error", sdk.KindQuotaExhausted},
		{403, "permission_error", "Permission denied", sdk.KindPermissionDenied},
		{404, "not_found_error", "Not found", sdk.KindUnknown},
		{409, "conflict_error", "Conflict", sdk.KindUnknown},
		{413, "request_too_large", "Request too large", sdk.KindUnknown},
		{429, "rate_limit_error", "Rate limited", sdk.KindRateLimited},
		{500, "api_error", "Internal server error", sdk.KindServerError},
		{504, "timeout_error", "Request timeout", sdk.KindServerError},
		{529, "overloaded_error", "Overloaded", sdk.KindServerError},
	}
	cases := make([]providertest.ErrorCase, 0, len(types))
	for _, et := range types {
		cases = append(cases, providertest.ErrorCase{
			Name:   et.typ,
			Status: et.status,
			Body:   `{"type":"error","error":{"type":"` + et.typ + `","message":"` + et.message + `"},"request_id":"req_body"}`,
			Want:   sdk.APIError{Type: et.typ, Message: et.message, RequestID: "req_body", Kind: et.kind},
		})
	}
	providertest.RunErrorCases(t, "anthropic-messages", decodeError, cases)
}

// OpenRouter's Messages endpoint puts its metadata at the top level. The
// first two bodies are verbatim from OpenRouter on 2026-10-15; the third is
// constructed.
func TestDecodeErrorOpenRouterUpstream(t *testing.T) {
	const alibabaMessage = "<400> InternalError.Algo.InvalidParameter: The tool_choice parameter does not support being set to required or object in thinking mode"
	cases := []providertest.ErrorCase{
		{Name: "no provider reached", Status: 400,
			Body: `{"type":"error","error":{"type":"invalid_request_error","message":"Expected temperature to be at most 2, received 50","error_type":"invalid_request"},"request_id":"gen-1792055641-LJ9gOVcgPY6jjVAUEQsq","metadata":{"provider_name":null}}`,
			Want: sdk.APIError{Type: "invalid_request_error", Message: "Expected temperature to be at most 2, received 50",
				RequestID: "gen-1792055641-LJ9gOVcgPY6jjVAUEQsq", Kind: sdk.KindUnknown}},
		{Name: "upstream message already in message", Status: 400,
			Body: `{"type":"error","error":{"type":"invalid_request_error","message":"<400> InternalError.Algo.InvalidParameter: The tool_choice parameter does not support being set to required or object in thinking mode","error_type":"invalid_request"},"request_id":"gen-1792055670-rWjJulPaod4C5NYsQekK","metadata":{"raw":"{\"error\":{\"message\":\"<400> InternalError.Algo.InvalidParameter: The tool_choice parameter does not support being set to required or object in thinking mode\",\"type\":\"invalid_request_error\",\"param\":null,\"code\":\"invalid_parameter_error\"},\"id\":\"chatcmpl-4fa49dde-5456-4c31-be20-c7c1ab6ee0ed\",\"request_id\":\"4fa49dde-5456-4c31-be20-c7c1ab6ee0ed\"}","provider_name":"Alibaba","is_byok":false,"provider_error_code":"invalid_parameter_error"}}`,
			Want: sdk.APIError{Type: "invalid_request_error", Message: alibabaMessage + " (Alibaba)",
				RequestID: "gen-1792055670-rWjJulPaod4C5NYsQekK", Kind: sdk.KindUnknown}},
		{Name: "generic message", Status: 422,
			Body: `{"type":"error","error":{"type":"invalid_request_error","message":"Provider returned error"},"request_id":"gen-1","metadata":{"raw":"{\"error\":{\"message\":\"Input should be 'text'\",\"type\":\"invalid_request_error\"}}","provider_name":"DeepInfra"}}`,
			Want: sdk.APIError{Type: "invalid_request_error", Message: "Provider returned error (DeepInfra: Input should be 'text')",
				RequestID: "gen-1", Kind: sdk.KindUnknown}},
		{Name: "garbage raw", Status: 400,
			Body: `{"type":"error","error":{"type":"invalid_request_error","message":"Provider returned error"},"request_id":"gen-2","metadata":{"raw":"{not json","provider_name":"DeepInfra"}}`,
			Want: sdk.APIError{Type: "invalid_request_error", Message: "Provider returned error", RequestID: "gen-2", Kind: sdk.KindUnknown}},
	}
	providertest.RunErrorCases(t, "anthropic-messages", decodeError, cases)
}
