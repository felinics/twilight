package errorformat_test

import (
	"net/http"
	"testing"

	"github.com/felinics/twilight/internal/errorformat"
	"github.com/felinics/twilight/provider/providertest"
	"github.com/felinics/twilight/sdk"
)

func reqID(name, value string) http.Header {
	h := http.Header{}
	h.Set(name, value)
	return h
}

// OpenAI error codes and statuses: https://platform.openai.com/docs/guides/error-codes
// (mirrored at https://github.com/openai/openai-cookbook/blob/6dc6324fb9ed780b32b787f23fad336e9f1eff15/examples/data/oai_docs/error-codes.txt).
// The 401 body is verbatim from the official .NET SDK's recording:
// https://github.com/openai/openai-dotnet/blob/4e5ae90621089e0b1f5571546a75cea6b0cc713f/tests/SessionRecords/ChatTests/AuthFailure.json
func TestDecodeOpenAI(t *testing.T) {
	providertest.RunErrorCases(t, "openai-completions", errorformat.DecodeOpenAI, []providertest.ErrorCase{
		{
			Name:   "invalid api key",
			Status: http.StatusUnauthorized,
			Header: reqID("x-request-id", "req_401"),
			Body:   `{"error":{"message":"Incorrect API key provided: not-a-re**************************ized. You can find your API key at https://platform.openai.com/account/api-keys.","type":"invalid_request_error","param":null,"code":"invalid_api_key"}}`,
			Want: sdk.APIError{
				Type: "invalid_request_error", Code: "invalid_api_key", RequestID: "req_401", Kind: sdk.KindAuthentication,
				Message: "Incorrect API key provided: not-a-re**************************ized. You can find your API key at https://platform.openai.com/account/api-keys.",
			},
		},
		{
			// The code wins over the 429 status.
			Name:   "insufficient quota",
			Status: http.StatusTooManyRequests,
			Body:   `{"error":{"message":"You exceeded your current quota, please check your plan and billing details.","type":"insufficient_quota","param":null,"code":"insufficient_quota"}}`,
			Want: sdk.APIError{
				Type: "insufficient_quota", Code: "insufficient_quota", Kind: sdk.KindQuotaExhausted,
				Message: "You exceeded your current quota, please check your plan and billing details.",
			},
		},
		{
			Name:   "rate limit",
			Status: http.StatusTooManyRequests,
			Body:   `{"error":{"message":"Rate limit reached for requests","type":"requests","param":null,"code":"rate_limit_exceeded"}}`,
			Want:   sdk.APIError{Type: "requests", Code: "rate_limit_exceeded", Message: "Rate limit reached for requests", Kind: sdk.KindRateLimited},
		},
		{
			Name:   "server error",
			Status: http.StatusInternalServerError,
			Body:   `{"error":{"message":"The server had an error while processing your request.","type":"server_error","param":null,"code":null}}`,
			Want:   sdk.APIError{Type: "server_error", Message: "The server had an error while processing your request.", Kind: sdk.KindServerError},
		},
		{
			// The type classifies when the code has no mapping.
			Name:   "insufficient quota type",
			Status: http.StatusBadRequest,
			Body:   `{"error":{"message":"quota","type":"insufficient_quota","param":null,"code":"made_up"}}`,
			Want:   sdk.APIError{Type: "insufficient_quota", Code: "made_up", Message: "quota", Kind: sdk.KindQuotaExhausted},
		},
		{
			Name:   "server error type",
			Status: http.StatusBadRequest,
			Body:   `{"error":{"message":"boom","type":"server_error","param":null,"code":"made_up"}}`,
			Want:   sdk.APIError{Type: "server_error", Code: "made_up", Message: "boom", Kind: sdk.KindServerError},
		},
		{
			Name:   "unsupported region falls back to status",
			Status: http.StatusForbidden,
			Body:   `{"error":{"message":"Country, region, or territory not supported","type":"request_forbidden","param":null,"code":"unsupported_country_region_territory"}}`,
			Want: sdk.APIError{
				Type: "request_forbidden", Code: "unsupported_country_region_territory",
				Message: "Country, region, or territory not supported", Kind: sdk.KindPermissionDenied,
			},
		},
		{
			Name:   "unknown code and status",
			Status: http.StatusBadRequest,
			Body:   `{"error":{"message":"bad","type":"invalid_request_error","param":"model","code":"model_not_found"}}`,
			Want:   sdk.APIError{Type: "invalid_request_error", Code: "model_not_found", Message: "bad", Kind: sdk.KindUnknown},
		},
		{
			// OpenAI-compatible APIs (OpenRouter) send a numeric code.
			Name:   "numeric code",
			Status: http.StatusPaymentRequired,
			Body:   `{"error":{"code":402,"message":"Insufficient credits"}}`,
			Want:   sdk.APIError{Code: "402", Message: "Insufficient credits", Kind: sdk.KindQuotaExhausted},
		},
		{
			Name:   "bedrock request id",
			Status: http.StatusServiceUnavailable,
			Header: reqID("x-amzn-requestid", "0b6c3e1a-bedrock"),
			Body:   `not json`,
			Want:   sdk.APIError{RequestID: "0b6c3e1a-bedrock", Kind: sdk.KindServerError},
		},
	})
}

// A request made with a ChatGPT account's token (Sign in with ChatGPT) that
// fails admission gets a detail body instead of an error object. The first
// three bodies are verbatim from https://api.openai.com/v1/responses, captured
// on 2026-10-10 for a request with temperature, one with stream false and one
// for gpt-4o.
func TestDecodeOpenAIDetail(t *testing.T) {
	providertest.RunErrorCases(t, "openai-responses", errorformat.DecodeOpenAI, []providertest.ErrorCase{
		{
			Name:   "unsupported parameter",
			Status: http.StatusBadRequest,
			Body:   `{"detail":"Unsupported parameter: temperature"}`,
			Want:   sdk.APIError{Message: "Unsupported parameter: temperature", Kind: sdk.KindUnknown},
		},
		{
			Name:   "stream required",
			Status: http.StatusBadRequest,
			Body:   `{"detail":"Stream must be set to true"}`,
			Want:   sdk.APIError{Message: "Stream must be set to true", Kind: sdk.KindUnknown},
		},
		{
			Name:   "model not supported",
			Status: http.StatusBadRequest,
			Body:   `{"detail":"The 'gpt-4o' model is not supported when using Codex with a ChatGPT account."}`,
			Want:   sdk.APIError{Message: "The 'gpt-4o' model is not supported when using Codex with a ChatGPT account.", Kind: sdk.KindUnknown},
		},
		{
			// A detail that is not a string is kept as its compact JSON text.
			Name:   "structured detail",
			Status: http.StatusUnprocessableEntity,
			Body:   `{"detail": [{"loc": ["body", "model"], "msg": "Field required"}]}`,
			Want:   sdk.APIError{Message: `[{"loc":["body","model"],"msg":"Field required"}]`, Kind: sdk.KindUnknown},
		},
		{
			Name:   "null detail",
			Status: http.StatusBadRequest,
			Body:   `{"detail":null}`,
			Want:   sdk.APIError{Kind: sdk.KindUnknown},
		},
		{
			// The error object's message wins over detail.
			Name:   "error object and detail",
			Status: http.StatusBadRequest,
			Body:   `{"error":{"message":"bad","type":"invalid_request_error","code":"made_up"},"detail":"ignored"}`,
			Want:   sdk.APIError{Type: "invalid_request_error", Code: "made_up", Message: "bad", Kind: sdk.KindUnknown},
		},
	})
}

// The "error" event's top-level fields follow openai-go's ResponseErrorEvent:
// https://github.com/openai/openai-go/blob/d7fd0c65cc247957d5b247ad42283fc8e4061868/responses/response.go
// The nested shape is the one the Codex CLI reads:
// https://github.com/openai/codex/blob/1b1835f751ebdc0cfc50b3fe55d4571dbb294563/codex-rs/codex-api/src/sse/responses.rs
func TestDecodeOpenAIErrorEvent(t *testing.T) {
	providertest.RunErrorCases(t, "openai-responses", errorformat.DecodeOpenAIErrorEvent, []providertest.ErrorCase{
		{
			Name:   "top-level code and message",
			Header: reqID("x-request-id", "req_ev"),
			Body:   `{"type":"error","code":"server_error","message":"The server had an error while processing your request.","param":null,"sequence_number":7}`,
			Want: sdk.APIError{
				Code: "server_error", Message: "The server had an error while processing your request.",
				RequestID: "req_ev", Kind: sdk.KindServerError,
			},
		},
		{
			Name: "nested error object",
			Body: `{"type":"error","sequence_number":2,"error":{"type":"invalid_request_error","code":"rate_limit_exceeded","message":"Rate limit reached"}}`,
			Want: sdk.APIError{Type: "invalid_request_error", Code: "rate_limit_exceeded", Message: "Rate limit reached", Kind: sdk.KindRateLimited},
		},
		{
			Name: "unknown code",
			Body: `{"type":"error","code":"made_up","message":"boom"}`,
			Want: sdk.APIError{Code: "made_up", Message: "boom", Kind: sdk.KindUnknown},
		},
		{
			Name: "not json",
			Body: `not json`,
			Want: sdk.APIError{Kind: sdk.KindUnknown},
		},
	})
}

// The body is verbatim from the Codex CLI's response.failed fixture:
// https://github.com/openai/codex/blob/1b1835f751ebdc0cfc50b3fe55d4571dbb294563/codex-rs/codex-api/src/sse/responses.rs
func TestDecodeOpenAIFailedEvent(t *testing.T) {
	const message = "Rate limit reached for gpt-5.1 in organization org-AAA on tokens per min (TPM): Limit 30000, Used 22999, Requested 12528. Please try again in 11.054s. Visit https://platform.openai.com/account/rate-limits to learn more."
	providertest.RunErrorCases(t, "openai-responses", errorformat.DecodeOpenAIFailedEvent, []providertest.ErrorCase{
		{
			Name: "rate limit",
			Body: `{"type":"response.failed","sequence_number":3,"response":{"id":"resp_689bcf18d7f08194bf3440ba62fe05d803fee0cdac429894","object":"response","created_at":1755041560,"status":"failed","background":false,"error":{"code":"rate_limit_exceeded","message":"` + message + `"}, "usage":null,"user":null,"metadata":{}}}`,
			Want: sdk.APIError{Code: "rate_limit_exceeded", Message: message, Kind: sdk.KindRateLimited},
		},
		{
			Name: "no error object",
			Body: `{"type":"response.failed","response":{"status":"failed"}}`,
			Want: sdk.APIError{Kind: sdk.KindUnknown},
		},
		{
			Name: "not json",
			Body: `not json`,
			Want: sdk.APIError{Kind: sdk.KindUnknown},
		},
	})
}

// Canonical statuses: https://cloud.google.com/apis/design/errors. ErrorInfo
// reasons: https://github.com/googleapis/googleapis/blob/563e22e733b315aca418482f644c878997f328ea/google/api/error_reason.proto
// The "api key invalid", "api key missing" and "not found" bodies are verbatim
// from generativelanguage.googleapis.com on 2026-09-29. The RESOURCE_EXHAUSTED body
// is the design guide's example, verbatim. The daily quota bodies are the
// Gemini API responses quoted in
// https://github.com/google-gemini/gemini-cli/issues/9248 (taken out of the
// CLI's wrapping error) and
// https://github.com/google-gemini/gemini-cli/issues/8437 (converted from a
// Python dict to JSON); the per-minute body is the latter without its daily
// violation. The others put the proto's ErrorInfo examples inside the same
// envelope.
func TestDecodeGoogle(t *testing.T) {
	providertest.RunErrorCases(t, "google-generative-ai", errorformat.DecodeGoogle, []providertest.ErrorCase{
		{
			// The reason wins over the 400 status.
			Name:   "api key invalid",
			Status: http.StatusBadRequest,
			Body: `{
  "error": {
    "code": 400,
    "message": "API key not valid. Please pass a valid API key.",
    "status": "INVALID_ARGUMENT",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.ErrorInfo",
        "reason": "API_KEY_INVALID",
        "domain": "googleapis.com",
        "metadata": {
          "service": "generativelanguage.googleapis.com"
        }
      },
      {
        "@type": "type.googleapis.com/google.rpc.LocalizedMessage",
        "locale": "en-US",
        "message": "API key not valid. Please pass a valid API key."
      }
    ]
  }
}`,
			Want: sdk.APIError{
				Type: "INVALID_ARGUMENT", Code: "API_KEY_INVALID",
				Message: "API key not valid. Please pass a valid API key.", Kind: sdk.KindAuthentication,
			},
		},
		{
			Name:   "billing disabled",
			Status: http.StatusForbidden,
			Body:   `{"error":{"code":403,"message":"Billing disabled","status":"PERMISSION_DENIED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"BILLING_DISABLED","domain":"googleapis.com","metadata":{"consumer":"projects/123","service":"pubsub.googleapis.com"}}]}}`,
			Want:   sdk.APIError{Type: "PERMISSION_DENIED", Code: "BILLING_DISABLED", Message: "Billing disabled", Kind: sdk.KindQuotaExhausted},
		},
		{
			Name:   "permission denied",
			Status: http.StatusForbidden,
			Body:   `{"error":{"code":403,"message":"denied","status":"PERMISSION_DENIED"}}`,
			Want:   sdk.APIError{Type: "PERMISSION_DENIED", Message: "denied", Kind: sdk.KindPermissionDenied},
		},
		{
			Name:   "resource exhausted",
			Status: http.StatusTooManyRequests,
			Body:   `{"error":{"code":429,"message":"The zone 'us-east1-a' does not have enough resources available to fulfill the request. Try a different zone, or try again later.","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"RESOURCE_AVAILABILITY","domain":"compute.googleapis.com"}]}}`,
			Want: sdk.APIError{
				Type: "RESOURCE_EXHAUSTED", Code: "RESOURCE_AVAILABILITY", Kind: sdk.KindRateLimited,
				Message: "The zone 'us-east1-a' does not have enough resources available to fulfill the request. Try a different zone, or try again later.",
			},
		},
		{
			// RESOURCE_EXHAUSTED alone is rate_limited; the daily quota in
			// the QuotaFailure makes it quota_exhausted.
			Name:   "daily quota",
			Status: http.StatusTooManyRequests,
			Body: `{
  "error": {
    "code": 429,
    "message": "You exceeded your current quota, please check your plan and billing details. For more information on this error, head to: https://ai.google.dev/gemini-api/docs/rate-limits.\n* Quota exceeded for metric: generativelanguage.googleapis.com/generate_content_free_tier_requests, limit: 50\nPlease retry in 34.074824224s.",
    "status": "RESOURCE_EXHAUSTED",
    "details": [
      {
        "@type": "type.googleapis.com/google.rpc.QuotaFailure",
        "violations": [
          {
            "quotaMetric": "generativelanguage.googleapis.com/generate_content_free_tier_requests",
            "quotaId": "GenerateRequestsPerDayPerProjectPerModel-FreeTier",
            "quotaDimensions": {
              "location": "global",
              "model": "gemini-2.5-pro"
            },
            "quotaValue": "50"
          }
        ]
      },
      {
        "@type": "type.googleapis.com/google.rpc.Help",
        "links": [
          {
            "description": "Learn more about Gemini API quotas",
            "url": "https://ai.google.dev/gemini-api/docs/rate-limits"
          }
        ]
      },
      {
        "@type": "type.googleapis.com/google.rpc.RetryInfo",
        "retryDelay": "34s"
      }
    ]
  }
}`,
			Want: sdk.APIError{
				Type: "RESOURCE_EXHAUSTED", Kind: sdk.KindQuotaExhausted,
				Message: "You exceeded your current quota, please check your plan and billing details. For more information on this error, head to: https://ai.google.dev/gemini-api/docs/rate-limits.\n* Quota exceeded for metric: generativelanguage.googleapis.com/generate_content_free_tier_requests, limit: 50\nPlease retry in 34.074824224s.",
			},
		},
		{
			// One daily violation among per-minute ones is enough.
			Name:   "daily and per-minute quotas",
			Status: http.StatusTooManyRequests,
			Body:   `{"error":{"code":429,"message":"You exceeded your current quota, please check your plan and billing details. For more information on this error, head to: https://ai.google.dev/gemini-api/docs/rate-limits.","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[{"quotaMetric":"generativelanguage.googleapis.com/generate_content_free_tier_input_token_count","quotaId":"GenerateContentInputTokensPerModelPerMinute-FreeTier","quotaDimensions":{"location":"global","model":"gemini-2.5-flash-preview-image"}},{"quotaMetric":"generativelanguage.googleapis.com/generate_content_free_tier_requests","quotaId":"GenerateRequestsPerMinutePerProjectPerModel-FreeTier","quotaDimensions":{"location":"global","model":"gemini-2.5-flash-preview-image"}},{"quotaMetric":"generativelanguage.googleapis.com/generate_content_free_tier_requests","quotaId":"GenerateRequestsPerDayPerProjectPerModel-FreeTier","quotaDimensions":{"model":"gemini-2.5-flash-preview-image","location":"global"}}]},{"@type":"type.googleapis.com/google.rpc.Help","links":[{"description":"Learn more about Gemini API quotas","url":"https://ai.google.dev/gemini-api/docs/rate-limits"}]},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"55s"}]}}`,
			Want:   sdk.APIError{Type: "RESOURCE_EXHAUSTED", Message: "You exceeded your current quota, please check your plan and billing details. For more information on this error, head to: https://ai.google.dev/gemini-api/docs/rate-limits.", Kind: sdk.KindQuotaExhausted},
		},
		{
			Name:   "per-minute quotas",
			Status: http.StatusTooManyRequests,
			Body:   `{"error":{"code":429,"message":"You exceeded your current quota, please check your plan and billing details. For more information on this error, head to: https://ai.google.dev/gemini-api/docs/rate-limits.","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.QuotaFailure","violations":[{"quotaMetric":"generativelanguage.googleapis.com/generate_content_free_tier_input_token_count","quotaId":"GenerateContentInputTokensPerModelPerMinute-FreeTier","quotaDimensions":{"location":"global","model":"gemini-2.5-flash-preview-image"}},{"quotaMetric":"generativelanguage.googleapis.com/generate_content_free_tier_requests","quotaId":"GenerateRequestsPerMinutePerProjectPerModel-FreeTier","quotaDimensions":{"location":"global","model":"gemini-2.5-flash-preview-image"}}]},{"@type":"type.googleapis.com/google.rpc.Help","links":[{"description":"Learn more about Gemini API quotas","url":"https://ai.google.dev/gemini-api/docs/rate-limits"}]},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"55s"}]}}`,
			Want:   sdk.APIError{Type: "RESOURCE_EXHAUSTED", Message: "You exceeded your current quota, please check your plan and billing details. For more information on this error, head to: https://ai.google.dev/gemini-api/docs/rate-limits.", Kind: sdk.KindRateLimited},
		},
		{
			Name:   "unavailable",
			Status: http.StatusServiceUnavailable,
			Body:   `{"error":{"code":503,"message":"The model is overloaded. Please try again later.","status":"UNAVAILABLE"}}`,
			Want:   sdk.APIError{Type: "UNAVAILABLE", Message: "The model is overloaded. Please try again later.", Kind: sdk.KindServerError},
		},
		{
			Name:   "not found",
			Status: http.StatusNotFound,
			Body: `{
  "error": {
    "code": 404,
    "message": "models/gemini-does-not-exist is not found for API version v1beta, or is not supported for generateContent. Call ModelService.ListModels to see the list of available models and their supported methods.",
    "status": "NOT_FOUND"
  }
}`,
			Want: sdk.APIError{Type: "NOT_FOUND", Kind: sdk.KindUnknown, Message: "models/gemini-does-not-exist is not found for API version v1beta, or is not supported for generateContent. Call ModelService.ListModels to see the list of available models and their supported methods."},
		},
		{
			// A request without a key carries no ErrorInfo, so the canonical
			// status decides.
			Name:   "api key missing",
			Status: http.StatusForbidden,
			Body: `{
  "error": {
    "code": 403,
    "message": "Method doesn't allow unregistered callers (callers without established identity). Please use API Key or other form of API consumer identity to call this API.",
    "status": "PERMISSION_DENIED"
  }
}`,
			Want: sdk.APIError{Type: "PERMISSION_DENIED", Kind: sdk.KindPermissionDenied, Message: "Method doesn't allow unregistered callers (callers without established identity). Please use API Key or other form of API consumer identity to call this API."},
		},
		{
			// The reason wins over the 400 status.
			Name:   "api key service blocked",
			Status: http.StatusBadRequest,
			Body:   `{"error":{"code":400,"message":"Requests to this API are blocked.","status":"PERMISSION_DENIED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"API_KEY_SERVICE_BLOCKED","domain":"googleapis.com","metadata":{"service":"storage.googleapis.com"}}]}}`,
			Want:   sdk.APIError{Type: "PERMISSION_DENIED", Code: "API_KEY_SERVICE_BLOCKED", Message: "Requests to this API are blocked.", Kind: sdk.KindPermissionDenied},
		},
		{
			// The reason wins over the 403 status.
			Name:   "rate limit exceeded",
			Status: http.StatusForbidden,
			Body:   `{"error":{"code":403,"message":"Quota exceeded.","status":"RESOURCE_EXHAUSTED","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"RATE_LIMIT_EXCEEDED","domain":"googleapis.com","metadata":{"service":"pubsub.googleapis.com"}}]}}`,
			Want:   sdk.APIError{Type: "RESOURCE_EXHAUSTED", Code: "RATE_LIMIT_EXCEEDED", Message: "Quota exceeded.", Kind: sdk.KindRateLimited},
		},
		{
			// The canonical status wins over the 400 status.
			Name:   "unauthenticated",
			Status: http.StatusBadRequest,
			Body:   `{"error":{"code":401,"message":"Request had invalid authentication credentials.","status":"UNAUTHENTICATED"}}`,
			Want:   sdk.APIError{Type: "UNAUTHENTICATED", Message: "Request had invalid authentication credentials.", Kind: sdk.KindAuthentication},
		},
		{
			Name:   "not json keeps the status kind",
			Status: http.StatusBadGateway,
			Body:   `<html>Bad Gateway</html>`,
			Want:   sdk.APIError{Kind: sdk.KindServerError},
		},
	})
}

// Error body and error_type values: https://openrouter.ai/docs/api-reference/errors
func TestDecodeOpenRouter(t *testing.T) {
	cases := []providertest.ErrorCase{
		{Name: "typed rate limit", Status: 429,
			Body: `{
    "error": {
      "code": 429,
      "message": "Rate limit exceeded",
      "metadata": {
        "error_type": "rate_limit_exceeded",
        "provider_code": "rate_limited"
      }
    }
  }`,
			Want: sdk.APIError{Type: "rate_limit_exceeded", Code: "429", Message: "Rate limit exceeded", Kind: sdk.KindRateLimited}},
		{Name: "credits", Status: 402,
			Body: `{"error":{"code":402,"message":"Insufficient credits","metadata":{"error_type":"payment_required"}}}`,
			Want: sdk.APIError{Type: "payment_required", Code: "402", Message: "Insufficient credits", Kind: sdk.KindQuotaExhausted}},
		{Name: "untyped falls back to status", Status: 401,
			Body: `{"error":{"code":401,"message":"No auth credentials found"}}`,
			Want: sdk.APIError{Code: "401", Message: "No auth credentials found", Kind: sdk.KindAuthentication}},
		{Name: "provider overloaded", Status: 503,
			Body: `{"error":{"code":503,"message":"busy","metadata":{"error_type":"provider_overloaded"}}}`,
			Want: sdk.APIError{Type: "provider_overloaded", Code: "503", Message: "busy", Kind: sdk.KindServerError}},
		{Name: "typed authentication wins over 400", Status: 400,
			Body: `{"error":{"code":400,"message":"Invalid API key","metadata":{"error_type":"authentication"}}}`,
			Want: sdk.APIError{Type: "authentication", Code: "400", Message: "Invalid API key", Kind: sdk.KindAuthentication}},
		{Name: "typed permission denied wins over 400", Status: 400,
			Body: `{"error":{"code":400,"message":"denied","metadata":{"error_type":"permission_denied"}}}`,
			Want: sdk.APIError{Type: "permission_denied", Code: "400", Message: "denied", Kind: sdk.KindPermissionDenied}},
		{Name: "typed payment required wins over 400", Status: 400,
			Body: `{"error":{"code":400,"message":"Insufficient credits","metadata":{"error_type":"payment_required"}}}`,
			Want: sdk.APIError{Type: "payment_required", Code: "400", Message: "Insufficient credits", Kind: sdk.KindQuotaExhausted}},
		{Name: "generation ID header", Status: 400,
			Header: http.Header{"X-Generation-Id": {"gen-1790715538-w19zoag2Gx6Tz8MumgPl"}},
			Body:   `{"error":{"message":"openai/gpt-audio-nope is not a valid model ID","code":400},"user_id":"user_38F4QMS0L6p8ecSSfk02ZWB1BW0"}`,
			Want:   sdk.APIError{Code: "400", Message: "openai/gpt-audio-nope is not a valid model ID", RequestID: "gen-1790715538-w19zoag2Gx6Tz8MumgPl", Kind: sdk.KindUnknown}},
		{Name: "unmapped timeout falls back to status", Status: 504,
			Body: `{"error":{"code":504,"message":"Provider did not respond before the upstream deadline","metadata":{"error_type":"timeout"}}}`,
			Want: sdk.APIError{Type: "timeout", Code: "504", Message: "Provider did not respond before the upstream deadline", Kind: sdk.KindServerError}},
	}
	providertest.RunErrorCases(t, "openrouter-videos", errorformat.DecodeOpenRouter, cases)
}

// Error body: https://help.aliyun.com/zh/model-studio/text-to-image-v2-api-reference
// (异常响应). Codes and statuses: https://help.aliyun.com/zh/model-studio/error-code
func TestDecodeDashScope(t *testing.T) {
	cases := []providertest.ErrorCase{
		{Name: "no api key", Status: 401,
			Body: `{
 "code": "InvalidApiKey",
 "message": "No API-key provided.",
 "request_id": "7438d53d-6eb8-4596-8835-xxxxxx"
}`,
			Want: sdk.APIError{Code: "InvalidApiKey", Message: "No API-key provided.", RequestID: "7438d53d-6eb8-4596-8835-xxxxxx", Kind: sdk.KindAuthentication}},
		{Name: "arrearage is 400", Status: 400,
			Body: `{"request_id":"r1","code":"Arrearage","message":"Access denied, please make sure your account is in good standing."}`,
			Want: sdk.APIError{Code: "Arrearage", Message: "Access denied, please make sure your account is in good standing.", RequestID: "r1", Kind: sdk.KindQuotaExhausted}},
		{Name: "free tier only is 403", Status: 403,
			Body: `{"request_id":"r2","code":"AllocationQuota.FreeTierOnly","message":"The free tier of the model has been exhausted."}`,
			Want: sdk.APIError{Code: "AllocationQuota.FreeTierOnly", Message: "The free tier of the model has been exhausted.", RequestID: "r2", Kind: sdk.KindQuotaExhausted}},
		{Name: "rate quota", Status: 429,
			Body: `{"request_id":"r3","code":"Throttling.RateQuota","message":"Requests rate limit exceeded, please try again later."}`,
			Want: sdk.APIError{Code: "Throttling.RateQuota", Message: "Requests rate limit exceeded, please try again later.", RequestID: "r3", Kind: sdk.KindRateLimited}},
		{Name: "invalid parameter", Status: 400,
			Body: `{"request_id":"a4d78a5f-655f-9639-8437-xxxxxx","code":"InvalidParameter","message":"n must be 1"}`,
			Want: sdk.APIError{Code: "InvalidParameter", Message: "n must be 1", RequestID: "a4d78a5f-655f-9639-8437-xxxxxx", Kind: sdk.KindUnknown}},
		{Name: "unknown code falls back to status", Status: 500,
			Body: `{"request_id":"r4","code":"InternalError","message":"boom"}`,
			Want: sdk.APIError{Code: "InternalError", Message: "boom", RequestID: "r4", Kind: sdk.KindServerError}},
		{Name: "failed task in a 2xx body",
			Body: `{"request_id":"r5","output":{"task_id":"t1","task_status":"FAILED","code":"DataInspectionFailed","message":"Input data may contain inappropriate content."}}`,
			Want: sdk.APIError{Code: "DataInspectionFailed", Message: "Input data may contain inappropriate content.", RequestID: "r5", Kind: sdk.KindUnknown}},
		{Name: "business code in a 2xx body",
			Body: `{"request_id":"r6","code":"Arrearage","message":"Access denied, please make sure your account is in good standing."}`,
			Want: sdk.APIError{Code: "Arrearage", Message: "Access denied, please make sure your account is in good standing.", RequestID: "r6", Kind: sdk.KindQuotaExhausted}},
		{Name: "non-JSON body keeps the status kind", Status: 503,
			Body: "upstream unavailable",
			Want: sdk.APIError{Kind: sdk.KindServerError}},
	}
	providertest.RunErrorCases(t, "alibabacloud-images", errorformat.DecodeDashScope, cases)
}

// Deepgram error bodies are verbatim from
// https://developers.deepgram.com/docs/errors, with the documented "uuid"
// placeholder replaced by a sample ID.
const deepgramInvalidAuthBody = `{"err_code":"INVALID_AUTH","err_msg":"Invalid credentials.","request_id":"9a1c5e7f-0000-4000-8000-000000000401"}`

func TestDecodeDeepgram(t *testing.T) {
	providertest.RunErrorCases(t, "deepgram-speech", errorformat.DecodeDeepgram, []providertest.ErrorCase{
		{
			Name:   "invalid auth",
			Status: http.StatusUnauthorized,
			Body:   deepgramInvalidAuthBody,
			Want: sdk.APIError{
				Code: "INVALID_AUTH", Message: "Invalid credentials.", RequestID: "9a1c5e7f-0000-4000-8000-000000000401", Kind: sdk.KindAuthentication,
			},
		},
		{
			// Deepgram sends insufficient permissions as a 401 too.
			Name:   "insufficient permissions as 401",
			Status: http.StatusUnauthorized,
			Body:   `{"err_code":"INSUFFICIENT_PERMISSIONS","err_msg":"User does not have sufficient permissions.","request_id":"r401p"}`,
			Want: sdk.APIError{
				Code: "INSUFFICIENT_PERMISSIONS", Message: "User does not have sufficient permissions.", RequestID: "r401p", Kind: sdk.KindPermissionDenied,
			},
		},
		{
			Name:   "insufficient credits",
			Status: http.StatusPaymentRequired,
			Body:   `{"err_code":"ASR_PAYMENT_REQUIRED","err_msg":"Project does not have enough credits for an ASR request and does not have an overage agreement.","request_id":"r402"}`,
			Want: sdk.APIError{
				Code: "ASR_PAYMENT_REQUIRED", Message: "Project does not have enough credits for an ASR request and does not have an overage agreement.", RequestID: "r402", Kind: sdk.KindQuotaExhausted,
			},
		},
		{
			Name:   "text to speech rate limit",
			Status: http.StatusTooManyRequests,
			Body:   `{"err_code":"Too Many Requests","err_msg":"Please try again later.","request_id":"r429"}`,
			Want: sdk.APIError{
				Code: "Too Many Requests", Message: "Please try again later.", RequestID: "r429", Kind: sdk.KindRateLimited,
			},
		},
		{
			Name:   "service unavailable spells the code error_code",
			Status: http.StatusServiceUnavailable,
			Body:   `{"error_code":"Service Unavailable","err_msg":"Please try again later","request_id":"r503"}`,
			Want: sdk.APIError{
				Code: "Service Unavailable", Message: "Please try again later", RequestID: "r503", Kind: sdk.KindServerError,
			},
		},
		{
			Name:   "invalid JSON",
			Status: http.StatusBadRequest,
			Body:   "{\"category\":\"INVALID_JSON\",\"message\":\"Invalid JSON submitted.\",\"details\":\"Json deserialize error: missing field `xxx` at line 7 column 1\",\"request_id\":\"r400\"}",
			Want: sdk.APIError{
				Type: "INVALID_JSON", Message: "Invalid JSON submitted.", RequestID: "r400", Kind: sdk.KindUnknown,
			},
		},
		{
			Name:   "dg-request-id header without a JSON body",
			Status: http.StatusBadGateway,
			Header: http.Header{"Dg-Request-Id": {"hdr_502"}},
			Body:   "bad gateway",
			Want:   sdk.APIError{RequestID: "hdr_502", Kind: sdk.KindServerError},
		},
	})
}

// ElevenLabs error bodies follow
// https://elevenlabs.io/docs/eleven-api/resources/errors. The validation body
// is the page's example verbatim; the others fill the documented fields with
// rows of its error-code table.
const (
	elevenLabsValidationBody = `{"detail":{"type":"validation_error","code":"invalid_parameters","message":"The 'keyterms' parameter is only supported with the 'scribe_v2' model. You specified 'scribe_v1'.","status":"invalid_parameters","request_id":"3c807fc4c3a1705f9638ecc764a91c01","param":"keyterms"}}`
	elevenLabsCreditsBody    = `{"detail":{"type":"payment_required","code":"insufficient_credits","message":"Your account does not have enough credits for this operation.","status":"insufficient_credits","request_id":"req_402"}}`
)

func TestDecodeElevenLabs(t *testing.T) {
	providertest.RunErrorCases(t, "elevenlabs-speech", errorformat.DecodeElevenLabs, []providertest.ErrorCase{
		{
			Name:   "validation error",
			Status: http.StatusBadRequest,
			Body:   elevenLabsValidationBody,
			Want: sdk.APIError{
				Type: "validation_error", Code: "invalid_parameters", RequestID: "3c807fc4c3a1705f9638ecc764a91c01", Kind: sdk.KindUnknown,
				Message: "The 'keyterms' parameter is only supported with the 'scribe_v2' model. You specified 'scribe_v1'.",
			},
		},
		{
			Name:   "invalid api key",
			Status: http.StatusUnauthorized,
			Body:   `{"detail":{"type":"authentication_error","code":"invalid_api_key","message":"The provided API key is invalid.","status":"invalid_api_key","request_id":"req_401"}}`,
			Want: sdk.APIError{
				Type: "authentication_error", Code: "invalid_api_key", Message: "The provided API key is invalid.", RequestID: "req_401", Kind: sdk.KindAuthentication,
			},
		},
		{
			Name:   "insufficient credits",
			Status: http.StatusPaymentRequired,
			Body:   elevenLabsCreditsBody,
			Want: sdk.APIError{
				Type: "payment_required", Code: "insufficient_credits", Message: "Your account does not have enough credits for this operation.", RequestID: "req_402", Kind: sdk.KindQuotaExhausted,
			},
		},
		{
			// The feature gate is an authorization_error, not a quota.
			Name:   "subscription required",
			Status: http.StatusForbidden,
			Body:   `{"detail":{"type":"authorization_error","code":"subscription_required","message":"A paid subscription is required to access this feature.","status":"subscription_required","request_id":"req_403"}}`,
			Want: sdk.APIError{
				Type: "authorization_error", Code: "subscription_required", Message: "A paid subscription is required to access this feature.", RequestID: "req_403", Kind: sdk.KindPermissionDenied,
			},
		},
		{
			Name:   "concurrency limit",
			Status: http.StatusTooManyRequests,
			Body:   `{"detail":{"type":"rate_limit_error","code":"concurrent_limit_exceeded","message":"Maximum number of concurrent requests exceeded.","status":"concurrent_limit_exceeded","request_id":"req_429"}}`,
			Want: sdk.APIError{
				Type: "rate_limit_error", Code: "concurrent_limit_exceeded", Message: "Maximum number of concurrent requests exceeded.", RequestID: "req_429", Kind: sdk.KindRateLimited,
			},
		},
		{
			// Live response, 2026-09-29: only the legacy fields, ID in x-trace-id.
			Name:   "legacy body",
			Status: http.StatusBadRequest,
			Header: http.Header{"X-Trace-Id": {"1f25c79bd623c2cb841446a20f14af00"}},
			Body:   `{"detail":{"status":"model_not_found","message":"A model with model ID eleven_nope does not exist ..."}}`,
			Want: sdk.APIError{
				Code: "model_not_found", Message: "A model with model ID eleven_nope does not exist ...", RequestID: "1f25c79bd623c2cb841446a20f14af00", Kind: sdk.KindUnknown,
			},
		},
		{
			Name:   "x-trace-id header without a JSON body",
			Status: http.StatusServiceUnavailable,
			Header: http.Header{"X-Trace-Id": {"hdr_503"}},
			Body:   "upstream unavailable",
			Want:   sdk.APIError{RequestID: "hdr_503", Kind: sdk.KindServerError},
		},
	})
}

func TestKindWithoutStatus(t *testing.T) {
	// Stream events carry no status; the type or code alone must classify.
	if got := errorformat.OpenAIKind("", "insufficient_quota"); got != sdk.KindQuotaExhausted {
		t.Errorf("OpenAIKind = %q", got)
	}
	if got := errorformat.OpenAIKind("", "made_up"); got != sdk.KindUnknown {
		t.Errorf("OpenAIKind unknown = %q", got)
	}
	if got := errorformat.GoogleKind("UNAVAILABLE", ""); got != sdk.KindServerError {
		t.Errorf("GoogleKind = %q", got)
	}
}
