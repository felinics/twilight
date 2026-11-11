package errorformat_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/felinics/twilight/internal/errorformat"
	"github.com/felinics/twilight/provider/providertest"
	"github.com/felinics/twilight/sdk"
)

// Bodies OpenRouter returned on 2026-10-15, verbatim except that user_id is
// removed.
const (
	// Chat Completions, deepseek/deepseek-v4.1-flash routed to DeepInfra.
	openRouterDeepInfra422 = `{"error":{"message":"Provider returned error","code":422,"metadata":{"raw":"{\"error\":{\"message\":\"Input should be 'text'\",\"type\":\"invalid_request_error\",\"param\":\"response_format.TextResponseFormat.type\",\"code\":null}}","provider_name":"DeepInfra","is_byok":false}}}`
	// Chat Completions, qwen/qwen3.8-flash routed to Alibaba.
	openRouterAlibaba400 = `{"error":{"message":"Provider returned error","code":400,"metadata":{"raw":"{\"error\":{\"message\":\"<400> InternalError.Algo.InvalidParameter: Format error : 'response_format.json_schema.schema'. the specific reason is as follows: None is not of type 'object', 'boolean'.\",\"type\":\"invalid_request_error\",\"param\":null,\"code\":\"invalid_parameter_error\"},\"id\":\"chatcmpl-acf98bc7-3df3-4a04-97c7-3fbc8d58bda9\",\"request_id\":\"acf98bc7-3df3-4a04-97c7-3fbc8d58bda9\"}","provider_name":"Alibaba","is_byok":false,"provider_error_code":"invalid_parameter_error"}}}`
	// Chat Completions, qwen/qwen3.8-flash routed to Alibaba while it was
	// rate limited; raw is plain text.
	openRouterAlibaba429 = `{"error":{"message":"Provider returned error","code":429,"metadata":{"raw":"qwen/qwen3.8-flash is temporarily rate-limited upstream. Please retry shortly, or add your own key to accumulate your rate limits: https://openrouter.ai/settings/integrations","provider_name":"Alibaba","is_byok":false,"provider_error_code":"Throttling.Concurrency","limit_source":"upstream_provider_shared_pool","remedy_hint":"Retry shortly, add your own provider key (https://openrouter.ai/settings/integrations), or route to another provider with provider routing: https://openrouter.ai/docs/features/provider-routing"}}}`
	// Responses, temperature 50: OpenRouter's own validation, with the
	// metadata at the top level and raw a JSON array.
	openRouterResponsesInvalid = `{"error":{"code":"invalid_prompt","message":"Invalid Responses API request"},"metadata":{"raw":"[\n  {\n    \"origin\": \"number\",\n    \"code\": \"too_big\",\n    \"maximum\": 2,\n    \"inclusive\": true,\n    \"path\": [\n      \"temperature\"\n    ],\n    \"message\": \"Too big: expected number to be <=2\"\n  }\n]"}}`
	// Responses, qwen/qwen3.8-flash routed to Alibaba with an invalid
	// text.format schema.
	openRouterResponsesAlibaba400 = `{"error":{"message":"Provider returned error","code":400,"metadata":{"raw":"{\"error\":{\"message\":\"<400> InternalError.Algo.InvalidParameter: Format error : 'response_format.json_schema.schema'. the specific reason is as follows: 'foo' is not valid under any of the given schemas.\",\"type\":\"invalid_request_error\",\"param\":null,\"code\":\"invalid_parameter_error\"},\"id\":\"chatcmpl-bfc2efe1-f39b-4b2f-8d94-fbda33264b98\",\"request_id\":\"bfc2efe1-f39b-4b2f-8d94-fbda33264b98\"}","provider_name":"Alibaba","is_byok":false,"provider_error_code":"invalid_parameter_error"}}}`
)

const (
	deepInfraMessage    = "Provider returned error (DeepInfra: Input should be 'text')"
	alibaba400Message   = "Provider returned error (Alibaba: <400> InternalError.Algo.InvalidParameter: Format error : 'response_format.json_schema.schema'. the specific reason is as follows: None is not of type 'object', 'boolean'.)"
	alibaba429Message   = "Provider returned error (Alibaba: qwen/qwen3.8-flash is temporarily rate-limited upstream. Please retry shortly, or add your own key to accumulate your rate limits: https://openrouter.ai/settings/integrations)"
	responsesInvalidMsg = "Invalid Responses API request (Too big: expected number to be <=2)"
)

// OpenRouter answers an upstream failure with a generic message and the
// upstream's response in metadata.raw. Cases not marked as captured are
// constructed.
func TestDecodeOpenAIOpenRouterUpstream(t *testing.T) {
	long := strings.Repeat("x", 400)
	providertest.RunErrorCases(t, "openai-completions", errorformat.DecodeOpenAI, []providertest.ErrorCase{
		{Name: "captured deepinfra 422", Status: http.StatusUnprocessableEntity, Body: openRouterDeepInfra422,
			Want: sdk.APIError{Code: "422", Message: deepInfraMessage, Kind: sdk.KindUnknown}},
		{Name: "captured alibaba 400", Status: http.StatusBadRequest, Body: openRouterAlibaba400,
			Want: sdk.APIError{Code: "400", Message: alibaba400Message, Kind: sdk.KindUnknown}},
		{Name: "captured alibaba 429 plain text raw", Status: http.StatusTooManyRequests, Body: openRouterAlibaba429,
			Want: sdk.APIError{Code: "429", Message: alibaba429Message, Kind: sdk.KindRateLimited}},
		{Name: "captured responses top-level metadata array raw", Status: http.StatusBadRequest, Body: openRouterResponsesInvalid,
			Want: sdk.APIError{Code: "invalid_prompt", Message: responsesInvalidMsg, Kind: sdk.KindUnknown}},
		{Name: "captured responses alibaba 400", Status: http.StatusBadRequest, Body: openRouterResponsesAlibaba400,
			Want: sdk.APIError{Code: "400", Kind: sdk.KindUnknown,
				Message: "Provider returned error (Alibaba: <400> InternalError.Algo.InvalidParameter: Format error : 'response_format.json_schema.schema'. the specific reason is as follows: 'foo' is not valid under any of the given schemas.)"}},
		{Name: "array raw items are joined", Status: http.StatusBadRequest,
			Body: `{"error":{"code":"invalid_prompt","message":"Invalid Responses API request"},"metadata":{"raw":"[{\"message\":\"first\"},{\"message\":\"second\"}]"}}`,
			Want: sdk.APIError{Code: "invalid_prompt", Message: "Invalid Responses API request (first; second)", Kind: sdk.KindUnknown}},
		{Name: "sse raw", Status: http.StatusBadGateway,
			Body: `{"error":{"message":"Provider returned error","code":502,"metadata":{"raw":"event: error\ndata: {\"type\":\"error\",\"error\":{\"type\":\"overloaded_error\",\"message\":\"Overloaded\"}}\n\ndata: [DONE]\n","provider_name":"Anthropic"}}}`,
			Want: sdk.APIError{Code: "502", Message: "Provider returned error (Anthropic: Overloaded)", Kind: sdk.KindServerError}},
		{Name: "error string and detail", Status: http.StatusBadRequest,
			Body: `{"error":{"message":"Provider returned error","code":400,"metadata":{"raw":"{\"detail\":[{\"loc\":[\"body\",\"n\"],\"msg\":\"n must be 1\",\"type\":\"value_error\"}]}","provider_name":"Together"}}}`,
			Want: sdk.APIError{Code: "400", Message: "Provider returned error (Together: n must be 1)", Kind: sdk.KindUnknown}},
		{Name: "raw sent as a JSON object", Status: http.StatusBadRequest,
			Body: `{"error":{"message":"Provider returned error","code":400,"metadata":{"raw":{"error":"bad model"},"provider_name":"Fireworks"}}}`,
			Want: sdk.APIError{Code: "400", Message: "Provider returned error (Fireworks: bad model)", Kind: sdk.KindUnknown}},
		{Name: "multi-line raw is collapsed and long raw cut", Status: http.StatusBadRequest,
			Body: `{"error":{"message":"Provider returned error","code":400,"metadata":{"raw":"line one\n\n  line two ` + long + `","provider_name":"DeepInfra"}}}`,
			Want: sdk.APIError{Code: "400", Kind: sdk.KindUnknown,
				Message: "Provider returned error (DeepInfra: line one line two " + strings.Repeat("x", 300-len("line one line two ")-1) + "…)"}},
		{Name: "upstream message already in message", Status: http.StatusBadRequest,
			Body: `{"error":{"message":"Input should be 'text'","code":400,"metadata":{"raw":"{\"error\":{\"message\":\"Input should be 'text'\"}}","provider_name":"DeepInfra"}}}`,
			Want: sdk.APIError{Code: "400", Message: "Input should be 'text' (DeepInfra)", Kind: sdk.KindUnknown}},
		{Name: "no message", Status: http.StatusBadRequest,
			Body: `{"error":{"code":400,"metadata":{"raw":"bad input","provider_name":"DeepInfra"}}}`,
			Want: sdk.APIError{Code: "400", Message: "DeepInfra: bad input", Kind: sdk.KindUnknown}},

		// Each of these leaves the message as OpenRouter sent it.
		{Name: "no metadata", Status: http.StatusBadRequest,
			Body: `{"error":{"message":"Provider returned error","code":400}}`,
			Want: sdk.APIError{Code: "400", Message: "Provider returned error", Kind: sdk.KindUnknown}},
		{Name: "metadata without raw", Status: http.StatusBadRequest,
			Body: `{"error":{"message":"Provider returned error","code":400,"metadata":{"provider_name":"DeepInfra"}}}`,
			Want: sdk.APIError{Code: "400", Message: "Provider returned error", Kind: sdk.KindUnknown}},
		{Name: "truncated JSON raw", Status: http.StatusBadRequest,
			Body: `{"error":{"message":"Provider returned error","code":400,"metadata":{"raw":"{\"error\":{\"mess","provider_name":"DeepInfra"}}}`,
			Want: sdk.APIError{Code: "400", Message: "Provider returned error", Kind: sdk.KindUnknown}},
		{Name: "JSON raw without a message", Status: http.StatusBadRequest,
			Body: `{"error":{"message":"Provider returned error","code":400,"metadata":{"raw":"{\"status\":400,\"ok\":false}","provider_name":"DeepInfra"}}}`,
			Want: sdk.APIError{Code: "400", Message: "Provider returned error", Kind: sdk.KindUnknown}},
		{Name: "html raw", Status: http.StatusBadGateway,
			Body: `{"error":{"message":"Provider returned error","code":502,"metadata":{"raw":"<!DOCTYPE html><html><body>Bad Gateway</body></html>","provider_name":"DeepInfra"}}}`,
			Want: sdk.APIError{Code: "502", Message: "Provider returned error", Kind: sdk.KindServerError}},
		{Name: "blank raw", Status: http.StatusBadRequest,
			Body: `{"error":{"message":"Provider returned error","code":400,"metadata":{"raw":"  ","provider_name":"DeepInfra"}}}`,
			Want: sdk.APIError{Code: "400", Message: "Provider returned error", Kind: sdk.KindUnknown}},
		{Name: "raw neither string nor JSON container", Status: http.StatusBadRequest,
			Body: `{"error":{"message":"Provider returned error","code":400,"metadata":{"raw":42,"provider_name":"DeepInfra"}}}`,
			Want: sdk.APIError{Code: "400", Message: "Provider returned error", Kind: sdk.KindUnknown}},
	})
}

// A failed Response object's top-level metadata is the caller's, not
// OpenRouter's, so it is never read.
func TestDecodeOpenAIResponseObjectMetadata(t *testing.T) {
	providertest.RunErrorCases(t, "openai-responses", errorformat.DecodeOpenAI, []providertest.ErrorCase{{
		Name: "caller metadata named raw",
		Body: `{"id":"resp_1","object":"response","status":"failed","error":{"code":"server_error","message":"The model failed to generate a response."},"metadata":{"raw":"caller value","provider_name":"caller"}}`,
		Want: sdk.APIError{Code: "server_error", Message: "The model failed to generate a response.", Kind: sdk.KindServerError},
	}})
}

func TestDecodeOpenAIErrorEventOpenRouterUpstream(t *testing.T) {
	providertest.RunErrorCases(t, "openai-responses", errorformat.DecodeOpenAIErrorEvent, []providertest.ErrorCase{{
		Name: "nested error with metadata",
		Body: `{"type":"error","sequence_number":3,"error":{"code":502,"message":"Provider returned error","metadata":{"raw":"{\"error\":{\"message\":\"upstream boom\"}}","provider_name":"DeepInfra"}}}`,
		Want: sdk.APIError{Code: "502", Message: "Provider returned error (DeepInfra: upstream boom)", Kind: sdk.KindUnknown},
	}})
}

func TestDecodeOpenRouterUpstream(t *testing.T) {
	providertest.RunErrorCases(t, "openrouter-videos", errorformat.DecodeOpenRouter, []providertest.ErrorCase{
		{Name: "captured deepinfra 422", Status: http.StatusUnprocessableEntity, Body: openRouterDeepInfra422,
			Want: sdk.APIError{Code: "422", Message: deepInfraMessage, Kind: sdk.KindUnknown}},
		{Name: "captured alibaba 429 plain text raw", Status: http.StatusTooManyRequests, Body: openRouterAlibaba429,
			Want: sdk.APIError{Code: "429", Message: alibaba429Message, Kind: sdk.KindRateLimited}},
		{Name: "error_type kept", Status: http.StatusServiceUnavailable,
			Body: `{"error":{"code":503,"message":"Provider returned error","metadata":{"error_type":"provider_unavailable","raw":"upstream down","provider_name":"DeepInfra"}}}`,
			Want: sdk.APIError{Type: "provider_unavailable", Code: "503", Message: "Provider returned error (DeepInfra: upstream down)", Kind: sdk.KindServerError}},
		{Name: "truncated JSON raw", Status: http.StatusBadRequest,
			Body: `{"error":{"message":"Provider returned error","code":400,"metadata":{"raw":"[{\"message\":","provider_name":"DeepInfra"}}}`,
			Want: sdk.APIError{Code: "400", Message: "Provider returned error", Kind: sdk.KindUnknown}},
	})
}

// The text the issue reported, with the duplicated status removed and the
// upstream cause added.
func TestOpenRouterUpstreamErrorText(t *testing.T) {
	var got *sdk.APIError
	providertest.RunErrorCases(t, "openai-completions", func(e *sdk.APIError) {
		errorformat.DecodeOpenAI(e)
		got = e
	}, []providertest.ErrorCase{{Name: "deepinfra", Status: http.StatusUnprocessableEntity, Body: openRouterDeepInfra422,
		Want: sdk.APIError{Code: "422", Message: deepInfraMessage, Kind: sdk.KindUnknown}}})
	const want = "openai-completions: 422: Provider returned error (DeepInfra: Input should be 'text')"
	if got.Error() != want {
		t.Errorf("Error() = %q, want %q", got.Error(), want)
	}
}
