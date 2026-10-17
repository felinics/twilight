// Package errorformat decodes the error body formats that more than one
// provider package receives, such as OpenAI's, which OpenAI-compatible APIs
// copy. A format that only one package receives stays in that package.
package errorformat

import (
	"bytes"
	"cmp"
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"github.com/felinics/twilight/sdk"
)

// openAIError is the error object of OpenAI's format. OpenAI-compatible APIs
// keep it but may send code as a number.
type openAIError struct {
	Message string          `json:"message"`
	Type    string          `json:"type"`
	Code    json.RawMessage `json:"code"`
}

// copyTo copies the object's type, code and message into e. A numeric code is
// copied as its decimal text.
func (o *openAIError) copyTo(e *sdk.APIError) {
	e.Type = o.Type
	e.Code = scalarString(o.Code)
	e.Message = o.Message
}

// openAIBody is OpenAI's error body:
// {"error":{"message":"...","type":"...","param":null,"code":"..."}}.
// A Responses API response that failed carries the same error object in a 2xx
// body. When a request made with a ChatGPT account's token (Sign in with
// ChatGPT) fails admission, before any model runs, OpenAI's API answers 400
// with no error object, only detail:
// {"detail":"Unsupported parameter: temperature"}.
type openAIBody struct {
	Error  openAIError     `json:"error"`
	Detail json.RawMessage `json:"detail"`
}

// ParseOpenAI copies type, code and message from an OpenAI-format body into
// e. A numeric code is copied as its decimal text. A body whose error object
// has no message but that has a detail member gets detail as the message: a
// string as is, any other JSON value as its compact text.
func ParseOpenAI(e *sdk.APIError) {
	var body openAIBody
	if json.Unmarshal(e.Body, &body) != nil {
		return
	}
	body.Error.copyTo(e)
	if e.Message == "" {
		e.Message = jsonText(body.Detail)
	}
}

// openAIErrorEvent is the Responses API's "error" stream event. The API
// reference puts code and message at the top level, as generated into
// openai-go's ResponseErrorEvent
// (https://github.com/openai/openai-go/blob/d7fd0c65cc247957d5b247ad42283fc8e4061868/responses/response.go):
// {"type":"error","code":"...","message":"...","param":null,"sequence_number":1}.
// The Codex CLI reads them from a nested error object instead
// (https://github.com/openai/codex/blob/1b1835f751ebdc0cfc50b3fe55d4571dbb294563/codex-rs/codex-api/src/sse/responses.rs):
// {"type":"error","error":{"type":"...","code":"...","message":"..."}}.
// The top-level type is the event's own and is not copied.
type openAIErrorEvent struct {
	Code    json.RawMessage `json:"code"`
	Message string          `json:"message"`
	Error   *openAIError    `json:"error"`
}

// ParseOpenAIErrorEvent copies code and message from a Responses "error"
// stream event into e: from the top level when it carries either, otherwise
// type, code and message from the nested error object.
func ParseOpenAIErrorEvent(e *sdk.APIError) {
	var event openAIErrorEvent
	if json.Unmarshal(e.Body, &event) != nil {
		return
	}
	if code := scalarString(event.Code); code != "" || event.Message != "" {
		e.Code = code
		e.Message = event.Message
		return
	}
	if event.Error != nil {
		event.Error.copyTo(e)
	}
}

// openAIFailedEvent is the Responses API's "response.failed" stream event,
// whose response carries the error object:
// {"type":"response.failed","response":{"status":"failed","error":{"code":"...","message":"..."}}}.
type openAIFailedEvent struct {
	Response struct {
		Error openAIError `json:"error"`
	} `json:"response"`
}

// ParseOpenAIFailedEvent copies type, code and message from the error object
// of a Responses "response.failed" stream event into e.
func ParseOpenAIFailedEvent(e *sdk.APIError) {
	var event openAIFailedEvent
	if json.Unmarshal(e.Body, &event) != nil {
		return
	}
	event.Response.Error.copyTo(e)
}

// DecodeOpenAI is the error decoder for OpenAI's API and the OpenAI-compatible
// endpoints the OpenAI-format packages are pointed at. The request ID comes
// from x-request-id; from x-oai-request-id, which OpenAI's API sends instead
// to a ChatGPT account's token (Sign in with ChatGPT); or from
// x-amzn-requestid on Amazon Bedrock.
func DecodeOpenAI(e *sdk.APIError) {
	ParseOpenAI(e)
	classifyOpenAI(e)
}

// DecodeOpenAIErrorEvent is DecodeOpenAI for a Responses "error" stream event.
func DecodeOpenAIErrorEvent(e *sdk.APIError) {
	ParseOpenAIErrorEvent(e)
	classifyOpenAI(e)
}

// DecodeOpenAIFailedEvent is DecodeOpenAI for a Responses "response.failed"
// stream event.
func DecodeOpenAIFailedEvent(e *sdk.APIError) {
	ParseOpenAIFailedEvent(e)
	classifyOpenAI(e)
}

// classifyOpenAI sets the request ID and, when OpenAI's type or code
// identifies one, the Kind of an APIError whose type and code are parsed.
func classifyOpenAI(e *sdk.APIError) {
	e.RequestID = cmp.Or(e.Header.Get("x-request-id"), e.Header.Get("x-oai-request-id"), e.Header.Get("x-amzn-requestid"))
	if k := OpenAIKind(e.Type, e.Code); k != sdk.KindUnknown {
		e.Kind = k
	}
}

// OpenAIKind classifies OpenAI's error type and code. It returns KindUnknown
// for values it has no mapping for, including every value an
// OpenAI-compatible API invents.
func OpenAIKind(typ, code string) sdk.ErrorKind {
	switch code {
	case "invalid_api_key":
		return sdk.KindAuthentication
	case "insufficient_quota",
		"credit_balance_exhausted",
		"organization_spend_limit_exceeded",
		"project_spend_limit_exceeded",
		"organization_usage_limit_exceeded":
		return sdk.KindQuotaExhausted
	case "rate_limit_exceeded", "slow_down":
		return sdk.KindRateLimited
	case "server_error", "server_is_overloaded":
		return sdk.KindServerError
	}
	switch typ {
	case "insufficient_quota":
		return sdk.KindQuotaExhausted
	case "server_error":
		return sdk.KindServerError
	}
	return sdk.KindUnknown
}

// googleBody is google.rpc.Status as the JSON APIs send it:
// {"error":{"code":429,"message":"...","status":"RESOURCE_EXHAUSTED","details":[...]}}.
type googleBody struct {
	Error struct {
		Message string `json:"message"`
		Status  string `json:"status"`
		Details []struct {
			Type       string                 `json:"@type"`
			Reason     string                 `json:"reason"`
			Violations []googleQuotaViolation `json:"violations"`
		} `json:"details"`
	} `json:"error"`
}

// googleQuotaViolation is a violation of a google.rpc.QuotaFailure detail.
type googleQuotaViolation struct {
	QuotaID string `json:"quotaId"`
}

// daily reports whether the violated quota is a daily one. Google publishes
// no list of quota IDs; a daily one's ID contains PerDay or Daily, which is
// how the Gemini CLI tells them apart
// (https://github.com/google-gemini/gemini-cli/blob/2139b121bc028e0b4c96b97385555b19c2dd629d/packages/core/src/utils/googleQuotaErrors.ts).
func (v googleQuotaViolation) daily() bool {
	return strings.Contains(v.QuotaID, "PerDay") || strings.Contains(v.QuotaID, "Daily")
}

const (
	googleErrorInfo    = "type.googleapis.com/google.rpc.ErrorInfo"
	googleQuotaFailure = "type.googleapis.com/google.rpc.QuotaFailure"
)

// DecodeGoogle is the error decoder for Google APIs, which report errors as a
// google.rpc.Status. Type is the canonical status (error.status) and Code is
// the reason of the google.rpc.ErrorInfo detail, when there is one. The APIs
// document no request ID header, so RequestID stays empty.
//
// RESOURCE_EXHAUSTED covers per-minute and per-day quotas alike. When a
// google.rpc.QuotaFailure detail names a daily quota among its violations,
// the Kind is KindQuotaExhausted: the quota resets once a day, whatever delay
// the RetryInfo detail suggests.
func DecodeGoogle(e *sdk.APIError) {
	var body googleBody
	if json.Unmarshal(e.Body, &body) != nil {
		return
	}
	e.Type = body.Error.Status
	e.Message = body.Error.Message
	for _, d := range body.Error.Details {
		if d.Type == googleErrorInfo && d.Reason != "" {
			e.Code = d.Reason
			break
		}
	}
	if k := GoogleKind(e.Type, e.Code); k != sdk.KindUnknown {
		e.Kind = k
	}
	for _, d := range body.Error.Details {
		if d.Type == googleQuotaFailure && slices.ContainsFunc(d.Violations, googleQuotaViolation.daily) {
			e.Kind = sdk.KindQuotaExhausted
			break
		}
	}
}

// GoogleKind classifies a google.rpc.Status by its ErrorInfo reason and, when
// the reason has no mapping, by its canonical status. The reasons are values
// of google.api.ErrorReason
// (https://github.com/googleapis/googleapis/blob/master/google/api/error_reason.proto).
func GoogleKind(status, reason string) sdk.ErrorKind {
	switch reason {
	case "API_KEY_INVALID", "CREDENTIALS_MISSING", "ACCESS_TOKEN_EXPIRED",
		"ACCESS_TOKEN_TYPE_UNSUPPORTED", "CREDENTIAL_TYPE_UNSUPPORTED", "JWT_TOKEN_INVALID":
		return sdk.KindAuthentication
	case "API_KEY_SERVICE_BLOCKED", "API_KEY_HTTP_REFERRER_BLOCKED", "API_KEY_IP_ADDRESS_BLOCKED",
		"API_KEY_ANDROID_APP_BLOCKED", "API_KEY_IOS_APP_BLOCKED", "ACCESS_TOKEN_SCOPE_INSUFFICIENT",
		"IAM_PERMISSION_DENIED", "USER_PROJECT_DENIED", "SERVICE_DISABLED", "CONSUMER_SUSPENDED":
		return sdk.KindPermissionDenied
	case "BILLING_DISABLED":
		return sdk.KindQuotaExhausted
	case "RATE_LIMIT_EXCEEDED":
		return sdk.KindRateLimited
	}
	switch status {
	case "UNAUTHENTICATED":
		return sdk.KindAuthentication
	case "PERMISSION_DENIED":
		return sdk.KindPermissionDenied
	case "RESOURCE_EXHAUSTED":
		return sdk.KindRateLimited
	case "INTERNAL", "UNAVAILABLE", "DEADLINE_EXCEEDED":
		return sdk.KindServerError
	}
	return sdk.KindUnknown
}

// scalarString returns a JSON string's value or a JSON number's text, and ""
// for anything else.
func scalarString(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return ""
	}
	switch raw[0] {
	case '"':
		var s string
		if json.Unmarshal(raw, &s) == nil {
			return s
		}
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		if _, err := strconv.ParseFloat(string(raw), 64); err == nil {
			return string(raw)
		}
	}
	return ""
}

// jsonText returns a JSON string's value, the compact text of any other JSON
// value, and "" for null or nothing.
func jsonText(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	if raw[0] == '"' {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			return s
		}
		return ""
	}
	var b bytes.Buffer
	if json.Compact(&b, raw) != nil {
		return ""
	}
	return b.String()
}

// openRouterBody is the part of OpenRouter's error body that OpenAI's format
// lacks: {"error":{"code":402,"message":"...","metadata":{"error_type":"..."}}}.
type openRouterBody struct {
	Error struct {
		Metadata struct {
			ErrorType string `json:"error_type"`
		} `json:"metadata"`
	} `json:"error"`
}

// DecodeOpenRouter is the error decoder for OpenRouter
// (https://openrouter.ai/docs/api-reference/errors). Code is the numeric code
// as text and Type is metadata.error_type, which OpenRouter documents as the
// field to classify by. RequestID is the x-generation-id response header,
// which OpenRouter sends on chat completion errors and lists in
// Access-Control-Expose-Headers; its API reference does not describe it.
func DecodeOpenRouter(e *sdk.APIError) {
	ParseOpenAI(e)
	e.RequestID = e.Header.Get("x-generation-id")
	var body openRouterBody
	if json.Unmarshal(e.Body, &body) == nil && body.Error.Metadata.ErrorType != "" {
		e.Type = body.Error.Metadata.ErrorType
	}
	if k := OpenRouterKind(e.Type); k != sdk.KindUnknown {
		e.Kind = k
	}
}

// OpenRouterKind classifies an OpenRouter metadata.error_type.
func OpenRouterKind(errorType string) sdk.ErrorKind {
	switch errorType {
	case "authentication":
		return sdk.KindAuthentication
	case "permission_denied":
		return sdk.KindPermissionDenied
	case "payment_required":
		return sdk.KindQuotaExhausted
	case "rate_limit_exceeded":
		return sdk.KindRateLimited
	case "provider_overloaded", "provider_unavailable", "server":
		return sdk.KindServerError
	default:
		return sdk.KindUnknown
	}
}

// dashScopeBody is DashScope's error body:
// {"request_id":"...","code":"InvalidApiKey","message":"..."}.
// A failed asynchronous task is reported in a 2xx body with the code in its
// output instead:
// {"request_id":"...","output":{"task_id":"...","task_status":"FAILED","code":"...","message":"..."}}.
type dashScopeBody struct {
	RequestID string `json:"request_id"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Output    struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"output"`
}

// DecodeDashScope is the error decoder for Alibaba Cloud Model Studio's
// DashScope API (https://help.aliyun.com/zh/model-studio/error-code). The
// request ID comes from the body.
func DecodeDashScope(e *sdk.APIError) {
	var body dashScopeBody
	if json.Unmarshal(e.Body, &body) != nil {
		return
	}
	e.Code = cmp.Or(body.Code, body.Output.Code)
	e.Message = cmp.Or(body.Message, body.Output.Message)
	e.RequestID = body.RequestID
	if k := DashScopeKind(e.Code); k != sdk.KindUnknown {
		e.Kind = k
	}
}

// DashScopeKind classifies a DashScope error code. Arrearage and the free-tier
// stop arrive as 400 and 403, so the status alone would misfile them.
func DashScopeKind(code string) sdk.ErrorKind {
	switch code {
	case "InvalidApiKey":
		return sdk.KindAuthentication
	case "Arrearage", "AllocationQuota.FreeTierOnly":
		return sdk.KindQuotaExhausted
	case "Throttling", "Throttling.RateQuota":
		return sdk.KindRateLimited
	default:
		return sdk.KindUnknown
	}
}

// DecodeDeepgram fills e from a Deepgram error body, documented at
// https://developers.deepgram.com/docs/errors. Most endpoints send
// {"err_code":"...","err_msg":"...","request_id":"..."}; the 503 example
// spells the code error_code, and JSON validation failures send
// {"category":"...","message":"...","details":"...","request_id":"..."}.
// category becomes Type. The dg-request-id response header carries the same
// ID, see https://developers.deepgram.com/docs/text-to-speech.
func DecodeDeepgram(e *sdk.APIError) {
	var body struct {
		ErrCode   string `json:"err_code"`
		ErrorCode string `json:"error_code"`
		ErrMsg    string `json:"err_msg"`
		Category  string `json:"category"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	}
	_ = json.Unmarshal(e.Body, &body)
	e.Type = body.Category
	e.Code = cmp.Or(body.ErrCode, body.ErrorCode)
	e.Message = cmp.Or(body.ErrMsg, body.Message)
	e.RequestID = cmp.Or(body.RequestID, e.Header.Get("dg-request-id"))
	if k := DeepgramKind(e.Code); k != sdk.KindUnknown {
		e.Kind = k
	}
}

// DeepgramKind classifies a Deepgram err_code. INSUFFICIENT_PERMISSIONS arrives as
// 401 as well as 403, so the status alone would misfile it.
func DeepgramKind(code string) sdk.ErrorKind {
	switch code {
	case "INVALID_AUTH":
		return sdk.KindAuthentication
	case "INSUFFICIENT_PERMISSIONS":
		return sdk.KindPermissionDenied
	case "ASR_PAYMENT_REQUIRED":
		return sdk.KindQuotaExhausted
	case "TOO_MANY_REQUESTS":
		return sdk.KindRateLimited
	default:
		return sdk.KindUnknown
	}
}

// DecodeElevenLabs fills e from an ElevenLabs error body, documented at
// https://elevenlabs.io/docs/eleven-api/resources/errors:
// {"detail":{"type":"...","code":"...","message":"...","status":"...","request_id":"...","param":"..."}}.
// Some endpoints still send only the legacy fields,
// {"detail":{"status":"model_not_found","message":"..."}}, so Code falls back
// to status. The x-trace-id response header carries the same ID as
// request_id and is the only ID on a legacy body.
// A body in any other shape keeps the Kind derived from the status.
func DecodeElevenLabs(e *sdk.APIError) {
	var body struct {
		Detail struct {
			Type      string `json:"type"`
			Code      string `json:"code"`
			Message   string `json:"message"`
			Status    string `json:"status"`
			RequestID string `json:"request_id"`
		} `json:"detail"`
	}
	_ = json.Unmarshal(e.Body, &body)
	e.Type = body.Detail.Type
	e.Code = cmp.Or(body.Detail.Code, body.Detail.Status)
	e.Message = body.Detail.Message
	e.RequestID = cmp.Or(body.Detail.RequestID, e.Header.Get("x-trace-id"))
	if k := ElevenLabsKind(e.Type, e.Code); k != sdk.KindUnknown {
		e.Kind = k
	}
}

// ElevenLabsKind classifies an ElevenLabs error type and code.
func ElevenLabsKind(typ, code string) sdk.ErrorKind {
	switch code {
	case "insufficient_credits":
		return sdk.KindQuotaExhausted
	case "rate_limit_exceeded", "concurrent_limit_exceeded", "system_busy":
		return sdk.KindRateLimited
	}
	switch typ {
	case "authentication_error":
		return sdk.KindAuthentication
	case "authorization_error":
		return sdk.KindPermissionDenied
	case "payment_required":
		return sdk.KindQuotaExhausted
	case "rate_limit_error":
		return sdk.KindRateLimited
	case "internal_error", "service_unavailable":
		return sdk.KindServerError
	}
	return sdk.KindUnknown
}
