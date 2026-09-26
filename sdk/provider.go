package sdk

import (
	"context"
	"fmt"
	"net/http"
)

// ProviderStatus represents the health status of a provider.
type ProviderStatus string

const (
	ProviderStatusOK          ProviderStatus = "ok"
	ProviderStatusUnhealthy   ProviderStatus = "unhealthy"
	ProviderStatusUnreachable ProviderStatus = "unreachable"
)

// ProviderTestResult holds the result of a provider health check.
type ProviderTestResult struct {
	Status  ProviderStatus
	Message string
	Error   error
}

// HTTPStatusError is implemented by provider errors that carry the HTTP
// status the provider answered with, so callers above the provider can
// classify a failure (rate limit, authentication, server outage) without
// knowing the provider's error type.
type HTTPStatusError interface {
	error
	HTTPStatus() int
}

// ModelTestResult holds the result of a model support check.
type ModelTestResult struct {
	Supported bool
	Message   string
}

// ClassifyProbeStatus maps an HTTP status code from a minimal generation
// request to a ModelTestResult. Providers use this as a fallback when the
// models listing API (GET /models/{id}) is unavailable.
func ClassifyProbeStatus(statusCode int) (*ModelTestResult, error) {
	switch {
	case statusCode >= 200 && statusCode <= 299:
		return &ModelTestResult{Supported: true, Message: "supported"}, nil
	case statusCode == http.StatusBadRequest,
		statusCode == http.StatusUnprocessableEntity,
		statusCode == http.StatusTooManyRequests:
		return &ModelTestResult{Supported: true, Message: "supported"}, nil
	case statusCode == http.StatusNotFound:
		return &ModelTestResult{Supported: false, Message: "model not found"}, nil
	case statusCode == http.StatusUnauthorized, statusCode == http.StatusForbidden:
		return nil, fmt.Errorf("authentication failed (HTTP %d)", statusCode)
	default:
		return nil, fmt.Errorf("unexpected status %d", statusCode)
	}
}

// Provider is the interface that AI backends must implement.
//
// DoGenerate and DoStream are the seam between the SDK and a backend, and they
// speak the single-call boundary: a Request goes in, and a ModelResult or a
// channel of StreamParts comes out. Tool schemas arrive in Request.Tools as
// JSON Schema.
//
// DoStream must close the returned channel, must respect ctx while producing and
// while sending, and reports a mid-stream failure as an ErrorPart rather than by
// dropping the failure. Assembling the parts into a ModelResult is the SDK's
// job, not the provider's, so that streamed and generated results cannot drift.
type Provider interface {
	Name() string
	ListModels(ctx context.Context) ([]Model, error)
	Test(ctx context.Context) *ProviderTestResult
	TestModel(ctx context.Context, modelID string) (*ModelTestResult, error)
	DoGenerate(ctx context.Context, req Request) (ModelResult, error)
	DoStream(ctx context.Context, req Request) (<-chan StreamPart, error)
}
