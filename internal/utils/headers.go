package utils

import (
	"context"

	"github.com/felinics/twilight/internal/reqheaders"
)

// MergeHeaders returns a fresh map with canonical HTTP header names. Later
// layers override earlier layers without retaining any caller-owned maps.
func MergeHeaders(layers ...map[string]string) map[string]string {
	return reqheaders.Merge(layers...)
}

// RequestHeaders merges defaults, provider headers, then context headers.
func RequestHeaders(ctx context.Context, defaults, provider map[string]string) map[string]string {
	return reqheaders.Merge(defaults, provider, reqheaders.FromContext(ctx))
}
