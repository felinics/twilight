// Package reqheaders carries per-request HTTP headers on a context. It has no
// dependencies beyond the standard library so the core sdk package can import
// it without pulling in provider helpers.
package reqheaders

import (
	"context"
	"net/http"
)

type contextKey struct{}

// Merge returns a fresh map with canonical HTTP header names. Later layers
// override earlier layers without retaining any caller-owned maps.
func Merge(layers ...map[string]string) map[string]string {
	headers := make(map[string]string)
	for _, layer := range layers {
		for key, value := range layer {
			headers[http.CanonicalHeaderKey(key)] = value
		}
	}
	return headers
}

// WithContext snapshots headers onto ctx, merging any inherited values.
func WithContext(ctx context.Context, headers map[string]string) context.Context {
	return context.WithValue(ctx, contextKey{}, Merge(FromContext(ctx), headers))
}

// FromContext returns the headers carried by ctx, or nil when there are none.
// The returned map must not be modified.
func FromContext(ctx context.Context) map[string]string {
	headers, _ := ctx.Value(contextKey{}).(map[string]string)
	return headers
}
