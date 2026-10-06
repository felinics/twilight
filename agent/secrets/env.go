package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
)

// Env resolves secret names from process environment variables. It is
// intended for local development and tests; deployment code can use Dir or a
// vault-backed Resolver instead.
type Env struct{}

// Lookup reads the environment variable named name. An unset variable is
// ErrNotFound so a fallback resolver can continue; an explicitly empty value
// is returned and can be rejected by the catalog builder.
func (Env) Lookup(ctx context.Context, name string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if name == "" {
		return "", fmt.Errorf("%w: empty name", ErrNotFound)
	}
	value, ok := os.LookupEnv(name)
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	return value, nil
}

// Fallback tries resolvers in order. ErrNotFound moves to the next resolver;
// every other error is returned immediately. A found empty value is returned
// as-is so callers can distinguish it from a missing secret.
type Fallback []Resolver

// Lookup implements Resolver.
func (f Fallback) Lookup(ctx context.Context, name string) (string, error) {
	var last error
	for _, resolver := range f {
		if resolver == nil {
			continue
		}
		value, err := resolver.Lookup(ctx, name)
		if err == nil {
			return value, nil
		}
		if !errors.Is(err, ErrNotFound) {
			return "", err
		}
		last = err
	}
	if last != nil {
		return "", last
	}
	return "", fmt.Errorf("%w: %s", ErrNotFound, name)
}
