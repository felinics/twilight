// Package secrets is the credential seam: a name in a configuration
// document resolves to a value at startup. Resolvers can read a mounted
// directory, process environment, in-memory test values, or a vault-backed
// implementation. Consumers name secrets and never carry their values.
package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Resolver reads one secret by name. Local callers may compose Dir and Env
// with Fallback; deployment callers can provide a mounted volume or vault.
type Resolver interface {
	Lookup(ctx context.Context, name string) (string, error)
}

// ErrNotFound reports a name the Resolver does not hold.
var ErrNotFound = errors.New("secrets: not found")

// Dir is a Resolver over a directory with one file per secret, the shape a
// Kubernetes Secret volume has: the file's name is the secret's name, its
// content the value. A name is one path element (Kubernetes allows dots
// and leading dots in a key; only "", "." and ".." and anything with a
// separator are refused). The value is opaque except for one trailing
// newline, which a value written from a file or an editor commonly has and
// no credential ends in; nothing else is trimmed.
type Dir string

// Lookup is Resolver.
func (d Dir) Lookup(_ context.Context, name string) (string, error) {
	if name == "" || name == "." || name == ".." || name != filepath.Base(name) {
		return "", fmt.Errorf("%w: invalid secret name %q", ErrNotFound, name)
	}
	raw, err := os.ReadFile(filepath.Join(string(d), name))
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("%w: %s", ErrNotFound, name)
		}
		return "", err
	}
	value := strings.TrimSuffix(string(raw), "\n")
	return strings.TrimSuffix(value, "\r"), nil
}

// Static is a Resolver over values already in memory: what a local agent
// loaded from its configuration, or a test supplies.
type Static map[string]string

// Lookup is Resolver.
func (s Static) Lookup(_ context.Context, name string) (string, error) {
	v, ok := s[name]
	if !ok {
		return "", fmt.Errorf("%w: %s", ErrNotFound, name)
	}
	return v, nil
}
