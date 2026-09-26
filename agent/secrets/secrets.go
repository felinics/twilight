// Package secrets is the deployment's credential seam: a name in a
// configuration document resolves to a value at startup, and how depends
// on where the process runs. A cloud agent reads the directory a Kubernetes
// Secret is mounted as (Dir); a local agent reads what it loaded from its
// own configuration (Static); a host with a vault implements Resolver over
// it. Consumers (the model catalog today; tools, workspaces and backends as
// they need credentials) name secrets and never carry values.
package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Resolver reads one secret by name.
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
