// Package mock provides the development-only workspace provider. It uses
// local directories and os/exec to exercise the workspace/tool protocol
// without pretending to be a security sandbox. Cloud deployments must use a
// real sandbox provider such as E2B; local agents should use environment/local
// with an explicitly configured user directory.
package mock

import (
	"context"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agent/environment/local"
)

// Backend is the provider identity recorded in Workspace RuntimeBindings.
const Backend environment.Backend = "mock"

// Provider is a development-only provider backed by directories and os/exec.
type Provider struct {
	inner *local.Provider
}

var _ environment.Provider = (*Provider)(nil)

// New creates a mock provider rooted at root. Environments are directories
// under root and are lost when that filesystem is lost.
func New(root string) (*Provider, error) {
	inner, err := local.New(root)
	if err != nil {
		return nil, err
	}
	return &Provider{inner: inner}, nil
}

func (p *Provider) Create(ctx context.Context, spec environment.Spec) (environment.Environment, error) {
	return p.inner.Create(ctx, spec)
}

func (p *Provider) Restore(ctx context.Context, spec environment.RestoreSpec) (environment.Environment, error) {
	return p.inner.Restore(ctx, spec)
}

func (p *Provider) Attach(ctx context.Context, ref environment.EnvironmentRef) (environment.Environment, error) {
	return p.inner.Attach(ctx, ref)
}
