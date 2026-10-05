// Package toolbackend composes the tool sandbox backend component
// (CLD-TOL-1): the workspace backend over a workspace store and an
// environment provider, exposed through the Backend protocol beside the
// workspace control face (APP-WSP-7).
package toolbackend

import (
	"context"
	"errors"
	"fmt"
	stdhttp "net/http"

	"github.com/felinics/twilight/agent/component/stores"
	"github.com/felinics/twilight/agent/config"
	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agent/environment/agentsandbox"
	"github.com/felinics/twilight/agent/environment/local"
	"github.com/felinics/twilight/agent/environment/mock"
	"github.com/felinics/twilight/agent/executor/backendhttp"
	"github.com/felinics/twilight/agent/executor/sandbox"
	"github.com/felinics/twilight/agent/tools"
	"github.com/felinics/twilight/agent/workspace"
	wshttp "github.com/felinics/twilight/agent/workspace/http"
	"github.com/felinics/twilight/agentcore/executor"
)

// Config is the tool backend's document.
type Config struct {
	Listen string `json:"listen"`
	// Workspaces is the workspace record store.
	Workspaces config.Store `json:"workspaces"`
	// Environments selects the provider; today the local one.
	Environments Environments `json:"environments"`
}

// Environments is the provider selection.
type Environments struct {
	// Local is for a trusted single-process/local deployment. It must not be
	// used as a cloud sandbox.
	Local *LocalProvider `json:"local,omitempty"`
	// Mock is a development-only directory/os/exec provider for unit and
	// protocol tests.
	Mock *MockProvider `json:"mock,omitempty"`
	// AgentSandbox is the Kubernetes development provider: one Sandbox Pod
	// per workspace, managed by the Agent Sandbox controller.
	AgentSandbox *AgentSandboxProvider `json:"agentSandbox,omitempty"`
}

// LocalProvider is agent/environment/local: one directory per environment
// under Root.
type LocalProvider struct {
	Root string `json:"root"`
}

// MockProvider is the development-only mock environment provider.
type MockProvider struct {
	Root string `json:"root"`
}

// AgentSandboxProvider configures the Agent Sandbox warm pool.
type AgentSandboxProvider struct {
	Namespace string `json:"namespace"`
	WarmPool  string `json:"warmPool"`
}

// Component is the composed tool backend.
type Component struct {
	Backend  *sandbox.Backend
	progress *executor.ProgressHub
	closes   []func() error
}

// Options compose a Component from ready dependencies.
type Options struct {
	Workspaces workspace.Store
	Provider   environment.Provider
	Backend    environment.Backend
	// Tools are the workspace tools served; nil selects tools.Default.
	Tools []tools.Tool
}

// New composes a tool backend from ready dependencies.
func New(opts Options) (*Component, error) {
	progress := executor.NewProgressHub(0)
	ts := opts.Tools
	if ts == nil {
		ts = tools.Default()
	}
	backend, err := sandbox.New(sandbox.Options{Workspaces: opts.Workspaces, Provider: opts.Provider, Backend: opts.Backend, Tools: ts, Progress: progress})
	if err != nil {
		return nil, err
	}
	return &Component{Backend: backend, progress: progress}, nil
}

// Compose builds the tool backend its Config describes.
func Compose(ctx context.Context, cfg Config) (*Component, error) {
	var (
		provider  environment.Provider
		backend   environment.Backend
		err       error
		providers int
	)
	if cfg.Environments.Local != nil {
		providers++
	}
	if cfg.Environments.Mock != nil {
		providers++
	}
	if cfg.Environments.AgentSandbox != nil {
		providers++
	}
	if providers != 1 {
		return nil, errors.New("toolbackend: configure exactly one environment provider (agentSandbox, mock, or local)")
	}
	switch {
	case cfg.Environments.AgentSandbox != nil:
		provider, err = agentsandbox.New(ctx, agentsandbox.Config{
			Namespace: cfg.Environments.AgentSandbox.Namespace,
			WarmPool:  cfg.Environments.AgentSandbox.WarmPool,
		})
		backend = agentsandbox.Backend
	case cfg.Environments.Mock != nil:
		provider, err = mock.New(cfg.Environments.Mock.Root)
		backend = mock.Backend
	case cfg.Environments.Local != nil:
		provider, err = local.New(cfg.Environments.Local.Root)
		backend = local.Backend
	}
	if err != nil {
		return nil, err
	}
	db, err := stores.Open(ctx, cfg.Workspaces)
	if err != nil {
		return nil, fmt.Errorf("toolbackend: workspaces: %w", err)
	}
	c, err := New(Options{Workspaces: db.Workspaces(), Provider: provider, Backend: backend})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	c.closes = append(c.closes, db.Close)
	return c, nil
}

// Handler serves the Backend protocol at the root and the workspace
// control face under /workspaces/.
func (c *Component) Handler() stdhttp.Handler {
	mux := stdhttp.NewServeMux()
	mux.Handle("/workspaces/", (&wshttp.Server{Snapshots: c.Backend}).Handler())
	mux.Handle("/", (&backendhttp.Server{Backend: c.Backend, Progress: c.progress}).Handler())
	return mux
}

// Close detaches the environments this process attached; the workspaces
// keep their RuntimeBindings.
func (c *Component) Close(ctx context.Context) error {
	err := c.Backend.Close(ctx)
	for _, close := range c.closes {
		if cerr := close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}
