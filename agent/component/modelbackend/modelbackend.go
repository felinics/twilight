// Package modelbackend composes the model backend component (CLD-MDL): the
// in-process executor over a model catalog and no tools, exposed through
// the Backend protocol. It is the one process that holds provider
// credentials, resolved from a mounted secrets directory.
package modelbackend

import (
	"context"
	"errors"
	stdhttp "net/http"

	"github.com/felinics/twilight/agent/executor/backendhttp"
	"github.com/felinics/twilight/agent/models"
	"github.com/felinics/twilight/agent/secrets"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
)

// Config is the model backend's document.
type Config struct {
	Listen string `json:"listen"`
	// Catalog is the models.File the deployment mounts.
	Catalog string `json:"catalog"`
	// Secrets is the directory the catalog's credential names resolve in.
	Secrets string `json:"secrets"`
	// Streaming selects streaming model execution.
	Streaming bool `json:"streaming,omitempty"`
}

// Component is the composed model backend.
type Component struct {
	Backend  *loop.LocalExecutor
	progress *executor.ProgressHub
}

// New composes a model backend over a ready catalog.
func New(catalog loop.ModelCatalog, streaming bool) (*Component, error) {
	if catalog == nil {
		return nil, errors.New("modelbackend: a model catalog is required")
	}
	progress := executor.NewProgressHub(0)
	backend, err := loop.NewLocalExecutor(catalog, noTools{}, progress, streaming)
	if err != nil {
		return nil, err
	}
	return &Component{Backend: backend, progress: progress}, nil
}

// Compose builds the model backend its Config describes.
func Compose(ctx context.Context, cfg Config) (*Component, error) {
	if cfg.Catalog == "" || cfg.Secrets == "" {
		return nil, errors.New("modelbackend: catalog and secrets are required")
	}
	entries, err := models.LoadFile(cfg.Catalog)
	if err != nil {
		return nil, err
	}
	catalog, err := models.Build(ctx, entries, secrets.Dir(cfg.Secrets))
	if err != nil {
		return nil, err
	}
	return New(catalog, cfg.Streaming)
}

// Handler is the Backend protocol.
func (c *Component) Handler() stdhttp.Handler {
	return (&backendhttp.Server{Backend: c.Backend, Progress: c.progress}).Handler()
}

// Close releases nothing durable: executions in flight finish or are
// observed missing by the Worker, which restarts model calls (RUN-EXE-9).
func (c *Component) Close(context.Context) error { return nil }

type noTools struct{}

func (noTools) ResolveTool(ref run.ToolRef) (loop.ExecutableTool, error) {
	return nil, errors.New("modelbackend: serves no tools: " + string(ref))
}
