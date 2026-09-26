// Package local is the colocated executor: model and tool effects run in
// goroutines of this process against a static Catalog. It is the
// implementation side of the effect layer; nothing in it enters an
// AgentPreset.
package local

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
)

// Catalog is a static effect catalog for a colocated deployment: the model
// invokers and tool implementations one process serves.
type Catalog struct {
	models map[run.ModelRef]loop.ModelInvoker
	tools  map[run.ToolRef]loop.ExecutableTool
}

// NewCatalog builds a Catalog; models maps each ModelRef to its invoker.
func NewCatalog(models map[run.ModelRef]loop.ModelInvoker, tools ...loop.ExecutableTool) (*Catalog, error) {
	c := &Catalog{models: make(map[run.ModelRef]loop.ModelInvoker, len(models)), tools: make(map[run.ToolRef]loop.ExecutableTool, len(tools))}
	for ref, inv := range models {
		if ref == "" || inv == nil {
			return nil, errors.New("local: catalog requires a model ref and an invoker")
		}
		c.models[ref] = inv
	}
	for _, t := range tools {
		if t == nil {
			return nil, errors.New("local: catalog got a nil tool")
		}
		if _, dup := c.tools[t.Ref()]; dup {
			return nil, fmt.Errorf("local: duplicate tool %q", t.Ref())
		}
		c.tools[t.Ref()] = t
	}
	return c, nil
}

func (c *Catalog) ResolveModel(ref run.ModelRef) (loop.ModelInvoker, error) {
	inv, ok := c.models[ref]
	if !ok {
		return nil, fmt.Errorf("local: unknown model %q", ref)
	}
	return inv, nil
}

func (c *Catalog) ResolveTool(ref run.ToolRef) (loop.ExecutableTool, error) {
	t, ok := c.tools[ref]
	if !ok {
		return nil, fmt.Errorf("local: unknown tool %q", ref)
	}
	return t, nil
}

// Provider is the ExecutionRef provider of the colocated backend.
const Provider = "local"

// NewLocalExecutor is the colocated Backend: effects run in goroutines of
// this process against the Catalog, and progress frames go to sink, normally
// the Worker's ProgressHub (RUN-EXE-12).
// Model assignments must carry the request inline (RUN-EXE-7); the authority
// still writes frozen bodies to the content store (RUN-WIR-4) for the
// Session record, and the backend never reads them back. streaming selects
// StreamingModelInvoker when an invoker offers it. Agent Core reaches the
// backend through an executor.Worker (RUN-EXE-8).
func NewLocalExecutor(cat *Catalog, sink effect.ProgressSink, streaming bool) (executor.ExecutionBackend, error) {
	if cat == nil {
		return nil, errors.New("local: nil catalog")
	}
	return loop.NewLocalExecutor(cat, cat, sink, streaming)
}

// Route is the Worker route that hands every remaining Assignment to b.
func Route(b executor.ExecutionBackend) executor.Route { return executor.Default(Provider, b) }

var _ executor.ExecutionBackend = (*loop.LocalExecutor)(nil)
