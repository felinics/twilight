package local

import (
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/run"
)

// Catalog is a static effect catalog for a colocated deployment: the model
// invokers and tool implementations one process serves.
type Catalog struct {
	models map[run.ModelRef]ModelInvoker
	tools  map[run.ToolRef]ExecutableTool
}

// NewCatalog builds a Catalog; models maps each ModelRef to its invoker.
func NewCatalog(models map[run.ModelRef]ModelInvoker, tools ...ExecutableTool) (*Catalog, error) {
	c := &Catalog{models: make(map[run.ModelRef]ModelInvoker, len(models)), tools: make(map[run.ToolRef]ExecutableTool, len(tools))}
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

func (c *Catalog) ResolveModel(ref run.ModelRef) (ModelInvoker, error) {
	inv, ok := c.models[ref]
	if !ok {
		return nil, fmt.Errorf("local: unknown model %q", ref)
	}
	return inv, nil
}

func (c *Catalog) ResolveTool(ref run.ToolRef) (ExecutableTool, error) {
	t, ok := c.tools[ref]
	if !ok {
		return nil, fmt.Errorf("local: unknown tool %q", ref)
	}
	return t, nil
}

// Provider is the ExecutionRef provider of the colocated backend.
const Provider = "local"

// Route is the Worker route that hands every remaining Assignment to b.
func Route(b executor.ExecutionBackend) executor.Route { return executor.Default(Provider, b) }
