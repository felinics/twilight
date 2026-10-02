package prompt

import (
	"fmt"

	"github.com/felinics/twilight/agentcore/preset"
)

// BuilderFactory builds the Builder of one BuilderRef for one
// AgentPreset. The factory is pure configuration: the builder it returns
// reads state only through the Sources.
type BuilderFactory func(preset.AgentPreset, Sources) Builder

// Catalog resolves BuilderRefs on the Owner side. It is the only registry
// of this seam: every other input to a builder is data on the AgentPreset
// itself.
type Catalog struct {
	factories map[BuilderRef]BuilderFactory
}

// NewCatalog builds a registry; empty refs and nil factories are rejected,
// duplicates conflict.
func NewCatalog(entries map[BuilderRef]BuilderFactory) (*Catalog, error) {
	c := &Catalog{factories: make(map[BuilderRef]BuilderFactory, len(entries))}
	for ref, f := range entries {
		if err := c.Register(ref, f); err != nil {
			return nil, err
		}
	}
	return c, nil
}

// Register adds one builder; the ref must be new.
func (c *Catalog) Register(ref BuilderRef, f BuilderFactory) error {
	if ref == "" || f == nil {
		return fmt.Errorf("prompt: builder registration requires a ref and a factory")
	}
	if _, dup := c.factories[ref]; dup {
		return fmt.Errorf("prompt: builder %q registered twice", ref)
	}
	c.factories[ref] = f
	return nil
}

// Resolve returns the builder of the preset's PromptBuilder ref or
// ErrUnknownBuilder.
func (c *Catalog) Resolve(ap preset.AgentPreset, sources Sources) (Builder, error) {
	if c == nil {
		return nil, fmt.Errorf("prompt: no builders configured")
	}
	f, ok := c.factories[ap.PromptBuilder]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownBuilder, ap.PromptBuilder)
	}
	return f(ap, sources), nil
}
