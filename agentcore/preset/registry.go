// Package preset is the authority-side registry of decision identities:
// an AgentPreset in, a digest-checked PresetRef out. It never holds a model
// client or a tool implementation; those live behind the effect port.
package preset

import (
	"errors"
	"fmt"
	"slices"
	"sync"
)

// Registry registers presets and resolves PresetRefs.
type Registry interface {
	Register(PresetID, AgentPreset) (PresetRef, error)
	Resolve(PresetRef) (AgentPreset, error)
}

// ErrUnavailable reports a PresetRef this process cannot resolve.
var ErrUnavailable = errors.New("preset: preset_unavailable")

// Memory is the in-memory Registry.
type Memory struct {
	mu    sync.RWMutex
	byRef map[PresetRef]AgentPreset
}

// NewMemory returns an empty in-memory Registry.
func NewMemory() *Memory { return &Memory{byRef: make(map[PresetRef]AgentPreset)} }

// Register validates and retains an immutable preset version.
// Re-registration under the same ID retains previous digest-addressed
// versions.
func (r *Memory) Register(id PresetID, p AgentPreset) (PresetRef, error) {
	if id == "" {
		return PresetRef{}, errors.New("preset: register requires a preset id")
	}
	if err := ValidatePreset(&p); err != nil {
		return PresetRef{}, err
	}
	digest, err := DigestPreset(&p)
	if err != nil {
		return PresetRef{}, err
	}
	ref := PresetRef{ID: id, Digest: digest}
	r.mu.Lock()
	r.byRef[ref] = clone(p)
	r.mu.Unlock()
	return ref, nil
}

// Resolve returns the AgentPreset when the ref's digest matches the
// registered one.
func (r *Memory) Resolve(ref PresetRef) (AgentPreset, error) {
	r.mu.RLock()
	p, ok := r.byRef[ref]
	r.mu.RUnlock()
	if !ok {
		return AgentPreset{}, fmt.Errorf("%w: unknown preset %s", ErrUnavailable, ref.ID)
	}
	digest, err := DigestPreset(&p)
	if err != nil {
		return AgentPreset{}, err
	}
	if digest != ref.Digest {
		return AgentPreset{}, fmt.Errorf("%w: preset %s digest mismatch", ErrUnavailable, ref.ID)
	}
	return clone(p), nil
}

func clone(p AgentPreset) AgentPreset {
	p.Tools = slices.Clone(p.Tools)
	for i := range p.Tools {
		if cache := p.Tools[i].Definition.CacheControl; cache != nil {
			cloned := *cache
			p.Tools[i].Definition.CacheControl = &cloned
		}
	}
	return p
}
