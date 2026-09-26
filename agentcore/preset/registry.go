// Package preset is the authority-side registry of decision identities
// (PST): an AgentPreset in, a digest-checked PresetRef out. It never
// holds a model client or a tool implementation; those live behind the
// effect port.
package preset

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/felinics/twilight/agentcore/turn"
)

// Registry registers presets and resolves PresetRefs (PST-1).
type Registry interface {
	Register(turn.PresetID, turn.AgentPreset) (turn.PresetRef, error)
	Resolve(turn.PresetRef) (turn.AgentPreset, error)
}

// ErrUnavailable reports a PresetRef this process cannot resolve
// (PST-2).
var ErrUnavailable = errors.New("preset: preset_unavailable")

// Memory is the in-memory Registry.
type Memory struct {
	mu    sync.RWMutex
	byRef map[turn.PresetRef]turn.AgentPreset
}

// NewMemory returns an empty in-memory Registry.
func NewMemory() *Memory { return &Memory{byRef: make(map[turn.PresetRef]turn.AgentPreset)} }

// Register validates and retains an immutable preset version (TRN-PST-2).
// Re-registration under the same ID retains previous digest-addressed
// versions.
func (r *Memory) Register(id turn.PresetID, p turn.AgentPreset) (turn.PresetRef, error) {
	if id == "" {
		return turn.PresetRef{}, errors.New("preset: register requires a preset id")
	}
	if err := turn.ValidatePreset(&p); err != nil {
		return turn.PresetRef{}, err
	}
	digest, err := turn.DigestPreset(&p)
	if err != nil {
		return turn.PresetRef{}, err
	}
	ref := turn.PresetRef{ID: id, Digest: digest}
	r.mu.Lock()
	r.byRef[ref] = clone(p)
	r.mu.Unlock()
	return ref, nil
}

// Resolve returns the AgentPreset when the ref's digest matches the
// registered one (TRN-PST-2).
func (r *Memory) Resolve(ref turn.PresetRef) (turn.AgentPreset, error) {
	r.mu.RLock()
	p, ok := r.byRef[ref]
	r.mu.RUnlock()
	if !ok {
		return turn.AgentPreset{}, fmt.Errorf("%w: unknown preset %s", ErrUnavailable, ref.ID)
	}
	digest, err := turn.DigestPreset(&p)
	if err != nil {
		return turn.AgentPreset{}, err
	}
	if digest != ref.Digest {
		return turn.AgentPreset{}, fmt.Errorf("%w: preset %s digest mismatch", ErrUnavailable, ref.ID)
	}
	return clone(p), nil
}

func clone(p turn.AgentPreset) turn.AgentPreset {
	p.Tools = slices.Clone(p.Tools)
	for i := range p.Tools {
		if cache := p.Tools[i].Definition.CacheControl; cache != nil {
			cloned := *cache
			p.Tools[i].Definition.CacheControl = &cloned
		}
	}
	return p
}
