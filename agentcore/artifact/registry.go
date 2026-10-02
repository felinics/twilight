package artifact

import (
	"fmt"

	"github.com/felinics/twilight/agentcore/jsonstable"
)

type (
	ProviderKindID     string
	ProviderInstanceID string
)

// SchemeDefinition is the resolution contract of one Scheme (ART-CAP-2).
type SchemeDefinition struct {
	Scheme                Scheme
	SupportedDurabilities []Durability
	// ValidateRef checks scheme-specific Key and Integrity rules; nil accepts
	// every Ref that passes Ref.Validate.
	ValidateRef func(Ref) error
}

// ProviderDescriptor describes one provider kind and the schemes it serves.
type ProviderDescriptor struct {
	KindID       ProviderKindID
	Schemes      []Scheme
	ConfigSchema jsonstable.Value
}

// ProviderBinding binds one (Scheme, Authority) to a provider instance.
type ProviderBinding struct {
	Scheme     Scheme
	Authority  Authority
	InstanceID ProviderInstanceID
}

// Registry is the immutable scheme and provider index of one deployment
// (ART-PRO-1): every Scheme has one definition, every (Scheme, Authority) one
// provider instance, and verified use requires both.
type Registry struct {
	schemes   map[Scheme]SchemeDefinition
	providers map[providerKey]ProviderBinding
}

type providerKey struct {
	scheme    Scheme
	authority Authority
}

// BuildRegistry validates the scheme set and provider bindings and freezes
// them.
func BuildRegistry(schemes []SchemeDefinition, bindings []ProviderBinding) (*Registry, error) {
	r := &Registry{schemes: make(map[Scheme]SchemeDefinition, len(schemes)), providers: make(map[providerKey]ProviderBinding, len(bindings))}
	for _, def := range schemes {
		if def.Scheme == "" {
			return nil, &Error{Code: ErrInvalid, Operation: opRegistry, Detail: "empty scheme"}
		}
		if _, dup := r.schemes[def.Scheme]; dup {
			return nil, &Error{Code: ErrConflict, Operation: opRegistry, Identity: string(def.Scheme), Detail: "scheme defined twice"}
		}
		if len(def.SupportedDurabilities) == 0 {
			return nil, &Error{Code: ErrInvalid, Operation: opRegistry, Identity: string(def.Scheme), Detail: "no supported durability"}
		}
		for _, d := range def.SupportedDurabilities {
			if d.Rank() < 0 {
				return nil, &Error{Code: ErrInvalid, Operation: opRegistry, Identity: string(def.Scheme), Detail: detailUnknownDurability}
			}
		}
		r.schemes[def.Scheme] = def
	}
	for _, b := range bindings {
		if b.Authority == "" || b.InstanceID == "" {
			return nil, &Error{Code: ErrInvalid, Operation: opRegistry, Identity: string(b.Scheme), Detail: "provider binding with empty authority or instance"}
		}
		if _, ok := r.schemes[b.Scheme]; !ok {
			return nil, &Error{Code: ErrInvalid, Operation: opRegistry, Identity: string(b.Scheme), Detail: "provider binding for an unregistered scheme"}
		}
		k := providerKey{b.Scheme, b.Authority}
		if _, dup := r.providers[k]; dup {
			return nil, &Error{Code: ErrConflict, Operation: opRegistry, Identity: fmt.Sprintf("%s/%s", b.Scheme, b.Authority), Detail: "authority bound twice"}
		}
		r.providers[k] = b
	}
	return r, nil
}

// Scheme returns the definition of s.
func (r *Registry) Scheme(s Scheme) (SchemeDefinition, bool) {
	def, ok := r.schemes[s]
	return def, ok
}

// Provider returns the provider instance bound to (scheme, authority).
func (r *Registry) Provider(scheme Scheme, authority Authority) (ProviderBinding, bool) {
	b, ok := r.providers[providerKey{scheme, authority}]
	return b, ok
}

// Verify admits a Ref for verified use: a registered scheme that supports the
// Ref's durability, the scheme's own rules, and a bound provider for its
// Authority. An unknown scheme is ErrUnsupported so callers can keep such a
// Ref conservatively (ART-RET-2) rather than treat it as invalid.
func (r *Registry) Verify(ref Ref) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	def, ok := r.schemes[ref.Scheme]
	if !ok {
		return &Error{Code: ErrUnsupported, Operation: opVerify, Identity: string(ref.Key), Detail: fmt.Sprintf("unknown scheme %s", ref.Scheme)}
	}
	supported := false
	for _, d := range def.SupportedDurabilities {
		if d == ref.Durability {
			supported = true
		}
	}
	if !supported {
		return &Error{Code: ErrInvalid, Operation: opVerify, Identity: string(ref.Key), Detail: fmt.Sprintf("scheme %s does not support durability %s", ref.Scheme, ref.Durability)}
	}
	if def.ValidateRef != nil {
		if err := def.ValidateRef(ref); err != nil {
			return err
		}
	}
	if _, bound := r.providers[providerKey{ref.Scheme, ref.Authority}]; !bound {
		return &Error{Code: ErrUnavailable, Operation: opVerify, Identity: string(ref.Key), Detail: fmt.Sprintf("no provider bound for %s/%s", ref.Scheme, ref.Authority)}
	}
	return nil
}

// CASScheme is the standard cas definition: every durability, and a Key that
// equals the Ref's integrity in <algorithm>:<value> form (ART-CAP-2).
func CASScheme() SchemeDefinition {
	return SchemeDefinition{
		Scheme:                SchemeCAS,
		SupportedDurabilities: []Durability{Ephemeral, EventBound, Pinned},
		ValidateRef: func(ref Ref) error {
			if ref.Integrity == nil {
				return &Error{Code: ErrInvalid, Operation: opRef, Identity: string(ref.Key), Detail: "cas ref requires integrity"}
			}
			if want := Key(ref.Integrity.Algorithm + ":" + ref.Integrity.Value); ref.Key != want {
				return &Error{Code: ErrInvalid, Operation: opRef, Identity: string(ref.Key), Detail: "cas key must equal <algorithm>:<value> of the integrity"}
			}
			return nil
		},
	}
}
