// Package module is the framework the ledger's writers declare themselves
// through: a module names its event types with versioned codecs, the
// stream domains it owns and how each reads across a parent edge, the
// projections it folds and the modules it requires. The Registry checks
// the declarations against one another and is the one place events are
// encoded, decoded and folded; trust is a property of how a module reached
// the Registry, never of what its descriptor says.
package module

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"strings"
	"unicode/utf8"
)

// ProjectionID names one projection of the module framework.
type (
	ProjectionID      string
	ProjectionVersion uint16
)

// SourceTwilight is the source reserved for this repository's first-party
// modules; application modules register under their own SourceID (EXT-REG-1).
const SourceTwilight SourceID = "twilight"

// TwilightModule is the ModuleKey of a first-party module.
func TwilightModule(id ModuleID) ModuleKey { return ModuleKey{Source: SourceTwilight, ID: id} }

// PayloadCodec encodes and decodes one payload version. Encode never writes
// the `v` field: the Registry adds it (EXT-COD-2).
type PayloadCodec interface {
	Encode(value any) (jsonstable.Value, error)
	Decode(wire jsonstable.Value) (any, error)
	Validate(value any) error
}

// EventDefinition declares one event type, the codec of every version it was
// ever written under, and the version new payloads are written with.
type EventDefinition struct {
	Type   ledger.EventType
	Codecs map[PayloadVersion]PayloadCodec
	// Version is the PayloadVersion Encode writes; zero selects the highest
	// key of Codecs. It must name one of them.
	Version  PayloadVersion
	Bindings []BindingReferenceDefinition
	// Ignorable marks purely informational events: a fold that cannot decode
	// the event's payload version skips it instead of failing (EXT-PRJ-2).
	Ignorable bool
	// Stream names the stream domain the event may be appended to: one the
	// same module declares in ModuleDescriptor.Streams (EXT-STR-1). The
	// Writer rejects an event placed in a batch of another domain.
	Domain string
}

// StreamDefinition declares one logical stream domain a module owns
// (EXT-STR-1): the boundary of the invariants its events keep. The kernel
// names no domain; every domain a Session writes is declared here by
// exactly one module, which is the only module allowed to append to it.
type StreamDefinition struct {
	// Domain is the Domain.Domain of every stream of the definition.
	Domain string
	// Key extracts, from a typed event value of this domain, the ID of the
	// stream it belongs to. Nil declares a singleton domain: one stream, no
	// ID. Non-nil declares a keyed domain whose streams are domain/<id>; the
	// Writer requires Key(value) to equal the batch's stream ID
	// (EXT-STR-1). The binding is a property of the module's Go values, not
	// of a payload field name.
	Key StreamKey
	// Inheritance is how a child segment reads the domain:
	// Inherited for state the child continues, Own
	// for history that stays with the segment that wrote it.
	Inheritance Inheritance
}

// StreamKey names the stream a typed event value belongs to; it fails for a
// value that is not one of the domain's event types.
type StreamKey func(value any) (string, error)

// Keyed reports whether the domain's streams carry an ID.
func (d StreamDefinition) Keyed() bool { return d.Key != nil }

// Ref names one stream of the domain; id is empty for a singleton.
func (d StreamDefinition) Ref(id string) ledger.Domain {
	return ledger.Domain{Name: d.Domain, Id: id}
}

// ModuleRequirement declares that a module consumes another module's events
// (EXT-REG-4). Source is required: module identity is the (Source, ID)
// pair. Versions are not part of the handshake: the producer's codecs
// upcast every version to its current value, which is all a consumer sees.
type ModuleRequirement struct {
	Source SourceID
	Module ModuleID
	Events []ledger.EventType
}

// Key is the identity the requirement points at.
func (r ModuleRequirement) Key() ModuleKey { return ModuleKey{Source: r.Source, ID: r.Module} }

type ModuleDescriptor struct {
	Source   SourceID
	ID       ModuleID
	Requires []ModuleRequirement
	// Streams are the stream domains the module owns (EXT-STR-1). Every
	// event the module declares names one of them.
	Streams     []StreamDefinition
	Events      []EventDefinition
	Projections []ProjectionDefinition
}

// Key is the module's registry identity.
func (m *ModuleDescriptor) Key() ModuleKey { return ModuleKey{Source: m.Source, ID: m.ID} }

// DecodedEvent is one ledger event decoded against the registry. Stream is
// the logical stream the fold read the event from; Decode alone cannot know
// it, so folds set it after decoding.
type DecodedEvent struct {
	Domain ledger.Domain
	// Position is the event's ledger position; a fold fills it, a bare Decode
	// leaves it zero.
	Position ledger.Position
	Event    ledger.Event
	Module   ModuleKey
	Version  PayloadVersion
	Value    any
	Unknown  bool
}

// Registry is the immutable index built once at startup (EXT-REG-1).
type Registry struct {
	modules     map[ModuleKey]ModuleDescriptor
	streams     map[string]streamEntry
	events      map[ledger.EventType]eventEntry
	projections map[projectionKey]projectionEntry
}

type streamEntry struct {
	module ModuleKey
	def    StreamDefinition
}

type eventEntry struct {
	module ModuleKey
	def    EventDefinition
}

type projectionKey struct {
	id      ProjectionID
	version ProjectionVersion
}

type projectionEntry struct {
	module ModuleKey
	def    ProjectionDefinition
}

// validSegment checks one identity segment of an EventType prefix.
func validSegment(kind, v string) error {
	if v == "" {
		return fmt.Errorf("empty %s", kind)
	}
	if strings.Contains(v, "/") {
		return fmt.Errorf("%s %q contains %q", kind, v, "/")
	}
	if !utf8.ValidString(v) {
		return fmt.Errorf("%s is not valid UTF-8", kind)
	}
	return nil
}

// BuildRegistry validates the module set and freezes the indexes. Every
// module passed here is trusted: the caller vouches for it, so it may
// declare authoritative projections (EXT-PRJ-9). Modules from outside the
// deployment's trust boundary go through BuildRegistryWithExtensions.
func BuildRegistry(modules ...ModuleDescriptor) (*Registry, error) {
	return BuildRegistryWithExtensions(modules, nil)
}

// BuildRegistryWithExtensions builds a registry from trusted core modules
// and untrusted extensions. Trust is a property of how a module reached the
// registry, not of what its descriptor says: an extension may not declare an
// authoritative projection and may not use the SourceTwilight source, so no
// descriptor can claim the first-party capability for itself.
func BuildRegistryWithExtensions(core, extensions []ModuleDescriptor) (*Registry, error) {
	modules := make([]ModuleDescriptor, 0, len(core)+len(extensions))
	trusted := make(map[ModuleKey]bool, len(core))
	for i := range core {
		modules = append(modules, core[i])
		trusted[core[i].Key()] = true
	}
	for i := range extensions {
		m := &extensions[i]
		if m.Source == SourceTwilight {
			return nil, ledger.NewInvalid("", fmt.Sprintf("extension module %q claims the %s source", m.ID, SourceTwilight))
		}
		for _, p := range m.Projections {
			if p.Authoritative {
				return nil, ledger.NewInvalid("", fmt.Sprintf("projection %q of extension %s/%s declares Authoritative; only trusted core modules may", p.ID, m.Source, m.ID))
			}
		}
		modules = append(modules, *m)
	}
	r := &Registry{
		modules: make(map[ModuleKey]ModuleDescriptor), streams: make(map[string]streamEntry),
		events: make(map[ledger.EventType]eventEntry), projections: make(map[projectionKey]projectionEntry)}
	for i := range modules {
		m := &modules[i]
		if err := validSegment("source", string(m.Source)); err != nil {
			return nil, ledger.NewInvalid("", fmt.Sprintf("module %q: %v", m.ID, err))
		}
		if err := validSegment("module id", string(m.ID)); err != nil {
			return nil, ledger.NewInvalid("", fmt.Sprintf("source %q: %v", m.Source, err))
		}
		key := m.Key()
		if _, dup := r.modules[key]; dup {
			return nil, ledger.NewInvalid("", fmt.Sprintf("duplicate module %s/%s", key.Source, key.ID))
		}
		r.modules[key] = *m
		for _, sd := range m.Streams {
			if err := ledger.ValidateStreamRef(ledger.Domain{Name: sd.Domain}); err != nil {
				return nil, ledger.NewInvalid("", fmt.Sprintf("module %s/%s: %v", key.Source, key.ID, err))
			}
			if prev, dup := r.streams[sd.Domain]; dup {
				return nil, ledger.NewInvalid("", fmt.Sprintf("duplicate stream domain %q: declared by %s/%s and %s/%s",
					sd.Domain, prev.module.Source, prev.module.ID, key.Source, key.ID))

			}
			if err := ValidateInheritance(sd.Inheritance); err != nil {
				return nil, ledger.NewInvalid("", fmt.Sprintf("stream domain %q: %v", sd.Domain, err))
			}
			r.streams[sd.Domain] = streamEntry{module: key, def: sd}
		}
		for _, def := range m.Events {
			if err := r.registerEvent(m, key, def); err != nil {
				return nil, err
			}
		}
		for _, p := range m.Projections {
			if p.ID == "" || p.Version == 0 || p.Initial == nil || p.Apply == nil || p.StateCodec == nil {
				return nil, ledger.NewInvalid("", fmt.Sprintf("projection %q is incomplete", p.ID))
			}
			// Refusing commits is a capability of trusted core modules, not
			// something a descriptor declares for itself (EXT-PRJ-9); the
			// extension path above already refused it, this guards the map.
			if p.Authoritative && !trusted[key] {
				return nil, ledger.NewInvalid("", fmt.Sprintf("projection %q of module %s/%s declares Authoritative without trust", p.ID, m.Source, m.ID))
			}
			k := projectionKey{p.ID, p.Version}
			if _, dup := r.projections[k]; dup {
				return nil, ledger.NewInvalid("", fmt.Sprintf("duplicate projection %q v%d", p.ID, p.Version))
			}
			r.projections[k] = projectionEntry{module: key, def: p}
		}
	}
	if err := r.checkRequirements(); err != nil {
		return nil, err
	}
	return r, nil
}

// checkRequirements enforces EXT-REG-4: registered dependencies, no cycles,
// projection consumption within scope, and handled payload versions.
func (r *Registry) checkRequirements() error {
	state := make(map[ModuleKey]int) // 0 unvisited, 1 visiting, 2 done
	var visit func(ModuleKey) error
	visit = func(key ModuleKey) error {
		switch state[key] {
		case 1:
			return ledger.NewInvalid("", fmt.Sprintf("module requirement cycle through %s/%s", key.Source, key.ID))
		case 2:
			return nil
		}
		state[key] = 1
		for _, req := range r.modules[key].Requires {
			if req.Source == "" {
				return ledger.NewInvalid("", fmt.Sprintf("module %s/%s: requirement on %q has no source", key.Source, key.ID, req.Module))
			}
			depKey := req.Key()
			dep, ok := r.modules[depKey]
			if !ok {
				return ledger.NewInvalid("", fmt.Sprintf("module %s/%s requires unregistered module %s/%s", key.Source, key.ID, depKey.Source, depKey.ID))
			}
			for _, typ := range req.Events {
				entry, ok := r.events[typ]
				if !ok || entry.module != dep.Key() {
					return ledger.NewInvalid(typ, fmt.Sprintf("module %s/%s requires event not owned by %s/%s", key.Source, key.ID, depKey.Source, depKey.ID))
				}
			}
			if err := visit(depKey); err != nil {
				return err
			}
		}
		state[key] = 2
		return nil
	}
	for key := range r.modules {
		if err := visit(key); err != nil {
			return err
		}
	}
	for k := range r.projections {
		p := r.projections[k]
		scope := r.scopeOf(p.module)
		for _, typ := range p.def.Consumes {
			entry, ok := r.events[typ]
			if !ok {
				return ledger.NewInvalid(typ, fmt.Sprintf("projection %q consumes unregistered event", k.id))
			}
			if _, inScope := scope[entry.module]; !inScope {
				return ledger.NewInvalid(typ, fmt.Sprintf("projection %q consumes event of module %s/%s outside its Requires", k.id, entry.module.Source, entry.module.ID))
			}
		}
	}
	return nil
}

// scopeOf is the module plus its Requires: the modules whose unknown events a
// projection must not silently skip (EXT-PRJ-2).
func (r *Registry) scopeOf(key ModuleKey) map[ModuleKey]struct{} {
	scope := map[ModuleKey]struct{}{key: {}}
	for _, req := range r.modules[key].Requires {
		scope[req.Key()] = struct{}{}
	}
	return scope
}

// registerEvent validates one event definition of module m and indexes it:
// the type under the module's prefix and unique, at least one codec with a
// non-zero version, one codec at most when a version is prerelease, a write
// Version that names one of them (defaulting to the highest), a stream
// domain the module declares, and valid bindings.
func (r *Registry) registerEvent(m *ModuleDescriptor, key ModuleKey, def EventDefinition) error {
	prefix := ModulePrefix(m.Source, m.ID)
	if !strings.HasPrefix(string(def.Type), string(prefix)) || len(def.Type) == len(prefix) {
		return ledger.NewInvalid(def.Type, fmt.Sprintf("event type is not under module %s/%s", key.Source, key.ID))
	}
	if _, dup := r.events[def.Type]; dup {
		return ledger.NewInvalid(def.Type, "duplicate event type")
	}
	if len(def.Codecs) == 0 {
		return ledger.NewInvalid(def.Type, "no codec for any payload version")
	}
	explicit := !def.Version.IsZero()
	for v, codec := range def.Codecs {
		if v.IsZero() || codec == nil {
			return ledger.NewInvalid(def.Type, "nil codec or zero payload version")
		}
		if v.Prerelease && len(def.Codecs) > 1 {
			return ledger.NewInvalid(def.Type,
				fmt.Sprintf("prerelease version %s beside other codecs; a prerelease shape keeps no history", v))

		}
		if !explicit && def.Version.Less(v) {
			def.Version = v
		}
	}
	if def.Codecs[def.Version] == nil {
		return ledger.NewInvalid(def.Type, fmt.Sprintf("write version %s has no codec", def.Version))
	}
	if def.Domain == "" {
		return ledger.NewInvalid(def.Type, "event declares no stream domain")
	}
	if se, declared := r.streams[def.Domain]; !declared || se.module != key {
		return ledger.NewInvalid(def.Type,
			fmt.Sprintf("event names stream domain %q, which module %s/%s does not declare", def.Domain, key.Source, key.ID))

	}
	for _, b := range def.Bindings {
		if err := b.validate(); err != nil {
			return ledger.NewInvalid(def.Type, err.Error())
		}
	}
	r.events[def.Type] = eventEntry{module: key, def: def}
	return nil
}

func (r *Registry) LookupEvent(typ ledger.EventType) (ModuleKey, EventDefinition, bool) {
	e, ok := r.events[typ]
	return e.module, e.def, ok
}

// LookupStream resolves a stream domain to the module that declared it and
// the declaration.
func (r *Registry) LookupStream(domain string) (ModuleKey, StreamDefinition, bool) {
	e, ok := r.streams[domain]
	return e.module, e.def, ok
}

// ModuleOf names the module an EventType belongs to by its
// <source>/<module>/ prefix; false when the prefix names no registered module.
func (r *Registry) ModuleOf(typ ledger.EventType) (ModuleKey, bool) {
	parts := strings.SplitN(string(typ), "/", 3)
	if len(parts) == 3 {
		key := ModuleKey{Source: SourceID(parts[0]), ID: ModuleID(parts[1])}
		if _, registered := r.modules[key]; registered {
			return key, true
		}
	}
	return ModuleKey{}, false
}

func (r *Registry) LookupProjection(id ProjectionID, v ProjectionVersion) (ProjectionDefinition, ModuleKey, bool) {
	e, ok := r.projections[projectionKey{id, v}]
	return e.def, e.module, ok
}

// Projections lists every registered projection with its owning module.
func (r *Registry) Projections() []ProjectionDefinition {
	out := make([]ProjectionDefinition, 0, len(r.projections))
	for k := range r.projections {
		out = append(out, r.projections[k].def)
	}
	return out
}

// ModulePrefix is the EventType prefix of one module: <source>/<module>/.
func ModulePrefix(source SourceID, id ModuleID) ledger.EventType {
	return ledger.EventType(fmt.Sprintf("%s/%s/", source, id))
}

// Encode validates value, encodes it with the codec of the event type's
// write Version and records that Version as the payload's `v` (EXT-REG-2).
func (r *Registry) Encode(typ ledger.EventType, value any) (jsonstable.Value, error) {
	_, def, ok := r.LookupEvent(typ)
	if !ok {
		return jsonstable.Value{}, ledger.NewUnknownEvent(typ, "")
	}
	codec := def.Codecs[def.Version]
	if err := codec.Validate(value); err != nil {
		return jsonstable.Value{}, ledger.NewCodec(typ, err.Error())
	}
	body, err := codec.Encode(value)
	if err != nil {
		return jsonstable.Value{}, ledger.NewCodec(typ, err.Error())
	}
	wire, err := addVersion(body, def.Version)
	if err != nil {
		return jsonstable.Value{}, ledger.NewCodec(typ, err.Error())
	}
	// The canonical Encode/Decode/Encode round trip is a module test
	// obligation (EXT-COD-1), not re-verified per Encode.
	return wire, nil
}

// Decode selects the codec by (EventType, v). Unknown types or versions are
// returned as Unknown with the raw payload retained (EXT-REG-3).
func (r *Registry) Decode(e ledger.Event) (DecodedEvent, error) {
	out := DecodedEvent{Event: e}
	module, def, ok := r.LookupEvent(e.Type)
	if !ok {
		out.Module, _ = r.ModuleOf(e.Type)
		out.Unknown = true
		return out, nil
	}
	out.Module = module
	body, v, err := splitVersion(e.Payload)
	if err != nil {
		return out, ledger.NewCodec(e.Type, err.Error())
	}
	out.Version = v
	codec := def.Codecs[v]
	if codec == nil {
		out.Unknown = true
		return out, nil
	}
	value, err := codec.Decode(body)
	if err != nil {
		return out, ledger.NewCodec(e.Type, err.Error())
	}
	out.Value = value
	return out, nil
}

// addVersion inserts the `v` field, the version's wire form, into the first
// level of body.
func addVersion(body jsonstable.Value, v PayloadVersion) (jsonstable.Value, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body.Bytes(), &m); err != nil {
		return jsonstable.Value{}, fmt.Errorf("payload is not an object: %w", err)
	}
	if m == nil {
		m = map[string]json.RawMessage{}
	}
	if _, has := m["v"]; has {
		return jsonstable.Value{}, errors.New("payload must not define its own \"v\" field")
	}
	vs, err := json.Marshal(v)
	if err != nil {
		return jsonstable.Value{}, err
	}
	m["v"] = vs
	return jsonstable.FromValue(m)
}

// splitVersion removes `v` and returns the codec-facing body.
func splitVersion(payload jsonstable.Value) (jsonstable.Value, PayloadVersion, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(payload.Bytes(), &m); err != nil {
		return jsonstable.Value{}, PayloadVersion{}, fmt.Errorf("payload is not an object: %w", err)
	}
	raw, ok := m["v"]
	if !ok {
		return jsonstable.Value{}, PayloadVersion{}, errors.New("payload has no \"v\" field")
	}
	var v PayloadVersion
	if err := json.Unmarshal(raw, &v); err != nil {
		return jsonstable.Value{}, PayloadVersion{}, fmt.Errorf("payload \"v\": %w", err)
	}
	delete(m, "v")
	body, err := jsonstable.FromValue(m)
	if err != nil {
		return jsonstable.Value{}, PayloadVersion{}, err
	}
	return body, v, nil
}

// JSONCodec is a PayloadCodec for a plain Go struct type T with json tags.
// Decode is strict: unknown fields, duplicate keys and trailing data are
// rejected by the canonical parse and DisallowUnknownFields.
type JSONCodec[T any] struct {
	// Check validates a decoded/encoded value; nil accepts every T.
	Check func(*T) error
}

func (c JSONCodec[T]) Validate(value any) error {
	v, ok := value.(T)
	if !ok {
		p, isPtr := value.(*T)
		if !isPtr || p == nil {
			return fmt.Errorf("value is %T, want %T", value, v)
		}
		v = *p
	}
	if c.Check != nil {
		return c.Check(&v)
	}
	return nil
}

func (c JSONCodec[T]) Encode(value any) (jsonstable.Value, error) {
	if err := c.Validate(value); err != nil {
		return jsonstable.Value{}, err
	}
	if p, ok := value.(*T); ok {
		value = *p
	}
	return jsonstable.FromValue(value)
}

func (c JSONCodec[T]) Decode(wire jsonstable.Value) (any, error) {
	var v T
	if err := StrictDecode(wire, &v); err != nil {
		return nil, err
	}
	if c.Check != nil {
		if err := c.Check(&v); err != nil {
			return nil, err
		}
	}
	return v, nil
}

// StrictDecode decodes canonical JSON into dst rejecting unknown fields.
func StrictDecode(wire jsonstable.Value, dst any) error {
	dec := json.NewDecoder(strings.NewReader(wire.String()))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing data after JSON value")
	}
	return nil
}

// --- errors -------------------------------------------------------------------
