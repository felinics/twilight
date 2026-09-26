// Package extension is the Session Module Framework
// (docs/design/agent-session-extension.md): typed event codecs with
// payload versions, Binding admission, the in-process Writer that serializes
// every write and holds the idempotency index, and pure projections with an
// optional cache.
package extension

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
)

// SourceID, ModuleID and ModuleKey are the kernel's module identity types
// (session.ModuleKey), so extension slots on headers and commits can be
// keyed by module (SES-WIR-5).
type (
	SourceID          = session.SourceID
	ModuleID          = session.ModuleID
	ModuleKey         = session.ModuleKey
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

// PayloadVersion is the version of one event type's payload codec
// (SES-VER-1, EXT-REG-2). It belongs to the event type, not to the segment:
// every payload carries the version it was written under as `v`, the module
// keeps a codec for every version it ever published, and every codec of one
// type decodes to the module's current in-memory value (upcasting inside the
// codec), so consumers never see a version. The kernel reads none of this.
type PayloadVersion uint16

// EventDefinition declares one event type, the codec of every version it was
// ever written under, and the version new payloads are written with.
type EventDefinition struct {
	Type   session.EventType
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
	Stream string
}

// StreamDefinition declares one logical stream domain a module owns
// (EXT-STR-1): the boundary of the invariants its events keep. The kernel
// names no domain; every domain a Session writes is declared here by
// exactly one module, which is the only module allowed to append to it.
type StreamDefinition struct {
	// Domain is the StreamRef.Domain of every stream of the definition.
	Domain string
	// Key extracts, from a typed event value of this domain, the ID of the
	// stream it belongs to. Nil declares a singleton domain: one stream, no
	// ID. Non-nil declares a keyed domain whose streams are domain/<id>; the
	// Writer requires Key(value) to equal the batch's stream ID
	// (EXT-STR-1). The binding is a property of the module's Go values, not
	// of a payload field name.
	Key StreamKey
	// Lineage is how a fork reads the domain (SES-FRK-5):
	// LineageSession for state the child Session continues, LineageSegment
	// for history that stays with the segment that wrote it.
	Lineage session.StreamLineage
}

// StreamKey names the stream a typed event value belongs to; it fails for a
// value that is not one of the domain's event types.
type StreamKey func(value any) (string, error)

// Keyed reports whether the domain's streams carry an ID.
func (d StreamDefinition) Keyed() bool { return d.Key != nil }

// Ref names one stream of the domain; id is empty for a singleton.
func (d StreamDefinition) Ref(id string) session.StreamRef {
	return session.StreamRef{Domain: d.Domain, ID: id}
}

// ModuleRequirement declares that a module consumes another module's events
// (EXT-REG-4). Source is required: module identity is the (Source, ID)
// pair. Versions are not part of the handshake: the producer's codecs
// upcast every version to its current value, which is all a consumer sees.
type ModuleRequirement struct {
	Source SourceID
	Module ModuleID
	Events []session.EventType
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
	Stream session.StreamRef
	// Position is the event's ledger position; a fold fills it, a bare Decode
	// leaves it zero.
	Position session.Position
	Event    session.Event
	Module   ModuleKey
	Version  PayloadVersion
	Value    any
	Unknown  bool
}

// Registry is the immutable index built once at startup (EXT-REG-1).
type Registry struct {
	modules     map[ModuleKey]ModuleDescriptor
	streams     map[string]streamEntry
	events      map[session.EventType]eventEntry
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
			return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("extension module %q claims the %s source", m.ID, SourceTwilight)}
		}
		for _, p := range m.Projections {
			if p.Authoritative {
				return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("projection %q of extension %s/%s declares Authoritative; only trusted core modules may", p.ID, m.Source, m.ID)}
			}
		}
		modules = append(modules, *m)
	}
	r := &Registry{
		modules: make(map[ModuleKey]ModuleDescriptor), streams: make(map[string]streamEntry),
		events: make(map[session.EventType]eventEntry), projections: make(map[projectionKey]projectionEntry)}
	for i := range modules {
		m := &modules[i]
		if err := validSegment("source", string(m.Source)); err != nil {
			return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("module %q: %v", m.ID, err)}
		}
		if err := validSegment("module id", string(m.ID)); err != nil {
			return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("source %q: %v", m.Source, err)}
		}
		key := m.Key()
		if _, dup := r.modules[key]; dup {
			return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("duplicate module %s/%s", key.Source, key.ID)}
		}
		r.modules[key] = *m
		for _, sd := range m.Streams {
			if err := session.ValidateStreamRef(session.StreamRef{Domain: sd.Domain}); err != nil {
				return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("module %s/%s: %v", key.Source, key.ID, err)}
			}
			if prev, dup := r.streams[sd.Domain]; dup {
				return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("duplicate stream domain %q: declared by %s/%s and %s/%s",
					sd.Domain, prev.module.Source, prev.module.ID, key.Source, key.ID)}
			}
			if err := session.ValidateStreamLineage(sd.Lineage); err != nil {
				return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("stream domain %q: %v", sd.Domain, err)}
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
				return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("projection %q is incomplete", p.ID)}
			}
			// Refusing commits is a capability of trusted core modules, not
			// something a descriptor declares for itself (EXT-PRJ-9); the
			// extension path above already refused it, this guards the map.
			if p.Authoritative && !trusted[key] {
				return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("projection %q of module %s/%s declares Authoritative without trust", p.ID, m.Source, m.ID)}
			}
			k := projectionKey{p.ID, p.Version}
			if _, dup := r.projections[k]; dup {
				return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("duplicate projection %q v%d", p.ID, p.Version)}
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
			return &Error{Code: ErrInvalid, Detail: fmt.Sprintf("module requirement cycle through %s/%s", key.Source, key.ID)}
		case 2:
			return nil
		}
		state[key] = 1
		for _, req := range r.modules[key].Requires {
			if req.Source == "" {
				return &Error{Code: ErrInvalid, Detail: fmt.Sprintf("module %s/%s: requirement on %q has no source", key.Source, key.ID, req.Module)}
			}
			depKey := req.Key()
			dep, ok := r.modules[depKey]
			if !ok {
				return &Error{Code: ErrInvalid, Detail: fmt.Sprintf("module %s/%s requires unregistered module %s/%s", key.Source, key.ID, depKey.Source, depKey.ID)}
			}
			for _, typ := range req.Events {
				entry, ok := r.events[typ]
				if !ok || entry.module != dep.Key() {
					return &Error{Code: ErrInvalid, Type: typ, Detail: fmt.Sprintf("module %s/%s requires event not owned by %s/%s", key.Source, key.ID, depKey.Source, depKey.ID)}
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
				return &Error{Code: ErrInvalid, Type: typ, Detail: fmt.Sprintf("projection %q consumes unregistered event", k.id)}
			}
			if _, inScope := scope[entry.module]; !inScope {
				return &Error{Code: ErrInvalid, Type: typ, Detail: fmt.Sprintf("projection %q consumes event of module %s/%s outside its Requires", k.id, entry.module.Source, entry.module.ID)}
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
// non-zero version, a write Version that names one of them (defaulting to
// the highest), a stream domain the module declares, and valid bindings.
func (r *Registry) registerEvent(m *ModuleDescriptor, key ModuleKey, def EventDefinition) error {
	prefix := ModulePrefix(m.Source, m.ID)
	if !strings.HasPrefix(string(def.Type), string(prefix)) || len(def.Type) == len(prefix) {
		return &Error{Code: ErrInvalid, Type: def.Type, Detail: fmt.Sprintf("event type is not under module %s/%s", key.Source, key.ID)}
	}
	if _, dup := r.events[def.Type]; dup {
		return &Error{Code: ErrInvalid, Type: def.Type, Detail: "duplicate event type"}
	}
	if len(def.Codecs) == 0 {
		return &Error{Code: ErrInvalid, Type: def.Type, Detail: "no codec for any payload version"}
	}
	for v, codec := range def.Codecs {
		if v == 0 || codec == nil {
			return &Error{Code: ErrInvalid, Type: def.Type, Detail: "nil codec or zero payload version"}
		}
		if def.Version == 0 || v > def.Version && !explicitVersion(m.Events, def.Type) {
			def.Version = max(def.Version, v)
		}
	}
	if def.Codecs[def.Version] == nil {
		return &Error{Code: ErrInvalid, Type: def.Type, Detail: fmt.Sprintf("write version %d has no codec", def.Version)}
	}
	if def.Stream == "" {
		return &Error{Code: ErrInvalid, Type: def.Type, Detail: "event declares no stream domain"}
	}
	if se, declared := r.streams[def.Stream]; !declared || se.module != key {
		return &Error{Code: ErrInvalid, Type: def.Type,
			Detail: fmt.Sprintf("event names stream domain %q, which module %s/%s does not declare", def.Stream, key.Source, key.ID)}
	}
	for _, b := range def.Bindings {
		if err := b.validate(); err != nil {
			return &Error{Code: ErrInvalid, Type: def.Type, Detail: err.Error()}
		}
	}
	r.events[def.Type] = eventEntry{module: key, def: def}
	return nil
}

// explicitVersion reports whether the module declared a write Version for
// typ, in which case Build leaves it alone.
func explicitVersion(events []EventDefinition, typ session.EventType) bool {
	for i := range events {
		if events[i].Type == typ {
			return events[i].Version != 0
		}
	}
	return false
}

func (r *Registry) LookupEvent(typ session.EventType) (ModuleKey, EventDefinition, bool) {
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
func (r *Registry) ModuleOf(typ session.EventType) (ModuleKey, bool) {
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
func ModulePrefix(source SourceID, id ModuleID) session.EventType {
	return session.EventType(fmt.Sprintf("%s/%s/", source, id))
}

// Encode validates value, encodes it with the codec of the event type's
// write Version and records that Version as the payload's `v` (EXT-REG-2).
func (r *Registry) Encode(typ session.EventType, value any) (jsonstable.Value, error) {
	_, def, ok := r.LookupEvent(typ)
	if !ok {
		return jsonstable.Value{}, &Error{Code: ErrUnknownEvent, Type: typ}
	}
	codec := def.Codecs[def.Version]
	if err := codec.Validate(value); err != nil {
		return jsonstable.Value{}, &Error{Code: ErrCodec, Type: typ, Detail: err.Error()}
	}
	body, err := codec.Encode(value)
	if err != nil {
		return jsonstable.Value{}, &Error{Code: ErrCodec, Type: typ, Detail: err.Error()}
	}
	wire, err := addVersion(body, def.Version)
	if err != nil {
		return jsonstable.Value{}, &Error{Code: ErrCodec, Type: typ, Detail: err.Error()}
	}
	// The canonical Encode/Decode/Encode round trip is a module test
	// obligation (EXT-COD-1), not re-verified per Encode.
	return wire, nil
}

// Decode selects the codec by (EventType, v). Unknown types or versions are
// returned as Unknown with the raw payload retained (EXT-REG-3).
func (r *Registry) Decode(e session.Event) (DecodedEvent, error) {
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
		return out, &Error{Code: ErrCodec, Type: e.Type, Detail: err.Error()}
	}
	out.Version = v
	codec := def.Codecs[v]
	if codec == nil {
		out.Unknown = true
		return out, nil
	}
	value, err := codec.Decode(body)
	if err != nil {
		return out, &Error{Code: ErrCodec, Type: e.Type, Detail: err.Error()}
	}
	out.Value = value
	return out, nil
}

// addVersion inserts the integer `v` field into the first level of body.
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
	m["v"] = json.RawMessage(fmt.Sprintf("%d", v))
	return jsonstable.FromValue(m)
}

// splitVersion removes `v` and returns the codec-facing body.
func splitVersion(payload jsonstable.Value) (jsonstable.Value, PayloadVersion, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(payload.Bytes(), &m); err != nil {
		return jsonstable.Value{}, 0, fmt.Errorf("payload is not an object: %w", err)
	}
	raw, ok := m["v"]
	if !ok {
		return jsonstable.Value{}, 0, errors.New("payload has no \"v\" field")
	}
	var v uint16
	if err := json.Unmarshal(raw, &v); err != nil || v == 0 {
		return jsonstable.Value{}, 0, errors.New("payload \"v\" is not a positive integer")
	}
	delete(m, "v")
	body, err := jsonstable.FromValue(m)
	if err != nil {
		return jsonstable.Value{}, 0, err
	}
	return body, PayloadVersion(v), nil
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

type ErrorCode string

const (
	ErrInvalid       ErrorCode = "invalid"
	ErrUnknownEvent  ErrorCode = "unknown_event"
	ErrCodec         ErrorCode = "codec"
	ErrBinding       ErrorCode = "binding"
	ErrConflict      ErrorCode = "conflict"
	ErrOwnershipLost ErrorCode = "ownership_lost"
	// ErrProjectionUnhealthy: a derived projection failed to fold a commit
	// the Writer applied; its state is frozen at the last good commit until
	// the Writer reopens and rebuilds it (EXT-PRJ-9).
	ErrProjectionUnhealthy ErrorCode = "projection_unhealthy"
	// ErrUnknownOutcome: an Append failed in a way that leaves what reached
	// the log unknown (an IO error, or the kernel's ErrHandleFailed). The
	// Writer's head and projections may no longer match the log, so it fails
	// closed; the host reopens and replays (EXT-WRT-4).
	ErrUnknownOutcome ErrorCode = "unknown_outcome"
)

type Error struct {
	Code   ErrorCode
	Type   session.EventType
	Detail string
}

func (e *Error) Error() string {
	s := "extension: " + string(e.Code)
	if e.Type != "" {
		s += " " + string(e.Type)
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}
