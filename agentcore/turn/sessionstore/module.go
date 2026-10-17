// Package sessionstore is the Turn domain over a Session ledger: the
// twilight/turn/ event definitions, the twilight/turn/machine projection, the
// frozen-body store over the content store, and the TurnStore adapter
// (SessionTurnStore) that commits Turn commands through a Session Writer.
package sessionstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/wire"
)

const (
	ModuleID module.ModuleID = "turn"
	// Prefix is the EventType namespace of every Turn fact.
	Prefix ledger.EventType = "twilight/turn/"
	// StreamDomain is the stream domain of Turn facts: one keyed stream per
	// Turn, bound by the payload's turnId.
	StreamDomain = "turn"
)

// streamDefinition inherits Turn history by identity. Execution reads use
// module.Own explicitly; the active machine does not inherit ancestor facts.
var streamDefinition = module.StreamDefinition{Domain: StreamDomain, Key: streamKey, Inheritance: module.Inherited}

// streamKey binds a turn event to its Turn's stream.
func streamKey(value any) (string, error) {
	ev, ok := value.(Event)
	if !ok {
		return "", fmt.Errorf("value is %T, want sessionstore.Event", value)
	}
	return string(ev.TurnID), nil
}

// Stream is the logical stream of one Turn's facts.
func Stream(turnID turn.TurnID) ledger.Domain { return streamDefinition.Ref(string(turnID)) }

// factNames is the closed list of fact discriminators, from the Turn core's
// variant registry: a fact the core knows is a wire type this module
// registers, with no second list to keep in step.
var factNames = wire.FactTypes()

// EventType returns the EventType of a fact.
func EventType(f turn.Fact) ledger.EventType {
	return Prefix + ledger.EventType(wire.Facts{}.FactType(f))
}

// Event is the typed value of one twilight/turn/ event: the fact plus the
// TurnID that every payload carries at its first level.
type Event struct {
	TurnID turn.TurnID
	Fact   turn.Fact
}

// factCodec is the codec of one fact type at its current payload version:
// the wire the Turn core defines today. The payload is the canonical fact
// object with "turnId" added; `v` is the Registry's.
type factCodec struct {
	local string
	wire  wire.Facts
}

func (c factCodec) Validate(value any) error {
	ev, ok := value.(Event)
	if !ok {
		return fmt.Errorf("value is %T, want sessionstore.Event", value)
	}
	if ev.TurnID == "" || ev.Fact == nil {
		return errors.New("event requires turnId and fact")
	}
	if got := c.wire.FactType(ev.Fact); got != c.local {
		return fmt.Errorf("fact is %s, codec is %s", got, c.local)
	}
	if started, ok := ev.Fact.(turn.TurnStarted); ok {
		if started.TurnID != ev.TurnID {
			return errors.New("fact turnId disagrees with event turnId")
		}
		if err := turn.ValidateNewTurn(turn.NewTurn(started)); err != nil {
			return err
		}
	}
	return nil
}

func (c factCodec) Encode(value any) (jsonstable.Value, error) {
	if err := c.Validate(value); err != nil {
		return jsonstable.Value{}, err
	}
	ev, _ := value.(Event) // Validate checked the type
	raw, err := jsonstable.MarshalCanonical(ev.Fact)
	if err != nil {
		return jsonstable.Value{}, err
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return jsonstable.Value{}, err
	}
	if m == nil {
		m = map[string]json.RawMessage{}
	}
	if existing, has := m["turnId"]; has {
		// TurnStarted already carries turnId; it must agree.
		var id turn.TurnID
		if err := json.Unmarshal(existing, &id); err != nil || id != ev.TurnID {
			return jsonstable.Value{}, errors.New("fact turnId disagrees with event turnId")
		}
	}
	m["turnId"] = json.RawMessage(fmt.Sprintf("%q", string(ev.TurnID)))
	return jsonstable.FromValue(m)
}

func (c factCodec) Decode(w jsonstable.Value) (any, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(w.Bytes(), &m); err != nil {
		return nil, err
	}
	rawID, ok := m["turnId"]
	if !ok {
		return nil, errors.New("turn event has no turnId")
	}
	var id turn.TurnID
	if err := json.Unmarshal(rawID, &id); err != nil || id == "" {
		return nil, errors.New("turn event turnId is not a string")
	}
	if c.local != "turn_started" {
		delete(m, "turnId")
	}
	body, err := jsonstable.FromValue(m)
	if err != nil {
		return nil, err
	}
	fact, err := c.wire.DecodeFact(c.local, body.Bytes())
	if err != nil {
		return nil, err
	}
	if created, ok := fact.(turn.TurnStarted); ok && created.TurnID != id {
		return nil, errors.New("turn_started turnId disagrees with payload turnId")
	}
	ev := Event{TurnID: id, Fact: fact}
	if err := c.Validate(ev); err != nil {
		return nil, err
	}
	return ev, nil
}

// frozenBodyFacts are the fact types whose digest names a frozen body; each
// declares one artifact reference so the Writer admits the body's Binding
// and claims it for the commit.
var frozenBodyFacts = map[string]bool{
	"model_step_prepared": true, "model_step_completed": true, "tool_call_completed": true, "tool_call_answered": true,
}

var frozenBinding = module.BindingReferenceDefinition{
	Extractor:          module.BindingExtractorFunc(frozenRefs),
	Cardinality:        module.Cardinality{Min: 1, Max: &one},
	AllowedSchemes:     []artifact.Scheme{artifact.SchemeCAS},
	RequiredDurability: artifact.EventBound,
}

var one uint32 = 1

// Module registers Turn facts, the history surface, and the active machine.
var Module = buildModule()

// factCodecs is the codec history of one fact type:
// one PayloadCodec per payload version ever written, keyed by version, and
// the current version. older holds the superseded versions 1..n-1, each a
// codec of its own that decodes that version's wire shape and upcasts to
// the current turn.Fact; the current shape is version n, served by factCodec
// for the wire the Turn core defines today. Nothing is dropped: a fact type
// whose shape changes keeps its previous codec under its old version and
// moves up one version by itself. A history with a gap is a programming
// error in this module's own table and stops the build.
func factCodecs(name string, older map[module.PayloadVersion]module.PayloadCodec) (map[module.PayloadVersion]module.PayloadCodec, module.PayloadVersion) {
	codecs := make(map[module.PayloadVersion]module.PayloadCodec, len(older)+1)
	for v := module.PayloadVersion(1); int(v) <= len(older); v++ {
		c, ok := older[v]
		if !ok || c == nil {
			panic(fmt.Sprintf("sessionstore: fact %s: codec history has no version %d", name, v))
		}
		codecs[v] = c
	}
	if len(codecs) != len(older) {
		panic(fmt.Sprintf("sessionstore: fact %s: codec history is not contiguous from 1", name))
	}
	current := module.PayloadVersion(len(older) + 1) //nolint:gosec // G115: a handful of versions
	codecs[current] = factCodec{local: name, wire: wire.Facts{}}
	return codecs, current
}

// olderFactCodecs is the superseded payload versions of one fact type. No
// fact type has changed wire shape since it was first written, so every
// type is at version 1 with no history; the first change adds the codec of
// the shape it replaces here, under version 1, and keeps it for good.
func olderFactCodecs(string) map[module.PayloadVersion]module.PayloadCodec { return nil }

// eventDefinition is the EventDefinition of one fact type over its codec
// history.
func eventDefinition(name string, codecs map[module.PayloadVersion]module.PayloadCodec, current module.PayloadVersion) module.EventDefinition {
	def := module.EventDefinition{
		Type:    Prefix + ledger.EventType(name),
		Domain:  StreamDomain,
		Codecs:  codecs,
		Version: current,
	}
	if frozenBodyFacts[name] {
		def.Bindings = []module.BindingReferenceDefinition{frozenBinding}
	}
	return def
}

func buildModule() module.ModuleDescriptor {
	m := module.ModuleDescriptor{Source: module.SourceTwilight, ID: ModuleID,
		Streams: []module.StreamDefinition{streamDefinition}, Projections: []module.ProjectionDefinition{MachineProjection, SurfaceProjection}}
	for _, name := range factNames {
		codecs, current := factCodecs(name, olderFactCodecs(name))
		m.Events = append(m.Events, eventDefinition(name, codecs, current))
	}
	return m
}

// Type returns the EventType of one fact discriminator.
func Type(name string) ledger.EventType { return Prefix + ledger.EventType(name) }

// AllTypes lists every registered twilight/turn/ EventType.
func AllTypes() []ledger.EventType {
	out := make([]ledger.EventType, len(factNames))
	for i, name := range factNames {
		out[i] = Prefix + ledger.EventType(name)
	}
	return out
}
