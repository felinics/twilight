package sessionstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/store"
	"github.com/felinics/twilight/agentcore/turn/wire"
	"unicode/utf8"
)

const MachineProjectionID module.ProjectionID = "twilight/turn/machine"

// Machine is the twilight/turn/machine projection state: every
// non-terminal Turn of the Session with its last event position and schema.
// Terminal Turns leave the projection entirely; Record and the turn surface
// keep their results, and a second creation of a used TurnID is refused at
// commit time from the ledger's stream index (CreateTurn), so the projection
// is bounded by the Turns active now.
type Machine struct {
	Active    map[turn.TurnID]turn.MachineState
	Positions map[turn.TurnID]turn.TurnPosition
}

func newMachine() Machine {
	return Machine{Active: map[turn.TurnID]turn.MachineState{}, Positions: map[turn.TurnID]turn.TurnPosition{}}
}

func (m Machine) clone() Machine {
	out := newMachine()
	for k := range m.Active {
		out.Active[k] = m.Active[k]
	}
	for k, v := range m.Positions {
		out.Positions[k] = v
	}
	return out
}

// snapshot returns the store.Snapshot of an active Turn.
func (m Machine) snapshot(turnID turn.TurnID) (store.Snapshot, bool) {
	ms, ok := m.Active[turnID]
	if !ok {
		return store.Snapshot{}, false
	}
	return store.Snapshot{State: ms, Position: m.Positions[turnID]}, true
}

// Apply folds one decoded Turn event (via Protocol.Evolve).
func (m Machine) Apply(e module.DecodedEvent) (Machine, error) { //nolint:gocritic // hugeParam: DecodedEvent is the extension Apply shape
	ev, ok := e.Value.(Event)
	if !ok {
		return m, fmt.Errorf("turn machine: unexpected %T", e.Value)
	}
	if want := Stream(ev.TurnID); e.Domain != want {
		return m, fmt.Errorf("turn machine: fact for %s arrived via stream %s", ev.TurnID, e.Domain)
	}
	out := m.clone()
	var state turn.MachineState
	if _, isCreated := ev.Fact.(turn.TurnStarted); isCreated {
		if _, dup := out.Active[ev.TurnID]; dup {
			return m, fmt.Errorf("turn machine: %s created twice", ev.TurnID)
		}
	} else {
		cur, active := out.Active[ev.TurnID]
		if !active {
			return m, fmt.Errorf("turn machine: fact %s for unknown or terminal turn %s", wire.FactType(ev.Fact), ev.TurnID)
		}
		state = cur
	}
	next, err := schema.Machine().Evolve(state, ev.Fact)
	if err != nil {
		return m, err
	}
	if next.Status.Terminal() {
		delete(out.Active, ev.TurnID)
		delete(out.Positions, ev.TurnID)
		return out, nil
	}
	if _, tracked := out.Positions[ev.TurnID]; tracked {
		out.Positions[ev.TurnID]++
	} else {
		out.Positions[ev.TurnID] = 0
	}
	out.Active[ev.TurnID] = next
	return out, nil
}

type machineWire struct {
	Turns map[turn.TurnID]machineTurnWire `json:"turns"`
}

type machineTurnWire struct {
	Position turn.TurnPosition `json:"position"`
	State    jsonstable.Value  `json:"state"`
}

type machineCodec struct{}

func (machineCodec) Validate(value any) error {
	if _, ok := value.(Machine); !ok {
		return fmt.Errorf("state is %T, want sessionstore.Machine", value)
	}
	return nil
}

func (c machineCodec) Encode(value any) (jsonstable.Value, error) {
	if err := c.Validate(value); err != nil {
		return jsonstable.Value{}, err
	}
	m, _ := value.(Machine) // Validate checked the type
	w := machineWire{Turns: make(map[turn.TurnID]machineTurnWire, len(m.Active))}
	for id := range m.Active {
		state := m.Active[id]
		raw, err := schema.Snapshot().Encode(&state)
		if err != nil {
			return jsonstable.Value{}, err
		}
		encoded, err := jsonstable.Parse(raw)
		if err != nil {
			return jsonstable.Value{}, err
		}
		w.Turns[id] = machineTurnWire{Position: m.Positions[id], State: encoded}
	}
	return jsonstable.FromValue(w)
}

func (machineCodec) Decode(wr jsonstable.Value) (any, error) {
	if wr.IsZero() {
		return nil, errors.New("empty machine snapshot")
	}
	var w machineWire
	if err := json.Unmarshal(wr.Bytes(), &w); err != nil {
		return nil, err
	}
	m := newMachine()
	for id, r := range w.Turns {
		state, err := schema.Snapshot().Decode(r.State.Bytes())
		if err != nil {
			return nil, err
		}
		m.Active[id] = state
		m.Positions[id] = r.Position
	}
	return m, nil
}

// MachineProjection consumes every twilight/turn/ event.
var MachineProjection = module.ProjectionDefinition{
	ID: MachineProjectionID, Version: 1,
	Consumes: AllTypes(), RequiredForCommit: true,
	InheritanceOverride: module.InheritNone,
	Initial:             func() (any, error) { return newMachine(), nil },
	Apply: func(state any, e module.DecodedEvent) (any, error) {
		m, ok := state.(Machine)
		if !ok {
			return nil, fmt.Errorf("turn machine: state is %T", state)
		}
		return m.Apply(e)
	},
	StateCodec: machineCodec{},
	CloneState: cloneMachine,
}

var _ ledger.EventType = Prefix

// WriterCachePolicy is this module's use of the projection cache for the
// Writer: every projection at the deployment's interval, except the machine
// projection, whose entry the SessionTurnStore refreshes itself through SnapshotPolicy
// and which must never be cached mid-step. every is the interval in
// commits; zero or less takes module.DefaultCacheEvery.
func WriterCachePolicy(every ledger.CommitSeq) module.CachePolicy {
	return module.CacheEvery(every).Exclude(MachineProjectionID)
}

// cloneMachine only copies active turns, like machineCodec's wire round trip.
// Positions without a corresponding active turn are not part of that encoding.
func cloneMachine(value any) (any, error) {
	codec := machineCodec{}
	if err := codec.Validate(value); err != nil {
		return nil, err
	}
	m, _ := value.(Machine) // Validate checked the type
	// The outer codec also normalizes map keys and positions. Preserve that
	// behavior for unusual programmatically constructed states.
	for id := range m.Active {
		if !utf8.ValidString(string(id)) || m.Positions[id] >= 1<<53 {
			encoded, err := codec.Encode(value)
			if err != nil {
				return nil, err
			}
			return codec.Decode(encoded)
		}
	}
	out := newMachine()
	for id := range m.Active {
		state := m.Active[id]
		detached, err := schema.Snapshot().Clone(&state)
		if err != nil {
			return nil, err
		}
		out.Active[id] = detached
		out.Positions[id] = m.Positions[id]
	}
	return out, nil
}
