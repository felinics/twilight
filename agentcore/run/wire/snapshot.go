package wire

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model"
)

// StatesEquivalent compares two states via their canonical snapshot encoding.
func StatesEquivalent(a, b *run.MachineState) bool { return statesEquivalent(a, b) }

func statesEquivalent(a, b *run.MachineState) bool {
	ab, errA := encodeMachineState(a)
	bb, errB := encodeMachineState(b)
	if errA != nil || errB != nil {
		return false
	}
	return bytes.Equal(ab, bb)
}

// --- v1 snapshot --------------------------------------------------------------------

type Snapshot struct{}

// machineStateWire is the persisted snapshot shape of MachineState. It
// flattens the interface-typed Current into a discriminator
// plus at most one step body so the snapshot round-trips through JSON. Its
// canonical bytes are the InitialStateDigest preimage (RUN-NEW-1), so field
// names and omission rules are frozen with the schema.
type machineStateWire struct {
	RunID         run.RunID        `json:"runId"`
	Status        run.RunStatus    `json:"status"`
	ModelSteps    int              `json:"modelSteps"`
	Usage         model.Usage      `json:"usage"`
	PendingInputs []run.AgentInput `json:"pendingInputs"`
	Result        *run.RunResult   `json:"result"`
	LastToolStep  *run.ToolStep    `json:"lastToolStep,omitempty"`
	// Current is "open", "model" or "tool" for an active Run and absent for a
	// terminal one.
	Current   string         `json:"current,omitempty"`
	ModelStep *run.ModelStep `json:"modelStep,omitempty"`
	ToolStep  *run.ToolStep  `json:"toolStep,omitempty"`
}

const (
	currentWireOpen  = "open"
	currentWireModel = "model"
	currentWireTool  = "tool"
)

func machineStateToWire(s *run.MachineState) (machineStateWire, error) {
	w := machineStateWire{
		RunID: s.RunID, Status: s.Status, ModelSteps: s.ModelSteps,
		Usage: s.Usage, PendingInputs: s.PendingInputs,
		Result: s.Result, LastToolStep: s.LastToolStep,
	}
	switch cur := s.Current.(type) {
	case nil:
	case run.Open:
		w.Current = currentWireOpen
	case run.ModelStep:
		w.Current = currentWireModel
		w.ModelStep = &cur
	case run.ToolStep:
		w.Current = currentWireTool
		w.ToolStep = &cur
	default:
		return machineStateWire{}, fmt.Errorf("agent: snapshot: unknown current variant %T", s.Current)
	}
	return w, nil
}

func machineStateFromWire(w *machineStateWire) (run.MachineState, error) {
	s := run.MachineState{
		RunID: w.RunID, Status: w.Status, ModelSteps: w.ModelSteps,
		Usage: w.Usage, PendingInputs: w.PendingInputs,
		Result: w.Result, LastToolStep: w.LastToolStep,
	}
	switch w.Current {
	case "":
		if w.ModelStep != nil || w.ToolStep != nil {
			return run.MachineState{}, errors.New("agent: snapshot: step body without current discriminator")
		}
	case currentWireOpen:
		if w.ModelStep != nil || w.ToolStep != nil {
			return run.MachineState{}, errors.New("agent: snapshot: open state carries a step body")
		}
		s.Current = run.Open{}
	case currentWireModel:
		if w.ModelStep == nil || w.ToolStep != nil {
			return run.MachineState{}, errors.New("agent: snapshot: model current requires exactly a modelStep body")
		}
		s.Current = *w.ModelStep
	case currentWireTool:
		if w.ToolStep == nil || w.ModelStep != nil {
			return run.MachineState{}, errors.New("agent: snapshot: tool current requires exactly a toolStep body")
		}
		s.Current = *w.ToolStep
	default:
		return run.MachineState{}, fmt.Errorf("agent: snapshot: unknown current %q", w.Current)
	}
	return s, nil
}

// encodeMachineState renders the canonical v1 snapshot bytes.
func encodeMachineState(s *run.MachineState) ([]byte, error) {
	if s == nil {
		return nil, errors.New("agent: snapshot: nil state")
	}
	w, err := machineStateToWire(s)
	if err != nil {
		return nil, err
	}
	return es.MarshalCanonical(w)
}

// decodeMachineState parses v1 snapshot bytes, rejecting unknown fields,
// trailing data, and non-canonical-equivalent wire, then validates the
// structural invariants of the restored state.
func decodeMachineState(raw []byte) (run.MachineState, error) {
	var w machineStateWire
	if err := es.DecodeStrict(raw, &w); err != nil {
		return run.MachineState{}, fmt.Errorf("agent: snapshot: %w", err)
	}
	s, err := machineStateFromWire(&w)
	if err != nil {
		return run.MachineState{}, err
	}
	if err := requireCanonicalEquivalent(raw, w); err != nil {
		return run.MachineState{}, fmt.Errorf("agent: snapshot: %w", err)
	}
	if err := run.ValidateMachineState(&s); err != nil {
		return run.MachineState{}, err
	}
	return s, nil
}
