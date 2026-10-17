package sessionstore

import (
	"context"
	"fmt"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset/ref"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
)

// TurnRef addresses a logical Turn within its Session history.
type TurnRef struct {
	SessionID session.SessionID
	TurnID    turn.TurnID
}

const SurfaceProjectionID module.ProjectionID = "twilight/turn/surface"
const TypeStarted ledger.EventType = Prefix + "turn_started"

// TurnView is the conversation-facing fold of a Turn's authoritative facts.
type TurnView struct {
	TurnID   turn.TurnID     `json:"turnId"`
	Status   turn.TurnStatus `json:"status"`
	InputIDs []turn.InputID  `json:"inputIds,omitempty"`
	Preset   ref.Ref         `json:"preset"`
	End      *turn.TurnEnded `json:"end,omitempty"`
}

func (v *TurnView) Ended() turn.TurnEnd {
	if v == nil || v.End == nil {
		return nil
	}
	return v.End.End
}

type TurnSurface struct {
	Order []turn.TurnID            `json:"order"`
	Turns map[turn.TurnID]TurnView `json:"turns"`
}

func (s *TurnSurface) Active() (TurnView, bool) {
	for _, id := range s.Order {
		if v := s.Turns[id]; v.Status == turn.TurnActive {
			return v, true
		}
	}
	return TurnView{}, false
}

// History inherits facts; execution's MachineProjection explicitly does not.
var SurfaceProjection = module.ProjectionDefinition{
	ID: SurfaceProjectionID, Version: 1,
	Consumes: []ledger.EventType{TypeStarted, Prefix + "input_accepted", Prefix + "turn_ended"},
	Inherits: module.InheritAll, Authoritative: true,
	Initial:    func() (any, error) { return TurnSurface{Turns: map[turn.TurnID]TurnView{}}, nil },
	StateCodec: module.JSONStateCodec[TurnSurface]{},
	Apply:      applySurface,
}

func applySurface(state any, e module.DecodedEvent) (any, error) {
	prev, ok := state.(TurnSurface)
	if !ok {
		return nil, fmt.Errorf("turn surface: state is %T", state)
	}
	ev, ok := e.Value.(Event)
	if !ok {
		return nil, fmt.Errorf("turn surface: event is %T", e.Value)
	}
	out := TurnSurface{Order: prev.Order, Turns: make(map[turn.TurnID]TurnView, len(prev.Turns)+1)}
	for id, v := range prev.Turns {
		out.Turns[id] = v
	}
	v, exists := out.Turns[ev.TurnID]
	switch f := ev.Fact.(type) {
	case turn.TurnStarted:
		if exists || f.TurnID != ev.TurnID {
			return nil, fmt.Errorf("turn %s started twice or with a different identity", ev.TurnID)
		}
		v = TurnView{TurnID: f.TurnID, Preset: f.Preset, Status: turn.TurnActive}
		out.Order = append(append([]turn.TurnID(nil), prev.Order...), ev.TurnID)
	case turn.InputAccepted:
		if !exists || v.Status.Terminal() {
			return nil, fmt.Errorf("turn %s accepted input outside active lifetime", ev.TurnID)
		}
		for _, id := range v.InputIDs {
			if id == f.Input.ID {
				return nil, fmt.Errorf("turn %s accepted input %s twice", ev.TurnID, id)
			}
		}
		v.InputIDs = append(append([]turn.InputID(nil), v.InputIDs...), f.Input.ID)
	case turn.TurnEnded:
		if !exists || v.Status.Terminal() {
			return nil, fmt.Errorf("turn %s ended outside active lifetime", ev.TurnID)
		}
		v.End = &f
		switch f.End.(type) {
		case turn.TurnCompletedEnd:
			v.Status = turn.TurnCompleted
		case turn.TurnStoppedEnd:
			v.Status = turn.TurnStopped
		case turn.TurnFailedEnd:
			v.Status = turn.TurnFailed
		default:
			return nil, fmt.Errorf("turn surface: unknown end %T", f.End)
		}
	default:
		return nil, fmt.Errorf("turn surface: unexpected fact %T", ev.Fact)
	}
	out.Turns[ev.TurnID] = v
	return out, nil
}

func ReadSurface(ctx context.Context, r session.ProjectionReader, sid session.SessionID) (TurnSurface, error) {
	state, _, err := r.Load(ctx, sid, SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return TurnSurface{}, err
	}
	surface, ok := state.(TurnSurface)
	if !ok {
		return TurnSurface{}, fmt.Errorf("turn surface: state is %T", state)
	}
	return surface, nil
}
