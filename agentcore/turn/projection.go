package turn

import (
	"fmt"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
)

const SurfaceProjectionID module.ProjectionID = "twilight/turn/surface"

type TurnStatus string

const (
	TurnActive     TurnStatus = "active"
	TurnCompleted  TurnStatus = "completed"
	TurnFailed     TurnStatus = "failed"
	TurnStopped    TurnStatus = "stopped"
	TurnSuperseded TurnStatus = "superseded"
)

type TurnView struct {
	TurnID   TurnID            `json:"turnId"`
	Status   TurnStatus        `json:"status"`
	InputIDs []chatlog.InputID `json:"inputIds,omitempty"`
	Preset   preset.PresetRef  `json:"preset"`
	// RunID is the one Run that executes this Turn; End is its terminal
	// result from twilight/run/run_ended, nil while the Run is open.
	RunID run.RunID     `json:"runId"`
	End   *run.RunEnded `json:"end,omitempty"`
	// Settlement is the Turn's own settlement fact, when one was written
	// (Stop writes failed{stopped} beside the Run's cancellation); a Turn
	// settled by its Run's end alone has none.
	Settlement        Settlement `json:"settlement,omitempty"`
	FailureClass      string     `json:"failureClass,omitempty"`
	Reason            string     `json:"reason,omitempty"`
	ReplacementTurnID TurnID     `json:"replacementTurnId,omitempty"`
}

// Ended is the Run's terminal end, nil while it is open.
func (v *TurnView) Ended() run.RunEnd {
	if v == nil || v.End == nil {
		return nil
	}
	return v.End.End
}

type TurnSurface struct {
	Order []TurnID            `json:"order"`
	Turns map[TurnID]TurnView `json:"turns"`
	// RunOwner routes a Run to the Turn it executes, from twilight/turn/started
	// (TRN-PRJ-1).
	RunOwner map[run.RunID]TurnID `json:"runOwner"`
}

// OwnerOf is the Turn a Run executes.
func (s *TurnSurface) OwnerOf(runID run.RunID) (TurnID, bool) {
	turnID, ok := s.RunOwner[runID]
	return turnID, ok
}

func (s TurnSurface) clone() TurnSurface {
	out := TurnSurface{Order: append([]TurnID(nil), s.Order...), Turns: make(map[TurnID]TurnView, len(s.Turns)), RunOwner: make(map[run.RunID]TurnID, len(s.RunOwner))}
	for k := range s.Turns {
		v := s.Turns[k]
		v.InputIDs = append([]chatlog.InputID(nil), v.InputIDs...)
		out.Turns[k] = v
	}
	for k, v := range s.RunOwner {
		out.RunOwner[k] = v
	}
	return out
}

// Active returns the single active Turn of the Session, if any (TRN-SCP-2).
func (s *TurnSurface) Active() (TurnView, bool) {
	for _, id := range s.Order {
		if v := s.Turns[id]; v.Status == TurnActive {
			return v, true
		}
	}
	return TurnView{}, false
}

var SurfaceProjection = module.ProjectionDefinition{
	ID: SurfaceProjectionID, Version: 1,
	Consumes: []ledger.EventType{TypeStarted, TypeFailed, TypeSuperseded,
		chatlog.TypeInputDelivered, sessionstore.Prefix + "run_ended"},
	// Attempt settlement is folded from run_ended, so inherited Turns settle
	// from the parent's run streams, whose domain is of segment lineage and
	// would otherwise be skipped (EXT-PRJ-8); a fork point inside a Turn is
	// refused by the Owner (OWN-FRK-1).
	Inherits:      module.InheritAll,
	Authoritative: true,
	Initial: func() (any, error) {
		return TurnSurface{Turns: map[TurnID]TurnView{}, RunOwner: map[run.RunID]TurnID{}}, nil
	},
	Apply:      applySurface,
	StateCodec: module.JSONStateCodec[TurnSurface]{},
}

//nolint:gocritic // hugeParam: DecodedEvent is the extension Apply shape
func applySurface(state any, e module.DecodedEvent) (any, error) {
	prev, ok := state.(TurnSurface)
	if !ok {
		return nil, fmt.Errorf("turn surface: state is %T", state)
	}
	s := prev.clone()
	switch p := e.Value.(type) {
	case StartedPayload:
		if _, dup := s.Turns[p.TurnID]; dup {
			return nil, fmt.Errorf("turn %s started twice", p.TurnID)
		}
		if owner, dup := s.RunOwner[p.RunID]; dup {
			return nil, fmt.Errorf("run %s already executes turn %s", p.RunID, owner)
		}
		s.Order = append(s.Order, p.TurnID)
		s.Turns[p.TurnID] = TurnView{TurnID: p.TurnID, Status: TurnActive, InputIDs: append([]chatlog.InputID(nil), p.InputIDs...),
			Preset: p.Preset, RunID: p.RunID}
		s.RunOwner[p.RunID] = p.TurnID
	case sessionstore.Event:
		return s.applyRun(p)
	case FailedPayload:
		v, err := s.settling(p.TurnID)
		if err != nil {
			return nil, err
		}
		v.Settlement, v.FailureClass, v.Reason = p.Settlement, p.FailureClass, p.Reason
		if p.Settlement == SettlementStopped {
			v.Status = TurnStopped
		} else {
			v.Status = TurnFailed
		}
		s.Turns[p.TurnID] = v
	case SupersededPayload:
		v, err := s.settling(p.TurnID)
		if err != nil {
			return nil, err
		}
		if v.Status != TurnActive {
			return nil, fmt.Errorf("turn %s superseded while %s", p.TurnID, v.Status)
		}
		v.Settlement, v.Status, v.ReplacementTurnID = "", TurnSuperseded, p.ReplacementTurnID
		s.Turns[p.TurnID] = v
	case chatlog.InputDeliveredPayload:
		v, ok := s.Turns[TurnID(p.TurnID)]
		if !ok {
			return s, nil
		}
		for _, have := range v.InputIDs {
			if have == p.InputID {
				return s, nil
			}
		}
		v.InputIDs = append(v.InputIDs, p.InputID)
		s.Turns[TurnID(p.TurnID)] = v
	default:
		return nil, fmt.Errorf("turn surface: unexpected %T", e.Value)
	}
	return s, nil
}

// applyRun settles the Turn a Run executes: completed when the Run
// completed, failed otherwise; a Stop already settled it as stopped in the
// same commit and the end is only recorded. A Run owned by no Turn of this
// Session is not ours.
func (s TurnSurface) applyRun(ev sessionstore.Event) (any, error) {
	ended, ok := ev.Fact.(run.RunEnded)
	if !ok {
		return nil, fmt.Errorf("turn surface: unexpected run fact %T", ev.Fact)
	}
	turnID, ok := s.OwnerOf(ev.RunID)
	if !ok {
		return s, nil
	}
	v := s.Turns[turnID]
	if v.End != nil {
		return nil, fmt.Errorf("turn %s run %s ended twice", turnID, ev.RunID)
	}
	v.End = &ended
	if _, completed := ended.End.(run.RunCompletedEnd); completed {
		if v.Status != TurnActive {
			return nil, fmt.Errorf("turn %s completed while %s", turnID, v.Status)
		}
		v.Status = TurnCompleted
	} else if v.Status == TurnActive {
		v.Status = TurnFailed
	}
	s.Turns[turnID] = v
	return s, nil
}

func (s *TurnSurface) settling(id TurnID) (TurnView, error) {
	v, ok := s.Turns[id]
	if !ok {
		return TurnView{}, fmt.Errorf("turn %s settled before started", id)
	}
	// A Turn is settled once: while active, or right after its Run ended
	// without completing, when the unit that ended the Run (Stop) writes
	// the Turn's own settlement beside it (TRN-STP-1).
	if v.Status == TurnActive || (v.Status == TurnFailed && v.Settlement == "") {
		return v, nil
	}
	return TurnView{}, fmt.Errorf("turn %s settled twice", id)
}
