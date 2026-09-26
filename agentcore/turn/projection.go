package turn

import (
	"fmt"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/attempt"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
)

const SurfaceProjectionID extension.ProjectionID = "twilight/turn/surface"

type TurnStatus string

const (
	TurnActive        TurnStatus = "active"
	TurnAttemptFailed TurnStatus = "attempt_failed"
	TurnCompleted     TurnStatus = "completed"
	TurnFailed        TurnStatus = "failed"
	TurnStopped       TurnStatus = "stopped"
	TurnSuperseded    TurnStatus = "superseded"
)

type AttemptView struct {
	RunID   run.RunID `json:"runId"`
	Attempt uint32    `json:"attempt"`
	// End is the terminal result from twilight/run/run_ended; nil while active.
	End *run.RunEnded `json:"end,omitempty"`
}

// Ended returns the RunEnd variant, or nil for a non-terminal attempt.
func (a *AttemptView) Ended() run.RunEnd {
	if a == nil || a.End == nil {
		return nil
	}
	return a.End.End
}

type TurnView struct {
	TurnID            TurnID            `json:"turnId"`
	Status            TurnStatus        `json:"status"`
	InputIDs          []chatlog.InputID `json:"inputIds,omitempty"`
	Preset            PresetRef         `json:"preset"`
	Attempts          []AttemptView     `json:"attempts,omitempty"`
	ActiveRun         run.RunID         `json:"activeRun,omitempty"`
	ReplacementTurnID TurnID            `json:"replacementTurnId,omitempty"`
}

// LastAttempt returns the most recent attempt, if any.
func (v *TurnView) LastAttempt() *AttemptView {
	if len(v.Attempts) == 0 {
		return nil
	}
	return &v.Attempts[len(v.Attempts)-1]
}

// ActiveAttempt returns the attempt behind ActiveRun.
func (v *TurnView) ActiveAttempt() *AttemptView {
	for i := range v.Attempts {
		if v.Attempts[i].RunID == v.ActiveRun && v.ActiveRun != "" {
			return &v.Attempts[i]
		}
	}
	return nil
}

type TurnSurface struct {
	Order []TurnID            `json:"order"`
	Turns map[TurnID]TurnView `json:"turns"`
	// RunOwner maps each active Run to its Turn, from attempt/started until
	// run_ended, so a settlement is routed to the attempt it ends
	// (TRN-PRJ-1). An ended Run leaves it: its attempt keeps the result, and
	// OwnerOf still answers for it from the attempts.
	RunOwner map[run.RunID]TurnID `json:"runOwner"`
}

// OwnerOf returns the Turn a Run belongs to: the active table first, then
// the attempts of every Turn for a Run that already ended.
func (s *TurnSurface) OwnerOf(runID run.RunID) (TurnID, bool) {
	if turnID, ok := s.RunOwner[runID]; ok {
		return turnID, true
	}
	for _, id := range s.Order {
		for _, att := range s.Turns[id].Attempts {
			if att.RunID == runID {
				return id, true
			}
		}
	}
	return "", false
}

func (s TurnSurface) clone() TurnSurface {
	out := TurnSurface{Order: append([]TurnID(nil), s.Order...), Turns: make(map[TurnID]TurnView, len(s.Turns)), RunOwner: make(map[run.RunID]TurnID, len(s.RunOwner))}
	for k := range s.Turns {
		v := s.Turns[k]
		v.InputIDs = append([]chatlog.InputID(nil), v.InputIDs...)
		v.Attempts = append([]AttemptView(nil), v.Attempts...)
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

var SurfaceProjection = extension.ProjectionDefinition{
	ID: SurfaceProjectionID, Version: 1,
	Consumes: []session.EventType{TypeStarted, TypeFailed, TypeSuperseded,
		attempt.TypeStarted, chatlog.TypeInputDelivered, runmod.Prefix + "run_ended"},
	// Attempt settlement is folded from run_ended, so inherited Turns settle
	// from the parent's run streams, whose domain is of segment lineage and
	// would otherwise be skipped (EXT-PRJ-8); a fork point inside a Turn is
	// refused by the Owner (OWN-FRK-1).
	Inherits:      extension.InheritAll,
	Authoritative: true,
	Initial: func() (any, error) {
		return TurnSurface{Turns: map[TurnID]TurnView{}, RunOwner: map[run.RunID]TurnID{}}, nil
	},
	Apply:      applySurface,
	StateCodec: extension.JSONStateCodec[TurnSurface]{},
}

//nolint:gocritic // hugeParam: DecodedEvent is the extension Apply shape
func applySurface(state any, e extension.DecodedEvent) (any, error) {
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
		s.Order = append(s.Order, p.TurnID)
		s.Turns[p.TurnID] = TurnView{TurnID: p.TurnID, Status: TurnActive, InputIDs: append([]chatlog.InputID(nil), p.InputIDs...),
			Preset: p.Preset}
	case runmod.Event:
		return s.applyRun(p)
	case FailedPayload:
		v, err := s.settling(p.TurnID)
		if err != nil {
			return nil, err
		}
		v.ActiveRun = ""
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
		v.Status, v.ActiveRun, v.ReplacementTurnID = TurnSuperseded, "", p.ReplacementTurnID
		s.Turns[p.TurnID] = v
	case attempt.StartedPayload:
		turnID := TurnID(p.TurnID)
		v, ok := s.Turns[turnID]
		if !ok {
			return nil, fmt.Errorf("turn %s attempt before started", turnID)
		}
		if v.ActiveRun != "" {
			return nil, fmt.Errorf("turn %s already has active run %s", turnID, v.ActiveRun)
		}
		v.Attempts = append(v.Attempts, AttemptView{RunID: p.RunID, Attempt: p.Attempt})
		v.ActiveRun, v.Status = p.RunID, TurnActive
		s.Turns[turnID] = v
		s.RunOwner[p.RunID] = turnID
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

// applyRun settles the attempt a run_ended fact ends (TRN-PRJ-1): a
// completed Run completes the Turn; any other end leaves the Turn
// attempt_failed until Retry, Stop or Settle decide. A Run owned by no Turn
// of this Session is not ours.
func (s TurnSurface) applyRun(ev runmod.Event) (any, error) {
	ended, ok := ev.Fact.(run.RunEnded)
	if !ok {
		return nil, fmt.Errorf("turn surface: unexpected run fact %T", ev.Fact)
	}
	turnID, ok := s.OwnerOf(ev.RunID)
	if !ok {
		return s, nil
	}
	v := s.Turns[turnID]
	if err := v.end(ev.RunID, &ended); err != nil {
		return nil, err
	}
	delete(s.RunOwner, ev.RunID)
	v.ActiveRun = ""
	if _, completed := ended.End.(run.RunCompletedEnd); completed {
		if v.Status != TurnActive {
			return nil, fmt.Errorf("turn %s completed while %s", turnID, v.Status)
		}
		v.Status = TurnCompleted
	} else if v.Status == TurnActive {
		v.Status = TurnAttemptFailed
	}
	s.Turns[turnID] = v
	return s, nil
}

// end records the terminal result of the attempt behind runID. A settlement
// naming a Run the Turn never started, or an attempt that already ended, is a
// fold error rather than a silent no-op: the surface would otherwise report
// the Turn settled with no attempt carrying the result.
func (v *TurnView) end(runID run.RunID, result *run.RunEnded) error {
	for i := range v.Attempts {
		if v.Attempts[i].RunID != runID {
			continue
		}
		if v.Attempts[i].End != nil {
			return fmt.Errorf("turn %s attempt %s ended twice", v.TurnID, runID)
		}
		v.Attempts[i].End = result
		return nil
	}
	return fmt.Errorf("turn %s has no attempt %s", v.TurnID, runID)
}

func (s *TurnSurface) settling(id TurnID) (TurnView, error) {
	v, ok := s.Turns[id]
	if !ok {
		return TurnView{}, fmt.Errorf("turn %s settled before started", id)
	}
	if v.Status != TurnActive && v.Status != TurnAttemptFailed {
		return TurnView{}, fmt.Errorf("turn %s settled twice", id)
	}
	return v, nil
}
