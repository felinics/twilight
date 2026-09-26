package turn

import (
	"context"
	"fmt"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// ReadSurface loads the turn surface of one Session through r.
func ReadSurface(ctx context.Context, r extension.ProjectionReader, sid session.SessionID) (TurnSurface, error) {
	state, _, err := r.Load(ctx, sid, SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return TurnSurface{}, err
	}
	surface, ok := state.(TurnSurface)
	if !ok {
		return TurnSurface{}, fmt.Errorf("turn: surface projection is %T", state)
	}
	return surface, nil
}

// RequireNoActiveTurn is the turn layer's quiescence precondition for a
// commit of another domain, evaluated inside the Writer's critical section:
// ErrConflict while a Turn is active (APP-CKP-1).
func RequireNoActiveTurn(v writer.View) error {
	surface, err := surfaceOf(v)
	if err != nil {
		return err
	}
	if active, ok := surface.Active(); ok {
		return fmt.Errorf("%w: turn %s is active", ErrConflict, active.TurnID)
	}
	return nil
}

// RequireQuiescentRun is the compaction guard that also admits a point
// between the steps of a Turn (APP-CKP-1): no active Turn, or the active
// Turn's Run is Open, or it is in a ToolStep with no Executing call. A
// model step Prepared or Executing, or a tool call Executing, is
// ErrConflict: the context such a step reads or settles into must not move
// under it. Pending and Waiting calls are fine; the closure rule keeps
// their assistant (APP-CKP-2).
func RequireQuiescentRun(v writer.View) error {
	surface, err := surfaceOf(v)
	if err != nil {
		return err
	}
	active, ok := surface.Active()
	if !ok {
		return nil
	}
	mstate, err := v.Projection(runmod.MachineProjectionID, runmod.MachineProjection.Version)
	if err != nil {
		return err
	}
	machine, ok := mstate.(runmod.Machine)
	if !ok {
		return fmt.Errorf("turn: machine projection is %T", mstate)
	}
	state, ok := machine.Active[active.ActiveRun]
	if !ok {
		return nil // no live Run: nothing is mid-step
	}
	switch state.Current.(type) {
	case run.Open:
		return nil
	case run.ToolStep:
		if calls := plan.ExecutingCalls(state); len(calls) > 0 {
			return fmt.Errorf("%w: turn %s has %d executing tool call(s)", ErrConflict, active.TurnID, len(calls))
		}
		return nil
	default:
		return fmt.Errorf("%w: turn %s is in a model step", ErrConflict, active.TurnID)
	}
}

func surfaceOf(v writer.View) (TurnSurface, error) {
	state, err := v.Projection(SurfaceProjectionID, SurfaceProjection.Version)
	if err != nil {
		return TurnSurface{}, err
	}
	surface, ok := state.(TurnSurface)
	if !ok {
		return TurnSurface{}, fmt.Errorf("turn: surface projection is %T", state)
	}
	return surface, nil
}
