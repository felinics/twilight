package sessionkernel

import (
	"fmt"

	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
)

// surfaceOf loads the turn surface from a commit View.
func surfaceOf(v writer.View) (sessionstore.TurnSurface, error) {
	state, err := v.Projection(sessionstore.SurfaceProjectionID, sessionstore.SurfaceProjection.Version)
	if err != nil {
		return sessionstore.TurnSurface{}, err
	}
	surface, ok := state.(sessionstore.TurnSurface)
	if !ok {
		return sessionstore.TurnSurface{}, fmt.Errorf("sessionkernel: surface projection is %T", state)
	}
	return surface, nil
}

// RequireNoActiveTurn is the quiescence precondition for a commit of another
// domain, evaluated inside the Writer's critical section: turn.ErrConflict
// while a Turn is active.
func RequireNoActiveTurn(v writer.View) error {
	surface, err := surfaceOf(v)
	if err != nil {
		return err
	}
	if active, ok := surface.Active(); ok {
		return fmt.Errorf("%w: turn %s is active", turn.ErrConflict, active.TurnID)
	}
	return nil
}

// RequireQuiescentTurn is the guard that also admits a point between the
// steps of a Turn: no active Turn, or the active Turn's Turn is Open, or it
// is in a ToolStep with no Executing call. A model step Prepared or
// Executing, or a tool call Executing, is turn.ErrConflict: the context such
// a step reads or settles into must not move under it. Pending and Waiting
// calls are fine; the closure rule keeps their assistant.
func RequireQuiescentTurn(v writer.View) error {
	surface, err := surfaceOf(v)
	if err != nil {
		return err
	}
	active, ok := surface.Active()
	if !ok {
		return nil
	}
	mstate, err := v.Projection(sessionstore.MachineProjectionID, sessionstore.MachineProjection.Version)
	if err != nil {
		return err
	}
	machine, ok := mstate.(sessionstore.Machine)
	if !ok {
		return fmt.Errorf("sessionkernel: machine projection is %T", mstate)
	}
	state, ok := machine.Active[active.TurnID]
	if !ok {
		return nil // no live Turn: nothing is mid-step
	}
	switch state.Current.(type) {
	case turn.Open:
		return nil
	case turn.ToolStep:
		if calls := turn.ExecutingCalls(state); len(calls) > 0 {
			return fmt.Errorf("%w: turn %s has %d executing tool call(s)", turn.ErrConflict, active.TurnID, len(calls))
		}
		return nil
	default:
		return fmt.Errorf("%w: turn %s is in a model step", turn.ErrConflict, active.TurnID)
	}
}
