package turn

import (
	"context"
	"fmt"
	"github.com/felinics/twilight/agentcore/session"
)

// ReadSurface loads the turn surface of one Session through r.
func ReadSurface(ctx context.Context, r session.ProjectionReader, sid session.SessionID) (TurnSurface, error) {
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
