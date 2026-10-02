package app

import (
	"context"
	"fmt"

	"github.com/felinics/twilight/agentcore/execution"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
)

// Owned is one acquired Session without a conversation over it: the
// ownership Handle and the outcome of the takeover that ran when it was
// acquired. It is what a caller that drives through the Handle itself holds.
type Owned struct {
	// Handle is the ownership capability every command commits through.
	Handle *owner.Handle
	// Recovered is the number of recovery commands the takeover issued.
	Recovered int
	app       *Application
}

// Acquire takes ownership of the Session and runs the takeover disposition;
// the waits a previous owner left are answered in the background and
// reported through the engine's notice. Close releases it.
func (app *Application) Acquire(ctx context.Context, sid session.SessionID) (*Owned, error) {
	h, err := app.owner.Open(ctx, sid)
	if err != nil {
		return nil, err
	}
	n, err := app.engine.Takeover(ctx, h.Writer())
	if err != nil {
		app.engine.Detach(sid)
		_ = h.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	return &Owned{Handle: h, Recovered: n, app: app}, nil
}

// resumeWaiting answers, under the engine, the waits a previous owner left
// open. It runs after the Session is reachable by the notice its answers
// send.
func (o *Owned) resumeWaiting(ctx context.Context) {
	o.app.engine.ResumeWaiting(ctx, o.Handle.Writer())
}

// Drive steps the Turn on until it ends or waits on something this process
// does not carry, waiting between steps for the Outcomes the engine awaits:
// what a Session's Settle does for a Session held open, for a holder that
// drives through the Handle itself.
func (o *Owned) Drive(ctx context.Context, turnID turn.TurnID) (turn.TurnResult, error) {
	subCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	events := o.app.Events(subCtx, o.Handle.ID())
	ref := turn.TurnRef{SessionID: o.Handle.ID(), TurnID: turnID}
	for {
		surface, err := turn.ReadSurface(ctx, o.Handle.Writer().Projections(), ref.SessionID)
		if err != nil {
			return turn.TurnResult{}, err
		}
		view, ok := surface.Turns[turnID]
		if !ok {
			return turn.TurnResult{}, fmt.Errorf("%w: unknown turn %s", turn.ErrConflict, turnID)
		}
		var step execution.DriveResult
		if view.Status == turn.TurnActive {
			if step, err = o.app.engine.Drive(ctx, o.Handle.Writer(), view.RunID, view.Preset); err != nil {
				return turn.TurnResult{}, err
			}
		}
		res, err := o.app.svc.Turns.Status(ctx, ref)
		if err != nil {
			return turn.TurnResult{}, err
		}
		if res.Status != turn.TurnActive || step.InFlight == 0 {
			return res, nil
		}
		if err := awaitProgress(ctx, events, res.RunID, turnID); err != nil {
			return res, err
		}
	}
}

// Close ends the engine's listeners for the Session and releases the
// ownership.
func (o *Owned) Close(ctx context.Context) error {
	o.app.engine.Detach(o.Handle.ID())
	return o.Handle.Close(ctx)
}
