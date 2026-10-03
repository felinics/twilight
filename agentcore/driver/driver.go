package driver

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Driver drives an active Turn to its next quiescent point. A wait a
// Responder can answer is not a quiescent point: the answer is committed
// and the drive continues from it. A drive that quiesces with executions in
// flight and no local waiter hands them to the Recovery.
type Driver struct {
	// Runs is the Run module's Session adapter; every drive binds it to the
	// caller's Writer.
	Runs     *sessionstore.SessionRunStore
	Loops    *Loops
	Recovery *Recovery
	// Responders answer the ExternalResponse waits a drive leaves; nil
	// answers none.
	Responders *Responders
	// Sink receives the drives' provisional observations: the executor's
	// progress frames relayed by the Loop. nil discards them.
	Sink loop.EventSink
}

// Drive drives the Turn while it is active: resolve its recorded preset and
// drive its Run to the next quiescent point. alreadyDriving is true when a
// concurrent local driver of the same Run was already carrying it, in which
// case this call drove nothing. w is the caller's ownership capability over
// the Session: the decision whether to drive reads w's own projections, and
// the Loop commits through w, so a superseded owner plans against its own
// epoch's view and is fenced at commit instead of adopting the new owner's
// state. The caller's ctx bounds the drive, so cancellation is the caller's
// decision. The Turn's committed status is the Turn protocol's read, not
// this method's.
func (d *Driver) Drive(ctx context.Context, w writer.Writer, turnID turn.TurnID) (alreadyDriving bool, err error) {
	ref := turn.TurnRef{SessionID: w.SessionID(), TurnID: turnID}
	surface, err := turn.ReadSurface(ctx, w.Projections(), ref.SessionID)
	if err != nil {
		return false, err
	}
	view, ok := surface.Turns[ref.TurnID]
	if !ok {
		return false, fmt.Errorf("%w: unknown turn %s", turn.ErrConflict, ref.TurnID)
	}
	if view.Status != turn.TurnActive {
		return false, nil
	}
	l, err := d.Loops.For(view.Preset)
	if err != nil {
		return false, err
	}
	for {
		res, err := l.Run(ctx, d.Runs.Bind(w), view.RunID, d.Sink)
		if err != nil {
			if errors.Is(err, loop.ErrRunAlreadyRunning) {
				return true, nil
			}
			return false, err
		}
		if res.ExecutionRecovery {
			// The drive quiesced with executions in flight and no local
			// waiter. Offer every Executing target reattachment and dispose
			// what no executor answers, instead of leaving the Turn to a
			// driver that already returned.
			if _, err := d.Recovery.Recover(context.WithoutCancel(ctx), w); err != nil {
				d.Recovery.fail(ref.SessionID, fmt.Errorf("driver: recovering a quiesced drive: %w", err))
			}
		}
		if res.Disposition == loop.LoopWaiting && d.Responders != nil {
			settled, err := d.Responders.answerWaiting(ctx, w, view.RunID)
			if err != nil {
				return false, err
			}
			if settled {
				continue
			}
		}
		return false, nil
	}
}

// ResumeWaiting answers, under the Session's recovery lifetime, every
// ExternalResponse wait a previous owner left with a Responder and drives
// the owning Turn on from each answer. A Session that opens runs it after
// the takeover disposition: the Responder continues from its durable
// state. It returns at once; the answers run in the background and their
// failures reach the Recovery's Fail.
func (d *Driver) ResumeWaiting(ctx context.Context, w writer.Writer) {
	if d.Responders == nil {
		return
	}
	lt := d.Recovery.lifetimeOf(w)
	d.Responders.answerAll(ctx, w, lt.ctx, func(a *answer) {
		turnID, err := ownerOf(lt.ctx, lt.w, a.call.Request.RunID)
		if err == nil {
			_, err = d.Drive(lt.ctx, lt.w, turnID)
		}
		if err != nil {
			d.Recovery.fail(lt.w.SessionID(), fmt.Errorf("driver: drive after response of run %s: %w", a.call.Request.RunID, err))
		}
	})
}
