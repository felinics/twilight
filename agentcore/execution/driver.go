package execution

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/run/store"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// driver advances a Run by one step. An effect the step dispatches is
// awaited under the Session's lifetime and its Outcome settles through
// notify; a wait a Responder can answer is answered and the step goes on
// from the answer; a step that leaves effects in flight nobody here awaits
// hands them to the recovery.
type driver struct {
	// runs is the Run module's Session adapter; every drive binds it to the
	// caller's Writer.
	runs *sessionstore.SessionRunStore
	// loop steps every Run; builders resolves the prompt Builder of each
	// Run's preset.
	loop     *loop.Loop
	builders *builders
	recovery *recovery
	// responders answer the ExternalResponse waits a drive leaves; nil
	// answers none.
	responders *responders
}

// Drive advances the Run while it is active, under the Builder of the
// preset ref names. w is the caller's ownership capability over the
// Session: the Loop reads and commits through w, so a superseded owner
// plans against its own epoch's view and is fenced at commit instead of
// adopting the new owner's state. The caller's ctx bounds the step; what the
// step dispatched is awaited beyond it.
func (d *driver) Drive(ctx context.Context, w writer.Writer, runID run.RunID, ref preset.PresetRef) (DriveResult, error) {
	builder, err := d.builders.For(ref)
	if err != nil {
		return DriveResult{}, err
	}
	lt := d.recovery.lifetimeOf(w)
	for {
		if err := lt.fenced(); err != nil {
			return DriveResult{}, err
		}
		res, err := d.loop.Advance(ctx, d.runs.Bind(w), builder, runID)
		if err != nil {
			return DriveResult{}, err
		}
		switch res.Disposition {
		case loop.LoopFinished:
			return DriveResult{Finished: true}, nil
		case loop.LoopDispatched:
			for _, key := range res.Dispatched {
				d.recovery.awaitOutcome(lt, key)
			}
			return DriveResult{Dispatched: len(res.Dispatched), InFlight: d.inFlight(lt, runID)}, nil
		}
		// The Run waits. Effects in flight that this process awaits settle
		// through their Outcomes; any other -- an execution a previous owner
		// started, a dispatch whose answer was lost -- is reconciled with
		// the executor now rather than left to a drive that already returned.
		for _, key := range res.Executing {
			if !lt.awaiting(key) {
				if _, err := d.recovery.Recover(context.WithoutCancel(ctx), w); err != nil {
					if errors.Is(err, store.ErrOwnershipLost) {
						d.recovery.lost(lt)
						return DriveResult{}, err
					}
					d.recovery.report(w.SessionID(), fmt.Errorf("execution: recovering a waiting drive: %w", err))
				}
				break
			}
		}
		if d.responders == nil {
			return DriveResult{Waiting: true, InFlight: d.inFlight(lt, runID)}, nil
		}
		settled, err := d.responders.answerWaiting(ctx, w, runID)
		if err != nil {
			return DriveResult{}, err
		}
		if !settled {
			return DriveResult{Waiting: true, InFlight: d.inFlight(lt, runID)}, nil
		}
		// An answered wait is not a quiescent point: the Run moves on from
		// the answer in this same step.
	}
}

// inFlight counts what this process carries for the Run after a step: the
// effects whose Outcomes it awaits and the waits a Responder is answering.
func (d *driver) inFlight(lt *lifetime, runID run.RunID) int {
	n := lt.inFlight(runID)
	if d.responders != nil {
		n += d.responders.answeringFor(runID)
	}
	return n
}

// ResumeWaiting answers, under the Session's lifetime, every
// ExternalResponse wait a previous owner left with a Responder. A Session
// that opens runs it after the takeover disposition: the Responder
// continues from its durable state. It returns at once; each answer is
// committed in the background and reported through notify, so the host
// drives the owning Run on. Failures reach the recovery's fail.
func (d *driver) ResumeWaiting(ctx context.Context, w writer.Writer, notify func(*lifetime)) {
	if d.responders == nil {
		return
	}
	lt := d.recovery.lifetimeOf(w)
	d.responders.answerAll(ctx, w, lt.ctx, func(*answer) { notify(lt) })
}
