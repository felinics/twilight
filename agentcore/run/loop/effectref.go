package loop

import (
	"context"
	"errors"

	run "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
)

// effectRef is one effect of a Run as this Loop addresses it: the derived
// EffectID the start fact records, with the schema whose Identity derives the
// start, settlement and recovery CommandIDs from it (RUN-WIR-1). It holds
// nothing of the physical execution -- worker, lease and backend handle stay
// in the executor -- so any owner that reads the same state derives the same
// identities and a repeated command replays instead of granting twice.
type effectRef struct {
	runID run.RunID
	id    run.EffectID
}

// modelEffect is the next model effect of a Prepared ModelStep: its sequence
// is the count of results the step rejected so far, so a step started again
// after a rejection requests a new effect.
func modelEffect(runID run.RunID, step *run.ModelStep) effectRef {
	return effectRef{runID: runID, id: schema.Identity().DeriveEffectID(runID, step.RefValue.ID, "", step.Rejects)}
}

// toolEffect is the one tool effect of a call: a call starts at most once.
func toolEffect(runID run.RunID, stepID run.StepID, callID run.CallID) effectRef {
	return effectRef{runID: runID, id: schema.Identity().DeriveEffectID(runID, stepID, callID, 0)}
}

func (e *effectRef) startID() run.CommandID { return schema.Identity().DeriveStartCommandID(e.id) }
func (e *effectRef) settlementID() run.CommandID {
	return schema.Identity().DeriveSettlementCommandID(e.id)
}
func (e *effectRef) recoveryID() run.CommandID {
	return schema.Identity().DeriveRecoveryCommandID(e.id)
}

// settle commits the owner settlement of an effect under its derived
// CommandID. A sentinel rejection means the effect is over (another actor
// moved the target); ownership loss is returned as is.
//
// When the accepted settlement terminates the Run, the terminal RunResult is
// returned: the RunStore already handed back the folded state, so the Loop
// finishes from it instead of reloading a Run the projection no longer holds.
func (l *Loop) settle(ctx context.Context, rt runtime.RunStore, events EventSink, e *effectRef, base run.RunPosition, cmd run.AgentCommand) (*run.RunResult, error) {
	id := e.settlementID()
	if _, recovering := cmd.(run.RecoverModelExecution); recovering {
		id = e.recoveryID()
	}
	res, err := l.commit(context.WithoutCancel(ctx), rt, e.runID, id, base, cmd)
	if err != nil {
		if retriable(err) {
			return nil, nil
		}
		return nil, err
	}
	l.emitCommitted(ctx, events, rt.Scope(), e.runID, res.Facts)
	// The settlement is a Session fact: the executor may collect the
	// effect's record (RUN-EXE-13). A refused or failed acknowledgement
	// changes nothing here; the executor's time-based collection covers it.
	if a, ok := l.Executor.(Acknowledger); ok {
		_ = a.Acknowledge(ctx, AssignmentKey{Session: rt.Scope(), RunID: e.runID, Effect: e.id})
	}
	if res.Snapshot.State.Status.Terminal() {
		return res.Snapshot.State.Result, nil
	}
	return nil, nil
}

// ownershipLost reports the terminal ownership error (RUN-LOP-5).
func ownershipLost(err error) bool { return errors.Is(err, runtime.ErrOwnershipLost) }
