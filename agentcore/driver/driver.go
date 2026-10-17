package driver

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/compaction"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/loop"
	"github.com/felinics/twilight/agentcore/turn/plan"
	"github.com/felinics/twilight/agentcore/turn/reconcile"
	"github.com/felinics/twilight/agentcore/turn/redispatch"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
	"github.com/felinics/twilight/agentcore/turn/store"
)

// Disposition describes work, not ownership or an Activation's lifecycle.
// Waiting is not permission to release a Writer: the host must preserve every
// outstanding obligation and its independent rediscovery path first.
type Disposition uint8

const (
	Continue Disposition = iota
	Waiting
	Finished
)

type StepResult struct {
	Disposition Disposition
	Dispatched  []effect.AssignmentKey
}

// Driver interprets durable Session state for a finite interval. It never
// awaits an external execution, starts a background continuation, acquires
// ownership, or retains the caller's Writer after a method returns.
type Driver struct {
	TurnStore       *sessionstore.SessionTurnStore
	Loops           *Loops
	Compactions     *Compactions
	MissingEffects  reconcile.MissingPolicy
	Redispatches    redispatch.Store
	MaxRedispatches int
	Sink            loop.EventSink
}

// Step reconciles existing executions once, settles available outcomes, and
// advances a Turn to an external boundary. Scheduling another Step, including
// after an uncertain dispatch or a temporary failure, belongs to the host.
func (d *Driver) Step(ctx context.Context, w writer.Writer, turnID turn.TurnID) (StepResult, error) {
	if err := ctx.Err(); err != nil {
		return StepResult{}, err
	}
	if w == nil {
		return StepResult{}, errors.New("driver: a writer is required")
	}
	// Maintenance may replace context. Never prepare an ordinary request
	// until its durable operation has settled, even after an ownership change.
	if d.Compactions != nil {
		maintenance, err := d.Compactions.Advance(ctx, w)
		if err != nil {
			return StepResult{}, err
		}
		if maintenance.Pending {
			return StepResult{Disposition: Waiting}, nil
		}
		if maintenance.Progress {
			return StepResult{Disposition: Continue}, nil
		}
	}
	surface, err := sessionstore.ReadSurface(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return StepResult{}, err
	}
	view, ok := surface.Turns[turnID]
	if !ok {
		return StepResult{}, fmt.Errorf("%w: unknown turn %s", turn.ErrConflict, turnID)
	}
	if view.Status != turn.TurnActive {
		return StepResult{Disposition: Finished}, nil
	}
	lp, err := d.Loops.For(view.Preset)
	if err != nil {
		return StepResult{}, err
	}
	st := d.TurnStore.Bind(w)
	snapshot, err := st.Load(ctx, view.TurnID)
	if err != nil {
		return StepResult{}, err
	}
	// No callbacks are installed. Reconciliation is a one-shot comparison of
	// two authorities; active effects belong to the Executor, not this call.
	rec := &reconcile.Reconciler{Executions: d.Loops.Executor, Missing: d.MissingEffects,
		Attempts: d.Redispatches, Epoch: w.Epoch(), MaxRedispatches: d.MaxRedispatches,
		Redispatch: func(ctx context.Context, key effect.AssignmentKey) error {
			return lp.Redispatch(ctx, st, key)
		}}
	for _, target := range plan.RecoveryTargets(&snapshot.State) {
		key := effect.AssignmentKey{Session: st.Scope(), OwnerKind: effect.OwnerTurn, OwnerID: string(view.TurnID), Effect: target.Effect}
		decision, found, err := rec.ReconcileEffect(ctx, st, &snapshot, key)
		if err != nil {
			return StepResult{}, err
		}
		if !found || decision.Recovery != nil {
			return StepResult{Disposition: Continue}, nil
		}
		out, err := d.Loops.Executor.GetOutcome(ctx, key)
		if errors.Is(err, effect.ErrOutcomeNotReady) || errors.Is(err, effect.ErrExecutionNotFound) {
			continue
		}
		if err != nil {
			return StepResult{}, err
		}
		res, err := lp.Deliver(ctx, st, out, d.Sink)
		if err != nil {
			return StepResult{}, err
		}
		if res.Disposition == loop.LoopFinished {
			return StepResult{Disposition: Finished}, nil
		}
		// Return at a durable boundary instead of recursively driving the
		// next effect. The host's budget can now choose to continue or yield.
		return StepResult{Disposition: Continue}, nil
	}
	res, err := lp.Advance(ctx, st, view.TurnID, d.Sink)
	if err != nil {
		if errors.Is(err, compaction.ErrPending) {
			return StepResult{Disposition: Continue}, nil
		}
		return StepResult{}, err
	}
	switch res.Disposition {
	case loop.LoopFinished:
		return StepResult{Disposition: Finished}, nil
	case loop.LoopDispatched:
		return StepResult{Disposition: Waiting, Dispatched: res.Dispatched}, nil
	case loop.LoopWaiting:
		if res.Reason == loop.ContextMaintenance {
			return StepResult{Disposition: Continue}, nil
		}
		return StepResult{Disposition: Waiting}, nil
	default:
		return StepResult{Disposition: Continue}, nil
	}
}

// ReconcileExecution handles a discovered execution even after its Turn left
// the active projection. A durable domain disposition authorizes cancellation
// of still-running work and later ACK; a lost Activation alone never does.
func (d *Driver) ReconcileExecution(ctx context.Context, w writer.Writer, key effect.AssignmentKey) error {
	if key.Session != turn.Scope(w.SessionID()) {
		return errors.New("driver: execution belongs to another session")
	}
	if d.Compactions != nil {
		state, err := compaction.Read(ctx, w.Projections(), w.SessionID())
		if err != nil {
			return err
		}
		for id := range state.Operations {
			op := state.Operations[id]
			if op.Request.Key(w.SessionID()) == key {
				_, err := d.Compactions.Advance(ctx, w)
				return err
			}
		}
	}
	if key.OwnerKind != effect.OwnerTurn {
		return errors.New("driver: unknown execution owner")
	}
	lp, err := d.Loops.ForTurn(ctx, w, turn.TurnID(key.OwnerID))
	if err != nil {
		return err
	}
	st := d.TurnStore.Bind(w)
	proof, ok := st.(store.EffectDispositionReader)
	if !ok {
		return store.ErrEffectProofUnsupported
	}
	_, disposed, err := proof.EffectDisposition(ctx, turn.TurnID(key.OwnerID), key.Effect)
	if err != nil {
		return err
	}
	if disposed {
		att, err := d.Loops.Executor.Abort(ctx, key)
		if err != nil {
			return err
		}
		switch att.State {
		case effect.AttachmentAborted:
			return nil
		case effect.AttachmentTerminal:
			_, err := lp.AcknowledgeSettled(ctx, st, key)
			return err
		default:
			// This is backed by a permanent semantic disposition, unlike
			// cancellation inferred from a local context or fencing error.
			if err := d.Loops.Executor.Cancel(ctx, key); err != nil {
				return err
			}
			if att.State == effect.AttachmentOrphaned {
				if recoverer, ok := d.Loops.Executor.(effect.Recoverer); ok {
					return recoverer.RecoverExecution(ctx, key)
				}
			}
			return nil
		}
	}
	out, err := d.Loops.Executor.GetOutcome(ctx, key)
	if errors.Is(err, effect.ErrOutcomeNotReady) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = d.Deliver(ctx, w, out)
	return err
}

// Deliver admits one externally discovered Outcome under the current Writer.
// A dropped result is not, by itself, proof that the Executor may collect it.
func (d *Driver) Deliver(ctx context.Context, w writer.Writer, out effect.Outcome) (StepResult, error) {
	if out.Key.Session != turn.Scope(w.SessionID()) {
		return StepResult{}, errors.New("driver: outcome belongs to another session")
	}
	if d.Compactions != nil {
		state, err := compaction.Read(ctx, w.Projections(), w.SessionID())
		if err != nil {
			return StepResult{}, err
		}
		for id := range state.Operations {
			op := state.Operations[id]
			if op.Request.Key(w.SessionID()) == out.Key {
				_, err := d.Compactions.Deliver(ctx, w, out)
				return StepResult{Disposition: Continue}, err
			}
		}
	}
	if out.Key.OwnerKind != effect.OwnerTurn {
		return StepResult{}, errors.New("driver: unknown outcome owner")
	}
	lp, err := d.Loops.ForTurn(ctx, w, turn.TurnID(out.Key.OwnerID))
	if err != nil {
		return StepResult{}, err
	}
	st := d.TurnStore.Bind(w)
	res, err := lp.Deliver(ctx, st, out, d.Sink)
	if err != nil {
		return StepResult{}, err
	}
	if res.Disposition == loop.LoopDropped {
		if _, err := lp.AcknowledgeSettled(ctx, st, out.Key); err != nil {
			return StepResult{}, err
		}
	}
	if res.Disposition == loop.LoopFinished {
		return StepResult{Disposition: Finished}, nil
	}
	return StepResult{Disposition: Continue}, nil
}
