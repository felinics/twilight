// Package reconcile compares durable Turn intents with Execution Authority.
// A pass is finite: it installs no listeners, timers or background work.
// Hosts schedule later passes from the returned decisions and durable state.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/plan"
	"github.com/felinics/twilight/agentcore/turn/redispatch"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/store"
)

type Verdict string

const (
	Keep       Verdict = "keep"
	Defer      Verdict = "defer"
	Dispose    Verdict = "dispose"
	Redispatch Verdict = "redispatch"
)

// Decision is a recovery verdict, not a promise that a local goroutine will
// resume the Turn. Retry means a refusal before the effect boundary. Unknown
// dispatch is never interpreted as proof that an effect did not happen.
type Decision struct {
	Target   plan.RecoveryTarget
	Observed effect.AttachmentState
	Verdict  Verdict
	Recovery *plan.RecoveryDisposition
	Retry    bool
}

var (
	ErrNoExecutionPort     = errors.New("reconcile: executing targets but no execution port; set Abandon only when no executor serves the scope")
	ErrAbandonWithExecutor = errors.New("reconcile: Abandon cannot bypass an existing executor's Abort protocol")
	ErrMissingPolicyPorts  = errors.New("reconcile: RedispatchMissing requires Redispatch and Attempts")
	ErrTargetWithoutEffect = errors.New("reconcile: executing target records no effect")
)

type MissingPolicy uint8

const (
	DisposeMissing MissingPolicy = iota
	RedispatchMissing
)

func (p MissingPolicy) String() string {
	switch p {
	case DisposeMissing:
		return "dispose"
	case RedispatchMissing:
		return "redispatch"
	default:
		return fmt.Sprintf("MissingPolicy(%d)", uint8(p))
	}
}

// Reconciler carries only one-pass dependencies. An unreachable Executor is
// not an absent execution. Disposal closes the assignment key with Abort
// before committing a recovery fact, fencing any delayed Dispatch.
type Reconciler struct {
	Executions effect.ExecutionPort
	// Abandon is only for an offline scope that no executor serves. It is
	// never inferred from network errors or used alongside Executions.
	Abandon         bool
	Missing         MissingPolicy
	Redispatch      func(context.Context, effect.AssignmentKey) error
	Attempts        redispatch.Store
	Epoch           ledger.Epoch
	MaxRedispatches int
	Now             func() time.Time
}

const DefaultMaxRedispatches = 3

func AssignmentFromTarget(scope turn.Scope, t plan.RecoveryTarget) effect.Assignment {
	a := effect.Assignment{Session: scope, OwnerKind: effect.OwnerTurn, OwnerID: string(t.TurnID), StepID: t.StepID, CallID: t.CallID, Effect: t.Effect}
	if t.Call != nil {
		a.Body = effect.ToolAssignment{ToolRef: t.Call.ToolRef, DefinitionDigest: t.Call.DefinitionDigest, Arguments: t.Call.Arguments, Policy: t.Call.Policy}
	} else if t.Model != nil {
		a.Body = effect.ModelAssignment{Model: t.Model.Model, RequestDigest: t.Model.RequestDigest}
	}
	return a
}

func verdictOf(state effect.AttachmentState) (Verdict, error) {
	switch state {
	case effect.AttachmentActive, effect.AttachmentTerminal:
		return Keep, nil
	case effect.AttachmentOrphaned:
		return Defer, nil
	case effect.AttachmentMissing, effect.AttachmentAborted:
		return Dispose, nil
	default:
		return Dispose, fmt.Errorf("reconcile: unknown attachment state %q", state)
	}
}

// Plan observes every executing target once. It writes no Turn facts, but
// may close a missing key, record a redispatch, or request orphan recovery.
func (r *Reconciler) Plan(ctx context.Context, scope turn.Scope, snapshot *store.Snapshot) ([]Decision, error) {
	return r.plan(ctx, scope, snapshot, nil)
}

func (r *Reconciler) plan(ctx context.Context, scope turn.Scope, snapshot *store.Snapshot, only *effect.AssignmentKey) ([]Decision, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	targets := plan.RecoveryTargets(&snapshot.State)
	if only != nil {
		filtered := targets[:0]
		for _, t := range targets {
			if (effect.AssignmentKey{Session: scope, OwnerKind: effect.OwnerTurn, OwnerID: string(t.TurnID), Effect: t.Effect}) == *only {
				filtered = append(filtered, t)
			}
		}
		targets = filtered
	}
	if len(targets) == 0 {
		return nil, nil
	}
	if err := r.checkMissingPolicy(); err != nil {
		return nil, err
	}
	if r.Abandon && r.Executions != nil {
		return nil, ErrAbandonWithExecutor
	}
	decisions := make([]Decision, 0, len(targets))
	for _, t := range targets {
		if t.Effect == "" {
			return nil, fmt.Errorf("%w: turn %s step %s call %q", ErrTargetWithoutEffect, t.TurnID, t.StepID, t.CallID)
		}
		d := Decision{Target: t, Observed: effect.AttachmentMissing, Verdict: Dispose}
		if !r.Abandon {
			if r.Executions == nil {
				return nil, ErrNoExecutionPort
			}
			key := AssignmentFromTarget(scope, t).Key()
			attachment, err := r.Executions.Attach(ctx, key)
			if err != nil {
				return nil, err
			}
			d.Observed = attachment.State
			if d.Verdict, err = verdictOf(attachment.State); err != nil {
				return nil, err
			}
			if d.Verdict == Dispose && attachment.State == effect.AttachmentMissing {
				if d.Verdict, d.Retry, err = r.missing(ctx, key); err != nil {
					return nil, err
				}
			}
			if d.Verdict == Dispose {
				closed, err := r.Executions.Abort(ctx, key)
				if err != nil {
					return nil, err
				}
				d.Observed = closed.State
				if d.Verdict, err = verdictOf(closed.State); err != nil {
					return nil, err
				}
			}
			if d.Verdict == Defer && d.Observed == effect.AttachmentOrphaned {
				if recoverer, ok := r.Executions.(effect.Recoverer); ok {
					if err := recoverer.RecoverExecution(ctx, key); err != nil {
						return nil, err
					}
				}
			}
		}
		if d.Verdict == Dispose {
			recovery := plan.RecoveryCommand(schema.Identity(), t)
			d.Recovery = &recovery
		}
		decisions = append(decisions, d)
	}
	return decisions, nil
}

// A planned-but-unconfirmed redispatch is reused, not charged as a fresh
// attempt. The same assignment key remains authoritative across Activations.
func (r *Reconciler) missing(ctx context.Context, key effect.AssignmentKey) (Verdict, bool, error) {
	if r.Missing == DisposeMissing {
		return Dispose, false, nil
	}
	state, _, _, err := r.Attempts.Load(ctx, key)
	if err != nil {
		return Dispose, false, err
	}
	if state.GivenUp {
		return Dispose, false, nil
	}
	budget := r.MaxRedispatches
	if budget <= 0 {
		budget = DefaultMaxRedispatches
	}
	if state.Pending() == 0 && state.Planned >= budget {
		if err := redispatch.GiveUp(ctx, r.Attempts, r.Epoch, key, fmt.Sprintf("redispatch budget of %d exhausted", budget), r.now()); err != nil {
			return Dispose, false, err
		}
		return Dispose, false, nil
	}
	attempt, err := redispatch.Plan(ctx, r.Attempts, r.Epoch, key, r.now())
	if err != nil {
		return Dispose, false, err
	}
	err = r.Redispatch(ctx, key)
	switch {
	case err == nil:
		if err := redispatch.MarkDispatched(ctx, r.Attempts, r.Epoch, key, attempt, r.now()); err != nil {
			return Dispose, false, err
		}
		return Redispatch, false, nil
	case errors.Is(err, effect.ErrDispatchRetryable):
		return Defer, true, nil
	case errors.Is(err, effect.ErrDispatchUnknown):
		return Defer, false, nil
	default:
		if gerr := redispatch.GiveUp(ctx, r.Attempts, r.Epoch, key, err.Error(), r.now()); gerr != nil {
			return Dispose, false, gerr
		}
		return Dispose, false, nil
	}
}

func (r *Reconciler) checkMissingPolicy() error {
	switch r.Missing {
	case DisposeMissing:
		return nil
	case RedispatchMissing:
		if r.Redispatch == nil || r.Attempts == nil {
			return ErrMissingPolicyPorts
		}
		return nil
	default:
		return fmt.Errorf("reconcile: unknown missing policy %s", r.Missing)
	}
}

func (r *Reconciler) now() int64 {
	if r.Now != nil {
		return r.Now().UnixMilli()
	}
	return time.Now().UnixMilli()
}

func Apply(ctx context.Context, st store.TurnStore, decisions []Decision) (int, error) {
	n := 0
	for i := range decisions {
		d := &decisions[i]
		if d.Recovery == nil {
			continue
		}
		env, err := schema.Wire().Envelope(d.Target.TurnID, d.Recovery.ID, d.Recovery.Command)
		if err != nil {
			return n, err
		}
		res, err := st.Commit(ctx, store.CommitRequest{Command: env})
		if err != nil {
			if errors.Is(err, turn.ErrStaleRuntime) || errors.Is(err, turn.ErrTurnTerminal) || errors.Is(err, turn.ErrCommandConflict) {
				continue
			}
			return n, err
		}
		if res.Status == store.CommitAccepted {
			n++
		}
	}
	return n, nil
}

func (r *Reconciler) Reconcile(ctx context.Context, st store.TurnStore, snapshot *store.Snapshot) (int, error) {
	decisions, err := r.Plan(ctx, st.Scope(), snapshot)
	if err != nil {
		return 0, err
	}
	return Apply(ctx, st, decisions)
}

func (r *Reconciler) ReconcileEffect(ctx context.Context, st store.TurnStore, snapshot *store.Snapshot, key effect.AssignmentKey) (Decision, bool, error) {
	decisions, err := r.plan(ctx, st.Scope(), snapshot, &key)
	if err != nil {
		return Decision{}, false, err
	}
	if len(decisions) == 0 {
		return Decision{}, false, nil
	}
	if _, err := Apply(ctx, st, decisions); err != nil {
		return Decision{}, false, err
	}
	return decisions[0], true, nil
}
