// Package reconcile compares what the Run machine believes about an effect
// with what the execution store actually holds, and turns the difference into
// Run commands or a second Dispatch (RUN-CMT-7, RUN-EXE-15). It is the
// process manager between the two authorities and the one place that reads
// three sources: the machine says which effects are outstanding (an
// Executing model step or tool call and the EffectID it requested), the
// executor says whether it still holds an attempt for that effect, and the
// dispatch ledger (agentcore/run/redispatch) says which redispatch of this effect
// was decided and whether it reached the executor. Per effect the Reconciler decides whether
// the Run keeps waiting for the attempt's Outcome, hands the effect to the
// executor again, or disposes it. Neither the Loop nor the store adapter
// interprets executor observations, and the Run never learns which attempt
// the executor made.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/run/redispatch"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/run/store"
)

// Verdict is the reconciler's decision for one Executing target.
type Verdict string

const (
	// Keep: the executor still holds an attempt for the effect (active, or
	// terminal with an Outcome to read), so the target stays Executing and
	// its Outcome settles under the effect's settlement identity.
	Keep Verdict = "keep"
	// Defer: the executor holds a durable record nobody is running (orphaned).
	// The target stays Executing and its Outcome is still awaited: nothing
	// proves the effect was absent. The Reconciler asks a port that can
	// (effect.Recoverer) to take the record back once; only an explicit
	// disposal ends the wait otherwise.
	Defer Verdict = "defer"
	// Dispose: the executor holds no attempt for the effect and none will be
	// made -- the key is closed with Abort first (RUN-EXE-16) -- so the Run
	// recovers the target itself: an Executing model step is withdrawn to
	// Open, an Executing tool call settles as Unknown.
	Dispose Verdict = "dispose"
	// Redispatch: the executor held no attempt for the effect and was handed
	// the Assignment again within the budget (RUN-EXE-15). The target stays
	// Executing and its Outcome is awaited like a kept one.
	Redispatch Verdict = "redispatch"
)

// Decision is one target's verdict and, for Dispose, the recovery command.
// A target kept or deferred stays Executing under its effect; waiting on
// that effect's Outcome is the caller's.
type Decision struct {
	Target   plan.RecoveryTarget
	Observed effect.AttachmentState
	Verdict  Verdict
	Recovery *plan.RecoveryDisposition
}

// ErrNoExecutionPort reports a Plan over Executing targets with no executor
// to ask and no Abandon: an executor that cannot be reached proves nothing
// about the executions it may hold, so the targets cannot be disposed.
var ErrNoExecutionPort = errors.New("reconcile: executing targets but no execution port to ask; set Abandon to dispose without proof")

// ErrAbandonWithExecutor reports Abandon set beside an Executions port: with
// an executor to ask, disposal must go through Abort (RUN-EXE-16), and a
// caller that wants to bypass it has to give up the port explicitly.
var ErrAbandonWithExecutor = errors.New("reconcile: Abandon set with an execution port; disposal without Abort is only for a scope no executor serves")

// MissingPolicy is what the reconciler does with an Executing effect the
// executor holds nothing for (RUN-CMT-7, RUN-EXE-15). It is the
// deployment's recovery decision, stated explicitly: the ports a policy
// needs are checked against the policy, and no port's presence or absence
// selects a policy by itself.
type MissingPolicy uint8

const (
	// DisposeMissing, the zero value: the Run recovers the target itself
	// (an Executing model step is withdrawn to Open, an Executing tool call
	// settles as Unknown). No dispatch ledger is needed.
	DisposeMissing MissingPolicy = iota
	// RedispatchMissing: the Assignment is handed to the executor again
	// within the redispatch budget, the attempts recorded in the dispatch
	// ledger. Requires Redispatch and Attempts.
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

// ErrMissingPolicyPorts reports RedispatchMissing without the ports it
// needs: the Redispatch port and the dispatch ledger (Attempts).
var ErrMissingPolicyPorts = errors.New("reconcile: RedispatchMissing requires the Redispatch port and the dispatch ledger (Attempts)")

// ErrTargetWithoutEffect reports an Executing target whose start fact
// recorded no EffectID: the executor cannot be asked about it and the
// machine state is inconsistent (RUN-WIR-1).
var ErrTargetWithoutEffect = errors.New("reconcile: executing target records no effect")

// Reconciler is the recovery decision of one owner over a Scope.
type Reconciler struct {
	// Executions is the execution store's port, asked once per Executing
	// target. Nil with Abandon unset is an error whenever a target exists:
	// "no executor to ask" is not "no execution" (RUN-CMT-7).
	Executions effect.ExecutionPort
	// Recover is the executor's recovery capability, asked to take an
	// orphaned record back; nil leaves orphaned records to an external
	// controller.
	Recover effect.Recoverer
	// Abandon disposes every Executing target without asking an executor and
	// without closing the keys (RUN-EXE-16). It is the caller's statement
	// that no executor serves this Scope at all: none holds an attempt, and
	// none can receive a late Dispatch, because there is no executor. That
	// holds for a scope that never had an execution port (conformance
	// tests, an offline repair of a Session whose executor is gone); it is
	// never inferred from an unreachable Executions, and it is not a way to
	// skip Abort when an executor exists: Abandon beside a non-nil
	// Executions is ErrAbandonWithExecutor.
	Abandon bool
	// Missing is the policy for an effect the executor holds nothing for.
	// The zero value disposes (RUN-CMT-7); RedispatchMissing requires
	// Redispatch and Attempts (RUN-EXE-15).
	Missing MissingPolicy
	// Redispatch hands the Assignment of an Executing effect the executor
	// holds nothing for to the executor again (loop.Redispatch on the
	// owner's Writer). Used under RedispatchMissing only.
	Redispatch func(ctx context.Context, key effect.AssignmentKey) error
	// Attempts is the dispatch ledger: which redispatch of each effect was
	// planned, whether it reached the executor, and whether the reconciler
	// gave up (RUN-EXE-15).
	Attempts redispatch.Store
	// Epoch fences the dispatch ledger: the Session owner's.
	Epoch ledger.Epoch
	// MaxRedispatches bounds redispatches per effect; zero selects
	// DefaultMaxRedispatches.
	MaxRedispatches int
	// Now stamps the dispatch ledger; nil selects time.Now.
	Now func() time.Time
}

// DefaultMaxRedispatches is the redispatch budget of one effect.
const DefaultMaxRedispatches = 3

// AssignmentFromTarget rebuilds the Assignment of an Executing target from
// the machine state, so the executor can be asked whether it still holds an
// attempt for the effect. Only the key and the digest-level description are
// known here; the inline request body never travels this way (RUN-EXE-7).
func AssignmentFromTarget(scope run.Scope, t plan.RecoveryTarget) effect.Assignment {
	a := effect.Assignment{Session: scope, RunID: t.RunID, StepID: t.StepID, CallID: t.CallID, Effect: t.Effect}
	switch {
	case t.Call != nil:
		a.Body = effect.ToolAssignment{ToolRef: t.Call.ToolRef, DefinitionDigest: t.Call.DefinitionDigest, Arguments: t.Call.Arguments, Policy: t.Call.Policy}
	case t.Model != nil:
		a.Body = effect.ModelAssignment{Model: t.Model.Model, RequestDigest: t.Model.RequestDigest}
	}
	return a
}

// verdictOf maps an executor observation onto the reconciler's vocabulary.
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

// Plan decides every Executing target of one Run. It asks the executor once
// per effect and starts the Outcome read of every effect it does not dispose.
// It writes no Run fact: the Dispose decisions it returns are committed by
// Apply. It does write outside the Run when a policy asks for it: under
// RedispatchMissing the dispatch ledger records each redispatch attempt, and
// every effect it disposes has its key closed at the executor first (Abort,
// RUN-EXE-16). The recovery command of a disposed effect is identified by
// the effect (RUN-WIR-1), so any owner that plans the same state issues the
// same command.
func (r *Reconciler) Plan(ctx context.Context, scope run.Scope, snapshot *store.Snapshot) ([]Decision, error) {
	targets := plan.RecoveryTargets(&snapshot.State)
	if len(targets) == 0 {
		return nil, nil
	}
	if err := r.checkMissingPolicy(); err != nil {
		return nil, err
	}
	if r.Abandon && r.Executions != nil {
		return nil, ErrAbandonWithExecutor
	}
	out := make([]Decision, 0, len(targets))
	for _, t := range targets {
		if t.Effect == "" {
			return nil, fmt.Errorf("%w: run %s step %s call %q", ErrTargetWithoutEffect, t.RunID, t.StepID, t.CallID)
		}
		d := Decision{Target: t, Observed: effect.AttachmentMissing, Verdict: Dispose}
		if !r.Abandon {
			if r.Executions == nil {
				return nil, ErrNoExecutionPort
			}
			assignment := AssignmentFromTarget(scope, t)
			attachment, err := r.Executions.Attach(ctx, assignment.Key())
			if err != nil {
				return nil, err
			}
			if !attachment.State.Valid() {
				return nil, fmt.Errorf("reconcile: executor returned attachment state %q", attachment.State)
			}
			d.Observed = attachment.State
			if d.Verdict, err = verdictOf(attachment.State); err != nil {
				return nil, err
			}
			if d.Verdict == Defer {
				r.recoverOrphan(ctx, assignment.Key())
			}
			if d.Verdict == Dispose && attachment.State == effect.AttachmentMissing {
				if d.Verdict, err = r.missing(ctx, assignment.Key()); err != nil {
					return nil, err
				}
			}
			if d.Verdict == Dispose {
				// Missing is what the executor holds now, not what it will
				// accept: close the key before the Run disposes the effect,
				// so a Dispatch that arrives later starts nothing
				// (RUN-EXE-16). An acceptance that got there first is kept.
				closed, err := r.Executions.Abort(ctx, assignment.Key())
				if err != nil {
					return nil, err
				}
				d.Observed = closed.State
				if d.Verdict, err = verdictOf(closed.State); err != nil {
					return nil, err
				}
				if d.Verdict == Defer {
					r.recoverOrphan(ctx, assignment.Key())
				}
			}
		}
		if d.Verdict == Dispose {
			rec := plan.RecoveryCommand(schema.Identity(), t)
			d.Recovery = &rec
		}
		out = append(out, d)
	}
	return out, nil
}

// missing decides an effect the executor holds nothing for: within the
// redispatch budget it is handed over again and stays Executing, otherwise
// it is disposed (RUN-EXE-15). The dispatch ledger records each attempt in
// two steps: planned before the Dispatch, dispatched once the Dispatch
// reached the executor. A planned attempt the ledger does not show as
// dispatched is owed, and is made again before any new one is planned, so
// a crash between the two steps costs no budget and never leaves a Dispatch
// the ledger does not know about; the executor recognises the replay by
// its key (RUN-EXE-3). A refusal the executor may lift later
// (ErrDispatchRetryable) or a lost response (ErrDispatchUnknown) leaves the
// attempt owed and the target Executing for the next reconciliation; any
// other rejection ends the attempts.
func (r *Reconciler) missing(ctx context.Context, key effect.AssignmentKey) (Verdict, error) {
	if r.Missing == DisposeMissing {
		return Dispose, nil
	}
	state, _, _, err := r.Attempts.Load(ctx, key)
	if err != nil {
		return Dispose, err
	}
	if state.GivenUp {
		return Dispose, nil
	}
	budget := r.MaxRedispatches
	if budget <= 0 {
		budget = DefaultMaxRedispatches
	}
	if state.Pending() == 0 && state.Planned >= budget {
		if err := redispatch.GiveUp(ctx, r.Attempts, r.Epoch, key, fmt.Sprintf("redispatch budget of %d exhausted", budget), r.now()); err != nil {
			return Dispose, err
		}
		return Dispose, nil
	}
	attempt, err := redispatch.Plan(ctx, r.Attempts, r.Epoch, key, r.now())
	if err != nil {
		return Dispose, err
	}
	err = r.Redispatch(ctx, key)
	switch {
	case err == nil:
		if err := redispatch.MarkDispatched(ctx, r.Attempts, r.Epoch, key, attempt, r.now()); err != nil {
			return Dispose, err
		}
		return Redispatch, nil
	case errors.Is(err, effect.ErrDispatchRetryable), errors.Is(err, effect.ErrDispatchUnknown):
		return Defer, nil
	default:
		if gerr := redispatch.GiveUp(ctx, r.Attempts, r.Epoch, key, err.Error(), r.now()); gerr != nil {
			return Dispose, gerr
		}
		return Dispose, nil
	}
}

// checkMissingPolicy verifies the ports the configured policy needs.
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

// recoverOrphan asks the executor to take an orphaned record back, when
// Recover is set. It is the Owner acting on its own Run's effect: the
// record was just observed orphaned, so this is the moment to ask. A failed
// or impossible recovery leaves the target deferred; giving the execution
// up is a separate decision, made through Dispose.
func (r *Reconciler) recoverOrphan(ctx context.Context, key effect.AssignmentKey) {
	if r.Recover != nil {
		_ = r.Recover.RecoverExecution(ctx, key)
	}
}

// Apply commits the Dispose decisions through the bound store and returns
// how many were accepted. A decision another actor has already overtaken
// (stale, terminal, conflict) is skipped.
func Apply(ctx context.Context, st store.RunStore, decisions []Decision) (int, error) {
	n := 0
	for i := range decisions {
		d := &decisions[i]
		if d.Recovery == nil {
			continue
		}
		env, err := schema.Wire().Envelope(d.Target.RunID, d.Recovery.ID, d.Recovery.Command)
		if err != nil {
			return n, err
		}
		res, err := st.Commit(ctx, store.CommitRequest{Command: env})
		if err != nil {
			if errors.Is(err, run.ErrStaleRuntime) || errors.Is(err, run.ErrRunTerminal) || errors.Is(err, run.ErrCommandConflict) {
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

// Reconcile is Plan then Apply for one Run: the takeover disposition of its
// Executing targets (RUN-CMT-7). It returns the number of accepted recovery
// commands. Each command is identified by the effect it disposes (RUN-WIR-1),
// so a repeated or a later takeover replays the same commands idempotently.
func (r *Reconciler) Reconcile(ctx context.Context, st store.RunStore, snapshot *store.Snapshot) (int, error) {
	decisions, err := r.Plan(ctx, st.Scope(), snapshot)
	if err != nil {
		return 0, err
	}
	return Apply(ctx, st, decisions)
}

// ReconcileAll is Reconcile over every snapshot: the takeover disposition of
// a Scope's active Runs. It returns the accepted recovery commands of all of
// them; an error stops it with the count so far.
func (r *Reconciler) ReconcileAll(ctx context.Context, st store.RunStore, snapshots []store.Snapshot) (int, error) {
	n := 0
	for i := range snapshots {
		accepted, err := r.Reconcile(ctx, st, &snapshots[i])
		n += accepted
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
