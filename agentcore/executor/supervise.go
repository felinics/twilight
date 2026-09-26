package executor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agentcore/executor/protocol"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// This file is the supervision of one execution from the moment the Worker
// holds its lease to the moment its settlement is committed: acquiring the
// lease and heartbeating it, attaching to or replaying the Backend's
// execution, starting it, observing its Outcome, retrying a failure that
// declares itself retryable, and settling. The port methods the Worker
// exposes (Dispatch, Abort, Attach, GetOutcome, Cancel, ...) stay in
// worker.go and hand a key to acquireAndStart; nothing here is reachable
// from outside the package.

// acquireAndStart takes the key's lease and continues the execution from its
// fold (RUN-EXE-3, RUN-EXE-9): an execution with an attachable backend
// execution is observed, one whose execution the backend proves missing is
// re-started (a model, or a tool whose Replay policy allows it) or settled
// Unknown (any other tool, TRN-DUR-4), one whose execution the backend
// cannot confirm (orphaned) is held under the lease and asked about again
// until the backend can, and one never started is started.
func (w *Worker) acquireAndStart(ctx context.Context, key effect.AssignmentKey) error {
	lease, acquired, err := w.store.Acquire(ctx, key, w.id, w.lease)
	if err != nil {
		return err
	}
	if !acquired {
		return nil
	}
	claimed, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	// A ledger opened without its binding (an older store, a test fixture)
	// is routed and prepared here, before any backend call, and the Ref is
	// committed first (RUN-EXE-9).
	if claimed.ExecutionRef.Provider == "" && claimed.State != effect.ExecutionCancelRequested {
		route, err := w.route(claimed.Assignment)
		if err != nil {
			return err
		}
		ref, err := route.Backend.Prepare(ctx, claimed.Assignment)
		if err != nil {
			return err
		}
		bound := ExecutionRef{Provider: route.Provider, Ref: ref}
		err = w.commit(ctx, lease, key, func(state *executionstore.Execution, _ executionstore.Head) (*executionstore.Commit, error) {
			if state.ExecutionRef.Provider != "" {
				return nil, nil
			}
			return &executionstore.Commit{CommitID: executionstore.DeriveCommitID(key, "bind", fmt.Sprint(uint64(lease.Epoch))),
				Events: []executionstore.Event{w.event(executionstore.EventExecutionBound, executionstore.Bound{Ref: bound})}}, nil
		})
		if err != nil {
			return err
		}
		claimed.ExecutionRef = bound
	}
	backend, err := w.backend(claimed.ExecutionRef)
	if err != nil {
		return err
	}
	ref := claimed.ExecutionRef.Ref
	if claimed.State == effect.ExecutionCancelRequested {
		leaseDone := make(chan struct{})
		w.spawn(func() { w.heartbeat(lease, leaseDone) })
		attachment, attachErr := backend.Attach(ctx, ref)
		if attachErr != nil {
			close(leaseDone)
			return attachErr
		}
		if attachment.State != effect.AttachmentActive && attachment.State != effect.AttachmentTerminal {
			env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, Unknown: true,
				Error: &protocol.WireError{Code: "cancel_reconciliation_unknown", Message: "cancelled execution is no longer attached"}}
			_ = w.finishOwned(ctx, lease, &env, effect.ExecutionUnknown, nil)
			close(leaseDone)
			return nil
		}
		cancelErr := backend.Cancel(ctx, ref)
		w.spawn(func() { w.watch(lease, backend, ref, leaseDone) })
		return cancelErr
	}
	leaseDone := make(chan struct{})
	w.spawn(func() { w.heartbeat(lease, leaseDone) })
	if claimed.State == effect.ExecutionRunning || claimed.State == effect.ExecutionDispatching {
		// Prefer adoption over retry. Recovery is allowed to retry only after
		// the backend proves that the old execution is missing; an answer it
		// cannot give yet (orphaned) keeps the lease and the question open.
		attachment, attachErr := backend.Attach(ctx, ref)
		if attachErr != nil {
			close(leaseDone)
			return attachErr
		}
		switch attachment.State {
		case effect.AttachmentActive, effect.AttachmentTerminal:
			// Keep Dispatching as a conservative pre-outcome state. The watcher
			// will terminalize it after the adopted backend produces an outcome.
			w.spawn(func() { w.watch(lease, backend, ref, leaseDone) })
			return nil
		case effect.AttachmentOrphaned:
			held := claimed
			w.spawn(func() { w.awaitBackend(lease, &held, backend, ref, leaseDone) })
			return nil
		}
		return w.replay(ctx, lease, &claimed, backend, ref, leaseDone)
	}
	return w.start(ctx, lease, &claimed, backend, ref, leaseDone)
}

// awaitBackend holds a taken-over execution whose backend could not confirm
// it (orphaned): it asks the backend again with backoff, under the
// heartbeat leaseDone ends, until the backend observes the execution (then
// it is watched), proves it missing (then it is replayed or settled as a
// missing one would be), or this incarnation loses the lease. Nothing is
// re-dispatched while the backend is undecided (RUN-EXE-3, TRN-DUR-4).
func (w *Worker) awaitBackend(lease executionstore.Lease, claimed *executionstore.Execution, backend ExecutionBackend, ref string, leaseDone chan struct{}) {
	delay := 10 * time.Millisecond
	for {
		if !w.waitOwned(lease, delay) {
			close(leaseDone)
			return
		}
		delay = min(delay*2, time.Second)
		attachCtx, cancel := context.WithTimeout(w.lifecycle, w.lease)
		attachment, err := backend.Attach(attachCtx, ref)
		cancel()
		if err != nil {
			continue
		}
		switch attachment.State {
		case effect.AttachmentActive, effect.AttachmentTerminal:
			w.watch(lease, backend, ref, leaseDone)
			return
		case effect.AttachmentMissing:
			// A failed replay has released the lease (replay closes leaseDone
			// on every error path), so the execution reads as orphaned again
			// and the Owner's next recovery request retries it.
			_ = w.replay(w.lifecycle, lease, claimed, backend, ref, leaseDone)
			return
		}
	}
}

// replay is what a takeover does with an execution the backend proved
// missing. A model is always replayed; a tool only when the Replay policy
// its Assignment carries allows it, because its lost execution may have
// crossed the effect boundary before its worker died (TRN-DUR-4). The policy
// travels with the Assignment, so a local and a remote Worker decide alike
// from the ledger; a forbidden or unjudged tool settles Unknown, the message
// naming the declaration. Restart replays the same Assignment as a new
// generation: it allocates the Ref of the new physical execution and the old
// Ref, just proved missing, moves to the audit trail as execution_restarted
// (RUN-EXE-9). leaseDone ends the running heartbeat when nothing is left to
// watch.
func (w *Worker) replay(ctx context.Context, lease executionstore.Lease, claimed *executionstore.Execution, backend ExecutionBackend, ref string, leaseDone chan struct{}) error {
	if tool, ok := claimed.Assignment.Tool(); ok && tool.Replay != run.ReplayAllowed {
		env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: lease.Key, Unknown: true,
			Error: &protocol.WireError{Code: "adopted_without_replay", Message: fmt.Sprintf("tool %q declares replay %s; the lost execution is not re-dispatched", tool.ToolRef, tool.Replay)}}
		err := w.finishOwned(ctx, lease, &env, effect.ExecutionUnknown, nil)
		close(leaseDone)
		return err
	}
	fresh, err := backend.Restart(ctx, ref, claimed.Assignment)
	if err != nil {
		close(leaseDone)
		return err
	}
	if fresh == "" {
		close(leaseDone)
		return errors.New("executor: backend restarted with an empty execution ref")
	}
	if fresh != ref {
		if err := w.restarted(ctx, lease, claimed.ExecutionRef, fresh); err != nil {
			close(leaseDone)
			return err
		}
		claimed.Superseded = append(claimed.Superseded, claimed.ExecutionRef)
		claimed.ExecutionRef.Ref = fresh
		ref = fresh
		w.progress.Reset(lease.Key)
	}
	return w.start(ctx, lease, claimed, backend, ref, leaseDone)
}

// restarted commits execution_restarted: from leaves, fresh continues.
func (w *Worker) restarted(ctx context.Context, lease executionstore.Lease, from ExecutionRef, fresh string) error {
	return w.commit(ctx, lease, lease.Key, func(state *executionstore.Execution, _ executionstore.Head) (*executionstore.Commit, error) {
		if state.ExecutionRef.Ref == fresh {
			return nil, nil
		}
		if state.ExecutionRef != from {
			return nil, fmt.Errorf("%w: restart of %+v, ledger holds %+v", executionstore.ErrStateConflict, from, state.ExecutionRef)
		}
		return &executionstore.Commit{CommitID: executionstore.DeriveCommitID(lease.Key, "restart", from.Ref),
			Events: []executionstore.Event{w.event(executionstore.EventExecutionRestarted, executionstore.Restarted{Superseded: from, Ref: fresh})}}, nil
	})
}

// start moves the execution through Dispatching to Running around
// Backend.Start and hands it to a watcher; the heartbeat leaseDone ends is
// already running.
func (w *Worker) start(ctx context.Context, lease executionstore.Lease, claimed *executionstore.Execution, backend ExecutionBackend, ref string, leaseDone chan struct{}) error {
	key := lease.Key
	if claimed.State != effect.ExecutionDispatching {
		if err := w.commit(ctx, lease, key, w.transition(lease, executionstore.EventExecutionStarted, effect.ExecutionDispatching, "start")); err != nil {
			close(leaseDone)
			if errors.Is(err, executionstore.ErrLeaseLost) || errors.Is(err, executionstore.ErrStateConflict) {
				return nil
			}
			return err
		}
	}
	if err := backend.Start(context.WithoutCancel(ctx), ref, claimed.Assignment); err != nil {
		if errors.Is(err, effect.ErrDispatchUnknown) {
			w.spawn(func() { w.watch(lease, backend, ref, leaseDone) })
			return err
		}
		settleErr := w.finishOwned(ctx, lease, &protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key,
			Error: &protocol.WireError{Code: "dispatch_failed", Message: err.Error()}}, effect.ExecutionFailed, err)
		close(leaseDone)
		return settleErr
	}
	if err := w.commit(ctx, lease, key, w.transition(lease, executionstore.EventExecutionRunning, effect.ExecutionRunning, "run")); err != nil {
		// The backend call may already have crossed its external boundary.
		// Keep the watcher alive and report Dispatch as accepted; recovery
		// must reconcile the Dispatching/Running execution rather than replan.
		w.spawn(func() { w.watch(lease, backend, ref, leaseDone) })
		return nil
	}
	w.spawn(func() { w.watch(lease, backend, ref, leaseDone) })
	return nil
}

// watch reads the backend's Outcome for ref and settles the execution under
// this incarnation's lease.
func (w *Worker) watch(lease executionstore.Lease, backend ExecutionBackend, ref string, done chan struct{}) {
	defer close(done)
	for {
		if !w.observe(lease, backend, &ref) {
			return
		}
	}
}

// observe reads the Outcome of ref and settles it, or restarts the effect
// after a retryable failure and reports true with ref moved to the new
// execution; false ends the watch. It reads once, then waits for the
// Backend's notice that the Ref settled (RUN-EXE-17); a read at intervals
// bounded by the lease covers a Backend without notices and a notice that
// was lost, and every wait re-checks that this Worker still owns the record.
func (w *Worker) observe(lease executionstore.Lease, backend ExecutionBackend, ref *string) bool {
	wait := w.notices.await(backend, *ref)
	defer wait.cancel()
	delay := 10 * time.Millisecond
	var out effect.Outcome
	for {
		readCtx, cancelRead := context.WithTimeout(w.lifecycle, w.lease)
		var err error
		out, err = backend.Outcome(readCtx, *ref)
		cancelRead()
		switch {
		case err == nil:
		case errors.Is(err, effect.ErrOutcomeNotReady):
			// A live notice stream lets the read wait a whole lease; until
			// the stream is known live, and for a Backend without one, a
			// read every second covers a notice recorded before the
			// subscription began.
			interval := w.lease
			if !wait.established() {
				interval = min(w.lease, time.Second)
			}
			if !w.waitSignal(lease, wait.signal, interval) {
				return false
			}
			continue
		case errors.Is(err, effect.ErrExecutionNotFound), errors.Is(err, effect.ErrOutcomeUnavailable):
			// A definitive answer: the Backend will never produce this
			// Outcome. The record settles Unknown instead of waiting on a
			// read that cannot change (RUN-EXE-3).
			env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: lease.Key, Unknown: true,
				Error: &protocol.WireError{Code: "outcome_unavailable", Message: err.Error()}}
			w.settleWithRetry(lease, &env, effect.ExecutionUnknown, delay)
			return false
		default:
			// A transport or read failure: try again shortly.
			if !w.waitOwned(lease, delay) {
				return false
			}
			delay = min(delay*2, time.Second)
			continue
		}
		break
	}
	// A Known transient failure of an effect whose policy allows it is
	// re-dispatched under the same ledger within the budget (RUN-EXE-11):
	// the old Ref joins the audit trail and the watch continues on the new one.
	if next, ok := w.retryAfter(lease, backend, *ref, out); ok {
		*ref = next
		return true
	}
	// The backend knows the Ref, not the attempt: the ledger's key is the
	// Outcome's key (RUN-EXE-9).
	out.Key = lease.Key
	env := protocol.EncodeOutcome(out)
	w.settleWithRetry(lease, &env, protocol.StatusForOutcome(out), delay)
	return false
}

// settleWithRetry commits the settlement, retrying transient store failures
// while this Worker still owns the record.
func (w *Worker) settleWithRetry(lease executionstore.Lease, env *protocol.OutcomeEnvelope, state effect.ExecutionStatus, delay time.Duration) {
	for {
		if err := w.finishOwned(w.lifecycle, lease, env, state, nil); err == nil {
			return
		}
		if !w.waitOwned(lease, delay) {
			return
		}
		delay = min(delay*2, time.Second)
	}
}

// waitSignal waits for the Backend's notice on signal or for interval to
// elapse, whichever is first, and reports whether this Worker still owns
// the record. A nil signal (a Backend without notices) waits the interval.
func (w *Worker) waitSignal(lease executionstore.Lease, signal <-chan struct{}, interval time.Duration) bool {
	if !w.ownershipIntact(lease) {
		return false
	}
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-w.lifecycle.Done():
		return false
	case <-signal:
	case <-timer.C:
	}
	return w.ownershipIntact(lease)
}

// retryAfter decides whether out, the Outcome of ref, is a Known failure
// that declares itself retryable and the Worker's budget still allows,
// and if so restarts the effect: Backoff scaled by the attempts so far, then
// Backend.Restart and Start under the same lease, the old Ref recorded as
// superseded. It returns the new Ref. A backend whose Restart returns the
// same Ref cannot re-execute (a Port-shaped adapter), so nothing is retried.
func (w *Worker) retryAfter(lease executionstore.Lease, backend ExecutionBackend, ref string, out effect.Outcome) (string, bool) {
	if w.retry.MaxAttempts <= 0 || !retryableFailure(out.Result) {
		return "", false
	}
	ctx := w.lifecycle
	state, _, ok, err := w.store.Load(ctx, lease.Key)
	if err != nil || !ok || state.Lease.Owner != w.id || state.Lease.Epoch != lease.Epoch {
		return "", false
	}
	attempts := len(state.Superseded) + 1
	if attempts >= w.retry.MaxAttempts {
		return "", false
	}
	if delay := w.retry.Backoff * time.Duration(attempts); delay > 0 && !w.waitOwned(lease, delay) {
		return "", false
	}
	fresh, err := backend.Restart(ctx, ref, state.Assignment)
	if err != nil || fresh == "" || fresh == ref {
		return "", false
	}
	if err := w.restarted(ctx, lease, state.ExecutionRef, fresh); err != nil {
		return "", false
	}
	// The next generation of frames starts here; what the receiver saw of
	// the failed attempt is void (RUN-EXE-12).
	w.progress.Reset(lease.Key)
	if err := backend.Start(context.WithoutCancel(ctx), fresh, state.Assignment); err != nil && !errors.Is(err, effect.ErrDispatchUnknown) {
		// The retry itself was refused before starting: settle the original
		// failure rather than loop on the refusal.
		return "", false
	}
	return fresh, true
}

// retryableFailure reports whether a Known outcome declares itself worth a
// second execution (RUN-EXE-11): the disposition the effect layer derived
// for a model failure, or the one the tool gave its failure.
func retryableFailure(result effect.OutcomeResult) bool {
	switch r := result.(type) {
	case effect.ModelFailed:
		return r.Retry() == run.RetryAllowed
	case effect.ToolExecutionFailed:
		return r.Retry == run.RetryAllowed
	default:
		return false
	}
}

func (w *Worker) waitOwned(lease executionstore.Lease, delay time.Duration) bool {
	if !w.ownershipIntact(lease) {
		return false
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-w.lifecycle.Done():
		return false
	case <-timer.C:
		return w.ownershipIntact(lease)
	}
}

// ownershipIntact reports whether this Worker incarnation still owns the
// execution: same owner, same Epoch, unsettled. A lapsed lease does not end
// ownership — Renew re-establishes it once transient store errors stop — so
// watchers keep polling through outages shorter than adoption. A missing
// ledger, a settled execution, or a re-acquired one (a higher Epoch) end
// ownership; the heartbeat then exits too, because Renew reports
// ErrLeaseLost for them.
func (w *Worker) ownershipIntact(lease executionstore.Lease) bool {
	state, _, ok, err := w.store.Load(w.lifecycle, lease.Key)
	if err != nil {
		// A transient read error must not stop the watcher; the caller's
		// backoff retries the ownership check.
		return true
	}
	if !ok {
		return false
	}
	return !state.Terminal() && state.Lease.Owner == w.id && state.Lease.Epoch == lease.Epoch
}

func (w *Worker) heartbeat(lease executionstore.Lease, done <-chan struct{}) {
	interval := w.lease / 3
	if interval < 10*time.Millisecond {
		interval = 10 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-w.lifecycle.Done():
			return
		case <-ticker.C:
			if err := w.store.Renew(w.lifecycle, lease, w.lease); err != nil {
				if errors.Is(err, executionstore.ErrLeaseLost) || errors.Is(err, effect.ErrExecutionNotFound) {
					return
				}
				// Transient store errors must not silently stop lease
				// maintenance; the next tick retries. If the lease nonetheless
				// expires, the next RecoverExecution re-adopts the execution.
			}
		}
	}
}

// finishOwned settles the execution under lease. A settlement another writer
// made first (a Dispose, or a watcher of a later Epoch) is the execution's;
// this one is dropped and dispatchErr, the error the caller was going to
// report, is returned as it was.
func (w *Worker) finishOwned(ctx context.Context, lease executionstore.Lease, outcome *protocol.OutcomeEnvelope, state effect.ExecutionStatus, dispatchErr error) error {
	err := w.commit(ctx, lease, lease.Key, func(current *executionstore.Execution, _ executionstore.Head) (*executionstore.Commit, error) {
		if current.Terminal() {
			return nil, nil
		}
		return w.settlement(lease.Key, outcome, state), nil
	})
	switch {
	case err == nil:
	case errors.Is(err, executionstore.ErrLeaseLost):
		return dispatchErr
	default:
		return err
	}
	w.progress.End(lease.Key)
	w.settled(lease.Key)
	return dispatchErr
}
