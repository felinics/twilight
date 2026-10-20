package executor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/felinics/twilight/agentcore/executor/protocol"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
)

// This file is the supervision of one execution from the moment the Worker
// holds its lease to the moment its settlement is committed: acquiring the
// lease and heartbeating it, attaching to or replaying the Backend's
// execution, starting it, observing its Outcome, retrying a failure that
// declares itself retryable, and settling. The port methods the Worker
// exposes (Dispatch, Abort, Attach, GetOutcome, Cancel,...) stay in
// worker.go and hand a key to acquireAndStart; nothing here is reachable
// from outside the package.

// acquireAndStart takes the key's lease and continues the execution from its
// fold: an execution with an attachable backend
// execution is observed, one whose execution the backend proves missing is
// re-started (a model, or a tool whose Replay policy allows it) or settled
// Unknown (any other tool), one whose execution the backend
// cannot confirm (orphaned) is held under the lease and asked about again
// until the backend can, and one never started is started.
func (w *Worker) acquireAndStart(caller context.Context, key effect.AssignmentKey) error {
	if !w.beginWork() {
		return context.Canceled
	}
	defer w.wg.Done()
	// Acceptance has handed the assignment to the Worker. Ending an
	// activation must not interrupt local preparation after a lease or
	// dispatch intent was committed. Bound preparation by this incarnation
	// and a timeout; the caller still bounds a Start RPC already in flight.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(caller), w.startTimeout)
	stop := context.AfterFunc(w.lifecycle, cancel)
	defer func() { stop(); cancel() }()
	if w.lifecycle.Err() != nil {
		cancel()
	}
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
	// committed first.
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
		err = w.commit(ctx, lease, key, func(state *executionstore.Execution, _ ledger.Head) (*ledger.Commit, error) {
			if state.ExecutionRef.Provider != "" {
				return nil, nil
			}
			return &ledger.Commit{CommitID: executionstore.DeriveCommitID(key, "bind", fmt.Sprint(uint64(lease.Epoch))),
				Batches: []ledger.EventBatch{{Events: []ledger.Event{w.event(executionstore.EventExecutionBound, executionstore.Bound{Ref: bound})}}}}, nil
		})
		if err != nil {
			return err
		}
		claimed.ExecutionRef = bound
	}
	if claimed.State == effect.ExecutionCancelRequested && claimed.ExecutionRef.Provider == "" {
		return w.finishOwned(ctx, lease, &protocol.OutcomeEnvelope{
			ProtocolVersion: protocol.ProtocolVersion, Key: key, Cancelled: true,
		}, effect.ExecutionCancelled)
	}
	backend, err := w.backend(claimed.ExecutionRef)
	if err != nil {
		return err
	}
	ref := claimed.ExecutionRef.Ref
	if claimed.State == effect.ExecutionCancelRequested {
		leaseDone := make(chan struct{})
		w.spawn(func() { w.heartbeat(lease, leaseDone) })
		return w.cancelOwned(ctx, lease, backend, ref, leaseDone)
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
		return w.replay(ctx, caller, lease, &claimed, backend, ref, leaseDone)
	}
	return w.start(ctx, caller, lease, &claimed, backend, ref, leaseDone)
}

// cancelOwned never prepares, restarts or starts. A persisted binding may
// already have crossed the effect boundary; absent backend evidence is not
// proof of a side-effect-free cancellation.
func (w *Worker) cancelOwned(ctx context.Context, lease executionstore.Lease, backend ExecutionBackend, ref string, done chan struct{}) error {
	attachment, err := backend.Attach(ctx, ref)
	if err != nil {
		close(done)
		return err
	}
	cancelErr := backend.Cancel(ctx, ref)
	if attachment.State != effect.AttachmentActive && attachment.State != effect.AttachmentTerminal {
		env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: lease.Key, Unknown: true,
			Error: &protocol.WireError{Code: "cancel_reconciliation_unknown", Message: "cancelled execution is no longer attached"}}
		err := w.finishOwned(ctx, lease, &env, effect.ExecutionUnknown)
		close(done)
		return err
	}
	w.spawn(func() { w.watch(lease, backend, ref, done) })
	return cancelErr
}

// awaitBackend holds a taken-over execution whose backend could not confirm
// it (orphaned): it asks the backend again with backoff, under the
// heartbeat leaseDone ends, until the backend observes the execution (then
// it is watched), proves it missing (then it is replayed or settled as a
// missing one would be), or this incarnation loses the lease. Nothing is
// re-dispatched while the backend is undecided.
func (w *Worker) awaitBackend(lease executionstore.Lease, claimed *executionstore.Execution, backend ExecutionBackend, ref string, leaseDone chan struct{}) {
	delay := 10 * time.Millisecond
	for {
		if !w.waitOwned(lease, delay) {
			close(leaseDone)
			return
		}
		delay = min(delay*2, time.Second)
		if state, _, ok, err := w.store.Load(w.lifecycle, lease.Key); err == nil && ok && state.State == effect.ExecutionCancelRequested {
			cancelCtx, cancel := context.WithTimeout(w.lifecycle, w.lease)
			_ = w.cancelOwned(cancelCtx, lease, backend, ref, leaseDone)
			cancel()
			return
		}
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
			// A failed replay stops the heartbeat. The execution becomes
			// orphaned after lease expiry, when recovery can retry it.
			_ = w.replay(w.lifecycle, w.lifecycle, lease, claimed, backend, ref, leaseDone)
			return
		}
	}
}

// replay is what a takeover does with an execution the backend proved
// missing. A model is always replayed; a tool only when the Replay policy
// its Assignment carries allows it, because its lost execution may have
// crossed the effect boundary before its worker died. The policy
// travels with the Assignment, so a local and a remote Worker decide alike
// from the ledger; a forbidden or unjudged tool settles Unknown, the message
// naming the declaration. Restart replays the same Assignment as a new
// generation: it allocates the Ref of the new physical execution and the old
// Ref, just proved missing, moves to the audit trail as execution_restarted
// leaseDone ends the running heartbeat when nothing is left to
// watch.
func (w *Worker) replay(ctx, caller context.Context, lease executionstore.Lease, claimed *executionstore.Execution, backend ExecutionBackend, ref string, leaseDone chan struct{}) error {
	if tool, ok := claimed.Assignment.Tool(); ok && tool.Replay != turn.ReplayAllowed {
		env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: lease.Key, Unknown: true,
			Error: &protocol.WireError{Code: "adopted_without_replay", Message: fmt.Sprintf("tool %q declares replay %s; the lost execution is not re-dispatched", tool.ToolRef, tool.Replay)}}
		err := w.finishOwned(ctx, lease, &env, effect.ExecutionUnknown)
		close(leaseDone)
		return err
	}
	current, err := w.startable(ctx, lease, ref)
	if err != nil {
		if errors.Is(err, executionstore.ErrStateConflict) {
			return w.cancelOwned(ctx, lease, backend, ref, leaseDone)
		}
		close(leaseDone)
		return err
	}
	fresh, err := backend.Restart(ctx, ref, current.Assignment)
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
			// Restart allocates a Ref, not permission to execute it. A
			// cancellation committed during the RPC wins; never Start fresh.
			_ = backend.Cancel(ctx, fresh)
			w.spawn(func() { w.watch(lease, backend, ref, leaseDone) })
			if errors.Is(err, executionstore.ErrStateConflict) {
				return nil
			}
			return err
		}
		claimed.Superseded = append(claimed.Superseded, claimed.ExecutionRef)
		claimed.ExecutionRef.Ref = fresh
		ref = fresh
		w.progress.Reset(lease.Key)
	}
	return w.start(ctx, caller, lease, claimed, backend, ref, leaseDone)
}

// restarted commits execution_restarted: from leaves, fresh continues.
func (w *Worker) restarted(ctx context.Context, lease executionstore.Lease, from ExecutionRef, fresh string) error {
	return w.commit(ctx, lease, lease.Key, func(state *executionstore.Execution, _ ledger.Head) (*ledger.Commit, error) {
		if state.Terminal() || state.State == effect.ExecutionCancelRequested {
			return nil, executionstore.ErrStateConflict
		}
		if state.ExecutionRef.Ref == fresh {
			return nil, nil
		}
		if state.ExecutionRef != from {
			return nil, fmt.Errorf("%w: restart of %+v, ledger holds %+v", executionstore.ErrStateConflict, from, state.ExecutionRef)
		}
		return &ledger.Commit{CommitID: executionstore.DeriveCommitID(lease.Key, "restart", from.Ref),
			Batches: []ledger.EventBatch{{Events: []ledger.Event{w.event(executionstore.EventExecutionRestarted, executionstore.Restarted{Superseded: from, Ref: fresh})}}}}, nil
	})
}

// startable checks physical execution authority, not just lease ownership.
// Controller cancellation and restart commits are serialized by the ledger;
// each external boundary rechecks the latest fold as well.
func (w *Worker) startable(ctx context.Context, lease executionstore.Lease, ref string) (executionstore.Execution, error) {
	state, _, ok, err := w.store.Load(ctx, lease.Key)
	if err != nil {
		return state, err
	}
	if !ok {
		return state, effect.ErrExecutionNotFound
	}
	if state.Terminal() || state.State == effect.ExecutionCancelRequested || state.ExecutionRef.Ref != ref {
		return state, executionstore.ErrStateConflict
	}
	if !w.holdsLease(&state) || state.Lease.Epoch != lease.Epoch {
		return state, executionstore.ErrLeaseLost
	}
	return state, nil
}

// beginStart commits dispatch intent even on retries, then checks again just
// before the RPC. No state check can revoke a request already sent: a Cancel
// racing the external boundary must keep observing and cancelling the Ref.
func (w *Worker) beginStart(ctx context.Context, lease executionstore.Lease, ref string) error {
	err := w.commit(ctx, lease, lease.Key, func(state *executionstore.Execution, head ledger.Head) (*ledger.Commit, error) {
		if state.ExecutionRef.Ref != ref {
			return nil, executionstore.ErrStateConflict
		}
		return w.transition(lease, executionstore.EventExecutionStarted, effect.ExecutionDispatching, "start")(state, head)
	})
	if err != nil {
		return err
	}
	_, err = w.startable(ctx, lease, ref)
	return err
}

// callStart bounds only the synchronous RPC. Backends manage the external
// execution's lifetime independently and must honor this context for the RPC.
// Cancellation/timeout cannot prove that the effect was never dispatched.
func (w *Worker) callStart(ctx context.Context, backend ExecutionBackend, ref string, a effect.Assignment) error {
	if !w.beginWork() {
		return fmt.Errorf("%w: %w", effect.ErrDispatchUnknown, context.Canceled)
	}
	defer w.wg.Done()
	// No RPC has been sent yet. A vanished caller cannot revoke a durable
	// acceptance: start it under the Worker rather than watch a Ref that was
	// never started. Once sent, cancellation remains uncertain and must never
	// trigger a blind retry.
	if ctx.Err() != nil {
		ctx = w.lifecycle
	}
	rpc, cancel := context.WithTimeout(ctx, w.startTimeout)
	stop := context.AfterFunc(w.lifecycle, cancel)
	defer func() { stop(); cancel() }()
	if w.lifecycle.Err() != nil {
		cancel()
	}
	if err := rpc.Err(); err != nil {
		// The caller may have ended while the RPC context was being built.
		// We still have not invoked Start, so transfer this request too.
		if ctx.Err() != nil && w.lifecycle.Err() == nil {
			return w.callStart(w.lifecycle, backend, ref, a)
		}
		return fmt.Errorf("%w: %w", effect.ErrDispatchUnknown, err)
	}
	err := backend.Start(rpc, ref, a)
	if rpc.Err() != nil {
		return fmt.Errorf("%w: %w", effect.ErrDispatchUnknown, rpc.Err())
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %w", effect.ErrDispatchUnknown, err)
	}
	return err
}

// start moves the execution through Dispatching to Running around
// Backend.Start and hands it to a watcher; the heartbeat leaseDone ends is
// already running.
func (w *Worker) start(ctx, caller context.Context, lease executionstore.Lease, claimed *executionstore.Execution, backend ExecutionBackend, ref string, leaseDone chan struct{}) error {
	key := lease.Key
	if err := w.beginStart(ctx, lease, ref); err != nil {
		// A cancelling execution with a physical Ref remains observable.
		// A context ending before the RPC is also not proof of no effect
		// for a Dispatching Ref adopted from an earlier invocation.
		if errors.Is(err, executionstore.ErrStateConflict) || errors.Is(err, executionstore.ErrLeaseLost) {
			w.spawn(func() { w.watch(lease, backend, ref, leaseDone) })
			return nil
		}
		close(leaseDone)
		return err
	}
	startErr := w.callStart(caller, backend, ref, claimed.Assignment)
	// Preparation's deadline does not bound recording the RPC's result.
	// The Worker owns this continuation and keeps its lease until settlement.
	ctx = w.lifecycle
	if startErr != nil {
		if errors.Is(startErr, effect.ErrDispatchUnknown) {
			w.spawn(func() { w.watch(lease, backend, ref, leaseDone) })
			return startErr
		}
		env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key,
			Error: &protocol.WireError{Code: "dispatch_failed", Message: startErr.Error()}}
		if err := w.finishOwned(ctx, lease, &env, effect.ExecutionFailed); err != nil {
			// Retry only the known rejection's settlement, never Start or an
			// Outcome read of a Ref the backend definitively refused to run.
			w.spawn(func() {
				defer close(leaseDone)
				w.settleWithRetry(lease, &env, effect.ExecutionFailed, 10*time.Millisecond)
			})
			return err
		}
		close(leaseDone)
		return startErr
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
// Backend's notice that the Ref settled; a read at intervals
// bounded by the lease covers a Backend without notices and a notice that
// was lost, and every wait re-checks that this Worker still owns the record.
func (w *Worker) observe(lease executionstore.Lease, backend ExecutionBackend, ref *string) bool {
	if w.lifecycle.Err() != nil || !w.ownershipIntact(lease) {
		return false
	}
	wait := w.notices.await(backend, *ref)
	defer wait.cancel()
	delay := 10 * time.Millisecond
	var out effect.Outcome
	for {
		// Cancellation is a durable command, not cancellation of an RPC
		// context. Repeat it while observing a Ref that may already have
		// crossed the effect boundary, including a racing Start.
		if state, _, ok, err := w.store.Load(w.lifecycle, lease.Key); err == nil && ok && state.State == effect.ExecutionCancelRequested {
			cancelCtx, cancel := context.WithTimeout(w.lifecycle, w.lease)
			_ = backend.Cancel(cancelCtx, *ref)
			cancel()
		}
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
			// read that cannot change.
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
	// re-dispatched under the same ledger within the budget:
	// the old Ref joins the audit trail and the watch continues on the new one.
	if next, ok := w.retryAfter(lease, backend, *ref, out); ok {
		*ref = next
		return true
	}
	// The backend knows the Ref, not the attempt: the ledger's key is the
	// Outcome's key.
	out.Key = lease.Key
	env := protocol.EncodeOutcome(out)
	w.settleWithRetry(lease, &env, protocol.StatusForOutcome(out), delay)
	return false
}

// settleWithRetry commits the settlement, retrying transient store failures
// while this Worker still owns the record.
func (w *Worker) settleWithRetry(lease executionstore.Lease, env *protocol.OutcomeEnvelope, state effect.ExecutionStatus, delay time.Duration) {
	for {
		if err := w.finishOwned(w.lifecycle, lease, env, state); err == nil {
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
	state, err := w.startable(ctx, lease, ref)
	if err != nil {
		return "", false
	}
	attempts := len(state.Superseded) + 1
	if attempts >= w.retry.MaxAttempts {
		return "", false
	}
	if delay := w.retry.Backoff * time.Duration(attempts); delay > 0 && !w.waitOwned(lease, delay) {
		return "", false
	}
	// Backoff and backend reads may have overlapped the controller's
	// command. Ownership alone is not permission to retry.
	state, err = w.startable(ctx, lease, ref)
	if err != nil {
		return "", false
	}
	fresh, err := backend.Restart(ctx, ref, state.Assignment)
	if err != nil || fresh == "" || fresh == ref {
		return "", false
	}
	if err := w.restarted(ctx, lease, state.ExecutionRef, fresh); err != nil {
		_ = backend.Cancel(ctx, fresh)
		return "", false
	}
	// The next generation of frames starts here; what the receiver saw of
	// the failed attempt is void.
	w.progress.Reset(lease.Key)
	if err := w.beginStart(ctx, lease, fresh); err != nil {
		// The new binding was committed before cancellation. Observe it,
		// never the superseded outcome, even when Start is now forbidden.
		return fresh, true
	}
	if err := w.callStart(ctx, backend, fresh, state.Assignment); err != nil {
		if !errors.Is(err, effect.ErrDispatchUnknown) {
			_ = w.finishOwned(ctx, lease, &protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: lease.Key,
				Error: &protocol.WireError{Code: "dispatch_failed", Message: err.Error()}}, effect.ExecutionFailed)
		}
		return fresh, true
	}
	_ = w.commit(ctx, lease, lease.Key, w.transition(lease, executionstore.EventExecutionRunning, effect.ExecutionRunning, "run"))
	return fresh, true
}

// retryableFailure reports whether a Known outcome declares itself worth a
// second execution: the disposition the effect layer derived
// for a model failure, or the one the tool gave its failure.
func retryableFailure(result effect.OutcomeResult) bool {
	switch r := result.(type) {
	case effect.ModelFailed:
		return r.Retry() == turn.RetryAllowed
	case effect.ToolExecutionFailed:
		return r.Retry == turn.RetryAllowed
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
// this one is dropped without changing the settled outcome.
func (w *Worker) finishOwned(ctx context.Context, lease executionstore.Lease, outcome *protocol.OutcomeEnvelope, state effect.ExecutionStatus) error {
	err := w.commit(ctx, lease, lease.Key, func(current *executionstore.Execution, _ ledger.Head) (*ledger.Commit, error) {
		if current.Terminal() {
			return nil, nil
		}
		return w.settlement(lease.Key, outcome, state), nil
	})
	switch {
	case err == nil:
	case errors.Is(err, executionstore.ErrLeaseLost):
		return nil
	default:
		return err
	}
	w.progress.End(lease.Key)
	w.settled(lease.Key)
	return nil
}
