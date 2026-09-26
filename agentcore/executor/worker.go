// Package executor makes provider effects durable: a Worker owns the
// execution record of every accepted Assignment -- its payload, its
// ExecutionRef, its lifecycle state and its Outcome -- and hands the effect
// itself to one Backend chosen once, at Dispatch (RUN-EXE-3, RUN-EXE-10).
// Agent Core sees the Worker as an effect.ExecutionPort addressed by AssignmentKey;
// Backends see only the Ref the record holds for them.
package executor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/executor/protocol"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/schema"
)

// WorkerOptions configure one Worker incarnation.
type WorkerOptions struct {
	// ID identifies this process incarnation. It must not be reused by a
	// restarted process while an older incarnation could still be alive.
	ID            string
	LeaseDuration time.Duration
	// Clock reads the lease clock. It must agree with the Store's clock; the
	// Store remains the fencing authority. Defaults to time.Now.
	Clock func() time.Time
	// Retry is the deployment's budget for re-dispatching an effect after a
	// Known failure that declares itself retryable (RUN-EXE-11): at most
	// MaxAttempts executions in total, Backoff multiplied by the attempts so
	// far between them. The zero value disables retries; whether a given
	// failure is worth retrying is the failure's own disposition.
	Retry RetryBudget
	// Progress is the hub the Worker's backends publish progress into and
	// the Worker serves through Progress (RUN-EXE-12); nil builds one with
	// the default window. The composer hands the same hub to its backends.
	Progress *ProgressHub
	// Settlements is the hub the Worker records settlements into and serves
	// through Settlements (effect.SettlementPort); nil builds one for this
	// incarnation with the default window.
	Settlements *SettlementHub
}

// RetryBudget bounds the Worker's retries of one effect.
type RetryBudget struct {
	MaxAttempts int
	Backoff     time.Duration
}

const defaultLeaseDuration = 30 * time.Second

// Worker owns execution leases, not Session ownership. It selects a Backend
// for an Assignment once, persists the resulting ExecutionRef, and from then
// on resolves record -> provider -> Backend for every lifecycle operation.
//
// The Worker knows how to recover one execution and nothing about when:
// RecoverExecution (effect.Recoverer) takes an expired record back under a
// live lease and continues it from the persisted payload and Ref, and
// Dispose settles a record its caller has given up on. Which records to
// recover, when to ask and when to give up are decisions of whoever observes
// the record as orphaned — the Owner reconciling its own Run (RUN-CMT-7) or
// an external controller — and the Worker runs no loop of its own
// (RUN-EXE-6). Neither operation is part of effect.ExecutionPort, which
// stays the per-assignment data plane.
type Worker struct {
	store     executionstore.Store
	routes    []Route
	backends  map[string]ExecutionBackend
	id        string
	lease     time.Duration
	now       func() time.Time
	lifecycle context.Context
	stop      context.CancelFunc
	// wg counts the goroutines the Worker started: each record's heartbeat
	// and watcher. Close cancels lifecycle and waits.
	wg sync.WaitGroup

	retry       RetryBudget
	progress    *ProgressHub
	settlements *SettlementHub
	// notices is the Worker\'s one subscription per Backend to the Backend\'s
	// settled Refs (notice.Source); observe waits on it.
	notices backendNotices
}

// NewWorker builds a Worker over records with the given routes; the last
// route is normally Default. A Worker with no route serves nothing.
func NewWorker(ctx context.Context, records executionstore.Store, routes []Route, options ...WorkerOptions) (*Worker, error) {
	if records == nil {
		return nil, errors.New("executor: nil worker store")
	}
	if len(routes) == 0 {
		return nil, errors.New("executor: worker requires at least one route")
	}
	backends := make(map[string]ExecutionBackend, len(routes))
	for _, r := range routes {
		if r.Provider == "" || r.Backend == nil {
			return nil, errors.New("executor: route requires a provider and a backend")
		}
		if _, dup := backends[r.Provider]; dup {
			return nil, fmt.Errorf("executor: duplicate provider %q", r.Provider)
		}
		backends[r.Provider] = r.Backend
	}
	var opts WorkerOptions
	if len(options) > 0 {
		opts = options[0]
	}
	if opts.ID == "" {
		var raw [16]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return nil, fmt.Errorf("executor: generate worker id: %w", err)
		}
		opts.ID = "worker-" + hex.EncodeToString(raw[:])
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = defaultLeaseDuration
	}
	now := opts.Clock
	if now == nil {
		now = time.Now
	}
	lifecycle, stop := context.WithCancel(context.WithoutCancel(ctx))
	progress := opts.Progress
	if progress == nil {
		progress = NewProgressHub(0)
	}
	settlements := opts.Settlements
	if settlements == nil {
		settlements = NewSettlementHub(opts.ID+"/"+strconv.FormatInt(now().UnixNano(), 36), 0)
	}
	w := &Worker{store: records, routes: routes, backends: backends, id: opts.ID, lease: opts.LeaseDuration, now: now,
		retry: opts.Retry, progress: progress, settlements: settlements,
		lifecycle: lifecycle, stop: stop}
	w.notices.worker = w
	if err := w.recover(ctx); err != nil {
		stop()
		return nil, err
	}
	return w, nil
}

// spawn runs fn as a Worker goroutine counted by Close.
func (w *Worker) spawn(fn func()) {
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		fn()
	}()
}

// Close stops every heartbeat and every watcher, and
// waits for them. Records keep their leases until they expire: another
// incarnation adopts them through RecoverExecution (RUN-EXE-6). Close
// does not cancel backend executions.
func (w *Worker) Close() {
	w.stop()
	w.wg.Wait()
	w.settlements.Close()
}

// route selects the Backend for an Assignment: the first Route whose Match
// accepts it (RUN-EXE-10).
func (w *Worker) route(a effect.Assignment) (Route, error) {
	for _, r := range w.routes {
		if r.Match == nil || r.Match(a) {
			return r, nil
		}
	}
	return Route{}, fmt.Errorf("%w: no route accepts the assignment", ErrUnknownProvider)
}

// backend resolves the Backend a record's ExecutionRef names.
func (w *Worker) backend(ref ExecutionRef) (ExecutionBackend, error) {
	b, ok := w.backends[ref.Provider]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownProvider, ref.Provider)
	}
	return b, nil
}

func (w *Worker) Validate(ctx context.Context, a effect.Assignment) (*run.ToolFailure, error) {
	r, err := w.route(a)
	if err != nil {
		return nil, err
	}
	return r.Backend.Validate(ctx, a)
}

// --- ledger helpers ---

// commitFn decides the commit to append from the current fold of a ledger;
// a nil commit means there is nothing to do. The Seq is the caller's.
type commitFn func(state *executionstore.Execution, head executionstore.Head) (*executionstore.Commit, error)

// commit loads the key's fold, lets fn decide, and appends under lease (a
// zero lease is an unfenced append). A commit the ledger already holds is
// success; a Seq conflict reloads and decides again, so a Worker never
// writes from a fold another writer has moved past.
func (w *Worker) commit(ctx context.Context, lease executionstore.Lease, key effect.AssignmentKey, fn commitFn) error {
	for attempt := 0; ; attempt++ {
		state, head, ok, err := w.store.Load(ctx, key)
		if err != nil {
			return err
		}
		if !ok {
			return effect.ErrExecutionNotFound
		}
		c, err := fn(&state, head)
		if err != nil || c == nil {
			return err
		}
		c.Seq = head.Next
		err = w.store.Append(ctx, lease, key, *c)
		switch {
		case err == nil, errors.Is(err, executionstore.ErrAlreadyApplied):
			return nil
		case errors.Is(err, executionstore.ErrConflict) && attempt < 8:
			continue
		default:
			return err
		}
	}
}

func (w *Worker) event(typ executionstore.EventType, payload any) executionstore.Event {
	ev, err := executionstore.NewEvent(typ, w.now().UnixMilli(), payload)
	if err != nil {
		// The payloads are the store's own closed types; encoding cannot fail.
		panic(err)
	}
	return ev
}

// transition is the commitFn of one state-machine step under lease: it
// appends typ when the fold allows it, does nothing when the fold already
// stands at to (a replay of this step), and is ErrStateConflict when the
// fold has moved somewhere the step cannot follow (a settlement or a cancel
// another writer landed first), so the caller stops rather than acts on a
// step it did not take.
func (w *Worker) transition(lease executionstore.Lease, typ executionstore.EventType, to effect.ExecutionStatus, command string) commitFn {
	return func(state *executionstore.Execution, _ executionstore.Head) (*executionstore.Commit, error) {
		if state.State == to {
			return nil, nil
		}
		if !executionstore.LegalTransition(state.State, to) {
			return nil, fmt.Errorf("%w: %s from %s", executionstore.ErrStateConflict, typ, state.State)
		}
		return &executionstore.Commit{CommitID: executionstore.DeriveCommitID(lease.Key, command, fmt.Sprintf("%d/%s", uint64(lease.Epoch), state.State)), Events: []executionstore.Event{w.event(typ, nil)}}, nil
	}
}

// holdsLease reports whether this Worker's lease on the fold is live.
func (w *Worker) holdsLease(s *executionstore.Execution) bool {
	return !s.Terminal() && s.Lease.Owner == w.id && s.Lease.Epoch > 0 && s.Lease.UntilUnixMilli > w.now().UnixMilli()
}

// Dispatch accepts an Assignment (RUN-EXE-3): it selects the Backend,
// prepares the Ref, opens the execution's ledger with the acceptance and
// the binding in one commit, then starts the execution. A replay of the
// same Assignment is answered from the ledger; recovery of an existing
// execution goes through RecoverExecution.
func (w *Worker) Dispatch(ctx context.Context, a effect.Assignment) error {
	if a.Body == nil {
		return errors.New("executor: assignment without body")
	}
	if model, ok := a.Model(); ok {
		if model.Request == nil {
			return errors.New("executor: model assignment requires an inline request payload")
		}
		requestDigest, err := schema.Canonical().DigestRequest(*model.Request)
		if err != nil {
			return err
		}
		if requestDigest != model.RequestDigest || model.Request.Model != string(model.Model) {
			return errors.New("executor: model request digest or model mismatch")
		}
	}
	digest, err := a.Digest()
	if err != nil {
		return err
	}
	key := a.Key()
	// A key whose ledger is already open is answered from the ledger before
	// any backend is touched: a replay, a conflicting Assignment or a
	// tombstone prepares nothing (RUN-EXE-3, RUN-EXE-16).
	if state, _, ok, loadErr := w.store.Load(ctx, key); loadErr != nil {
		return fmt.Errorf("%w: %w", effect.ErrDispatchRetryable, loadErr)
	} else if ok {
		return w.opened(ctx, &state, digest)
	}
	route, err := w.route(a)
	if err != nil {
		return err
	}
	ref, err := route.Backend.Prepare(ctx, a)
	if err != nil {
		return err
	}
	if ref == "" {
		return errors.New("executor: backend prepared an empty execution ref")
	}
	c := executionstore.Commit{Seq: 0, CommitID: executionstore.AcceptCommitID(key), Events: []executionstore.Event{
		w.event(executionstore.EventExecutionAccepted, executionstore.Accepted{Assignment: a}),
		w.event(executionstore.EventExecutionBound, executionstore.Bound{Ref: ExecutionRef{Provider: route.Provider, Ref: ref}}),
	}}
	err = w.store.Append(ctx, executionstore.Lease{}, key, c)
	switch {
	case err == nil:
	case errors.Is(err, executionstore.ErrAlreadyApplied), errors.Is(err, executionstore.ErrConflict):
		// The ledger was opened between the read above and this write (a
		// concurrent Dispatch, an Abort): the ledger, not this Worker,
		// decided (RUN-EXE-14, RUN-EXE-16).
		state, _, ok, loadErr := w.store.Load(ctx, key)
		if loadErr != nil {
			return fmt.Errorf("%w: %w", effect.ErrDispatchRetryable, loadErr)
		}
		if !ok {
			return fmt.Errorf("%w: ledger conflict without a ledger", effect.ErrDispatchRetryable)
		}
		return w.opened(ctx, &state, digest)
	default:
		// The ledger store, not the Assignment, refused: nothing started,
		// and the same Dispatch may succeed later (RUN-EXE-3).
		return fmt.Errorf("%w: %w", effect.ErrDispatchRetryable, err)
	}
	if err := w.acquireAndStart(ctx, key); err != nil {
		if errors.Is(err, effect.ErrDispatchUnknown) {
			return err
		}
		// Between acceptance and Backend.Start only this Worker's own store
		// operations can fail; a Start failure settles the execution instead
		// of returning. The ledger is open and Accepted: a replay of the same
		// Dispatch continues it once no lease is live on it (opened), and
		// the Reconciler's RecoverExecution does the same for an acceptance
		// it finds orphaned.
		return fmt.Errorf("%w: %w", effect.ErrDispatchRetryable, err)
	}
	return nil
}

// opened answers a Dispatch whose key already has a ledger: a tombstone is a
// definite rejection; another Assignment is a conflict; the same Assignment
// acknowledges the acceptance and starts nothing new, with one exception: an
// acceptance that never started and that no live lease holds is continued,
// because the Dispatch that wrote it failed before Start (or its Worker died
// before Start) and this replay is the same request (RUN-EXE-3). An
// execution that has started belongs to its lease holder, or to
// RecoverExecution once the Owner observes it orphaned. The ledger holds one
// acceptance per key and does not compare content, so telling the two
// Assignments apart is the Worker's reading (RUN-EXE-14).
func (w *Worker) opened(ctx context.Context, state *executionstore.Execution, digest run.Digest) error {
	if state.Aborted() {
		return effect.ErrExecutionAborted
	}
	accepted, err := state.Assignment.Digest()
	if err != nil {
		return err
	}
	if accepted != digest {
		return executionstore.ErrAssignmentConflict
	}
	liveLease := state.Lease.Epoch > 0 && state.Lease.UntilUnixMilli > w.now().UnixMilli()
	if state.State != effect.ExecutionAccepted || liveLease {
		return nil
	}
	err = w.acquireAndStart(ctx, state.Assignment.Key())
	if err != nil && !errors.Is(err, effect.ErrDispatchUnknown) {
		return fmt.Errorf("%w: %w", effect.ErrDispatchRetryable, err)
	}
	return err
}

// Abort closes key before any acceptance (RUN-EXE-16): it writes the
// tombstone as the first commit of the key's ledger, where it contends with
// the acceptance a Dispatch writes; the store's Seq rule lets exactly one of
// the two stand. The result is the key's attachment afterwards: aborted when
// the tombstone stands, whether written now or earlier; otherwise the live
// state of the acceptance that won.
func (w *Worker) Abort(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	c := executionstore.Commit{Seq: 0, CommitID: executionstore.AbortCommitID(key), Events: []executionstore.Event{
		w.event(executionstore.EventExecutionAborted, executionstore.Aborted{Reason: "closed by its controller before acceptance"}),
	}}
	err := w.store.Append(ctx, executionstore.Lease{}, key, c)
	if err != nil && !errors.Is(err, executionstore.ErrAlreadyApplied) && !errors.Is(err, executionstore.ErrConflict) {
		return effect.Attachment{}, err
	}
	return w.Attach(ctx, key)
}

// RecoverExecution is effect.Recoverer: it takes the execution of key back
// under this Worker's lease when the previous lease expired or was never
// held, and continues it from the persisted payload and Ref — attaching the
// previous backend execution, restarting it once the backend proves it
// missing, or settling it as Unknown when the tool's replay declaration
// forbids a restart (RUN-EXE-3, RUN-EXE-9). A settled execution, one already
// under this Worker's lease and one under another live lease are left as
// they are. The caller has observed the execution as orphaned; how often to
// ask again, and when to give up through Dispose, is the caller's.
func (w *Worker) RecoverExecution(ctx context.Context, key effect.AssignmentKey) error {
	state, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if state.Terminal() {
		return nil
	}
	if state.ExecutionRef.Provider != "" {
		if _, err := w.backend(state.ExecutionRef); err != nil {
			return err
		}
	}
	if w.holdsLease(&state) {
		return nil
	}
	return w.acquireAndStart(ctx, key)
}

// Acknowledge is effect.Acknowledger (RUN-EXE-13): the Owner reports that
// the Outcome of key is settled as a Session fact. The ledger records
// outcome_acknowledged and its fold collects the execution: the payload,
// Outcome and Superseded refs leave the fold, while the key, its digest,
// state and ExecutionRef remain, so the execution still answers Attach with
// terminal, refuses a Dispatch that would execute the key again, and answers
// GetOutcome with ErrOutcomeCollected. The execution must be settled;
// acknowledging one still in flight is a caller error (ErrStateConflict),
// and a key without a ledger is ErrExecutionNotFound. Repeating it changes
// nothing.
func (w *Worker) Acknowledge(ctx context.Context, key effect.AssignmentKey) error {
	return w.commit(ctx, executionstore.Lease{}, key, func(state *executionstore.Execution, _ executionstore.Head) (*executionstore.Commit, error) {
		if !state.Terminal() {
			return nil, fmt.Errorf("%w: acknowledging an execution in state %s", executionstore.ErrStateConflict, state.State)
		}
		if state.Acknowledged || state.Aborted() {
			return nil, nil // nothing was served, nothing to collect
		}
		return &executionstore.Commit{CommitID: executionstore.AcknowledgeCommitID(key),
			Events: []executionstore.Event{w.event(executionstore.EventOutcomeAcknowledged, nil)}}, nil
	})
}

// Dispose settles an unsettled execution as Unknown without re-dispatching
// it. The caller has given the execution up: it will not be recovered, and
// the Owner disposes the Run target on its next read (RUN-CMT-7). Unlike
// RecoverExecution, Dispose is unconditional — it also applies to executions
// whose lease holder is dead or absent — and unlike Cancel it does not
// require backend reachability: the backend is cancelled best-effort after
// the settle.
func (w *Worker) Dispose(ctx context.Context, key effect.AssignmentKey) error {
	var ref ExecutionRef
	err := w.commit(ctx, executionstore.Lease{}, key, func(state *executionstore.Execution, _ executionstore.Head) (*executionstore.Commit, error) {
		ref = state.ExecutionRef
		if state.Terminal() {
			return nil, nil
		}
		env := protocol.OutcomeEnvelope{ProtocolVersion: protocol.ProtocolVersion, Key: key, Unknown: true,
			Error: &protocol.WireError{Code: "disposed", Message: "execution disposed by its controller"}}
		return w.settlement(key, &env, effect.ExecutionUnknown), nil
	})
	if err != nil {
		return err
	}
	if b, err := w.backend(ref); err == nil {
		_ = b.Cancel(context.WithoutCancel(ctx), ref.Ref)
	}
	w.settled(key)
	return nil
}

// settlement is the commit that ends an execution: execution_settled under
// the settle CommitID, so the watcher's settle and a controller's Dispose
// race for one identity and the loser reads the winner's Outcome.
func (w *Worker) settlement(key effect.AssignmentKey, outcome *protocol.OutcomeEnvelope, state effect.ExecutionStatus) *executionstore.Commit {
	return &executionstore.Commit{CommitID: executionstore.SettleCommitID(key),
		Events: []executionstore.Event{w.event(executionstore.EventExecutionSettled, executionstore.Settled{State: state, Outcome: *outcome})}}
}

// settled records that key reached a terminal state in the ledger: the
// notice every Settlements subscriber waits for. It follows the commit; a
// subscriber that reads before the notice arrives finds the Outcome anyway.
func (w *Worker) settled(key effect.AssignmentKey) {
	w.settlements.Record(key)
}

// Settlements is effect.SettlementPort: the settlements this Worker wrote,
// from its hub.
func (w *Worker) Settlements(ctx context.Context, epoch string, after uint64, fn func(effect.Settlement) bool) error {
	return w.settlements.Settlements(ctx, epoch, after, fn)
}

// Progress is effect.ProgressPort (RUN-EXE-12): the frames of an execution
// this Worker's backends published, from the hub. An execution this Worker
// holds but whose Backend is itself a port (a remote Worker behind
// PortBackend) is served from that port, so a chain of Workers relays the
// frames of the one that runs the effect. A key without a ledger is
// ErrExecutionNotFound; a settled execution the hub no longer holds ends the
// stream at once.
func (w *Worker) Progress(ctx context.Context, key effect.AssignmentKey, after uint64, fn func(effect.ProgressFrame) bool) error {
	if w.progress.Known(key) {
		return w.progress.Progress(ctx, key, after, fn)
	}
	state, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if backend, err := w.backend(state.ExecutionRef); err == nil {
		if relay, ok := backend.(effect.ProgressPort); ok {
			return relay.Progress(ctx, key, after, fn)
		}
	}
	if state.Terminal() {
		return nil
	}
	return w.progress.Progress(ctx, key, after, fn)
}

// Attach reports the execution's observation state (RUN-EXE-3). The ledger
// is the authority: missing without a ledger, terminal once settled,
// orphaned when no incarnation holds a live lease on it, active while one
// does. Which Worker answers does not matter: a live lease held by another
// incarnation is proof of its heartbeat, and its watcher settles the Outcome
// into the same store GetOutcome reads. Only for its own live lease does
// this Worker also ask the Backend, and a Backend that no longer finds the
// Ref makes the execution orphaned until RecoverExecution restarts it or
// Dispose settles it.
func (w *Worker) Attach(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	state, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return effect.Attachment{}, err
	}
	if !ok {
		// Dispatch opens the ledger before any Start and the store is
		// durable, so a key without a ledger never started: missing is a
		// proof (RUN-EXE-3).
		return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
	}
	if state.Aborted() {
		return effect.Attachment{State: effect.AttachmentAborted, Execution: effect.ExecutionAborted}, nil
	}
	attachment := effect.Attachment{Execution: state.State, Owner: state.Lease.Owner, FencingEpoch: uint64(state.Lease.Epoch), LeaseUntilUnixMilli: state.Lease.UntilUnixMilli}
	if state.Terminal() {
		attachment.State = effect.AttachmentTerminal
		attachment.BackendAttached = false
		return attachment, nil
	}
	if state.Lease.Epoch == 0 || state.Lease.UntilUnixMilli <= w.now().UnixMilli() {
		attachment.State = effect.AttachmentOrphaned
		return attachment, nil
	}
	if state.Lease.Owner != w.id {
		attachment.State = effect.AttachmentActive
		return attachment, nil
	}
	backend, err := w.backend(state.ExecutionRef)
	if err != nil {
		return effect.Attachment{}, err
	}
	backendAttachment, err := backend.Attach(ctx, state.ExecutionRef.Ref)
	if err != nil {
		return effect.Attachment{}, err
	}
	attachment.BackendAttached = backendAttachment.State == effect.AttachmentActive || backendAttachment.State == effect.AttachmentTerminal
	if attachment.BackendAttached {
		attachment.State = effect.AttachmentActive
	} else {
		attachment.State = effect.AttachmentOrphaned
	}
	return attachment, nil
}

// GetStatus is the execution's state; while it is with the backend
// (Dispatching, Running, CancelRequested) it is the backend's view of the Ref.
func (w *Worker) GetStatus(ctx context.Context, key effect.AssignmentKey) (effect.ExecutionStatus, error) {
	state, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return effect.ExecutionNotFound, err
	}
	if !ok {
		return effect.ExecutionNotFound, effect.ErrExecutionNotFound
	}
	switch state.State {
	case effect.ExecutionDispatching, effect.ExecutionRunning, effect.ExecutionCancelRequested:
		backend, err := w.backend(state.ExecutionRef)
		if err != nil {
			return state.State, err
		}
		return backend.Status(ctx, state.ExecutionRef.Ref)
	}
	return state.State, nil
}

// GetOutcome is effect.ExecutionPort's read of the key's Outcome: one fold
// of the ledger, answered at once. An unsettled execution is
// effect.ErrOutcomeNotReady; when to read again is what Settlements tells a
// subscriber, so no request is held open for the length of an execution.
func (w *Worker) GetOutcome(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	state, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return effect.Outcome{}, err
	}
	if !ok {
		return effect.Outcome{}, effect.ErrExecutionNotFound
	}
	if state.Aborted() {
		return effect.Outcome{}, effect.ErrExecutionAborted
	}
	if state.Acknowledged {
		return effect.Outcome{}, effect.ErrOutcomeCollected
	}
	if state.Outcome == nil {
		return effect.Outcome{}, effect.ErrOutcomeNotReady
	}
	return protocol.DecodeOutcome(state.Outcome), nil
}

// GetOutcomeEnvelope returns the persisted wire outcome without losing the
// stable error/status representation used by the HTTP binding.
func (w *Worker) GetOutcomeEnvelope(ctx context.Context, key effect.AssignmentKey) (protocol.OutcomeEnvelope, error) {
	if _, err := w.GetOutcome(ctx, key); err != nil {
		return protocol.OutcomeEnvelope{}, err
	}
	state, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return protocol.OutcomeEnvelope{}, err
	}
	if !ok || state.Outcome == nil {
		return protocol.OutcomeEnvelope{}, effect.ErrOutcomeNotReady
	}
	return *state.Outcome, nil
}

func (w *Worker) Cancel(ctx context.Context, key effect.AssignmentKey) error {
	state, _, ok, err := w.store.Load(ctx, key)
	if err != nil {
		return err
	}
	if !ok {
		return effect.ErrExecutionNotFound
	}
	if state.Terminal() {
		return nil
	}
	backend, err := w.backend(state.ExecutionRef)
	if err != nil {
		return err
	}
	lease := state.Lease
	if err := w.requestCancelOwned(ctx, lease); err != nil {
		return err
	}
	return backend.Cancel(ctx, state.ExecutionRef.Ref)
}

// requestCancelOwned commits cancel_requested under the current lease; an
// execution already cancelling or settled is left as it is.
func (w *Worker) requestCancelOwned(ctx context.Context, lease executionstore.Lease) error {
	err := w.commit(ctx, lease, lease.Key, w.transition(lease, executionstore.EventCancelRequested, effect.ExecutionCancelRequested, "cancel"))
	if errors.Is(err, executionstore.ErrStateConflict) {
		// Settled between the caller's read and this write: nothing left to
		// cancel, and the caller's backend Cancel is harmless on a finished
		// execution.
		return nil
	}
	return err
}

// recover resumes observation of the executions this incarnation still
// holds after a restart with the same ID. It is the lease holder's own duty
// and reads only its own leases; it adopts nothing.
func (w *Worker) recover(ctx context.Context) error {
	keys, err := w.store.ListOwned(ctx, w.id)
	if err != nil {
		return err
	}
	for _, key := range keys {
		state, _, ok, err := w.store.Load(ctx, key)
		if err != nil {
			return err
		}
		if !ok || !w.holdsLease(&state) {
			continue
		}
		backend, err := w.backend(state.ExecutionRef)
		if err != nil {
			continue
		}
		attachment, attachErr := backend.Attach(ctx, state.ExecutionRef.Ref)
		if attachErr != nil {
			// One broken backend read must not block recovery of the other
			// executions; a later RecoverExecution retries this one.
			continue
		}
		if attachment.State == effect.AttachmentActive || attachment.State == effect.AttachmentTerminal {
			lease := state.Lease
			done := make(chan struct{})
			w.spawn(func() { w.heartbeat(lease, done) })
			w.spawn(func() { w.watch(lease, backend, state.ExecutionRef.Ref, done) })
		}
	}
	return nil
}

var (
	_ effect.ExecutionPort  = (*Worker)(nil)
	_ effect.Acknowledger   = (*Worker)(nil)
	_ effect.Recoverer      = (*Worker)(nil)
	_ effect.SettlementPort = (*Worker)(nil)
)
