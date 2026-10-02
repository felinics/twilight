package execution

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agentcore/observe"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/effect/watch"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/redispatch"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/run/store"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// recovery holds the lifetime of every Session this process owns: the
// Outcome waits of the effects dispatched here and the takeover supervision.
// Open installs a Session's lifetime and runs the takeover disposition: the
// reconciler compares every Executing effect with the execution store, keeps
// waiting for attempts that survived, hands missing effects to the Executor
// again within the redispatch budget or disposes them. Every Outcome, of an
// effect dispatched here or of a kept attempt, settles through the Loop and
// is reported through notify; nothing here drives the Run on. Stop ends a Session's lifetime; Close ends every one.
type recovery struct {
	// runs is the Run module's Session adapter; recovery binds it to the
	// Writer the Session was opened with.
	runs  *sessionstore.SessionRunStore
	ports effect.Ports
	loop  *loop.Loop
	// watcher is where every Reconciler waits for Outcomes: the settlement
	// subscription shared with the Loop; required.
	watcher *watch.Watcher
	// fail receives failures of work done outside any caller's call, such as
	// settling a reattached Outcome; nil discards them.
	fail func(session.SessionID, error)
	// notify reports a settlement committed outside any caller's call; the
	// host advances the Session from there. nil discards.
	notify func(*lifetime)
	// missingEffects is the takeover policy for an Executing effect the
	// Executor holds nothing for: the zero value disposes,
	// reconcile.RedispatchMissing redispatches within the budget and
	// requires redispatches.
	missingEffects reconcile.MissingPolicy
	// redispatches is the dispatch ledger the reconciler writes before and
	// after it hands an effect to the Executor again; required under
	// RedispatchMissing, unused otherwise.
	redispatches redispatch.Store
	// maxRedispatches bounds redispatches per effect; zero selects the
	// reconciler's default.
	maxRedispatches int
	// progress is the transient stream the effects' frames are published to;
	// nil publishes none.
	progress *observe.Progresses

	mu        sync.Mutex
	lifetimes map[session.SessionID]*lifetime
}

// lifetime is the detached context a Session's background work -- the
// Outcome waits of the effects dispatched here, reattached outcome reads,
// resumed answers -- lives under, and the cancel that stops it. w is the
// Writer the Session was opened with; everything the lifetime settles
// commits through it. pending are the effects this process dispatched and
// still awaits, by key, each with the drop of its Watcher registration.
type lifetime struct {
	ctx    context.Context
	cancel context.CancelFunc
	w      writer.Writer

	mu      sync.Mutex
	pending map[effect.AssignmentKey]func()
	// lost is set once a settlement through w was fenced: another process
	// owns the Session, and every further step here is refused with it.
	lost error
}

// fenced is the ownership loss this lifetime recorded, nil while it owns.
func (lt *lifetime) fenced() error {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	return lt.lost
}

// track records an awaited effect; untrack forgets it once its Outcome is
// in hand. awaiting reports whether this lifetime still waits on key.
func (lt *lifetime) track(key effect.AssignmentKey, drop func()) {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	if lt.pending == nil {
		lt.pending = make(map[effect.AssignmentKey]func())
	}
	lt.pending[key] = drop
}

func (lt *lifetime) untrack(key effect.AssignmentKey) {
	lt.mu.Lock()
	delete(lt.pending, key)
	lt.mu.Unlock()
}

func (lt *lifetime) awaiting(key effect.AssignmentKey) bool {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	_, ok := lt.pending[key]
	return ok
}

// inFlight counts the effects of runID this lifetime still awaits.
func (lt *lifetime) inFlight(runID run.RunID) int {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	n := 0
	for key := range lt.pending {
		if key.RunID == runID {
			n++
		}
	}
	return n
}

// drain drops every registration and returns the keys that were still
// awaited.
func (lt *lifetime) drain() []effect.AssignmentKey {
	lt.mu.Lock()
	defer lt.mu.Unlock()
	keys := make([]effect.AssignmentKey, 0, len(lt.pending))
	for key, drop := range lt.pending {
		drop()
		keys = append(keys, key)
	}
	lt.pending = nil
	return keys
}

func (r *recovery) report(sid session.SessionID, err error) {
	if r.fail != nil {
		r.fail(sid, err)
	}
}

func (r *recovery) settled(lt *lifetime) {
	if r.notify != nil && lt.ctx.Err() == nil {
		r.notify(lt)
	}
}

// awaitOutcome registers key on the shared Watcher under lt: its Outcome is
// delivered through the Loop and reported through notify, a read
// the executor answers definitively is reported as a failure. The effect's
// progress frames reach the transient stream meanwhile. Nothing is held open
// for the length of the execution; the lifetime's end drops the wait. The
// delivery leaves the Watcher's goroutine: it takes the Run's step lock,
// and the step holding that lock may itself be waiting on the Watcher.
func (r *recovery) awaitOutcome(lt *lifetime, key effect.AssignmentKey) {
	drop := r.watcher.Watch(lt.ctx, key,
		func(out effect.Outcome) {
			// The key stays awaited until its settlement is committed, so
			// the Run reads as carried here throughout.
			go func() {
				defer lt.untrack(key)
				r.deliver(lt, out)
			}()
		},
		func(err error) {
			lt.untrack(key)
			if lt.ctx.Err() == nil {
				r.report(lt.w.SessionID(), fmt.Errorf("execution: outcome of run %s effect %s cannot be read; the target stays executing until the next takeover: %w", key.RunID, key.Effect, err))
			}
		})
	lt.track(key, drop)
	if r.ports.Progress != nil && r.progress != nil {
		go forwardProgress(lt.ctx, r.ports.Progress, key, r.progress)
	}
}

// deliver settles out through the Loop and reports the settlement through
// notify; the host advances the Run from
// there. A settlement the Writer fences means another process owns the
// Session now: every effect still awaited here is cancelled and the
// lifetime ends.
func (r *recovery) deliver(lt *lifetime, out effect.Outcome) {
	ctx, w := lt.ctx, lt.w
	if ctx.Err() != nil {
		return
	}
	sid := w.SessionID()
	res, err := r.loop.Deliver(ctx, r.runs.Bind(w), out)
	if err != nil {
		if errors.Is(err, store.ErrOwnershipLost) {
			r.lost(lt)
		}
		r.report(sid, fmt.Errorf("execution: settling outcome for run %s: %w", out.Key.RunID, err))
		return
	}
	if res.Disposition == loop.LoopDelivered || res.Disposition == loop.LoopFinished {
		r.settled(lt)
	}
}

// lost ends lt after its Writer was fenced: the effects it still awaits are
// cancelled, their Outcomes are not this process's to write any more, and
// every later step of the Session here is refused until it is detached.
func (r *recovery) lost(lt *lifetime) {
	lt.mu.Lock()
	if lt.lost == nil {
		lt.lost = fmt.Errorf("%w: the session was taken over", store.ErrOwnershipLost)
	}
	lt.mu.Unlock()
	r.end(lt)
}

// end cancels lt and every effect it still awaited.
func (r *recovery) end(lt *lifetime) {
	lt.cancel()
	for _, key := range lt.drain() {
		_ = r.ports.Execution.Cancel(context.WithoutCancel(lt.ctx), key)
	}
}

// installLocked replaces the Session's lifetime with one derived from
// parent, stopping the previous listeners. Callers hold r.mu.
func (r *recovery) installLocked(w writer.Writer, parent context.Context) *lifetime {
	sid := w.SessionID()
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent)) //nolint:gosec // G118: the lifetime owns cancel; Stop and Close call it
	if previous := r.lifetimes[sid]; previous != nil {
		previous.cancel()
	}
	if r.lifetimes == nil {
		r.lifetimes = make(map[session.SessionID]*lifetime)
	}
	lt := &lifetime{ctx: ctx, cancel: cancel, w: w}
	r.lifetimes[sid] = lt
	return lt
}

// writerOf is the Writer of the Session this process owns under sid, if
// any: the one every step of the Session here commits through.
func (r *recovery) writerOf(sid session.SessionID) (writer.Writer, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	lt, ok := r.lifetimes[sid]
	if !ok {
		return nil, false
	}
	return lt.w, true
}

// lifetimeOf returns the Session's lifetime, installing a detached one when
// absent. Open replaces it instead: a takeover supersedes the previous
// owner's listeners.
func (r *recovery) lifetimeOf(w writer.Writer) *lifetime {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lt, ok := r.lifetimes[w.SessionID()]; ok {
		return lt
	}
	return r.installLocked(w, context.Background())
}

// Open installs the Session's recovery lifetime under w and runs the
// takeover disposition. It returns the number of recovery commands issued.
func (r *recovery) Open(ctx context.Context, w writer.Writer) (int, error) {
	r.mu.Lock()
	r.installLocked(w, ctx)
	r.mu.Unlock()
	n, err := r.Recover(ctx, w)
	if err != nil {
		r.Stop(w.SessionID())
	}
	return n, err
}

// Recover runs the takeover disposition for the Session (RUN-CMT-7): every
// Executing target of its active Runs is compared with the execution store
// and only what no record answers for is disposed. The recovery commands
// are identified by the effects they dispose, so a repeated takeover, or a
// later owner's, replays them idempotently. It is also the explicit
// recovery behind an unknown dispatch boundary -- a drive kept the call
// Executing, so the durable record, not a duplicate dispatch, decides the
// settlement. It returns the number of accepted recovery commands.
func (r *recovery) Recover(ctx context.Context, w writer.Writer) (int, error) {
	lt := r.lifetimeOf(w)
	rec := &reconcile.Reconciler{Executions: r.ports.Execution, Recover: r.ports.Recover, Missing: r.missingEffects}
	if r.missingEffects == reconcile.RedispatchMissing {
		// Missing effects are handed to the Executor again within the
		// budget; the dispatch ledger remembers the attempts.
		rec.Attempts, rec.Epoch, rec.MaxRedispatches = r.redispatches, w.Epoch(), r.maxRedispatches
		rec.Redispatch = func(ctx context.Context, key effect.AssignmentKey) error { return r.redispatch(ctx, lt.w, key) }
	}
	snapshots, err := r.runs.ActiveRuns(ctx, w)
	if err != nil {
		return 0, err
	}
	a := &awaiting{r: r, lt: lt, rec: rec}
	st := r.runs.Bind(w)
	n := 0
	for i := range snapshots {
		accepted, err := a.reconcile(ctx, st, &snapshots[i])
		n += accepted
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// awaiting is the takeover disposition of one Session: the reconciler
// decides every Executing target, and every target it does not dispose is
// awaited under the Session's lifetime like an effect dispatched here, so
// one table holds everything this process waits on.
type awaiting struct {
	r   *recovery
	lt  *lifetime
	rec *reconcile.Reconciler
}

func (a *awaiting) reconcile(ctx context.Context, st store.RunStore, snapshot *store.Snapshot) (int, error) {
	if err := a.lt.ctx.Err(); err != nil {
		return 0, err
	}
	decisions, err := a.rec.Plan(ctx, st.Scope(), snapshot)
	if err != nil {
		return 0, err
	}
	for i := range decisions {
		d := &decisions[i]
		if d.Verdict != reconcile.Dispose {
			a.r.awaitOutcome(a.lt, effect.AssignmentKey{Session: st.Scope(), RunID: d.Target.RunID, Effect: d.Target.Effect})
		}
	}
	return reconcile.Apply(ctx, st, decisions)
}

// redispatch is the reconciler's Redispatch port: the Assignment of an
// Executing effect is rebuilt on the Session's Writer and handed to the
// Executor again.
func (r *recovery) redispatch(ctx context.Context, w writer.Writer, key effect.AssignmentKey) error {
	return r.loop.Redispatch(ctx, r.runs.Bind(w), key)
}

// Stop ends the Session's lifetime: its listeners stop and the effects it
// still awaited are cancelled.
func (r *recovery) Stop(sid session.SessionID) {
	r.mu.Lock()
	lt := r.lifetimes[sid]
	delete(r.lifetimes, sid)
	r.mu.Unlock()
	if lt != nil {
		r.end(lt)
	}
}

// Close ends every Session's lifetime.
func (r *recovery) Close() {
	r.mu.Lock()
	lts := make([]*lifetime, 0, len(r.lifetimes))
	for sid, lt := range r.lifetimes {
		lts = append(lts, lt)
		delete(r.lifetimes, sid)
	}
	r.mu.Unlock()
	for _, lt := range lts {
		r.end(lt)
	}
}
