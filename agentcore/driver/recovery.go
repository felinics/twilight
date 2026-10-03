package driver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/redispatch"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// Recovery is the takeover supervisor of the Sessions this process owns.
// Open installs a Session's recovery lifetime and runs the takeover
// disposition: the reconciler compares every Executing effect with the
// execution store, keeps waiting for attempts that survived, hands missing
// effects to the Executor again within the redispatch budget or disposes
// them. An Outcome of a kept attempt settles through the Loop of the Turn
// that owns its Run and drives the Run on. Stop ends a Session's listeners;
// Close ends every Session's.
type Recovery struct {
	// Runs is the Run module's Session adapter; recovery binds it to the
	// Writer the Session was opened with.
	Runs     *sessionstore.SessionRunStore
	Executor effect.ExecutionPort
	Loops    *Loops
	// Watcher is where every Reconciler waits for Outcomes: the settlement
	// subscription shared with the Loops; required.
	Watcher *effect.Watcher
	// Fail receives failures of work done outside any caller's call, such as
	// settling a reattached Outcome; nil discards them.
	Fail func(session.SessionID, error)
	// MissingEffects is the takeover policy for an Executing effect the
	// Executor holds nothing for: the zero value disposes,
	// reconcile.RedispatchMissing redispatches within the budget and
	// requires Redispatches.
	MissingEffects reconcile.MissingPolicy
	// Redispatches is the dispatch ledger the reconciler writes before and
	// after it hands an effect to the Executor again; required under
	// RedispatchMissing, unused otherwise.
	Redispatches redispatch.Store
	// MaxRedispatches bounds redispatches per effect; zero selects the
	// reconciler's default.
	MaxRedispatches int
	// OrphanProbe is how often an effect still waiting is attached and, when
	// orphaned, handed to RecoverExecution by the Reconciler of a takeover;
	// zero selects its default.
	OrphanProbe time.Duration
	// Sink receives the provisional observations of a Run driven on after a
	// reattached Outcome; nil discards them.
	Sink loop.EventSink

	mu        sync.Mutex
	lifetimes map[session.SessionID]*lifetime
}

// lifetime is the detached context a Session's recovery goroutines --
// reattached outcome reads, resumed answers -- live under, and the cancel
// that stops them. w is the Writer the Session was opened with; everything
// the lifetime settles commits through it.
type lifetime struct {
	ctx    context.Context
	cancel context.CancelFunc
	w      writer.Writer
}

func (r *Recovery) fail(sid session.SessionID, err error) {
	if r.Fail != nil {
		r.Fail(sid, err)
	}
}

// installLocked replaces the Session's lifetime with one derived from
// parent, stopping the previous listeners. Callers hold r.mu.
func (r *Recovery) installLocked(w writer.Writer, parent context.Context) *lifetime {
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

// lifetimeOf returns the Session's lifetime, installing a detached one when
// absent. Open replaces it instead: a takeover supersedes the previous
// owner's listeners.
func (r *Recovery) lifetimeOf(w writer.Writer) *lifetime {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lt, ok := r.lifetimes[w.SessionID()]; ok {
		return lt
	}
	return r.installLocked(w, context.Background())
}

// Open installs the Session's recovery lifetime under w and runs the
// takeover disposition. It returns the number of recovery commands issued.
func (r *Recovery) Open(ctx context.Context, w writer.Writer) (int, error) {
	r.mu.Lock()
	r.installLocked(w, ctx)
	r.mu.Unlock()
	n, err := r.Recover(ctx, w)
	if err != nil {
		r.Stop(w.SessionID())
	}
	return n, err
}

// Recover runs the takeover disposition for the Session: the reconciler
// compares every Executing target with the execution store and disposes
// only what no record answers for. It is the explicit recovery behind an
// unknown dispatch boundary -- a drive kept the call Executing, so the
// durable record, not a duplicate dispatch, decides the settlement.
func (r *Recovery) Recover(ctx context.Context, w writer.Writer) (int, error) {
	lt := r.lifetimeOf(w)
	sid := w.SessionID()
	rec := &reconcile.Reconciler{Executions: r.Executor, Lifetime: lt.ctx, Watcher: r.Watcher, Deliver: r.reattachDeliver(lt),
		Fail: func(key effect.AssignmentKey, err error) {
			r.fail(sid, fmt.Errorf("driver: outcome of run %s effect %s cannot be read; the target stays executing until the next takeover: %w", key.RunID, key.Effect, err))
		}}
	rec.Missing = r.MissingEffects
	rec.OrphanProbe = r.OrphanProbe
	if r.MissingEffects == reconcile.RedispatchMissing {
		// Missing effects are handed to the Executor again within the
		// budget; the dispatch ledger remembers the attempts.
		rec.Attempts, rec.Epoch, rec.MaxRedispatches = r.Redispatches, w.Epoch(), r.MaxRedispatches
		rec.Redispatch = func(ctx context.Context, key effect.AssignmentKey) error { return r.redispatch(ctx, lt.w, key) }
	}
	return r.Runs.RecoverInterrupted(ctx, w, rec)
}

// redispatch is the reconciler's Redispatch port: the Assignment of an
// Executing effect is rebuilt on the Session's Writer and handed to the
// Executor again.
func (r *Recovery) redispatch(ctx context.Context, w writer.Writer, key effect.AssignmentKey) error {
	l, _, err := r.Loops.ForRun(ctx, w, key.RunID)
	if err != nil {
		return fmt.Errorf("driver: redispatch: %w", err)
	}
	return l.Redispatch(ctx, r.Runs.Bind(w), key)
}

// reattachDeliver is what the reconciler hands a kept attempt's Outcome to:
// an Outcome of an attempt that survived the previous owner is settled
// through the Loop of the Turn that owns its Run, and the Run is driven on
// from there.
func (r *Recovery) reattachDeliver(lt *lifetime) func(effect.Outcome) {
	ctx, w := lt.ctx, lt.w
	sid := w.SessionID()
	return func(out effect.Outcome) {
		if ctx.Err() != nil {
			return
		}
		l, _, err := r.Loops.ForRun(ctx, w, out.Key.RunID)
		if err != nil {
			r.fail(sid, fmt.Errorf("driver: reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		res, err := l.Deliver(ctx, r.Runs.Bind(w), out, r.Sink)
		if err != nil {
			r.fail(sid, fmt.Errorf("driver: settling reattached outcome for run %s: %w", out.Key.RunID, err))
			return
		}
		if res.Disposition != loop.LoopDelivered {
			return
		}
		if _, err := l.Run(ctx, r.Runs.Bind(w), out.Key.RunID, r.Sink); err != nil && !errors.Is(err, loop.ErrRunAlreadyRunning) {
			r.fail(sid, fmt.Errorf("driver: driving run %s after a reattached outcome: %w", out.Key.RunID, err))
		}
	}
}

// Stop cancels the Session's recovery listeners.
func (r *Recovery) Stop(sid session.SessionID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if lt := r.lifetimes[sid]; lt != nil {
		lt.cancel()
		delete(r.lifetimes, sid)
	}
}

// Close cancels every Session's recovery listeners.
func (r *Recovery) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for sid, lt := range r.lifetimes {
		lt.cancel()
		delete(r.lifetimes, sid)
	}
}
