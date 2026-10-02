// Package watch waits on the Outcomes of many effects over one execution
// port: one settlement subscription, one read per key, however many wait.
package watch

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/run/effect"
)

// Watcher turns one ExecutionPort into a source of Outcomes for many keys
// over one subscription. Callers register the keys they wait on; the
// Watcher reads each once (GetOutcome is a plain read), keeps one
// Settlements stream open to the port, and on every notice for a
// registered key reads it and hands the Outcome to the key's callback.
//
// The stream is the fast path, the read is the truth: a notice the Watcher
// missed (it was not yet subscribed, the stream dropped, the port
// restarted) costs nothing, because on every reconnect, and at Poll
// intervals regardless, every registered key is read again. A port without
// SettlementPort is served by that polling alone.
//
// One Watcher per (owner process, executor) is the intended shape: the cost
// of waiting on N effects is one connection and N map entries, not N
// goroutines and N requests.
type Watcher struct {
	Port effect.ExecutionPort
	// Settlements is the port's notice stream, when the executor offers one;
	// nil serves the registered keys by polling alone. Recover is the port's
	// recovery capability, asked to take an orphaned key back; nil leaves
	// orphaned keys to an external controller.
	Settlements effect.SettlementPort
	Recover     effect.Recoverer
	// Poll is how often every registered key is read regardless of notices:
	// the bound on how late a settlement can be seen when the stream is
	// silent for any reason. Zero selects DefaultPoll.
	Poll time.Duration
	// Reconnect is the pause before the stream is opened again after it
	// ends or fails. Zero selects DefaultReconnect.
	Reconnect time.Duration
	// Probe is how long a registered key may wait without an Outcome before
	// the Watcher attaches it (RUN-EXE-3): an orphaned execution, whose
	// Worker died holding it, is handed to Recover when set, so a live
	// drive survives a worker replacement without an owner takeover
	// (CLD-DEV-2). Each key is probed at most once per Probe. Zero selects
	// DefaultProbe; negative disables probing.
	Probe time.Duration

	mu      sync.Mutex
	keys    map[effect.AssignmentKey]*waiting
	running bool
	// wake asks the loop to read every registered key now: a key was just
	// registered or the stream told us something.
	wake chan struct{}
	// epoch and after are the stream's last position, for a resubscription
	// that asks only for what it missed.
	epoch string
	after uint64
	ctx   context.Context
	stop  context.CancelFunc
	done  chan struct{}
}

// DefaultPoll is the Watcher's read interval when no notice arrives.
const DefaultPoll = 30 * time.Second

// DefaultReconnect is the Watcher's pause before reopening its stream.
const DefaultReconnect = time.Second

// DefaultProbe is how long a key waits before the Watcher attaches it.
const DefaultProbe = 15 * time.Second

// registration is one wait on a key.
type registration struct {
	deliver func(effect.Outcome)
	// fail receives a read the port answers definitively: nothing will ever
	// be read for this key. The registration is dropped afterwards.
	fail func(error)
}

// waiting is everything registered on one key and the probe state the
// registrations share: a key is read and probed once however many wait on
// it, and every one of them receives what the read settles.
type waiting struct {
	watchers map[*registration]struct{}
	// since is when the key was first registered; probed when it was last
	// attached by the probe.
	since  time.Time
	probed time.Time
	// asked records that recovery was requested for the current orphaned
	// episode; a record seen active again clears it, so the next episode
	// asks once more.
	asked bool
}

// Watch registers key: deliver receives its Outcome once, then the
// registration is dropped. fail, when set, receives a definitive read error
// (ErrExecutionNotFound, ErrOutcomeUnavailable, ErrOutcomeCollected,
// ErrExecutionAborted) and the registration is dropped too; nil discards
// it. A key registered more than once is read once and every registration
// receives the answer. The returned cancel drops this registration without
// delivering.
func (w *Watcher) Watch(ctx context.Context, key effect.AssignmentKey, deliver func(effect.Outcome), fail func(error)) (cancel func()) {
	w.mu.Lock()
	if w.keys == nil {
		w.keys = make(map[effect.AssignmentKey]*waiting)
	}
	ks := w.keys[key]
	if ks == nil {
		ks = &waiting{watchers: make(map[*registration]struct{}), since: time.Now()}
		w.keys[key] = ks
	}
	entry := &registration{deliver: deliver, fail: fail}
	ks.watchers[entry] = struct{}{}
	if !w.running {
		w.running = true
		w.wake = make(chan struct{}, 1)
		w.ctx, w.stop = context.WithCancel(context.WithoutCancel(ctx))
		w.done = make(chan struct{})
		go w.run()
	}
	wake := w.wake
	w.mu.Unlock()
	select {
	case wake <- struct{}{}:
	default:
	}
	return func() {
		w.mu.Lock()
		if ks := w.keys[key]; ks != nil {
			delete(ks.watchers, entry)
			if len(ks.watchers) == 0 {
				delete(w.keys, key)
			}
		}
		w.mu.Unlock()
	}
}

// Await is the synchronous form of Watch for a caller that dispatched one
// effect and has nothing else to do until it answers: it registers key and
// returns its Outcome, the definitive read error of a key the executor will
// never answer for, or ctx's error. It shares the Watcher's one subscription
// with every other waiter instead of opening its own.
func (w *Watcher) Await(ctx context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	type answer struct {
		out effect.Outcome
		err error
	}
	done := make(chan answer, 1)
	cancel := w.Watch(ctx, key,
		func(out effect.Outcome) { done <- answer{out: out} },
		func(err error) { done <- answer{err: err} })
	defer cancel()
	select {
	case a := <-done:
		return a.out, a.err
	case <-ctx.Done():
		return effect.Outcome{}, ctx.Err()
	}
}

// Close stops the stream and the polling and drops every registration.
// The Watcher can be used again afterwards.
func (w *Watcher) Close() {
	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return
	}
	stop, done := w.stop, w.done
	w.mu.Unlock()
	stop()
	<-done
	w.mu.Lock()
	w.keys = nil
	w.running = false
	w.mu.Unlock()
}

func (w *Watcher) run() {
	defer close(w.done)
	poll := w.Poll
	if poll <= 0 {
		poll = DefaultPoll
	}
	reconnect := w.Reconnect
	if reconnect <= 0 {
		reconnect = DefaultReconnect
	}
	notices := make(chan effect.AssignmentKey, 64)
	if w.Settlements != nil {
		go w.stream(w.Settlements, notices, reconnect)
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	probe := w.Probe
	if probe == 0 {
		probe = DefaultProbe
	}
	var probes <-chan time.Time
	if probe > 0 {
		probeTicker := time.NewTicker(probe)
		defer probeTicker.Stop()
		probes = probeTicker.C
	}
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.wake:
			w.readAll()
		case <-ticker.C:
			w.readAll()
		case <-probes:
			w.probeStale(probe)
		case key := <-notices:
			w.read(key)
		}
	}
}

// probeStale attaches every key that has waited at least probe since it
// was registered or last probed, and asks once per orphaned episode for
// the recovery of an orphaned one (RUN-EXE-3, RUN-EXE-6). Attach failures
// and refusals are left to the next probe: the Worker's record is the
// authority, the probe only asks.
func (w *Watcher) probeStale(probe time.Duration) {
	now := time.Now()
	w.mu.Lock()
	var due []effect.AssignmentKey
	for key, ks := range w.keys {
		last := ks.probed
		if last.IsZero() {
			last = ks.since
		}
		if now.Sub(last) >= probe {
			ks.probed = now
			due = append(due, key)
		}
	}
	w.mu.Unlock()
	for _, key := range due {
		if w.ctx.Err() != nil {
			return
		}
		att, err := w.Port.Attach(w.ctx, key)
		if err != nil {
			continue
		}
		w.mu.Lock()
		ks, ok := w.keys[key]
		ask := false
		if ok {
			switch att.State {
			case effect.AttachmentOrphaned:
				ask, ks.asked = !ks.asked, true
			case effect.AttachmentActive:
				ks.asked = false
			}
		}
		w.mu.Unlock()
		if ask && w.Recover != nil {
			_ = w.Recover.RecoverExecution(w.ctx, key)
		}
	}
}

// stream keeps one Settlements subscription open, forwarding notices for
// registered keys and asking for a full read whenever it (re)connects, so
// what settled while it was down is not waited for.
func (w *Watcher) stream(port effect.SettlementPort, notices chan<- effect.AssignmentKey, reconnect time.Duration) {
	for {
		w.mu.Lock()
		epoch, after := w.epoch, w.after
		w.mu.Unlock()
		err := port.Settlements(w.ctx, epoch, after, func(s effect.Settlement) bool {
			w.mu.Lock()
			changed := w.epoch != "" && s.Epoch != w.epoch
			w.epoch, w.after = s.Epoch, s.Sequence
			_, registered := w.keys[s.Key]
			w.mu.Unlock()
			if s.Key == (effect.AssignmentKey{}) {
				// The stream's announcement: connected to another
				// incarnation than the one the position came from, so what
				// settled in between is read now rather than at the poll.
				if changed {
					select {
					case w.wake <- struct{}{}:
					default:
					}
				}
				return true
			}
			if registered {
				select {
				case notices <- s.Key:
				case <-w.ctx.Done():
					return false
				}
			}
			return true
		})
		if w.ctx.Err() != nil {
			return
		}
		if errors.Is(err, effect.ErrSettlementsEvicted) {
			// The ring moved past us: forget the position and re-read.
			w.mu.Lock()
			w.epoch, w.after = "", 0
			w.mu.Unlock()
		}
		select {
		case w.wake <- struct{}{}:
		default:
		}
		timer := time.NewTimer(reconnect)
		select {
		case <-w.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// readAll reads every registered key once.
func (w *Watcher) readAll() {
	w.mu.Lock()
	keys := make([]effect.AssignmentKey, 0, len(w.keys))
	for key := range w.keys {
		keys = append(keys, key)
	}
	w.mu.Unlock()
	for _, key := range keys {
		if w.ctx.Err() != nil {
			return
		}
		w.read(key)
	}
}

// read reads one key and settles its registrations when the answer is
// final.
func (w *Watcher) read(key effect.AssignmentKey) {
	w.mu.Lock()
	ks, ok := w.keys[key]
	w.mu.Unlock()
	if !ok {
		return
	}
	out, err := w.Port.GetOutcome(w.ctx, key)
	switch {
	case err == nil:
	case errors.Is(err, effect.ErrOutcomeNotReady):
		return
	case definitiveRead(err):
	default:
		// A read failure says nothing about the execution: the next notice
		// or poll reads again.
		return
	}
	w.mu.Lock()
	if w.keys[key] != ks {
		w.mu.Unlock()
		return
	}
	delete(w.keys, key)
	watchers := make([]*registration, 0, len(ks.watchers))
	for entry := range ks.watchers {
		watchers = append(watchers, entry)
	}
	w.mu.Unlock()
	if w.ctx.Err() != nil {
		return
	}
	for _, entry := range watchers {
		if err != nil {
			if entry.fail != nil {
				entry.fail(err)
			}
			continue
		}
		entry.deliver(out)
	}
}

// definitiveRead reports a read error the port will answer the same way for
// ever: the key has no Outcome to wait for. ErrOutcomeCollected and
// ErrExecutionAborted wrap ErrOutcomeUnavailable.
func definitiveRead(err error) bool {
	return errors.Is(err, effect.ErrExecutionNotFound) || errors.Is(err, effect.ErrOutcomeUnavailable)
}
