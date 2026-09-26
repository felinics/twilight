package effect

import (
	"context"
	"errors"
	"sync"
	"time"
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
	Port ExecutionPort
	// Poll is how often every registered key is read regardless of notices:
	// the bound on how late a settlement can be seen when the stream is
	// silent for any reason. Zero selects DefaultWatchPoll.
	Poll time.Duration
	// Reconnect is the pause before the stream is opened again after it
	// ends or fails. Zero selects DefaultWatchReconnect.
	Reconnect time.Duration
	// Probe is how long a registered key may wait without an Outcome before
	// the Watcher attaches it (RUN-EXE-3): an orphaned execution, whose
	// Worker died holding it, is handed to Port's RecoverExecution when
	// Port implements Recoverer, so a live drive survives a worker
	// replacement without an owner takeover (CLD-DEV-2). Each key is probed
	// at most once per Probe. Zero selects DefaultWatchProbe; negative
	// disables probing.
	Probe time.Duration

	mu      sync.Mutex
	keys    map[AssignmentKey]*watch
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

// DefaultWatchPoll is the Watcher's read interval when no notice arrives.
const DefaultWatchPoll = 30 * time.Second

// DefaultWatchReconnect is the Watcher's pause before reopening its stream.
const DefaultWatchReconnect = time.Second

// DefaultWatchProbe is how long a key waits before the Watcher attaches it.
const DefaultWatchProbe = 15 * time.Second

type watch struct {
	deliver func(Outcome)
	// fail receives a read the port answers definitively: nothing will ever
	// be read for this key. The registration is dropped afterwards.
	fail func(error)
	// since is when the key was registered; probed when it was last
	// attached by the probe.
	since  time.Time
	probed time.Time
}

// Watch registers key: deliver receives its Outcome once, then the
// registration is dropped. fail, when set, receives a definitive read error
// (ErrExecutionNotFound, ErrOutcomeUnavailable, ErrOutcomeCollected,
// ErrExecutionAborted) and the registration is dropped too; nil discards
// it. Registering a key already registered replaces its callbacks. The
// returned cancel drops the registration without delivering.
func (w *Watcher) Watch(ctx context.Context, key AssignmentKey, deliver func(Outcome), fail func(error)) (cancel func()) {
	w.mu.Lock()
	if w.keys == nil {
		w.keys = make(map[AssignmentKey]*watch)
	}
	entry := &watch{deliver: deliver, fail: fail, since: time.Now()}
	w.keys[key] = entry
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
		if w.keys[key] == entry {
			delete(w.keys, key)
		}
		w.mu.Unlock()
	}
}

// Close stops the stream and the polling and drops every registration.
// The Watcher can be used again afterwards.
// Await is the synchronous form of Watch for a caller that dispatched one
// effect and has nothing else to do until it answers: it registers key and
// returns its Outcome, the definitive read error of a key the executor will
// never answer for, or ctx's error. It shares the Watcher's one subscription
// with every other waiter instead of opening its own.
func (w *Watcher) Await(ctx context.Context, key AssignmentKey) (Outcome, error) {
	type answer struct {
		out Outcome
		err error
	}
	done := make(chan answer, 1)
	cancel := w.Watch(ctx, key,
		func(out Outcome) { done <- answer{out: out} },
		func(err error) { done <- answer{err: err} })
	defer cancel()
	select {
	case a := <-done:
		return a.out, a.err
	case <-ctx.Done():
		return Outcome{}, ctx.Err()
	}
}

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
		poll = DefaultWatchPoll
	}
	reconnect := w.Reconnect
	if reconnect <= 0 {
		reconnect = DefaultWatchReconnect
	}
	notices := make(chan AssignmentKey, 64)
	if settlements, ok := w.Port.(SettlementPort); ok {
		go w.stream(settlements, notices, reconnect)
	}
	ticker := time.NewTicker(poll)
	defer ticker.Stop()
	probe := w.Probe
	if probe == 0 {
		probe = DefaultWatchProbe
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
// was registered or last probed, and asks for the recovery of an orphaned
// one (RUN-EXE-3, RUN-EXE-6). Attach failures and refusals are left to the
// next probe: the Worker's record is the authority, the probe only asks.
func (w *Watcher) probeStale(probe time.Duration) {
	now := time.Now()
	w.mu.Lock()
	var due []AssignmentKey
	for key, entry := range w.keys {
		last := entry.probed
		if last.IsZero() {
			last = entry.since
		}
		if now.Sub(last) >= probe {
			entry.probed = now
			due = append(due, key)
		}
	}
	w.mu.Unlock()
	recoverer, canRecover := w.Port.(Recoverer)
	for _, key := range due {
		if w.ctx.Err() != nil {
			return
		}
		att, err := w.Port.Attach(w.ctx, key)
		if err != nil || att.State != AttachmentOrphaned || !canRecover {
			continue
		}
		_ = recoverer.RecoverExecution(w.ctx, key)
	}
}

// stream keeps one Settlements subscription open, forwarding notices for
// registered keys and asking for a full read whenever it (re)connects, so
// what settled while it was down is not waited for.
func (w *Watcher) stream(port SettlementPort, notices chan<- AssignmentKey, reconnect time.Duration) {
	for {
		w.mu.Lock()
		epoch, after := w.epoch, w.after
		w.mu.Unlock()
		err := port.Settlements(w.ctx, epoch, after, func(s Settlement) bool {
			w.mu.Lock()
			changed := w.epoch != "" && s.Epoch != w.epoch
			w.epoch, w.after = s.Epoch, s.Sequence
			_, registered := w.keys[s.Key]
			w.mu.Unlock()
			if s.Key == (AssignmentKey{}) {
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
		if errors.Is(err, ErrSettlementsEvicted) {
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
	keys := make([]AssignmentKey, 0, len(w.keys))
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

// read reads one key and settles its registration when the answer is final.
func (w *Watcher) read(key AssignmentKey) {
	w.mu.Lock()
	entry, ok := w.keys[key]
	w.mu.Unlock()
	if !ok {
		return
	}
	out, err := w.Port.GetOutcome(w.ctx, key)
	switch {
	case err == nil:
	case errors.Is(err, ErrOutcomeNotReady):
		return
	case definitiveRead(err):
	default:
		// A read failure says nothing about the execution: the next notice
		// or poll reads again.
		return
	}
	w.mu.Lock()
	if w.keys[key] != entry {
		w.mu.Unlock()
		return
	}
	delete(w.keys, key)
	w.mu.Unlock()
	if w.ctx.Err() != nil {
		return
	}
	if err != nil {
		if entry.fail != nil {
			entry.fail(err)
		}
		return
	}
	entry.deliver(out)
}

// definitiveRead reports a read error the port will answer the same way for
// ever: the key has no Outcome to wait for. ErrOutcomeCollected and
// ErrExecutionAborted wrap ErrOutcomeUnavailable.
func definitiveRead(err error) bool {
	return errors.Is(err, ErrExecutionNotFound) || errors.Is(err, ErrOutcomeUnavailable)
}
