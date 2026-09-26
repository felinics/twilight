package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
)

// Activation is the ownership model under which a Session is held for
// active work only (APP-ACT-1): a command that reaches this process opens
// the Session here when no live owner holds it, the Session is driven while
// there is work, and once it has been quiescent for IdleRelease its
// ownership is released. Two Turns of one Session may then run on two
// processes over the same stores, and a gateway may send a command to any
// replica.
type Activation struct {
	// Preset is the decision identity an activation opens a Session with.
	Preset turn.PresetID
	// Options is the SessionOptions template of an activated Session; its
	// Preset and ResumeActive are set by the activation.
	Options SessionOptions
	// IdleRelease is how long a Session stays quiescent (no active Turn, no
	// background drive, no pending command) before its ownership is
	// released; zero keeps an activated Session open until Close.
	IdleRelease time.Duration
	// Scan is the period at which this process looks for Sessions that
	// need an owner (APP-ACT-3): pending inboxes and expired leases. Zero
	// disables the scan; activation then happens only through Enqueue.
	Scan time.Duration
	// ScanLimit bounds each scan's candidates from each source; zero
	// selects DefaultScanLimit. A bounded page keeps the scan's cost
	// independent of the number of Sessions; what it leaves out is read
	// on a later scan or by another replica.
	ScanLimit int
}

// DefaultScanLimit is the page each scan reads when ScanLimit is zero.
const DefaultScanLimit = 64

// ErrNoActivation reports an Activate without Config.Activation.
var ErrNoActivation = errors.New("app: no activation is configured")

// activationTimeout bounds one background activation: an Open with its
// takeover disposition and the inbox pass.
const activationTimeout = time.Minute

// Activate opens the Session for work under the Activation options, or
// wakes it when this process already holds it (APP-ACT-1). Another live
// owner is session.ErrOwned: the command is durable and that owner applies
// it. Concurrent activations of one Session in this process coalesce.
func (app *Application) Activate(ctx context.Context, sid session.SessionID) (*Session, error) {
	for {
		if s, ok := app.Opened(sid); ok {
			s.Wake()
			return s, nil
		}
		if app.activation == nil {
			return nil, ErrNoActivation
		}
		app.actMu.Lock()
		if wait, busy := app.activating[sid]; busy {
			app.actMu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		done := make(chan struct{})
		app.activating[sid] = done
		app.actMu.Unlock()
		s, err := app.activate(ctx, sid)
		app.actMu.Lock()
		delete(app.activating, sid)
		app.actMu.Unlock()
		close(done)
		return s, err
	}
}

func (app *Application) activate(ctx context.Context, sid session.SessionID) (*Session, error) {
	if app.activation.Preset == "" {
		return nil, errors.New("app: activation requires a preset")
	}
	ref, err := app.PresetRef(app.activation.Preset)
	if err != nil {
		return nil, err
	}
	opts := app.activation.Options
	opts.Preset, opts.ResumeActive = ref, true
	s, err := app.OpenSession(ctx, sid, opts)
	if errors.Is(err, owner.ErrSessionOpen) {
		if s, ok := app.Opened(sid); ok {
			s.Wake()
			return s, nil
		}
	}
	return s, err
}

// activateInBackground is Enqueue's activation of a Session this process
// does not hold: the caller gets its durable entry at once, the Open runs
// under the application's lifetime.
func (app *Application) activateInBackground(sid session.SessionID) {
	app.releases.start()
	go func() {
		defer app.releases.done()
		ctx, cancel := context.WithTimeout(app.bg, activationTimeout)
		defer cancel()
		if _, err := app.Activate(ctx, sid); err != nil && !expectedActivationError(err) {
			app.warn(fmt.Errorf("app: activating %s: %w", sid, err))
		}
	}()
}

// expectedActivationError is an outcome that needs no report: another live
// owner holds the Session, this process is between closing and reopening
// it, or the application is shutting down.
func expectedActivationError(err error) bool {
	return session.IsCode(err, session.ErrOwned) || errors.Is(err, owner.ErrSessionOpen) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// scanLoop runs scan every Activation.Scan until Close.
func (app *Application) scanLoop() {
	defer app.loops.Done()
	ticker := time.NewTicker(app.activation.Scan)
	defer ticker.Stop()
	for {
		select {
		case <-app.bg.Done():
			return
		case <-ticker.C:
			app.scan(app.bg)
		}
	}
}

// scan activates the Sessions that need an owner and have none here
// (APP-ACT-3): those with pending commands (CLD-CMD-4) and those whose
// lease has expired, which a dead owner left. Open arbitrates between
// replicas: a live foreign lease is ErrOwned and skipped.
func (app *Application) scan(ctx context.Context) {
	limit := app.activation.ScanLimit
	if limit <= 0 {
		limit = DefaultScanLimit
	}
	candidates := map[session.SessionID]struct{}{}
	pending, err := app.inbox.Sessions(ctx, limit)
	if err != nil {
		app.warn(fmt.Errorf("app: scanning pending inboxes: %w", err))
	}
	for _, sid := range pending {
		candidates[sid] = struct{}{}
	}
	expired, err := app.Owner.Store.ExpiredLeases(ctx, app.ownership.Now().UnixMilli(), limit)
	if err != nil {
		app.warn(fmt.Errorf("app: scanning expired leases: %w", err))
	}
	for _, l := range expired {
		candidates[l.Session] = struct{}{}
	}
	for sid := range candidates {
		if _, ok := app.Opened(sid); ok {
			continue
		}
		if ctx.Err() != nil {
			return
		}
		if _, err := app.Activate(ctx, sid); err != nil && !expectedActivationError(err) {
			app.warn(fmt.Errorf("app: activating %s from the scan: %w", sid, err))
		}
	}
}

// startIdleRelease runs the Session's quiescence watch (APP-ACT-2) when
// the activation asks for one.
func (s *Session) startIdleRelease() {
	act := s.app.activation
	if act == nil || act.IdleRelease <= 0 {
		return
	}
	tick := act.IdleRelease / 4
	if tick < 10*time.Millisecond {
		tick = 10 * time.Millisecond
	}
	s.loops.Add(1)
	go func() {
		defer s.loops.Done()
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		for {
			select {
			case <-s.bg.Done():
				return
			case <-ticker.C:
			}
			if s.idleFor(act.IdleRelease) && s.quiescent(s.bg) {
				s.app.releaseIdle(s)
				return
			}
		}
	}()
}

// touch records activity: the idle clock restarts.
func (s *Session) touch() {
	s.bgMu.Lock()
	s.lastActive = time.Now()
	s.bgMu.Unlock()
}

// idleFor reports no background drive for at least d since the last
// activity.
func (s *Session) idleFor(d time.Duration) bool {
	s.bgMu.Lock()
	defer s.bgMu.Unlock()
	return s.bgN == 0 && time.Since(s.lastActive) >= d
}

// quiescent reports the Session has no active Turn and no pending command
// (APP-ACT-2). A read failure is not quiescence.
func (s *Session) quiescent(ctx context.Context) bool {
	status, err := s.Status(ctx)
	if err != nil || status.Active != "" {
		return false
	}
	if s.app.inbox != nil {
		pending, err := s.app.inbox.Pending(ctx, s.sid)
		if err != nil || len(pending) > 0 {
			return false
		}
	}
	return true
}

// releaseIdle closes the quiescent Session, releasing its ownership, then
// re-activates it when a command landed in the inbox meanwhile: the wake
// that command sent may have reached an applier already stopped.
func (app *Application) releaseIdle(s *Session) {
	app.releases.start()
	go func() {
		defer app.releases.done()
		ctx, cancel := context.WithTimeout(context.WithoutCancel(app.bg), activationTimeout)
		defer cancel()
		if err := s.Close(ctx); err != nil {
			app.warn(fmt.Errorf("app: releasing idle %s: %w", s.sid, err))
		}
		if app.bg.Err() != nil {
			return
		}
		pending, err := app.inbox.Pending(ctx, s.sid)
		if err != nil || len(pending) == 0 {
			return
		}
		if _, err := app.Activate(ctx, s.sid); err != nil && !expectedActivationError(err) {
			app.warn(fmt.Errorf("app: reactivating %s: %w", s.sid, err))
		}
	}()
}

// group counts goroutines whose start may race a wait, which a
// sync.WaitGroup forbids.
type group struct {
	mu   sync.Mutex
	n    int
	idle chan struct{}
}

func (g *group) start() {
	g.mu.Lock()
	if g.n == 0 {
		g.idle = make(chan struct{})
	}
	g.n++
	g.mu.Unlock()
}

func (g *group) done() {
	g.mu.Lock()
	g.n--
	if g.n == 0 {
		close(g.idle)
	}
	g.mu.Unlock()
}

func (g *group) wait(ctx context.Context) error {
	g.mu.Lock()
	idle, n := g.idle, g.n
	g.mu.Unlock()
	if n == 0 {
		return nil
	}
	select {
	case <-idle:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
