package app

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/context/compaction"
	"github.com/felinics/twilight/agent/input"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/turn"
)

// SessionOptions tunes OpenSession.
type SessionOptions struct {
	// Preset is the decision identity new Turns run under (required).
	Preset turn.PresetRef
	// NewTurnID mints TurnIDs; nil selects the random default.
	NewTurnID func() turn.TurnID
	// ResumeActive resumes a still-active Turn synchronously inside
	// OpenSession. Interactive hosts leave it false and call Resume themselves.
	ResumeActive bool
	// CompactAfterEntries triggers automatic compaction when the context
	// grows past this many entries after a settlement; zero disables it.
	CompactAfterEntries int
	// CompactRetainEntries is the pair-closed suffix a compaction keeps
	// verbatim; zero selects the default.
	CompactRetainEntries int
	// CompactWarn receives automatic-compaction failures; they never change
	// the settled results. Nil discards them.
	CompactWarn func(error)
	// RouteRetries bounds how many times one input's route is re-committed
	// after a conflict with a concurrent route (APP-SES-3) before the last
	// conflict is returned to the caller; the input stays submitted and the
	// next Send, Drain or Resume routes it. Zero selects DefaultRouteRetries.
	RouteRetries int
	// DrainBudget bounds how many Turns one settlement drains from the
	// backlog before returning ErrDrainBudget with the Results so far; the
	// remaining backlog stays submitted for the next call (APP-RTE-2). Zero
	// selects DefaultDrainBudget.
	DrainBudget int
	// InboxPoll is how often the open Session reads its command inbox
	// besides being woken (APP-INB-3); zero selects DefaultInboxPoll.
	InboxPoll time.Duration
	// InheritedWorkspace is what this Session does, when opened, with a
	// workspace binding it inherited from its fork parent (APP-WSP-5); the
	// zero value keeps the parent's Workspace.
	InheritedWorkspace workspace.InheritedPolicy
}

// DefaultRouteRetries and DefaultDrainBudget are the liveness bounds a
// SessionOptions with zero values takes.
const (
	DefaultRouteRetries = 4
	DefaultDrainBudget  = 64
)

// ErrDrainBudget reports a settlement that stopped draining the backlog at
// the DrainBudget with inputs still submitted.
var ErrDrainBudget = errors.New("app: drain budget exhausted with inputs still submitted")

func (o SessionOptions) routeRetries() int {
	if o.RouteRetries <= 0 {
		return DefaultRouteRetries
	}
	return o.RouteRetries
}

func (o SessionOptions) drainBudget() int {
	if o.DrainBudget <= 0 {
		return DefaultDrainBudget
	}
	return o.DrainBudget
}

// Result is the conversation-level outcome of one settled (or steered) Turn.
type Result struct {
	TurnID      turn.TurnID
	Status      turn.TurnStatus
	Disposition turn.ResumeDisposition
	// AlreadyDriving reports that another driver in this process carries the
	// Turn: the input is committed, its settlement and reply are reported by
	// that driver. It is a fact about this process, not a Turn disposition.
	AlreadyDriving bool
	// Reply is the settled Turn's last assistant text; empty while the Turn
	// still runs or when the attempt produced no text.
	Reply string
}

// SessionStatus reports what a caller may need to act on after opening.
type SessionStatus struct {
	Active turn.TurnID
	Failed []turn.TurnID
}

// Session is the application's conversation over one owned Session
// (APP-SES): the routing, driving, draining and compaction policies. Every
// command runs through the Writer of the Handle it holds; reads go by
// SessionID.
// Concurrent calls are safe: writes serialize in the Session Writer, and a
// call whose input lands in a running Turn reports already_driving.
type Session struct {
	// Recovered is the takeover disposition count from opening (RUN-CMT-7).
	Recovered int

	app       *Application
	a         *owner.Owner
	h         *owner.Handle
	sid       session.SessionID
	opts      SessionOptions
	newTurnID func() turn.TurnID

	// bg bounds the background drives Submit starts; Close cancels it and
	// waits for them (APP-SES-4). bgN counts the drives in flight and bgIdle
	// is closed when the count returns to zero, so Wait can observe quiescence
	// while later Submits are still allowed.
	bg         context.Context
	cancel     context.CancelFunc
	bgMu       sync.Mutex
	bgN        int
	bgIdle     chan struct{}
	lastActive time.Time

	// loops are the Session's service goroutines (the inbox applier);
	// Close waits for them after cancelling bg. They are not background
	// drives, so Wait does not count them.
	loops sync.WaitGroup
	// inboxWake wakes the applier; inboxMu serializes applier passes.
	inboxWake chan struct{}
	inboxMu   sync.Mutex
	// snapshotMu serializes the background snapshots of this Session.
	snapshotMu sync.Mutex
}

// OpenSession ensures the stream exists, takes ownership per the
// application's Ownership configuration, runs the takeover disposition and
// returns the conversation (APP-SES-1).
func (app *Application) OpenSession(ctx context.Context, sid session.SessionID, opts SessionOptions) (*Session, error) {
	if opts.Preset.ID == "" || opts.Preset.Digest == "" {
		return nil, errors.New("app: open session requires a preset ref")
	}
	a := app.Owner
	if _, err := a.Presets.Resolve(opts.Preset); err != nil {
		return nil, err
	}
	if err := a.EnsureSession(ctx, sid); err != nil {
		return nil, err
	}
	h, err := a.Open(ctx, sid)
	if err != nil {
		return nil, err
	}
	s := &Session{Recovered: h.Recovered, app: app, a: a, h: h, sid: sid, opts: opts, newTurnID: opts.NewTurnID}
	if s.newTurnID == nil {
		s.newTurnID = turn.NewTurnID
	}
	s.bg, s.cancel = context.WithCancel(context.Background())
	s.touch()
	app.track(s)
	// A binding inherited from the fork parent is settled by this Session's
	// policy before anything runs in it (APP-WSP-5).
	if err := s.applyInheritedWorkspace(ctx); err != nil {
		_ = s.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	// Commands left while no one owned the Session are applied before
	// anything else this owner does (APP-INB-3).
	s.startInbox(ctx)
	if opts.ResumeActive {
		if _, _, err := s.Resume(ctx); err != nil {
			_ = s.Close(context.WithoutCancel(ctx))
			return nil, err
		}
	}
	s.startIdleRelease()
	return s, nil
}

// ID is the Session's identity.
func (s *Session) ID() session.SessionID { return s.sid }

// Handle is the ownership capability the conversation runs under.
func (s *Session) Handle() *owner.Handle { return s.h }

func (s *Session) ref(turnID turn.TurnID) turn.TurnRef {
	return turn.TurnRef{SessionID: s.sid, TurnID: turnID}
}

func (s *Session) bgStart() {
	s.bgMu.Lock()
	if s.bgN == 0 {
		s.bgIdle = make(chan struct{})
	}
	s.bgN++
	s.lastActive = time.Now()
	s.bgMu.Unlock()
}

func (s *Session) bgDone() {
	s.bgMu.Lock()
	s.bgN--
	if s.bgN == 0 {
		close(s.bgIdle)
	}
	s.lastActive = time.Now()
	s.bgMu.Unlock()
}

// Wait blocks until every background drive Submit has started so far has
// finished, or ctx ends. It does not cancel anything; Close does.
func (s *Session) Wait(ctx context.Context) error {
	s.bgMu.Lock()
	idle, n := s.bgIdle, s.bgN
	s.bgMu.Unlock()
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

// Status reports the active Turn and the Turns awaiting Retry or Settle.
func (s *Session) Status(ctx context.Context) (SessionStatus, error) {
	surface, err := turn.ReadSurface(ctx, s.a.Projections, s.sid)
	if err != nil {
		return SessionStatus{}, err
	}
	var out SessionStatus
	if v, ok := surface.Active(); ok {
		out.Active = v.TurnID
	}
	for _, id := range surface.Order {
		if surface.Turns[id].Status == turn.TurnAttemptFailed {
			out.Failed = append(out.Failed, id)
		}
	}
	return out, nil
}

// Events is this Session's event stream from now on (OBS-1).
func (s *Session) Events(ctx context.Context) <-chan Event { return s.app.Events(ctx, s.sid) }

// Send submits text and blocks until it is settled or absorbed: the first
// Result is the Turn the input landed in, further Results are backlog Turns
// this call drained after settlement (APP-SES-2).
func (s *Session) Send(ctx context.Context, text string) ([]Result, error) {
	in, err := s.a.Chatlog.Submit(ctx, s.h.Writer(), chatlog.NewInputID(), input.Text(text))
	if err != nil {
		return nil, err
	}
	ref, absorbed, err := s.routeInput(ctx, in)
	if err != nil {
		return nil, err
	}
	if absorbed != nil {
		return []Result{*absorbed}, nil
	}
	resp, err := s.a.Driver.Drive(ctx, s.h.Writer(), ref.TurnID)
	if err != nil {
		return nil, err
	}
	return s.settled(ctx, &resp)
}

// Submit submits text under a fresh InputID; see SubmitInput.
func (s *Session) Submit(ctx context.Context, text string) (turn.TurnRef, error) {
	return s.SubmitInput(ctx, chatlog.NewInputID(), text)
}

// SubmitInput submits text under the caller's InputID, commits its route
// and returns the Turn it landed in without waiting (APP-SES-4). The
// InputID is the idempotency key: a retried submission replays. The Turn is
// driven to settlement -- and the backlog drained -- in the background;
// progress and the reply arrive on Events, failures on Events and
// Config.Warn. Close cancels the background drive; a cancelled Turn stays
// active and resumes on the next open.
func (s *Session) SubmitInput(ctx context.Context, id run.InputID, text string) (turn.TurnRef, error) {
	in, err := s.a.Chatlog.Submit(ctx, s.h.Writer(), id, input.Text(text))
	if err != nil {
		return turn.TurnRef{}, err
	}
	ref, absorbed, err := s.routeInput(ctx, in)
	if err != nil {
		return turn.TurnRef{}, err
	}
	if absorbed != nil {
		// A running driver carries the input; nothing to drive here.
		return s.ref(absorbed.TurnID), nil
	}
	s.driveInBackground(ref)
	return ref, nil
}

// driveInBackground drives the Turn to settlement and drains the backlog
// under bg; failures reach Events and Config.Warn.
func (s *Session) driveInBackground(ref turn.TurnRef) {
	s.bgStart()
	go func() {
		defer s.bgDone()
		resp, err := s.a.Driver.Drive(s.bg, s.h.Writer(), ref.TurnID)
		if err != nil {
			s.app.fail(s.sid, fmt.Errorf("app: driving turn %s: %w", ref.TurnID, err))
			return
		}
		if _, err := s.settled(s.bg, &resp); err != nil {
			s.app.fail(s.sid, fmt.Errorf("app: settling turn %s: %w", ref.TurnID, err))
		}
	}()
}

// Stop stops the active Turn (TRN-CMD Stop); ok is false when no Turn is
// active. The stopped Turn's drive observes the cancellation and returns.
func (s *Session) Stop(ctx context.Context, reason string) (turn.TurnResponse, bool, error) {
	status, err := s.Status(ctx)
	if err != nil || status.Active == "" {
		return turn.TurnResponse{}, false, err
	}
	resp, err := s.a.Turns.Stop(ctx, s.h.Writer(), turn.StopRequest{Ref: s.ref(status.Active), Reason: reason})
	if err != nil {
		return turn.TurnResponse{}, false, err
	}
	return resp, true, nil
}

// ErrRouteContended reports a route that lost to concurrent routes
// RouteRetries times (APP-SES-3). The input is submitted and stays in the
// backlog; the next Send, Drain or Resume routes it. It is a transient
// answer, unlike the turn.ErrConflict of a Turn that awaits Retry or Settle.
var ErrRouteContended = errors.New("app: route contended")

// routeInput commits one input's route with the conflict retry of APP-SES-3,
// bounded by SessionOptions.RouteRetries. It returns the Turn to drive, the
// AlreadyDriving Result when another driver took the input first, or
// ErrRouteContended when every attempt lost to a concurrent route.
func (s *Session) routeInput(ctx context.Context, in run.AgentInput) (turn.TurnRef, *Result, error) {
	var lastErr error
	for attempt := 0; attempt < s.opts.routeRetries(); attempt++ {
		ref, err := s.commitRoute(ctx, []run.AgentInput{in})
		if err == nil {
			return ref, nil, nil
		}
		if !errors.Is(err, turn.ErrConflict) {
			return turn.TurnRef{}, nil, err
		}
		if awaitsDecision(err) {
			// A Turn awaiting Retry or Settle is the committed state's
			// answer, not a race.
			return turn.TurnRef{}, nil, err
		}
		lastErr = err
		if r, taken := s.absorbed(ctx, in); taken {
			return turn.TurnRef{}, &r, nil
		}
	}
	return turn.TurnRef{}, nil, fmt.Errorf("%w after %d attempts: %s", ErrRouteContended, s.opts.routeRetries(), lastErr.Error())
}

// errAwaitsDecision marks the route conflict of a Turn that awaits Retry or
// Settle: a state, not a race.
var errAwaitsDecision = errors.New("turn awaits retry or settle")

func awaitsDecision(err error) bool { return errors.Is(err, errAwaitsDecision) }

// Route is APP-RTE-1: commit the inputs' route -- Deliver into the active
// Turn, or Start a new one -- then drive the Turn to its next quiescent point.
// A Turn awaiting Retry or Settle is a conflict: those are the caller's
// decisions.
func (s *Session) Route(ctx context.Context, inputs []run.AgentInput) (driver.DriveResult, error) {
	ref, err := s.commitRoute(ctx, inputs)
	if err != nil {
		return driver.DriveResult{}, err
	}
	return s.a.Driver.Drive(ctx, s.h.Writer(), ref.TurnID)
}

// commitRoute is the commit half of Route: Deliver into the active Turn or
// Start a new one, returning the Turn the inputs landed in.
func (s *Session) commitRoute(ctx context.Context, inputs []run.AgentInput) (turn.TurnRef, error) {
	surface, err := turn.ReadSurface(ctx, s.a.Projections, s.sid)
	if err != nil {
		return turn.TurnRef{}, err
	}
	if active, ok := surface.Active(); ok {
		ref := s.ref(active.TurnID)
		if _, err := s.a.Turns.Deliver(ctx, s.h.Writer(), turn.DeliverRequest{Ref: ref, Inputs: inputs}); err != nil {
			return turn.TurnRef{}, err
		}
		return ref, nil
	}
	for _, id := range surface.Order {
		if surface.Turns[id].Status == turn.TurnAttemptFailed {
			return turn.TurnRef{}, fmt.Errorf("%w: %w: turn %s", turn.ErrConflict, errAwaitsDecision, id)
		}
	}
	ref := s.ref(s.newTurnID())
	if _, err := s.a.Turns.Start(ctx, s.h.Writer(), turn.StartRequest{Ref: ref, Inputs: inputs, Preset: s.opts.Preset}); err != nil {
		return turn.TurnRef{}, err
	}
	return ref, nil
}

// Drain is APP-RTE-2: start the next Turn from the backlog of submitted,
// undelivered inputs; ok is false when there is none.
func (s *Session) Drain(ctx context.Context) (driver.DriveResult, bool, error) {
	chat, err := chatlog.ReadSurface(ctx, s.a.Projections, s.sid)
	if err != nil {
		return driver.DriveResult{}, false, err
	}
	pending := chat.SubmittedInputs()
	if len(pending) == 0 {
		return driver.DriveResult{}, false, nil
	}
	inputs := make([]run.AgentInput, len(pending))
	for i, in := range pending {
		inputs[i] = run.AgentInput{ID: run.InputID(in.ID), Digest: in.Digest}
	}
	resp, err := s.Route(ctx, inputs)
	return resp, err == nil, err
}

// absorbed reports whether another driver already delivered the input; the
// Turn that took it settles and reports there.
func (s *Session) absorbed(ctx context.Context, in run.AgentInput) (Result, bool) {
	chat, err := chatlog.ReadSurface(ctx, s.a.Projections, s.sid)
	if err != nil {
		return Result{}, false
	}
	v, ok := chat.Inputs.Get(chatlog.InputID(in.ID))
	if !ok || v.Status == chatlog.InputSubmitted {
		return Result{}, false
	}
	r := Result{TurnID: turn.TurnID(v.Input.TurnID), AlreadyDriving: true}
	if surface, serr := turn.ReadSurface(ctx, s.a.Projections, s.sid); serr == nil {
		r.Status = surface.Turns[r.TurnID].Status
	}
	return r, true
}

// Resume drives a still-active Turn (after a restart) to settlement; ok is
// false when no Turn is active.
func (s *Session) Resume(ctx context.Context) ([]Result, bool, error) {
	status, err := s.Status(ctx)
	if err != nil || status.Active == "" {
		return nil, false, err
	}
	resp, err := s.a.Driver.Drive(ctx, s.h.Writer(), status.Active)
	if err != nil {
		return nil, false, err
	}
	out, err := s.settled(ctx, &resp)
	return out, true, err
}

// Retry retries the first Turn awaiting Retry; ok is false when none is.
func (s *Session) Retry(ctx context.Context) ([]Result, bool, error) {
	ref, ok, err := s.retryCommit(ctx, "app retry")
	if err != nil || !ok {
		return nil, false, err
	}
	resp, err := s.a.Driver.Drive(ctx, s.h.Writer(), ref.TurnID)
	if err != nil {
		return nil, false, err
	}
	out, err := s.settled(ctx, &resp)
	return out, true, err
}

// retryCommit commits the Retry of the first Turn awaiting one; ok is false
// when none is.
func (s *Session) retryCommit(ctx context.Context, reason string) (turn.TurnRef, bool, error) {
	status, err := s.Status(ctx)
	if err != nil || len(status.Failed) == 0 {
		return turn.TurnRef{}, false, err
	}
	ref := s.ref(status.Failed[0])
	previous, err := s.a.Turns.Status(ctx, ref)
	if err != nil {
		return turn.TurnRef{}, false, err
	}
	if _, err := s.a.Turns.Retry(ctx, s.h.Writer(), turn.RetryRequest{Ref: ref, PreviousRunID: previous.RunID, Reason: reason}); err != nil {
		return turn.TurnRef{}, false, err
	}
	return ref, true, nil
}

// settled turns a TurnResponse into Results and drains the backlog: while a
// settlement leaves submitted, undelivered inputs, the next Turn starts from
// them (APP-RTE-2). When the backlog is drained and no Turn is active, the
// automatic compaction policy runs (APP-CKP-1).
func (s *Session) settled(ctx context.Context, resp *driver.DriveResult) ([]Result, error) {
	out := []Result{s.result(ctx, resp)}
	if resp.AlreadyDriving {
		// The running driver settles the Turn and drains in its own call.
		return out, nil
	}
	for range s.opts.drainBudget() {
		next, ok, err := s.Drain(ctx)
		if err != nil {
			if errors.Is(err, turn.ErrConflict) {
				// A concurrent Send or drain took the backlog; it reports there.
				return out, nil
			}
			return out, err
		}
		if !ok {
			s.maybeSnapshot(ctx)
			s.maybeCompact(ctx)
			return out, nil
		}
		out = append(out, s.result(ctx, &next))
		if next.AlreadyDriving {
			return out, nil
		}
	}
	return out, ErrDrainBudget
}

// result wraps a TurnResponse with the settled Turn's reply; materialization
// failures are reported to Warn and leave Reply empty.
func (s *Session) result(ctx context.Context, resp *driver.DriveResult) Result {
	r := Result{TurnID: resp.Ref.TurnID, Status: resp.Status, Disposition: resp.Disposition, AlreadyDriving: resp.AlreadyDriving}
	if !resp.AlreadyDriving && resp.Disposition == turn.ResumeFinished {
		text, err := s.app.Reply(ctx, resp.Ref)
		if err != nil {
			s.app.warn(fmt.Errorf("app: materialize reply of turn %s: %w", resp.Ref.TurnID, err))
		}
		r.Reply = text
	}
	return r
}

// BindWorkspace binds the Session to the Workspace (APP-WSP-2): from the
// next tool call on, its workspace-placed tools run there. A Session that
// inherited the binding from its fork parent makes it its own.
func (s *Session) BindWorkspace(ctx context.Context, id workspace.ID) error {
	if s.app.workspaces == nil {
		return ErrNoWorkspaces
	}
	if _, err := s.app.workspaces.Store.Get(ctx, id); err != nil {
		return err
	}
	return s.app.bindings.Bind(ctx, s.h.Writer(), id)
}

// UnbindWorkspace records that the Session works in no Workspace, ending
// its own or an inherited binding.
func (s *Session) UnbindWorkspace(ctx context.Context, reason string) error {
	if s.app.workspaces == nil {
		return ErrNoWorkspaces
	}
	return s.app.bindings.Unbind(ctx, s.h.Writer(), reason)
}

// Workspace is the Session's current binding.
func (s *Session) Workspace(ctx context.Context) (workspace.Binding, error) {
	return s.app.Workspace(ctx, s.sid)
}

// ErrNoSnapshots reports a snapshot in a process with no Snapshotter: the
// workspace backend takes snapshots, in this process or through its client
// (Config.Workspaces.Snapshots).
var ErrNoSnapshots = errors.New("app: no workspace snapshotter is configured")

// ErrUnboundWorkspace reports a workspace operation on a Session bound to
// no Workspace.
var ErrUnboundWorkspace = errors.New("app: the session is bound to no workspace")

// SnapshotWorkspace takes a Snapshot of the Session's bound Workspace and
// records it on the Session (APP-WSP-7). A Workspace never materialized
// yields sandbox.ErrNothingToSnapshot.
func (s *Session) SnapshotWorkspace(ctx context.Context) (workspace.Snapshot, error) {
	if s.app.workspaces == nil {
		return workspace.Snapshot{}, ErrNoWorkspaces
	}
	if s.app.snapshots == nil {
		return workspace.Snapshot{}, ErrNoSnapshots
	}
	b, err := s.Workspace(ctx)
	if err != nil {
		return workspace.Snapshot{}, err
	}
	if !b.Bound {
		return workspace.Snapshot{}, ErrUnboundWorkspace
	}
	snap, err := s.app.snapshots.Snapshot(ctx, b.Workspace)
	if err != nil {
		return workspace.Snapshot{}, err
	}
	if err := s.app.bindings.RecordSnapshot(ctx, s.h.Writer(), &snap); err != nil {
		return workspace.Snapshot{}, err
	}
	return snap, nil
}

// maybeSnapshot is the SnapshotAfterTurn policy: after a settlement that
// drained the backlog, the bound Workspace is snapshotted in the
// background, one snapshot at a time per Session, so the settlement does
// not wait on the copy; Wait covers it. A Session bound to none or a
// Workspace with no environment yet is nothing to do, other failures reach
// Config.Warn.
func (s *Session) maybeSnapshot(context.Context) {
	if s.app.workspaces == nil || !s.app.workspaces.SnapshotAfterTurn || s.app.snapshots == nil {
		return
	}
	s.bgStart()
	go func() {
		defer s.bgDone()
		s.snapshotMu.Lock()
		defer s.snapshotMu.Unlock()
		_, err := s.SnapshotWorkspace(s.bg)
		if err != nil && !errors.Is(err, ErrUnboundWorkspace) && !errors.Is(err, workspace.ErrNothingToSnapshot) && s.bg.Err() == nil {
			s.app.warn(fmt.Errorf("app: snapshot of the workspace of %s: %w", s.sid, err))
		}
	}()
}

// applyInheritedWorkspace runs SessionOptions.InheritedWorkspace on a
// binding this Session inherited (APP-WSP-5); an own binding, or none, is
// left alone.
func (s *Session) applyInheritedWorkspace(ctx context.Context) error {
	if s.app.workspaces == nil || s.opts.InheritedWorkspace == workspace.InheritShare {
		return nil
	}
	b, err := s.Workspace(ctx)
	if err != nil {
		return err
	}
	if !b.InheritedBy(s.sid) {
		return nil
	}
	store := s.app.workspaces.Store
	parent, err := store.Get(ctx, b.Workspace)
	if err != nil {
		return fmt.Errorf("app: inherited workspace %s: %w", b.Workspace, err)
	}
	var own workspace.Workspace
	switch policy := s.opts.InheritedWorkspace; policy {
	case workspace.InheritNone:
		return s.UnbindWorkspace(ctx, "inherited workspace policy: none")
	case workspace.InheritAllocate:
		own = workspace.Workspace{ID: workspace.NewID(), Project: parent.Project, Base: parent.Base}
		if err := store.Create(ctx, own); err != nil {
			return err
		}
	case workspace.InheritClone:
		if parent.Snapshot == nil {
			return fmt.Errorf("app: inherited workspace policy clone: workspace %s has no snapshot", parent.ID)
		}
		if own, err = store.Fork(ctx, workspace.Fork{Source: parent.ID, Destination: workspace.NewID(), Snapshot: *parent.Snapshot}); err != nil {
			return err
		}
	case workspace.InheritRestore:
		if b.Snapshot == "" {
			return fmt.Errorf("app: inherited workspace policy restore: no snapshot of %s is recorded at the fork point", parent.ID)
		}
		if own, err = store.Fork(ctx, workspace.Fork{Source: parent.ID, Destination: workspace.NewID(), Snapshot: b.Snapshot}); err != nil {
			return err
		}
	default:
		return fmt.Errorf("app: unknown inherited workspace policy %d", policy)
	}
	return s.BindWorkspace(ctx, own.ID)
}

// Compact summarizes the context with the preset's model and commits a
// compaction retaining a pair-closed suffix; ok is false when the context is
// already within the retain window (APP-CKP-1). It runs between Turns and,
// under the quiescent-Run guard, between the steps of a Turn.
func (s *Session) Compact(ctx context.Context) (chatlog.CompactionID, bool, error) {
	policy := compaction.Policy{AfterEntries: s.opts.CompactAfterEntries, RetainEntries: s.opts.CompactRetainEntries}
	cctx, err := chatlog.ReadContext(ctx, s.h.Writer().Projections(), s.sid)
	if err != nil {
		return "", false, err
	}
	retain, withinWindow := policy.Retain(cctx.Entries)
	if withinWindow {
		return "", false, nil
	}
	materialized, err := chatlog.NewMaterializer(s.a.Content).Entries(ctx, cctx.Entries)
	if err != nil {
		return "", false, err
	}
	summary, err := compaction.Summarizer{
		ResolvePreset: s.a.Presets.Resolve, Content: s.a.Frozen, Executor: s.a.Executor, Watcher: s.a.Driver.OutcomeWatcher(),
	}.Summarize(ctx, s.sid, s.opts.Preset, materialized)
	if err != nil {
		return "", false, err
	}
	id, err := s.a.Chatlog.Compact(ctx, s.h.Writer(), summary, retain, turn.RequireQuiescentRun)
	if err != nil {
		return "", false, err
	}
	return id, true, nil
}

// maybeCompact runs the automatic policy after a settlement and between the
// steps of a Turn (APP-CKP-1); failures reach the caller through
// CompactWarn and never change the settled results or stop the drive.
func (s *Session) maybeCompact(ctx context.Context) {
	if s.opts.CompactAfterEntries <= 0 {
		return
	}
	cctx, err := chatlog.ReadContext(ctx, s.h.Writer().Projections(), s.sid)
	if err == nil && len(cctx.Entries) <= s.opts.CompactAfterEntries {
		return
	}
	if err == nil {
		_, _, err = s.Compact(ctx)
	}
	if err != nil && s.opts.CompactWarn != nil {
		s.opts.CompactWarn(err)
	}
}

// Close cancels the background drives Submit started, waits for them to
// return, then releases this Session's ownership; other Sessions of the
// application stay open. A Turn a cancelled drive left active resumes on the
// next open.
func (s *Session) Close(ctx context.Context) error {
	s.app.untrack(s)
	if s.cancel != nil {
		s.cancel()
	}
	s.loops.Wait()
	_ = s.Wait(context.Background()) // drives observe the cancelled bg ctx and return
	return s.h.Close(ctx)
}
