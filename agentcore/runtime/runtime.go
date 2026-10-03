// Package runtime is the conversation's execution over the fact and effect
// layers: the SessionRuntime admits inputs into Turns and advances the
// Session to its next quiescent point, and the Execution assembles the
// drive chain (the settlement subscription, the Driver, the Recovery, the
// decision identities, the effect port) over one Session kernel. The Turn
// protocol itself -- the Coordinator, the quiescence guards, the request
// and result vocabulary -- is the session kernel's (agentcore/sessionkernel);
// this package re-exports that vocabulary for the hosts and the runtime.
// Every call runs on the caller's goroutine and ctx; which calls run in
// the background, what a reply is and which policies run at quiescence are
// the host's decisions, taken on the Settlement each call returns.
package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"github.com/felinics/twilight/agentcore/turn"
)

// Turns is the Turn protocol the runtime routes inputs into and reads
// status back from: the session kernel's Commands and Reader.
type Turns interface {
	sessionkernel.Commands
	sessionkernel.Reader
}

// DriveResult is what one drive of a Turn reports: the Turn's committed
// answer, and whether another local driver of the same Run was already
// carrying it, in which case this call drove nothing and the answer is the
// status as read. AlreadyDriving is a fact about this process, not about
// the Turn, so it is not a Turn disposition: the Turn's durable vocabulary
// stays the Turn module's.
type DriveResult struct {
	sessionkernel.TurnResult
	AlreadyDriving bool
}

// Settlement is what one advance of the Session reports: the Turns it
// drove, in order -- the Turn it began with, then every Turn it started
// from inputs that were submitted but not yet delivered. Quiescent reports
// that no such input remains and no Turn is active: the point at which the
// host's quiescence policies apply. It is false when another driver
// carries the settlement (AlreadyDriving), when the advance stopped at the
// TurnBudget and when a concurrent advance took the pending inputs.
type Settlement struct {
	Turns     []DriveResult
	Quiescent bool
}

// Config composes one SessionRuntime. Writer is the ownership capability
// every command commits through; Driver, Turns, Chatlog and Projections are
// the composed core services of the same Session.
type Config struct {
	Writer      writer.Writer
	Driver      *driver.Driver
	Turns       Turns
	Chatlog     *chatlog.Commands
	Projections session.ProjectionReader
	// Preset is the decision identity every Turn the runtime starts runs
	// under; required.
	Preset preset.PresetRef
	// NewTurnID mints TurnIDs for new Turns; nil selects the random default.
	NewTurnID func() turn.TurnID
	// RouteRetries bounds how many times one input's route is re-committed
	// after a conflict with a concurrent route before the last conflict is
	// returned; the input stays submitted and the next Submit, Advance or
	// Resume routes it. Zero selects DefaultRouteRetries.
	RouteRetries int
	// TurnBudget bounds how many Turns one advance starts from submitted,
	// undelivered inputs before returning ErrTurnBudget with the Turns so
	// far; the remaining inputs stay submitted for the next call. Zero
	// selects DefaultTurnBudget.
	TurnBudget int
}

// DefaultRouteRetries and DefaultTurnBudget are the liveness bounds a
// Config with zero values takes.
const (
	DefaultRouteRetries = 4
	DefaultTurnBudget   = 64
)

var (
	// ErrRouteContended reports a route that lost to concurrent routes
	// RouteRetries times. The input is submitted and stays undelivered; the
	// next Submit, Advance or Resume routes it. It is a transient answer,
	// unlike the turn.ErrConflict of a Turn that admits no route.
	ErrRouteContended = errors.New("runtime: route contended")
	// ErrTurnBudget reports an advance that stopped at the TurnBudget with
	// inputs still submitted.
	ErrTurnBudget = errors.New("runtime: turn budget exhausted with inputs still submitted")
)

// SessionRuntime is the conversation process over one owned Session. Submit
// admits an input into a Turn, Advance drives a Turn to settlement and on
// through every Turn the remaining inputs start, Resume does the same for
// a Session as found after a restart, and Stop settles the active Turn.
// Every command runs through the Config's Writer; reads go by SessionID.
// Concurrent calls are safe: writes serialize in the Writer, and a call
// whose input lands in a running Turn reports AlreadyDriving. It starts no
// goroutine of its own.
type SessionRuntime struct {
	w      writer.Writer
	driver *driver.Driver
	turns  Turns
	chat   *chatlog.Commands
	proj   session.ProjectionReader
	sid    session.SessionID
	preset preset.PresetRef
	newID  func() turn.TurnID

	routeRetries int
	turnBudget   int
}

// New returns the conversation process for one owned Session.
func New(cfg Config) (*SessionRuntime, error) { //nolint:gocritic // hugeParam: Config is a by-value options struct read once
	if cfg.Writer == nil {
		return nil, errors.New("runtime: a writer is required")
	}
	if cfg.Driver == nil {
		return nil, errors.New("runtime: a driver is required")
	}
	if cfg.Turns == nil {
		return nil, errors.New("runtime: turn commands are required")
	}
	if cfg.Chatlog == nil {
		return nil, errors.New("runtime: chatlog commands are required")
	}
	if cfg.Projections == nil {
		return nil, errors.New("runtime: a projection reader is required")
	}
	if cfg.Preset.ID == "" || cfg.Preset.Digest == "" {
		return nil, errors.New("runtime: a preset ref is required")
	}
	newID := cfg.NewTurnID
	if newID == nil {
		newID = turn.NewTurnID
	}
	retry := cfg.RouteRetries
	if retry <= 0 {
		retry = DefaultRouteRetries
	}
	budget := cfg.TurnBudget
	if budget <= 0 {
		budget = DefaultTurnBudget
	}
	return &SessionRuntime{w: cfg.Writer, driver: cfg.Driver, turns: cfg.Turns, chat: cfg.Chatlog, proj: cfg.Projections,
		sid: cfg.Writer.SessionID(), preset: cfg.Preset, newID: newID, routeRetries: retry, turnBudget: budget}, nil
}

func (r *SessionRuntime) ref(turnID turn.TurnID) turn.TurnRef {
	return turn.TurnRef{SessionID: r.sid, TurnID: turnID}
}

// Submitted is what Submit reports: the Turn the input landed in, and
// whether another driver of this process is already carrying that Turn, in
// which case it settles and reports there and the caller has nothing to
// advance.
type Submitted struct {
	Ref            turn.TurnRef
	AlreadyDriving bool
}

// Submit records one input body under id and commits its route: into the
// active Turn when there is one, into a new Turn otherwise. The body is
// opaque to the runtime; the idempotency key is the id, so a retried
// submission replays. Nothing is driven: the caller advances the Turn,
// here or on another goroutine, with Advance.
func (r *SessionRuntime) Submit(ctx context.Context, id run.InputID, content run.CanonicalJSON) (Submitted, error) {
	in, err := r.chat.Submit(ctx, r.w, id, content)
	if err != nil {
		return Submitted{}, err
	}
	var lastErr error
	for attempt := 0; attempt < r.routeRetries; attempt++ {
		ref, err := r.route(ctx, []run.AgentInput{in})
		if err == nil {
			return Submitted{Ref: ref}, nil
		}
		if !errors.Is(err, turn.ErrConflict) {
			return Submitted{}, err
		}
		lastErr = err
		if taken, ok := r.absorbed(ctx, in); ok {
			return Submitted{Ref: taken.Ref, AlreadyDriving: true}, nil
		}
	}
	return Submitted{}, fmt.Errorf("%w after %d attempts: %s", ErrRouteContended, r.routeRetries, lastErr.Error())
}

// Send submits the input and advances the Session to quiescence: the first
// Turn of the Settlement is the one the input landed in, the rest are the
// Turns the remaining inputs started. When another driver of this process
// took the input, the single Turn reports AlreadyDriving and that driver
// settles.
func (r *SessionRuntime) Send(ctx context.Context, id run.InputID, content run.CanonicalJSON) (Settlement, error) {
	sub, err := r.Submit(ctx, id, content)
	if err != nil {
		return Settlement{}, err
	}
	if sub.AlreadyDriving {
		resp, _ := r.absorbedStatus(ctx, sub.Ref)
		return Settlement{Turns: []DriveResult{resp}}, nil
	}
	return r.Advance(ctx, sub.Ref.TurnID)
}

// Advance drives the Turn to its next quiescent point and, while its
// settlement leaves inputs submitted but undelivered, starts the next Turn
// from them and drives it, until no input remains and no Turn is active:
// the Settlement is then Quiescent. An advance that stops at the TurnBudget
// returns the Turns so far with ErrTurnBudget. The caller's ctx bounds the
// drive: a cancelled drive leaves the Turn active for the next Resume.
func (r *SessionRuntime) Advance(ctx context.Context, turnID turn.TurnID) (Settlement, error) {
	resp, err := r.drive(ctx, turnID)
	if err != nil {
		return Settlement{}, err
	}
	return r.settle(ctx, resp)
}

// Resume advances a Session as found after a restart: the still-active Turn
// when there is one, otherwise the Turn the submitted, undelivered inputs
// start. ok is false when there is neither.
func (r *SessionRuntime) Resume(ctx context.Context) (Settlement, bool, error) {
	if active, ok, err := r.active(ctx); err != nil {
		return Settlement{}, false, err
	} else if ok {
		out, err := r.Advance(ctx, active)
		return out, true, err
	}
	resp, ok, err := r.next(ctx)
	if err != nil || !ok {
		return Settlement{}, false, err
	}
	out, err := r.settle(ctx, resp)
	return out, true, err
}

// Stop stops the active Turn; ok is false when no Turn is active. The
// stopped Turn's drive observes the cancellation and returns.
func (r *SessionRuntime) Stop(ctx context.Context, reason string) (sessionkernel.TurnResult, bool, error) {
	active, ok, err := r.active(ctx)
	if err != nil || !ok {
		return sessionkernel.TurnResult{}, false, err
	}
	resp, err := r.turns.Stop(ctx, r.w, sessionkernel.StopRequest{Ref: r.ref(active), Reason: reason})
	return resp, true, err
}

func (r *SessionRuntime) active(ctx context.Context) (turn.TurnID, bool, error) {
	surface, err := turn.ReadSurface(ctx, r.proj, r.sid)
	if err != nil {
		return "", false, err
	}
	if a, ok := surface.Active(); ok {
		return a.TurnID, true, nil
	}
	return "", false, nil
}

// drive runs the Turn to its next quiescent point and reads its committed
// answer; AlreadyDriving reports a concurrent local driver of the same Run
// carried it, in which case the answer is the status as read.
func (r *SessionRuntime) drive(ctx context.Context, turnID turn.TurnID) (DriveResult, error) {
	taken, err := r.driver.Drive(ctx, r.w, turnID)
	if err != nil {
		return DriveResult{}, err
	}
	resp, err := r.turns.Status(ctx, r.ref(turnID))
	if err != nil {
		return DriveResult{}, err
	}
	return DriveResult{TurnResult: resp, AlreadyDriving: taken}, nil
}

// route commits the inputs' route: Deliver into the active Turn when there
// is one, Start a new one when there is none.
func (r *SessionRuntime) route(ctx context.Context, inputs []run.AgentInput) (turn.TurnRef, error) {
	surface, err := turn.ReadSurface(ctx, r.proj, r.sid)
	if err != nil {
		return turn.TurnRef{}, err
	}
	if active, ok := surface.Active(); ok {
		ref := r.ref(active.TurnID)
		if _, err := r.turns.Deliver(ctx, r.w, sessionkernel.DeliverRequest{Ref: ref, Inputs: inputs}); err != nil {
			return turn.TurnRef{}, err
		}
		return ref, nil
	}
	ref := r.ref(r.newID())
	if _, err := r.turns.Start(ctx, r.w, sessionkernel.StartRequest{Ref: ref, Inputs: inputs, Preset: r.preset}); err != nil {
		return turn.TurnRef{}, err
	}
	return ref, nil
}

// absorbed reports whether another driver already delivered the input; the
// Turn that took it settles and reports there.
func (r *SessionRuntime) absorbed(ctx context.Context, in run.AgentInput) (DriveResult, bool) {
	chat, err := chatlog.ReadSurface(ctx, r.proj, r.sid)
	if err != nil {
		return DriveResult{}, false
	}
	v, ok := chat.Inputs.Get(chatlog.InputID(in.ID))
	if !ok || v.Status == chatlog.InputSubmitted {
		return DriveResult{}, false
	}
	resp, _ := r.absorbedStatus(ctx, r.ref(turn.TurnID(v.Input.TurnID)))
	return resp, true
}

// absorbedStatus is the AlreadyDriving answer for a Turn another driver
// carries: its status as read, or its Ref alone when the read fails.
func (r *SessionRuntime) absorbedStatus(ctx context.Context, ref turn.TurnRef) (DriveResult, error) {
	out := DriveResult{TurnResult: sessionkernel.TurnResult{Ref: ref}, AlreadyDriving: true}
	resp, err := r.turns.Status(ctx, ref)
	if err == nil {
		out.TurnResult = resp
	}
	return out, err
}

// next starts a Turn from the submitted, undelivered inputs and drives it;
// ok is false when there is none.
func (r *SessionRuntime) next(ctx context.Context) (DriveResult, bool, error) {
	chat, err := chatlog.ReadSurface(ctx, r.proj, r.sid)
	if err != nil {
		return DriveResult{}, false, err
	}
	pending := chat.SubmittedInputs()
	if len(pending) == 0 {
		return DriveResult{}, false, nil
	}
	inputs := make([]run.AgentInput, len(pending))
	for i, in := range pending {
		inputs[i] = run.AgentInput{ID: run.InputID(in.ID), Digest: in.Digest}
	}
	ref, err := r.route(ctx, inputs)
	if err != nil {
		return DriveResult{}, false, err
	}
	resp, err := r.drive(ctx, ref.TurnID)
	return resp, err == nil, err
}

// settle is what follows one drive: while the settlement leaves submitted,
// undelivered inputs, the next Turn starts from them; when none remains and
// no Turn is active, the Settlement is Quiescent.
func (r *SessionRuntime) settle(ctx context.Context, resp DriveResult) (Settlement, error) {
	out := Settlement{Turns: []DriveResult{resp}}
	if resp.AlreadyDriving {
		// The running driver settles the Turn and advances in its own call.
		return out, nil
	}
	for range r.turnBudget {
		next, ok, err := r.next(ctx)
		if err != nil {
			if errors.Is(err, turn.ErrConflict) {
				// A concurrent advance took the inputs; it reports there.
				return out, nil
			}
			return out, err
		}
		if !ok {
			out.Quiescent = true
			return out, nil
		}
		out.Turns = append(out.Turns, next)
		if next.AlreadyDriving {
			return out, nil
		}
	}
	return out, ErrTurnBudget
}
