package spawn

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agentcore/execution"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// Options configures the subagent tool (SPN).
type Options struct {
	// Tool is the ToolRef the model calls; empty selects DefaultTool.
	Tool run.ToolRef
	// Presets resolves the optional preset name in the arguments to the
	// PresetRef the child runs under. Nil allows no name: the child runs
	// under the parent Turn's preset.
	Presets func(name string) (preset.PresetRef, error)
	// MaxDepth bounds nesting: a Session at this depth cannot spawn. Zero
	// selects DefaultDepth.
	MaxDepth int
}

// ToolRef is the tool the model calls.
func (o Options) ToolRef() run.ToolRef {
	if o.Tool == "" {
		return DefaultTool
	}
	return o.Tool
}

// ExecutableTool is the model-facing definition of the spawn tool for preset
// catalogs. Its Execute never runs: the tool's ResponsePolicy is
// ExternalResponse and the Responder answers the wait (SPN-1).
func (o Options) ExecutableTool() local.ExecutableTool { return Tool(o.ToolRef()) }

func (o Options) depth() int {
	if o.MaxDepth <= 0 {
		return DefaultDepth
	}
	return o.MaxDepth
}

// Children is what the Responder creates, reads and drives child Sessions
// through: the host's conversation over an owned Session, so a child is
// routed and advanced exactly as any Session the host opens.
type Children interface {
	// Open opens sid as an owned child under preset and returns its
	// conversation; the caller closes it.
	Open(ctx context.Context, sid session.SessionID, preset preset.PresetRef) (Child, error)
	// Create makes an empty child Session whose segment carries ext.
	Create(ctx context.Context, sid session.SessionID, ext module.Extensions) error
	// ForkBeforeInputs forks parent at the commit before turnID's inputs
	// were submitted, the child's segment carrying ext: the conversation as
	// it stood before that Turn was asked.
	ForkBeforeInputs(ctx context.Context, parent session.SessionID, turnID turn.TurnID, child session.SessionID, ext module.Extensions) error
	// Header is the Session's tip segment header; a Session that does not
	// exist is session.ErrNotFound.
	Header(ctx context.Context, sid session.SessionID) (session.SegmentHeader, error)
	// TurnSurface reads the Session's Turns.
	TurnSurface(ctx context.Context, sid session.SessionID) (turn.TurnSurface, error)
	// AwaitingRecovery reports that the Turn is active with an execution in
	// flight that no process of this host drives: its Run waits for the
	// takeover disposition.
	AwaitingRecovery(ctx context.Context, ref turn.TurnRef) (bool, error)
	// InputText is the text of the Turn's first input, empty when it has none
	// or its body is not text.
	InputText(ctx context.Context, ref turn.TurnRef) (string, error)
	// Reply is the settled Turn's reply.
	Reply(ctx context.Context, ref turn.TurnRef) (string, error)
}

// Child is one opened child Session's conversation.
type Child interface {
	// Resume steps the child as found: its active Turn, or the Turn its
	// submitted inputs start; ok is false when there is neither. It returns
	// the Turns it stepped, in order.
	Resume(ctx context.Context) (turns []turn.TurnID, ok bool, err error)
	// Send submits text and returns once the Turn it landed in ended or
	// waits on something the host does not carry; it returns that Turn.
	Send(ctx context.Context, text string) (turn.TurnID, error)
	// Settle steps the Turn on until it ends or waits on something the host
	// does not carry.
	Settle(ctx context.Context, turnID turn.TurnID) error
	Close(ctx context.Context) error
}

// Responder is the subagent tool's execution.Responder (SPN-1): a spawn call
// waits for an external response, and this answers it by creating the child
// Session (or continuing the one on record), driving it through the host's
// conversation to a settled Turn and returning the child's reply. Every
// step is idempotent against the child's durable state, so a process that
// died anywhere in the sequence is continued, not repeated, when the next
// owner opens the parent and the Engine asks for the answer again (SPN-4).
// Nothing about the call lives in an execution record: the child Session is
// the durable state.
type Responder struct {
	opts     Options
	children Children

	mu       sync.Mutex
	inflight map[session.SessionID]context.CancelFunc
	closed   bool
}

// NewResponder returns the subagent Responder; Bind supplies the host it
// drives children through once that host exists.
func NewResponder(opts Options) *Responder {
	return &Responder{opts: opts, inflight: make(map[session.SessionID]context.CancelFunc)}
}

// Bind supplies the host's child conversations. It must precede the first
// Respond.
func (r *Responder) Bind(children Children) { r.children = children }

// Close cancels every child drive; their Turns stay active and resume on
// the next open of the parent, when the Engine asks for the answer again.
func (r *Responder) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for _, cancel := range r.inflight {
		cancel()
	}
}

// Respond is execution.Responder. Argument, preset and depth errors are
// returned before any child exists, so the call is rejected and no Session
// is created (SPN-3); a child on record for different arguments is a
// conflict.
func (r *Responder) Respond(ctx context.Context, w writer.Writer, call *execution.WaitingCall) (jsonstable.Value, error) {
	if r.children == nil {
		return jsonstable.Value{}, errors.New("spawn: responder is not bound to a host")
	}
	parent := w.SessionID()
	args, err := DecodeArguments(call.Arguments)
	if err != nil {
		return jsonstable.Value{}, err
	}
	depth, err := r.depthOf(ctx, parent)
	if err != nil {
		return jsonstable.Value{}, err
	}
	if DepthExceeded(depth, r.opts.depth()) {
		return jsonstable.Value{}, fmt.Errorf("spawn: session %s is at depth %d, the limit", parent, depth)
	}
	pref, err := r.childPreset(ctx, parent, call.Request.RunID, args)
	if err != nil {
		return jsonstable.Value{}, err
	}
	child := ChildID(parent, call.Request.RunID, call.Request.CallID)
	ctx, done, err := r.track(ctx, child)
	if err != nil {
		return jsonstable.Value{}, err
	}
	defer done()
	prov, exists, err := r.provenance(ctx, child)
	if err != nil {
		return jsonstable.Value{}, err
	}
	if !exists {
		if prov, err = r.create(ctx, parent, call.Request.RunID, call.Request.CallID, child, args, depth+1); err != nil {
			return jsonstable.Value{}, fmt.Errorf("create subagent: %w", err)
		}
	} else if ArgumentsConflict(prov, args) {
		return jsonstable.Value{}, fmt.Errorf("call %s already spawned %s with different arguments", call.Request.CallID, child)
	}
	c, err := r.children.Open(ctx, child, pref)
	if err != nil {
		return jsonstable.Value{}, fmt.Errorf("open subagent: %w", err)
	}
	defer func() { _ = c.Close(context.WithoutCancel(ctx)) }()
	turnID, err := r.settle(ctx, c, child, prov.Arguments.Task)
	if err != nil {
		return jsonstable.Value{}, fmt.Errorf("drive subagent: %w", err)
	}
	ref := turn.TurnRef{SessionID: child, TurnID: turnID}
	surface, err := r.children.TurnSurface(ctx, child)
	if err != nil {
		return jsonstable.Value{}, err
	}
	status := surface.Turns[turnID].Status
	if status != turn.TurnCompleted {
		return jsonstable.Value{}, fmt.Errorf("subagent %s turn %s ended %s", child, turnID, status)
	}
	reply, err := r.children.Reply(ctx, ref)
	if err != nil {
		return jsonstable.Value{}, err
	}
	return jsonstable.FromValue(Result{ChildSession: child, TurnID: turnID, Status: status, Reply: reply})
}

// track registers an in-flight answer for child so Close can cancel it; a
// second answer for the same child while one runs is refused.
func (r *Responder) track(ctx context.Context, child session.SessionID) (context.Context, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, nil, errors.New("spawn: responder closed")
	}
	if _, dup := r.inflight[child]; dup {
		return nil, nil, fmt.Errorf("spawn: subagent %s is already being answered", child)
	}
	ctx, cancel := context.WithCancel(ctx)
	r.inflight[child] = cancel
	return ctx, func() {
		cancel()
		r.mu.Lock()
		delete(r.inflight, child)
		r.mu.Unlock()
	}, nil
}

// depthOf is the nesting depth of a Session from its provenance chain
// (SPN-3): zero for a Session no spawn created.
func (r *Responder) depthOf(ctx context.Context, sid session.SessionID) (int, error) {
	prov, ok, err := r.provenance(ctx, sid)
	if err != nil || !ok {
		return 0, err
	}
	return prov.Depth, nil
}

func (r *Responder) provenance(ctx context.Context, sid session.SessionID) (Provenance, bool, error) {
	header, err := r.children.Header(ctx, sid)
	if err != nil {
		if session.IsNotFound(err) {
			return Provenance{}, false, nil
		}
		return Provenance{}, false, err
	}
	prov, ok, err := ProvenanceFromHeader(header)
	if err != nil || !ok {
		return Provenance{}, err == nil, err
	}
	return prov, true, nil
}

// create makes the child Session with its provenance in the segment's spawn
// extension slot: empty for Empty, a fork of the parent's history before the
// calling Turn for Fork (SPN-5).
func (r *Responder) create(ctx context.Context, parent session.SessionID, runID run.RunID, callID run.CallID, child session.SessionID, args Arguments, depth int) (Provenance, error) {
	prov := Provenance{ParentSession: parent, ParentRun: runID, CallID: callID, Depth: depth, Arguments: args}
	ext, err := Extension(prov)
	if err != nil {
		return Provenance{}, err
	}
	switch args.Mode {
	case Fork:
		turnID, err := r.callingTurn(ctx, parent, runID)
		if err != nil {
			return Provenance{}, err
		}
		return prov, r.children.ForkBeforeInputs(ctx, parent, turnID, child, ext)
	default:
		return prov, r.children.Create(ctx, child, ext)
	}
}

// callingTurn is the parent Turn that owns the Run the call belongs to.
func (r *Responder) callingTurn(ctx context.Context, parent session.SessionID, runID run.RunID) (turn.TurnID, error) {
	surface, err := r.children.TurnSurface(ctx, parent)
	if err != nil {
		return "", err
	}
	turnID, ok := surface.OwnerOf(runID)
	if !ok {
		return "", fmt.Errorf("spawn: run %s of %s has no owning turn", runID, parent)
	}
	return turnID, nil
}

func (r *Responder) childPreset(ctx context.Context, parent session.SessionID, runID run.RunID, args Arguments) (preset.PresetRef, error) {
	if args.Preset != "" {
		if r.opts.Presets == nil {
			return preset.PresetRef{}, errors.New("named presets are not configured")
		}
		return r.opts.Presets(args.Preset)
	}
	surface, err := r.children.TurnSurface(ctx, parent)
	if err != nil {
		return preset.PresetRef{}, err
	}
	turnID, ok := surface.OwnerOf(runID)
	if !ok {
		return preset.PresetRef{}, fmt.Errorf("spawn: run %s of %s has no owning turn", runID, parent)
	}
	return surface.Turns[turnID].Preset, nil
}

// settle brings the child to a settled Turn for task and returns it. It
// starts from wherever the child's durable state is: an active Turn or an
// input awaiting delivery is resumed; a Turn that already settled for the
// task is the answer; otherwise the task is sent. A child runs exactly one
// Turn per task: the only input ever submitted is the task itself.
func (r *Responder) settle(ctx context.Context, c Child, child session.SessionID, task string) (turn.TurnID, error) {
	for {
		turns, ok, err := c.Resume(ctx)
		if err != nil {
			return "", err
		}
		var turnID turn.TurnID
		if ok && len(turns) > 0 {
			turnID = turns[len(turns)-1]
			if err := c.Settle(ctx, turnID); err != nil {
				return "", err
			}
		} else {
			if turnID, err = r.settledFor(ctx, child, task); err != nil {
				return "", err
			}
			if turnID == "" {
				if turnID, err = c.Send(ctx, task); err != nil {
					return "", err
				}
			}
		}
		// A Turn left active waiting for recovery (its executions belong to
		// a dead owner and await the control plane) is not settled: wait
		// for it to move and resume again.
		active, err := r.awaitRecovery(ctx, child, turnID)
		if err != nil {
			return "", err
		}
		if !active {
			return turnID, nil
		}
	}
}

// settledFor is the Turn already settled for task, if the child's last Turn
// was asked exactly that; empty otherwise.
func (r *Responder) settledFor(ctx context.Context, child session.SessionID, task string) (turn.TurnID, error) {
	surface, err := r.children.TurnSurface(ctx, child)
	if err != nil || len(surface.Order) == 0 {
		return "", err
	}
	last := surface.Order[len(surface.Order)-1]
	text, err := r.children.InputText(ctx, turn.TurnRef{SessionID: child, TurnID: last})
	if err != nil || text != task {
		return "", err
	}
	return last, nil
}

// awaitRecovery waits while the Turn is active and its Run needs recovery;
// active reports whether the Turn is still active once the wait ends.
func (r *Responder) awaitRecovery(ctx context.Context, child session.SessionID, turnID turn.TurnID) (active bool, err error) {
	delay := 10 * time.Millisecond
	for {
		surface, err := r.children.TurnSurface(ctx, child)
		if err != nil {
			return false, err
		}
		view, ok := surface.Turns[turnID]
		if !ok {
			return false, fmt.Errorf("spawn: subagent %s has no turn %s", child, turnID)
		}
		if view.Status != turn.TurnActive {
			return false, nil
		}
		waiting, err := r.children.AwaitingRecovery(ctx, turn.TurnRef{SessionID: child, TurnID: turnID})
		if err != nil {
			return false, err
		}
		if !waiting {
			return true, nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false, ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, 250*time.Millisecond)
	}
}

var _ execution.Responder = (*Responder)(nil)
