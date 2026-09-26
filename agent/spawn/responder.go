package spawn

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agent/input"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/driver"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
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
	Presets func(name string) (turn.PresetRef, error)
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
func (o Options) ExecutableTool() loop.ExecutableTool { return Tool(o.ToolRef()) }

func (o Options) depth() int {
	if o.MaxDepth <= 0 {
		return DefaultDepth
	}
	return o.MaxDepth
}

// Responder is the subagent tool's driver.Responder (SPN-1, DRV-4): a spawn
// call waits for an external response, and this answers it by creating the
// child Session (or continuing the one on record), driving it through the
// authority to a settled Turn and returning the child's reply. Every step
// is idempotent against the child's durable state, so a process that died
// anywhere in the sequence is continued, not repeated, when the next owner
// opens the parent and the Driver asks again (SPN-4). Nothing about the call
// lives in an execution record: the child Session is the durable state.
type Responder struct {
	opts Options
	// a is the Owner children are created in and driven through: Open
	// yields the child's ownership Handle and its Turns, Driver and Chatlog
	// commands run through that Handle's Writer.
	a *owner.Owner

	mu       sync.Mutex
	inflight map[session.SessionID]context.CancelFunc
	closed   bool
}

// NewResponder returns the subagent Responder; Bind supplies the authority
// it drives children through once that authority exists.
func NewResponder(opts Options) *Responder {
	return &Responder{opts: opts, inflight: make(map[session.SessionID]context.CancelFunc)}
}

// Bind supplies the authority. It must precede the first Respond.
func (r *Responder) Bind(a *owner.Owner) { r.a = a }

// Close cancels every child drive; their Turns stay active and resume on
// the next open of the parent, when the Driver asks for the answer again.
func (r *Responder) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for _, cancel := range r.inflight {
		cancel()
	}
}

// Respond is driver.Responder. Argument, preset and depth errors are
// returned before any child exists, so the call is rejected and no Session
// is created (SPN-3); a child on record for different arguments is a
// conflict.
func (r *Responder) Respond(ctx context.Context, w writer.Writer, call *driver.WaitingCall) (run.CanonicalJSON, error) {
	if r.a == nil {
		return run.CanonicalJSON{}, errors.New("spawn: responder is not bound to an authority")
	}
	parent := w.SessionID()
	args, err := DecodeArguments(call.Arguments)
	if err != nil {
		return run.CanonicalJSON{}, err
	}
	depth, err := r.depthOf(ctx, parent)
	if err != nil {
		return run.CanonicalJSON{}, err
	}
	if DepthExceeded(depth, r.opts.depth()) {
		return run.CanonicalJSON{}, fmt.Errorf("spawn: session %s is at depth %d, the limit", parent, depth)
	}
	preset, err := r.childPreset(ctx, parent, call.Request.RunID, args)
	if err != nil {
		return run.CanonicalJSON{}, err
	}
	child := ChildID(parent, call.Request.RunID, call.Request.CallID)
	ctx, done, err := r.track(ctx, child)
	if err != nil {
		return run.CanonicalJSON{}, err
	}
	defer done()
	prov, exists, err := r.provenance(ctx, child)
	if err != nil {
		return run.CanonicalJSON{}, err
	}
	if !exists {
		if prov, err = r.create(ctx, parent, call.Request.RunID, call.Request.CallID, child, args, depth+1); err != nil {
			return run.CanonicalJSON{}, fmt.Errorf("create subagent: %w", err)
		}
	} else if ArgumentsConflict(prov, args) {
		return run.CanonicalJSON{}, fmt.Errorf("call %s already spawned %s with different arguments", call.Request.CallID, child)
	}
	h, err := r.a.Open(ctx, child)
	if err != nil {
		return run.CanonicalJSON{}, fmt.Errorf("open subagent: %w", err)
	}
	defer func() { _ = h.Close(context.WithoutCancel(ctx)) }()
	turnID, err := r.settle(ctx, h, preset, prov.Arguments.Task)
	if err != nil {
		return run.CanonicalJSON{}, fmt.Errorf("drive subagent: %w", err)
	}
	ref := turn.TurnRef{SessionID: child, TurnID: turnID}
	status, err := r.a.Turns.Status(ctx, ref)
	if err != nil {
		return run.CanonicalJSON{}, err
	}
	if status.Status != turn.TurnCompleted {
		return run.CanonicalJSON{}, fmt.Errorf("subagent %s turn %s ended %s", child, turnID, status.Status)
	}
	reply, err := chatlog.LastAssistantText(ctx, r.a.Projections, r.a.Content, ref.SessionID, chatlog.TurnID(ref.TurnID))
	if err != nil {
		return run.CanonicalJSON{}, err
	}
	return run.CanonicalJSONFromValue(Result{ChildSession: child, TurnID: turnID, Status: status.Status, Reply: reply})
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
	header, err := r.a.Store.Header(ctx, sid)
	if err != nil {
		if session.IsCode(err, session.ErrNotFound) {
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
// extension slot:
// empty for Empty, a fork of the parent's history before the calling Turn
// for Fork (SPN-5).
func (r *Responder) create(ctx context.Context, parent session.SessionID, runID run.RunID, callID run.CallID, child session.SessionID, args Arguments, depth int) (Provenance, error) {
	prov := Provenance{ParentSession: parent, ParentRun: runID, CallID: callID, Depth: depth, Arguments: args}
	ext, err := Extension(prov)
	if err != nil {
		return Provenance{}, err
	}
	now := r.a.Clock().UnixMilli()
	switch args.Mode {
	case Fork:
		turnID, err := r.callingTurn(ctx, parent, runID)
		if err != nil {
			return Provenance{}, err
		}
		at, err := r.a.History.PrefixCommit(ctx, parent, turnID)
		if err != nil {
			return Provenance{}, err
		}
		_, err = writer.Fork(ctx, r.a.Store, r.a.Registry, writer.ForkRequest{Parent: parent, At: at, Child: child, CreatedAtUnixMilli: now, Ext: ext})
		return prov, err
	default:
		_, err := r.a.Store.Create(ctx, session.CreateRequest{SessionID: child, CreatedAtUnixMilli: now, Ext: ext})
		return prov, err
	}
}

// callingTurn is the parent Turn that owns the Run the call belongs to.
func (r *Responder) callingTurn(ctx context.Context, parent session.SessionID, runID run.RunID) (turn.TurnID, error) {
	surface, err := turn.ReadSurface(ctx, r.a.Projections, parent)
	if err != nil {
		return "", err
	}
	turnID, ok := surface.OwnerOf(runID)
	if !ok {
		return "", fmt.Errorf("spawn: run %s of %s has no owning turn", runID, parent)
	}
	return turnID, nil
}

func (r *Responder) childPreset(ctx context.Context, parent session.SessionID, runID run.RunID, args Arguments) (turn.PresetRef, error) {
	if args.Preset != "" {
		if r.opts.Presets == nil {
			return turn.PresetRef{}, errors.New("named presets are not configured")
		}
		return r.opts.Presets(args.Preset)
	}
	turnID, err := r.callingTurn(ctx, parent, runID)
	if err != nil {
		return turn.PresetRef{}, err
	}
	surface, err := turn.ReadSurface(ctx, r.a.Projections, parent)
	if err != nil {
		return turn.PresetRef{}, err
	}
	return surface.Turns[turnID].Preset, nil
}

// settle brings the child to a settled Turn for task and returns it. It
// starts from wherever the child's durable state is: nothing submitted yet,
// an input awaiting delivery, an active Turn, or a Turn that settled before
// the parent learned of it. A child runs exactly one Turn per task: the
// submitted backlog is the task itself, so there is no draining loop.
func (r *Responder) settle(ctx context.Context, h *owner.Handle, preset turn.PresetRef, task string) (turn.TurnID, error) {
	turns, err := turn.ReadSurface(ctx, r.a.Projections, h.ID())
	if err != nil {
		return "", err
	}
	if active, ok := turns.Active(); ok {
		return r.driveTurn(ctx, h, active.TurnID)
	}
	chat, err := chatlog.ReadSurface(ctx, r.a.Projections, h.ID())
	if err != nil {
		return "", err
	}
	if pending := chat.SubmittedInputs(); len(pending) > 0 {
		inputs := make([]run.AgentInput, len(pending))
		for i, in := range pending {
			inputs[i] = run.AgentInput{ID: run.InputID(in.ID), Digest: in.Digest}
		}
		return r.startAndDrive(ctx, h, preset, inputs)
	}
	if last, found := newestInput(&chat); found {
		var body struct {
			Text string `json:"text"`
		}
		if last.Input.Content.Decode(&body) == nil && body.Text == task && len(turns.Order) > 0 {
			return turns.Order[len(turns.Order)-1], nil
		}
	}
	in, err := r.a.Chatlog.Submit(ctx, h.Writer(), chatlog.NewInputID(), input.Text(task))
	if err != nil {
		return "", err
	}
	return r.startAndDrive(ctx, h, preset, []run.AgentInput{in})
}

func (r *Responder) startAndDrive(ctx context.Context, h *owner.Handle, preset turn.PresetRef, inputs []run.AgentInput) (turn.TurnID, error) {
	ref := turn.TurnRef{SessionID: h.ID(), TurnID: turn.NewTurnID()}
	if _, err := r.a.Turns.Start(ctx, h.Writer(), turn.StartRequest{Ref: ref, Inputs: inputs, Preset: preset}); err != nil {
		return "", err
	}
	return r.driveTurn(ctx, h, ref.TurnID)
}

// driveTurn drives the Turn to settlement. A drive that quiesces waiting
// for recovery (the child's own execution records belong to a dead owner
// and await the control plane, RUN-EXE-6) is not the end of the Turn: this
// waits for the Turn to move and drives again.
func (r *Responder) driveTurn(ctx context.Context, h *owner.Handle, turnID turn.TurnID) (turn.TurnID, error) {
	ref := turn.TurnRef{SessionID: h.ID(), TurnID: turnID}
	for {
		resp, err := r.a.Driver.Drive(ctx, h.Writer(), turnID)
		if err != nil {
			return "", err
		}
		if resp.AlreadyDriving {
			return "", fmt.Errorf("subagent %s is driven elsewhere", h.ID())
		}
		switch resp.Disposition {
		case turn.ResumeWaitingForRecovery:
			if err := r.awaitRecovery(ctx, ref); err != nil {
				return "", err
			}
		default:
			return turnID, nil
		}
	}
}

// awaitRecovery waits until the Turn leaves waiting_for_recovery.
func (r *Responder) awaitRecovery(ctx context.Context, ref turn.TurnRef) error {
	delay := 10 * time.Millisecond
	for {
		resp, err := r.a.Turns.Status(ctx, ref)
		if err != nil {
			return err
		}
		if resp.Status != turn.TurnActive || resp.Disposition != turn.ResumeWaitingForRecovery {
			return nil
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		delay = min(delay*2, 250*time.Millisecond)
	}
}

// newestInput is the most recently submitted input of the chatlog surface.
func newestInput(chat *chatlog.Surface) (chatlog.InputView, bool) {
	var best chatlog.InputView
	var found bool
	chat.Inputs.Range(func(_ chatlog.InputID, v chatlog.InputView) bool {
		if !found || best.Position.Less(v.Position) {
			best, found = v, true
		}
		return true
	})
	return best, found
}

var _ driver.Responder = (*Responder)(nil)
