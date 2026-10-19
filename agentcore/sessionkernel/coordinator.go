package sessionkernel

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
	"github.com/felinics/twilight/agentcore/turn/store"
	"time"
)

// Coordinator commits Turn lifecycle commands. Input admission and Turn
// transitions prepare against the same Writer View and commit atomically.
// Chatlog derives delivery state from the Turn's InputAccepted facts. The
// Coordinator has no hidden state and returns after committing; the Loop
// drives execution.
type Coordinator struct {
	// Projections is the lease-free read side for Status.
	Projections session.ProjectionReader
	// TurnStore reads Turn state and contributes its command Parts.
	TurnStore *sessionstore.SessionTurnStore
	// Now stamps event times; nil selects time.Now.
	Now func() time.Time
}

func (c *Coordinator) now() int64 {
	if c.Now != nil {
		return c.Now().UnixMilli()
	}
	return time.Now().UnixMilli()
}

func (c *Coordinator) surface(ctx context.Context, sid session.SessionID) (sessionstore.TurnSurface, error) {
	return sessionstore.ReadSurface(ctx, c.Projections, sid)
}

// owned checks that the request addresses the Session the Writer owns.
func owned(w writer.Writer, ref sessionstore.TurnRef) error {
	if w == nil {
		return errors.New("sessionkernel: command requires the session's writer")
	}
	if ref.SessionID != w.SessionID() {
		return fmt.Errorf("%w: request for %s through the writer of %s", turn.ErrConflict, ref.SessionID, w.SessionID())
	}
	return nil
}

// commit appends one unit of work and maps the outcome. The CommitID names
// the operation, so a replay is already applied. A chatlog refusal (an
// input no longer submitted) is the Turn's conflict.
func (c *Coordinator) commit(ctx context.Context, w writer.Writer, op string, work unit.Work) error {
	res, err := unit.Commit(ctx, w, c.now(), work)
	if err != nil {
		switch {
		case errors.Is(err, &ledger.Error{Code: ledger.CodeOwnershipLost}):
			return fmt.Errorf("%w: %w", store.ErrOwnershipLost, err)
		case errors.Is(err, chatlog.ErrNotSubmitted), errors.Is(err, sessionstore.ErrTurnExists), errors.Is(err, turn.ErrCommandConflict):
			return fmt.Errorf("%w: %w", turn.ErrConflict, err)
		}
		return err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied, writer.CommitNoop:
		return nil
	case writer.CommitConflict:
		return fmt.Errorf("%w: %s replayed with different content", turn.ErrConflict, op)
	default:
		return fmt.Errorf("sessionkernel: %s: %s: %s", op, res.Outcome, res.Detail)
	}
}

// Start records TurnStarted and initial InputAccepted facts in one commit.
// Admission checks that every input is still submitted with matching content.
func (c *Coordinator) Start(ctx context.Context, w writer.Writer, req StartRequest) (TurnResult, error) {
	if req.Ref.SessionID == "" || req.Ref.TurnID == "" || req.Preset.ID == "" || req.Preset.Digest == "" {
		return TurnResult{}, errors.New("sessionkernel: start requires ref and preset")
	}
	if err := owned(w, req.Ref); err != nil {
		return TurnResult{}, err
	}
	seen := map[turn.InputID]struct{}{}
	for _, in := range req.Inputs {
		if _, dup := seen[in.ID]; dup || in.ID == "" {
			return TurnResult{}, errors.New("sessionkernel: start inputs must have unique non-empty IDs")
		}
		seen[in.ID] = struct{}{}
	}
	sid, turnID := req.Ref.SessionID, req.Ref.TurnID
	p := sessionstore.PlanDigest(turnID, req.Preset, req.Inputs)
	commitID := ledger.CommitID(sessionstore.StartOperationDigest(sid, turnID, p))
	newTurn, err := turn.BuildNewTurn(turnID, ledger.CausationID(commitID), req.Preset)
	if err != nil {
		return TurnResult{}, err
	}
	work := unit.Work{CommitID: commitID, Parts: []unit.Part{
		unit.PartFunc(func(_ context.Context, view writer.View, now int64) ([]writer.TypedBatch, error) {
			surface, err := surfaceOf(view)
			if err != nil {
				return nil, err
			}
			if _, exists := surface.Turns[turnID]; exists {
				return nil, fmt.Errorf("%w: turn %s already started", turn.ErrConflict, turnID)
			}
			if _, active := surface.Active(); active {
				return nil, fmt.Errorf("%w: session already has an active turn", turn.ErrConflict)
			}
			return nil, nil
		}),
		chatlog.AdmitInputs(req.Inputs),
		sessionstore.CreateTurn(newTurn, req.Inputs),
	}}
	if err := c.commit(ctx, w, "start", work); err != nil {
		return TurnResult{}, err
	}
	return c.respond(ctx, req.Ref)
}

// Deliver atomically validates submitted content and records InputAccepted.
// The command identity derives from ordered InputIDs; replay also checks the
// accepted content digests before reusing a stored commit.
func (c *Coordinator) Deliver(ctx context.Context, w writer.Writer, req DeliverRequest) (TurnResult, error) {
	if err := owned(w, req.Ref); err != nil {
		return TurnResult{}, err
	}
	sid := req.Ref.SessionID
	surface, err := sessionstore.ReadSurface(ctx, w.Projections(), sid)
	if err != nil {
		return TurnResult{}, err
	}
	view, ok := surface.Turns[req.Ref.TurnID]
	if !ok || view.Status != turn.TurnActive {
		return TurnResult{}, fmt.Errorf("%w: turn %s is not active", turn.ErrConflict, req.Ref.TurnID)
	}
	// AcceptInput is not a hard-CAS command: no Base is needed; the machine
	// projection is read only for the Turn's protocol version.
	turnID := view.TurnID
	if len(req.Inputs) == 0 {
		return TurnResult{}, fmt.Errorf("%w: deliver without inputs", turn.ErrConflict)
	}
	cmd := turn.AcceptInput{Inputs: req.Inputs}
	env, err := schema.Wire().Envelope(turnID, schema.Identity().DeriveInputCommandID(turnID, cmd.InputIDs()...), cmd)
	if err != nil {
		return TurnResult{}, err
	}
	accept, err := c.TurnStore.Command(ctx, store.CommitRequest{Command: env})
	if err != nil {
		return TurnResult{}, err
	}
	work := unit.Work{CommitID: ledger.CommitID(env.ID), ReplayCheck: accept.CheckInputReplay, Parts: []unit.Part{accept, chatlog.AdmitInputs(req.Inputs)}}
	if err := c.commit(ctx, w, "deliver", work); err != nil {
		if errors.Is(err, turn.ErrTurnTerminal) {
			// The last step settled first: the inputs stay submitted.
			return c.respond(ctx, req.Ref)
		}
		return TurnResult{}, err
	}
	return c.respond(ctx, req.Ref)
}

// Respond commits a response command for a waiting external or approval call.
// The command identity is derived from the response request, so answering a
// request is idempotent and a retry cannot accidentally answer another call.
// It is kept off Commands because response answering is a host-side drive
// operation rather than a Session lifecycle primitive.

func (c *Coordinator) Respond(ctx context.Context, w writer.Writer, req turn.ResponseRequest, cmd turn.AgentCommand) (TurnResult, error) {
	if w == nil {
		return TurnResult{}, errors.New("sessionkernel: response requires the session's writer")
	}
	if err := owned(w, sessionstore.TurnRef{SessionID: w.SessionID(), TurnID: req.TurnID}); err != nil {
		return TurnResult{}, err
	}
	var id turn.CommandID
	switch v := cmd.(type) {
	case turn.SubmitToolResponse:
		if v.StepID != req.StepID || v.CallID != req.CallID || v.ResponseID != req.ID {
			return TurnResult{}, fmt.Errorf("sessionkernel: response identity does not match request")
		}
		id = schema.Identity().DeriveResponseCommandID(req.TurnID, req.StepID, req.CallID, req.ID)
	case turn.ApproveToolCall:
		if v.StepID != req.StepID || v.CallID != req.CallID || v.ResponseID != req.ID {
			return TurnResult{}, fmt.Errorf("sessionkernel: response identity does not match request")
		}
		id = schema.Identity().DeriveResponseCommandID(req.TurnID, req.StepID, req.CallID, req.ID)
	case turn.RejectToolCall:
		if v.StepID != req.StepID || v.CallID != req.CallID || v.ResponseID != req.ID {
			return TurnResult{}, fmt.Errorf("sessionkernel: response identity does not match request")
		}
		id = schema.Identity().DeriveResponseCommandID(req.TurnID, req.StepID, req.CallID, req.ID)
	default:
		return TurnResult{}, fmt.Errorf("sessionkernel: unsupported response command %T", cmd)
	}
	env, err := schema.Wire().Envelope(req.TurnID, id, cmd)
	if err != nil {
		return TurnResult{}, err
	}
	part, err := c.TurnStore.Command(ctx, store.CommitRequest{Command: env})
	if err != nil {
		return TurnResult{}, err
	}
	if err := c.commit(ctx, w, "respond", unit.Work{CommitID: ledger.CommitID(env.ID), ReplayCheck: part.CheckResponseReplay, Parts: []unit.Part{part}}); err != nil && !errors.Is(err, turn.ErrTurnTerminal) {
		return TurnResult{}, err
	}
	return c.respond(ctx, sessionstore.TurnRef{SessionID: w.SessionID(), TurnID: req.TurnID})
}

// Stop commits cancellation dispositions and TurnEnded as one transition.
// CancelTurn rebases on the latest state inside the Writer transaction.
func (c *Coordinator) Stop(ctx context.Context, w writer.Writer, req StopRequest) (TurnResult, error) {
	if err := owned(w, req.Ref); err != nil {
		return TurnResult{}, err
	}
	sid, turnID := req.Ref.SessionID, req.Ref.TurnID
	surface, err := sessionstore.ReadSurface(ctx, w.Projections(), sid)
	if err != nil {
		return TurnResult{}, err
	}
	view, ok := surface.Turns[turnID]
	if !ok || view.Status != turn.TurnActive {
		return TurnResult{}, fmt.Errorf("%w: turn %s is not active", turn.ErrConflict, turnID)
	}
	env, err := schema.Wire().Envelope(turnID, sessionstore.CancelCommandID(sid, turnID), turn.CancelTurn{Message: req.Reason})
	if err != nil {
		return TurnResult{}, err
	}
	cancel, err := c.TurnStore.Command(ctx, store.CommitRequest{Command: env})
	if err != nil {
		return TurnResult{}, err
	}
	work := unit.Work{CommitID: ledger.CommitID(env.ID), Parts: []unit.Part{cancel}}
	if err := c.commit(ctx, w, "stop", work); err != nil && !errors.Is(err, turn.ErrTurnTerminal) {
		return TurnResult{}, err
	}
	return c.respond(ctx, req.Ref)
}

func (c *Coordinator) Status(ctx context.Context, ref sessionstore.TurnRef) (TurnResult, error) {
	return c.respond(ctx, ref)
}

// respond reads the projections and fills the disposition.
func (c *Coordinator) respond(ctx context.Context, ref sessionstore.TurnRef) (TurnResult, error) {
	surface, err := c.surface(ctx, ref.SessionID)
	if err != nil {
		return TurnResult{}, err
	}
	view, ok := surface.Turns[ref.TurnID]
	if !ok {
		return TurnResult{}, fmt.Errorf("%w: unknown turn %s", turn.ErrConflict, ref.TurnID)
	}
	return c.responseFor(ctx, ref, &view)
}

func (c *Coordinator) responseFor(ctx context.Context, ref sessionstore.TurnRef, view *sessionstore.TurnView) (TurnResult, error) {
	resp := TurnResult{Ref: ref, Status: view.Status, End: view.Ended()}
	if view.End != nil {
		resp.Disposition = ResumeFinished
		return resp, nil
	}
	record, err := c.TurnStore.Record(ctx, ref.SessionID, view.TurnID)
	if err != nil {
		return TurnResult{}, err
	}
	snapshot := record.Snapshot
	switch {
	case snapshot.State.Status.Terminal():
		resp.Disposition = ResumeFinished
	case turn.NeedsRecovery(snapshot.State):
		resp.Disposition = ResumeWaitingForRecovery
	default:
		resp.Waiting = turn.WaitingCalls(snapshot.State)
		if len(resp.Waiting) > 0 {
			resp.Disposition = ResumeWaitingForResponse
		}
	}
	return resp, nil
}

var (
	_ Commands = (*Coordinator)(nil)
	_ Reader   = (*Coordinator)(nil)
)
