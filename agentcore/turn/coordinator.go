package turn

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/run/store"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
	"time"
)

// Coordinator commits the Turn protocol's commands. Each is one unit of
// work spanning the modules a Turn's transition touches -- the Turn's own
// fact, the chatlog's deliveries and the Run's command -- prepared against
// one View and appended as one commit, so the Turn, its inputs' delivery
// state and its Run never disagree. The Coordinator has no hidden state:
// every method reads the turn surface first. It never encodes another
// module's events by hand and never drives a Run: it commits protocol
// transitions and computes dispositions.
type Coordinator struct {
	// Projections is the lease-free read side for Status.
	Projections session.ProjectionReader
	// Runs is the Run module's Session adapter: it reads Runs for Status and
	// contributes the Run Parts of every Turn unit.
	Runs *sessionstore.SessionRunStore
	// Now stamps event times; nil selects time.Now.
	Now func() time.Time
}

func (c *Coordinator) now() int64 {
	if c.Now != nil {
		return c.Now().UnixMilli()
	}
	return time.Now().UnixMilli()
}

func (c *Coordinator) surface(ctx context.Context, sid session.SessionID) (TurnSurface, error) {
	return ReadSurface(ctx, c.Projections, sid)
}

// owned checks that the request addresses the Session the Writer owns.
func owned(w writer.Writer, ref TurnRef) error {
	if w == nil {
		return errors.New("turn: command requires the session's writer")
	}
	if ref.SessionID != w.SessionID() {
		return fmt.Errorf("%w: request for %s through the writer of %s", ErrConflict, ref.SessionID, w.SessionID())
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
		case errors.Is(err, chatlog.ErrNotSubmitted), errors.Is(err, sessionstore.ErrRunExists):
			return fmt.Errorf("%w: %w", ErrConflict, err)
		}
		return err
	}
	switch res.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied, writer.CommitNoop:
		return nil
	case writer.CommitConflict:
		return fmt.Errorf("%w: %s replayed with different content", ErrConflict, op)
	default:
		return fmt.Errorf("turn: %s: %s: %s", op, res.Outcome, res.Detail)
	}
}

// turnBatch is one batch of events in the Turn's stream.
func turnBatch(turnID TurnID, now int64, events ...writer.TypedEvent) []writer.TypedBatch {
	for i := range events {
		events[i].RecordedAtUnixMilli = now
	}
	return []writer.TypedBatch{{Domain: Stream(turnID), Events: events}}
}

// Start opens a new Turn: the Turn's started fact, the chatlog's deliveries
// and the Run's creation are one unit; the chatlog Part enforces, on the
// same View, that every input is still submitted.
func (c *Coordinator) Start(ctx context.Context, w writer.Writer, req StartRequest) (TurnResult, error) {
	if req.Ref.SessionID == "" || req.Ref.TurnID == "" || req.Preset.ID == "" || req.Preset.Digest == "" {
		return TurnResult{}, errors.New("turn: start requires ref and preset")
	}
	if err := owned(w, req.Ref); err != nil {
		return TurnResult{}, err
	}
	inputIDs := make([]chatlog.InputID, len(req.Inputs))
	seen := map[run.InputID]struct{}{}
	for i, in := range req.Inputs {
		if _, dup := seen[in.ID]; dup || in.ID == "" {
			return TurnResult{}, errors.New("turn: start inputs must have unique non-empty IDs")
		}
		seen[in.ID] = struct{}{}
		inputIDs[i] = chatlog.InputID(in.ID)
	}
	sid, turnID := req.Ref.SessionID, req.Ref.TurnID
	p := PlanDigest(turnID, req.Preset.Digest, inputIDs)
	commitID := ledger.CommitID(StartOperationDigest(sid, turnID, p))
	runID := DeriveRunID(sid, turnID)
	newRun, err := run.BuildNewRun(runID, ledger.CausationID(commitID))
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
				return nil, fmt.Errorf("%w: turn %s already started", ErrConflict, turnID)
			}
			if _, active := surface.Active(); active {
				return nil, fmt.Errorf("%w: session already has an active turn", ErrConflict)
			}
			return turnBatch(turnID, now,
				writer.TypedEvent{Type: TypeStarted, Value: StartedPayload{TurnID: turnID, RunID: runID, InputIDs: inputIDs, Preset: req.Preset}}), nil
		}),
		chatlog.DeliverInputs(chatlog.TurnID(turnID), runID, req.Inputs),
		sessionstore.CreateRun(newRun, req.Inputs),
	}}
	if err := c.commit(ctx, w, "start", work); err != nil {
		return TurnResult{}, err
	}
	return c.respond(ctx, req.Ref)
}

// Deliver carries inputs into an active Turn: the Run's AcceptInput and the
// chatlog's deliveries are one unit, so the Run accepts every input and the
// chatlog delivers every input, or nothing is written. The command derives
// from the ordered InputIDs; a replay of the same batch is AlreadyApplied.
func (c *Coordinator) Deliver(ctx context.Context, w writer.Writer, req DeliverRequest) (TurnResult, error) {
	if err := owned(w, req.Ref); err != nil {
		return TurnResult{}, err
	}
	sid := req.Ref.SessionID
	surface, err := ReadSurface(ctx, w.Projections(), sid)
	if err != nil {
		return TurnResult{}, err
	}
	view, ok := surface.Turns[req.Ref.TurnID]
	if !ok || view.Status != TurnActive {
		return TurnResult{}, fmt.Errorf("%w: turn %s is not active", ErrConflict, req.Ref.TurnID)
	}
	// AcceptInput is not a hard-CAS command: no Base is needed; the machine
	// projection is read only for the Run's protocol version.
	runID := view.RunID
	if len(req.Inputs) == 0 {
		return TurnResult{}, fmt.Errorf("%w: deliver without inputs", ErrConflict)
	}
	cmd := run.AcceptInput{Inputs: req.Inputs}
	env, err := schema.Wire().Envelope(runID, schema.Identity().DeriveInputCommandID(runID, cmd.InputIDs()...), cmd)
	if err != nil {
		return TurnResult{}, err
	}
	accept, err := c.Runs.Command(ctx, store.CommitRequest{Command: env})
	if err != nil {
		return TurnResult{}, err
	}
	work := unit.Work{CommitID: ledger.CommitID(env.ID), Parts: []unit.Part{accept, chatlog.DeliverInputs(chatlog.TurnID(req.Ref.TurnID), runID, req.Inputs)}}
	if err := c.commit(ctx, w, "deliver", work); err != nil {
		if errors.Is(err, run.ErrRunTerminal) {
			// The last step settled first: the inputs stay submitted.
			return c.respond(ctx, req.Ref)
		}
		return TurnResult{}, err
	}
	return c.respond(ctx, req.Ref)
}

// Stop settles the active Turn as stopped: CancelRun rebases on the current
// state, and the Run's cancellation and the Turn's failed settlement are one
// unit.
func (c *Coordinator) Stop(ctx context.Context, w writer.Writer, req StopRequest) (TurnResult, error) {
	if err := owned(w, req.Ref); err != nil {
		return TurnResult{}, err
	}
	sid, turnID := req.Ref.SessionID, req.Ref.TurnID
	surface, err := ReadSurface(ctx, w.Projections(), sid)
	if err != nil {
		return TurnResult{}, err
	}
	view, ok := surface.Turns[turnID]
	if !ok || view.Status != TurnActive {
		return TurnResult{}, fmt.Errorf("%w: turn %s is not active", ErrConflict, turnID)
	}
	runID := view.RunID
	env, err := schema.Wire().Envelope(runID, CancelCommandID(sid, turnID, runID), run.CancelRun{})
	if err != nil {
		return TurnResult{}, err
	}
	cancel, err := c.Runs.Command(ctx, store.CommitRequest{Command: env})
	if err != nil {
		return TurnResult{}, err
	}
	work := unit.Work{CommitID: ledger.CommitID(env.ID), Parts: []unit.Part{cancel,
		unit.PartFunc(func(_ context.Context, _ writer.View, now int64) ([]writer.TypedBatch, error) {
			return turnBatch(turnID, now, writer.TypedEvent{Type: TypeFailed,
				Value: FailedPayload{TurnID: turnID, RunID: runID, Settlement: SettlementStopped, FailureClass: "cancelled", Reason: req.Reason}}), nil
		}),
	}}
	if err := c.commit(ctx, w, "stop", work); err != nil && !errors.Is(err, run.ErrRunTerminal) {
		return TurnResult{}, err
	}
	return c.respond(ctx, req.Ref)
}

func (c *Coordinator) Status(ctx context.Context, ref TurnRef) (TurnResult, error) {
	return c.respond(ctx, ref)
}

// respond reads the projections and fills the disposition.
func (c *Coordinator) respond(ctx context.Context, ref TurnRef) (TurnResult, error) {
	surface, err := c.surface(ctx, ref.SessionID)
	if err != nil {
		return TurnResult{}, err
	}
	view, ok := surface.Turns[ref.TurnID]
	if !ok {
		return TurnResult{}, fmt.Errorf("%w: unknown turn %s", ErrConflict, ref.TurnID)
	}
	return c.responseFor(ctx, ref, &view)
}

func (c *Coordinator) responseFor(ctx context.Context, ref TurnRef, view *TurnView) (TurnResult, error) {
	resp := TurnResult{Ref: ref, Status: view.Status, RunID: view.RunID, End: view.Ended()}
	if view.End != nil {
		resp.Disposition = ResumeFinished
		return resp, nil
	}
	record, err := c.Runs.Record(ctx, ref.SessionID, view.RunID)
	if err != nil {
		return TurnResult{}, err
	}
	snapshot := record.Snapshot
	switch {
	case snapshot.State.Status.Terminal():
		resp.Disposition = ResumeFinished
	case run.NeedsRecovery(snapshot.State):
		resp.Disposition = ResumeExecuting
	default:
		resp.Waiting = run.WaitingCalls(snapshot.State)
		if len(resp.Waiting) > 0 {
			resp.Disposition = ResumeWaitingForResponse
		}
	}
	return resp, nil
}
