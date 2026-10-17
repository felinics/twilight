package driver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/compaction"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/frozen"
	"github.com/felinics/twilight/agentcore/turn/loop"
	"github.com/felinics/twilight/agentcore/turn/model"
	"github.com/felinics/twilight/agentcore/turn/schema"
)

// DefaultCompactionAttempts bounds maintenance for one preparation even if
// concurrent context changes repeatedly invalidate generated summaries.
const DefaultCompactionAttempts = 4

var (
	ErrCompactionNoProgress = errors.New("driver: compaction cannot bring this candidate within policy")
	ErrSummaryFailed        = errors.New("driver: summary model failed")
)

// ContextDecision is policy, not structure: the core selects a tool-pair-closed
// suffix for RetainEntries. A negative retention is invalid.
type ContextDecision struct {
	Compact       bool
	RetainEntries int
}

// CompactionContext is a snapshot. Policy may render its materialized entries
// into a summary request without having to discover legal tool-call boundaries.
type CompactionContext struct {
	SessionID session.SessionID
	Plan      chatlog.CompactionPlan
	Entries   []chatlog.Materialized
}

type SummaryRequest struct {
	Model   turn.ModelRef
	Request model.ModelRequest
	Target  *turn.TargetRef
}

// ContextPolicy inspects the complete candidate request. It supplies token
// estimation, model limits, thresholds and summary content/model, not an effect
// implementation: BuildSummary only builds a request; Compactions executes it.
type ContextPolicy interface {
	Decide(context.Context, prompt.Input, *prompt.Prompt) (ContextDecision, error)
	BuildSummary(context.Context, *CompactionContext) (SummaryRequest, error)
}

type summaryRequests interface {
	FrozenRequest(context.Context, turn.Digest) (model.ModelRequest, error)
}

// MaintenanceResult describes one finite maintenance coordination. Pending
// means the host may release the Session and arrange a later activation.
type MaintenanceResult struct {
	Pending  bool
	Progress bool
	Key      effect.AssignmentKey
}

// Compactions coordinates durable Session maintenance without waiting for a
// model. Local locks cover one call only; intent, executor identity and Writer
// fences provide the cross-process guarantees.
type Compactions struct {
	Executor    effect.ExecutionPort
	Requests    summaryRequests
	Frozen      frozen.Store
	Content     chatlog.ContentResolver
	Policy      ContextPolicy
	MaxAttempts int
	Now         func() time.Time

	mu    sync.Mutex
	locks map[session.SessionID]*compactionLock
}

type compactionLock struct {
	token chan struct{}
	refs  int
}

func (c *Compactions) acquire(ctx context.Context, sid session.SessionID) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	if c.locks == nil {
		c.locks = map[session.SessionID]*compactionLock{}
	}
	lock := c.locks[sid]
	if lock == nil {
		lock = &compactionLock{token: make(chan struct{}, 1)}
		lock.token <- struct{}{}
		c.locks[sid] = lock
	}
	lock.refs++
	c.mu.Unlock()
	drop := func() {
		c.mu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(c.locks, sid)
		}
		c.mu.Unlock()
	}
	select {
	case <-ctx.Done():
		drop()
		return nil, ctx.Err()
	case <-lock.token:
		return func() { lock.token <- struct{}{}; drop() }, nil
	}
}

func (c *Compactions) now() int64 {
	if c.Now != nil {
		return c.Now().UnixMilli()
	}
	return time.Now().UnixMilli()
}

func preparation(input *prompt.Input) (turn.Digest, error) {
	body, err := jsonstable.EncodeTypedPayload(1, "twilight/compaction/preparation", input)
	if err != nil {
		return "", err
	}
	return jsonstable.DigestBytes(body), nil
}

// CheckCandidate records work but never calls a model. The Loop returns to the
// Session driver before any summary wait, then builds a fresh candidate after
// maintenance. A successful automatic compaction is allowed once per request
// boundary, durably, so an oversized retained suffix cannot cause an endless
// summarize/rebuild loop (including after an owner restart).
func (c *Compactions) CheckCandidate(ctx context.Context, w writer.Writer, input prompt.Input, candidate *prompt.Prompt) (loop.CandidateDecision, error) {
	release, err := c.acquire(ctx, w.SessionID())
	if err != nil {
		return loop.CandidateReady, err
	}
	defer release()
	state, err := compaction.Read(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return loop.CandidateReady, err
	}
	if state.Pending != "" {
		return loop.CandidateMaintenance, nil
	}
	if c.Policy == nil {
		return loop.CandidateReady, nil
	}
	decision, err := c.Policy.Decide(ctx, input, candidate)
	if err != nil || !decision.Compact {
		return loop.CandidateReady, err
	}
	boundary, err := preparation(&input)
	if err != nil {
		return loop.CandidateReady, err
	}
	attempts := 0
	for id := range state.Operations {
		op := state.Operations[id]
		if op.Request.Preparation == boundary {
			attempts++
			if op.Status == compaction.Applied {
				return loop.CandidateReady, ErrCompactionNoProgress
			}
		}
	}
	limit := c.MaxAttempts
	if limit <= 0 {
		limit = DefaultCompactionAttempts
	}
	if attempts >= limit {
		return loop.CandidateReady, fmt.Errorf("%w: summary attempt limit %d", ErrCompactionNoProgress, limit)
	}
	contextState, err := chatlog.ReadContext(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return loop.CandidateReady, err
	}
	plan, possible, err := chatlog.PlanCompaction(contextState.Entries, decision.RetainEntries)
	if err != nil {
		return loop.CandidateReady, err
	}
	if !possible {
		return loop.CandidateReady, ErrCompactionNoProgress
	}
	entries, err := chatlog.NewMaterializer(c.Content).Entries(ctx, contextState.Entries)
	if err != nil {
		return loop.CandidateReady, err
	}
	request, err := c.Policy.BuildSummary(ctx, &CompactionContext{SessionID: w.SessionID(), Plan: plan, Entries: entries})
	if err != nil {
		return loop.CandidateReady, err
	}
	id, err := newCompactionID()
	if err != nil {
		return loop.CandidateReady, err
	}
	intent, err := c.freeze(ctx, id, &plan, boundary, &request)
	if err != nil {
		return loop.CandidateReady, err
	}
	if err := c.start(ctx, w, &intent); err != nil {
		if errors.Is(err, chatlog.ErrContextChanged) {
			return loop.CandidateRebuild, nil
		}
		if errors.Is(err, compaction.ErrPending) {
			return loop.CandidateMaintenance, nil
		}
		return loop.CandidateReady, err
	}
	return loop.CandidateMaintenance, nil
}

func newCompactionID() (compaction.ID, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return compaction.ID(hex.EncodeToString(bytes[:])), nil
}

func (c *Compactions) freeze(ctx context.Context, id compaction.ID, plan *chatlog.CompactionPlan, boundary turn.Digest, request *SummaryRequest) (compaction.Request, error) {
	if c.Frozen == nil {
		return compaction.Request{}, errors.New("driver: compaction requires a frozen store")
	}
	if request.Model == "" {
		request.Model = turn.ModelRef(request.Request.Model)
	}
	if request.Model == "" || string(request.Model) != request.Request.Model || len(request.Request.Tools) != 0 {
		return compaction.Request{}, errors.New("driver: summary request must name its model and have no tools")
	}
	digest, err := schema.Canonical().DigestRequest(request.Request)
	if err != nil {
		return compaction.Request{}, err
	}
	body, err := schema.Bodies().EncodeRequest(&request.Request, digest)
	if err != nil {
		return compaction.Request{}, err
	}
	if err := c.Frozen.Put(ctx, digest, body); err != nil {
		return compaction.Request{}, err
	}
	return compaction.Request{ID: id, Plan: *plan, Model: request.Model, RequestDigest: digest, Preparation: boundary, Target: request.Target}, nil
}

// Start explicitly accepts a preplanned operation. Its ID belongs to the
// caller; an identical retry is a no-op, while a changed request conflicts.
// This entry point and automatic checks share the same durable protocol.
func (c *Compactions) Start(ctx context.Context, w writer.Writer, id compaction.ID, plan chatlog.CompactionPlan, request *SummaryRequest) error {
	release, err := c.acquire(ctx, w.SessionID())
	if err != nil {
		return err
	}
	defer release()
	intent, err := c.freeze(ctx, id, &plan, "", request)
	if err != nil {
		return err
	}
	return c.start(ctx, w, &intent)
}

// compactionIdentity scopes protocol actions and conversation artifacts to the
// Session. Compaction's Own stream does not inherit operation obligations, but
// the ledger still sees inherited commit IDs and chatlog's Inherited stream
// carries the parent's summaries. Neither may alias the child's own work.
func compactionIdentity(sid session.SessionID, id compaction.ID, action string) (jsonstable.Digest, error) {
	body, err := jsonstable.EncodeTypedPayload(1, "twilight/compaction/identity", struct {
		SessionID   session.SessionID `json:"sessionId"`
		OperationID compaction.ID     `json:"operationId"`
		Action      string            `json:"action"`
	}{sid, id, action})
	if err != nil {
		return "", err
	}
	return jsonstable.DigestBytes(body), nil
}

func (c *Compactions) start(ctx context.Context, w writer.Writer, request *compaction.Request) error {
	identity, err := compactionIdentity(w.SessionID(), request.ID, "start")
	if err != nil {
		return err
	}
	part := compaction.Start(*request)
	res, err := w.Commit(ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		batches, err := part.Prepare(ctx, v, c.now())
		if err != nil {
			return nil, err
		}
		if len(batches) == 0 {
			return nil, nil
		}
		if err := sessionkernel.RequireQuiescentTurn(v); err != nil {
			return nil, err
		}
		return &writer.SemanticGroup{CommitID: ledger.CommitID(identity), Batches: batches}, nil
	})
	return commitOK("start", &res, err)
}

// NeedsRecovery is the activation/idle-release predicate for hosts. Pending
// operations and unacknowledged settlements remain work even without a Turn.
func (c *Compactions) NeedsRecovery(ctx context.Context, w writer.Writer) (bool, error) {
	state, err := compaction.Read(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return false, err
	}
	return state.NeedsRecovery(), nil
}

// Advance performs one bounded coordination. It never subscribes or waits for
// an outcome. In particular, a dispatch always returns Pending immediately.
func (c *Compactions) Advance(ctx context.Context, w writer.Writer) (MaintenanceResult, error) {
	release, err := c.acquire(ctx, w.SessionID())
	if err != nil {
		return MaintenanceResult{}, err
	}
	defer release()
	state, err := compaction.Read(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return MaintenanceResult{}, err
	}
	result := MaintenanceResult{Pending: state.Pending != ""}
	if result.Pending {
		op := state.Operations[state.Pending]
		result.Key = op.Request.Key(w.SessionID())
	}
	result.Progress, err = c.acknowledgeFinished(ctx, w, &state)
	if err != nil || !result.Pending {
		return result, err
	}
	op := state.Operations[state.Pending]
	attachment, err := c.Executor.Attach(ctx, result.Key)
	if err != nil {
		return result, err
	}
	settle := func(status compaction.Status, code, message string) (MaintenanceResult, error) {
		err := c.finish(ctx, w, &op, status, code, message, "")
		return c.settlementResult(ctx, w, result, err)
	}
	switch attachment.State {
	case effect.AttachmentMissing:
		// Close an obsolete key before disposing intent; Abort racing with
		// acceptance is not proof that dispatch never happened.
		contextState, err := chatlog.ReadContext(ctx, w.Projections(), w.SessionID())
		if err != nil {
			return result, err
		}
		if err := op.Request.Plan.Validate(contextState.Entries); err != nil {
			if !errors.Is(err, chatlog.ErrContextChanged) {
				return result, err
			}
			closed, err := c.Executor.Abort(ctx, result.Key)
			if err != nil {
				return result, err
			}
			if closed.State == effect.AttachmentAborted {
				return settle(compaction.Stale, "context_changed", "")
			}
			return result, nil
		}
		request, err := c.Requests.FrozenRequest(ctx, op.Request.RequestDigest)
		if err != nil {
			return result, err
		}
		assignment := effect.Assignment{Session: turn.Scope(w.SessionID()), OwnerKind: result.Key.OwnerKind, OwnerID: result.Key.OwnerID,
			StepID: "summary", Effect: result.Key.Effect, Target: op.Request.Target,
			Body: effect.ModelAssignment{Model: op.Request.Model, Request: &request, RequestDigest: op.Request.RequestDigest}}
		unavailable, err := c.Executor.Validate(ctx, assignment)
		if err != nil {
			return result, err
		}
		if unavailable != nil {
			closed, err := c.Executor.Abort(ctx, result.Key)
			if err != nil {
				return result, err
			}
			if closed.State == effect.AttachmentAborted {
				return settle(compaction.Failed, unavailable.Class, unavailable.Message)
			}
			return result, nil
		}
		// Even an accepted response lost in transport keeps this exact key.
		err = c.Executor.Dispatch(ctx, assignment)
		result.Progress = result.Progress || err == nil
		return result, err
	case effect.AttachmentOrphaned:
		if recoverer, ok := c.Executor.(effect.Recoverer); ok {
			if err := recoverer.RecoverExecution(ctx, result.Key); err != nil {
				return result, err
			}
			result.Progress = true
		}
		return result, nil
	case effect.AttachmentAborted:
		return settle(compaction.Failed, "aborted", "summary execution was aborted")
	case effect.AttachmentActive, effect.AttachmentTerminal:
		out, err := c.Executor.GetOutcome(ctx, result.Key)
		if errors.Is(err, effect.ErrOutcomeNotReady) {
			return result, nil
		}
		if err != nil {
			return result, err
		}
		if out.Key != result.Key {
			return result, errors.New("driver: summary outcome identity mismatch")
		}
		delivered, err := c.deliver(ctx, w, &state, out)
		delivered.Progress = delivered.Progress || result.Progress
		return delivered, err
	default:
		return result, fmt.Errorf("driver: invalid summary attachment %q", attachment.State)
	}
}

// Deliver adopts an outcome only for this Session's accepted effect. Duplicate
// delivery of a settled key only retries cleanup, never rewrites its result.
func (c *Compactions) Deliver(ctx context.Context, w writer.Writer, out effect.Outcome) (MaintenanceResult, error) {
	release, err := c.acquire(ctx, w.SessionID())
	if err != nil {
		return MaintenanceResult{}, err
	}
	defer release()
	state, err := compaction.Read(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return MaintenanceResult{}, err
	}
	return c.deliver(ctx, w, &state, out)
}

func (c *Compactions) deliver(ctx context.Context, w writer.Writer, state *compaction.State, out effect.Outcome) (MaintenanceResult, error) {
	result := MaintenanceResult{Pending: state.Pending != ""}
	if result.Pending {
		op := state.Operations[state.Pending]
		result.Key = op.Request.Key(w.SessionID())
	}
	for id := range state.Operations {
		op := state.Operations[id]
		if op.Request.Key(w.SessionID()) != out.Key {
			continue
		}
		if op.Status != compaction.Pending {
			err := c.acknowledge(ctx, w, &op)
			result.Progress = !op.Acknowledged && err == nil
			return result, err
		}
		if state.Pending != op.Request.ID {
			break
		}
		var err error
		switch value := out.Result.(type) {
		case effect.ModelSucceeded:
			if strings.TrimSpace(value.Result.Text) == "" || len(value.Result.ToolCalls) != 0 {
				err = c.finish(ctx, w, &op, compaction.Failed, "invalid_summary", "summary must be nonempty text without tool calls", "")
			} else {
				err = c.finish(ctx, w, &op, compaction.Applied, "", "", value.Result.Text)
			}
		case effect.ModelFailed:
			err = c.finish(ctx, w, &op, compaction.Failed, string(value.Code), value.Message, "")
		case effect.Cancelled:
			err = c.finish(ctx, w, &op, compaction.Failed, "cancelled", "summary execution was cancelled", "")
		case effect.Unknown:
			err = c.finish(ctx, w, &op, compaction.Failed, "unknown", value.Message, "")
		default:
			return result, fmt.Errorf("driver: unexpected summary outcome %T", out.Result)
		}
		return c.settlementResult(ctx, w, result, err)
	}
	return result, errors.New("driver: summary outcome identity mismatch")
}

func (c *Compactions) settlementResult(ctx context.Context, w writer.Writer, result MaintenanceResult, err error) (MaintenanceResult, error) {
	state, readErr := compaction.Read(ctx, w.Projections(), w.SessionID())
	if readErr != nil {
		return result, errors.Join(err, readErr)
	}
	result.Pending = state.Pending != ""
	result.Progress = result.Progress || !result.Pending
	return result, err
}

func (c *Compactions) finish(ctx context.Context, w writer.Writer, op *compaction.Operation, status compaction.Status, code, message, text string) error {
	identity, err := compactionIdentity(w.SessionID(), op.Request.ID, "finish")
	if err != nil {
		return err
	}
	result := compaction.FinishedPayload{ID: op.Request.ID, Status: status, Code: code, Message: message}
	parts := make([]unit.Part, 0, 3)
	if status == compaction.Applied {
		compactionID, err := compactionIdentity(w.SessionID(), op.Request.ID, "compaction")
		if err != nil {
			return err
		}
		summaryID, err := compactionIdentity(w.SessionID(), op.Request.ID, "summary")
		if err != nil {
			return err
		}
		result.CompactionID = chatlog.CompactionID(compactionID)
		summary := chatlog.Summary{ID: chatlog.SummaryID(summaryID), Parts: chatlog.Parts{chatlog.TextPart{Text: text}}}
		if summary.Digest, err = chatlog.DigestSummary(&summary); err != nil {
			return err
		}
		parts = append(parts, unit.PartFunc(func(_ context.Context, v writer.View, _ int64) ([]writer.TypedBatch, error) {
			return nil, sessionkernel.RequireQuiescentTurn(v)
		}), chatlog.CompactContext(result.CompactionID, summary, op.Request.Plan))
	}
	parts = append(parts, compaction.Finish(result))
	res, err := unit.Commit(ctx, w, c.now(), unit.Work{CommitID: ledger.CommitID(identity), Parts: parts})
	if errors.Is(err, chatlog.ErrContextChanged) && status == compaction.Applied {
		return c.finish(ctx, w, op, compaction.Stale, "context_changed", "", "")
	}
	if err := commitOK("finish", &res, err); err != nil {
		return err
	}
	state, err := compaction.Read(ctx, w.Projections(), w.SessionID())
	if err != nil {
		return err
	}
	settled, found := state.Operations[op.Request.ID]
	if !found || settled.Status == compaction.Pending {
		return errors.New("driver: compaction finish has no durable disposition")
	}
	ackErr := c.acknowledge(ctx, w, &settled)
	if settled.Status == compaction.Failed {
		return errors.Join(fmt.Errorf("%w: %s: %s", ErrSummaryFailed, settled.Code, settled.Message), ackErr)
	}
	return ackErr
}

func commitOK(operation string, result *writer.CommitResult, err error) error {
	if err != nil {
		return err
	}
	switch result.Outcome {
	case writer.CommitApplied, writer.CommitAlreadyApplied, writer.CommitNoop:
		return nil
	default:
		return fmt.Errorf("driver: compaction %s: %s: %s", operation, result.Outcome, result.Detail)
	}
}

func (c *Compactions) acknowledgeFinished(ctx context.Context, w writer.Writer, state *compaction.State) (bool, error) {
	progress := false
	ids := make([]compaction.ID, 0, len(state.Operations))
	for id := range state.Operations {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		op := state.Operations[id]
		if op.Status != compaction.Pending && !op.Acknowledged {
			if err := c.acknowledge(ctx, w, &op); err != nil {
				return progress, err
			}
			progress = true
		}
	}
	return progress, nil
}

func (c *Compactions) acknowledge(ctx context.Context, w writer.Writer, op *compaction.Operation) error {
	if op.Acknowledged || op.Status == compaction.Pending {
		return nil
	}
	identity, err := compactionIdentity(w.SessionID(), op.Request.ID, "ack")
	if err != nil {
		return err
	}
	if port, ok := c.Executor.(effect.Acknowledger); ok {
		err = port.Acknowledge(ctx, op.Request.Key(w.SessionID()))
	}
	if err == nil {
		var res writer.CommitResult
		res, err = unit.Commit(ctx, w, c.now(), unit.Work{CommitID: ledger.CommitID(identity), Parts: []unit.Part{compaction.Acknowledge(op.Request.ID)}})
		err = commitOK("acknowledge", &res, err)
	}
	if err != nil {
		return fmt.Errorf("driver: acknowledge summary %s: %w", op.Request.ID, err)
	}
	return nil
}
