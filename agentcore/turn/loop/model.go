package loop

import (
	"context"
	"fmt"

	"github.com/felinics/twilight/agentcore/prompt"
	turn "github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/frozen"
	"github.com/felinics/twilight/agentcore/turn/model"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/store"
)

func (l *Loop) planAndPrepare(ctx context.Context, rt store.TurnStore, events EventSink, snapshot *store.Snapshot, hint prompt.Input) (CandidateDecision, error) {
	hint.Scope = rt.Scope()
	p, err := l.Builder.Build(ctx, hint)
	if err != nil {
		return CandidateReady, err
	}
	frozenRequest := p.Request
	modelRef := p.Model
	if modelRef == "" {
		modelRef = turn.ModelRef(frozenRequest.Model)
	}
	if modelRef == "" {
		return CandidateReady, fmt.Errorf("agent: loop: empty model")
	}
	if turn.ModelRef(frozenRequest.Model) != modelRef {
		return CandidateReady, fmt.Errorf("agent: loop: request model %q does not match plan model %q", frozenRequest.Model, modelRef)
	}
	p.Model = modelRef
	if hook := l.Settings.CheckCandidate; hook != nil {
		decision, err := hook(ctx, rt, hint, &p)
		if err != nil {
			return CandidateReady, err
		}
		switch decision {
		case CandidateReady:
		case CandidateRebuild, CandidateMaintenance:
			return decision, nil
		default:
			return CandidateReady, fmt.Errorf("agent: loop: unknown candidate decision %d", decision)
		}
	}
	requestDigest, err := schema.Canonical().DigestRequest(frozenRequest)
	if err != nil {
		return CandidateReady, err
	}
	cmdID := schema.Identity().DeriveModelRequestCommandID(snapshot.State.TurnID, snapshot.Position)
	stepID := schema.Identity().DeriveModelStepID(snapshot.State.TurnID, cmdID)
	res, err := l.commit(ctx, rt, snapshot.State.TurnID, cmdID, snapshot.Position, turn.PrepareModelRequest{
		StepID:        stepID,
		Model:         modelRef,
		Request:       frozenRequest,
		RequestDigest: requestDigest,
		InputIDs:      p.InputIDs,
		PromptToken:   p.Token,
		Tools:         p.Tools,
	})
	if err == nil {
		// ModelStepPrepared carries the frozen request — the most informative
		// fact of the Turn; observers must see it like every other accepted
		// transition.
		l.emitCommitted(ctx, events, rt.Scope(), snapshot.State.TurnID, res.Facts)
		return CandidateReady, nil
	}
	if !retriable(err) {
		return CandidateReady, err
	}
	// A retriable rejection with no authority progress means the rejection
	// was about THIS plan's content (InputIDs, digests), not concurrency:
	// retrying the same prompt builder at the same revision would spin forever.
	after, loadErr := rt.Load(ctx, snapshot.State.TurnID)
	if loadErr != nil {
		return CandidateReady, loadErr
	}
	if after.Position == snapshot.Position {
		return CandidateReady, fmt.Errorf("agent: loop: prepare rejected without authority progress: %w", err)
	}
	return CandidateReady, nil // another actor advanced the Turn; reload decides the next action
}

// --- StartModelCall ---

// startModelStep commits the start barrier of the step's next model effect
// and hands the call to the Executor. It returns the dispatched key, or nil when
// the reload should decide (another actor moved the step). A model catalog
// that cannot serve the step withdraws it to Open and reports the error: no
// model call has happened.
func (l *Loop) startModelStep(ctx context.Context, rt store.TurnStore, events EventSink, snapshot *store.Snapshot, stepID turn.StepID) (*AssignmentKey, error) {
	turnID := snapshot.State.TurnID
	prepared, ok := snapshot.State.Current.(turn.ModelStep)
	if !ok || prepared.RefValue.ID != stepID {
		return nil, fmt.Errorf("agent: loop: model step %q is not current", stepID)
	}
	ref := modelEffect(turnID, &prepared)
	target, err := l.targetFor(ctx, EffectContext{Session: rt.Scope(), TurnID: turnID, StepID: stepID, Effect: ref.id, Kind: AssignmentModel})
	if err != nil {
		return nil, err
	}
	assignment := Assignment{Session: rt.Scope(), OwnerKind: OwnerTurn, OwnerID: string(turnID), StepID: stepID, Effect: ref.id, Target: target,
		Body: ModelAssignment{Model: prepared.Model, RequestDigest: prepared.RequestDigest}}
	// Pre-start check: an executor that cannot serve the model
	// fails here, with the step still Prepared and no start or recovery fact.
	unavailable, err := l.Executor.Validate(ctx, assignment)
	if err != nil {
		return nil, err
	}
	if unavailable != nil {
		return nil, fmt.Errorf("%w: %s: %s: %s", ErrModelUnavailable, prepared.Model, unavailable.Class, unavailable.Message)
	}
	start, err := l.commit(ctx, rt, turnID, ref.startID(), snapshot.Position, turn.StartModelExecution{StepID: stepID, Effect: ref.id})
	if err != nil {
		if retriable(err) {
			return nil, nil // another actor moved the step; reload decides
		}
		return nil, err
	}
	l.emitCommitted(ctx, events, rt.Scope(), turnID, start.Facts)

	modelStep, ok := start.Snapshot.State.Current.(turn.ModelStep)
	if !ok || modelStep.RefValue.ID != stepID || modelStep.Status != turn.ModelExecuting || modelStep.Effect != ref.id {
		// The start (or its one-shot replay) landed but the step is no longer
		// Executing: something settled it meanwhile. Reload decides.
		if start.Status == store.CommitAlreadyApplied {
			return nil, nil
		}
		return nil, fmt.Errorf("agent: loop: started step %q is not current", stepID)
	}

	request, err := rt.FrozenRequest(ctx, prepared.RequestDigest)
	if err != nil {
		if _, serr := l.settle(ctx, rt, events, &ref, start.Snapshot.Position,
			turn.RecoverModelExecution{StepID: stepID, Effect: ref.id}); serr != nil {
			return nil, serr
		}
		return nil, fmt.Errorf("agent: loop: load frozen model request: %w", err)
	}
	assignment.Body = ModelAssignment{Model: prepared.Model, RequestDigest: prepared.RequestDigest, Request: &request}

	if err := l.dispatch(ctx, assignment); err != nil {
		if recoverableDispatch(ctx, err) {
			// Durable acceptance can precede a retryable store/RPC failure.
			// Preserve the same intent; reconciliation, not a new effect,
			// decides what the Execution Authority actually accepted.
			return nil, fmt.Errorf("agent: loop: model dispatch outcome: %w", err)
		}
		// A definite permanent refusal withdraws the step under its recovery
		// identity. Settlement still respects the caller's bounded context.
		if _, serr := l.settle(ctx, rt, events, &ref, start.Snapshot.Position,
			turn.RecoverModelExecution{StepID: stepID, Effect: ref.id}); serr != nil {
			return nil, serr
		}
		return nil, fmt.Errorf("agent: loop: model dispatch: %w", err)
	}
	key := assignment.Key()
	return &key, nil
}

// modelCompletion maps a model Outcome to the effect's settlement command
// a cancelled call withdraws the step to Open (the next Advance
// plans again from the current state); a provider failure is
// SubmitModelFailure; a result that cannot be bound or frozen is
// RejectModelResult with the host's disposition; a result binds its tool
// calls into SubmitModelResult.
// A body the executor reports missing also withdraws the step, but the
// condition is returned as an error alongside the command: the settlement
// lands, and the drive stops instead of prompt building, freezing and dispatching
// again against the same missing store. Whether to try again is the host's
// decision, so a persistently unreadable store cannot spin the Turn.
func (l *Loop) modelCompletion(step *turn.ModelStep, out Outcome) (turn.AgentCommand, error) {
	stepID := step.RefValue.ID
	withdraw := turn.RecoverModelExecution{StepID: stepID, Effect: step.Effect}
	var result model.ModelResult
	switch r := out.Result.(type) {
	case effect.ModelSucceeded:
		result = r.Result
	case effect.Unknown:
		// No outcome exists: the executor closed its recovery without a
		// provider answer (Dispose, an adoption that could not restart).
		// A model effect has no world side effect, so the step is withdrawn
		// to Open and the next Advance replans, the same disposition the
		// recovery path gives a missing execution.
		return withdraw, nil
	case effect.Cancelled:
		return withdraw, nil
	case effect.ModelFailed:
		switch r.Code {
		case effect.FailureFrozenValueMissing:
			return withdraw, fmt.Errorf("agent: loop: model dispatch: %w: %s", frozen.ErrMissing, r.Message)
		case effect.FailureMalformedRequest, effect.FailureMalformedResult:
			failure := turn.StepFailure{Class: turn.FailureMalformedModel, Message: r.Message}
			return turn.RejectModelResult{StepID: stepID, Effect: step.Effect, Failure: failure, Disposition: l.modelRejectDisposition(step, failure)}, nil
		case effect.FailureDeadline:
			return withdraw, nil
		default:
			return turn.SubmitModelFailure{StepID: stepID, Effect: step.Effect, Failure: turn.StepFailure{Class: turn.FailureProvider, Message: r.Message}}, nil
		}
	default:
		// A tool result or no result for a model step: the executor answered
		// for the wrong effect. Nothing certain happened.
		return turn.SubmitModelFailure{StepID: stepID, Effect: step.Effect, Failure: turn.StepFailure{Class: turn.FailureProvider, Message: fmt.Sprintf("executor delivered %T for a model step", out.Result)}}, nil
	}
	bindings := l.bindToolCalls(&result, step)
	return turn.SubmitModelResult{StepID: stepID, Effect: step.Effect, Result: result, Calls: bindings, Scheduling: l.toolScheduling()}, nil
}

// modelRejectDisposition applies Settings.MalformedRetries: the step's
// Rejects counts the malformed results already recorded, so the step is
// retried while that count is below the bound and fails the Turn otherwise.
// Zero retries fails on the first malformed result.
func (l *Loop) modelRejectDisposition(step *turn.ModelStep, _ turn.StepFailure) turn.ModelRejectDisposition {
	if step.Rejects < int(l.Settings.MalformedRetries) {
		return turn.ModelRejectRetry
	}
	return turn.ModelRejectFailTurn
}

// bindToolCalls produces bindings from the frozen ToolSpecs.
// It never calls local.ExecutableTool.
func (l *Loop) bindToolCalls(result *model.ModelResult, step *turn.ModelStep) []turn.ToolCallBinding {
	if len(result.ToolCalls) == 0 {
		return nil
	}
	specByName := make(map[string]turn.ToolSpec, len(step.Tools))
	for _, s := range step.Tools {
		specByName[s.Name] = s
	}
	bindings := make([]turn.ToolCallBinding, len(result.ToolCalls))
	for i, tc := range result.ToolCalls {
		args := tc.Input.Canonical()
		// The Turn's CallID derives from the step and position; the provider's
		// id is carried for the round trip only, so a provider that repeats or
		// omits ids cannot break identity here.
		b := turn.ToolCallBinding{
			CallID:         schema.Identity().DeriveCallID(step.RefValue.ID, i),
			ProviderCallID: tc.ToolCallID,
			ToolRef:        turn.ToolRef(tc.ToolName),
			Arguments:      args,
			Policy:         turn.DirectExecution,
		}
		if spec, known := specByName[tc.ToolName]; known {
			// The binding's ToolRef is the frozen spec's Ref — the catalog
			// key — not the model-facing definition name; the two may differ
			// (aliased tools).
			b.ToolRef = spec.Ref
			b.DefinitionDigest = spec.DefinitionDigest
			b.Policy = spec.Policy
			b.Replay = spec.Replay
			b.Placement = spec.Placement
		}
		bindings[i] = b
	}
	return bindings
}
