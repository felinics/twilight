package loop

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/prompt"
	run "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/run/store"
)

// planAndPrepare asks builder for the step's request and commits the Prepare
// that freezes it, with the policy the builder decided for the step.
func (l *Loop) planAndPrepare(ctx context.Context, rt store.RunStore, builder prompt.Builder, snapshot *store.Snapshot, hint run.PromptInput) error {
	hint.Scope = rt.Scope()
	p, err := builder.Build(ctx, hint)
	if err != nil {
		return err
	}
	frozenRequest := p.Request
	modelRef := p.Model
	if modelRef == "" {
		modelRef = run.ModelRef(frozenRequest.Model)
	}
	if modelRef == "" {
		return fmt.Errorf("agent: loop: empty model")
	}
	if run.ModelRef(frozenRequest.Model) != modelRef {
		return fmt.Errorf("agent: loop: request model %q does not match plan model %q", frozenRequest.Model, modelRef)
	}
	requestDigest, err := schema.Canonical().DigestRequest(frozenRequest)
	if err != nil {
		return err
	}
	cmdID := schema.Identity().DeriveModelRequestCommandID(snapshot.State.RunID, snapshot.Position)
	stepID := schema.Identity().DeriveModelStepID(snapshot.State.RunID, cmdID)
	_, err = l.commit(ctx, rt, snapshot.State.RunID, cmdID, snapshot.Position, run.PrepareModelRequest{
		StepID:        stepID,
		Model:         modelRef,
		Request:       frozenRequest,
		RequestDigest: requestDigest,
		InputIDs:      p.InputIDs,
		PromptToken:   p.Token,
		Tools:         p.Tools,
		Policy:        p.Policy,
	})
	if err == nil {
		// ModelStepPrepared carries the frozen request — the most informative
		// fact of the run; observers must see it like every other accepted
		// transition.
		return nil
	}
	if !retriable(err) {
		return err
	}
	// A retriable rejection with no authority progress means the rejection
	// was about THIS plan's content (InputIDs, digests), not concurrency:
	// retrying the same prompt builder at the same revision would spin forever.
	after, loadErr := rt.Load(ctx, snapshot.State.RunID)
	if loadErr != nil {
		return loadErr
	}
	if after.Position == snapshot.Position {
		return fmt.Errorf("agent: loop: prepare rejected without authority progress: %w", err)
	}
	return nil // another actor advanced the run; reload decides the next action
}

// --- StartModelCall ---

// startModelStep commits the start barrier of the step's next model effect
// and hands the call to the Executor (RUN-LOP-3). It returns the dispatched key, or nil when
// the reload should decide (another actor moved the step). A model catalog
// that cannot serve the step withdraws it to Open and reports the error: no
// model call has happened.
func (l *Loop) startModelStep(ctx context.Context, rt store.RunStore, snapshot *store.Snapshot, stepID run.StepID) (*effect.AssignmentKey, error) {
	runID := snapshot.State.RunID
	prepared, ok := snapshot.State.Current.(run.ModelStep)
	if !ok || prepared.RefValue.ID != stepID {
		return nil, fmt.Errorf("agent: loop: model step %q is not current", stepID)
	}
	ref := modelEffect(runID, &prepared)
	target, err := l.targetFor(ctx, EffectContext{Session: rt.Scope(), RunID: runID, StepID: stepID, Effect: ref.id, Kind: effect.AssignmentModel})
	if err != nil {
		return nil, err
	}
	assignment := effect.Assignment{Session: rt.Scope(), RunID: runID, StepID: stepID, Effect: ref.id, Target: target,
		Body: effect.ModelAssignment{Model: prepared.Model, RequestDigest: prepared.RequestDigest}}
	// Pre-start check (RUN-EXE-5): an executor that cannot serve the model
	// fails here, with the step still Prepared and no start or recovery fact.
	unavailable, err := l.Ports.Execution.Validate(ctx, assignment)
	if err != nil {
		return nil, err
	}
	if unavailable != nil {
		return nil, fmt.Errorf("%w: %s: %s: %s", ErrModelUnavailable, prepared.Model, unavailable.Class, unavailable.Message)
	}
	start, err := l.commit(ctx, rt, runID, ref.startID(), snapshot.Position, run.StartModelExecution{StepID: stepID, Effect: ref.id})
	if err != nil {
		if retriable(err) {
			return nil, nil // another actor moved the step; reload decides
		}
		return nil, err
	}

	modelStep, ok := start.Snapshot.State.Current.(run.ModelStep)
	if !ok || modelStep.RefValue.ID != stepID || modelStep.Status != run.ModelExecuting || modelStep.Effect != ref.id {
		// The start (or its one-shot replay) landed but the step is no longer
		// Executing: something settled it meanwhile. Reload decides.
		if start.Status == store.CommitAlreadyApplied {
			return nil, nil
		}
		return nil, fmt.Errorf("agent: loop: started step %q is not current", stepID)
	}

	request, err := rt.FrozenRequest(ctx, prepared.RequestDigest)
	if err != nil {
		if _, serr := l.settle(context.WithoutCancel(ctx), rt, &ref, start.Snapshot.Position,
			run.RecoverModelExecution{StepID: stepID, Effect: ref.id}); serr != nil {
			return nil, serr
		}
		return nil, fmt.Errorf("agent: loop: load frozen model request: %w", err)
	}
	assignment.Body = effect.ModelAssignment{Model: prepared.Model, RequestDigest: prepared.RequestDigest, Request: &request}

	if err := l.dispatch(ctx, assignment); err != nil {
		if errors.Is(err, effect.ErrDispatchUnknown) {
			// The request may have crossed the external boundary. Leave the
			// model Executing so recovery can Attach/Reconcile/Takeover it.
			return nil, fmt.Errorf("agent: loop: model dispatch outcome: %w", err)
		}
		// Nothing was called: withdraw the step to Open under this effect's
		// recovery identity and surface the condition (RUN-LOP-3).
		if _, serr := l.settle(context.WithoutCancel(ctx), rt, &ref, start.Snapshot.Position,
			run.RecoverModelExecution{StepID: stepID, Effect: ref.id}); serr != nil {
			return nil, serr
		}
		return nil, fmt.Errorf("agent: loop: model dispatch: %w", err)
	}
	key := assignment.Key()
	return &key, nil
}

// modelCompletion maps a model Outcome to the effect's settlement command
// (RUN-LOP-3): a cancelled call withdraws the step to Open (the next Advance
// plans again from the current state); a provider failure is
// SubmitModelFailure; a result that cannot be bound or frozen is
// RejectModelResult, and the step's frozen policy decides whether it
// retries; a result binds its tool calls into SubmitModelResult.
//
// A body the executor reports missing also withdraws the step, but the
// condition is returned as an error alongside the command: the settlement
// lands, and the drive stops instead of prompt building, freezing and dispatching
// again against the same missing store. Whether to try again is the host's
// decision, so a persistently unreadable store cannot spin the Run.
func (l *Loop) modelCompletion(step *run.ModelStep, out effect.Outcome) (run.AgentCommand, error) {
	stepID := step.RefValue.ID
	withdraw := run.RecoverModelExecution{StepID: stepID, Effect: step.Effect}
	var result model.ModelResult
	switch r := out.Result.(type) {
	case effect.ModelSucceeded:
		result = r.Result
	case effect.Unknown:
		// No outcome exists: the executor closed its recovery without a
		// provider answer (Dispose, an adoption that could not restart).
		// A model effect has no world side effect, so the step is withdrawn
		// to Open and the next Advance replans, the same disposition the
		// recovery path gives a missing execution (RUN-CMT-7, RUN-EXE-6).
		return withdraw, nil
	case effect.Cancelled:
		return withdraw, nil
	case effect.ModelFailed:
		switch r.Code {
		case effect.FailureFrozenValueMissing:
			return withdraw, fmt.Errorf("agent: loop: model dispatch: %w: %s", frozen.ErrMissing, r.Message)
		case effect.FailureMalformedRequest, effect.FailureMalformedResult:
			failure := run.StepFailure{Class: run.FailureMalformedModel, Message: r.Message}
			return run.RejectModelResult{StepID: stepID, Effect: step.Effect, Failure: failure}, nil
		case effect.FailureDeadline:
			return withdraw, nil
		default:
			return run.SubmitModelFailure{StepID: stepID, Effect: step.Effect, Failure: run.StepFailure{Class: run.FailureProvider, Message: r.Message}}, nil
		}
	default:
		// A tool result or no result for a model step: the executor answered
		// for the wrong effect. Nothing certain happened.
		return run.SubmitModelFailure{StepID: stepID, Effect: step.Effect, Failure: run.StepFailure{Class: run.FailureProvider, Message: fmt.Sprintf("executor delivered %T for a model step", out.Result)}}, nil
	}
	bindings := l.bindToolCalls(&result, step)
	return run.SubmitModelResult{StepID: stepID, Effect: step.Effect, Result: result, Calls: bindings}, nil
}

// bindToolCalls produces bindings from the frozen ToolSpecs (RUN-MCH-2).
// It never calls local.ExecutableTool.
func (l *Loop) bindToolCalls(result *model.ModelResult, step *run.ModelStep) []run.ToolCallBinding {
	if len(result.ToolCalls) == 0 {
		return nil
	}
	specByName := make(map[string]run.ToolSpec, len(step.Tools))
	for _, s := range step.Tools {
		specByName[s.Name] = s
	}
	bindings := make([]run.ToolCallBinding, len(result.ToolCalls))
	for i, tc := range result.ToolCalls {
		args := tc.Input.Canonical()
		// The Run's CallID derives from the step and position; the provider's
		// id is carried for the round trip only, so a provider that repeats or
		// omits ids cannot break identity here.
		b := run.ToolCallBinding{
			CallID:         schema.Identity().DeriveCallID(step.RefValue.ID, i),
			ProviderCallID: tc.ToolCallID,
			ToolRef:        run.ToolRef(tc.ToolName),
			Arguments:      args,
			Policy:         run.DirectExecution,
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
