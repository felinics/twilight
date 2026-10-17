package loop

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/felinics/twilight/agentcore/prompt"
	"github.com/felinics/twilight/agentcore/testutil/local"
	turn "github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/model"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/store"
	"github.com/felinics/twilight/sdk"
)

type candidateBuilderFunc func(context.Context, prompt.Input) (prompt.Prompt, error)

func (f candidateBuilderFunc) Build(ctx context.Context, input prompt.Input) (prompt.Prompt, error) {
	return f(ctx, input)
}

func TestCandidateChecksCompleteRequestBeforeEveryModelCall(t *testing.T) {
	rt, w := loopRuntime(t)
	spec := toolSpec(t, "echo", turn.DirectExecution)
	tool := &fakeTool{ref: "echo", def: toolDef("echo"), policy: turn.DirectExecution,
		execute: func(context.Context, local.ToolExecutionRequest) ToolExecutionOutcome {
			return ToolExecutionSucceeded{Result: turn.ToolExecutionResult{Output: cj(`"ok"`)}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1"), textResult("done")}}
	var built prompt.Prompt
	var order []string
	builds, checks := 0, 0
	builder := candidateBuilderFunc(func(ctx context.Context, input prompt.Input) (prompt.Prompt, error) {
		order = append(order, "build")
		builds++
		p, err := (staticBuilder{specs: []turn.ToolSpec{spec}}).Build(ctx, input)
		p.Request.System = "system policy"
		p.Request.Messages = append([]model.Message{{Role: model.MessageRoleSystem,
			Content: []model.MessagePart{{Type: model.MessagePartTypeText, Text: "system message"}}}}, p.Request.Messages...)
		p.Token = turn.PromptToken(fmt.Sprintf("opaque-%d", builds))
		built = p
		return p, err
	})
	settings := Settings{
		BeforePrepare: func(context.Context, store.TurnStore, prompt.Input) error {
			order = append(order, "before")
			return nil
		},
		CheckCandidate: func(_ context.Context, st store.TurnStore, input prompt.Input, p *prompt.Prompt) (CandidateDecision, error) {
			order = append(order, "check")
			checks++
			if input.Scope != st.Scope() || input.TurnID != "run-1" || !reflect.DeepEqual(p, &built) {
				t.Fatalf("candidate or input not passed intact: input=%+v candidate=%+v", input, p)
			}
			if p.Request.System != "system policy" || p.Request.Messages[0].Role != model.MessageRoleSystem || len(p.Request.Tools) != 1 || len(p.Tools) != 1 || p.Token == "" {
				t.Fatalf("incomplete candidate: %+v", p)
			}
			return CandidateReady, nil
		},
	}
	l, err := newLoop(t, nil, fakeCatalog{invoker}, fakeToolCatalog{map[turn.ToolRef]local.ExecutableTool{"echo": tool}}, builder, settings, false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := l.Run(context.Background(), rt.Bind(w), "run-1", nil)
	if err != nil || res.Disposition != LoopFinished || checks != 2 || builds != 2 || invoker.calls.Load() != 2 {
		t.Fatalf("Run = %+v, %v; builds=%d checks=%d calls=%d", res, err, builds, checks, invoker.calls.Load())
	}
	if !reflect.DeepEqual(order, []string{"before", "build", "check", "before", "build", "check"}) {
		t.Fatalf("hook order = %v", order)
	}
}

func TestCandidateMaintenanceAndErrorsDoNotPrepareOrDispatch(t *testing.T) {
	hookErr := errors.New("estimator failed")
	buildErr := errors.New("build failed")
	for _, blocking := range []bool{false, true} {
		for _, test := range []struct {
			name     string
			decision CandidateDecision
			hookErr  error
			buildErr error
			budget   *prompt.TokenBudget
			wantErr  bool
		}{
			{name: "maintenance", decision: CandidateMaintenance},
			{name: "over budget", budget: &prompt.TokenBudget{ContextWindow: 100, OutputReserve: 20, SafetyMargin: 10}},
			{name: "invalid budget", budget: &prompt.TokenBudget{ContextWindow: 100, OutputReserve: 100, SafetyMargin: 1}, wantErr: true},
			{name: "hook error", hookErr: hookErr, wantErr: true},
			{name: "build error", buildErr: buildErr, wantErr: true},
			{name: "invalid decision", decision: CandidateDecision(255), wantErr: true},
		} {
			t.Run(fmt.Sprintf("blocking=%v/%s", blocking, test.name), func(t *testing.T) {
				rt, w := loopRuntime(t)
				st := rt.Bind(w)
				before := loadState(t, rt, w, "run-1")
				exec := newRecordingExecutor()
				checks, emitted := 0, 0
				builder := candidateBuilderFunc(func(ctx context.Context, input prompt.Input) (prompt.Prompt, error) {
					if test.buildErr != nil {
						return prompt.Prompt{}, test.buildErr
					}
					return (staticBuilder{}).Build(ctx, input)
				})
				l, err := New(exec, builder, Settings{CheckCandidate: func(context.Context, store.TurnStore, prompt.Input, *prompt.Prompt) (CandidateDecision, error) {
					checks++
					if test.budget != nil {
						exceeded, err := test.budget.Exceeded(71)
						if err != nil {
							return CandidateReady, err
						}
						if exceeded {
							return CandidateMaintenance, nil
						}
					}
					return test.decision, test.hookErr
				}})
				if err != nil {
					t.Fatal(err)
				}
				drive := l.Advance
				if blocking {
					drive = l.Run
				}
				res, err := drive(context.Background(), st, "run-1", sinkFunc(func(context.Context, Event) error { emitted++; return nil }))
				if (err != nil) != test.wantErr || (test.hookErr != nil && !errors.Is(err, test.hookErr)) || (test.buildErr != nil && !errors.Is(err, test.buildErr)) {
					t.Fatalf("result=%+v err=%v", res, err)
				}
				if !test.wantErr && (res.Disposition != LoopWaiting || res.Reason != ContextMaintenance || res.ExecutionRecovery) {
					t.Fatalf("maintenance result = %+v", res)
				}
				after := loadState(t, rt, w, "run-1")
				if before.Position != after.Position || after.State.ModelSteps != 0 || !reflect.DeepEqual(before.State.Current, after.State.Current) || len(exec.dispatched) != 0 || emitted != 0 {
					t.Fatalf("blocked candidate changed Run or emitted/dispatched: before=%+v after=%+v dispatch=%d events=%d", before, after, len(exec.dispatched), emitted)
				}
				wantChecks := 1
				if test.buildErr != nil {
					wantChecks = 0
				}
				if checks != wantChecks {
					t.Fatalf("checks=%d want %d", checks, wantChecks)
				}
			})
		}
	}
}

func TestCandidateRebuildRefreshesHintAndPreparePosition(t *testing.T) {
	rt, w := loopRuntime(t)
	st := rt.Bind(w)
	before := loadState(t, rt, w, "run-1")
	exec := newRecordingExecutor()
	var hints [][]turn.InputID
	builder := candidateBuilderFunc(func(ctx context.Context, input prompt.Input) (prompt.Prompt, error) {
		p, err := (staticBuilder{}).Build(ctx, input)
		hints = append(hints, append([]turn.InputID(nil), p.InputIDs...))
		p.Token = turn.PromptToken(fmt.Sprintf("candidate-%d", len(hints)))
		return p, err
	})
	checks := 0
	var freshPosition turn.TurnPosition
	l, err := New(exec, builder, Settings{CheckCandidate: func(ctx context.Context, st store.TurnStore, input prompt.Input, p *prompt.Prompt) (CandidateDecision, error) {
		checks++
		if checks == 1 {
			freshPosition = candidateAcceptInput(t, ctx, st, "new")
			return CandidateRebuild, nil
		}
		if input.Scope != testScope || len(input.Inputs) != 2 || p.Token != "candidate-2" {
			t.Fatalf("stale input/candidate: %+v, %+v", input, p)
		}
		return CandidateReady, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	res, err := l.Advance(context.Background(), st, "run-1", nil)
	if err != nil || res.Disposition != LoopDispatched || checks != 2 || len(exec.dispatched) != 1 {
		t.Fatalf("Advance=%+v, %v checks=%d dispatch=%d", res, err, checks, len(exec.dispatched))
	}
	if !reflect.DeepEqual(hints, [][]turn.InputID{{"seed"}, {"seed", "new"}}) || freshPosition == before.Position {
		t.Fatalf("hints=%v positions=%v -> %v", hints, before.Position, freshPosition)
	}
	// A stale Prepare position would be rejected and cause a third Build.
	step := loadState(t, rt, w, "run-1").State.Current.(turn.ModelStep)
	wantStep := schema.Identity().DeriveModelStepID("run-1", schema.Identity().DeriveModelRequestCommandID("run-1", freshPosition))
	if step.RefValue.ID != wantStep {
		t.Fatalf("prepared with stale position: step=%+v want=%s", step, wantStep)
	}
	prepared := 0
	for _, fact := range recordFacts(t, rt, "run-1") {
		if f, ok := fact.(turn.ModelStepPrepared); ok {
			prepared++
			if !reflect.DeepEqual(f.InputIDs, []turn.InputID{"seed", "new"}) {
				t.Fatalf("prepared stale input IDs: %v", f.InputIDs)
			}
		}
	}
	if prepared != 1 {
		t.Fatalf("prepared %d candidates, want only the rebuilt candidate", prepared)
	}
}

func TestCandidateRebuildRefreshesContextWithoutRunProgress(t *testing.T) {
	rt, w := loopRuntime(t)
	exec := newRecordingExecutor()
	builds, checks, befores := 0, 0, 0
	contextText := "old context"
	builder := candidateBuilderFunc(func(ctx context.Context, input prompt.Input) (prompt.Prompt, error) {
		builds++
		p, err := (staticBuilder{}).Build(ctx, input)
		p.Request.System = contextText
		return p, err
	})
	l, err := New(exec, builder, Settings{
		BeforePrepare: func(context.Context, store.TurnStore, prompt.Input) error {
			befores++
			return nil
		},
		CheckCandidate: func(ctx context.Context, st store.TurnStore, _ prompt.Input, p *prompt.Prompt) (CandidateDecision, error) {
			checks++
			if checks == 1 {
				contextText = "new context"
				return CandidateRebuild, nil
			}
			if p.Request.System != "new context" {
				t.Fatalf("reused rejected candidate: %+v", p)
			}
			return CandidateReady, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	res, err := l.Advance(context.Background(), rt.Bind(w), "run-1", nil)
	if err != nil || res.Disposition != LoopDispatched || builds != 2 || checks != 2 || befores != 2 || len(exec.dispatched) != 1 {
		t.Fatalf("Advance=%+v, %v builds=%d checks=%d befores=%d dispatch=%d", res, err, builds, checks, befores, len(exec.dispatched))
	}
	body := exec.dispatched[0].Body.(ModelAssignment)
	if body.Request == nil || body.Request.System != "new context" {
		t.Fatalf("dispatched rejected candidate: %+v", body.Request)
	}
}

func candidateAcceptInput(t *testing.T, ctx context.Context, st store.TurnStore, id turn.InputID) turn.TurnPosition {
	t.Helper()
	snap, err := st.Load(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	cmd := turn.NextStep(turn.AgentInput{ID: id, Digest: inputDigest(string(id))})
	env, err := schema.Wire().Envelope("run-1", schema.Identity().DeriveInputCommandID("run-1", id), cmd)
	if err != nil {
		t.Fatal(err)
	}
	res, err := st.Commit(ctx, store.CommitRequest{Base: snap.Position, Command: env})
	if err != nil {
		t.Fatal(err)
	}
	return res.Snapshot.Position
}

func TestCandidateRebuildBoundAndAuthorityProgress(t *testing.T) {
	for _, limit := range []int{0, 1, 2} {
		for _, progress := range []bool{false, true} {
			t.Run(fmt.Sprintf("limit=%d/progress=%v", limit, progress), func(t *testing.T) {
				rt, w := loopRuntime(t)
				exec := newRecordingExecutor()
				checks := 0
				progressed := false
				max := limit
				if max == 0 {
					max = 4
				}
				l, err := New(exec, staticBuilder{}, Settings{MaxCandidateRebuilds: limit, CheckCandidate: func(ctx context.Context, st store.TurnStore, _ prompt.Input, _ *prompt.Prompt) (CandidateDecision, error) {
					checks++
					if progress && !progressed && checks == max+1 {
						candidateAcceptInput(t, ctx, st, "progress")
						progressed = true
					}
					return CandidateRebuild, nil
				}})
				if err != nil {
					t.Fatal(err)
				}
				_, err = l.Advance(context.Background(), rt.Bind(w), "run-1", nil)
				wantChecks := max + 1
				if progress {
					wantChecks *= 2
				}
				if !errors.Is(err, ErrCandidateContended) || checks != wantChecks || len(exec.dispatched) != 0 {
					t.Fatalf("err=%v checks=%d want=%d dispatch=%d", err, checks, wantChecks, len(exec.dispatched))
				}
				// A fresh Advance has its own bound, independent of prior calls.
				checks = 0
				_, err = l.Advance(context.Background(), rt.Bind(w), "run-1", nil)
				if !errors.Is(err, ErrCandidateContended) || checks != max+1 {
					t.Fatalf("second Advance err=%v checks=%d", err, checks)
				}
			})
		}
	}
}

func TestNewRejectsNegativeCandidateRebuilds(t *testing.T) {
	if _, err := New(newRecordingExecutor(), staticBuilder{}, Settings{MaxCandidateRebuilds: -1}); err == nil {
		t.Fatal("negative MaxCandidateRebuilds accepted")
	}
}
