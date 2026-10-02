package loop

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/executor/local"
	. "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/store"
	"github.com/felinics/twilight/sdk"
)

// The owner process dies while a tool call is Executing. A new owner takes
// the Session over, the takeover settles the call as Unknown, a fresh
// Loop finishes the Run, and the dead owner's late settlement is fenced with
// ownership loss (RUN-CMT-6/7, RUN-LOP-5).
func TestTakeoverDisposesExecutingCallAndFencesOldOwner(t *testing.T) {
	stack := newTestStack(t, nil)
	stack.createRun(t, "run-1", AgentInput{ID: "seed", Digest: inputDigest(`{}`)})
	oldRuntime := stack.runtime
	oldWriter := stack.writer(t) // the superseded owner's capability

	spec := toolSpec(t, "slow", DirectExecution)
	block := make(chan struct{})
	started := make(chan struct{}, 1)
	slow := &fakeTool{ref: "slow", def: toolDef(spec.Name), policy: DirectExecution,
		execute: func(ctx context.Context, req local.ToolExecutionRequest) effect.ToolExecutionOutcome {
			started <- struct{}{}
			<-block
			return effect.ToolExecutionSucceeded{Result: ToolExecutionResult{Output: req.Arguments}}
		}}
	call := sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 2},
		ToolCalls: []sdk.ToolCall{{ToolCallID: "c1", ToolName: "slow", Input: sdk.ParseToolArguments(`{"x":1}`)}}}
	builder := staticBuilder{specs: []ToolSpec{spec}}
	first, err := newLoop(t, fakeCatalog{&fakeInvoker{results: []sdk.ModelResult{call}}}, fakeToolCatalog{map[ToolRef]local.ExecutableTool{"slow": slow}}, Settings{}, false)
	if err != nil {
		t.Fatal(err)
	}
	firstWatcher := awaiting(t, first)
	firstDone := make(chan error, 1)
	go func() {
		_, err := settle(context.Background(), first, firstWatcher, oldRuntime.Bind(oldWriter), builder, "run-1")
		firstDone <- err
	}()
	<-started

	// The old owner is presumed dead; a new owner opens with Takeover.
	stack.open(t)
	n, err := recoverRuns(context.Background(), t, stack.runtime, stack.writer(t), &reconcile.Reconciler{Abandon: true})
	if err != nil || n != 1 {
		t.Fatalf("takeover = %d %v, want 1", n, err)
	}
	again, err := recoverRuns(context.Background(), t, stack.runtime, stack.writer(t), &reconcile.Reconciler{Abandon: true})
	if err != nil || again != 0 {
		t.Fatalf("second takeover = %d %v, want 0", again, err)
	}
	snap := loadState(t, stack.runtime, stack.writer(t), "run-1")
	if _, open := snap.State.Current.(Open); !open {
		t.Fatalf("after takeover current = %T, want Open", snap.State.Current)
	}

	second, err := newLoop(t, fakeCatalog{&fakeInvoker{results: []sdk.ModelResult{textResult("done")}}}, fakeToolCatalog{map[ToolRef]local.ExecutableTool{"slow": slow}}, Settings{}, false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := settle(context.Background(), second, awaiting(t, second), stack.runtime.Bind(stack.writer(t)), builder, "run-1")
	if err != nil || res.Disposition != LoopFinished || res.Result.Status != RunCompleted {
		t.Fatalf("second loop = %+v %v", res, err)
	}
	unknown := 0
	for _, f := range recordFacts(t, stack.runtime, "run-1") {
		if failed, ok := f.(ToolCallFailed); ok && failed.Outcome == ToolOutcomeUnknown {
			unknown++
		}
	}
	if unknown != 1 {
		t.Fatalf("unknown settlements = %d, want 1", unknown)
	}

	// The dead owner's worker finally returns: its settlement is fenced.
	close(block)
	if err := <-firstDone; !errors.Is(err, store.ErrOwnershipLost) {
		t.Fatalf("old owner loop error = %v, want ownership lost", err)
	}
	// Nothing of the old owner reached the ledger after the takeover.
	for _, f := range recordFacts(t, stack.runtime, "run-1") {
		if _, ok := f.(ToolCallCompleted); ok {
			t.Fatal("fenced worker's result reached the ledger")
		}
	}
}
