package loop

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/sdk"
)

// commitLog wraps a bound RunStore and records every AgentCommand it is
// asked to commit, so a test can assert what a fenced Loop still tried to
// write.
type commitLog struct {
	runtime.RunStore
	mu   sync.Mutex
	cmds []AgentCommand
}

func (c *commitLog) Commit(ctx context.Context, req runtime.CommitRequest) (runtime.CommitResult, error) {
	c.mu.Lock()
	c.cmds = append(c.cmds, req.Command.Command)
	c.mu.Unlock()
	return c.RunStore.Commit(ctx, req)
}

func (c *commitLog) settlementsFor(callID CallID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, cmd := range c.cmds {
		switch s := cmd.(type) {
		case SubmitToolResult:
			if s.CallID == callID {
				n++
			}
		case SubmitToolFailure:
			if s.CallID == callID {
				n++
			}
		}
	}
	return n
}

// Two tool calls run in parallel. The Session is taken over while both are
// executing; the first worker to finish has its settlement fenced with
// ErrOwnershipLost. RUN-LOP-5 then requires the Loop to cancel every other
// worker's ctx, commit no further settlement, and return that error without
// waiting for the blocked worker to be released on its own.
func TestOwnershipLossCancelsWorkersAndStopsSettling(t *testing.T) {
	stack := newTestStack(t, nil)
	stack.createRun(t, "run-1", AgentInput{ID: "seed", Digest: inputDigest(`{}`)})
	oldWriter := stack.writer(t) // the superseded owner's capability
	oldRuntime := &commitLog{RunStore: stack.runtime.Bind(oldWriter)}

	spec := toolSpec(t, "echo", DirectExecution)
	started := make(chan CallID, 2)
	takenOver := make(chan struct{})
	block := make(chan struct{}) // never closed: B may only leave through ctx
	var bSawCancel atomic.Bool
	var order atomic.Int32
	var slowCall atomic.Value // CallID of the worker that arrived second
	tool := &fakeTool{ref: "echo", def: toolDef(spec.Name), policy: DirectExecution,
		execute: func(ctx context.Context, req ToolExecutionRequest) ToolExecutionOutcome {
			slow := order.Add(1) == 2 // the second worker to start is B
			if slow {
				slowCall.Store(req.CallID)
			}
			started <- req.CallID
			if slow {
				select {
				case <-ctx.Done():
					bSawCancel.Store(true)
					return ToolExecutionUnknown{Failure: ToolFailure{Class: FailureEffectUnknown, Message: ctx.Err().Error()}}
				case <-block:
					return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: req.Arguments}}
				}
			}
			<-takenOver // A settles only after the Session changed hands
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: req.Arguments}}
		}}
	loop, err := newLoop(t, nil, fakeCatalog{&fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1", "c2")}}},
		fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": tool}}, staticBuilder{specs: []ToolSpec{spec}}, Settings{Scheduling: ToolScheduling{MaxParallel: 2}}, false)
	if err != nil {
		t.Fatal(err)
	}
	var completed []CallID
	var sinkMu sync.Mutex
	sink := sinkFunc(func(_ context.Context, e Event) error {
		if e.Kind == EventToolCompleted {
			sinkMu.Lock()
			completed = append(completed, e.CallID)
			sinkMu.Unlock()
		}
		return nil
	})
	done := make(chan error, 1)
	go func() {
		_, err := loop.Run(context.Background(), oldRuntime, "run-1", sink)
		done <- err
	}()
	<-started
	<-started
	slowID := slowCall.Load().(CallID)

	// A new owner takes the Session over: opening its Writer bumps the Epoch
	// and its takeover disposition records both Executing calls as Unknown.
	stack.open(t)
	if n, err := stack.runtime.RecoverInterrupted(context.Background(), stack.writer(t), &reconcile.Reconciler{Abandon: true}); err != nil || n != 2 {
		t.Fatalf("RecoverInterrupted = %d %v, want 2", n, err)
	}
	close(takenOver)

	select {
	case err := <-done:
		if !errors.Is(err, runtime.ErrOwnershipLost) {
			t.Fatalf("loop error = %v, want ErrOwnershipLost", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not return after ownership loss; it waited for the blocked worker")
	}
	if !bSawCancel.Load() {
		t.Fatal("the blocked worker's ctx was not cancelled")
	}
	if n := oldRuntime.settlementsFor(slowID); n != 0 {
		t.Fatalf("fenced loop committed %d settlement(s) for the cancelled worker", n)
	}
	sinkMu.Lock()
	defer sinkMu.Unlock()
	for _, id := range completed {
		if id == slowID {
			t.Fatal("sink saw ToolCompleted for the cancelled worker")
		}
	}
	for _, f := range recordFacts(t, stack.runtime, "run-1") {
		switch f := f.(type) {
		case ToolCallCompleted:
			t.Fatalf("fenced settlement %s reached the ledger", f.CallID)
		case ToolCallFailed:
			if f.Outcome != ToolOutcomeUnknown {
				t.Fatalf("unexpected failure fact %+v", f)
			}
		}
	}
}

// The model path has the same terminal rule: a model-step settlement fenced by
// ErrOwnershipLost is returned as is, without the one-shot replay a
// non-sentinel commit error would get (RUN-LOP-5).
func TestOwnershipLossOnModelSettlementIsNotRetried(t *testing.T) {
	stack := newTestStack(t, nil)
	stack.createRun(t, "run-1", AgentInput{ID: "seed", Digest: inputDigest(`{}`)})
	oldWriter := stack.writer(t) // the superseded owner's capability
	oldRuntime := &commitLog{RunStore: stack.runtime.Bind(oldWriter)}

	invoker := &blockingInvoker{started: make(chan struct{}), release: make(chan struct{})}
	loop, err := newLoop(t, nil, fakeCatalog{invoker}, fakeToolCatalog{nil}, staticBuilder{}, Settings{}, false)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := loop.Run(context.Background(), oldRuntime, "run-1", nil)
		done <- err
	}()
	<-invoker.started

	stack.open(t)
	if n, err := stack.runtime.RecoverInterrupted(context.Background(), stack.writer(t), &reconcile.Reconciler{Abandon: true}); err != nil || n != 1 {
		t.Fatalf("RecoverInterrupted = %d %v, want 1", n, err)
	}
	close(invoker.release)

	select {
	case err := <-done:
		if !errors.Is(err, runtime.ErrOwnershipLost) {
			t.Fatalf("loop error = %v, want ErrOwnershipLost", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not return after ownership loss")
	}
	settlements := 0
	oldRuntime.mu.Lock()
	for _, cmd := range oldRuntime.cmds {
		if _, ok := cmd.(SubmitModelResult); ok {
			settlements++
		}
	}
	oldRuntime.mu.Unlock()
	if settlements != 1 {
		t.Fatalf("fenced model settlement was committed %d times, want exactly 1 (no replay)", settlements)
	}
}
