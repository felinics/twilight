package loop

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	. "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/store"
)

// commitLog wraps a bound RunStore and records every AgentCommand it is
// asked to commit, so a test can assert what a fenced Loop still tried to
// write.
type commitLog struct {
	store.RunStore
	mu   sync.Mutex
	cmds []AgentCommand
}

func (c *commitLog) Commit(ctx context.Context, req store.CommitRequest) (store.CommitResult, error) {
	c.mu.Lock()
	c.cmds = append(c.cmds, req.Command.Command)
	c.mu.Unlock()
	return c.RunStore.Commit(ctx, req)
}

// A model-step settlement fenced by ownership loss is returned as is,
// without the one-shot replay a non-sentinel commit error would get
// (RUN-LOP-5). Cancelling the other effects of the Run is the host's act.
func TestOwnershipLossOnModelSettlementIsNotRetried(t *testing.T) {
	stack := newTestStack(t, nil)
	stack.createRun(t, "run-1", AgentInput{ID: "seed", Digest: inputDigest(`{}`)})
	oldWriter := stack.writer(t) // the superseded owner's capability
	oldRuntime := &commitLog{RunStore: stack.runtime.Bind(oldWriter)}

	invoker := &blockingInvoker{started: make(chan struct{}), release: make(chan struct{})}
	loop, err := newLoop(t, fakeCatalog{invoker}, fakeToolCatalog{nil}, Settings{}, false)
	if err != nil {
		t.Fatal(err)
	}
	watcher := awaiting(t, loop)
	done := make(chan error, 1)
	go func() {
		_, err := settle(context.Background(), loop, watcher, oldRuntime, staticBuilder{}, "run-1")
		done <- err
	}()
	<-invoker.started

	stack.open(t)
	if n, err := recoverRuns(context.Background(), t, stack.runtime, stack.writer(t), &reconcile.Reconciler{Abandon: true}); err != nil || n != 1 {
		t.Fatalf("takeover = %d %v, want 1", n, err)
	}
	close(invoker.release)

	select {
	case err := <-done:
		if !errors.Is(err, store.ErrOwnershipLost) {
			t.Fatalf("loop error = %v, want ownership lost", err)
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
