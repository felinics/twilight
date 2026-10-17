package loop

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/store"
	"github.com/felinics/twilight/sdk"
)

type blockedSettlement struct {
	store.TurnStore
	release <-chan struct{}
}

func (s blockedSettlement) Commit(ctx context.Context, _ store.CommitRequest) (store.CommitResult, error) {
	select {
	case <-ctx.Done():
		return store.CommitResult{}, ctx.Err()
	case <-s.release:
		return store.CommitResult{}, errors.New("settlement test released")
	}
}

func TestSettlementDeadlinePreservesOutcomeForRetry(t *testing.T) {
	turns, w := loopRuntime(t)
	lp, err := newLoop(t, nil, fakeCatalog{&fakeInvoker{results: []sdk.ModelResult{textResult("durable result")}}},
		fakeToolCatalog{}, staticBuilder{}, Settings{}, false)
	if err != nil {
		t.Fatal(err)
	}
	bound := turns.Bind(w)
	step, err := lp.Advance(context.Background(), bound, "run-1", nil)
	if err != nil || len(step.Dispatched) != 1 {
		t.Fatalf("advance=%+v error=%v", step, err)
	}
	control, stop := context.WithTimeout(context.Background(), 3*time.Second)
	defer stop()
	observer := &effect.Watcher{Port: lp.Executor, Poll: time.Millisecond}
	defer observer.Close()
	out, err := observer.Await(control, step.Dispatched[0])
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	defer close(release)
	ctx, cancel := context.WithTimeout(control, 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := lp.Deliver(ctx, blockedSettlement{TurnStore: bound, release: release}, out, nil)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("settlement error=%v", err)
		}
	case <-control.Done():
		t.Fatal("settlement detached its RPC deadline and pinned the writer")
	}
	if _, err := lp.Executor.GetOutcome(control, step.Dispatched[0]); err != nil {
		t.Fatalf("unsettled outcome was collected: %v", err)
	}
	settled, err := lp.Deliver(control, bound, out, nil)
	if err != nil || settled.Disposition != LoopFinished {
		t.Fatalf("replacement delivery=%+v error=%v", settled, err)
	}
}
