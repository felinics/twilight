package executor_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/turn/effect"
)

type unclosedProgressBackend struct {
	*testBackend
	streams atomic.Int32
}

func (b *unclosedProgressBackend) Progress(ctx context.Context, _ effect.AssignmentKey, _ uint64, _ func(effect.ProgressFrame) bool) error {
	b.streams.Add(1)
	<-ctx.Done()
	return ctx.Err()
}

func TestWorkerProgressAfterAnotherReplicaSettles(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	records := storetest.NewMap(nil)
	backend := &unclosedProgressBackend{testBackend: newTestBackend()}
	hub := executor.NewProgressHub(0)
	a := testAssignment()
	hub.Publish(ctx, effect.ProgressFrame{Key: a.Key(), Kind: effect.ProgressTextDelta, Payload: []byte(`"buffered"`)})
	owner, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{ID: "executing-worker", Progress: hub})
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	other, err := executor.NewWorker(ctx, records, routes(backend), executor.WorkerOptions{ID: "observing-worker"})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if err := owner.Dispatch(ctx, a); err != nil {
		t.Fatal(err)
	}
	if _, err := awaitOutcome(ctx, owner, a.Key()); err != nil {
		t.Fatal(err)
	}
	if err := other.Progress(ctx, a.Key(), 0, func(effect.ProgressFrame) bool {
		t.Fatal("another replica has no buffered frames")
		return false
	}); err != nil {
		t.Fatalf("terminal progress on other replica = %v", err)
	}
	if n := backend.streams.Load(); n != 0 {
		t.Fatalf("terminal execution opened %d backend streams", n)
	}
	var frames []effect.ProgressFrame
	if err := owner.Progress(ctx, a.Key(), 0, func(f effect.ProgressFrame) bool {
		frames = append(frames, f)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(frames) != 2 || frames[0].Kind != effect.ProgressTextDelta || frames[1].Kind != effect.ProgressEnd {
		t.Fatalf("local buffered progress lost: %+v", frames)
	}
}
