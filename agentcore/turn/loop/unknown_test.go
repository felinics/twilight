package loop_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/loop"
	"github.com/felinics/twilight/agentcore/turn/plan"
)

func TestUnknownToolOutcomeContinues(t *testing.T) {
	f := newFeature(t)
	f.Unknown("echo")
	f.Model(ToolCalls("echo", "c1"), Text("recovered"))
	f.Run()
	f.RequireCompleted("recovered")
	f.RequireCallFailed("c1", turn.ToolOutcomeUnknown)
	f.RequireFailureClass(turn.FailureEffectUnknown)
}

func TestUnknownToolOutcomeLeavesSiblingRunning(t *testing.T) {
	f := newFeature(t)
	f.Unknown("lost")
	f.Tool("echo", turn.DirectExecution)
	f.Model(Calls(Call("lost", "c1"), Call("echo", "c2")), Text("done"))
	f.Run()
	f.RequireCompleted("done")
	f.RequireRan("lost")
	f.RequireRan("echo")
	f.RequireCallFailed("c1", turn.ToolOutcomeUnknown)
	f.RequireBuilderSawTool("c2", `{"x":1}`)
}

// refusingPort answers the first n tool dispatches with a retryable refusal
// and forwards the rest.
type refusingPort struct {
	effect.ExecutionPort
	mu      sync.Mutex
	refuse  int
	refused int
}

func (p *refusingPort) Dispatch(ctx context.Context, a loop.Assignment) error {
	p.mu.Lock()
	if a.Kind() == loop.AssignmentTool && p.refused < p.refuse {
		p.refused++
		p.mu.Unlock()
		return fmt.Errorf("%w: store unavailable", effect.ErrDispatchRetryable)
	}
	p.mu.Unlock()
	return p.ExecutionPort.Dispatch(ctx, a)
}

// Settlements keeps the wrapped port's notification capability visible
// through the wrapper, so the Loop's Watcher does not fall back to polling.
func (p *refusingPort) Settlements(ctx context.Context, epoch string, after uint64, fn func(effect.Settlement) bool) error {
	if s, ok := p.ExecutionPort.(effect.SettlementPort); ok {
		return s.Settlements(ctx, epoch, after, fn)
	}
	<-ctx.Done()
	return ctx.Err()
}

// Retryable refusal is retried within Advance's bound. Exhausting that local
// budget preserves durable intent: a future pass offers the same assignment,
// rather than manufacturing a Known failure or a fresh external effect.
func TestRetryableDispatchRefusal(t *testing.T) {
	for _, tc := range []struct {
		name   string
		refuse int
		ran    bool
	}{{"refused twice then accepted", 2, true}, {"refused past the bound", 5, false}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFeature(t)
			port := &refusingPort{refuse: tc.refuse}
			f.wrapPort = func(inner effect.ExecutionPort) effect.ExecutionPort { port.ExecutionPort = inner; return port }
			f.Tool("echo", turn.DirectExecution)
			f.Model(ToolCalls("echo", "c1"), Text("done"))
			if tc.ran {
				f.Run()
				f.RequireCompleted("done")
				f.RequireRan("echo")
				return
			}
			f.RunError(effect.ErrDispatchRetryable)
			snapshot := f.load()
			targets := plan.RecoveryTargets(&snapshot.State)
			if len(targets) != 1 {
				t.Fatalf("temporary refusal lost durable intent: %+v", snapshot.State)
			}
			key := effect.AssignmentKey{Session: f.rt.Scope(), OwnerKind: effect.OwnerTurn, OwnerID: string(f.turnID), Effect: targets[0].Effect}
			if err := f.loop.Redispatch(f.ctx, f.rt, key); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(f.ctx, 3*time.Second)
			defer cancel()
			observer := &effect.Watcher{Port: port, Poll: time.Millisecond}
			defer observer.Close()
			outcome, err := observer.Await(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.loop.Deliver(ctx, f.rt, outcome, nil); err != nil {
				t.Fatal(err)
			}
			f.Run()
			f.RequireCompleted("done")
			f.RequireRan("echo")
		})
	}
}
