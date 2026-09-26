package loop_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
)

func TestUnknownToolOutcomeContinues(t *testing.T) {
	f := newFeature(t)
	f.Unknown("echo")
	f.Model(ToolCalls("echo", "c1"), Text("recovered"))
	f.Run()
	f.RequireCompleted("recovered")
	f.RequireCallFailed("c1", run.ToolOutcomeUnknown)
	f.RequireFailureClass(run.FailureEffectUnknown)
}

func TestUnknownToolOutcomeLeavesSiblingRunning(t *testing.T) {
	f := newFeature(t)
	f.Unknown("lost")
	f.Tool("echo", run.DirectExecution)
	f.Model(Calls(Call("lost", "c1"), Call("echo", "c2")), Text("done"))
	f.Run()
	f.RequireCompleted("done")
	f.RequireRan("lost")
	f.RequireRan("echo")
	f.RequireCallFailed("c1", run.ToolOutcomeUnknown)
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

// RUN-EXE-3: a retryable refusal is dispatched again inside the Advance and
// the call completes; a refusal that persists past the bound is a Known
// dispatch failure of the call, and the Run continues.
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
			f.Tool("echo", run.DirectExecution)
			f.Model(ToolCalls("echo", "c1"), Text("done"))
			f.Run()
			f.RequireCompleted("done")
			if tc.ran {
				f.RequireRan("echo")
				return
			}
			f.RequireCallFailed("c1", run.ToolOutcomeKnown)
			f.RequireFailureClass(run.FailureExecution)
		})
	}
}
