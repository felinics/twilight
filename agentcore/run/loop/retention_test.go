package loop

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/executor/local"
	. "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/sdk"
)

// echoExecutor is a local.LocalExecutor over one tool that returns its arguments.
func echoExecutor(t *testing.T) (*local.LocalExecutor, ToolSpec) {
	t.Helper()
	tool := &fakeTool{ref: "echo", def: toolDef("echo"), policy: DirectExecution,
		execute: func(_ context.Context, req local.ToolExecutionRequest) effect.ToolExecutionOutcome {
			return effect.ToolExecutionSucceeded{Result: ToolExecutionResult{Output: req.Arguments}}
		}}
	exec, err := local.NewLocalExecutor(fakeCatalog{&fakeInvoker{}}, fakeToolCatalog{map[ToolRef]local.ExecutableTool{"echo": tool}}, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	return exec, toolSpec(t, "echo", DirectExecution)
}

// A local.LocalExecutor keeps a bounded number of terminal entries: once more than
// the retained count have closed, the oldest is forgotten and every read of
// its ref reports a missing execution, while the newer ones stay terminal.
func TestLocalExecutorRetainsBoundedOutcomes(t *testing.T) {
	exec, spec := echoExecutor(t)
	exec.SetRetainedOutcomes(2)
	ctx := context.Background()
	var keys []string
	for _, n := range []string{"1", "2", "3"} {
		a := effect.Assignment{Session: testScope, RunID: "run-1", StepID: StepID("step-" + n), CallID: CallID("call-" + n),
			Effect: EffectID("effect-" + n),
			Body:   effect.ToolAssignment{ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest, Arguments: cj(`{}`), Policy: DirectExecution}}
		ref, err := exec.Prepare(ctx, a)
		if err != nil {
			t.Fatal(err)
		}
		if err := exec.Start(ctx, ref, a); err != nil {
			t.Fatal(err)
		}
		if _, err := awaitRef(ctx, exec, ref); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, ref)
	}
	cases := []struct {
		name    string
		key     string
		evicted bool
	}{
		{"oldest", keys[0], true},
		{"middle", keys[1], false},
		{"newest", keys[2], false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := exec.Status(ctx, tc.key)
			if got := errors.Is(err, effect.ErrExecutionNotFound); got != tc.evicted {
				t.Fatalf("status error = %v; evicted = %v, want %v", err, got, tc.evicted)
			}
			if !tc.evicted && err != nil {
				t.Fatal(err)
			}
			_, err = exec.Outcome(ctx, tc.key)
			if got := errors.Is(err, effect.ErrExecutionNotFound); got != tc.evicted {
				t.Fatalf("outcome error = %v; evicted = %v, want %v", err, got, tc.evicted)
			}
			want := effect.AttachmentTerminal
			if tc.evicted {
				want = effect.AttachmentMissing
			}
			if att, err := exec.Attach(ctx, tc.key); err != nil || att.State != want {
				t.Fatalf("attach = %+v %v; want %s", att, err, want)
			}
		})
	}
	// Lowering the bound drops the surplus at once.
	exec.SetRetainedOutcomes(1)
	if att, err := exec.Attach(ctx, keys[1]); err != nil || att.State != effect.AttachmentMissing {
		t.Fatalf("attach after lowering the bound = %+v %v; want missing", att, err)
	}
	if att, err := exec.Attach(ctx, keys[2]); err != nil || att.State != effect.AttachmentTerminal {
		t.Fatalf("attach newest after lowering the bound = %+v %v; want terminal", att, err)
	}
}

// Every entry point releases its slot on return, so the slot table only holds
// the Runs being driven at that moment.
func TestLoopReleasesSlots(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name  string
		drive func(t *testing.T) *Loop
	}{
		{"advance", func(t *testing.T) *Loop {
			builder := staticBuilder{}
			l, err := New(effect.PortsOf(newRecordingExecutor()), Settings{})
			if err != nil {
				t.Fatal(err)
			}
			rt, w := loopRuntime(t)
			if _, err := l.Advance(ctx, rt.Bind(w), builder, "run-1"); err != nil {
				t.Fatal(err)
			}
			return l
		}},
		{"deliver", func(t *testing.T) *Loop {
			rt, w := loopRuntime(t)
			exec := newRecordingExecutor()
			l, err := New(effect.PortsOf(exec), Settings{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := l.Advance(ctx, rt.Bind(w), staticBuilder{}, "run-1"); err != nil {
				t.Fatal(err)
			}
			result := mustFreezeResult(t, textResult("done"))
			if _, err := l.Deliver(ctx, rt.Bind(w), effect.Outcome{Key: exec.last().Key(), Result: effect.ModelSucceeded{Result: result}}); err != nil {
				t.Fatal(err)
			}
			return l
		}},
		{"settle", func(t *testing.T) *Loop {
			invoker := &fakeInvoker{results: []sdk.ModelResult{textResult("done")}}
			l, err := newLoop(t, fakeCatalog{invoker}, fakeToolCatalog{}, Settings{}, false)
			if err != nil {
				t.Fatal(err)
			}
			rt, w := loopRuntime(t)
			res, err := settle(ctx, l, awaiting(t, l), rt.Bind(w), staticBuilder{}, "run-1")
			if err != nil || res.Result == nil || res.Result.Status != RunCompleted {
				t.Fatalf("run = %+v %v", res, err)
			}
			return l
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := tc.drive(t)
			l.mu.Lock()
			held := len(l.slots)
			l.mu.Unlock()
			if held != 0 {
				t.Fatalf("slots held after return = %d, want 0", held)
			}
		})
	}
}
