package loop

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	. "github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/run/reconcile"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/session/writer"

	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// --- fakes ---

type fakeInvoker struct {
	results []sdk.ModelResult
	calls   atomic.Int32
}

func (f *fakeInvoker) Generate(ctx context.Context, req sdk.Request) (sdk.ModelResult, error) {
	if err := ctx.Err(); err != nil {
		return sdk.ModelResult{}, err
	}
	n := int(f.calls.Add(1)) - 1
	if n >= len(f.results) {
		return sdk.ModelResult{}, errors.New("fake: no scripted result")
	}
	return f.results[n], nil
}

type blockingInvoker struct {
	started chan struct{}
	release chan struct{}
}

func (b *blockingInvoker) Generate(context.Context, sdk.Request) (sdk.ModelResult, error) {
	close(b.started)
	<-b.release
	return textResult("done"), nil
}

type fakeCatalog struct{ invoker ModelInvoker }

func (c fakeCatalog) ResolveModel(ModelRef) (ModelInvoker, error) { return c.invoker, nil }

type fakeTool struct {
	ref       ToolRef
	def       sdk.ToolDefinition
	policy    ResponsePolicy
	execute   func(context.Context, ToolExecutionRequest) ToolExecutionOutcome
	valErr    error
	replay    ReplayPolicy
	placement ToolPlacement
}

func (f *fakeTool) Ref() ToolRef                          { return f.ref }
func (f *fakeTool) Definition() sdk.ToolDefinition        { return f.def }
func (f *fakeTool) ResponsePolicy() ResponsePolicy        { return f.policy }
func (f *fakeTool) Replay() ReplayPolicy                  { return f.replay }
func (f *fakeTool) Placement() ToolPlacement              { return f.placement }
func (f *fakeTool) ValidateArguments(CanonicalJSON) error { return f.valErr }
func (f *fakeTool) Execute(ctx context.Context, req ToolExecutionRequest) ToolExecutionOutcome {
	return f.execute(ctx, req)
}

type fakeToolCatalog struct{ tools map[ToolRef]ExecutableTool }

func (c fakeToolCatalog) ResolveTool(ref ToolRef) (ExecutableTool, error) {
	t, ok := c.tools[ref]
	if !ok {
		return nil, fmt.Errorf("unknown tool %q", ref)
	}
	return t, nil
}

// staticBuilder freezes one request per Plan call; tools mirror the catalog.
type staticBuilder struct {
	model ModelRef
	specs []ToolSpec
}

func (p staticBuilder) Build(_ context.Context, hint plan.PromptInput) (Prompt, error) {
	model := p.model
	if model == "" {
		model = testModel
	}
	req := sdk.Request{Model: string(model), Messages: []sdk.Message{sdk.UserMessage("go")}}
	for _, s := range p.specs {
		req.Tools = append(req.Tools, toolDef(s.Name))
	}
	ids := make([]InputID, len(hint.Inputs))
	for i, in := range hint.Inputs {
		ids[i] = in.ID
	}
	return Prompt{Model: model, Request: req, InputIDs: ids, Tools: p.specs}, nil
}

// toolDef is the provider definition every test tool shares; ToolSpec keeps
// only its digest, so tests rebuild the body from the name.
func toolDef(name string) sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: name, Parameters: &jsonschema.Schema{Type: "object"}}
}

func toolSpec(t *testing.T, name string, policy ResponsePolicy) ToolSpec {
	t.Helper()
	frozen, err := sdkconv.FreezeToolDefinition(toolDef(name))
	if err != nil {
		t.Fatal(err)
	}
	d, err := schema.Canonical().DigestToolDefinition(frozen)
	if err != nil {
		t.Fatal(err)
	}
	return ToolSpec{Ref: ToolRef(name), Name: name, DefinitionDigest: d, Policy: policy}
}

func textResult(text string) sdk.ModelResult {
	return sdk.ModelResult{Text: text, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}
}

func toolCallResult(ids ...string) sdk.ModelResult {
	r := sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 2}}
	for _, id := range ids {
		r.ToolCalls = append(r.ToolCalls, sdk.ToolCall{ToolCallID: id, ToolName: "echo", Input: sdk.ParseToolArguments(`{"x":1}`)})
	}
	return r
}

// --- tests ---

func TestNewLeavesEmptySchedulingMode(t *testing.T) {
	loop, err := newLoop(t, nil, fakeCatalog{&fakeInvoker{}}, fakeToolCatalog{}, staticBuilder{}, Settings{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if loop.Settings.Scheduling.Mode != "" {
		t.Fatalf("scheduling mode = %q, want empty (parallel by default at freeze time)", loop.Settings.Scheduling.Mode)
	}
}

func TestLoopRejectsConcurrentRunForSameID(t *testing.T) {
	rt, w := loopRuntime(t)
	invoker := &blockingInvoker{started: make(chan struct{}), release: make(chan struct{})}
	loop, err := newLoop(t, nil, fakeCatalog{invoker}, fakeToolCatalog{}, staticBuilder{}, Settings{}, false)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, runErr := loop.Run(context.Background(), rt.Bind(w), "run-1", nil)
		done <- runErr
	}()
	<-invoker.started
	if _, err := loop.Run(context.Background(), rt.Bind(w), "run-1", nil); !errors.Is(err, ErrRunAlreadyRunning) {
		t.Fatalf("concurrent Run error = %v, want ErrRunAlreadyRunning", err)
	}
	close(invoker.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type errCatalog struct{ err error }

func (c errCatalog) ResolveModel(ModelRef) (ModelInvoker, error) { return nil, c.err }

func TestLoopModelCatalogErrorRecoversWithFreshLoop(t *testing.T) {
	rt, w := loopRuntime(t)
	missing := errors.New("missing provider")
	broken, err := newLoop(t, nil, errCatalog{missing}, fakeToolCatalog{}, staticBuilder{}, Settings{}, false)
	if err != nil {
		t.Fatal(err)
	}
	_, err = broken.Run(context.Background(), rt.Bind(w), "run-1", nil)
	if !errors.Is(err, ErrModelUnavailable) || !strings.Contains(err.Error(), missing.Error()) {
		t.Fatalf("err = %v, want ErrModelUnavailable carrying %q", err, missing)
	}
	snap, loadErr := rt.Bind(w).Load(context.Background(), "run-1")
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if snap.State.Status != RunActive {
		t.Fatalf("status = %v", snap.State.Status)
	}
	// Validate caught the missing model before the start barrier: the step is
	// still Prepared and no start or recovery fact was written, so a working
	// Loop starts the same step (RUN-EXE-5, RUN-LOP-3).
	step, isModel := snap.State.Current.(ModelStep)
	if !isModel || step.Status != ModelPrepared {
		t.Fatalf("current = %+v, want the Prepared model step", snap.State.Current)
	}
	for _, f := range recordFacts(t, rt, "run-1") {
		switch f.(type) {
		case ModelStepStarted, ModelStepRecovered:
			t.Fatalf("a missing model wrote %T before the start barrier", f)
		}
	}

	invoker := &fakeInvoker{results: []sdk.ModelResult{textResult("resumed")}}
	ready, err := newLoop(t, nil, fakeCatalog{invoker}, fakeToolCatalog{}, staticBuilder{}, Settings{}, false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := ready.Run(context.Background(), rt.Bind(w), "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result == nil || res.Result.Status != RunCompleted || invoker.calls.Load() != 1 {
		t.Fatalf("res = %+v, model calls = %d", res, invoker.calls.Load())
	}
	final, err := rt.Bind(w).Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if final.State.ModelSteps != 1 {
		t.Fatalf("ModelSteps = %d after resume, want 1", final.State.ModelSteps)
	}
}

func TestLoopParallelBounded(t *testing.T) {
	spec := toolSpec(t, "echo", DirectExecution)
	var concurrent, peak atomic.Int32
	gate := make(chan struct{})
	started := make(chan struct{}, 3)
	echo := &fakeTool{ref: "echo", def: toolDef(spec.Name), policy: DirectExecution,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			cur := concurrent.Add(1)
			for {
				p := peak.Load()
				if cur <= p || peak.CompareAndSwap(p, cur) {
					break
				}
			}
			started <- struct{}{}
			<-gate // hold every worker until released so concurrency is real
			concurrent.Add(-1)
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: cj(`"ok"`)}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1", "c2", "c3"), textResult("done")}}
	rt, w := loopRuntime(t)
	loop, _ := newLoop(t, nil, fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticBuilder{specs: []ToolSpec{spec}}, Settings{Scheduling: ToolScheduling{MaxParallel: 2}}, false)

	done := make(chan struct{})
	var res LoopResult
	var runErr error
	go func() {
		res, runErr = loop.Run(context.Background(), rt.Bind(w), "run-1", nil)
		close(done)
	}()

	// Exactly MaxParallel workers must be running before the gate opens.
	<-started
	<-started
	if concurrent.Load() != 2 {
		t.Fatalf("concurrent = %d before gate, want 2", concurrent.Load())
	}
	close(gate)
	<-done
	if runErr != nil {
		t.Fatal(runErr)
	}
	if res.Result.Status != RunCompleted {
		t.Fatalf("res = %+v", res)
	}
	if peak.Load() != 2 {
		t.Fatalf("peak concurrency = %d, want exactly 2 (bounded and actually parallel)", peak.Load())
	}
}

type staleCommitRuntime struct{ runtime.RunStore }

func (staleCommitRuntime) Commit(context.Context, runtime.CommitRequest) (runtime.CommitResult, error) {
	return runtime.CommitResult{}, ErrStaleRuntime
}

// A stale start rejection is not an error: the Loop returns and the next
// Load decides what the other actor left behind.
func TestToolStartStaleIsNotAnError(t *testing.T) {
	spec := toolSpec(t, "echo", DirectExecution)
	args := cj(`{}`)
	callID := schema.Identity().DeriveCallID("model-1", 0)
	echo := &fakeTool{ref: "echo", def: toolDef(spec.Name), policy: DirectExecution,
		execute: func(context.Context, ToolExecutionRequest) ToolExecutionOutcome {
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: args}}
		}}
	loop, err := newLoop(t, nil, fakeCatalog{&fakeInvoker{}}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticBuilder{}, Settings{}, false)
	if err != nil {
		t.Fatal(err)
	}
	stepID := StepID("step-1")
	snapshot := &runtime.Snapshot{State: MachineState{
		RunID: "run-1", Status: RunActive,
		Current: ToolStep{
			RefValue: StepRef{RunID: "run-1", ID: stepID},
			Source:   "model-1",
			Calls: []ToolCallState{{
				CallID: callID, ProviderCallID: "c1", ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest,
				Arguments: args, Policy: DirectExecution, Status: ToolPending,
			}},
		},
	}, Position: 1}

	rt, w := loopRuntime(t)
	dispatched, err := loop.startToolCalls(context.Background(), staleCommitRuntime{RunStore: rt.Bind(w)}, nil, snapshot,
		plan.StartToolCalls{StepID: stepID, CallIDs: []CallID{callID}})
	if err != nil || len(dispatched) != 0 {
		t.Fatalf("stale start: dispatched=%d err=%v", len(dispatched), err)
	}
}

// responseLossRuntime is a run store whose bound ports lose the response of
// some accepted commits, so a test can see the Loop's one-shot replay.
type responseLossRuntime struct {
	*runmod.SessionRunStore
	mu             sync.Mutex
	count          map[CommandID]int
	loseModelStart bool
}

func newResponseLossRuntime(t *testing.T) (*responseLossRuntime, writer.Writer) {
	t.Helper()
	rt, w := loopRuntime(t)
	return &responseLossRuntime{SessionRunStore: rt, count: make(map[CommandID]int)}, w
}

func (r *responseLossRuntime) Bind(w writer.Writer) runtime.RunStore {
	return lossyStore{RunStore: r.SessionRunStore.Bind(w), r: r}
}

type lossyStore struct {
	runtime.RunStore
	r *responseLossRuntime
}

func (s lossyStore) Commit(ctx context.Context, req runtime.CommitRequest) (runtime.CommitResult, error) {
	r := s.r
	result, err := s.RunStore.Commit(ctx, req)
	if err != nil {
		return result, err
	}
	r.mu.Lock()
	r.count[req.Command.ID]++
	count := r.count[req.Command.ID]
	_, lose := req.Command.Command.(StartModelExecution)
	lose = lose && r.loseModelStart
	if _, ok := req.Command.Command.(SubmitToolResult); ok {
		lose = true
	}
	r.mu.Unlock()
	if lose && count <= 2 {
		return runtime.CommitResult{}, errors.New("test: response lost")
	}
	return result, nil
}

func TestLoopReplaysStartAfterTwoLostResponses(t *testing.T) {
	rt, w := newResponseLossRuntime(t)
	rt.loseModelStart = true
	invoker := &fakeInvoker{results: []sdk.ModelResult{textResult("recovered")}}
	loop, err := newLoop(t, nil, fakeCatalog{invoker}, fakeToolCatalog{}, staticBuilder{}, Settings{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loop.Run(context.Background(), rt.Bind(w), "run-1", nil); err == nil {
		t.Fatal("first run unexpectedly completed after lost start responses")
	}
	snapshot, err := rt.Bind(w).Load(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	// The first Loop reaches the start barrier; both start responses are lost,
	// so the Owner remains Executing while the worker's attempt is gone
	// with the aborted process. A second Run has nothing to execute (RUN-LOP-4).
	if current, ok := snapshot.State.Current.(ModelStep); !ok || current.Status != ModelExecuting {
		t.Fatalf("current = %#v, want Executing ModelStep", snapshot.State.Current)
	}
	res, err := loop.Run(context.Background(), rt.Bind(w), "run-1", nil)
	if err != nil || res.Disposition != LoopWaiting || !res.ExecutionRecovery {
		t.Fatalf("run with an orphaned Executing step = %+v %v, want waiting for recovery", res, err)
	}
	// The owner's takeover disposition withdraws the orphaned step; the next
	// Run plans again and calls the model exactly once (RUN-CMT-7).
	if n, err := rt.RecoverInterrupted(context.Background(), w, &reconcile.Reconciler{Abandon: true}); err != nil || n != 1 {
		t.Fatalf("RecoverInterrupted = %d %v", n, err)
	}
	rt.loseModelStart = false // the transport is healthy again
	if _, err := loop.Run(context.Background(), rt.Bind(w), "run-1", nil); err != nil {
		t.Fatal(err)
	}
	if invoker.calls.Load() != 1 {
		t.Fatalf("model calls = %d, want 1", invoker.calls.Load())
	}
}

func TestLoopReplaysSettlementWithoutRepeatingTool(t *testing.T) {
	rt, w := newResponseLossRuntime(t)
	spec := toolSpec(t, "echo", DirectExecution)
	var executions atomic.Int32
	echo := &fakeTool{ref: "echo", def: toolDef(spec.Name), policy: DirectExecution,
		execute: func(_ context.Context, req ToolExecutionRequest) ToolExecutionOutcome {
			executions.Add(1)
			return ToolExecutionSucceeded{Result: ToolExecutionResult{Output: req.Arguments}}
		}}
	invoker := &fakeInvoker{results: []sdk.ModelResult{toolCallResult("c1"), textResult("done")}}
	loop, err := newLoop(t, nil, fakeCatalog{invoker}, fakeToolCatalog{map[ToolRef]ExecutableTool{"echo": echo}},
		staticBuilder{specs: []ToolSpec{spec}}, Settings{}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := loop.Run(context.Background(), rt.Bind(w), "run-1", nil); err == nil {
		t.Fatal("first run unexpectedly completed after lost settlement responses")
	}
	if executions.Load() != 1 {
		t.Fatalf("tool executions = %d, want 1", executions.Load())
	}
	res, err := loop.Run(context.Background(), rt.Bind(w), "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Disposition != LoopFinished || res.Result == nil || res.Result.Status != RunCompleted {
		t.Fatalf("result = %+v", res)
	}
	if executions.Load() != 1 {
		t.Fatalf("tool executions after replay = %d, want 1", executions.Load())
	}
}

func TestLoopMalformedModelResultDispositionFailsRun(t *testing.T) {
	rt, w := loopRuntime(t)
	// Non-JSON argument text is tolerated (bound raw, fails later as
	// invalid_arguments), but invalid UTF-8 cannot be frozen at all: the
	// result is structurally malformed and goes through RejectModelResult.
	bad := sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		ToolCalls:    []sdk.ToolCall{{ToolCallID: "c1", ToolName: "echo", Input: sdk.ParseToolArguments("\xff\xfe")}},
		Usage:        sdk.Usage{TotalTokens: 1},
	}
	invoker := &fakeInvoker{results: []sdk.ModelResult{bad, bad, bad}}
	loop, err := newLoop(t, nil, fakeCatalog{invoker}, fakeToolCatalog{}, staticBuilder{}, Settings{MalformedRetries: 2}, false)
	if err != nil {
		t.Fatal(err)
	}
	res, err := loop.Run(context.Background(), rt.Bind(w), "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := invoker.calls.Load(); got != 3 {
		t.Fatalf("model calls = %d, want 3", got)
	}
	if res.Result == nil || res.Result.Status != RunFailed || res.Result.Reason != ReasonMalformedModel {
		t.Fatalf("loop result = %+v", res)
	}
}

// cancellingInvoker cancels the outer ctx from inside Generate, simulating a
// shutdown arriving mid-execution.
type cancellingInvoker struct{ cancel context.CancelFunc }

func (c *cancellingInvoker) Generate(ctx context.Context, _ sdk.Request) (sdk.ModelResult, error) {
	c.cancel()
	<-ctx.Done()
	return sdk.ModelResult{}, ctx.Err()
}

func TestLoopMidExecutionCancelRecoversModelStep(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	invoker := &cancellingInvoker{cancel: cancel}
	rt, w := loopRuntime(t)
	loop, _ := newLoop(t, nil, fakeCatalog{invoker}, fakeToolCatalog{}, staticBuilder{}, Settings{}, false)

	_, err := loop.Run(ctx, rt.Bind(w), "run-1", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	// The model step is withdrawn via RecoverModelExecution: the Run is Open,
	// still active, and the aborted step is not counted (RUN-LOP-3).
	snap, _ := rt.Bind(w).Load(context.Background(), "run-1")
	if _, open := snap.State.Current.(Open); !open {
		t.Fatalf("current = %#v, want Open", snap.State.Current)
	}
	if snap.State.ModelSteps != 0 {
		t.Fatalf("ModelSteps = %d", snap.State.ModelSteps)
	}
	recovered := false
	for _, e := range recordFacts(t, rt, "run-1") {
		if _, ok := e.(ModelStepRecovered); ok {
			recovered = true
		}
	}
	if !recovered {
		t.Fatal("no ModelStepRecovered fact committed")
	}

	// A fresh Loop plans a new step; the cancelled one left no count behind.
	invoker2 := &fakeInvoker{results: []sdk.ModelResult{textResult("resumed")}}
	loop2, _ := newLoop(t, nil, fakeCatalog{invoker2}, fakeToolCatalog{}, staticBuilder{}, Settings{}, false)
	res, err := loop2.Run(context.Background(), rt.Bind(w), "run-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Result == nil || res.Result.Status != RunCompleted || invoker2.calls.Load() != 1 {
		t.Fatalf("res = %+v, model calls = %d", res, invoker2.calls.Load())
	}
	final, _ := rt.Bind(w).Load(context.Background(), "run-1")
	if final.State.ModelSteps != 1 {
		t.Fatalf("ModelSteps = %d after resume, want 1 (only the replanned step counts)", final.State.ModelSteps)
	}
}
