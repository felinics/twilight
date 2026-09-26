// The feature tests drive the Loop end to end against the reference RunStore.
//
// A Feature owns one in-process Runtime (a Memory Session with the run module
// and its SessionRunStore) and, when Run is called, one Loop. Tests name
// protocol features and speak in Tool/Model/Run/RunError/Approve/Require*.
// Digest, envelope, revision, and derived effects stay inside the driver.

package loop_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/plan"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/session/run/runmodtest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
	"testing"
)

const (
	defaultRunID                     = "run-1"
	defaultModel                     = "m-1"
	defaultSession session.SessionID = "s-1"
)

// newRuntime assembles the Memory Session stack with only the run module and
// creates the Run with its seed input through a Start-like group.
func newRuntime(t testing.TB, inputs ...run.AgentInput) (*runmod.SessionRunStore, writer.Writer) {
	t.Helper()
	store := filestoretest.Store(t)
	registry, err := extension.BuildRegistry(runmod.Module)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: defaultSession}); err != nil {
		t.Fatal(err)
	}
	bindings, ledger := artifacttest.Stores(t)
	writers := writer.NewWriters(store, registry, writer.Admission{Bindings: bindings, Ledger: ledger}, session.OpenOptions{}, writer.WritersConfig{})
	rt, err := runmod.NewSessionRunStore(runmod.Config{Registry: registry, Store: store, Frozen: runmodtest.Frozen(t, bindings)})
	if err != nil {
		t.Fatal(err)
	}
	newRun, err := run.BuildNewRun(defaultRunID, "")
	if err != nil {
		t.Fatal(err)
	}
	facts, err := schema.Machine().CreateGroup(newRun, inputs)
	if err != nil {
		t.Fatal(err)
	}
	runEvents := make([]writer.TypedEvent, 0, len(facts))
	for _, f := range facts {
		runEvents = append(runEvents, writer.TypedEvent{Type: runmod.EventType(f), Value: runmod.Event{RunID: defaultRunID, Fact: f}})
	}
	group := &writer.SemanticGroup{CommitID: "create/" + defaultRunID,
		Batches: []writer.TypedBatch{{Stream: runmod.Stream(defaultRunID), Events: runEvents}}}
	w, err := writers.Writer(ctx, defaultSession)
	if err != nil {
		t.Fatal(err)
	}
	res, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) { return group, nil })
	if err != nil {
		t.Fatal(err)
	}
	if res.Outcome != writer.CommitApplied {
		t.Fatalf("create run: %s %s", res.Outcome, res.Detail)
	}
	return rt, w
}

// Feature is one seeded Run plus the Loop/Runtime used to drive it.
type Feature struct {
	t      testing.TB
	ctx    context.Context
	runCtx context.Context
	runID  run.RunID
	runs   *runmod.SessionRunStore
	rt     runtime.RunStore // runs bound to w
	w      writer.Writer    // the owner's capability over defaultSession

	model   run.ModelRef
	results []sdk.ModelResult
	specs   []run.ToolSpec
	defs    map[run.ToolRef]sdk.ToolDefinition // provider bodies behind specs; ToolSpec keeps only the digest
	tools   map[run.ToolRef]*scriptTool
	invoker *scriptInvoker
	// wrapPort decorates the ExecutionPort the Loop drives, for tests that
	// inject dispatch answers between the Loop and the Worker.
	wrapPort func(effect.ExecutionPort) effect.ExecutionPort
	builder  *scriptBuilder
	loop     *loop.Loop
	seq      int

	modelStepID run.StepID
	last        loop.LoopResult
	resolveErr  error
}

// newFeature creates a Runtime, a Run, and the seed input. Configure tools and
// model results before Run or Executing*.
func newFeature(t testing.TB) *Feature {
	t.Helper()
	runs, w := newRuntime(t, run.AgentInput{ID: "seed", Digest: "sha256:seed"})
	f := &Feature{
		t:      t,
		ctx:    context.Background(),
		runCtx: context.Background(),
		runID:  defaultRunID,
		runs:   runs,
		rt:     runs.Bind(w),
		w:      w,
		model:  defaultModel,
		defs:   make(map[run.ToolRef]sdk.ToolDefinition),
		tools:  make(map[run.ToolRef]*scriptTool),
	}
	return f
}

// Tool registers a catalog tool. Default execution echoes the call arguments.
func (f *Feature) Tool(name string, policy run.ResponsePolicy) *Feature {
	f.t.Helper()
	f.guardConfig()
	spec, def := f.mustSpec(name, policy)
	f.specs = append(f.specs, spec)
	f.defs[spec.Ref] = def
	f.tools[spec.Ref] = &scriptTool{
		ref:    spec.Ref,
		def:    def,
		policy: policy,
	}
	return f
}

// Unknown registers a DirectExecution tool whose Execute returns Unknown.
func (f *Feature) Unknown(name string) *Feature {
	f.t.Helper()
	f.Tool(name, run.DirectExecution)
	f.tools[run.ToolRef(name)].unknown = true
	return f
}

// KnownFailure registers a DirectExecution tool whose Execute returns a known failure.
func (f *Feature) KnownFailure(name, class string) *Feature {
	f.t.Helper()
	f.Tool(name, run.DirectExecution)
	f.tools[run.ToolRef(name)].fail = class
	return f
}

// Model sets the scripted provider results, in Generate order.
func (f *Feature) Model(results ...sdk.ModelResult) *Feature {
	f.t.Helper()
	f.guardConfig()
	f.results = append(f.results, results...)
	return f
}

// ModelResolveError makes the Loop catalog Resolve return err.
func (f *Feature) ModelResolveError(err error) *Feature {
	f.t.Helper()
	f.guardConfig()
	f.resolveErr = err
	return f
}

// Context sets the context passed to the next Loop.Run. Load and Commit
// keep using the Feature's background context.
func (f *Feature) Context(ctx context.Context) *Feature {
	f.t.Helper()
	f.runCtx = ctx
	return f
}

// Run interprets executable effects through Loop until it yields or finishes.
// A Loop error fails the test; expected errors use RunError.
func (f *Feature) Run() *Feature {
	f.t.Helper()
	if err := f.drive(); err != nil {
		f.t.Fatalf("Run: %v", err)
	}
	return f
}

// RunError drives Loop and checks the error with errors.Is.
func (f *Feature) RunError(want error) *Feature {
	f.t.Helper()
	if want == nil {
		f.t.Fatal("RunError: nil want")
	}
	err := f.drive()
	if !errors.Is(err, want) {
		f.t.Fatalf("Run error = %v, want %v", err, want)
	}
	return f
}

func (f *Feature) drive() error {
	f.t.Helper()
	f.ensureLoop()
	res, err := f.loop.Run(f.runCtx, f.rt, f.runID, nil)
	f.last = res
	return err
}

// Waiting returns the current ResponseRequest. Tests that submit a
// malformed ingress command use this with TryCommit.
func (f *Feature) Waiting() run.ResponseRequest {
	f.t.Helper()
	return f.waiting()
}

// TryCommit submits cmd and returns the Runtime error. Feature tests use
// this for rejected ingress; happy-path commands go through Approve/Reject.
func (f *Feature) TryCommit(cmd run.AgentCommand) error {
	f.t.Helper()
	snap := f.load()
	cmd = f.withEffect(cmd, snap)
	id := f.commandID(cmd, snap)
	env, err := schema.Wire().Envelope(f.runID, id, cmd)
	if err != nil {
		return err
	}
	_, err = f.rt.Commit(f.ctx, runtime.CommitRequest{Base: snap.Position, Command: env})
	return err
}

// Approve commits ApproveToolCall for the current waiting call.
func (f *Feature) Approve() *Feature {
	f.t.Helper()
	w := f.waiting()
	digest, err := schema.Canonical().DigestToolResponseDecision(w.Kind, run.ResponseDecisionApproved, "")
	if err != nil {
		f.t.Fatal(err)
	}
	f.commit(run.ApproveToolCall{
		StepID: w.StepID, CallID: w.CallID, ResponseID: w.ID, ResponseDigest: digest,
	})
	return f
}

// Reject commits RejectToolCall for the current waiting call.
func (f *Feature) Reject(reason string) *Feature {
	f.t.Helper()
	w := f.waiting()
	digest, err := schema.Canonical().DigestToolResponseDecision(w.Kind, run.ResponseDecisionRejected, reason)
	if err != nil {
		f.t.Fatal(err)
	}
	f.commit(run.RejectToolCall{
		StepID: w.StepID, CallID: w.CallID, ResponseID: w.ID,
		ResponseDigest: digest, Reason: reason,
	})
	return f
}

// Cancel commits CancelRun.
func (f *Feature) Cancel() *Feature {
	f.t.Helper()
	f.commit(run.CancelRun{})
	return f
}

// ExecutingModel leaves the Run on an Executing ModelStep (no Loop).
func (f *Feature) ExecutingModel() *Feature {
	f.t.Helper()
	f.commitPrepare()
	f.commit(run.StartModelExecution{StepID: f.modelStepID})
	return f
}

// callByProvider resolves a provider tool_call_id to the Run's derived CallID
// by scanning every ToolStepOpened committed so far.
func (f *Feature) callByProvider(providerID string) run.CallID {
	f.t.Helper()
	for _, fact := range f.facts() {
		opened, ok := fact.(run.ToolStepOpened)
		if !ok {
			continue
		}
		for _, b := range opened.Calls {
			if b.ProviderCallID == providerID {
				return b.CallID
			}
		}
	}
	f.t.Fatalf("no tool call with provider id %q", providerID)
	return ""
}

// ExecutingTool leaves the named tool call Executing (no Loop). callID is the
// provider-side id the scripted model emits.
func (f *Feature) ExecutingTool(name string, callID run.CallID) *Feature {
	f.t.Helper()
	var spec run.ToolSpec
	found := false
	for _, candidate := range f.specs {
		if candidate.Ref == run.ToolRef(name) {
			spec = candidate
			found = true
			break
		}
	}
	if !found {
		f.t.Fatalf("ExecutingTool: tool %q not registered", name)
	}
	f.ExecutingModel()
	providerID := string(callID)
	callID = schema.Identity().DeriveCallID(f.modelStepID, 0)
	args := run.MustParseCanonicalJSON(`{"x":1}`)
	frozen, err := sdkconv.FreezeModelResult(sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{TotalTokens: 2},
		ToolCalls: []sdk.ToolCall{{
			ToolCallID: providerID, ToolName: string(spec.Ref), Input: sdk.ParseToolArguments(`{"x":1}`),
		}},
	})
	if err != nil {
		f.t.Fatal(err)
	}
	res := f.commit(run.SubmitModelResult{
		StepID: f.modelStepID,
		Result: frozen,
		Calls: []run.ToolCallBinding{{
			CallID: callID, ProviderCallID: providerID, ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest,
			Arguments: args, Policy: spec.Policy,
		}},
	})
	ts, ok := res.Snapshot.State.Current.(run.ToolStep)
	if !ok {
		f.t.Fatalf("after model result: %T", res.Snapshot.State.Current)
	}
	f.commit(run.StartToolCall{StepID: ts.Ref().ID, CallID: callID})
	return f
}

func (f *Feature) guardConfig() {
	f.t.Helper()
	if f.loop != nil {
		f.t.Fatal("configure Tool/Model/ModelResolveError before Run")
	}
}

func (f *Feature) ensureLoop() {
	f.t.Helper()
	if f.loop != nil {
		return
	}
	f.invoker = &scriptInvoker{results: f.results}
	f.builder = &scriptBuilder{model: f.model, specs: f.specs, defs: f.defs}
	tools := make(map[run.ToolRef]loop.ExecutableTool, len(f.tools))
	for ref, tool := range f.tools {
		tools[ref] = tool
	}
	backend, err := loop.NewLocalExecutor(scriptCatalog{invoker: f.invoker, err: f.resolveErr}, scriptToolCatalog{tools}, nil, false)
	if err != nil {
		f.t.Fatal(err)
	}
	exec, err := executor.NewWorker(f.ctx, storetest.NewMap(nil), []executor.Route{executor.Default("local", backend)})
	if err != nil {
		f.t.Fatal(err)
	}
	var port effect.ExecutionPort = exec
	if f.wrapPort != nil {
		port = f.wrapPort(port)
	}
	l, err := loop.New(port, f.builder, loop.Settings{})
	if err != nil {
		f.t.Fatal(err)
	}
	f.loop = l
}

func (f *Feature) load() runtime.Snapshot {
	f.t.Helper()
	snap, err := f.rt.Load(f.ctx, f.runID)
	if err != nil {
		f.t.Fatal(err)
	}
	return snap
}

func (f *Feature) state() run.MachineState {
	f.t.Helper()
	return f.load().State
}

func (f *Feature) waiting() run.ResponseRequest {
	f.t.Helper()
	reqs := plan.WaitingCalls(f.state())
	if len(reqs) == 0 {
		f.t.Fatal("no waiting call")
	}
	return reqs[0]
}

func (f *Feature) commit(cmd run.AgentCommand) runtime.CommitResult {
	f.t.Helper()
	snap := f.load()
	cmd = f.withEffect(cmd, snap)
	id := f.commandID(cmd, snap)
	env, err := schema.Wire().Envelope(f.runID, id, cmd)
	if err != nil {
		f.t.Fatal(err)
	}
	res, err := f.rt.Commit(f.ctx, runtime.CommitRequest{
		Base: snap.Position, Command: env,
	})
	if err != nil {
		f.t.Fatalf("commit %T: %v", cmd, err)
	}
	return res
}

func (f *Feature) commandID(cmd run.AgentCommand, snap runtime.Snapshot) run.CommandID {
	switch c := cmd.(type) {
	case run.AcceptInput:
		return schema.Identity().DeriveInputCommandID(f.runID, c.InputIDs()...)
	case run.ApproveToolCall:
		return schema.Identity().DeriveResponseCommandID(f.runID, c.StepID, c.CallID, c.ResponseID)
	case run.RejectToolCall:
		return schema.Identity().DeriveResponseCommandID(f.runID, c.StepID, c.CallID, c.ResponseID)
	case run.SubmitToolResponse:
		return schema.Identity().DeriveResponseCommandID(f.runID, c.StepID, c.CallID, c.ResponseID)
	case run.PrepareModelRequest:
		return schema.Identity().DeriveModelRequestCommandID(f.runID, snap.Position)
	case run.RecoverModelExecution:
		return schema.Identity().DeriveRecoveryCommandID(c.Effect)
	case run.StartModelExecution:
		return schema.Identity().DeriveStartCommandID(c.Effect)
	case run.StartToolCall:
		return schema.Identity().DeriveStartCommandID(c.Effect)
	case run.SubmitModelResult:
		return schema.Identity().DeriveSettlementCommandID(c.Effect)
	case run.SubmitModelFailure:
		return schema.Identity().DeriveSettlementCommandID(c.Effect)
	case run.RejectModelResult:
		return schema.Identity().DeriveSettlementCommandID(c.Effect)
	case run.SubmitToolResult:
		return schema.Identity().DeriveSettlementCommandID(c.Effect)
	case run.SubmitToolFailure:
		return schema.Identity().DeriveSettlementCommandID(c.Effect)
	case run.DeclineToolCall:
		return schema.Identity().DeriveDeclineCommandID(f.runID, c.StepID, c.CallID)
	default:
		f.seq++
		return run.CommandID(fmt.Sprintf("cmd-%d", f.seq))
	}
}

func (f *Feature) commitPrepare() {
	f.t.Helper()
	snap := f.load()
	req := sdk.Request{Model: string(f.model), Messages: []sdk.Message{sdk.UserMessage("go")}}
	for _, spec := range f.specs {
		req.Tools = append(req.Tools, f.defs[spec.Ref])
	}
	frozen, err := sdkconv.FreezeModelRequest(req)
	if err != nil {
		f.t.Fatal(err)
	}
	reqDigest, err := schema.Canonical().DigestRequest(frozen)
	if err != nil {
		f.t.Fatal(err)
	}
	cmdID := schema.Identity().DeriveModelRequestCommandID(f.runID, snap.Position)
	stepID := schema.Identity().DeriveModelStepID(f.runID, cmdID)
	ids := make([]run.InputID, len(snap.State.PendingInputs))
	for i, in := range snap.State.PendingInputs {
		ids[i] = in.ID
	}
	f.modelStepID = stepID
	f.commit(run.PrepareModelRequest{
		StepID: stepID, Model: f.model, Request: frozen,
		RequestDigest: reqDigest, InputIDs: ids, Tools: f.specs,
	})
}

// mustSpec returns the agent-side spec and the provider definition it digests.
func (f *Feature) mustSpec(name string, policy run.ResponsePolicy) (run.ToolSpec, sdk.ToolDefinition) {
	f.t.Helper()
	def := sdk.ToolDefinition{Name: name, Parameters: &jsonschema.Schema{Type: "object"}}
	frozen, err := sdkconv.FreezeToolDefinition(def)
	if err != nil {
		f.t.Fatal(err)
	}
	d, err := schema.Canonical().DigestToolDefinition(frozen)
	if err != nil {
		f.t.Fatal(err)
	}
	return run.ToolSpec{Ref: run.ToolRef(name), Name: name, DefinitionDigest: d, Policy: policy}, def
}

func (f *Feature) facts() []run.Fact {
	f.t.Helper()
	record, err := f.runs.Record(f.ctx, defaultSession, f.runID)
	if err != nil {
		f.t.Fatal(err)
	}
	return record.Facts
}

// withEffect fills the derived effect identity of a start or recovery that
// names none (RUN-WIR-1): the model effect from the current step's rejected
// count, the tool effect from the call, the recovered effect from the step.
func (f *Feature) withEffect(cmd run.AgentCommand, snap runtime.Snapshot) run.AgentCommand {
	switch c := cmd.(type) {
	case run.StartModelExecution:
		if c.Effect == "" {
			ms, _ := snap.State.Current.(run.ModelStep)
			c.Effect = schema.Identity().DeriveEffectID(f.runID, c.StepID, "", ms.Rejects)
		}
		return c
	case run.StartToolCall:
		if c.Effect == "" {
			c.Effect = schema.Identity().DeriveEffectID(f.runID, c.StepID, c.CallID, 0)
		}
		return c
	case run.RecoverModelExecution:
		if c.Effect == "" {
			c.Effect = executingModelEffect(snap)
		}
		return c
	case run.SubmitModelResult:
		if c.Effect == "" {
			c.Effect = executingModelEffect(snap)
		}
		return c
	case run.SubmitModelFailure:
		if c.Effect == "" {
			c.Effect = executingModelEffect(snap)
		}
		return c
	case run.RejectModelResult:
		if c.Effect == "" {
			c.Effect = executingModelEffect(snap)
		}
		return c
	case run.SubmitToolResult:
		if c.Effect == "" {
			c.Effect = executingToolEffect(snap, c.CallID)
		}
		return c
	case run.SubmitToolFailure:
		if c.Effect == "" {
			c.Effect = executingToolEffect(snap, c.CallID)
		}
		return c
	default:
		return cmd
	}
}

// executingModelEffect is the effect the current ModelStep is executing.
func executingModelEffect(snap runtime.Snapshot) run.EffectID {
	ms, _ := snap.State.Current.(run.ModelStep)
	return ms.Effect
}

// executingToolEffect is the effect the named call of the current ToolStep
// is executing.
func executingToolEffect(snap runtime.Snapshot, callID run.CallID) run.EffectID {
	ts, _ := snap.State.Current.(run.ToolStep)
	for _, call := range ts.Calls {
		if call.CallID == callID {
			return call.Effect
		}
	}
	return ""
}
