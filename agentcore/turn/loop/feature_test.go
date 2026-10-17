// The feature tests drive the Loop end to end against the reference TurnStore.
// A Feature owns one in-process Runtime (a Memory Session with the Turn module
// and its SessionTurnStore) and, when Run is called, one Loop. Tests name
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
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset/ref"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/session/writer/writertest"
	"github.com/felinics/twilight/agentcore/testutil/local"
	"github.com/felinics/twilight/agentcore/testutil/sdkconv"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/loop"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
	"github.com/felinics/twilight/agentcore/turn/sessionstore/sessionstoretest"
	"github.com/felinics/twilight/agentcore/turn/store"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
	"testing"
)

const (
	defaultTurnID                    = "run-1"
	defaultModel                     = "m-1"
	defaultSession session.SessionID = "s-1"
)

// newRuntime assembles the Memory Session stack with only the Turn module and
// creates the Turn with its seed input through a Start-like group.
func newRuntime(t testing.TB, inputs ...turn.AgentInput) (*sessionstore.SessionTurnStore, writer.Writer) {
	t.Helper()
	store := filestoretest.Store(t)
	registry, err := module.BuildRegistry(sessionstore.Module)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: defaultSession}); err != nil {
		t.Fatal(err)
	}
	bindings, ledger := artifacttest.Stores(t)
	factory := &writer.Factory{Store: store, Registry: registry, Admission: writer.Admission{Bindings: bindings, Ledger: ledger}}
	rt, err := sessionstore.NewSessionTurnStore(sessionstore.Config{Registry: registry, Store: store, Frozen: sessionstoretest.Frozen(t, bindings)})
	if err != nil {
		t.Fatal(err)
	}
	newTurn, err := turn.BuildNewTurn(defaultTurnID, "", ref.Ref{ID: "test", Digest: "sha256:test"})
	if err != nil {
		t.Fatal(err)
	}
	facts, err := schema.Machine().CreateGroup(newTurn, inputs)
	if err != nil {
		t.Fatal(err)
	}
	runEvents := make([]writer.TypedEvent, 0, len(facts))
	for _, f := range facts {
		runEvents = append(runEvents, writer.TypedEvent{Type: sessionstore.EventType(f), Value: sessionstore.Event{TurnID: defaultTurnID, Fact: f}})
	}
	group := &writer.SemanticGroup{CommitID: "create/" + defaultTurnID,
		Batches: []writer.TypedBatch{{Domain: sessionstore.Stream(defaultTurnID), Events: runEvents}}}
	w, err := writertest.Open(t, ctx, factory, defaultSession, session.OpenOptions{})
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
	turnID turn.TurnID
	turns  *sessionstore.SessionTurnStore
	rt     store.TurnStore // Turn bound to w
	w      writer.Writer   // the owner's capability over defaultSession

	model   turn.ModelRef
	results []sdk.ModelResult
	specs   []turn.ToolSpec
	defs    map[turn.ToolRef]sdk.ToolDefinition // provider bodies behind specs; ToolSpec keeps only the digest
	tools   map[turn.ToolRef]*scriptTool
	invoker *scriptInvoker
	// wrapPort decorates the ExecutionPort the Loop drives, for tests that
	// inject dispatch answers between the Loop and the Worker.
	wrapPort func(effect.ExecutionPort) effect.ExecutionPort
	builder  *scriptBuilder
	loop     *loop.Loop
	seq      int

	modelStepID turn.StepID
	last        loop.LoopResult
	resolveErr  error
}

// newFeature creates a Runtime, a Turn, and the seed input. Configure tools and
// model results before Run or Executing*.
func newFeature(t testing.TB) *Feature {
	t.Helper()
	turns, w := newRuntime(t, turn.AgentInput{ID: "seed", Digest: "sha256:seed"})
	f := &Feature{
		t:      t,
		ctx:    context.Background(),
		runCtx: context.Background(),
		turnID: defaultTurnID,
		turns:  turns,
		rt:     turns.Bind(w),
		w:      w,
		model:  defaultModel,
		defs:   make(map[turn.ToolRef]sdk.ToolDefinition),
		tools:  make(map[turn.ToolRef]*scriptTool),
	}
	return f
}

// Tool registers a catalog tool. Default execution echoes the call arguments.
func (f *Feature) Tool(name string, policy turn.ResponsePolicy) *Feature {
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
	f.Tool(name, turn.DirectExecution)
	f.tools[turn.ToolRef(name)].unknown = true
	return f
}

// KnownFailure registers a DirectExecution tool whose Execute returns a known failure.
func (f *Feature) KnownFailure(name, class string) *Feature {
	f.t.Helper()
	f.Tool(name, turn.DirectExecution)
	f.tools[turn.ToolRef(name)].fail = class
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
	res, err := f.loop.Run(f.runCtx, f.rt, f.turnID, nil)
	f.last = res
	return err
}

// Waiting returns the current ResponseRequest. Tests that submit a
// malformed ingress command use this with TryCommit.
func (f *Feature) Waiting() turn.ResponseRequest {
	f.t.Helper()
	return f.waiting()
}

// TryCommit submits cmd and returns the Runtime error. Feature tests use
// this for rejected ingress; happy-path commands go through Approve/Reject.
func (f *Feature) TryCommit(cmd turn.AgentCommand) error {
	f.t.Helper()
	snap := f.load()
	cmd = f.withEffect(cmd, snap)
	id := f.commandID(cmd, snap)
	env, err := schema.Wire().Envelope(f.turnID, id, cmd)
	if err != nil {
		return err
	}
	_, err = f.rt.Commit(f.ctx, store.CommitRequest{Base: snap.Position, Command: env})
	return err
}

// Approve commits ApproveToolCall for the current waiting call.
func (f *Feature) Approve() *Feature {
	f.t.Helper()
	w := f.waiting()
	digest, err := schema.Canonical().DigestToolResponseDecision(w.Kind, turn.ResponseDecisionApproved, "")
	if err != nil {
		f.t.Fatal(err)
	}
	f.commit(turn.ApproveToolCall{
		StepID: w.StepID, CallID: w.CallID, ResponseID: w.ID, ResponseDigest: digest,
	})
	return f
}

// Reject commits RejectToolCall for the current waiting call.
func (f *Feature) Reject(reason string) *Feature {
	f.t.Helper()
	w := f.waiting()
	digest, err := schema.Canonical().DigestToolResponseDecision(w.Kind, turn.ResponseDecisionRejected, reason)
	if err != nil {
		f.t.Fatal(err)
	}
	f.commit(turn.RejectToolCall{
		StepID: w.StepID, CallID: w.CallID, ResponseID: w.ID,
		ResponseDigest: digest, Reason: reason,
	})
	return f
}

// Cancel commits CancelTurn.
func (f *Feature) Cancel() *Feature {
	f.t.Helper()
	f.commit(turn.CancelTurn{})
	return f
}

// ExecutingModel leaves the Turn on an Executing ModelStep (no Loop).
func (f *Feature) ExecutingModel() *Feature {
	f.t.Helper()
	f.commitPrepare()
	f.commit(turn.StartModelExecution{StepID: f.modelStepID})
	return f
}

// callByProvider resolves a provider tool_call_id to the Turn's derived CallID
// by scanning every ToolStepOpened committed so far.
func (f *Feature) callByProvider(providerID string) turn.CallID {
	f.t.Helper()
	for _, fact := range f.facts() {
		opened, ok := fact.(turn.ToolStepOpened)
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
func (f *Feature) ExecutingTool(name string, callID turn.CallID) *Feature {
	f.t.Helper()
	var spec turn.ToolSpec
	found := false
	for _, candidate := range f.specs {
		if candidate.Ref == turn.ToolRef(name) {
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
	args := turn.MustParseCanonicalJSON(`{"x":1}`)
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
	res := f.commit(turn.SubmitModelResult{
		StepID: f.modelStepID,
		Result: frozen,
		Calls: []turn.ToolCallBinding{{
			CallID: callID, ProviderCallID: providerID, ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest,
			Arguments: args, Policy: spec.Policy,
		}},
	})
	ts, ok := res.Snapshot.State.Current.(turn.ToolStep)
	if !ok {
		f.t.Fatalf("after model result: %T", res.Snapshot.State.Current)
	}
	f.commit(turn.StartToolCall{StepID: ts.Ref().ID, CallID: callID})
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
	tools := make(map[turn.ToolRef]local.ExecutableTool, len(f.tools))
	for ref, tool := range f.tools {
		tools[ref] = tool
	}
	backend, err := local.NewLocalExecutor(scriptCatalog{invoker: f.invoker, err: f.resolveErr}, scriptToolCatalog{tools}, nil, false)
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

func (f *Feature) load() store.Snapshot {
	f.t.Helper()
	snap, err := f.rt.Load(f.ctx, f.turnID)
	if err != nil {
		f.t.Fatal(err)
	}
	return snap
}

func (f *Feature) state() turn.MachineState {
	f.t.Helper()
	return f.load().State
}

func (f *Feature) waiting() turn.ResponseRequest {
	f.t.Helper()
	reqs := turn.WaitingCalls(f.state())
	if len(reqs) == 0 {
		f.t.Fatal("no waiting call")
	}
	return reqs[0]
}

func (f *Feature) commit(cmd turn.AgentCommand) store.CommitResult {
	f.t.Helper()
	snap := f.load()
	cmd = f.withEffect(cmd, snap)
	id := f.commandID(cmd, snap)
	env, err := schema.Wire().Envelope(f.turnID, id, cmd)
	if err != nil {
		f.t.Fatal(err)
	}
	res, err := f.rt.Commit(f.ctx, store.CommitRequest{
		Base: snap.Position, Command: env,
	})
	if err != nil {
		f.t.Fatalf("commit %T: %v", cmd, err)
	}
	return res
}

func (f *Feature) commandID(cmd turn.AgentCommand, snap store.Snapshot) turn.CommandID {
	switch c := cmd.(type) {
	case turn.AcceptInput:
		return schema.Identity().DeriveInputCommandID(f.turnID, c.InputIDs()...)
	case turn.ApproveToolCall:
		return schema.Identity().DeriveResponseCommandID(f.turnID, c.StepID, c.CallID, c.ResponseID)
	case turn.RejectToolCall:
		return schema.Identity().DeriveResponseCommandID(f.turnID, c.StepID, c.CallID, c.ResponseID)
	case turn.SubmitToolResponse:
		return schema.Identity().DeriveResponseCommandID(f.turnID, c.StepID, c.CallID, c.ResponseID)
	case turn.PrepareModelRequest:
		return schema.Identity().DeriveModelRequestCommandID(f.turnID, snap.Position)
	case turn.RecoverModelExecution:
		return schema.Identity().DeriveRecoveryCommandID(c.Effect)
	case turn.StartModelExecution:
		return schema.Identity().DeriveStartCommandID(c.Effect)
	case turn.StartToolCall:
		return schema.Identity().DeriveStartCommandID(c.Effect)
	case turn.SubmitModelResult:
		return schema.Identity().DeriveSettlementCommandID(c.Effect)
	case turn.SubmitModelFailure:
		return schema.Identity().DeriveSettlementCommandID(c.Effect)
	case turn.RejectModelResult:
		return schema.Identity().DeriveSettlementCommandID(c.Effect)
	case turn.SubmitToolResult:
		return schema.Identity().DeriveSettlementCommandID(c.Effect)
	case turn.SubmitToolFailure:
		return schema.Identity().DeriveSettlementCommandID(c.Effect)
	case turn.DeclineToolCall:
		return schema.Identity().DeriveDeclineCommandID(f.turnID, c.StepID, c.CallID)
	default:
		f.seq++
		return turn.CommandID(fmt.Sprintf("cmd-%d", f.seq))
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
	cmdID := schema.Identity().DeriveModelRequestCommandID(f.turnID, snap.Position)
	stepID := schema.Identity().DeriveModelStepID(f.turnID, cmdID)
	ids := make([]turn.InputID, len(snap.State.PendingInputs))
	for i, in := range snap.State.PendingInputs {
		ids[i] = in.ID
	}
	f.modelStepID = stepID
	f.commit(turn.PrepareModelRequest{
		StepID: stepID, Model: f.model, Request: frozen,
		RequestDigest: reqDigest, InputIDs: ids, Tools: f.specs,
	})
}

// mustSpec returns the agent-side spec and the provider definition it digests.
func (f *Feature) mustSpec(name string, policy turn.ResponsePolicy) (turn.ToolSpec, sdk.ToolDefinition) {
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
	return turn.ToolSpec{Ref: turn.ToolRef(name), Name: name, DefinitionDigest: d, Policy: policy}, def
}

func (f *Feature) facts() []turn.Fact {
	f.t.Helper()
	record, err := f.turns.Record(f.ctx, defaultSession, f.turnID)
	if err != nil {
		f.t.Fatal(err)
	}
	return record.Facts
}

// withEffect fills the derived effect identity of a start or recovery that
// names none: the model effect from the current step's rejected
// count, the tool effect from the call, the recovered effect from the step.
func (f *Feature) withEffect(cmd turn.AgentCommand, snap store.Snapshot) turn.AgentCommand {
	switch c := cmd.(type) {
	case turn.StartModelExecution:
		if c.Effect == "" {
			ms, _ := snap.State.Current.(turn.ModelStep)
			c.Effect = schema.Identity().DeriveEffectID(f.turnID, c.StepID, "", ms.Rejects)
		}
		return c
	case turn.StartToolCall:
		if c.Effect == "" {
			c.Effect = schema.Identity().DeriveEffectID(f.turnID, c.StepID, c.CallID, 0)
		}
		return c
	case turn.RecoverModelExecution:
		if c.Effect == "" {
			c.Effect = executingModelEffect(snap)
		}
		return c
	case turn.SubmitModelResult:
		if c.Effect == "" {
			c.Effect = executingModelEffect(snap)
		}
		return c
	case turn.SubmitModelFailure:
		if c.Effect == "" {
			c.Effect = executingModelEffect(snap)
		}
		return c
	case turn.RejectModelResult:
		if c.Effect == "" {
			c.Effect = executingModelEffect(snap)
		}
		return c
	case turn.SubmitToolResult:
		if c.Effect == "" {
			c.Effect = executingToolEffect(snap, c.CallID)
		}
		return c
	case turn.SubmitToolFailure:
		if c.Effect == "" {
			c.Effect = executingToolEffect(snap, c.CallID)
		}
		return c
	default:
		return cmd
	}
}

// executingModelEffect is the effect the current ModelStep is executing.
func executingModelEffect(snap store.Snapshot) turn.EffectID {
	ms, _ := snap.State.Current.(turn.ModelStep)
	return ms.Effect
}

// executingToolEffect is the effect the named call of the current ToolStep
// is executing.
func executingToolEffect(snap store.Snapshot, callID turn.CallID) turn.EffectID {
	ts, _ := snap.State.Current.(turn.ToolStep)
	for _, call := range ts.Calls {
		if call.CallID == callID {
			return call.Effect
		}
	}
	return ""
}
