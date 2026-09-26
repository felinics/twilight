// Package runtimetest is the RUN-CMP-2 Runtime conformance suite. It takes a
// session.Store factory so the Memory store and every durable adapter run the
// same assertions; it asserts Run semantics only and leaves group atomicity,
// ownership fencing and cache equivalence to the kernel and
// Module Framework suites.
package runtimetest

import (
	"context"
	"fmt"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/session"
	attemptmod "github.com/felinics/twilight/agentcore/session/attempt"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/session/run/runmodtest"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
	"sync"
	"testing"
	"time"
)

// Fixture is one adapter under test.
type Fixture struct {
	Store session.Store
}

// Factory returns a fresh, empty Fixture for one test.
type Factory func(t testing.TB) Fixture

const sid session.SessionID = "conformance"

type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// harness is one owner process over a Store: registry, Writers, Runtime.
type harness struct {
	t        testing.TB
	ctx      context.Context
	fixture  Fixture
	store    session.Store
	registry *extension.Registry
	bindings artifact.BindingStore
	ledger   artifact.RetentionLedger
	frozen   frozen.Store
	cache    *extension.MemoryProjectionCache
	clock    *clock
	writers  writer.Writers
	rt       *runmod.SessionRunStore
	seq      int
}

func newHarness(t testing.TB, f Fixture) *harness {
	t.Helper()
	registry, err := extension.BuildRegistry(chatlog.Module, runmod.Module, attemptmod.Module, turn.Module)
	if err != nil {
		t.Fatal(err)
	}
	bindings, ledger := artifacttest.Stores(t)
	h := &harness{t: t, ctx: context.Background(), fixture: f, store: f.Store, registry: registry, bindings: bindings,
		ledger: ledger, frozen: runmodtest.Frozen(t, bindings),
		cache: extension.NewMemoryProjectionCache(), clock: &clock{now: time.Unix(1_000_000, 0)}}
	if _, err := f.Store.Create(h.ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	h.open()
	return h
}

// open starts an owner process: Writers over the shared store and a run store.
// Takeover lets it supersede the previous owner process, if any.
func (h *harness) open() {
	h.t.Helper()
	h.writers = writer.NewWriters(h.store, h.registry, writer.Admission{Bindings: h.bindings, Ledger: h.ledger}, session.OpenOptions{Takeover: true},
		writer.WritersConfig{Cache: h.cache, CachePolicy: runmod.WriterCachePolicy(0)})
	rt, err := runmod.NewSessionRunStore(runmod.Config{Registry: h.registry, Store: h.store,
		Frozen: h.frozen, Cache: h.cache, Now: h.clock.Now})
	if err != nil {
		h.fatal(err)
	}
	h.rt = rt
}

// takeover opens a new owner process over the same store; the previous
// store stays usable so tests can observe its fencing.
func (h *harness) takeover() (*runmod.SessionRunStore, writer.Writer) {
	h.t.Helper()
	old, oldWriter := h.rt, h.writer()
	h.open()
	// Ownership changes hands when the new process opens its Writer, not
	// when it reads: reads take no lease (OWN-HDL-2).
	h.writer()
	return old, oldWriter
}

func (h *harness) fatal(args ...any) { h.t.Helper(); h.t.Fatal(args...) }

func (h *harness) writer() writer.Writer {
	h.t.Helper()
	w, err := h.writers.Writer(h.ctx, sid)
	if err != nil {
		h.fatal(err)
	}
	return w
}

func (h *harness) head() session.Head {
	h.t.Helper()
	page, err := h.store.ReadCommits(h.ctx, session.CommitReadRequest{SessionID: sid, From: ^session.CommitSeq(0) >> 1})
	if err != nil {
		h.fatal(err)
	}
	return page.Head
}

// mustApply commits a typed group through the Writer and returns its events.
func (h *harness) mustApply(group writer.SemanticGroup) {
	h.t.Helper()
	res, err := h.writer().Commit(h.ctx, func(writer.View) (*writer.SemanticGroup, error) { return &group, nil })
	if err != nil {
		h.fatal(err)
	}
	if res.Outcome != writer.CommitApplied {
		h.fatal(fmt.Sprintf("append %s: %s %s", group.CommitID, res.Outcome, res.Detail))
	}
}

// flattenCommit returns the commit's events in batch order.
func flattenCommit(c session.Commit) []session.Event {
	var out []session.Event
	for _, b := range c.Batches {
		out = append(out, b.Events...)
	}
	return out
}

// inputContent is the body the harness submits for id; the AgentInput the
// Run sees carries only its digest.
func inputContent(id run.InputID) run.CanonicalJSON {
	return run.MustParseCanonicalJSON(fmt.Sprintf(`{"text":%q}`, id))
}

func input(id string) run.AgentInput {
	d, err := chatlog.DigestInput(chatlog.InputID(id), inputContent(run.InputID(id)))
	if err != nil {
		panic(err)
	}
	return run.AgentInput{ID: run.InputID(id), Digest: d}
}

// submitInputs writes chatlog input_submitted for each input.
func (h *harness) submitInputs(inputs ...run.AgentInput) {
	h.t.Helper()
	for _, in := range inputs {
		h.seq++
		h.mustApply(writer.SemanticGroup{CommitID: session.CommitID(fmt.Sprintf("submitted/%s/%d", in.ID, h.seq)),
			Batches: []writer.TypedBatch{{Stream: chatlog.Stream, Events: []writer.TypedEvent{{
				Type: chatlog.TypeInputSubmitted, RecordedAtUnixMilli: 1,
				Value: chatlog.InputSubmittedPayload{InputID: chatlog.InputID(in.ID), Content: inputContent(in.ID), SubmittedAtUnixMilli: 1}}}}}})
	}
}

// startGroup is TRN-STR-2 without a Coordinator: turn/started,
// attempt/started, input_delivered*, run/created, input_accepted*. Owner
// is the TurnID.
func (h *harness) startGroup(turnID turn.TurnID, runID run.RunID, attempt uint32, inputs ...run.AgentInput) writer.SemanticGroup {
	h.t.Helper()
	newRun, err := run.BuildNewRun(runID, "")
	if err != nil {
		h.fatal(err)
	}
	facts, err := schema.Machine().CreateGroup(newRun, inputs)
	if err != nil {
		h.fatal(err)
	}
	group := writer.SemanticGroup{CommitID: session.CommitID(fmt.Sprintf("start/%s/%d", turnID, attempt))}
	ids := make([]chatlog.InputID, len(inputs))
	for i, in := range inputs {
		ids[i] = chatlog.InputID(in.ID)
	}
	var turnEvents []writer.TypedEvent
	if attempt == 1 {
		turnEvents = append(turnEvents, writer.TypedEvent{Type: turn.TypeStarted, RecordedAtUnixMilli: 1,
			Value: turn.StartedPayload{TurnID: turnID, InputIDs: ids, Preset: turn.PresetRef{ID: "b", Digest: "sha256:b"}}})
	}
	var chatEvents []writer.TypedEvent
	if attempt == 1 {
		for _, id := range ids {
			chatEvents = append(chatEvents, writer.TypedEvent{Type: chatlog.TypeInputDelivered, RecordedAtUnixMilli: 1,
				Value: chatlog.InputDeliveredPayload{InputID: id, TurnID: chatlog.TurnID(turnID)}})
		}
	}
	runEvents := make([]writer.TypedEvent, 0, len(facts))
	for _, f := range facts {
		runEvents = append(runEvents, writer.TypedEvent{Type: runmod.EventType(f), RecordedAtUnixMilli: 1, Value: runmod.Event{RunID: runID, Fact: f}})
	}
	// One batch per stream: the Turn's on its first attempt, the attempt
	// module's binding of the Turn to this Run (it is what routes the Run's
	// run_ended to the attempt it settles, TRN-PRJ-1), the chatlog's when
	// inputs are delivered, and the Run's.
	if len(turnEvents) > 0 {
		group.Batches = append(group.Batches, writer.TypedBatch{Stream: turn.Stream(turnID), Events: turnEvents})
	}
	group.Batches = append(group.Batches, attemptmod.Started(attemptmod.TurnID(turnID), runID, attempt, 1))
	if len(chatEvents) > 0 {
		group.Batches = append(group.Batches, writer.TypedBatch{Stream: chatlog.Stream, Events: chatEvents})
	}
	group.Batches = append(group.Batches, writer.TypedBatch{Stream: runmod.Stream(runID), Events: runEvents})
	return group
}

// startRun creates a Run under turnID with the given inputs.
func (h *harness) startRun(turnID turn.TurnID, runID run.RunID, inputs ...run.AgentInput) {
	h.t.Helper()
	h.submitInputs(inputs...)
	h.mustApply(h.startGroup(turnID, runID, 1, inputs...))
}

func (h *harness) load(runID run.RunID) runtime.Snapshot {
	h.t.Helper()
	snap, err := h.rt.Bind(h.writer()).Load(h.ctx, runID)
	if err != nil {
		h.fatal(err)
	}
	return snap
}

func (h *harness) record(runID run.RunID) runmod.Record {
	h.t.Helper()
	rec, err := h.rt.Record(h.ctx, sid, runID)
	if err != nil {
		h.fatal(err)
	}
	return rec
}

// moduleEvent is another module's event a test commits in the same unit as
// a Run command: the chatlog contributing its Part.
type moduleEvent struct {
	Type  session.EventType
	Value any
}

// attachPart writes moduleEvents to the chatlog stream as one Part: the
// harness attaches what the chatlog contributes, so an event of another
// domain is refused by the Writer's stream check.
type attachPart []moduleEvent

func (a attachPart) Prepare(_ context.Context, _ writer.View, now int64) ([]writer.TypedBatch, error) {
	events := make([]writer.TypedEvent, len(a))
	for i, me := range a {
		events[i] = writer.TypedEvent{Type: me.Type, RecordedAtUnixMilli: now, Value: me.Value}
	}
	return []writer.TypedBatch{{Stream: chatlog.Stream, Events: events}}, nil
}

// commitResult is a Run command's result plus the stored commit it landed in.
type commitResult struct {
	runtime.CommitResult
	Events []session.Event
	Head   session.Head
}

// commit builds the envelope and submits it as one unit; attach events follow
// the facts in the same commit.
func (h *harness) commit(runID run.RunID, id run.CommandID, base run.RunPosition, cmd run.AgentCommand, attach ...moduleEvent) (commitResult, error) {
	h.t.Helper()
	return h.commitWith(h.rt, h.writer(), runID, id, base, cmd, attach...)
}

func (h *harness) commitWith(rt *runmod.SessionRunStore, w writer.Writer, runID run.RunID, id run.CommandID, base run.RunPosition, cmd run.AgentCommand, attach ...moduleEvent) (commitResult, error) {
	h.t.Helper()
	env, err := schema.Wire().Envelope(runID, id, cmd)
	if err != nil {
		h.fatal(err)
	}
	req := runtime.CommitRequest{Base: base, Command: env}
	if len(attach) == 0 {
		res, err := rt.Bind(w).Commit(h.ctx, req)
		if err != nil {
			return commitResult{}, err
		}
		return h.withCommit(res, session.CommitID(env.ID)), nil
	}
	part, err := rt.Command(h.ctx, req)
	if err != nil {
		return commitResult{}, err
	}
	res, err := unit.Commit(h.ctx, w, h.clock.Now().UnixMilli(), unit.Work{CommitID: session.CommitID(env.ID), Parts: []unit.Part{part, attachPart(attach)}})
	if err != nil {
		return commitResult{}, err
	}
	out, err := part.Result(h.ctx, w, &res)
	if err != nil {
		return commitResult{}, err
	}
	return commitResult{CommitResult: out, Events: flattenCommit(res.Commit), Head: session.Head{Next: res.Commit.Seq + 1}}, nil
}

// withCommit looks the command's stored commit up so a test can inspect the
// group it produced.
func (h *harness) withCommit(res runtime.CommitResult, id session.CommitID) commitResult {
	h.t.Helper()
	var out commitResult
	out.CommitResult = res
	_, err := h.writer().Commit(h.ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		c, ok, err := v.LookupCommit(id)
		if err != nil || !ok {
			return nil, fmt.Errorf("commit %s not found: %w", id, err)
		}
		out.Events = flattenCommit(c)
		out.Head = session.Head{Next: c.Seq + 1}
		return nil, nil
	})
	if err != nil {
		h.fatal(err)
	}
	return out
}

func (h *harness) mustCommit(runID run.RunID, id run.CommandID, base run.RunPosition, cmd run.AgentCommand, attach ...moduleEvent) commitResult {
	h.t.Helper()
	res, err := h.commit(runID, id, base, cmd, attach...)
	if err != nil {
		h.fatal(fmt.Sprintf("commit %T: %v", cmd, err))
	}
	return res
}

// modelEffect derives the next model effect of the Prepared step (RUN-WIR-1):
// the step's rejected-result count is the effect's sequence.
func (h *harness) modelEffect(runID run.RunID, step run.StepID) run.EffectID {
	h.t.Helper()
	ms, ok := h.load(runID).State.Current.(run.ModelStep)
	if !ok || ms.RefValue.ID != step {
		h.fatal(fmt.Sprintf("model effect of %s: current is not the step", step))
	}
	return schema.Identity().DeriveEffectID(runID, step, "", ms.Rejects)
}

// toolEffect derives the one tool effect of a call: a call starts at most once.
func toolEffect(runID run.RunID, step run.StepID, call run.CallID) run.EffectID {
	return schema.Identity().DeriveEffectID(runID, step, call, 0)
}

// --- run building blocks ---------------------------------------------------------

var toolDef = sdk.ToolDefinition{Name: "echo", Parameters: &jsonschema.Schema{Type: "object"}}

func (h *harness) spec() run.ToolSpec {
	h.t.Helper()
	store, err := sdkconv.FreezeToolDefinition(toolDef)
	if err != nil {
		h.fatal(err)
	}
	d, err := schema.Canonical().DigestToolDefinition(store)
	if err != nil {
		h.fatal(err)
	}
	return run.ToolSpec{Ref: "echo", Name: "echo", DefinitionDigest: d, Policy: run.DirectExecution}
}

// preparedCommand builds PrepareModelRequest against snap with the derived ids.
func (h *harness) preparedCommand(snap runtime.Snapshot, withTool bool) (run.PrepareModelRequest, run.CommandID) {
	h.t.Helper()
	req := sdk.Request{Model: "m-1", Messages: []sdk.Message{sdk.UserMessage("go")}}
	var specs []run.ToolSpec
	if withTool {
		req.Tools = []sdk.ToolDefinition{toolDef}
		specs = []run.ToolSpec{h.spec()}
	}
	store, err := sdkconv.FreezeModelRequest(req)
	if err != nil {
		h.fatal(err)
	}
	reqDigest, err := schema.Canonical().DigestRequest(store)
	if err != nil {
		h.fatal(err)
	}
	cmdID := schema.Identity().DeriveModelRequestCommandID(snap.State.RunID, snap.Position)
	ids := make([]run.InputID, len(snap.State.PendingInputs))
	for i, in := range snap.State.PendingInputs {
		ids[i] = in.ID
	}
	return run.PrepareModelRequest{StepID: schema.Identity().DeriveModelStepID(snap.State.RunID, cmdID), Model: "m-1", Request: store,
		RequestDigest: reqDigest, InputIDs: ids, Tools: specs}, cmdID
}

// prepare commits a Prepare at the Run's current position and returns the step.
func (h *harness) prepare(runID run.RunID, withTool bool) run.StepID {
	h.t.Helper()
	snap := h.load(runID)
	cmd, id := h.preparedCommand(snap, withTool)
	h.mustCommit(runID, id, snap.Position, cmd)
	return cmd.StepID
}

// startModel commits StartModelExecution for the step's next model effect
// and returns that effect.
func (h *harness) startModel(runID run.RunID, step run.StepID) run.EffectID {
	h.t.Helper()
	eff := h.modelEffect(runID, step)
	res := h.mustCommit(runID, schema.Identity().DeriveStartCommandID(eff), 0, run.StartModelExecution{StepID: step, Effect: eff})
	if res.Status != runtime.CommitAccepted {
		h.fatal("start was not accepted")
	}
	return eff
}

// executingModel drives a fresh Run to Model Executing.
func (h *harness) executingModel(runID run.RunID, withTool bool) (run.StepID, run.EffectID) {
	h.t.Helper()
	step := h.prepare(runID, withTool)
	return step, h.startModel(runID, step)
}

func textResult(text string) model.ModelResult {
	r, err := sdkconv.FreezeModelResult(sdk.ModelResult{Text: text, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}})
	if err != nil {
		panic(err)
	}
	return r
}

// toolCallResult is a model result issuing n calls of the harness tool.
func (h *harness) toolCallResult(step run.StepID, n int) (model.ModelResult, []run.ToolCallBinding) {
	h.t.Helper()
	spec := h.spec()
	calls := make([]sdk.ToolCall, n)
	bindings := make([]run.ToolCallBinding, n)
	for i := range calls {
		args := run.MustParseCanonicalJSON(fmt.Sprintf(`{"i":%d}`, i))
		calls[i] = sdk.ToolCall{ToolCallID: fmt.Sprintf("c%d", i), ToolName: "echo", Input: sdk.ParseToolArguments(args.String())}
		callID := schema.Identity().DeriveCallID(step, i)
		bindings[i] = run.ToolCallBinding{CallID: callID, ProviderCallID: calls[i].ToolCallID, ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest,
			Arguments: args, Policy: spec.Policy, Replay: spec.Replay, Placement: spec.Placement}
	}
	r, err := sdkconv.FreezeModelResult(sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 2}, ToolCalls: calls})
	if err != nil {
		h.fatal(err)
	}
	return r, bindings
}

// openToolStep drives a fresh Run to a ToolStep with n Pending calls.
func (h *harness) openToolStep(runID run.RunID, n int) (run.StepID, []run.CallID) {
	h.t.Helper()
	step, eff := h.executingModel(runID, true)
	result, bindings := h.toolCallResult(step, n)
	res := h.mustCommit(runID, schema.Identity().DeriveSettlementCommandID(eff), 0,
		run.SubmitModelResult{StepID: step, Effect: eff, Result: result, Calls: bindings})
	ts, ok := res.Snapshot.State.Current.(run.ToolStep)
	if !ok {
		h.fatal(fmt.Sprintf("after model result current = %T", res.Snapshot.State.Current))
	}
	ids := make([]run.CallID, n)
	for i := range bindings {
		ids[i] = bindings[i].CallID
	}
	return ts.RefValue.ID, ids
}

// startTool commits StartToolCall for the call's tool effect and returns it.
func (h *harness) startTool(runID run.RunID, step run.StepID, call run.CallID) run.EffectID {
	h.t.Helper()
	eff := toolEffect(runID, step, call)
	res := h.mustCommit(runID, schema.Identity().DeriveStartCommandID(eff), 0, run.StartToolCall{StepID: step, CallID: call, Effect: eff})
	if res.Status != runtime.CommitAccepted {
		h.fatal("tool start was not accepted")
	}
	return eff
}

func (h *harness) machine() runmod.Machine {
	h.t.Helper()
	state, _, err := h.writer().Projections().Load(h.ctx, sid, runmod.MachineProjectionID, runmod.MachineProjection.Version)
	if err != nil {
		h.fatal(err)
	}
	return state.(runmod.Machine)
}

func eventTypes(events []session.Event) []session.EventType {
	out := make([]session.EventType, len(events))
	for i := range events {
		out[i] = events[i].Type
	}
	return out
}
