// Package turntest is the Store-parameterized conformance suite of the Turn
// module (agent-turn.md section 8). The Coordinator is pure protocol, so the
// suite assembles Writers, a Runtime and a Coordinator over the Store under
// test and drives Runs step by step through Runtime commits: no Loop, driver,
// model or tool stub is involved.
package turntest

import (
	"context"
	"fmt"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/attempt"
	"github.com/felinics/twilight/agentcore/session/chatlog"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/session/run/runmodtest"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// Fixture is one adapter under test.
type Fixture struct{ Store session.Store }

// Factory returns a fresh, empty Fixture for one test.
type Factory func(t testing.TB) Fixture

const sid session.SessionID = "turn-conformance"

var preset = turn.PresetRef{ID: "p-1", Digest: "sha256:p-1"}

// harness is one owner process: Writers, Runtime and Coordinator over the
// Store. now is the clock every event is stamped with; tests move it to show
// that timestamps never take part in idempotency.
type harness struct {
	t        testing.TB
	ctx      context.Context
	store    session.Store
	registry *extension.Registry
	frozen   frozen.Store
	bindings artifact.BindingStore
	ledger   artifact.RetentionLedger
	now      int64
	seq      int
	writers  writer.Writers
	rt       *runmod.SessionRunStore
	c        *turn.Coordinator
}

func newHarness(t testing.TB, f Fixture) *harness {
	t.Helper()
	registry, err := extension.BuildRegistry(chatlog.Module, runmod.Module, attempt.Module, turn.Module)
	if err != nil {
		t.Fatal(err)
	}
	bindings, ledger := artifacttest.Stores(t)
	h := &harness{t: t, ctx: context.Background(), store: f.Store, registry: registry, frozen: runmodtest.Frozen(t, bindings), now: 1_000,
		bindings: bindings, ledger: ledger}
	if _, err := f.Store.Create(h.ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: 1}); err != nil {
		t.Fatal(err)
	}
	h.open()
	return h
}

// open starts an owner process over the Store; a second call is a takeover.
func (h *harness) open() {
	h.t.Helper()
	clock := func() time.Time { return time.UnixMilli(h.now) }
	h.writers = writer.NewWriters(h.store, h.registry, writer.Admission{Bindings: h.bindings, Ledger: h.ledger}, session.OpenOptions{Takeover: true}, writer.WritersConfig{})
	rt, err := runmod.NewSessionRunStore(runmod.Config{Registry: h.registry, Store: h.store, Frozen: h.frozen, Now: clock})
	if err != nil {
		h.t.Fatal(err)
	}
	h.rt = rt
	h.c = &turn.Coordinator{Projections: extension.NewProjectionReader(h.store, h.registry, nil), Runs: rt, Now: clock}
}

// takeover opens a new owner process and returns the superseded Coordinator
// and its Writer, so a test can observe their fencing.
func (h *harness) takeover() (*turn.Coordinator, writer.Writer) {
	h.t.Helper()
	old, oldWriter := h.c, h.writer()
	h.open()
	h.writer() // ownership changes hands on Open of the new Writer, not on a read
	return old, oldWriter
}

func (h *harness) fatal(args ...any) { h.t.Helper(); h.t.Fatal(args...) }

func (h *harness) ref(turnID turn.TurnID) turn.TurnRef {
	return turn.TurnRef{SessionID: sid, TurnID: turnID}
}

func (h *harness) writer() writer.Writer {
	h.t.Helper()
	w, err := h.writers.Writer(h.ctx, sid)
	if err != nil {
		h.fatal(err)
	}
	return w
}

// commit writes one typed group through the Writer and returns the outcome.
func (h *harness) commit(group writer.SemanticGroup) writer.CommitResult {
	h.t.Helper()
	res, err := h.writer().Commit(h.ctx, func(writer.View) (*writer.SemanticGroup, error) { return &group, nil })
	if err != nil {
		h.fatal(err)
	}
	return res
}

func (h *harness) mustApply(group writer.SemanticGroup) {
	h.t.Helper()
	res := h.commit(group)
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
func inputContent(id string) run.CanonicalJSON {
	return run.MustParseCanonicalJSON(fmt.Sprintf(`{"text":%q}`, id))
}

func input(id string) run.AgentInput {
	d, err := chatlog.DigestInput(chatlog.InputID(id), inputContent(id))
	if err != nil {
		panic(err)
	}
	return run.AgentInput{ID: run.InputID(id), Digest: d}
}

// submit writes twilight/chatlog/input_submitted for each id and returns the
// AgentInputs a Start or Deliver hands to the Coordinator.
func (h *harness) submit(ids ...string) []run.AgentInput {
	h.t.Helper()
	out := make([]run.AgentInput, len(ids))
	for i, id := range ids {
		in := input(id)
		h.mustApply(writer.SemanticGroup{CommitID: session.CommitID("submitted/" + id),
			Batches: []writer.TypedBatch{{Stream: chatlog.Stream, Events: []writer.TypedEvent{{
				Type: chatlog.TypeInputSubmitted, RecordedAtUnixMilli: h.now,
				Value: chatlog.InputSubmittedPayload{InputID: chatlog.InputID(id), Content: inputContent(id), SubmittedAtUnixMilli: h.now}}}}}})
		out[i] = in
	}
	return out
}

func (h *harness) startRequest(turnID turn.TurnID, inputs ...run.AgentInput) turn.StartRequest {
	return turn.StartRequest{Ref: h.ref(turnID), Inputs: inputs, Preset: preset}
}

// start submits ids and starts turnID with them.
func (h *harness) start(turnID turn.TurnID, ids ...string) turn.TurnResponse {
	h.t.Helper()
	resp, err := h.c.Start(h.ctx, h.writer(), h.startRequest(turnID, h.submit(ids...)...))
	if err != nil {
		h.fatal(fmt.Sprintf("start %s: %v", turnID, err))
	}
	return resp
}

func (h *harness) status(turnID turn.TurnID) turn.TurnResponse {
	h.t.Helper()
	resp, err := h.c.Status(h.ctx, h.ref(turnID))
	if err != nil {
		h.fatal(fmt.Sprintf("status %s: %v", turnID, err))
	}
	return resp
}

func (h *harness) surface() turn.TurnSurface {
	h.t.Helper()
	state, _, err := h.writer().Projections().Load(h.ctx, sid, turn.SurfaceProjectionID, turn.SurfaceProjection.Version)
	if err != nil {
		h.fatal(err)
	}
	return state.(turn.TurnSurface)
}

func (h *harness) chat() chatlog.Surface {
	h.t.Helper()
	state, _, err := h.writer().Projections().Load(h.ctx, sid, chatlog.SurfaceProjectionID, chatlog.SurfaceProjection.Version)
	if err != nil {
		h.fatal(err)
	}
	return state.(chatlog.Surface)
}

func (h *harness) rows() []session.Event {
	h.t.Helper()
	var out []session.Event
	for _, c := range h.commits() {
		out = append(out, flattenCommit(c)...)
	}
	return out
}

// commits reads the whole commit log in order.
func (h *harness) commits() []session.Commit {
	h.t.Helper()
	var out []session.Commit
	from := session.CommitSeq(0)
	for {
		page, err := h.store.ReadCommits(h.ctx, session.CommitReadRequest{SessionID: sid, From: from, Limit: 256})
		if err != nil {
			h.fatal(err)
		}
		out = append(out, page.Commits...)
		if !page.HasMore || len(page.Commits) == 0 {
			return out
		}
		from = page.Commits[len(page.Commits)-1].Seq + 1
	}
}

func (h *harness) head() session.Head {
	h.t.Helper()
	page, err := h.store.ReadCommits(h.ctx, session.CommitReadRequest{SessionID: sid, From: ^session.CommitSeq(0) >> 1})
	if err != nil {
		h.fatal(err)
	}
	return page.Head
}

// group returns the events of the commit commitID.
func (h *harness) group(commitID session.CommitID) []session.Event {
	h.t.Helper()
	for _, c := range h.commits() {
		if c.CommitID == commitID {
			return flattenCommit(c)
		}
	}
	return nil
}

func eventTypes(events []session.Event) []session.EventType {
	out := make([]session.EventType, len(events))
	for i := range events {
		out[i] = events[i].Type
	}
	return out
}

func sameTypes(got []session.Event, want ...session.EventType) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i].Type != want[i] {
			return false
		}
	}
	return true
}

func decode[T any](t testing.TB, registry *extension.Registry, event *session.Event) T {
	t.Helper()
	d, err := registry.Decode(*event)
	if err != nil {
		t.Fatal(err)
	}
	v, ok := d.Value.(T)
	if !ok {
		var zero T
		t.Fatalf("event is %T, want %T", d.Value, zero)
	}
	return v
}

func (h *harness) load(runID run.RunID) runtime.Snapshot {
	h.t.Helper()
	snap, err := h.rt.Bind(h.writer()).Load(h.ctx, runID)
	if err != nil {
		h.fatal(err)
	}
	return snap
}

// --- driving a Run without a Loop ----------------------------------------------------

// modelEffect derives the next model effect of the Prepared step (RUN-WIR-1).
func (h *harness) modelEffect(runID run.RunID, step run.StepID) run.EffectID {
	h.t.Helper()
	ms, ok := h.load(runID).State.Current.(run.ModelStep)
	if !ok || ms.RefValue.ID != step {
		h.fatal(fmt.Sprintf("model effect of %s: current is not the step", step))
	}
	return schema.Identity().DeriveEffectID(runID, step, "", ms.Rejects)
}

// commitResult is a Run command's result plus the events of the commit it
// landed in.
type commitResult struct {
	runtime.CommitResult
	Events []session.Event
}

func (h *harness) runCommit(runID run.RunID, id run.CommandID, base run.RunPosition, cmd run.AgentCommand) (commitResult, error) {
	h.t.Helper()
	env, err := schema.Wire().Envelope(runID, id, cmd)
	if err != nil {
		h.fatal(err)
	}
	res, err := h.rt.Bind(h.writer()).Commit(h.ctx, runtime.CommitRequest{Base: base, Command: env})
	if err != nil {
		return commitResult{}, err
	}
	out := commitResult{CommitResult: res}
	_, err = h.writer().Commit(h.ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		c, ok, err := v.LookupCommit(session.CommitID(env.ID))
		if err != nil || !ok {
			return nil, fmt.Errorf("commit %s not found: %w", env.ID, err)
		}
		for _, b := range c.Batches {
			out.Events = append(out.Events, b.Events...)
		}
		return nil, nil
	})
	if err != nil {
		h.fatal(err)
	}
	return out, nil
}

func (h *harness) mustRunCommit(runID run.RunID, id run.CommandID, base run.RunPosition, cmd run.AgentCommand) commitResult {
	h.t.Helper()
	res, err := h.runCommit(runID, id, base, cmd)
	if err != nil {
		h.fatal(fmt.Sprintf("commit %T: %v", cmd, err))
	}
	return res
}

var toolDef = sdk.ToolDefinition{Name: "ask", Parameters: &jsonschema.Schema{Type: "object"}}

func (h *harness) spec(policy run.ResponsePolicy) run.ToolSpec {
	h.t.Helper()
	store, err := sdkconv.FreezeToolDefinition(toolDef)
	if err != nil {
		h.fatal(err)
	}
	d, err := schema.Canonical().DigestToolDefinition(store)
	if err != nil {
		h.fatal(err)
	}
	return run.ToolSpec{Ref: "ask", Name: "ask", DefinitionDigest: d, Policy: policy}
}

// prepare commits PrepareModelRequest at the Run's current position; specs is
// nil for a model step without tools.
func (h *harness) prepare(runID run.RunID, specs []run.ToolSpec) run.StepID {
	h.t.Helper()
	snap := h.load(runID)
	req := sdk.Request{Model: "m-1", Messages: []sdk.Message{sdk.UserMessage("go")}}
	if len(specs) > 0 {
		req.Tools = []sdk.ToolDefinition{toolDef}
	}
	store, err := sdkconv.FreezeModelRequest(req)
	if err != nil {
		h.fatal(err)
	}
	proto := schema.Canonical()
	reqDigest, err := proto.DigestRequest(store)
	if err != nil {
		h.fatal(err)
	}
	cmdID := schema.Identity().DeriveModelRequestCommandID(runID, snap.Position)
	ids := make([]run.InputID, len(snap.State.PendingInputs))
	for i, in := range snap.State.PendingInputs {
		ids[i] = in.ID
	}
	cmd := run.PrepareModelRequest{StepID: schema.Identity().DeriveModelStepID(runID, cmdID), Model: "m-1", Request: store,
		RequestDigest: reqDigest, InputIDs: ids, Tools: specs}
	h.mustRunCommit(runID, cmdID, snap.Position, cmd)
	return cmd.StepID
}

// executingModel takes the Run to a model step that is Executing with no
// worker in this process: the state RecoverInterrupted acts on.
func (h *harness) executingModel(runID run.RunID) (run.StepID, run.EffectID) {
	h.t.Helper()
	step := h.prepare(runID, nil)
	eff := h.modelEffect(runID, step)
	res := h.mustRunCommit(runID, schema.Identity().DeriveStartCommandID(eff), 0, run.StartModelExecution{StepID: step, Effect: eff})
	if res.Status != runtime.CommitAccepted {
		h.fatal("start model was not accepted")
	}
	return step, eff
}

func textResult(text string) model.ModelResult {
	r, err := sdkconv.FreezeModelResult(sdk.ModelResult{Text: text, FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}})
	if err != nil {
		panic(err)
	}
	return r
}

// complete finishes the Run with a text result; the surface folds the Turn to
// completed from the run_ended of the same group.
func (h *harness) complete(runID run.RunID) commitResult {
	h.t.Helper()
	step, eff := h.executingModel(runID)
	return h.mustRunCommit(runID, schema.Identity().DeriveSettlementCommandID(eff), 0, run.SubmitModelResult{StepID: step, Effect: eff, Result: textResult("done")})
}

// waitingTool takes the Run to a ToolStep whose single call needs approval.
func (h *harness) waitingTool(runID run.RunID) {
	h.t.Helper()
	spec := h.spec(run.ApprovalRequired)
	step := h.prepare(runID, []run.ToolSpec{spec})
	eff := h.modelEffect(runID, step)
	if res := h.mustRunCommit(runID, schema.Identity().DeriveStartCommandID(eff), 0, run.StartModelExecution{StepID: step, Effect: eff}); res.Status != runtime.CommitAccepted {
		h.fatal("start model was not accepted")
	}
	args := run.MustParseCanonicalJSON(`{"q":1}`)
	callID := schema.Identity().DeriveCallID(step, 0)
	result, err := sdkconv.FreezeModelResult(sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 2},
		ToolCalls: []sdk.ToolCall{{ToolCallID: "c0", ToolName: "ask", Input: sdk.ParseToolArguments(args.String())}}})
	if err != nil {
		h.fatal(err)
	}
	binding := run.ToolCallBinding{CallID: callID, ProviderCallID: "c0", ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest,
		Arguments: args, Policy: spec.Policy, Replay: spec.Replay, Placement: spec.Placement}
	h.mustRunCommit(runID, schema.Identity().DeriveSettlementCommandID(eff), 0,
		run.SubmitModelResult{StepID: step, Effect: eff, Result: result, Calls: []run.ToolCallBinding{binding}})
}

// appCancel is the Application's own CancelRun: no settlement is attached, so
// the Turn becomes attempt_failed (TRN-STP-1).
func (h *harness) appCancel(runID run.RunID) {
	h.t.Helper()
	h.seq++
	h.mustRunCommit(runID, run.CommandID(fmt.Sprintf("app-cancel-%d", h.seq)), 0, run.CancelRun{})
}
