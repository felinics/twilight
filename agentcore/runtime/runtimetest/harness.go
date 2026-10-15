// Package turntest is the Store-parameterized conformance suite of the Turn
// module. The Coordinator is pure protocol, so the suite assembles Writers,
// a Runtime and a Coordinator over the Store under test and drives TurnStore step
// by step through Runtime commits: no Loop, driver, model or tool stub is
// involved.
package runtimetest

import (
	"context"
	"fmt"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/session/writer/writertest"
	"github.com/felinics/twilight/agentcore/sessionkernel"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/frozen"
	"github.com/felinics/twilight/agentcore/turn/model"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
	"github.com/felinics/twilight/agentcore/turn/sessionstore/sessionstoretest"
	"github.com/felinics/twilight/agentcore/turn/store"
	"testing"
	"time"
)

// Fixture is one adapter under test.
type Fixture struct{ Store session.Store }

// Factory returns a fresh, empty Fixture for one test.
type Factory func(t testing.TB) Fixture

const sid session.SessionID = "turn-conformance"

var presetRef = preset.PresetRef{ID: "p-1", Digest: "sha256:p-1"}

// harness is one owner process: Writers, Runtime and Coordinator over the
// Store. now is the clock every event is stamped with; tests move it to show
// that timestamps never take part in idempotency.
type harness struct {
	t         testing.TB
	ctx       context.Context
	store     session.Store
	registry  *module.Registry
	frozen    frozen.Store
	bindings  artifact.BindingStore
	retention artifact.RetentionLedger
	now       int64
	seq       int
	w         writer.Writer
	rt        *sessionstore.SessionTurnStore
	c         *sessionkernel.Coordinator
}

func newHarness(t testing.TB, f Fixture) *harness {
	t.Helper()
	registry, err := module.BuildRegistry(chatlog.Module, sessionstore.Module)
	if err != nil {
		t.Fatal(err)
	}
	bindings, retention := artifacttest.Stores(t)
	h := &harness{t: t, ctx: context.Background(), store: f.Store, registry: registry, frozen: sessionstoretest.Frozen(t, bindings), now: 1_000,
		bindings: bindings, retention: retention}
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
	factory := &writer.Factory{Store: h.store, Registry: h.registry, Admission: writer.Admission{Bindings: h.bindings, Ledger: h.retention}}
	var err error
	h.w, err = writertest.Open(h.t, h.ctx, factory, sid, session.OpenOptions{Takeover: true})
	if err != nil {
		h.t.Fatal(err)
	}
	turns, err := sessionstore.NewSessionTurnStore(sessionstore.Config{Registry: h.registry, Store: h.store, Frozen: h.frozen, Now: clock})
	if err != nil {
		h.t.Fatal(err)
	}
	h.rt = turns
	h.c = &sessionkernel.Coordinator{Projections: session.NewProjectionReader(h.store, h.registry, nil), TurnStore: turns, Now: clock}
}

// takeover opens a new owner process and returns the superseded Coordinator
// and its Writer, so a test can observe their fencing.
func (h *harness) takeover() (*sessionkernel.Coordinator, writer.Writer) {
	h.t.Helper()
	old, oldWriter := h.c, h.writer()
	h.open()
	h.writer() // ownership changes hands on Open of the new Writer, not on a read
	return old, oldWriter
}

func (h *harness) fatal(args ...any) { h.t.Helper(); h.t.Fatal(args...) }

func (h *harness) ref(turnID turn.TurnID) sessionstore.TurnRef {
	return sessionstore.TurnRef{SessionID: sid, TurnID: turnID}
}

func (h *harness) writer() writer.Writer {
	h.t.Helper()
	return h.w
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
func flattenCommit(c ledger.Commit) []ledger.Event {
	var out []ledger.Event
	for _, b := range c.Batches {
		out = append(out, b.Events...)
	}
	return out
}

// inputContent is the body the harness submits for id; the AgentInput the
// Turn sees carries only its digest.
func inputContent(id string) turn.CanonicalJSON {
	return turn.MustParseCanonicalJSON(fmt.Sprintf(`{"text":%q}`, id))
}

func input(id string) turn.AgentInput {
	d, err := chatlog.DigestInput(chatlog.InputID(id), inputContent(id))
	if err != nil {
		panic(err)
	}
	return turn.AgentInput{ID: turn.InputID(id), Digest: d}
}

// submit writes twilight/chatlog/input_submitted for each id and returns the
// AgentInputs a Start or Deliver hands to the Coordinator.
func (h *harness) submit(ids ...string) []turn.AgentInput {
	h.t.Helper()
	out := make([]turn.AgentInput, len(ids))
	for i, id := range ids {
		in := input(id)
		h.mustApply(writer.SemanticGroup{CommitID: ledger.CommitID("submitted/" + id),
			Batches: []writer.TypedBatch{{Domain: chatlog.Stream, Events: []writer.TypedEvent{{
				Type: chatlog.TypeInputSubmitted, RecordedAtUnixMilli: h.now,
				Value: chatlog.InputSubmittedPayload{InputID: chatlog.InputID(id), Content: inputContent(id), Placement: chatlog.PlacementNextTurn, SubmittedAtUnixMilli: h.now}}}}}})
		out[i] = in
	}
	return out
}

func (h *harness) startRequest(turnID turn.TurnID, inputs ...turn.AgentInput) sessionkernel.StartRequest {
	return sessionkernel.StartRequest{Ref: h.ref(turnID), Inputs: inputs, Preset: presetRef}
}

// start submits ids and starts turnID with them.
func (h *harness) start(turnID turn.TurnID, ids ...string) sessionkernel.TurnResult {
	h.t.Helper()
	resp, err := h.c.Start(h.ctx, h.writer(), h.startRequest(turnID, h.submit(ids...)...))
	if err != nil {
		h.fatal(fmt.Sprintf("start %s: %v", turnID, err))
	}
	return resp
}

func (h *harness) status(turnID turn.TurnID) sessionkernel.TurnResult {
	h.t.Helper()
	resp, err := h.c.Status(h.ctx, h.ref(turnID))
	if err != nil {
		h.fatal(fmt.Sprintf("status %s: %v", turnID, err))
	}
	return resp
}

func (h *harness) surface() sessionstore.TurnSurface {
	h.t.Helper()
	state, _, err := h.writer().Projections().Load(h.ctx, sid, sessionstore.SurfaceProjectionID, sessionstore.SurfaceProjection.Version)
	if err != nil {
		h.fatal(err)
	}
	surface, ok := state.(sessionstore.TurnSurface)
	if !ok {
		h.fatal(fmt.Errorf("turn surface projection is %T", state))
	}
	return surface
}

func (h *harness) chat() chatlog.Surface {
	h.t.Helper()
	state, _, err := h.writer().Projections().Load(h.ctx, sid, chatlog.SurfaceProjectionID, chatlog.SurfaceProjection.Version)
	if err != nil {
		h.fatal(err)
	}
	surface, ok := state.(chatlog.Surface)
	if !ok {
		h.fatal(fmt.Errorf("chatlog surface projection is %T", state))
	}
	return surface
}

// commits reads the whole commit log in order.
func (h *harness) commits() []ledger.Commit {
	h.t.Helper()
	var out []ledger.Commit
	from := ledger.CommitSeq(0)
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

func (h *harness) head() ledger.Head {
	h.t.Helper()
	page, err := h.store.ReadCommits(h.ctx, session.CommitReadRequest{SessionID: sid, From: ^ledger.CommitSeq(0) >> 1})
	if err != nil {
		h.fatal(err)
	}
	return page.Head
}

// group returns the events of the commit commitID.
func (h *harness) group(commitID ledger.CommitID) []ledger.Event {
	h.t.Helper()
	for _, c := range h.commits() {
		if c.CommitID == commitID {
			return flattenCommit(c)
		}
	}
	return nil
}

func eventTypes(events []ledger.Event) []ledger.EventType {
	out := make([]ledger.EventType, len(events))
	for i := range events {
		out[i] = events[i].Type
	}
	return out
}

func sameTypes(got []ledger.Event, want ...ledger.EventType) bool {
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

func decode[T any](t testing.TB, registry *module.Registry, event *ledger.Event) T {
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

func (h *harness) load(turnID turn.TurnID) store.Snapshot {
	h.t.Helper()
	snap, err := h.rt.Bind(h.writer()).Load(h.ctx, turnID)
	if err != nil {
		h.fatal(err)
	}
	return snap
}

// --- driving a Turn without a Loop ----------------------------------------------------

// modelEffect derives the next model effect of the Prepared step.
func (h *harness) modelEffect(turnID turn.TurnID, step turn.StepID) turn.EffectID {
	h.t.Helper()
	ms, ok := h.load(turnID).State.Current.(turn.ModelStep)
	if !ok || ms.RefValue.ID != step {
		h.fatal(fmt.Sprintf("model effect of %s: current is not the step", step))
	}
	return schema.Identity().DeriveEffectID(turnID, step, "", ms.Rejects)
}

// commitResult is a Turn command's result plus the events of the commit it
// landed in.
type commitResult struct {
	store.CommitResult
	Events []ledger.Event
}

func (h *harness) runCommit(turnID turn.TurnID, id turn.CommandID, base turn.TurnPosition, cmd turn.AgentCommand) (commitResult, error) {
	h.t.Helper()
	env, err := schema.Wire().Envelope(turnID, id, cmd)
	if err != nil {
		h.fatal(err)
	}
	res, err := h.rt.Bind(h.writer()).Commit(h.ctx, store.CommitRequest{Base: base, Command: env})
	if err != nil {
		return commitResult{}, err
	}
	out := commitResult{CommitResult: res}
	_, err = h.writer().Commit(h.ctx, func(v writer.View) (*writer.SemanticGroup, error) {
		c, ok, err := v.LookupCommit(ledger.CommitID(env.ID))
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

func (h *harness) mustTurnCommit(turnID turn.TurnID, id turn.CommandID, base turn.TurnPosition, cmd turn.AgentCommand) commitResult {
	h.t.Helper()
	res, err := h.runCommit(turnID, id, base, cmd)
	if err != nil {
		h.fatal(fmt.Sprintf("commit %T: %v", cmd, err))
	}
	return res
}

var toolDef = model.ToolDefinition{Name: "ask", Parameters: jsonstable.MustParse(`{"type":"object"}`)}

func (h *harness) spec(policy turn.ResponsePolicy) turn.ToolSpec {
	h.t.Helper()
	d, err := schema.Canonical().DigestToolDefinition(toolDef)
	if err != nil {
		h.fatal(err)
	}
	return turn.ToolSpec{Ref: "ask", Name: "ask", DefinitionDigest: d, Policy: policy}
}

// prepare commits PrepareModelRequest at the Turn's current position; specs is
// nil for a model step without tools.
func (h *harness) prepare(turnID turn.TurnID, specs []turn.ToolSpec) turn.StepID {
	h.t.Helper()
	snap := h.load(turnID)
	modelReq := userRequest("m-1", "go")
	if len(specs) > 0 {
		modelReq.Tools = []model.ToolDefinition{toolDef}
	}
	proto := schema.Canonical()
	reqDigest, err := proto.DigestRequest(modelReq)
	if err != nil {
		h.fatal(err)
	}
	cmdID := schema.Identity().DeriveModelRequestCommandID(turnID, snap.Position)
	ids := make([]turn.InputID, len(snap.State.PendingInputs))
	for i, in := range snap.State.PendingInputs {
		ids[i] = in.ID
	}
	cmd := turn.PrepareModelRequest{StepID: schema.Identity().DeriveModelStepID(turnID, cmdID), Model: "m-1", Request: modelReq,
		RequestDigest: reqDigest, InputIDs: ids, Tools: specs}
	h.mustTurnCommit(turnID, cmdID, snap.Position, cmd)
	return cmd.StepID
}

// executingModel takes the Turn to a model step that is Executing with no
// worker in this process: the state RecoverInterrupted acts on.
func (h *harness) executingModel(turnID turn.TurnID) (turn.StepID, turn.EffectID) {
	h.t.Helper()
	step := h.prepare(turnID, nil)
	eff := h.modelEffect(turnID, step)
	res := h.mustTurnCommit(turnID, schema.Identity().DeriveStartCommandID(eff), 0, turn.StartModelExecution{StepID: step, Effect: eff})
	if res.Status != store.CommitAccepted {
		h.fatal("start model was not accepted")
	}
	return step, eff
}

// userRequest is a request of one user message.
func userRequest(modelID, text string) model.ModelRequest {
	return model.ModelRequest{Model: modelID, Messages: []model.Message{{Role: model.MessageRoleUser,
		Content: []model.MessagePart{{Type: model.MessagePartTypeText, Text: text}}}}}
}

func textResult(text string) model.ModelResult {
	return model.ModelResult{Text: text, FinishReason: model.FinishReasonStop, Usage: model.Usage{TotalTokens: 1}}
}

// complete finishes the Turn with a text result; the surface folds the Turn to
// completed from the turn_ended of the same group.
func (h *harness) complete(turnID turn.TurnID) commitResult {
	h.t.Helper()
	step, eff := h.executingModel(turnID)
	return h.mustTurnCommit(turnID, schema.Identity().DeriveSettlementCommandID(eff), 0, turn.SubmitModelResult{StepID: step, Effect: eff, Result: textResult("done")})
}

// waitingTool takes the Turn to a ToolStep whose single call needs approval.
func (h *harness) waitingTool(turnID turn.TurnID) {
	h.t.Helper()
	spec := h.spec(turn.ApprovalRequired)
	step := h.prepare(turnID, []turn.ToolSpec{spec})
	eff := h.modelEffect(turnID, step)
	if res := h.mustTurnCommit(turnID, schema.Identity().DeriveStartCommandID(eff), 0, turn.StartModelExecution{StepID: step, Effect: eff}); res.Status != store.CommitAccepted {
		h.fatal("start model was not accepted")
	}
	args := turn.MustParseCanonicalJSON(`{"q":1}`)
	callID := schema.Identity().DeriveCallID(step, 0)
	result := model.ModelResult{FinishReason: model.FinishReasonToolCalls, Usage: model.Usage{TotalTokens: 2},
		ToolCalls: []model.ModelToolCall{{ToolCallID: "c0", ToolName: "ask", Input: model.ToolArguments{JSON: args}}}}
	binding := turn.ToolCallBinding{CallID: callID, ProviderCallID: "c0", ToolRef: spec.Ref, DefinitionDigest: spec.DefinitionDigest,
		Arguments: args, Policy: spec.Policy, Replay: spec.Replay, Placement: spec.Placement}
	h.mustTurnCommit(turnID, schema.Identity().DeriveSettlementCommandID(eff), 0,
		turn.SubmitModelResult{StepID: step, Effect: eff, Result: result, Calls: []turn.ToolCallBinding{binding}})
}

// appCancel is the Application's own CancelTurn: the Turn ends stopped and
// its Turn settles as failed from the turn_ended alone.
func (h *harness) appCancel(turnID turn.TurnID) {
	h.t.Helper()
	h.seq++
	h.mustTurnCommit(turnID, turn.CommandID(fmt.Sprintf("app-cancel-%d", h.seq)), 0, turn.CancelTurn{})
}
