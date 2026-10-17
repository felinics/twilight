package storetest

import (
	"context"
	"errors"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/preset/ref"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/frozen"
	"github.com/felinics/twilight/agentcore/turn/plan"
	"github.com/felinics/twilight/agentcore/turn/reconcile"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
	"github.com/felinics/twilight/agentcore/turn/store"
	"github.com/felinics/twilight/agentcore/turn/wire"
	"strings"
	"testing"
)

// Run executes the Runtime conformance suite against fixtures made
// by factory.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	for name, fn := range map[string]func(*testing.T, Factory){
		"Creation":           testCreation,
		"ReplayAndBase":      testReplayAndBase,
		"InputQueue":         testInputQueue,
		"StartAndEffect":     testStartAndEffect,
		"DeclineToolCall":    testDeclineToolCall,
		"GroupComposition":   testGroupComposition,
		"Admission":          testAdmission,
		"SettlementSnapshot": testSettlementSnapshot,
		"PrepareCAS":         testPrepareCASIgnoresOtherModules,
		"Projection":         testProjection,
		"SettlementReplay":   testSettlementReplay,
		"Isolation":          testIsolation,
		"Takeover":           testTakeover,
		"Reattach":           testReattach,
		"OwnershipLost":      testOwnershipLost,
		"FrozenValues":       testFrozenValues,
	} {
		t.Run(name, func(t *testing.T) { fn(t, factory) })
	}
}

// --- 建立与寻址 -----------------------------------------------------------------------

func testCreation(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	snap := h.load("r1")
	if snap.State.TurnID != "r1" || len(snap.State.PendingInputs) != 1 {
		t.Fatalf("created state = %+v", snap.State)
	}
	// Position is the StreamSeq of the Turn's last event: the start group's Turn
	// batch is created (0), accepted (1). The Turn's and the chatlog's events
	// of the same commit do not move it.
	if snap.Position != 1 {
		t.Fatalf("position = %d, want 1 (last Turn event of the start group)", snap.Position)
	}
	// Unknown TurnID.
	if _, err := h.rt.Bind(h.writer()).Load(h.ctx, "nope"); !errors.Is(err, store.ErrTurnNotFound) {
		t.Fatalf("load unknown = %v", err)
	}
	if _, err := h.rt.Record(h.ctx, sid, "nope"); !errors.Is(err, store.ErrTurnNotFound) {
		t.Fatalf("record unknown = %v", err)
	}
	env, _ := schema.Wire().Envelope("nope", schema.Identity().DeriveInputCommandID("nope", "x"), turn.NextStep(input("x")))
	if _, err := h.rt.Bind(h.writer()).Commit(h.ctx, store.CommitRequest{Command: env}); !errors.Is(err, store.ErrTurnNotFound) {
		t.Fatalf("commit unknown = %v", err)
	}
	// Terminated Turn: Load returns the terminal state, Commit is terminal.
	h.mustCommit("r1", "cancel-1", 0, turn.CancelTurn{})
	term := h.load("r1")
	if term.State.Status != turn.TurnStopped || term.State.Result == nil {
		t.Fatalf("terminal load = %+v", term.State)
	}
	rec := h.record("r1")
	if !wire.StatesEquivalent(&rec.Snapshot.State, &term.State) || rec.Snapshot.Position != term.Position {
		t.Fatal("terminal Load and Record disagree")
	}
	if _, err := h.commit("r1", schema.Identity().DeriveInputCommandID("r1", "late"), 0, turn.NextStep(input("late"))); !errors.Is(err, turn.ErrTurnTerminal) {
		t.Fatalf("commit on terminal = %v", err)
	}
	// A second created for the same TurnID -- active or ended -- is refused by
	// the Turn module's creation Part from the ledger's stream index, before
	// anything reaches the ledger.
	again, err := turn.BuildNewTurn("r1", "", ref.Ref{ID: "test", Digest: "sha256:test"})
	if err != nil {
		t.Fatal(err)
	}
	head := h.head()
	if _, err := unit.Commit(h.ctx, h.writer(), 1, unit.Work{CommitID: "start/t2/1", Parts: []unit.Part{sessionstore.CreateTurn(again, nil)}}); !errors.Is(err, sessionstore.ErrTurnExists) {
		t.Fatalf("duplicate created = %v, want ErrTurnExists", err)
	}
	if h.head() != head {
		t.Fatal("refused creation wrote to the ledger")
	}
}

// --- 重放与 Base ----------------------------------------------------------------------

func testReplayAndBase(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	h.submitInputs(input("in-2"))
	first := h.mustCommit("r1", schema.Identity().DeriveInputCommandID("r1", "in-2"), 0, turn.NextStep(input("in-2")))
	if first.Status != store.CommitAccepted {
		t.Fatal("first accept not accepted")
	}
	again := h.mustCommit("r1", schema.Identity().DeriveInputCommandID("r1", "in-2"), 0, turn.NextStep(input("in-2")))
	if again.Status != store.CommitAlreadyApplied || len(again.Events) != len(first.Events) || again.Head != first.Head {
		t.Fatalf("replay = %+v", again)
	}
	if h.head() != first.Head {
		t.Fatal("replay appended commits")
	}
	// Prepare is a hard CAS on the Turn's own position.
	snap := h.load("r1")
	stale := snap.Position - 1
	cmd, id := h.preparedCommand(store.Snapshot{State: snap.State, Position: stale}, false)
	if _, err := h.commit("r1", id, stale, cmd); !errors.Is(err, turn.ErrStaleRuntime) {
		t.Fatalf("stale prepare = %v", err)
	}
	cmd, id = h.preparedCommand(snap, false)
	prepared := h.mustCommit("r1", id, snap.Position, cmd)
	// Non-prepare commands accept a zero or stale Base (call-local rebase).
	h.submitInputs(input("in-3"))
	h.mustCommit("r1", schema.Identity().DeriveInputCommandID("r1", "in-3"), 0, turn.NextStep(input("in-3")))
	// Terminal replay: an accepted command replays after termination.
	h.mustCommit("r1", "cancel", prepared.Snapshot.Position, turn.CancelTurn{})
	replay := h.mustCommit("r1", schema.Identity().DeriveInputCommandID("r1", "in-3"), 0, turn.NextStep(input("in-3")))
	if replay.Status != store.CommitAlreadyApplied || !replay.Snapshot.State.Status.Terminal() {
		t.Fatalf("terminal replay = %+v", replay)
	}
	if _, err := h.commit("r1", schema.Identity().DeriveInputCommandID("r1", "in-4"), 0, turn.NextStep(input("in-4"))); !errors.Is(err, turn.ErrTurnTerminal) {
		t.Fatalf("new command after terminal = %v", err)
	}
	// Derived-identity families must use their derived CommandID.
	if _, err := h.commit("r1", "random", 0, turn.NextStep(input("in-5"))); !errors.Is(err, turn.ErrCommandConflict) && !errors.Is(err, turn.ErrTurnTerminal) {
		t.Fatalf("non-derived id = %v", err)
	}
}

// --- 输入入队 --------------------------------------------------------------------------

func testInputQueue(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	// Prepared: the input queues and Next asks to withdraw.
	step := h.prepare("r1", false)
	h.submitInputs(input("in-2"))
	h.mustCommit("r1", schema.Identity().DeriveInputCommandID("r1", "in-2"), 0, turn.NextStep(input("in-2")))
	snap := h.load("r1")
	eff, err := plan.Next(snap.State)
	if err != nil {
		t.Fatal(err)
	}
	if w, ok := eff.(plan.WithdrawPrepared); !ok || w.StepID != step {
		t.Fatalf("action while Prepared with input = %#v", eff)
	}
	h.mustCommit("r1", schema.Identity().DeriveWithdrawCommandID("r1", step), snap.Position, turn.WithdrawPreparedStep{StepID: step})
	snap = h.load("r1")
	if _, open := snap.State.Current.(turn.Open); !open || len(snap.State.PendingInputs) != 1 || snap.State.ModelSteps != 0 {
		t.Fatalf("after withdraw = %+v", snap.State)
	}
	cmd, id := h.preparedCommand(snap, false)
	if len(cmd.InputIDs) != 1 || cmd.InputIDs[0] != "in-2" {
		t.Fatalf("replanned prepare consumes %v", cmd.InputIDs)
	}
	h.mustCommit("r1", id, snap.Position, cmd)
	// Executing: the input queues; a result without calls reopens instead of ending.
	modelEff := h.startModel("r1", cmd.StepID)
	h.submitInputs(input("in-3"))
	h.mustCommit("r1", schema.Identity().DeriveInputCommandID("r1", "in-3"), 0, turn.NextStep(input("in-3")))
	res := h.mustCommit("r1", schema.Identity().DeriveSettlementCommandID(modelEff), 0, turn.SubmitModelResult{StepID: cmd.StepID, Effect: modelEff, Result: textResult("a")})
	if res.Snapshot.State.Status != turn.TurnActive {
		t.Fatal("Turn ended with a pending input")
	}
	if _, open := res.Snapshot.State.Current.(turn.Open); !open || len(res.Snapshot.State.PendingInputs) != 1 {
		t.Fatalf("after result with pending input = %+v", res.Snapshot.State)
	}
	// ToolStep: the input queues as well.
	h2 := newHarness(t, factory(t))
	h2.startTurn("r2", input("in-1"))
	h2.openToolStep("r2", 1)
	h2.submitInputs(input("in-9"))
	res = h2.mustCommit("r2", schema.Identity().DeriveInputCommandID("r2", "in-9"), 0, turn.NextStep(input("in-9")))
	if _, ok := res.Snapshot.State.Current.(turn.ToolStep); !ok || len(res.Snapshot.State.PendingInputs) != 1 {
		t.Fatalf("accept on tool step = %+v", res.Snapshot.State)
	}
}

// --- start 与 effect ------------------------------------------------------------------------

func testStartAndEffect(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	step, eff := h.executingModel("r1", false)
	// Same-effect replay is AlreadyApplied; a start naming another effect of
	// the step finds the step already Executing.
	replay := h.mustCommit("r1", schema.Identity().DeriveStartCommandID(eff), 0, turn.StartModelExecution{StepID: step, Effect: eff})
	if replay.Status != store.CommitAlreadyApplied {
		t.Fatalf("start replay = %+v", replay)
	}
	other := schema.Identity().DeriveEffectID("r1", step, "", 1)
	if _, err := h.commit("r1", schema.Identity().DeriveStartCommandID(other), 0, turn.StartModelExecution{StepID: step, Effect: other}); !errors.Is(err, turn.ErrStaleRuntime) {
		t.Fatalf("second effect start = %v", err)
	}
	// Starts, settlements and recoveries without an effect identity are
	// conflicts.
	if _, err := h.commit("r1", schema.Identity().DeriveStartCommandID(""), 0, turn.StartModelExecution{StepID: step}); !errors.Is(err, turn.ErrCommandConflict) {
		t.Fatalf("effectless start = %v", err)
	}
	if _, err := h.commit("r1", schema.Identity().DeriveSettlementCommandID(""), 0, turn.SubmitModelResult{StepID: step, Result: textResult("done")}); !errors.Is(err, turn.ErrCommandConflict) {
		t.Fatalf("effectless settlement = %v", err)
	}
	// A settlement of another effect of the step, under that effect's
	// settlement identity, finds the step executing a different effect; a
	// settlement under any CommandID but its effect's is a conflict.
	if _, err := h.commit("r1", schema.Identity().DeriveSettlementCommandID(other), 0, turn.SubmitModelResult{StepID: step, Effect: other, Result: textResult("done")}); !errors.Is(err, turn.ErrStaleRuntime) {
		t.Fatalf("settlement of another effect = %v", err)
	}
	if _, err := h.commit("r1", "random", 0, turn.SubmitModelResult{StepID: step, Effect: eff, Result: textResult("done")}); !errors.Is(err, turn.ErrCommandConflict) {
		t.Fatalf("settlement under a foreign id = %v", err)
	}
	// Settlement under the effect; its replay is AlreadyApplied.
	settleID := schema.Identity().DeriveSettlementCommandID(eff)
	res := h.mustCommit("r1", settleID, 0, turn.SubmitModelResult{StepID: step, Effect: eff, Result: textResult("done")})
	if !res.Snapshot.State.Status.Terminal() {
		t.Fatal("settlement did not end the Turn")
	}
	again := h.mustCommit("r1", settleID, 0, turn.SubmitModelResult{StepID: step, Effect: eff, Result: textResult("done")})
	if again.Status != store.CommitAlreadyApplied {
		t.Fatalf("settlement replay = %+v", again)
	}
	// After settlement the start still replays; a new command is terminal.
	replay = h.mustCommit("r1", schema.Identity().DeriveStartCommandID(eff), 0, turn.StartModelExecution{StepID: step, Effect: eff})
	if replay.Status != store.CommitAlreadyApplied {
		t.Fatalf("start replay after settlement = %+v", replay)
	}
}

// --- decline ------------------------------------------------------------------------------

func testDeclineToolCall(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	step, calls := h.openToolStep("r1", 2)
	failure := turn.ToolFailure{Class: turn.FailureToolLookup, Message: "no such tool"}
	decline := turn.DeclineToolCall{StepID: step, CallID: calls[0], Failure: failure}
	id := schema.Identity().DeriveDeclineCommandID("r1", step, calls[0])
	// The decline has no effect; its identity is the call's coordinates and
	// any other CommandID is a conflict.
	if _, err := h.commit("r1", "random", 0, decline); !errors.Is(err, turn.ErrCommandConflict) {
		t.Fatalf("decline under a foreign id = %v", err)
	}
	res := h.mustCommit("r1", id, 0, decline)
	if res.Status != store.CommitAccepted {
		t.Fatalf("decline = %v", res.Status)
	}
	if types := eventTypes(res.Events); len(types) != 1 || types[0] != sessionstore.Prefix+"tool_call_failed" {
		t.Fatalf("decline events = %v, want tool_call_failed alone", types)
	}
	ts := mustType[turn.ToolStep](h.t, res.Snapshot.State.Current)
	if ts.Calls[0].Status != turn.ToolFailed || ts.Calls[0].Effect != "" {
		t.Fatalf("declined call = %+v, want Failed with no effect", ts.Calls[0])
	}
	// The CommandID names the decline of this call: a replay, with the same
	// or another reason, is AlreadyApplied and the first decline stands
	if again := h.mustCommit("r1", id, 0, decline); again.Status != store.CommitAlreadyApplied {
		t.Fatalf("decline replay = %v", again.Status)
	}
	different := decline
	different.Failure.Message = "another reason"
	if again := h.mustCommit("r1", id, 0, different); again.Status != store.CommitAlreadyApplied {
		t.Fatalf("decline with other content = %v, want already applied", again.Status)
	}
	// A Pending call has no effect to settle: a Known failure naming the
	// effect it would start under finds the call not Executing.
	eff := toolEffect("r1", step, calls[1])
	knownFailure := turn.SubmitToolFailure{StepID: step, CallID: calls[1], Effect: eff, Failure: failure, Outcome: turn.ToolOutcomeKnown}
	if _, err := h.commit("r1", schema.Identity().DeriveSettlementCommandID(eff), 0, knownFailure); !errors.Is(err, turn.ErrStaleRuntime) {
		t.Fatalf("known failure of a Pending call = %v", err)
	}
	// Once started, the call settles under its effect and cannot be declined.
	h.startTool("r1", step, calls[1])
	if _, err := h.commit("r1", schema.Identity().DeriveDeclineCommandID("r1", step, calls[1]), 0,
		turn.DeclineToolCall{StepID: step, CallID: calls[1], Failure: failure}); !errors.Is(err, turn.ErrStaleRuntime) {
		t.Fatalf("decline of an Executing call = %v", err)
	}
	res = h.mustCommit("r1", schema.Identity().DeriveSettlementCommandID(eff), 0, knownFailure)
	if _, open := res.Snapshot.State.Current.(turn.Open); !open {
		t.Fatalf("after both calls failed current = %T, want Open", res.Snapshot.State.Current)
	}
}

// --- 组的组成 ----------------------------------------------------------------------------

func testGroupComposition(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	step, eff := h.executingModel("r1", true)
	result, bindings := h.toolCallResult(step, 1)
	before := h.head()
	res := h.mustCommit("r1", schema.Identity().DeriveSettlementCommandID(eff), 0,
		turn.SubmitModelResult{StepID: step, Effect: eff, Result: result, Calls: bindings})
	if h.head().Next != before.Next+1 {
		t.Fatal("one command did not produce exactly one commit")
	}
	types := eventTypes(res.Events)
	want := []ledger.EventType{sessionstore.Prefix + "model_step_completed", sessionstore.Prefix + "tool_step_opened"}
	if strings.Join(asStrings(types), ",") != strings.Join(asStrings(want), ",") {
		t.Fatalf("commit events = %v, want %v and no conversation copy", types, want)
	}
	if rec := h.record("r1"); res.Snapshot.Position != rec.Snapshot.Position {
		t.Fatalf("position = %d, want the record's last stream position %d", res.Snapshot.Position, rec.Snapshot.Position)
	}
	// The assistant entry is a projection of the fact: it names the frozen
	// result by the fact's ResultDigest, and the body is readable under it.
	var resultDigest jsonstable.Digest
	for _, f := range h.record("r1").Facts {
		if c, ok := f.(turn.ModelStepCompleted); ok {
			resultDigest = c.ResultDigest
		}
	}
	ts := mustType[turn.ToolStep](h.t, res.Snapshot.State.Current)
	call := ts.Calls[0].CallID
	entries := h.contextEntries()
	last := entries[len(entries)-1]
	if last.Kind != chatlog.EntryAssistant || last.Assistant.ResultDigest != resultDigest || last.Assistant.TurnID != "r1" ||
		len(last.Assistant.CallIDs) != 1 || last.Assistant.CallIDs[0] != chatlog.CallID(call) {
		t.Fatalf("assistant entry = %+v, want ResultDigest %s and call %s", last.Assistant, resultDigest, call)
	}
	if body, err := sessionstore.NewContent(h.frozen).ModelResult(h.ctx, resultDigest); err != nil || len(body.ToolCalls) != 1 {
		t.Fatalf("frozen result = %+v %v", body, err)
	}
	// Attach follows the facts; twilight/turn/ events are refused.
	toolEff := h.startTool("r1", ts.RefValue.ID, call)
	output := turn.MustParseCanonicalJSON(`{"ok":true}`)
	if _, err := h.commit("r1", schema.Identity().DeriveSettlementCommandID(toolEff), 0,
		turn.SubmitToolResult{StepID: ts.RefValue.ID, CallID: call, Effect: toolEff, Result: turn.ToolExecutionResult{Output: output}},
		moduleEvent{Type: sessionstore.Prefix + "input_accepted", Value: sessionstore.Event{TurnID: "r1", Fact: turn.InputAccepted{Input: input("x")}}}); err == nil {
		t.Fatal("Attach with a twilight/turn/ event accepted")
	}
	h.submitInputs(input("in-attach"))
	res = h.mustCommit("r1", schema.Identity().DeriveSettlementCommandID(toolEff), 0,
		turn.SubmitToolResult{StepID: ts.RefValue.ID, CallID: call, Effect: toolEff, Result: turn.ToolExecutionResult{Output: output}},
		moduleEvent{Type: chatlog.TypeInputRejected, Value: chatlog.InputRejectedPayload{InputID: "in-attach"}})
	types = eventTypes(res.Events)
	if len(types) != 2 || types[0] != sessionstore.Prefix+"tool_call_completed" || types[1] != chatlog.TypeInputRejected {
		t.Fatalf("group events = %v", types)
	}
	outputDigest, _ := schema.Canonical().DigestToolOutput(output)
	entries = h.contextEntries()
	var tr *chatlog.ToolResult
	for i := range entries {
		if entries[i].Kind == chatlog.EntryToolResult && entries[i].ToolResult.CallID == chatlog.CallID(call) {
			tr = entries[i].ToolResult
		}
	}
	if tr == nil || tr.OutputDigest != outputDigest || tr.Status != chatlog.ToolSuccess || tr.Source != chatlog.SourceToolOutput {
		t.Fatalf("tool_result entry = %+v", tr)
	}
	if body, err := sessionstore.NewContent(h.frozen).ToolOutput(h.ctx, outputDigest); err != nil || !body.Equal(output) {
		t.Fatalf("frozen output = %s %v", body, err)
	}
}

// contextEntries reads the chatlog Context projection through the owner's
// Writer.
func (h *harness) contextEntries() []chatlog.Entry {
	h.t.Helper()
	state, _, err := h.writer().Projections().Load(h.ctx, sid, chatlog.ContextProjectionID, chatlog.ContextProjection.Version)
	if err != nil {
		h.fatal(err)
	}
	return mustType[chatlog.Context](h.t, state).Entries
}

// turnSurface reads the turn surface projection through the owner's Writer.
func (h *harness) turnSurface() sessionstore.TurnSurface {
	h.t.Helper()
	state, _, err := h.writer().Projections().Load(h.ctx, sid, sessionstore.SurfaceProjectionID, sessionstore.SurfaceProjection.Version)
	if err != nil {
		h.fatal(err)
	}
	return mustType[sessionstore.TurnSurface](h.t, state)
}

func asStrings(types []ledger.EventType) []string {
	out := make([]string, len(types))
	for i, t := range types {
		out[i] = string(t)
	}
	return out
}

// --- admission -------------------------------------------------------------------------------

func testAdmission(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	artifactRef := artifact.Ref{Scheme: "cas", Authority: "local", Key: "k1", Durability: artifact.EventBound, Integrity: &artifact.Integrity{Algorithm: "sha256", Value: "x"}}
	binding, err := artifact.NewBinding("b1", artifactRef)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.bindings.CreateBinding(h.ctx, binding); err != nil {
		t.Fatal(err)
	}
	attach := func(bindingID artifact.BindingID) moduleEvent {
		s := chatlog.Summary{ID: "s-attach", Parts: chatlog.Parts{chatlog.ReferencePart{BindingID: bindingID, Name: "f"}}}
		s.Digest, _ = chatlog.DigestSummary(&s)
		return moduleEvent{Type: chatlog.TypeSummary, Value: chatlog.SummaryPayload{Summary: s}}
	}
	h.submitInputs(input("in-2"))
	before := h.head()
	// Unregistered binding: the whole group is refused and nothing is written.
	if _, err := h.commit("r1", schema.Identity().DeriveInputCommandID("r1", "in-2"), 0, turn.NextStep(input("in-2")), attach("missing")); err == nil {
		t.Fatal("unregistered binding admitted")
	}
	if h.head() != before {
		t.Fatal("refused commit wrote to the ledger")
	}
	if len(h.load("r1").State.PendingInputs) != 1 {
		t.Fatal("refused commit changed the Turn")
	}
	// Registered binding: the claim is Active once the commit is applied.
	cmdID := schema.Identity().DeriveInputCommandID("r1", "in-2")
	h.mustCommit("r1", cmdID, 0, turn.NextStep(input("in-2")), attach("b1"))
	commitID := ledger.CommitID(cmdID)
	header, err := h.store.Header(h.ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	seg := header.ID
	claimID := writer.DeriveClaimID(seg, commitID, mustSet(t, h, "b1").RefSetDigest)
	claim, ok, err := h.ledger.LookupClaim(h.ctx, claimID)
	if err != nil || !ok || claim.State != artifact.ClaimActive || claim.Owner != writer.CommitOwner(seg, commitID) {
		t.Fatalf("claim = %+v ok=%v err=%v", claim, ok, err)
	}
}

func mustSet(t *testing.T, h *harness, ids ...artifact.BindingID) artifact.BindingSet {
	t.Helper()
	set, err := artifact.SetBuilder{Resolver: h.bindings}.Build(h.ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	return set
}

// --- 结算返回值 --------------------------------------------------------------------------------

func testSettlementSnapshot(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	step, eff := h.executingModel("r1", false)
	res := h.mustCommit("r1", schema.Identity().DeriveSettlementCommandID(eff), 0, turn.SubmitModelResult{StepID: step, Effect: eff, Result: textResult("done")})
	if res.Snapshot.State.Status != turn.TurnCompleted || res.Snapshot.State.Result == nil || res.Snapshot.State.Result.Status != turn.TurnCompleted {
		t.Fatalf("settlement snapshot = %+v", res.Snapshot.State)
	}
	rec := h.record("r1")
	if !wire.StatesEquivalent(&rec.Snapshot.State, &res.Snapshot.State) || rec.Snapshot.Position != res.Snapshot.Position {
		t.Fatalf("settlement snapshot %+v disagrees with record %+v", res.Snapshot, rec.Snapshot)
	}
	// The Turn settles from the Turn's own turn_ended: no turn event is written.
	types := eventTypes(res.Events)
	if types[len(types)-1] != sessionstore.Prefix+"turn_ended" {
		t.Fatalf("terminal group events = %v, want turn_ended last", types)
	}
	if v := h.turnSurface().Turns["r1"]; v.Status != turn.TurnCompleted || v.End == nil {
		t.Fatalf("turn after turn_ended = %+v, want completed", v)
	}
}

// --- Prepare hard CAS 对其他模块不敏感 ---------------------------------------------------------------

func testPrepareCASIgnoresOtherModules(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	h.startTurn("r2", input("in-b"))
	snap := h.load("r1")
	cmd, id := h.preparedCommand(snap, false)
	// Other modules and another Turn write after the prompt builder loaded.
	head := h.head()
	h.submitInputs(input("late"))
	h.prepare("r2", false)
	after := h.load("r1")
	if after.Position != snap.Position {
		t.Fatalf("foreign writes moved r1 position %d -> %d", snap.Position, after.Position)
	}
	if h.head() == head {
		t.Fatal("session head did not move")
	}
	if _, err := h.commit("r1", id, snap.Position, cmd); err != nil {
		t.Fatalf("prepare against a moved session head: %v", err)
	}
}

// a settlement CommandID names the effect rather than the
// outcome, so every settlement of the same effect is the same operation: the
// first one stands and later ones, whatever they carry, are AlreadyApplied
// without a second Decide.
func testSettlementReplay(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	step, eff := h.executingModel("r1", false)
	id := schema.Identity().DeriveSettlementCommandID(eff)
	first := h.mustCommit("r1", id, 0, turn.SubmitModelResult{StepID: step, Effect: eff, Result: textResult("one")})
	if !first.Snapshot.State.Status.Terminal() {
		t.Fatalf("settlement = %+v", first.Snapshot.State.Status)
	}
	if again, err := h.commit("r1", id, 0, turn.SubmitModelResult{StepID: step, Effect: eff, Result: textResult("one")}); err != nil || again.Status != store.CommitAlreadyApplied {
		t.Fatalf("same result replay = %v %v", again.Status, err)
	}
	if again, err := h.commit("r1", id, 0, turn.SubmitModelResult{StepID: step, Effect: eff, Result: textResult("two")}); err != nil || again.Status != store.CommitAlreadyApplied {
		t.Fatalf("different result under the same effect = %v %v, want already applied", again.Status, err)
	}
	if again, err := h.commit("r1", id, 0, turn.SubmitModelFailure{StepID: step, Effect: eff, Failure: turn.StepFailure{Class: turn.FailureProvider, Message: "x"}}); err != nil || again.Status != store.CommitAlreadyApplied {
		t.Fatalf("failure under a settled effect = %v %v, want already applied", again.Status, err)
	}
	if after := h.load("r1"); after.Position != first.Snapshot.Position || !after.State.Status.Terminal() {
		t.Fatalf("replays moved the Turn: %+v", after)
	}
}

// --- 投影 -------------------------------------------------------------------------------------------

func testProjection(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	cached := func() (ledger.Head, bool) {
		_, through, ok, err := h.cache.Load(h.ctx, sid, sessionstore.MachineProjectionID, sessionstore.MachineProjection.Version)
		if err != nil {
			t.Fatal(err)
		}
		return through, ok
	}
	step, eff := h.executingModel("r1", true)
	if _, ok := cached(); ok {
		t.Fatal("projection cached while the Turn is mid-step")
	}
	result, bindings := h.toolCallResult(step, 1)
	opened := h.mustCommit("r1", schema.Identity().DeriveSettlementCommandID(eff), 0,
		turn.SubmitModelResult{StepID: step, Effect: eff, Result: result, Calls: bindings})
	toolStep := mustType[turn.ToolStep](h.t, opened.Snapshot.State.Current).RefValue.ID
	callID := bindings[0].CallID
	toolEff := h.startTool("r1", toolStep, callID)
	res := h.mustCommit("r1", schema.Identity().DeriveSettlementCommandID(toolEff), 0,
		turn.SubmitToolResult{StepID: toolStep, CallID: callID, Effect: toolEff, Result: turn.ToolExecutionResult{Output: turn.MustParseCanonicalJSON(`1`)}})
	if _, open := res.Snapshot.State.Current.(turn.Open); !open {
		t.Fatalf("after tool settlement current = %T", res.Snapshot.State.Current)
	}
	if through, ok := cached(); !ok || through != res.Head {
		t.Fatalf("cache after return to Open = %+v ok=%v, want head %+v", through, ok, res.Head)
	}
	// The cached state plus tail equals the Writer's state.
	observer := session.NewProjectionReader(h.store, h.registry, h.cache)
	fromCache, _, err := observer.Load(h.ctx, sid, sessionstore.MachineProjectionID, sessionstore.MachineProjection.Version)
	if err != nil {
		t.Fatal(err)
	}
	m := h.machine()
	loaded := h.load("r1")
	rec := h.record("r1")
	active, ok := m.Active["r1"]
	if !ok || !wire.StatesEquivalent(&active, &loaded.State) || !wire.StatesEquivalent(&active, &rec.Snapshot.State) {
		t.Fatal("projection, Load and Record disagree")
	}
	if cached := mustType[sessionstore.Machine](h.t, fromCache).Active["r1"]; !wire.StatesEquivalent(&cached, &active) {
		t.Fatal("observer's cache+tail disagrees with the writer's projection")
	}
	// Terminal Turn leaves the projection entirely; Load and Record still
	// answer from the Turn's stream, and a second creation of its TurnID is
	// refused from the ledger's stream index, not from projection state.
	h.mustCommit("r1", "cancel", 0, turn.CancelTurn{})
	m = h.machine()
	if _, still := m.Active["r1"]; still {
		t.Fatal("terminal Turn still in the projection")
	}
	if h.load("r1").State.Status != turn.TurnStopped || h.record("r1").Snapshot.State.Status != turn.TurnStopped {
		t.Fatal("terminal Turn not readable")
	}
	if again, err := turn.BuildNewTurn("r1", "", ref.Ref{ID: "test", Digest: "sha256:test"}); err != nil {
		t.Fatal(err)
	} else if _, err := unit.Commit(h.ctx, h.writer(), 1, unit.Work{CommitID: "recreate/r1", Parts: []unit.Part{sessionstore.CreateTurn(again, nil)}}); !errors.Is(err, sessionstore.ErrTurnExists) {
		t.Fatalf("recreating an ended Turn = %v, want ErrTurnExists", err)
	}
	// An illegal fact sequence does not fold.
	if _, err := store.FoldTurn([]turn.Fact{turn.InputAccepted{Input: input("x")}}); err == nil {
		t.Fatal("fold without created succeeded")
	}
}

// --- 隔离 --------------------------------------------------------------------------------------------

func testIsolation(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	h.startTurn("r2", input("in-b"))
	p1 := h.load("r1").Position
	h.prepare("r2", false)
	h.submitInputs(input("noise"))
	h.mustApply(writer.SemanticGroup{CommitID: "turn-noise", Batches: []writer.TypedBatch{{
		Domain: sessionstore.Stream("t9"),
		Events: []writer.TypedEvent{{Type: sessionstore.TypeStarted, RecordedAtUnixMilli: 1,
			Value: sessionstore.Event{TurnID: "t9", Fact: turn.TurnStarted{TurnID: "t9", Preset: ref.Ref{ID: "b", Digest: "sha256:b"}}}}},
	}}})
	if h.load("r1").Position != p1 {
		t.Fatal("r2, chatlog or turn writes moved r1")
	}
	for _, f := range h.record("r1").Facts {
		if c, ok := f.(turn.TurnStarted); ok && c.TurnID != "r1" {
			t.Fatal("record of r1 contains another Turn")
		}
	}
	if len(h.record("r1").Facts) != 2 || len(h.record("r2").Facts) != 3 {
		t.Fatalf("facts r1=%d r2=%d", len(h.record("r1").Facts), len(h.record("r2").Facts))
	}
}

// --- 接管处置 -----------------------------------------------------------------------------------------

func testTakeover(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	h.startTurn("r2", input("in-b"))
	// r1: executing model; r2: one executing and one pending tool call.
	h.executingModel("r1", false)
	before := h.load("r1")
	// An input delivered while the model runs waits in PendingInputs; the
	// recovery-time plan must include it.
	h.submitInputs(input("in-late"))
	h.mustCommit("r1", schema.Identity().DeriveInputCommandID("r1", "in-late"), 0, turn.NextStep(input("in-late")))
	toolStep, ids := h.openToolStep("r2", 2)
	h.startTool("r2", toolStep, ids[0])

	h.takeover()
	n, err := h.rt.RecoverInterrupted(h.ctx, h.writer(), &reconcile.Reconciler{Abandon: true})
	if err != nil || n != 2 {
		t.Fatalf("RecoverInterrupted = %d %v, want 2", n, err)
	}
	// The unreachable model attempt is withdrawn: the Turn is Open again with
	// the late input still pending and the step no longer counted, so the
	// next Prepare is a fresh decision, not a replay.
	r1 := h.load("r1")
	if _, open := r1.State.Current.(turn.Open); !open || r1.State.Status != turn.TurnActive || r1.State.ModelSteps != 0 ||
		len(r1.State.PendingInputs) != 1 || r1.State.PendingInputs[0].ID != "in-late" {
		t.Fatalf("model Turn after takeover = %+v, want Open with the late input pending", r1.State)
	}
	// The next Prepare is a new decision: a new StepID (its identity derives
	// from the Turn position, which the recovery moved) that consumes the late
	// input. What the request contains is the PromptBuilder's business; the harness
	// plans a fixed request, so only the identities and the input flow are
	// asserted here.
	aborted := mustType[turn.ModelStep](h.t, before.State.Current).RefValue.ID
	replanned := h.prepare("r1", false)
	after := h.load("r1")
	if replanned == aborted || mustType[turn.ModelStep](h.t, after.State.Current).RefValue.ID != replanned {
		t.Fatalf("replan reused the aborted step %s", aborted)
	}
	if len(after.State.PendingInputs) != 0 || after.State.ModelSteps != 1 {
		t.Fatalf("replan left pending=%d steps=%d, want the late input consumed and one counted step", len(after.State.PendingInputs), after.State.ModelSteps)
	}
	r2 := h.load("r2")
	ts := mustType[turn.ToolStep](h.t, r2.State.Current)
	if r2.State.Status != turn.TurnActive || ts.Calls[0].Status != turn.ToolFailed || ts.Calls[0].Failure == nil || ts.Calls[0].Failure.Outcome != turn.ToolOutcomeUnknown {
		t.Fatalf("executing call after takeover = %+v", ts.Calls[0])
	}
	if ts.Calls[1].Status != turn.ToolPending {
		t.Fatalf("pending sibling = %+v, want untouched", ts.Calls[1])
	}
	// The Unknown is the call's terminal fact; the conversation projects it
	// as a tool_result with status unknown and no body.
	rec := h.record("r2")
	found := false
	for _, e := range rec.Events {
		if strings.HasSuffix(string(e.Type), "tool_call_failed") {
			found = true
		}
	}
	if !found {
		t.Fatal("record of r2 has no tool_call_failed")
	}
	var unknown *chatlog.ToolResult
	for _, e := range h.contextEntries() {
		if e.Kind == chatlog.EntryToolResult && e.ToolResult.CallID == chatlog.CallID(ids[0]) {
			unknown = e.ToolResult
		}
	}
	if unknown == nil || unknown.Status != chatlog.ToolUnknown || unknown.OutputDigest != "" || unknown.Failure == nil {
		t.Fatalf("tool_result entry = %+v", unknown)
	}
	// Same owner repeats: idempotent, nothing new.
	head := h.head()
	if n, err := h.rt.RecoverInterrupted(h.ctx, h.writer(), &reconcile.Reconciler{Abandon: true}); err != nil || n != 0 || h.head() != head {
		t.Fatalf("second RecoverInterrupted = %d %v", n, err)
	}
	// Another takeover with nothing Executing does nothing.
	h.takeover()
	if n, err := h.rt.RecoverInterrupted(h.ctx, h.writer(), &reconcile.Reconciler{Abandon: true}); err != nil || n != 0 {
		t.Fatalf("RecoverInterrupted with no executing target = %d %v", n, err)
	}
}

// livePort is an execution store that still holds an attempt for the effects
// it lists and records every key the takeover asked about.
type livePort struct {
	live  map[turn.EffectID]bool
	asked []effect.AssignmentKey
}

func (p *livePort) Attach(_ context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	p.asked = append(p.asked, key)
	if p.live[key.Effect] {
		return effect.Attachment{State: effect.AttachmentActive, Execution: effect.ExecutionRunning}, nil
	}
	return effect.Attachment{State: effect.AttachmentMissing, Execution: effect.ExecutionNotFound}, nil
}

func (p *livePort) Abort(ctx context.Context, key effect.AssignmentKey) (effect.Attachment, error) {
	if p.live[key.Effect] {
		return p.Attach(ctx, key)
	}
	return effect.Attachment{State: effect.AttachmentAborted, Execution: effect.ExecutionAborted}, nil
}
func (p *livePort) Validate(context.Context, effect.Assignment) (*turn.ToolFailure, error) {
	return nil, nil
}
func (p *livePort) Dispatch(context.Context, effect.Assignment) error { return nil }
func (p *livePort) GetStatus(context.Context, effect.AssignmentKey) (effect.ExecutionStatus, error) {
	return effect.ExecutionRunning, nil
}
func (p *livePort) GetOutcome(context.Context, effect.AssignmentKey) (effect.Outcome, error) {
	return effect.Outcome{}, effect.ErrOutcomeNotReady
}
func (p *livePort) Cancel(context.Context, effect.AssignmentKey) error { return nil }

// with a reachable executor: a target whose effect the executor
// still executes is not disposed -- it stays Executing under its original
// effect and that effect's settlement is accepted afterwards -- while a
// target the executor no longer holds is disposed as before.
func testReattach(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	h.startTurn("r2", input("in-b"))
	modelStep, modelEff := h.executingModel("r1", false)
	// Two calls so the step stays open after one is disposed.
	toolStep, ids := h.openToolStep("r2", 2)
	toolEff := h.startTool("r2", toolStep, ids[0])

	h.takeover()
	port := &livePort{live: map[turn.EffectID]bool{modelEff: true}}
	n, err := h.rt.RecoverInterrupted(h.ctx, h.writer(), &reconcile.Reconciler{Executions: port})
	if err != nil || n != 1 {
		t.Fatalf("RecoverInterrupted = %d %v, want exactly the tool disposed", n, err)
	}
	if len(port.asked) != 2 {
		t.Fatalf("takeover asked about %d targets, want 2", len(port.asked))
	}
	for _, key := range port.asked {
		if key.Session != turn.Scope(sid) {
			t.Fatalf("takeover asked outside the session: %+v", key)
		}
		switch key.Effect {
		case modelEff:
			if key.OwnerID != "r1" {
				t.Fatalf("model key = %+v", key)
			}
		case toolEff:
			if key.OwnerID != "r2" {
				t.Fatalf("tool key = %+v", key)
			}
		default:
			t.Fatalf("takeover asked about an unknown effect %q", key.Effect)
		}
	}
	ms := mustType[turn.ModelStep](h.t, h.load("r1").State.Current)
	if ms.Status != turn.ModelExecuting || ms.Effect != modelEff {
		t.Fatalf("reattached model step = %+v, want Executing under the original effect", ms)
	}
	ts := mustType[turn.ToolStep](h.t, h.load("r2").State.Current)
	if ts.Calls[0].Status != turn.ToolFailed || ts.Calls[0].Failure == nil || ts.Calls[0].Failure.Outcome != turn.ToolOutcomeUnknown {
		t.Fatalf("unreachable tool call = %+v, want Unknown", ts.Calls[0])
	}
	if ts.Calls[1].Status != turn.ToolPending {
		t.Fatalf("pending sibling = %+v, want untouched", ts.Calls[1])
	}
	// The Outcome of the effect the executor kept settles under that effect.
	res := h.mustCommit("r1", schema.Identity().DeriveSettlementCommandID(modelEff), 0, turn.SubmitModelResult{StepID: modelStep, Effect: modelEff, Result: textResult("done")})
	if res.Status != store.CommitAccepted || !res.Snapshot.State.Status.Terminal() {
		t.Fatalf("settlement after reattach = %v %v", res.Status, res.Snapshot.State.Status)
	}
	for _, f := range h.record("r1").Facts {
		if _, recovered := f.(turn.ModelStepRecovered); recovered {
			t.Fatal("an effect the executor still executes must not be recovered")
		}
	}
}

// --- 所有权失效 ----------------------------------------------------------------------------------------

func testOwnershipLost(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	step, eff := h.executingModel("r1", false)
	old, oldWriter := h.takeover()
	head := h.head()
	_, err := h.commitWith(old, oldWriter, "r1", schema.Identity().DeriveSettlementCommandID(eff), 0, turn.SubmitModelResult{StepID: step, Effect: eff, Result: textResult("late")})
	if !errors.Is(err, store.ErrOwnershipLost) {
		t.Fatalf("old owner commit = %v, want ErrOwnershipLost", err)
	}
	if h.head() != head {
		t.Fatal("fenced commit reached the ledger")
	}
	// Reading needs no ownership: the superseded process still reads the
	// ledger by SessionID and sees the state as the new owner left it
	// only its Writer's view and commits are fenced.
	if rec, err := old.Record(h.ctx, sid, "r1"); err != nil || mustType[turn.ModelStep](h.t, rec.Snapshot.State.Current).Status != turn.ModelExecuting {
		t.Fatalf("old owner record = %v, want the current state without an ownership error", err)
	}
	// The new owner is unaffected.
	if mustType[turn.ModelStep](h.t, h.load("r1").State.Current).Status != turn.ModelExecuting {
		t.Fatal("new owner's view changed")
	}
}

// --- frozen.Store --------------------------------------------------------------------------------

func testFrozenValues(t *testing.T, factory Factory) {
	h := newHarness(t, factory(t))
	h.startTurn("r1", input("in-1"))
	step, eff := h.executingModel("r1", false)
	digest := mustType[turn.ModelStep](h.t, h.load("r1").State.Current).RequestDigest
	body, _, err := h.frozen.Get(h.ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.frozen.Put(h.ctx, digest, body); err != nil {
		t.Fatalf("idempotent put: %v", err)
	}
	if _, err := h.rt.FrozenRequest(h.ctx, "sha256:unknown"); !errors.Is(err, frozen.ErrMissing) {
		t.Fatalf("unknown digest = %v", err)
	}
	h.mustCommit("r1", schema.Identity().DeriveSettlementCommandID(eff), 0, turn.SubmitModelResult{StepID: step, Effect: eff, Result: textResult("done")})
	// The body is EventBound content the artifact layer retains; Record never
	// depends on it.
	if _, err := h.rt.Record(h.ctx, sid, "r1"); err != nil {
		t.Fatalf("record after settlement: %v", err)
	}
}
