package turn_test

import (
	"errors"
	"github.com/felinics/twilight/agentcore/preset/ref"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/testutil/sdkconv"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/model"
	"github.com/felinics/twilight/agentcore/turn/plan"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// --- helpers ---

const testModel turn.ModelRef = "m-1"

// isOpen reports whether the Turn is at Open, the position between steps.
func isOpen(c turn.Current) bool { _, ok := c.(turn.Open); return ok }

func cj(raw string) turn.CanonicalJSON { return turn.MustParseCanonicalJSON(raw) }

func newTurn(t *testing.T) turn.MachineState {
	t.Helper()
	s, err := turn.InitializeTurn("run-1")
	if err != nil {
		t.Fatal(err)
	}
	return fold(t, s, mustDecide(t, s, turn.NextStep(turn.AgentInput{ID: "seed", Digest: inputDigest(`{"q":"hi"}`)})))
}

func mustDecide(t *testing.T, s turn.MachineState, c turn.AgentCommand) []turn.Fact {
	t.Helper()
	facts, err := schema.Machine().Decide(s, settling(s, c))
	if err != nil {
		t.Fatalf("schema.Machine().Decide(%T): %v", c, err)
	}
	return facts
}

func fold(t *testing.T, s turn.MachineState, facts []turn.Fact) turn.MachineState {
	t.Helper()
	for _, f := range facts {
		var err error
		s, err = schema.Machine().Evolve(s, f)
		if err != nil {
			t.Fatalf("schema.Machine().Evolve(%T): %v", f, err)
		}
	}
	return s
}

func testRequest(tools ...sdk.ToolDefinition) sdk.Request {
	return sdk.Request{
		Model:    "m-1",
		Messages: []sdk.Message{sdk.UserMessage("hi")},
		Tools:    tools,
	}
}

func testToolDef(name string) sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: name, Parameters: &jsonschema.Schema{Type: "object"}}
}

func buildPrepare(t *testing.T, s turn.MachineState, req sdk.Request, specs []turn.ToolSpec) (turn.PrepareModelRequest, turn.CommandID) {
	t.Helper()
	frozenReq, err := sdkconv.FreezeModelRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	reqDigest, err := schema.Canonical().DigestRequest(frozenReq)
	if err != nil {
		t.Fatal(err)
	}
	modelRef := turn.ModelRef(frozenReq.Model)
	cmdID := schema.Identity().DeriveModelRequestCommandID(s.TurnID, 0)
	stepID := schema.Identity().DeriveModelStepID(s.TurnID, cmdID)
	ids := make([]turn.InputID, len(s.PendingInputs))
	for i, in := range s.PendingInputs {
		ids[i] = in.ID
	}
	return turn.PrepareModelRequest{
		StepID:        stepID,
		Model:         modelRef,
		Request:       frozenReq,
		RequestDigest: reqDigest,
		InputIDs:      ids,
		Tools:         specs,
	}, cmdID
}

func makeSpec(t *testing.T, def sdk.ToolDefinition, policy turn.ResponsePolicy) turn.ToolSpec {
	t.Helper()
	frozen, err := sdkconv.FreezeToolDefinition(def)
	if err != nil {
		t.Fatal(err)
	}
	d, err := schema.Canonical().DigestToolDefinition(frozen)
	if err != nil {
		t.Fatal(err)
	}
	return turn.ToolSpec{Ref: turn.ToolRef(def.Name), Name: def.Name, DefinitionDigest: d, Policy: policy}
}

func responseDecisionDigest(t *testing.T, kind turn.ResponseKind, decision turn.ResponseDecision, reason string) turn.Digest {
	t.Helper()
	d, err := schema.Canonical().DigestToolResponseDecision(kind, decision, reason)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func responsePayloadDigest(t *testing.T, payload turn.CanonicalJSON) turn.Digest {
	t.Helper()
	d, err := schema.Canonical().DigestToolResponsePayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// makeBinding builds the binding for the index-th tool call of source, whose
// provider id is providerID. Tests address calls by the derived CallID.
func makeBinding(t *testing.T, source turn.StepID, index int, providerID string, spec turn.ToolSpec, args string) turn.ToolCallBinding {
	t.Helper()
	parsedArgs := cj(args)
	callID := schema.Identity().DeriveCallID(source, index)
	return turn.ToolCallBinding{
		CallID:           callID,
		ProviderCallID:   providerID,
		ToolRef:          spec.Ref,
		DefinitionDigest: spec.DefinitionDigest,
		Arguments:        parsedArgs,
		Policy:           spec.Policy,
	}
}

// cid is the derived CallID of the index-th call of a step.
func cid(step turn.StepID, index int) turn.CallID {
	return schema.Identity().DeriveCallID(step, index)
}

func modelResultWithCalls(callIDs ...string) model.ModelResult {
	return modelResultWithNamedCalls("t", `{}`, callIDs...)
}

// modelResultWithNamedCalls builds a result whose tool calls carry the given
// tool name and argument text — bindings must cross-check against these.
func modelResultWithNamedCalls(toolName, args string, callIDs ...string) model.ModelResult {
	r := sdk.ModelResult{
		Text:         "",
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{InputTokens: 10, OutputTokens: 5, TotalTokens: 15},
	}
	for _, id := range callIDs {
		r.ToolCalls = append(r.ToolCalls, sdk.ToolCall{ToolCallID: id, ToolName: toolName, Input: sdk.ParseToolArguments(args)})
	}
	frozen, err := sdkconv.FreezeModelResult(r)
	if err != nil {
		panic(err)
	}
	return frozen
}

// startModel is the start of the current Prepared step's next model effect
// the step's rejected count is the effect's sequence.
func startModel(s turn.MachineState, stepID turn.StepID) turn.StartModelExecution {
	ms, _ := s.Current.(turn.ModelStep)
	return turn.StartModelExecution{StepID: stepID, Effect: schema.Identity().DeriveEffectID(s.TurnID, stepID, "", ms.Rejects)}
}

// startTool is the start of a call's one tool effect.
func startTool(s turn.MachineState, stepID turn.StepID, callID turn.CallID) turn.StartToolCall {
	return turn.StartToolCall{StepID: stepID, CallID: callID, Effect: schema.Identity().DeriveEffectID(s.TurnID, stepID, callID, 0)}
}

// settling fills the Effect of a settlement command from the state it
// settles: the executing model effect, or the executing effect of the named
// call. Tests that probe the effect check pass it explicitly.
func settling(s turn.MachineState, c turn.AgentCommand) turn.AgentCommand {
	switch cmd := c.(type) {
	case turn.SubmitModelResult:
		if cmd.Effect == "" {
			cmd.Effect = modelEffectOf(s)
		}
		return cmd
	case turn.SubmitModelFailure:
		if cmd.Effect == "" {
			cmd.Effect = modelEffectOf(s)
		}
		return cmd
	case turn.RejectModelResult:
		if cmd.Effect == "" {
			cmd.Effect = modelEffectOf(s)
		}
		return cmd
	case turn.SubmitToolResult:
		if cmd.Effect == "" {
			cmd.Effect = toolEffectOf(s, cmd.CallID)
		}
		return cmd
	case turn.SubmitToolFailure:
		if cmd.Effect == "" {
			cmd.Effect = toolEffectOf(s, cmd.CallID)
		}
		return cmd
	default:
		return c
	}
}

// modelEffectOf is the effect the current ModelStep is executing.
func modelEffectOf(s turn.MachineState) turn.EffectID {
	ms, _ := s.Current.(turn.ModelStep)
	return ms.Effect
}

// toolEffectOf is the effect the named call of the current ToolStep is
// executing.
func toolEffectOf(s turn.MachineState, callID turn.CallID) turn.EffectID {
	ts, _ := s.Current.(turn.ToolStep)
	for _, call := range ts.Calls {
		if call.CallID == callID {
			return call.Effect
		}
	}
	return ""
}

// advance runs prepare+start and returns the state in Executing plus stepID.
func advanceToExecuting(t *testing.T, s turn.MachineState, req sdk.Request, specs []turn.ToolSpec) (turn.MachineState, turn.StepID) {
	t.Helper()
	prep, _ := buildPrepare(t, s, req, specs)
	s = fold(t, s, mustDecide(t, s, prep))
	s = fold(t, s, mustDecide(t, s, startModel(s, prep.StepID)))
	return s, prep.StepID
}

// --- tests ---

func TestInitializeTurnIsMinimal(t *testing.T) {
	s, err := turn.InitializeTurn("r")
	if err != nil {
		t.Fatal(err)
	}
	if s.TurnID != "r" || s.Status != turn.TurnActive || !isOpen(s.Current) || len(s.PendingInputs) != 0 {
		t.Fatalf("initial state = %+v", s)
	}
}

func TestTurnStartedFoldsOntoZeroState(t *testing.T) {
	newTurn, err := turn.BuildNewTurn("r", "cause", ref.Ref{ID: "test", Digest: "sha256:test"})
	if err != nil {
		t.Fatal(err)
	}
	facts, err := schema.Machine().CreateGroup(newTurn, []turn.AgentInput{{ID: "in-1", Digest: inputDigest(`1`)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 2 {
		t.Fatalf("facts = %d, want [created, input_accepted]", len(facts))
	}
	s := fold(t, turn.MachineState{}, facts)
	if s.TurnID != "r" || !isOpen(s.Current) || len(s.PendingInputs) != 1 {
		t.Fatalf("state after create group = %+v", s)
	}
	if _, err := schema.Machine().Evolve(s, facts[0]); err == nil {
		t.Fatal("second TurnStarted folded")
	}
	if _, err := schema.Machine().Evolve(turn.MachineState{}, facts[1]); err == nil {
		t.Fatal("InputAccepted folded before TurnStarted")
	}
}

func TestNextOnFreshRunNeedsModelRequest(t *testing.T) {
	s := newTurn(t)
	eff, err := plan.Next(s)
	if err != nil {
		t.Fatal(err)
	}
	need, ok := eff.(plan.NeedModelRequest)
	if !ok {
		t.Fatalf("action = %T, want NeedModelRequest", eff)
	}
	if len(need.Hint.Inputs) != 1 || need.Hint.Inputs[0].ID != "seed" {
		t.Fatalf("hint inputs = %+v", need.Hint.Inputs)
	}
}

func TestPrepareConsumesInputsAndCounts(t *testing.T) {
	s := newTurn(t)
	prep, _ := buildPrepare(t, s, testRequest(), nil)
	facts := mustDecide(t, s, prep)
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want 1", len(facts))
	}
	s = fold(t, s, facts)
	if len(s.PendingInputs) != 0 {
		t.Fatal("pending inputs not consumed")
	}
	if s.ModelSteps != 1 {
		t.Fatalf("ModelSteps = %d", s.ModelSteps)
	}
	if ms, ok := s.Current.(turn.ModelStep); !ok || ms.Status != turn.ModelPrepared {
		t.Fatalf("current = %#v", s.Current)
	}
}

func TestPrepareRejectsIncompleteInputIDs(t *testing.T) {
	s := newTurn(t)
	prep, _ := buildPrepare(t, s, testRequest(), nil)
	prep.InputIDs = nil
	if _, err := schema.Machine().Decide(s, prep); err == nil {
		t.Fatal("prepare with missing InputIDs accepted")
	}
}

func TestModelCompleteWithToolsOpensToolStep(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, turn.DirectExecution)
	s := newTurn(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []turn.ToolSpec{spec})

	b := makeBinding(t, stepID, 0, "c1", spec, `{"x":1}`)
	facts := mustDecide(t, s, turn.SubmitModelResult{StepID: stepID, Result: modelResultWithNamedCalls("t", `{"x":1}`, "c1"), Calls: []turn.ToolCallBinding{b}})
	if len(facts) != 2 {
		t.Fatalf("facts = %d, want [completed, opened]", len(facts))
	}
	opened, ok := facts[1].(turn.ToolStepOpened)
	if !ok {
		t.Fatalf("facts[1] = %T", facts[1])
	}
	if opened.Source != stepID {
		t.Fatal("tool step source mismatch")
	}
	s = fold(t, s, facts)
	ts, ok := s.Current.(turn.ToolStep)
	if !ok {
		t.Fatalf("current = %T", s.Current)
	}
	if len(ts.Calls) != 1 || ts.Calls[0].Status != turn.ToolPending {
		t.Fatalf("calls = %+v", ts.Calls)
	}
	if err := turn.ValidateToolCallState(ts.Calls[0]); err != nil {
		t.Fatal(err)
	}
}

func TestExternalResponseRequiresPayloadDigest(t *testing.T) {
	def := testToolDef("ask")
	spec := makeSpec(t, def, turn.ExternalResponse)
	s := newTurn(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []turn.ToolSpec{spec})
	b := makeBinding(t, stepID, 0, "c1", spec, `{}`)
	facts := mustDecide(t, s, turn.SubmitModelResult{StepID: stepID, Result: modelResultWithNamedCalls("ask", `{}`, "c1"), Calls: []turn.ToolCallBinding{b}})
	opened := facts[1].(turn.ToolStepOpened)
	s = fold(t, s, facts)
	respID := opened.Calls[0].Response.ID
	payload := cj(`{"answer":"ok"}`)
	if _, err := schema.Machine().Decide(s, turn.SubmitToolResponse{StepID: opened.StepID, CallID: cid(stepID, 0), ResponseID: respID, ResponseDigest: "sha256:bad", Payload: payload}); err == nil {
		t.Fatal("external response with bad payload digest accepted")
	}
	facts = mustDecide(t, s, turn.SubmitToolResponse{StepID: opened.StepID, CallID: cid(stepID, 0), ResponseID: respID,
		ResponseDigest: responsePayloadDigest(t, payload), Payload: payload})
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want [answered]", len(facts))
	}

	s = newTurn(t)
	s, stepID = advanceToExecuting(t, s, testRequest(def), []turn.ToolSpec{spec})
	b = makeBinding(t, stepID, 0, "c1", spec, `{}`)
	facts = mustDecide(t, s, turn.SubmitModelResult{StepID: stepID, Result: modelResultWithNamedCalls("ask", `{}`, "c1"), Calls: []turn.ToolCallBinding{b}})
	opened = facts[1].(turn.ToolStepOpened)
	s = fold(t, s, facts)
	respID = opened.Calls[0].Response.ID
	facts, err := schema.Machine().Decide(s, turn.RejectToolCall{StepID: opened.StepID, CallID: cid(stepID, 0), ResponseID: respID,
		ResponseDigest: responseDecisionDigest(t, turn.ResponseExternal, turn.ResponseDecisionRejected, "user dismissed"), Reason: "user dismissed"})
	if err != nil {
		t.Fatal(err)
	}
	failed := facts[0].(turn.ToolCallFailed)
	if failed.Failure.Class != turn.FailureResponseRejected || failed.Outcome != turn.ToolOutcomeKnown {
		t.Fatalf("failed = %+v", failed)
	}
	s = fold(t, s, facts)
	if s.Status != turn.TurnActive || !isOpen(s.Current) {
		t.Fatal("run should continue after rejecting the external response")
	}
}

func TestToolSchedulingFrozenOnToolStepOpened(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, turn.DirectExecution)
	s := newTurn(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []turn.ToolSpec{spec})
	b := makeBinding(t, stepID, 0, "c1", spec, `{}`)
	facts := mustDecide(t, s, turn.SubmitModelResult{
		StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []turn.ToolCallBinding{b},
		Scheduling: turn.ToolScheduling{Mode: turn.ToolScheduleSequential, MaxParallel: 1},
	})
	s = fold(t, s, facts)
	ts := s.Current.(turn.ToolStep)
	if ts.Scheduling.Mode != turn.ToolScheduleSequential || ts.Scheduling.MaxParallel != 1 {
		t.Fatalf("scheduling = %+v", ts.Scheduling)
	}
}

func TestToolSchedulingRejectsUnknownMode(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, turn.DirectExecution)
	s := newTurn(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []turn.ToolSpec{spec})
	b := makeBinding(t, stepID, 0, "c1", spec, `{}`)
	_, err := schema.Machine().Decide(s, settling(s, turn.SubmitModelResult{
		StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []turn.ToolCallBinding{b},
		Scheduling: turn.ToolScheduling{Mode: "round-robin"},
	}))
	if err == nil {
		t.Fatal("unknown scheduling mode accepted")
	}
}

func TestParallelWaitingDoesNotBlockPending(t *testing.T) {
	defA, defB := testToolDef("a"), testToolDef("b")
	specA := makeSpec(t, defA, turn.ApprovalRequired)
	specB := makeSpec(t, defB, turn.DirectExecution)
	s := newTurn(t)
	s, stepID := advanceToExecuting(t, s, testRequest(defA, defB), []turn.ToolSpec{specA, specB})

	bA := makeBinding(t, stepID, 0, "cA", specA, `{}`)
	bB := makeBinding(t, stepID, 1, "cB", specB, `{}`)
	r, err := sdkconv.FreezeModelResult(sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{TotalTokens: 15},
		ToolCalls: []sdk.ToolCall{
			{ToolCallID: "cA", ToolName: "a", Input: sdk.ParseToolArguments(`{}`)},
			{ToolCallID: "cB", ToolName: "b", Input: sdk.ParseToolArguments(`{}`)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	facts := mustDecide(t, s, turn.SubmitModelResult{StepID: stepID, Result: r, Calls: []turn.ToolCallBinding{bA, bB}})
	opened := facts[1].(turn.ToolStepOpened)
	s = fold(t, s, facts)

	eff, err := plan.Next(s)
	if err != nil {
		t.Fatal(err)
	}
	start, ok := eff.(plan.StartToolCalls)
	if !ok || len(start.CallIDs) != 1 || start.CallIDs[0] != cid(stepID, 1) {
		t.Fatalf("action = %#v, want StartToolCalls[cB]", eff)
	}

	// Complete B; step must stay open because A is Waiting.
	s = fold(t, s, mustDecide(t, s, startTool(s, opened.StepID, cid(stepID, 1))))
	facts = mustDecide(t, s, turn.SubmitToolResult{StepID: opened.StepID, CallID: cid(stepID, 1), Result: turn.ToolExecutionResult{Output: cj(`"ok"`)}})
	if len(facts) != 1 {
		t.Fatalf("facts = %d, step must not close with A waiting", len(facts))
	}
	s = fold(t, s, facts)

	eff, err = plan.Next(s)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := eff.(plan.Idle); !ok {
		t.Fatalf("action after B completed = %#v, want Idle", eff)
	}
	if reqs := turn.WaitingCalls(s); len(reqs) != 1 || reqs[0].CallID != cid(stepID, 0) {
		t.Fatalf("WaitingCalls = %#v", turn.WaitingCalls(s))
	}

	// Answer A via approval; approving moves to Pending, then completing it
	// implicitly closes the step.
	respID := opened.Calls[0].Response.ID
	s = fold(t, s, mustDecide(t, s, turn.ApproveToolCall{StepID: opened.StepID, CallID: cid(stepID, 0), ResponseID: respID,
		ResponseDigest: responseDecisionDigest(t, turn.ResponseApproval, turn.ResponseDecisionApproved, "")}))
	s = fold(t, s, mustDecide(t, s, startTool(s, opened.StepID, cid(stepID, 0))))
	facts = mustDecide(t, s, turn.SubmitToolResult{StepID: opened.StepID, CallID: cid(stepID, 0), Result: turn.ToolExecutionResult{Output: cj(`"done"`)}})
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want [completed]", len(facts))
	}
	s = fold(t, s, facts)
	if !isOpen(s.Current) {
		t.Fatal("tool step should be closed")
	}
}

func TestUnknownToolFailureSettlesOnlyThatCall(t *testing.T) {
	defA, defB := testToolDef("a"), testToolDef("b")
	specA := makeSpec(t, defA, turn.DirectExecution)
	specB := makeSpec(t, defB, turn.DirectExecution)
	s := newTurn(t)
	s, stepID := advanceToExecuting(t, s, testRequest(defA, defB), []turn.ToolSpec{specA, specB})

	bA := makeBinding(t, stepID, 0, "cA", specA, `{}`)
	bB := makeBinding(t, stepID, 1, "cB", specB, `{}`)
	r, err := sdkconv.FreezeModelResult(sdk.ModelResult{
		FinishReason: sdk.FinishReasonToolCalls,
		Usage:        sdk.Usage{TotalTokens: 2},
		ToolCalls: []sdk.ToolCall{
			{ToolCallID: "cA", ToolName: "a", Input: sdk.ParseToolArguments(`{}`)},
			{ToolCallID: "cB", ToolName: "b", Input: sdk.ParseToolArguments(`{}`)},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	facts := mustDecide(t, s, turn.SubmitModelResult{StepID: stepID, Result: r, Calls: []turn.ToolCallBinding{bA, bB}})
	opened := facts[1].(turn.ToolStepOpened)
	s = fold(t, s, facts)
	s = fold(t, s, mustDecide(t, s, startTool(s, opened.StepID, cid(stepID, 0))))
	s = fold(t, s, mustDecide(t, s, startTool(s, opened.StepID, cid(stepID, 1))))

	facts = mustDecide(t, s, turn.SubmitToolFailure{
		StepID:  opened.StepID,
		CallID:  cid(stepID, 0),
		Failure: turn.ToolFailure{Class: turn.FailureEffectUnknown, Message: "lost"},
		Outcome: turn.ToolOutcomeUnknown,
	})
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want [ToolCallFailed]", len(facts))
	}
	failed := facts[0].(turn.ToolCallFailed)
	if failed.CallID != cid(stepID, 0) || failed.Outcome != turn.ToolOutcomeUnknown {
		t.Fatalf("failed = %+v", failed)
	}
	s = fold(t, s, facts)
	if s.Status != turn.TurnActive {
		t.Fatalf("status = %v, want active", s.Status)
	}
	ts, ok := s.Current.(turn.ToolStep)
	if !ok {
		t.Fatalf("current = %T, want ToolStep", s.Current)
	}
	if ts.Calls[0].Status != turn.ToolFailed || ts.Calls[1].Status != turn.ToolExecuting {
		t.Fatalf("calls = %+v", ts.Calls)
	}

	s = fold(t, s, mustDecide(t, s, turn.SubmitToolResult{
		StepID: opened.StepID, CallID: cid(stepID, 1), Result: turn.ToolExecutionResult{Output: cj(`"ok"`)},
	}))
	if s.Status != turn.TurnActive || !isOpen(s.Current) {
		t.Fatalf("after sibling complete: status=%v current=%T", s.Status, s.Current)
	}
	if s.LastToolStep == nil || s.LastToolStep.Calls[0].Status != turn.ToolFailed || s.LastToolStep.Calls[1].Status != turn.ToolCompleted {
		t.Fatalf("LastToolStep = %+v", s.LastToolStep)
	}
}

func TestRejectModelResultDispositionRetriesThenFails(t *testing.T) {
	s := newTurn(t)
	s, stepID := advanceToExecuting(t, s, testRequest(), nil)

	usage := model.Usage{TotalTokens: 3}
	// Reject 1: back to Prepared.
	facts := mustDecide(t, s, turn.RejectModelResult{StepID: stepID, Usage: usage, Failure: turn.StepFailure{Class: turn.FailureMalformedModel}})
	if len(facts) != 1 {
		t.Fatalf("facts = %d", len(facts))
	}
	s = fold(t, s, facts)
	if ms := s.Current.(turn.ModelStep); ms.Status != turn.ModelPrepared || ms.Rejects != 1 {
		t.Fatalf("model step = %+v", ms)
	}
	if s.Usage.TotalTokens != 3 {
		t.Fatal("usage not accumulated on reject")
	}

	// Start again, reject 2: host policy still chooses retry.
	s = fold(t, s, mustDecide(t, s, startModel(s, stepID)))
	s = fold(t, s, mustDecide(t, s, turn.RejectModelResult{StepID: stepID, Usage: usage, Failure: turn.StepFailure{Class: turn.FailureMalformedModel}}))
	if ms := s.Current.(turn.ModelStep); ms.Rejects != 2 {
		t.Fatalf("rejects = %d", ms.Rejects)
	}

	// Third reject: host policy chooses fail-run disposition.
	s = fold(t, s, mustDecide(t, s, startModel(s, stepID)))
	facts = mustDecide(t, s, turn.RejectModelResult{StepID: stepID, Usage: usage, Failure: turn.StepFailure{Class: turn.FailureMalformedModel}, Disposition: turn.ModelRejectFailTurn})
	if len(facts) != 2 {
		t.Fatalf("facts = %d, want [rejected, ended]", len(facts))
	}
	s = fold(t, s, facts)
	if s.Status != turn.TurnFailed || s.Result.Reason != turn.ReasonMalformedModel {
		t.Fatalf("result = %+v", s.Result)
	}
	if s.Usage.TotalTokens != 9 {
		t.Fatalf("usage = %d, want 9", s.Usage.TotalTokens)
	}
}

func TestAcceptInputDuplicateIsGuarded(t *testing.T) {
	s := newTurn(t)
	facts := mustDecide(t, s, turn.NextStep(turn.AgentInput{ID: "in-2", Digest: inputDigest(`1`)}))
	s = fold(t, s, facts)
	if len(s.PendingInputs) != 2 {
		t.Fatalf("pending = %d", len(s.PendingInputs))
	}
	// Decide rejects a duplicate and an exact command replay never reaches
	// Evolve, so a persisted duplicate InputAccepted is a corrupt log: the
	// guard refuses it instead of silently deduplicating.
	if _, err := schema.Machine().Evolve(s, facts[0]); err == nil {
		t.Fatal("duplicate InputAccepted folded silently")
	}
}

// Inputs queue in every non-terminal state. A Prepared step whose
// request predates the input is withdrawn and replanned; an Executing step
// keeps the input for the Open that follows it.
func TestAcceptInputQueuesInAnyActiveState(t *testing.T) {
	s := newTurn(t)
	prep, _ := buildPrepare(t, s, testRequest(), nil)
	s = fold(t, s, mustDecide(t, s, prep))

	// Prepared: input queues, Next withdraws, withdraw reopens with the input.
	s = fold(t, s, mustDecide(t, s, turn.NextStep(turn.AgentInput{ID: "in-3", Digest: inputDigest(`3`)})))
	if len(s.PendingInputs) != 1 || s.PendingInputs[0].ID != "in-3" {
		t.Fatalf("pending after accept while Prepared = %+v", s.PendingInputs)
	}
	eff, err := plan.Next(s)
	if err != nil {
		t.Fatal(err)
	}
	withdraw, ok := eff.(plan.WithdrawPrepared)
	if !ok || withdraw.StepID != prep.StepID {
		t.Fatalf("action = %#v, want WithdrawPrepared", eff)
	}
	facts := mustDecide(t, s, turn.WithdrawPreparedStep{StepID: prep.StepID})
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want [withdrawn]", len(facts))
	}
	s = fold(t, s, facts)
	if !isOpen(s.Current) || s.ModelSteps != 0 || len(s.PendingInputs) != 1 {
		t.Fatalf("state after withdraw = %+v", s)
	}
	// Withdraw without pending inputs is rejected: the request is complete.
	prep2, _ := buildPrepare(t, s, testRequest(), nil)
	s = fold(t, s, mustDecide(t, s, prep2))
	if _, err := schema.Machine().Decide(s, turn.WithdrawPreparedStep{StepID: prep2.StepID}); err == nil {
		t.Fatal("withdraw accepted with no pending inputs")
	}

	// Executing: input queues, Next stays Idle, no tool calls + pending input
	// returns to Open instead of ending the Turn.
	s = fold(t, s, mustDecide(t, s, startModel(s, prep2.StepID)))
	s = fold(t, s, mustDecide(t, s, turn.NextStep(turn.AgentInput{ID: "in-4", Digest: inputDigest(`4`)})))
	if eff, _ := plan.Next(s); eff != (plan.Idle{}) {
		t.Fatalf("action while Executing with pending input = %#v, want Idle", eff)
	}
	result, err := sdkconv.FreezeModelResult(sdk.ModelResult{Text: "answer", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}})
	if err != nil {
		t.Fatal(err)
	}
	facts = mustDecide(t, s, turn.SubmitModelResult{StepID: prep2.StepID, Result: result})
	if len(facts) != 1 {
		t.Fatalf("facts = %d, want [completed] without TurnEnded while inputs are pending", len(facts))
	}
	completed := facts[0].(turn.ModelStepCompleted)
	wantDigest, err := schema.Canonical().DigestModelResult(result)
	if err != nil {
		t.Fatal(err)
	}
	if completed.ResultDigest != wantDigest || completed.Usage.TotalTokens != 1 || completed.FinishReason != model.FinishReasonStop {
		t.Fatalf("completed = %+v", completed)
	}
	s = fold(t, s, facts)
	if s.Status != turn.TurnActive || !isOpen(s.Current) || len(s.PendingInputs) != 1 || s.PendingInputs[0].ID != "in-4" {
		t.Fatalf("state after completed with pending input = %+v", s)
	}
	if eff, _ := plan.Next(s); eff == nil {
		t.Fatal("no effect at Open")
	} else if _, ok := eff.(plan.NeedModelRequest); !ok {
		t.Fatalf("action = %#v, want NeedModelRequest", eff)
	}
}

func TestAcceptInputRejectsSeedDuplicateID(t *testing.T) {
	s := newTurn(t)
	_, err := schema.Machine().Decide(s, turn.NextStep(turn.AgentInput{ID: "seed", Digest: inputDigest(`{"q":"other"}`)}))
	if !errors.Is(err, turn.ErrCommandConflict) {
		t.Fatalf("duplicate seed input err = %v, want ErrCommandConflict", err)
	}
}

func TestEvolvePreparedRequiresCompleteOrderedPendingInputs(t *testing.T) {
	minimal, err := turn.InitializeTurn("run-1")
	if err != nil {
		t.Fatal(err)
	}
	withInputs := func(ids ...turn.InputID) turn.MachineState {
		t.Helper()
		s := minimal
		for _, id := range ids {
			var foldErr error
			s, foldErr = schema.Machine().Evolve(s, turn.InputAccepted{Input: turn.AgentInput{ID: id, Digest: inputDigest(`null`)}})
			if foldErr != nil {
				t.Fatal(foldErr)
			}
		}
		return s
	}
	prepared := func(ids ...turn.InputID) turn.ModelStepPrepared {
		request := model.ModelRequest{Model: string(testModel)}
		requestDigest, err := schema.Canonical().DigestRequest(request)
		if err != nil {
			t.Fatal(err)
		}
		return turn.ModelStepPrepared{StepID: "step-1", Model: testModel, RequestDigest: requestDigest, InputIDs: ids}
	}

	t.Run("nonexistent input", func(t *testing.T) {
		s := withInputs("in-1")
		if _, err := schema.Machine().Evolve(s, prepared("missing")); err == nil {
			t.Fatal("ModelStepPrepared consuming a nonexistent input folded")
		}
	})
	t.Run("length mismatch", func(t *testing.T) {
		s := withInputs("in-1", "in-2")
		if _, err := schema.Machine().Evolve(s, prepared("in-1")); err == nil {
			t.Fatal("ModelStepPrepared consuming only a pending-input prefix folded")
		}
	})
	t.Run("order mismatch", func(t *testing.T) {
		s := withInputs("in-1", "in-2")
		if _, err := schema.Machine().Evolve(s, prepared("in-2", "in-1")); err == nil {
			t.Fatal("ModelStepPrepared consuming pending inputs out of order folded")
		}
	})
	t.Run("complete ordered IDs", func(t *testing.T) {
		s := withInputs("in-1", "in-2")
		next, err := schema.Machine().Evolve(s, prepared("in-1", "in-2"))
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := next.Current.(turn.ModelStep); !ok || len(next.PendingInputs) != 0 {
			t.Fatalf("prepared state = %+v", next)
		}
	})
}

func TestEvolveRejectsModelPrepareOverCurrentStep(t *testing.T) {
	s := newTurn(t)
	s, _ = advanceToExecuting(t, s, testRequest(), nil)
	_, err := schema.Machine().Evolve(s, turn.ModelStepPrepared{
		StepID:        "other",
		Model:         testModel,
		RequestDigest: "sha256:req",
	})
	if err == nil {
		t.Fatal("Evolve accepted ModelStepPrepared over an existing step")
	}
}

// A provider that repeats or omits tool_call_id cannot break Turn identity:
// the derived CallID is unique per (step, index) and the provider id rides
// along as ProviderCallID.
func TestDerivedCallIDToleratesProviderIDReuse(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, turn.DirectExecution)
	s := newTurn(t)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []turn.ToolSpec{spec})
	result := modelResultWithNamedCalls("t", `{}`, "call_0", "call_0", "")
	bindings := []turn.ToolCallBinding{
		makeBinding(t, stepID, 0, "call_0", spec, `{}`),
		makeBinding(t, stepID, 1, "call_0", spec, `{}`),
		makeBinding(t, stepID, 2, "", spec, `{}`),
	}
	facts := mustDecide(t, s, turn.SubmitModelResult{StepID: stepID, Result: result, Calls: bindings})
	opened := facts[1].(turn.ToolStepOpened)
	seen := map[turn.CallID]bool{}
	for i, c := range opened.Calls {
		if c.CallID != cid(stepID, i) || seen[c.CallID] {
			t.Fatalf("call %d id = %s", i, c.CallID)
		}
		seen[c.CallID] = true
	}
	if opened.Calls[0].ProviderCallID != "call_0" || opened.Calls[2].ProviderCallID != "" {
		t.Fatalf("provider ids = %+v", opened.Calls)
	}
	// A binding whose CallID is not the derived one is rejected.
	forged := bindings
	forged[1].CallID = "call_0"
	if _, err := schema.Machine().Decide(s, settling(s, turn.SubmitModelResult{StepID: stepID, Result: result, Calls: forged})); err == nil {
		t.Fatal("non-derived CallID accepted")
	}
}

// TestSettlementNamesExecutingEffect: a settlement carries the effect it
// closes and Decide checks it against the Executing step or call.
// Decide is called directly because mustDecide fills a missing effect in.
func TestSettlementNamesExecutingEffect(t *testing.T) {
	defA, defB := testToolDef("a"), testToolDef("b")
	specA, specB := makeSpec(t, defA, turn.DirectExecution), makeSpec(t, defB, turn.DirectExecution)
	model := newTurn(t)
	model, modelStep := advanceToExecuting(t, model, testRequest(defA, defB), []turn.ToolSpec{specA, specB})
	modelEff := modelEffectOf(model)
	otherModel := schema.Identity().DeriveEffectID(model.TurnID, modelStep, "", 7)
	text, err := sdkconv.FreezeModelResult(sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop})
	if err != nil {
		t.Fatal(err)
	}

	bA, bB := makeBinding(t, modelStep, 0, "cA", specA, `{}`), makeBinding(t, modelStep, 1, "cB", specB, `{}`)
	calls, err := sdkconv.FreezeModelResult(sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls,
		ToolCalls: []sdk.ToolCall{{ToolCallID: "cA", ToolName: "a", Input: sdk.ParseToolArguments(`{}`)}, {ToolCallID: "cB", ToolName: "b", Input: sdk.ParseToolArguments(`{}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	facts := mustDecide(t, model, turn.SubmitModelResult{StepID: modelStep, Result: calls, Calls: []turn.ToolCallBinding{bA, bB}})
	toolStep := facts[1].(turn.ToolStepOpened).StepID
	tool := fold(t, model, facts)
	cA, cB := cid(modelStep, 0), cid(modelStep, 1)
	tool = fold(t, tool, mustDecide(t, tool, startTool(tool, toolStep, cA)))
	toolEff := toolEffectOf(tool, cA)
	otherTool := schema.Identity().DeriveEffectID(tool.TurnID, toolStep, cA, 1)
	failure := turn.StepFailure{Class: turn.FailureProvider, Message: "down"}
	toolFailure := turn.ToolFailure{Class: turn.FailureExecution, Message: "boom"}
	output := turn.ToolExecutionResult{Output: cj(`"ok"`)}

	rejected := []struct {
		name string
		s    turn.MachineState
		cmd  turn.AgentCommand
		want string
	}{
		{"model result without effect", model, turn.SubmitModelResult{StepID: modelStep, Result: text}, "missing effect"},
		{"model result of another effect", model, turn.SubmitModelResult{StepID: modelStep, Effect: otherModel, Result: text}, "not the step's executing effect"},
		{"model failure without effect", model, turn.SubmitModelFailure{StepID: modelStep, Failure: failure}, "missing effect"},
		{"model failure of another effect", model, turn.SubmitModelFailure{StepID: modelStep, Effect: otherModel, Failure: failure}, "not the step's executing effect"},
		{"reject without effect", model, turn.RejectModelResult{StepID: modelStep, Failure: failure, Disposition: turn.ModelRejectRetry}, "missing effect"},
		{"reject of another effect", model, turn.RejectModelResult{StepID: modelStep, Effect: otherModel, Failure: failure, Disposition: turn.ModelRejectRetry}, "not the step's executing effect"},
		{"tool result without effect", tool, turn.SubmitToolResult{StepID: toolStep, CallID: cA, Result: output}, "missing effect"},
		{"tool result of another effect", tool, turn.SubmitToolResult{StepID: toolStep, CallID: cA, Effect: otherTool, Result: output}, "not the executing effect"},
		{"tool failure without effect", tool, turn.SubmitToolFailure{StepID: toolStep, CallID: cA, Failure: toolFailure, Outcome: turn.ToolOutcomeKnown}, "missing effect"},
		{"tool failure of another effect", tool, turn.SubmitToolFailure{StepID: toolStep, CallID: cA, Effect: otherTool, Failure: toolFailure, Outcome: turn.ToolOutcomeKnown}, "not the executing effect"},
		{"unknown tool failure of another effect", tool, turn.SubmitToolFailure{StepID: toolStep, CallID: cA, Effect: otherTool, Outcome: turn.ToolOutcomeUnknown}, "not the executing effect"},
		// A Pending call has no effect to settle: it is declined, not failed.
		{"known failure of a Pending call", tool, turn.SubmitToolFailure{StepID: toolStep, CallID: cB, Effect: schema.Identity().DeriveEffectID(tool.TurnID, toolStep, cB, 0),
			Failure: toolFailure, Outcome: turn.ToolOutcomeKnown}, "not Executing"},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			_, err := schema.Machine().Decide(tc.s, tc.cmd)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Decide(%T) = %v, want rejection containing %q", tc.cmd, err, tc.want)
			}
		})
	}

	// The executing effect settles.
	if _, err := schema.Machine().Decide(model, turn.SubmitModelResult{StepID: modelStep, Effect: modelEff, Result: text}); err != nil {
		t.Fatalf("model settlement under its effect: %v", err)
	}
	if _, err := schema.Machine().Decide(tool, turn.SubmitToolResult{StepID: toolStep, CallID: cA, Effect: toolEff, Result: output}); err != nil {
		t.Fatalf("tool settlement under its effect: %v", err)
	}
}

// TestDeclineToolCallFailsPendingCallWithoutStart: DeclineToolCall records
// the Known failure of a Pending call with no start and no effect;
// any other call state is rejected.
func TestDeclineToolCallFailsPendingCallWithoutStart(t *testing.T) {
	defA, defB := testToolDef("a"), testToolDef("b")
	specA, specB := makeSpec(t, defA, turn.DirectExecution), makeSpec(t, defB, turn.DirectExecution)
	s := newTurn(t)
	s, modelStep := advanceToExecuting(t, s, testRequest(defA, defB), []turn.ToolSpec{specA, specB})
	bA, bB := makeBinding(t, modelStep, 0, "cA", specA, `{}`), makeBinding(t, modelStep, 1, "cB", specB, `{}`)
	r, err := sdkconv.FreezeModelResult(sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls,
		ToolCalls: []sdk.ToolCall{{ToolCallID: "cA", ToolName: "a", Input: sdk.ParseToolArguments(`{}`)}, {ToolCallID: "cB", ToolName: "b", Input: sdk.ParseToolArguments(`{}`)}}})
	if err != nil {
		t.Fatal(err)
	}
	facts := mustDecide(t, s, turn.SubmitModelResult{StepID: modelStep, Result: r, Calls: []turn.ToolCallBinding{bA, bB}})
	step := facts[1].(turn.ToolStepOpened).StepID
	s = fold(t, s, facts)
	cA, cB := cid(modelStep, 0), cid(modelStep, 1)
	s = fold(t, s, mustDecide(t, s, startTool(s, step, cB)))
	failure := turn.ToolFailure{Class: turn.FailureToolLookup, Message: "no such tool"}

	rejected := []struct {
		name string
		cmd  turn.DeclineToolCall
	}{
		{"executing call", turn.DeclineToolCall{StepID: step, CallID: cB, Failure: failure}},
		{"unknown call", turn.DeclineToolCall{StepID: step, CallID: "ghost", Failure: failure}},
		{"empty failure class", turn.DeclineToolCall{StepID: step, CallID: cA}},
	}
	for _, tc := range rejected {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := schema.Machine().Decide(s, tc.cmd); err == nil {
				t.Fatalf("decline of %s accepted", tc.name)
			}
		})
	}

	facts = mustDecide(t, s, turn.DeclineToolCall{StepID: step, CallID: cA, Failure: failure})
	if len(facts) != 1 {
		t.Fatalf("decline facts = %d, want [ToolCallFailed] and no ToolCallStarted", len(facts))
	}
	failed, ok := facts[0].(turn.ToolCallFailed)
	if !ok || failed.CallID != cA || failed.Outcome != turn.ToolOutcomeKnown || failed.Failure != failure {
		t.Fatalf("decline fact = %+v", facts[0])
	}
	s = fold(t, s, facts)
	ts := s.Current.(turn.ToolStep)
	if ts.Calls[0].Status != turn.ToolFailed || ts.Calls[0].Effect != "" {
		t.Fatalf("declined call = %+v, want Failed with no effect", ts.Calls[0])
	}
	// A declined call is terminal: it is neither declined nor settled again.
	if _, err := schema.Machine().Decide(s, turn.DeclineToolCall{StepID: step, CallID: cA, Failure: failure}); err == nil {
		t.Fatal("second decline accepted")
	}
	// The step closes once its Executing sibling settles.
	s = fold(t, s, mustDecide(t, s, turn.SubmitToolResult{StepID: step, CallID: cB, Result: turn.ToolExecutionResult{Output: cj(`"ok"`)}}))
	if !isOpen(s.Current) || s.LastToolStep == nil || s.LastToolStep.Calls[0].Status != turn.ToolFailed {
		t.Fatalf("after sibling settled: current=%T last=%+v", s.Current, s.LastToolStep)
	}
}
