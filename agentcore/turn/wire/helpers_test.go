package wire_test

import (
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/testutil/sdkconv"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/model"
	"github.com/felinics/twilight/agentcore/turn/schema"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

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

// advance runs prepare+start and returns the state in Executing plus stepID.
func advanceToExecuting(t *testing.T, s turn.MachineState, req sdk.Request, specs []turn.ToolSpec) (turn.MachineState, turn.StepID) {
	t.Helper()
	prep, _ := buildPrepare(t, s, req, specs)
	s = fold(t, s, mustDecide(t, s, prep))
	s = fold(t, s, mustDecide(t, s, startModel(s, prep.StepID)))
	return s, prep.StepID
}

// cid is the derived CallID of the index-th call of a step.
func cid(step turn.StepID, index int) turn.CallID {
	return schema.Identity().DeriveCallID(step, index)
}

func cj(raw string) turn.CanonicalJSON { return turn.MustParseCanonicalJSON(raw) }

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

// inputDigest names an input body in tests: the Turn stores only the digest,
// so any content-derived digest stands in for the chatlog's.
func inputDigest(raw string) turn.Digest { return jsonstable.DigestBytes([]byte(raw)) }

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

func modelResultWithCalls(callIDs ...string) model.ModelResult {
	return modelResultWithNamedCalls("t", `{}`, callIDs...)
}

func mustDecide(t *testing.T, s turn.MachineState, c turn.AgentCommand) []turn.Fact {
	t.Helper()
	facts, err := schema.Machine().Decide(s, settling(s, c))
	if err != nil {
		t.Fatalf("schema.Machine().Decide(%T): %v", c, err)
	}
	return facts
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

func newTurn(t *testing.T) turn.MachineState {
	t.Helper()
	s, err := turn.InitializeTurn("run-1")
	if err != nil {
		t.Fatal(err)
	}
	return fold(t, s, mustDecide(t, s, turn.NextStep(turn.AgentInput{ID: "seed", Digest: inputDigest(`{"q":"hi"}`)})))
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
