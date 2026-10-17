package wire_test

import (
	"encoding/json"
	"fmt"
	"github.com/felinics/twilight/agentcore/preset/ref"
	"reflect"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/canonical"
	"github.com/felinics/twilight/agentcore/turn/model"
	"github.com/felinics/twilight/agentcore/turn/wire"
)

func TestCommandEnvelopeJSONRoundTripRestoresVariants(t *testing.T) {
	commands := []turn.AgentCommand{
		turn.PrepareModelRequest{StepID: "s", Model: "m", Request: model.ModelRequest{Model: "m"}, RequestDigest: "sha256:req"},
		turn.WithdrawPreparedStep{StepID: "s"},
		turn.StartModelExecution{StepID: "s", Effect: "effect-s"},
		turn.RecoverModelExecution{StepID: "s", Effect: "effect-s"},
		turn.SubmitModelResult{StepID: "s", Effect: "effect-s", Result: model.ModelResult{Text: "ok"}},
		turn.SubmitModelFailure{StepID: "s", Effect: "effect-s", Failure: turn.StepFailure{Class: turn.FailureProvider, Message: "down"}},
		turn.RejectModelResult{StepID: "s", Effect: "effect-s", Usage: model.Usage{TotalTokens: 1}, Failure: turn.StepFailure{Class: turn.FailureMalformedModel}},
		turn.StartToolCall{StepID: "ts", CallID: "c", Effect: "effect-c"},
		turn.SubmitToolResult{StepID: "ts", CallID: "c", Effect: "effect-c", Result: turn.ToolExecutionResult{Output: cj(`{"ok":true}`)}},
		turn.SubmitToolFailure{StepID: "ts", CallID: "c", Effect: "effect-c", Failure: turn.ToolFailure{Class: turn.FailureExecution}, Outcome: turn.ToolOutcomeKnown},
		turn.DeclineToolCall{StepID: "ts", CallID: "c", Failure: turn.ToolFailure{Class: turn.FailureToolLookup, Message: "no such tool"}},
		turn.ApproveToolCall{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp"},
		turn.RejectToolCall{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp", Reason: "no"},
		turn.SubmitToolResponse{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp", Payload: cj(`{"answer":1}`)},
		turn.CancelTurn{},
		turn.NextStep(turn.AgentInput{ID: "in", Digest: inputDigest(`{"q":"hi"}`)}),
	}
	for _, cmd := range commands {
		env, err := (wire.Facts{}).Envelope("run-1", turn.CommandID("cmd-"+(wire.Facts{}).CommandType(cmd)), cmd)
		if err != nil {
			t.Fatalf("ProtocolV1().BuildEnvelope(%T): %v", cmd, err)
		}
		raw, err := json.Marshal(env)
		if err != nil {
			t.Fatalf("Marshal(%T): %v", cmd, err)
		}
		decoded, err := wire.DecodeCommandEnvelope(raw)
		if err != nil {
			t.Fatalf("DecodeCommandEnvelope(%T): %v\n%s", cmd, err, raw)
		}
		if reflect.TypeOf(decoded.Command) != reflect.TypeOf(cmd) {
			t.Fatalf("decoded command type = %T, want %T", decoded.Command, cmd)
		}
		if decoded.Type != env.Type || decoded.ID != env.ID {
			t.Fatalf("decoded envelope = %+v, want %+v", decoded, env)
		}
	}
}

// Every fact variant round-trips through the v1 fact codec: canonical bytes
// decode back to the same variant and re-encode to the same bytes.
func TestFactCodecRoundTripRestoresVariants(t *testing.T) {
	facts := []turn.Fact{
		turn.TurnStarted{Preset: ref.Ref{ID: "test", Digest: "sha256:test"}, TurnID: "run-1", CausationID: "cause"},
		turn.ModelStepPrepared{StepID: "s", Model: "m", RequestDigest: "sha256:req"},
		turn.ModelStepWithdrawn{StepID: "s"},
		turn.ModelStepStarted{StepID: "s", Effect: "effect-m"},
		turn.ModelStepRecovered{StepID: "s"},
		turn.ModelStepRejected{StepID: "s", Effect: "effect-m", Usage: model.Usage{TotalTokens: 1}, Failure: turn.StepFailure{Class: turn.FailureMalformedModel}},
		turn.ModelStepFailed{StepID: "s", Effect: "effect-m", Failure: turn.StepFailure{Class: turn.FailureProvider, Message: "down"}},
		turn.ModelStepCompleted{StepID: "s", Usage: model.Usage{TotalTokens: 1}, FinishReason: model.FinishReasonStop, ResultDigest: "sha256:result"},
		turn.ToolStepOpened{StepID: "ts", Source: "s", Calls: []turn.ToolCallBinding{{CallID: "c", ToolRef: "t", Arguments: cj(`{}`), Policy: turn.DirectExecution}}},
		turn.ToolCallStarted{StepID: "ts", CallID: "c", Effect: "effect-t"},
		turn.ToolCallApproved{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp"},
		turn.ToolCallCompleted{StepID: "ts", CallID: "c", OutputDigest: "sha256:output"},
		turn.ToolCallAnswered{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp"},
		turn.ToolCallFailed{StepID: "ts", CallID: "c", Failure: turn.ToolFailure{Class: turn.FailureExecution}, Outcome: turn.ToolOutcomeKnown},
		turn.InputAccepted{Input: turn.AgentInput{ID: "in", Digest: inputDigest(`{"q":"hi"}`)}},
		turn.TurnEnded{End: turn.TurnCompletedEnd{}},
	}
	for _, fact := range facts {
		typ := wire.FactType(fact)
		raw, err := jsonstable.MarshalCanonical(fact)
		if err != nil {
			t.Fatalf("marshal(%T): %v", fact, err)
		}
		decoded, err := (wire.Facts{}).DecodeFact(typ, raw)
		if err != nil {
			t.Fatalf("DecodeFact(%T): %v\n%s", fact, err, raw)
		}
		if reflect.TypeOf(decoded) != reflect.TypeOf(fact) {
			t.Fatalf("decoded fact type = %T, want %T", decoded, fact)
		}
		again, err := jsonstable.MarshalCanonical(decoded)
		if err != nil || string(again) != string(raw) {
			t.Fatalf("re-encode of %T differs:\n%s\n%s", fact, raw, again)
		}
		if _, err := (wire.Facts{}).DecodeFact("unknown", raw); err == nil {
			t.Fatalf("unknown fact type decoded for %T", fact)
		}
	}
}

func TestWireCodecRejectsAmbiguousJSONBeforeVariantDecode(t *testing.T) {
	cmd := turn.NextStep(turn.AgentInput{ID: "in", Digest: inputDigest(`1`)})
	env, err := (wire.Facts{}).Envelope("run-1", (canonical.Identity{}).DeriveInputCommandID("run-1", "in"), cmd)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(fmt.Sprintf(`{"schemaVersion":1,"type":"accept_input","turnId":"run-1","id":%q,"command":{"inputs":[{"id":"in","payload":1}],"inputs":[{"id":"in","payload":1}]}}`, env.ID))
	if _, err := wire.DecodeCommandEnvelope(raw); err == nil {
		t.Fatal("duplicate key command decoded")
	}

	canon, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	wrongCase := strings.Replace(string(canon), `"command":`, `"Command":`, 1)
	if _, err := wire.DecodeCommandEnvelope([]byte(wrongCase)); err == nil {
		t.Fatal("case-insensitive command field decoded")
	}
}

func TestWireCodecRejectsUnknownType(t *testing.T) {
	env, err := (wire.Facts{}).Envelope("run-1", "cmd-1", turn.CancelTurn{})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	badType := strings.Replace(string(raw), `"type":"cancel_turn"`, `"type":"unknown"`, 1)
	if _, err := wire.DecodeCommandEnvelope([]byte(badType)); err == nil {
		t.Fatal("unknown command type decoded")
	}
}

// TurnEnded wire is a tagged union: exactly one variant key.
func TestTurnEndedWireIsTaggedUnion(t *testing.T) {
	cases := map[string]turn.TurnEnded{
		"completed": {End: turn.TurnCompletedEnd{}},
		"stopped":   {End: turn.TurnStoppedEnd{Reason: turn.ReasonCancelled, UncertainCalls: []turn.CallID{"c1"}}},
		"failed":    {End: turn.TurnFailedEnd{Reason: turn.ReasonProviderFailure, Failure: turn.TurnFailure{Class: turn.FailureProvider}}},
	}
	for key, fact := range cases {
		raw, err := json.Marshal(fact)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		if len(m) != 1 || m[key] == nil {
			t.Fatalf("%s wire = %s, want single %q key", key, raw, key)
		}
		var back turn.TurnEnded
		if err := json.Unmarshal(raw, &back); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		if fmt.Sprint(back.End) != fmt.Sprint(fact.End) {
			t.Fatalf("%s round trip = %+v, want %+v", key, back.End, fact.End)
		}
	}
	for name, raw := range map[string]string{
		"no variant":        `{}`,
		"two variants":      `{"completed":{},"stopped":{"reason":"cancelled"}}`,
		"legacy flat":       `{"status":1}`,
		"stopped no reason": `{"stopped":{}}`,
	} {
		var back turn.TurnEnded
		if err := json.Unmarshal([]byte(raw), &back); err == nil {
			t.Fatalf("%s accepted: %s", name, raw)
		}
	}
}
