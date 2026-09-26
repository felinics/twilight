package wire_test

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/canonical"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/wire"
)

func TestCommandEnvelopeJSONRoundTripRestoresVariants(t *testing.T) {
	commands := []run.AgentCommand{
		run.PrepareModelRequest{StepID: "s", Model: "m", Request: model.ModelRequest{Model: "m"}, RequestDigest: "sha256:req"},
		run.WithdrawPreparedStep{StepID: "s"},
		run.StartModelExecution{StepID: "s", Effect: "effect-s"},
		run.RecoverModelExecution{StepID: "s", Effect: "effect-s"},
		run.SubmitModelResult{StepID: "s", Effect: "effect-s", Result: model.ModelResult{Text: "ok"}},
		run.SubmitModelFailure{StepID: "s", Effect: "effect-s", Failure: run.StepFailure{Class: run.FailureProvider, Message: "down"}},
		run.RejectModelResult{StepID: "s", Effect: "effect-s", Usage: model.Usage{TotalTokens: 1}, Failure: run.StepFailure{Class: run.FailureMalformedModel}},
		run.StartToolCall{StepID: "ts", CallID: "c", Effect: "effect-c"},
		run.SubmitToolResult{StepID: "ts", CallID: "c", Effect: "effect-c", Result: run.ToolExecutionResult{Output: cj(`{"ok":true}`)}},
		run.SubmitToolFailure{StepID: "ts", CallID: "c", Effect: "effect-c", Failure: run.ToolFailure{Class: run.FailureExecution}, Outcome: run.ToolOutcomeKnown},
		run.DeclineToolCall{StepID: "ts", CallID: "c", Failure: run.ToolFailure{Class: run.FailureToolLookup, Message: "no such tool"}},
		run.ApproveToolCall{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp"},
		run.RejectToolCall{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp", Reason: "no"},
		run.SubmitToolResponse{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp", Payload: cj(`{"answer":1}`)},
		run.CancelRun{},
		run.NextStep(run.AgentInput{ID: "in", Digest: inputDigest(`{"q":"hi"}`)}),
	}
	for _, cmd := range commands {
		env, err := (wire.Facts{}).Envelope("run-1", run.CommandID("cmd-"+(wire.Facts{}).CommandType(cmd)), cmd)
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
	facts := []run.Fact{
		run.RunCreated{RunID: "run-1", CausationID: "cause"},
		run.ModelStepPrepared{StepID: "s", Model: "m", RequestDigest: "sha256:req"},
		run.ModelStepWithdrawn{StepID: "s"},
		run.ModelStepStarted{StepID: "s", Effect: "effect-m"},
		run.ModelStepRecovered{StepID: "s"},
		run.ModelStepRejected{StepID: "s", Effect: "effect-m", Usage: model.Usage{TotalTokens: 1}, Failure: run.StepFailure{Class: run.FailureMalformedModel}},
		run.ModelStepFailed{StepID: "s", Effect: "effect-m", Failure: run.StepFailure{Class: run.FailureProvider, Message: "down"}},
		run.ModelStepCompleted{StepID: "s", Usage: model.Usage{TotalTokens: 1}, FinishReason: model.FinishReasonStop, ResultDigest: "sha256:result"},
		run.ToolStepOpened{StepID: "ts", Source: "s", Calls: []run.ToolCallBinding{{CallID: "c", ToolRef: "t", Arguments: cj(`{}`), Policy: run.DirectExecution}}},
		run.ToolCallStarted{StepID: "ts", CallID: "c", Effect: "effect-t"},
		run.ToolCallApproved{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp"},
		run.ToolCallCompleted{StepID: "ts", CallID: "c", OutputDigest: "sha256:output"},
		run.ToolCallAnswered{StepID: "ts", CallID: "c", ResponseID: "r", ResponseDigest: "sha256:resp"},
		run.ToolCallFailed{StepID: "ts", CallID: "c", Failure: run.ToolFailure{Class: run.FailureExecution}, Outcome: run.ToolOutcomeKnown},
		run.InputAccepted{Input: run.AgentInput{ID: "in", Digest: inputDigest(`{"q":"hi"}`)}},
		run.RunEnded{End: run.RunCompletedEnd{}},
	}
	for _, fact := range facts {
		typ := wire.FactType(fact)
		raw, err := es.MarshalCanonical(fact)
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
		again, err := es.MarshalCanonical(decoded)
		if err != nil || string(again) != string(raw) {
			t.Fatalf("re-encode of %T differs:\n%s\n%s", fact, raw, again)
		}
		if _, err := (wire.Facts{}).DecodeFact("unknown", raw); err == nil {
			t.Fatalf("unknown fact type decoded for %T", fact)
		}
	}
}

func TestWireCodecRejectsAmbiguousJSONBeforeVariantDecode(t *testing.T) {
	cmd := run.NextStep(run.AgentInput{ID: "in", Digest: inputDigest(`1`)})
	env, err := (wire.Facts{}).Envelope("run-1", (canonical.Identity{}).DeriveInputCommandID("run-1", "in"), cmd)
	if err != nil {
		t.Fatal(err)
	}
	raw := []byte(fmt.Sprintf(`{"schemaVersion":1,"type":"accept_input","runId":"run-1","id":%q,"command":{"inputs":[{"id":"in","payload":1}],"inputs":[{"id":"in","payload":1}]}}`, env.ID))
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
	env, err := (wire.Facts{}).Envelope("run-1", "cmd-1", run.CancelRun{})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	badType := strings.Replace(string(raw), `"type":"cancel_run"`, `"type":"unknown"`, 1)
	if _, err := wire.DecodeCommandEnvelope([]byte(badType)); err == nil {
		t.Fatal("unknown command type decoded")
	}
}

// RunEnded wire is a tagged union: exactly one variant key.
func TestRunEndedWireIsTaggedUnion(t *testing.T) {
	cases := map[string]run.RunEnded{
		"completed": {End: run.RunCompletedEnd{}},
		"stopped":   {End: run.RunStoppedEnd{Reason: run.ReasonCancelled, UncertainCalls: []run.CallID{"c1"}}},
		"failed":    {End: run.RunFailedEnd{Reason: run.ReasonProviderFailure, Failure: run.RunFailure{Class: run.FailureProvider}}},
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
		var back run.RunEnded
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
		var back run.RunEnded
		if err := json.Unmarshal([]byte(raw), &back); err == nil {
			t.Fatalf("%s accepted: %s", name, raw)
		}
	}
}
