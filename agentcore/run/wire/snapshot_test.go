package wire_test

import (
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/wire"
)

// The snapshot codec round-trips every Current variant and the terminal
// shape, and its bytes equal StatesEquivalent's identity.
func TestSnapshotCodecRoundTrip(t *testing.T) {
	def := testToolDef("t")
	spec := makeSpec(t, def, run.DirectExecution)
	check := func(name string, s run.MachineState) {
		t.Helper()
		raw, err := (wire.Snapshot{}).Encode(&s)
		if err != nil {
			t.Fatalf("%s: encode: %v", name, err)
		}
		decoded, err := (wire.Snapshot{}).Decode(raw)
		if err != nil {
			t.Fatalf("%s: decode: %v\n%s", name, err, raw)
		}
		if !wire.StatesEquivalent(&s, &decoded) {
			t.Fatalf("%s: round trip changed state\n%s", name, raw)
		}
		if (s.Current == nil) != (decoded.Current == nil) {
			t.Fatalf("%s: Current presence changed: %T -> %T", name, s.Current, decoded.Current)
		}
	}

	s := newRun(t)
	check("open", s)
	s, stepID := advanceToExecuting(t, s, testRequest(def), []run.ToolSpec{spec})
	check("model executing", s)
	b := makeBinding(t, stepID, 0, "c1", spec, `{}`)
	facts := mustDecide(t, s, run.SubmitModelResult{StepID: stepID, Result: modelResultWithCalls("c1"), Calls: []run.ToolCallBinding{b}})
	s = fold(t, s, facts)
	check("tool step pending", s)
	toolStep := facts[1].(run.ToolStepOpened).StepID
	s = fold(t, s, mustDecide(t, s, startTool(s, toolStep, cid(stepID, 0))))
	check("tool step executing", s)
	s = fold(t, s, mustDecide(t, s, run.SubmitToolResult{StepID: toolStep, CallID: cid(stepID, 0), Result: run.ToolExecutionResult{Output: cj(`"ok"`)}}))
	check("open with last tool step", s)
	s = fold(t, s, mustDecide(t, s, run.CancelRun{}))
	check("terminal", s)
}

func TestSnapshotCodecRejectsMalformedWire(t *testing.T) {
	initial, err := run.InitializeRun("run-1")
	if err != nil {
		t.Fatal(err)
	}
	good, err := (wire.Snapshot{}).Encode(&initial)
	if err != nil {
		t.Fatal(err)
	}
	for name, raw := range map[string]string{
		"unknown field":          strings.Replace(string(good), `"runId"`, `"extra":1,"runId"`, 1),
		"unknown current":        strings.Replace(string(good), `"current":"open"`, `"current":"weird"`, 1),
		"open with step body":    strings.Replace(string(good), `"current":"open"`, `"current":"open","modelStep":{}`, 1),
		"active without current": strings.Replace(string(good), `"current":"open",`, ``, 1),
		"trailing data":          string(good) + `{}`,
	} {
		if _, err := (wire.Snapshot{}).Decode([]byte(raw)); err == nil {
			t.Fatalf("%s: accepted\n%s", name, raw)
		}
	}
	if _, err := (wire.Snapshot{}).Decode(good); err != nil {
		t.Fatalf("canonical wire rejected: %v", err)
	}
}
