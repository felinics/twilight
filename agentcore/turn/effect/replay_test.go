package effect_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
)

// The tool's Replay policy travels with the Assignment across the wire and
// the execution record; an unjudged tool omits the field, so an
// Assignment written before the declaration existed still decodes as unknown.
func TestToolAssignmentCarriesReplayPolicy(t *testing.T) {
	cases := []struct {
		name    string
		policy  turn.ReplayPolicy
		onWire  bool
		decoded turn.ReplayPolicy
	}{
		{"allowed", turn.ReplayAllowed, true, turn.ReplayAllowed},
		{"forbidden", turn.ReplayForbidden, true, turn.ReplayForbidden},
		{"unknown", turn.ReplayUnknown, false, turn.ReplayUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := effect.Assignment{Session: "s", OwnerKind: effect.OwnerTurn, OwnerID: string("r"), StepID: "step", CallID: "c1", Effect: "e",
				Body: effect.ToolAssignment{ToolRef: "lookup", Arguments: turn.MustParseCanonicalJSON(`{}`), Policy: turn.DirectExecution, Replay: tc.policy}}
			raw, err := json.Marshal(a)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), `"Replay"`) != tc.onWire {
				t.Fatalf("wire = %s, want replay present=%v", raw, tc.onWire)
			}
			var back effect.Assignment
			if err := json.Unmarshal(raw, &back); err != nil {
				t.Fatal(err)
			}
			tool, ok := back.Tool()
			if !ok || tool.Replay != tc.decoded {
				t.Fatalf("decoded replay = %v (%v), want %v", tool.Replay, ok, tc.decoded)
			}
		})
	}
}
