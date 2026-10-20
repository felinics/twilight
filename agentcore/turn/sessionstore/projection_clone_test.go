package sessionstore

import (
	"reflect"
	"testing"

	"github.com/felinics/twilight/agentcore/turn"
)

func TestMachineCloneMatchesCodecAndDetaches(t *testing.T) {
	state, _ := turn.InitializeTurn("t")
	state.PendingInputs = []turn.AgentInput{{ID: "input", Digest: "digest"}}
	input := Machine{Active: map[turn.TurnID]turn.MachineState{"t": state}, Positions: map[turn.TurnID]turn.TurnPosition{"t": 3, "untracked": 9}}
	codec := machineCodec{}
	encoded, err := codec.Encode(input)
	if err != nil {
		t.Fatal(err)
	}
	expected, err := codec.Decode(encoded)
	if err != nil {
		t.Fatal(err)
	}
	copied, err := MachineProjection.CloneState(input)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(copied, expected) {
		t.Fatalf("clone differs: %#v / %#v", copied, expected)
	}
	clone := copied.(Machine)
	current := clone.Active["t"]
	current.PendingInputs[0].ID = "mutated"
	clone.Positions["t"] = 10
	delete(clone.Active, "t")
	if input.Active["t"].PendingInputs[0].ID != "input" || input.Positions["t"] != 3 {
		t.Fatal("clone mutated original maps or nested state")
	}
	for _, bad := range []any{nil, "bad", Machine{Active: map[turn.TurnID]turn.MachineState{"t": {}}}} {
		if _, err := MachineProjection.CloneState(bad); err == nil {
			t.Fatalf("accepted invalid state: %#v", bad)
		}
	}
}

func TestMachineClonePreservesOuterCodecNormalization(t *testing.T) {
	state, _ := turn.InitializeTurn("t")
	for name, source := range map[string]Machine{
		"invalid key":       {Active: map[turn.TurnID]turn.MachineState{turn.TurnID("t\xff"): state}},
		"rounded position":  {Active: map[turn.TurnID]turn.MachineState{"t": state}, Positions: map[turn.TurnID]turn.TurnPosition{"t": 1<<53 + 1}},
		"overflow position": {Active: map[turn.TurnID]turn.MachineState{"t": state}, Positions: map[turn.TurnID]turn.TurnPosition{"t": ^turn.TurnPosition(0)}},
	} {
		t.Run(name, func(t *testing.T) {
			codec := machineCodec{}
			encoded, encodeErr := codec.Encode(source)
			expected, decodeErr := codec.Decode(encoded)
			clone, cloneErr := MachineProjection.CloneState(source)
			if (cloneErr != nil) != (encodeErr != nil || decodeErr != nil) {
				t.Fatalf("errors differ: clone=%v, codec=%v/%v", cloneErr, encodeErr, decodeErr)
			}
			if cloneErr == nil && !reflect.DeepEqual(clone, expected) {
				t.Fatalf("normalization differs: %#v / %#v", clone, expected)
			}
		})
	}
}
