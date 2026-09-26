package chatlog

import (
	"encoding/json"
	"fmt"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/model"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// TestTablePersistence checks the persistent map on its own: a Set never
// changes the receiver, merges keep every entry, and the JSON shape is the
// plain object a map would produce.
func TestTablePersistence(t *testing.T) {
	var t0 Table[string, int]
	states := []Table[string, int]{t0}
	for i := 0; i < 500; i++ {
		states = append(states, states[len(states)-1].Set(fmt.Sprint(i), i))
	}
	for i, s := range states {
		if s.Len() != i {
			t.Fatalf("state %d has %d entries", i, s.Len())
		}
		if i > 0 {
			if v, ok := s.Get(fmt.Sprint(i - 1)); !ok || v != i-1 {
				t.Fatalf("state %d lost its newest key", i)
			}
		}
		if s.Has(fmt.Sprint(i)) {
			t.Fatalf("state %d sees a key written later", i)
		}
	}
	updated := states[500].Set("7", 700)
	if v, _ := states[500].Get("7"); v != 7 {
		t.Fatal("Set changed the receiver")
	}
	if v, _ := updated.Get("7"); v != 700 || updated.Len() != 500 {
		t.Fatalf("update = %d len %d", v, updated.Len())
	}
	raw, err := json.Marshal(updated)
	if err != nil {
		t.Fatal(err)
	}
	var back Table[string, int]
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.Len() != 500 {
		t.Fatalf("round trip lost entries: %d", back.Len())
	}
	if v, _ := back.Get("7"); v != 700 {
		t.Fatal("round trip lost the update")
	}
	plain, _ := json.Marshal(updated.Map())
	if string(plain) != string(raw) {
		t.Fatal("Table does not encode like a map")
	}
	var wrapper struct {
		T Table[string, int] `json:"t,omitzero"`
	}
	empty, _ := json.Marshal(wrapper)
	if string(empty) != "{}" {
		t.Fatalf("empty table not omitted: %s", empty)
	}
}

// TestSurfaceFoldIsPure folds two different continuations from one state: the
// old state keeps its contents and the two results do not see each other
// (EXT-PRJ-1). Each event carries its own ledger Position, as a fold would
// stamp it.
func TestSurfaceFoldIsPure(t *testing.T) {
	var seq session.CommitSeq
	next := func() session.Position { seq++; return session.Position{Commit: seq} }
	assistant := func(id string) extension.DecodedEvent {
		return extension.DecodedEvent{Position: next(), Value: runmod.Event{RunID: "r", Fact: run.ModelStepCompleted{StepID: run.StepID(id), FinishReason: model.FinishReasonStop, ResultDigest: "sha256:x"}}}
	}
	input := func(id string) extension.DecodedEvent {
		return extension.DecodedEvent{Position: next(), Value: InputSubmittedPayload{InputID: InputID(id), Content: jsonstable.MustParse(`{"text":"x"}`)}}
	}
	state, _ := SurfaceProjection.Initial()
	var err error
	for i := 0; i < 40; i++ {
		if state, err = SurfaceProjection.Apply(state, assistant(fmt.Sprint("a", i))); err != nil {
			t.Fatal(err)
		}
	}
	if state, err = SurfaceProjection.Apply(state, input("in-0")); err != nil {
		t.Fatal(err)
	}
	old := state.(Surface)
	left, err := SurfaceProjection.Apply(state, assistant("left"))
	if err != nil {
		t.Fatal(err)
	}
	left, err = SurfaceProjection.Apply(left, input("in-left"))
	if err != nil {
		t.Fatal(err)
	}
	right, err := SurfaceProjection.Apply(state, assistant("right"))
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name        string
		s           Surface
		assistants  int
		inputs      int
		has, lacks  string
		hasIn, noIn string
	}{
		{"old", old, 40, 1, "a39", "left", "in-0", "in-left"},
		{"left", left.(Surface), 41, 2, "left", "right", "in-left", ""},
		{"right", right.(Surface), 41, 1, "right", "left", "in-0", "in-left"},
	}
	for _, tc := range cases {
		if tc.s.Assistants.Len() != tc.assistants || tc.s.Inputs.Len() != tc.inputs {
			t.Fatalf("%s: assistants=%d inputs=%d", tc.name, tc.s.Assistants.Len(), tc.s.Inputs.Len())
		}
		if !tc.s.Assistants.Has(AssistantID(tc.has)) || tc.s.Assistants.Has(AssistantID(tc.lacks)) {
			t.Fatalf("%s: assistant visibility wrong", tc.name)
		}
		if !tc.s.Inputs.Has(InputID(tc.hasIn)) {
			t.Fatalf("%s: missing input %s", tc.name, tc.hasIn)
		}
		if tc.noIn != "" && tc.s.Inputs.Has(InputID(tc.noIn)) {
			t.Fatalf("%s: sees input %s", tc.name, tc.noIn)
		}
	}
}
