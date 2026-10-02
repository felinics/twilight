package writer

import (
	"context"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"testing"
)

type nestedEntry struct {
	Values []string `json:"values"`
}

type nestedState struct {
	Entries map[string][]*nestedEntry `json:"entries"`
}

func TestProjectionReadsAreDetached(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	mod := noteModule("nested")
	id := module.ProjectionID(string(tpfx("nested")) + "state")
	mod.Projections = []module.ProjectionDefinition{{
		ID: id, Version: 1, Consumes: []ledger.EventType{tpfx("nested") + "note"},
		Initial: func() (any, error) { return nestedState{Entries: map[string][]*nestedEntry{}}, nil },
		Apply: func(state any, event module.DecodedEvent) (any, error) {
			s := state.(nestedState)
			entries := make(map[string][]*nestedEntry, len(s.Entries)+1)
			for key, value := range s.Entries {
				entries[key] = value
			}
			text := event.Value.(notePayload).Text
			entries[text] = []*nestedEntry{{Values: []string{text}}}
			return nestedState{Entries: entries}, nil
		},
		StateCodec: module.JSONStateCodec[nestedState]{},
	}}
	var err error
	f.registry, err = module.BuildRegistry(mod)
	if err != nil {
		t.Fatal(err)
	}
	w := f.open(t, false)
	defer w.Close(ctx)
	commit := func(text string) {
		t.Helper()
		res, err := w.Commit(ctx, func(View) (*SemanticGroup, error) {
			return &SemanticGroup{CommitID: ledger.CommitID(text), Batches: noteBatch(TypedEvent{Type: tpfx("nested") + "note", Value: notePayload{Text: text}})}, nil
		})
		if err != nil || res.Outcome != CommitApplied {
			t.Fatalf("commit = %+v, %v", res, err)
		}
	}
	read := func() nestedState {
		t.Helper()
		state, _, err := w.Projections().Load(ctx, "s", id, 1)
		if err != nil {
			t.Fatal(err)
		}
		return state.(nestedState)
	}
	mutate := func(state nestedState) {
		state.Entries["one"][0].Values[0] = "changed nested value"
		state.Entries["one"][0] = &nestedEntry{Values: []string{"changed slice"}}
		state.Entries["injected"] = []*nestedEntry{{Values: []string{"changed map"}}}
	}
	assertOriginal := func(state nestedState) {
		t.Helper()
		entries := state.Entries["one"]
		if len(entries) != 1 || len(entries[0].Values) != 1 || entries[0].Values[0] != "one" {
			t.Fatalf("nested state was modified: %+v", state)
		}
		if _, ok := state.Entries["injected"]; ok {
			t.Fatal("projection map was modified")
		}
	}

	commit("one")
	mutate(read())
	assertOriginal(read())
	var retained nestedState
	res, err := w.Commit(ctx, func(v View) (*SemanticGroup, error) {
		state, err := v.Projection(id, 1)
		if err != nil {
			return nil, err
		}
		retained = state.(nestedState)
		mutate(retained)
		state, err = v.Projection(id, 1)
		if err != nil {
			return nil, err
		}
		assertOriginal(state.(nestedState))
		return nil, nil
	})
	if err != nil || res.Outcome != CommitNoop {
		t.Fatalf("view mutation = %+v, %v", res, err)
	}
	assertOriginal(read())
	commit("two")
	mutate(retained)
	assertOriginal(read())
	if entries := read().Entries["two"]; len(entries) != 1 || entries[0].Values[0] != "two" {
		t.Fatalf("next committed state = %+v", entries)
	}
}
