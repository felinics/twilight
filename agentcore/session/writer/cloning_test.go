package writer

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
)

type cloningCodec struct{ encodes, decodes int }

func (*cloningCodec) Validate(any) error { return nil }
func (c *cloningCodec) Encode(value any) (jsonstable.Value, error) {
	c.encodes++
	return jsonstable.FromValue(value)
}
func (c *cloningCodec) Decode(value jsonstable.Value) (any, error) {
	c.decodes++
	var state nestedState
	err := value.Decode(&state)
	return state, err
}
func cloneNested(value any) (any, error) {
	source := value.(nestedState)
	out := nestedState{Entries: make(map[string][]*nestedEntry, len(source.Entries))}
	for key, entries := range source.Entries {
		dest := make([]*nestedEntry, len(entries))
		for i, e := range entries {
			dest[i] = &nestedEntry{Values: append([]string(nil), e.Values...)}
		}
		out.Entries[key] = dest
	}
	return out, nil
}

func TestProjectionCloneUsedByViewAndReader(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	mod := noteModule("clone")
	id := module.ProjectionID(string(tpfx("clone")) + "state")
	codec := &cloningCodec{}
	mod.Projections = []module.ProjectionDefinition{{ID: id, Version: 1, Consumes: []ledger.EventType{tpfx("clone") + "note"},
		Initial: func() (any, error) { return nestedState{Entries: map[string][]*nestedEntry{}}, nil },
		Apply: func(state any, event module.DecodedEvent) (any, error) {
			out, _ := cloneNested(state)
			s := out.(nestedState)
			text := event.Value.(notePayload).Text
			s.Entries[text] = []*nestedEntry{{Values: []string{text}}}
			return s, nil
		},
		StateCodec: codec, CloneState: cloneNested,
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
			return &SemanticGroup{CommitID: ledger.CommitID(text), Batches: noteBatch(TypedEvent{Type: tpfx("clone") + "note", Value: notePayload{Text: text}})}, nil
		})
		if err != nil || res.Outcome != CommitApplied {
			t.Fatalf("commit=%+v,%v", res, err)
		}
	}
	read := func() nestedState {
		t.Helper()
		v, _, err := w.Projections().Load(ctx, "s", id, 1)
		if err != nil {
			t.Fatal(err)
		}
		return v.(nestedState)
	}
	commit("one")
	source := read()
	source.Entries["one"][0].Values[0] = "mutated"
	delete(source.Entries, "one")
	var retained nestedState
	_, err = w.Commit(ctx, func(view View) (*SemanticGroup, error) {
		v, err := view.Projection(id, 1)
		if err != nil {
			return nil, err
		}
		retained = v.(nestedState)
		retained.Entries["one"][0].Values[0] = "view mutation"
		next, err := view.Projection(id, 1)
		if err != nil {
			return nil, err
		}
		if next.(nestedState).Entries["one"][0].Values[0] != "one" {
			t.Fatal("view leaked mutation")
		}
		return nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	commit("two")
	retained.Entries["one"][0].Values[0] = "retained mutation"
	current := read()
	if current.Entries["one"][0].Values[0] != "one" || current.Entries["two"][0].Values[0] != "two" {
		t.Fatal("clone not isolated across commits")
	}
	if codec.encodes != 0 || codec.decodes != 0 {
		t.Fatalf("in-memory clone used codec: %d/%d", codec.encodes, codec.decodes)
	}
}

func TestProjectionCloneErrorsAndHealth(t *testing.T) {
	sentinel := errors.New("clone failed")
	key := projectionKey{id: "p", version: 1}
	calls := 0
	p := &projector{states: map[projectionKey]any{key: "state"}, scopes: map[projectionKey]*module.ProjectionScope{key: {Def: module.ProjectionDefinition{CloneState: func(any) (any, error) { calls++; return nil, sentinel }}}}, unhealthy: map[projectionKey]error{}}
	if _, err := p.detached("p", 1); !errors.Is(err, sentinel) {
		t.Fatalf("lost clone error: %v", err)
	}
	if _, err := p.detached("missing", 1); err == nil {
		t.Fatal("unknown projection accepted")
	}
	p.unhealthy[key] = errors.New("fold failed")
	if _, err := p.detached("p", 1); err == nil || errors.Is(err, sentinel) {
		t.Fatalf("unhealthy projection reached clone: %v", err)
	}
	if calls != 1 {
		t.Fatalf("clone calls=%d", calls)
	}
}
