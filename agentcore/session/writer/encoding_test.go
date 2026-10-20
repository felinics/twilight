package writer

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
)

// Deliberately implement PayloadCodec without embedding JSONStateCodec: custom
// codecs must not acquire the optional capability merely by forwarding calls.
type countedStateCodec struct {
	encodes, decodes     int
	encodeErr, decodeErr error
}

func (c *countedStateCodec) Validate(v any) error {
	return module.JSONStateCodec[noteState]{}.Validate(v)
}
func (c *countedStateCodec) Encode(v any) (jsonstable.Value, error) {
	c.encodes++
	if c.encodeErr != nil {
		return jsonstable.Value{}, c.encodeErr
	}
	return module.JSONStateCodec[noteState]{}.Encode(v)
}
func (c *countedStateCodec) Decode(v jsonstable.Value) (any, error) {
	c.decodes++
	if c.decodeErr != nil {
		return nil, c.decodeErr
	}
	return module.JSONStateCodec[noteState]{}.Decode(v)
}

type reusableStateCodec struct {
	*countedStateCodec
	reuse bool
}

func (c reusableStateCodec) CacheStateEncoding() bool { return c.reuse }

func encodingWriter(t *testing.T, codec module.PayloadCodec) (Writer, module.ProjectionID) {
	t.Helper()
	f := newFixture(t)
	m := noteModule("a")
	m.Projections[0].StateCodec = codec
	var err error
	f.registry, err = module.BuildRegistry(m)
	if err != nil {
		t.Fatal(err)
	}
	w := f.open(t, false)
	t.Cleanup(func() {
		if err := w.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return w, m.Projections[0].ID
}

func TestStateEncodingReuseTracksCommittedState(t *testing.T) {
	c := &countedStateCodec{}
	w, id := encodingWriter(t, reusableStateCodec{c, true})
	ctx := context.Background()
	read := func(wantEncodes, wantDecodes int, want ...string) {
		t.Helper()
		v, _, err := w.Projections().Load(ctx, "s", id, 1)
		if err != nil {
			t.Fatal(err)
		}
		notes := v.(noteState).Notes
		if len(notes) != len(want) {
			t.Fatalf("notes = %v, want %v", notes, want)
		}
		for i := range notes {
			if notes[i] != want[i] {
				t.Fatalf("notes = %v, want %v", notes, want)
			}
		}
		if c.encodes != wantEncodes || c.decodes != wantDecodes {
			t.Fatalf("encode/decode calls = %d/%d, want %d/%d", c.encodes, c.decodes, wantEncodes, wantDecodes)
		}
		if len(notes) > 0 {
			notes[0] = "caller mutation"
		}
	}
	commit := func(cid, text string, want CommitOutcome) {
		t.Helper()
		result, err := w.Commit(ctx, noteGroup(cid, text))
		if err != nil || result.Outcome != want {
			t.Fatalf("commit = %+v, %v; want %v", result, err, want)
		}
	}
	read(1, 1)
	read(1, 2)
	commit("bad", "reject", CommitInvalid) // speculative fold must not install state
	read(1, 3)
	commit("one", "first", CommitApplied)
	read(2, 4, "first")
	read(2, 5, "first") // caller mutation cannot change cached bytes or folded state
	commit("one", "duplicate", CommitAlreadyApplied)
	read(2, 6, "first")
	result, err := w.Commit(ctx, func(v View) (*SemanticGroup, error) {
		a, err := v.Projection(id, 1)
		if err != nil {
			return nil, err
		}
		a.(noteState).Notes[0] = "decision mutation"
		b, err := v.Projection(id, 1)
		if err == nil && b.(noteState).Notes[0] != "first" {
			t.Fatal("decision reads share state")
		}
		return nil, err
	})
	if err != nil || result.Outcome != CommitNoop {
		t.Fatalf("noop = %+v, %v", result, err)
	}
	read(2, 9, "first")
	commit("two", "second", CommitApplied)
	read(3, 10, "first", "second")
	result, err = w.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "hint", Batches: noteBatch(TypedEvent{Type: tpfx("a") + "hint", Value: notePayload{Text: "unconsumed"}})}, nil
	})
	if err != nil || result.Outcome != CommitApplied {
		t.Fatalf("unconsumed commit = %+v, %v", result, err)
	}
	read(3, 11, "first", "second")

	if _, _, err := w.Projections().Load(ctx, "s", "missing", 1); !errors.Is(err, &ledger.Error{Code: ledger.CodeInvalid}) {
		t.Fatalf("unknown read = %v", err)
	}
}

func TestCustomStateCodecKeepsEncodingEveryRead(t *testing.T) {
	for _, optOut := range []bool{false, true} {
		t.Run(map[bool]string{false: "no capability", true: "explicit opt out"}[optOut], func(t *testing.T) {
			c := &countedStateCodec{}
			var codec module.PayloadCodec = c
			if optOut {
				codec = reusableStateCodec{c, false}
			}
			w, id := encodingWriter(t, codec)
			for i := 0; i < 3; i++ {
				if _, _, err := w.Projections().Load(context.Background(), "s", id, 1); err != nil {
					t.Fatal(err)
				}
			}
			if c.encodes != 3 || c.decodes != 3 {
				t.Fatalf("calls = %d/%d", c.encodes, c.decodes)
			}
		})
	}
}

func TestStateEncodingErrorsRemainObservable(t *testing.T) {
	c := &countedStateCodec{encodeErr: errors.New("encode failure")}
	w, id := encodingWriter(t, reusableStateCodec{c, true})
	load := func(want error) {
		t.Helper()
		_, _, err := w.Projections().Load(context.Background(), "s", id, 1)
		if !errors.Is(err, want) {
			t.Fatalf("load = %v, want %v", err, want)
		}
	}
	load(c.encodeErr)
	load(c.encodeErr)
	if c.encodes != 2 || c.decodes != 0 {
		t.Fatalf("failed encode calls = %d/%d", c.encodes, c.decodes)
	}
	c.encodeErr = nil
	c.decodeErr = errors.New("decode failure")
	load(c.decodeErr)
	load(c.decodeErr)
	if c.encodes != 3 || c.decodes != 2 {
		t.Fatalf("failed decode calls = %d/%d", c.encodes, c.decodes)
	}
	c.decodeErr = nil
	load(nil)
	if c.encodes != 3 || c.decodes != 3 {
		t.Fatalf("recovered calls = %d/%d", c.encodes, c.decodes)
	}
}

func TestStateEncodingDiscardedForUnhealthyProjection(t *testing.T) {
	f := newFixture(t)
	m := healthModule()
	m.Projections[0].Apply = func(state any, _ module.DecodedEvent) (any, error) { return state, nil }
	c := &countedStateCodec{}
	m.Projections[1].StateCodec = reusableStateCodec{c, true}
	var err error
	f.registry, err = module.BuildRegistry(m)
	if err != nil {
		t.Fatal(err)
	}
	w := f.open(t, false)
	defer w.Close(context.Background())
	if _, _, err := w.Projections().Load(context.Background(), "s", "h/derived", 1); err != nil {
		t.Fatal(err)
	}
	result, err := w.Commit(context.Background(), func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "failure", Batches: noteBatch(TypedEvent{Type: tpfx("h") + "row", Value: notePayload{Text: "boom"}})}, nil
	})
	if err != nil || result.Outcome != CommitApplied {
		t.Fatalf("commit = %+v, %v", result, err)
	}
	if _, _, err := w.Projections().Load(context.Background(), "s", "h/derived", 1); !errors.Is(err, &ledger.Error{Code: ledger.CodeProjectionUnhealthy}) {
		t.Fatalf("unhealthy read = %v", err)
	}
	if c.encodes != 1 || c.decodes != 1 {
		t.Fatalf("unhealthy read called codec: %d/%d", c.encodes, c.decodes)
	}
}

func TestCachedEncodingDoesNotBypassFoldScopeChecks(t *testing.T) {
	m := noteModule("a")
	registry, err := module.BuildRegistry(m)
	if err != nil {
		t.Fatal(err)
	}
	p := newProjector(registry, "s", nil, nil)
	if _, err := p.prepare(context.Background(), session.SegmentHeader{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := p.detached(m.Projections[0].ID, 1); err != nil {
		t.Fatal(err)
	}
	_, err = p.fold(ledger.Commit{Batches: []ledger.EventBatch{{Domain: ledger.Domain{Name: noteDomain}, Events: []ledger.Event{{Type: tpfx("a") + "unknown"}}}}})
	if !errors.Is(err, &ledger.Error{Code: ledger.CodeUnknownEvent}) {
		t.Fatalf("unknown in-scope event = %v", err)
	}
	if _, err := p.detached(m.Projections[0].ID, 1); err != nil {
		t.Fatalf("rejected fold damaged prior state: %v", err)
	}
}
