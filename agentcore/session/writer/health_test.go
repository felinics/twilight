package writer

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
)

// healthModule has one event type and two projections over it: an
// authoritative one and a derived one, both of which fail on the text "boom".
func healthModule() extension.ModuleDescriptor {
	typ := tpfx("h") + "row"
	mk := func(id extension.ProjectionID, authoritative bool) extension.ProjectionDefinition {
		return extension.ProjectionDefinition{
			ID: id, Version: 1, Consumes: []session.EventType{typ}, Authoritative: authoritative,
			Initial: func() (any, error) { return noteState{}, nil },
			Apply: func(state any, e extension.DecodedEvent) (any, error) {
				text := e.Value.(notePayload).Text
				if text == "boom" {
					return nil, errors.New("cannot fold boom")
				}
				s := state.(noteState)
				s.Notes = append(append([]string(nil), s.Notes...), text)
				return s, nil
			},
			StateCodec: extension.JSONStateCodec[noteState]{},
		}
	}
	return extension.ModuleDescriptor{Source: extension.SourceTwilight, ID: "h", Streams: noteStreams(),
		Events:      []extension.EventDefinition{{Type: typ, Stream: noteDomain, Codecs: map[extension.PayloadVersion]extension.PayloadCodec{1: extension.JSONCodec[notePayload]{}}}},
		Projections: []extension.ProjectionDefinition{mk("h/authoritative", true), mk("h/derived", false)}}
}

// EXT-PRJ-9: a derived projection that cannot fold a commit does not refuse
// the commit; it is marked unhealthy, reads report it, and it is not cached.
// An authoritative projection that cannot fold refuses the commit.
func TestDerivedProjectionFailureDoesNotBlockCommit(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	registry, err := extension.BuildRegistry(healthModule())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	cache := extension.NewMemoryProjectionCache()
	w, err := openWriter(ctx, store, registry, Admission{}, "s", session.OpenOptions{}, WritersConfig{Cache: cache, CachePolicy: extension.CacheEvery(1)})
	if err != nil {
		t.Fatal(err)
	}
	commit := func(id, text string, only extension.ProjectionID) (CommitResult, error) {
		return w.Commit(ctx, func(View) (*SemanticGroup, error) {
			return &SemanticGroup{CommitID: session.CommitID(id), Batches: noteBatch(TypedEvent{Type: tpfx("h") + "row", Value: notePayload{Text: text}})}, nil
		})
	}
	if res, err := commit("c1", "one", ""); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("c1 = %+v %v", res, err)
	}
	// Both projections fail on boom, so the authoritative one refuses.
	if res, err := commit("c2", "boom", ""); err != nil || res.Outcome != CommitInvalid {
		t.Fatalf("authoritative refusal = %+v %v", res, err)
	}
	// Reopen with only the derived projection failing: rebuild the registry
	// with an authoritative projection that accepts everything.
	registry2, err := extension.BuildRegistry(func() extension.ModuleDescriptor {
		m := healthModule()
		m.Projections[0].Apply = func(state any, e extension.DecodedEvent) (any, error) { return state, nil }
		return m
	}())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	w, err = openWriter(ctx, store, registry2, Admission{}, "s", session.OpenOptions{}, WritersConfig{Cache: cache, CachePolicy: extension.CacheEvery(1)})
	if err != nil {
		t.Fatal(err)
	}
	if res, err := commit("c2", "boom", ""); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("commit with a failing derived projection = %+v %v, want applied", res, err)
	}
	if _, _, err := w.Projections().Load(ctx, "s", "h/derived", 1); !errors.Is(err, &extension.Error{Code: extension.ErrProjectionUnhealthy}) {
		t.Fatalf("derived read = %v, want unhealthy", err)
	}
	if _, _, err := w.Projections().Load(ctx, "s", "h/authoritative", 1); err != nil {
		t.Fatalf("authoritative read after a derived failure: %v", err)
	}
	// The next commit still lands and the unhealthy projection stays behind.
	if res, err := commit("c3", "three", ""); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("c3 = %+v %v", res, err)
	}
	if _, through, ok, _ := cache.Load(ctx, "s", "h/derived", 1); ok && through.Next > 1 {
		t.Fatalf("unhealthy projection cached at head %+v", through)
	}
	if _, through, ok, _ := cache.Load(ctx, "s", "h/authoritative", 1); !ok || through.Next != 3 {
		t.Fatalf("healthy projection cache = ok=%v %+v, want head 3", ok, through)
	}
}

// EXT-PRJ-9 across a reopen: a derived projection that cannot fold the log
// does not keep the Session from opening; it stops at its last good commit
// and is unhealthy, the authoritative projections fold to head.
func TestDerivedProjectionFailureDoesNotBlockReopen(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	// Write "one", "boom", "three" with a registry whose derived projection
	// accepts everything, then reopen with the one that fails on boom.
	permissive := healthModule()
	permissive.Projections[1].Apply = func(state any, e extension.DecodedEvent) (any, error) { return state, nil }
	permissive.Projections[0].Apply = permissive.Projections[1].Apply
	reg1, err := extension.BuildRegistry(permissive)
	if err != nil {
		t.Fatal(err)
	}
	w, err := openWriter(ctx, store, reg1, Admission{}, "s", session.OpenOptions{}, WritersConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for i, text := range []string{"one", "boom", "three"} {
		res, err := w.Commit(ctx, func(View) (*SemanticGroup, error) {
			return &SemanticGroup{CommitID: session.CommitID(fmt.Sprintf("c%d", i)), Batches: noteBatch(TypedEvent{Type: tpfx("h") + "row", Value: notePayload{Text: text}})}, nil
		})
		if err != nil || res.Outcome != CommitApplied {
			t.Fatalf("c%d = %+v %v", i, res, err)
		}
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	strict := healthModule()
	strict.Projections[0].Apply = permissive.Projections[0].Apply // authoritative one keeps folding
	reg2, err := extension.BuildRegistry(strict)
	if err != nil {
		t.Fatal(err)
	}
	w, err = openWriter(ctx, store, reg2, Admission{}, "s", session.OpenOptions{Takeover: true}, WritersConfig{})
	if err != nil {
		t.Fatalf("reopen with a failing derived projection: %v", err)
	}
	if _, _, err := w.Projections().Load(ctx, "s", "h/derived", 1); !errors.Is(err, &extension.Error{Code: extension.ErrProjectionUnhealthy}) {
		t.Fatalf("derived after reopen = %v, want unhealthy", err)
	}
	if _, head, err := w.Projections().Load(ctx, "s", "h/authoritative", 1); err != nil || head.Next != 3 {
		t.Fatalf("authoritative after reopen = %+v %v", head, err)
	}
	// And the Session still writes.
	if res, err := w.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c3", Batches: noteBatch(TypedEvent{Type: tpfx("h") + "row", Value: notePayload{Text: "four"}})}, nil
	}); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("commit after reopen = %+v %v", res, err)
	}
	// An authoritative projection that cannot fold the log refuses the open.
	failingAuth := healthModule()
	reg3, err := extension.BuildRegistry(failingAuth)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close(ctx)
	if _, err := openWriter(ctx, store, reg3, Admission{}, "s", session.OpenOptions{Takeover: true}, WritersConfig{}); err == nil {
		t.Fatal("reopen with a failing authoritative projection succeeded")
	}
}
