package writer

import (
	"context"
	"errors"
	"fmt"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"testing"
)

// healthModule has one event type and two projections over it: a
// required-for-commit one and a derived one, both of which fail on the text "boom".
func healthModule() module.ModuleDescriptor {
	typ := tpfx("h") + "row"
	mk := func(id module.ProjectionID, requiredForCommit bool) module.ProjectionDefinition {
		return module.ProjectionDefinition{
			ID: id, Version: 1, Consumes: []ledger.EventType{typ}, RequiredForCommit: requiredForCommit,
			Initial: func() (any, error) { return noteState{}, nil },
			Apply: func(state any, e module.DecodedEvent) (any, error) {
				text := e.Value.(notePayload).Text
				if text == "boom" {
					return nil, errors.New("cannot fold boom")
				}
				s := state.(noteState)
				s.Notes = append(append([]string(nil), s.Notes...), text)
				return s, nil
			},
			StateCodec: module.JSONStateCodec[noteState]{},
		}
	}
	return module.ModuleDescriptor{Source: module.SourceTwilight, ID: "h", Streams: noteStreams(),
		Events:      []module.EventDefinition{{Type: typ, Domain: noteDomain, Codecs: map[module.PayloadVersion]module.PayloadCodec{1: module.JSONCodec[notePayload]{}}}},
		Projections: []module.ProjectionDefinition{mk("h/required", true), mk("h/derived", false)}}
}

// a derived projection that cannot fold a commit does not refuse
// the commit; it is marked unhealthy, reads report it, and it is not cached.
// A required-for-commit projection that cannot fold refuses the commit.
func TestDerivedProjectionFailureDoesNotBlockCommit(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	registry, err := module.BuildRegistry(healthModule())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	cache := session.NewMemoryProjectionCache()
	w, err := acquireTestWriterConfig(ctx, store, registry, Admission{}, "s", session.OpenOptions{}, WritersConfig{Cache: cache, CachePolicy: module.CacheEvery(1)})
	if err != nil {
		t.Fatal(err)
	}
	commit := func(id, text string, only module.ProjectionID) (CommitResult, error) {
		return w.Commit(ctx, func(View) (*SemanticGroup, error) {
			return &SemanticGroup{CommitID: ledger.CommitID(id), Batches: noteBatch(TypedEvent{Type: tpfx("h") + "row", Value: notePayload{Text: text}})}, nil
		})
	}
	if res, err := commit("c1", "one", ""); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("c1 = %+v %v", res, err)
	}
	// Both projections fail on boom, so the required-for-commit one refuses.
	if res, err := commit("c2", "boom", ""); err != nil || res.Outcome != CommitInvalid {
		t.Fatalf("required-for-commit refusal = %+v %v", res, err)
	}
	// Reopen with only the derived projection failing: rebuild the registry
	// with a required-for-commit projection that accepts everything.
	registry2, err := module.BuildRegistry(func() module.ModuleDescriptor {
		m := healthModule()
		m.Projections[0].Apply = func(state any, e module.DecodedEvent) (any, error) { return state, nil }
		return m
	}())
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	w, err = acquireTestWriterConfig(ctx, store, registry2, Admission{}, "s", session.OpenOptions{}, WritersConfig{Cache: cache, CachePolicy: module.CacheEvery(1)})
	if err != nil {
		t.Fatal(err)
	}
	if res, err := commit("c2", "boom", ""); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("commit with a failing derived projection = %+v %v, want applied", res, err)
	}
	if _, _, err := w.Projections().Load(ctx, "s", "h/derived", 1); !errors.Is(err, &ledger.Error{Code: ledger.CodeProjectionUnhealthy}) {
		t.Fatalf("derived read = %v, want unhealthy", err)
	}
	if _, _, err := w.Projections().Load(ctx, "s", "h/required", 1); err != nil {
		t.Fatalf("required-for-commit read after a derived failure: %v", err)
	}
	// The next commit still lands and the unhealthy projection stays behind.
	if res, err := commit("c3", "three", ""); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("c3 = %+v %v", res, err)
	}
	if _, through, ok, _ := cache.Load(ctx, "s", "h/derived", 1); ok && through.Next > 1 {
		t.Fatalf("unhealthy projection cached at head %+v", through)
	}
	if _, through, ok, _ := cache.Load(ctx, "s", "h/required", 1); !ok || through.Next != 3 {
		t.Fatalf("healthy projection cache = ok=%v %+v, want head 3", ok, through)
	}
}

// across a reopen: a derived projection that cannot fold the log
// does not keep the Session from opening; it stops at its last good commit
// and is unhealthy, the required-for-commit projections fold to head.
func TestDerivedProjectionFailureDoesNotBlockReopen(t *testing.T) {
	ctx := context.Background()
	store := filestoretest.Store(t)
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	// Write "one", "boom", "three" with a registry whose derived projection
	// accepts everything, then reopen with the one that fails on boom.
	permissive := healthModule()
	permissive.Projections[1].Apply = func(state any, e module.DecodedEvent) (any, error) { return state, nil }
	permissive.Projections[0].Apply = permissive.Projections[1].Apply
	reg1, err := module.BuildRegistry(permissive)
	if err != nil {
		t.Fatal(err)
	}
	w, err := acquireTestWriterConfig(ctx, store, reg1, Admission{}, "s", session.OpenOptions{}, WritersConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for i, text := range []string{"one", "boom", "three"} {
		res, err := w.Commit(ctx, func(View) (*SemanticGroup, error) {
			return &SemanticGroup{CommitID: ledger.CommitID(fmt.Sprintf("c%d", i)), Batches: noteBatch(TypedEvent{Type: tpfx("h") + "row", Value: notePayload{Text: text}})}, nil
		})
		if err != nil || res.Outcome != CommitApplied {
			t.Fatalf("c%d = %+v %v", i, res, err)
		}
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	strict := healthModule()
	strict.Projections[0].Apply = permissive.Projections[0].Apply // required-for-commit one keeps folding
	reg2, err := module.BuildRegistry(strict)
	if err != nil {
		t.Fatal(err)
	}
	w, err = acquireTestWriterConfig(ctx, store, reg2, Admission{}, "s", session.OpenOptions{Takeover: true}, WritersConfig{})
	if err != nil {
		t.Fatalf("reopen with a failing derived projection: %v", err)
	}
	if _, _, err := w.Projections().Load(ctx, "s", "h/derived", 1); !errors.Is(err, &ledger.Error{Code: ledger.CodeProjectionUnhealthy}) {
		t.Fatalf("derived after reopen = %v, want unhealthy", err)
	}
	if _, head, err := w.Projections().Load(ctx, "s", "h/required", 1); err != nil || head.Next != 3 {
		t.Fatalf("required-for-commit after reopen = %+v %v", head, err)
	}
	// And the Session still writes.
	if res, err := w.Commit(ctx, func(View) (*SemanticGroup, error) {
		return &SemanticGroup{CommitID: "c3", Batches: noteBatch(TypedEvent{Type: tpfx("h") + "row", Value: notePayload{Text: "four"}})}, nil
	}); err != nil || res.Outcome != CommitApplied {
		t.Fatalf("commit after reopen = %+v %v", res, err)
	}
	// A required-for-commit projection that cannot fold the log refuses the open.
	failingRequired := healthModule()
	reg3, err := module.BuildRegistry(failingRequired)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Close(ctx)
	if _, err := acquireTestWriterConfig(ctx, store, reg3, Admission{}, "s", session.OpenOptions{Takeover: true}, WritersConfig{}); err == nil {
		t.Fatal("reopen with a failing required-for-commit projection succeeded")
	}
}
