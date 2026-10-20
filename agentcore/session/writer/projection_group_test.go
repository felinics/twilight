package writer

import (
	"context"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
)

type groupedReadStore struct {
	session.Store
	reads []session.CommitReadRequest
}

func (s *groupedReadStore) ReadCommits(ctx context.Context, req session.CommitReadRequest) (session.CommitPage, error) {
	s.reads = append(s.reads, req)
	return s.Store.ReadCommits(ctx, req)
}

func TestGroupedProjectionReadsShareTailAndStayDetached(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	w := f.open(t, WritersConfig{})
	defer w.Close(ctx)
	f.commit(t, w, "c1", "one")
	f.commit(t, w, "c2", "two")
	f.commit(t, w, "c3", "three")
	if err := f.cache.Save(ctx, "s", alphaID, 1, f.encodeState(t, "one"), ledger.Head{Next: 1}); err != nil {
		t.Fatal(err)
	}
	if err := f.cache.Save(ctx, "s", betaID, 1, f.encodeState(t, "one", "two"), ledger.Head{Next: 2}); err != nil {
		t.Fatal(err)
	}
	counted := &groupedReadStore{Store: f.store}
	reader := session.NewProjectionReader(counted, f.registry, f.cache)
	refs := []session.ProjectionRef{{ID: alphaID, Version: 1}, {ID: betaID, Version: 1}}
	states, err := session.LoadProjections(ctx, reader, "s", refs...)
	if err != nil {
		t.Fatal(err)
	}
	if len(counted.reads) != 1 {
		t.Fatalf("reads=%v, want one shared boundary and tail", counted.reads)
	}
	for _, state := range states {
		if !sameNotes(state.State.(noteState).Notes, []string{"one", "two", "three"}) || state.Through.Next != 3 {
			t.Fatalf("state=%+v", state)
		}
	}
	states[0].State.(noteState).Notes[0] = "caller mutation"
	f.commit(t, w, "c4", "four")
	states, err = session.LoadProjections(ctx, reader, "s", refs...)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range states {
		if !sameNotes(state.State.(noteState).Notes, []string{"one", "two", "three", "four"}) || state.Through.Next != 4 {
			t.Fatalf("fresh read=%+v", state)
		}
	}
	if len(counted.reads) != 2 {
		t.Fatalf("reads=%v", counted.reads)
	}
}

func TestGroupedProjectionReadRejectsInvalidCheckpoints(t *testing.T) {
	for _, kind := range []string{"ahead", "bad_codec", "unsealed_required"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			f := newCacheFixture(t)
			if kind == "unsealed_required" {
				mod := cacheModule(f.counter)
				mod.Projections[0].RequiredForCommit = true
				var err error
				f.registry, err = module.BuildRegistry(mod)
				if err != nil {
					t.Fatal(err)
				}
			}
			w := f.open(t, WritersConfig{})
			defer w.Close(ctx)
			f.commit(t, w, "c1", "one")
			f.commit(t, w, "c2", "two")
			f.commit(t, w, "c3", "three")
			encoded := f.encodeState(t, "wrong")
			through := ledger.Head{Next: 2}
			switch kind {
			case "ahead":
				through.Next = 99
			case "bad_codec":
				encoded, _ = module.JSONStateCodec[struct{ Other string }]{}.Encode(struct{ Other string }{"wrong"})
			}
			if err := f.cache.Save(ctx, "s", alphaID, 1, encoded, through); err != nil {
				t.Fatal(err)
			}
			if err := f.cache.Save(ctx, "s", betaID, 1, f.encodeState(t, "one", "two"), ledger.Head{Next: 2}); err != nil {
				t.Fatal(err)
			}
			states, err := session.LoadProjections(ctx, session.NewProjectionReader(f.store, f.registry, f.cache), "s", session.ProjectionRef{ID: alphaID, Version: 1}, session.ProjectionRef{ID: betaID, Version: 1})
			if err != nil {
				t.Fatal(err)
			}
			for _, state := range states {
				if !sameNotes(state.State.(noteState).Notes, []string{"one", "two", "three"}) || state.Through.Next != 3 {
					t.Fatalf("state=%+v", state)
				}
			}
		})
	}
}

func TestGroupedProjectionReadRejectsInheritedCheckpoints(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	mod := cacheModule(f.counter)
	mod.Projections[0].InheritanceOverride = module.InheritNone
	var err error
	f.registry, err = module.BuildRegistry(mod)
	if err != nil {
		t.Fatal(err)
	}
	w := f.open(t, WritersConfig{})
	defer w.Close(ctx)
	f.commit(t, w, "c1", "one")
	f.commit(t, w, "c2", "two")
	if _, err := f.store.Create(ctx, session.CreateRequest{SessionID: "child", Fork: &session.ForkOrigin{Session: "s", Seq: 1}}); err != nil {
		t.Fatal(err)
	}
	child, err := acquireTestWriterConfig(ctx, f.store, f.registry, Admission{}, "child", session.OpenOptions{}, WritersConfig{})
	if err != nil {
		t.Fatal(err)
	}
	defer child.Close(ctx)
	f.commit(t, child, "c3", "own")
	for _, id := range []module.ProjectionID{alphaID, betaID} {
		if err := f.cache.Save(ctx, "child", id, 1, f.encodeState(t, "one", "two"), ledger.Head{Next: 2}); err != nil {
			t.Fatal(err)
		}
	}
	states, err := session.LoadProjections(ctx, session.NewProjectionReader(f.store, f.registry, f.cache), "child", session.ProjectionRef{ID: alphaID, Version: 1}, session.ProjectionRef{ID: betaID, Version: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !sameNotes(states[0].State.(noteState).Notes, []string{"own"}) {
		t.Fatalf("own=%+v", states[0])
	}
	if !sameNotes(states[1].State.(noteState).Notes, []string{"one", "two", "own"}) {
		t.Fatalf("inherited=%+v", states[1])
	}
}
