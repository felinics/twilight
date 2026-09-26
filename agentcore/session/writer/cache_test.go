package writer

import (
	"context"
	"sync"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
)

// This file covers EXT-PRJ-3: a projection's folded state living in the
// extension.ProjectionCache, and a reopening Writer resuming from it instead of folding
// the log again.

const (
	alphaID = extension.ProjectionID("twilight/k/alpha")
	betaID  = extension.ProjectionID("twilight/k/beta")
)

// applyCounter records how many events each projection folded, which is how a
// test tells a fold that started from a cache entry from a full one.
type applyCounter struct {
	mu    sync.Mutex
	calls map[extension.ProjectionID]int
}

func newApplyCounter() *applyCounter { return &applyCounter{calls: map[extension.ProjectionID]int{}} }

func (c *applyCounter) inc(id extension.ProjectionID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls[id]++
}

func (c *applyCounter) get(id extension.ProjectionID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls[id]
}

func (c *applyCounter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = map[extension.ProjectionID]int{}
}

// cacheModule declares two projections over one event type. alpha stands for a
// projection the Writer refreshes; beta for one whose owning component does.
func cacheModule(c *applyCounter) extension.ModuleDescriptor {
	typ := tpfx("k") + "row"
	mk := func(id extension.ProjectionID) extension.ProjectionDefinition {
		return extension.ProjectionDefinition{
			ID: id, Version: 1, Consumes: []session.EventType{typ},
			Initial: func() (any, error) { return noteState{}, nil },
			Apply: func(state any, e extension.DecodedEvent) (any, error) {
				c.inc(id)
				s := state.(noteState)
				s.Notes = append(append([]string(nil), s.Notes...), e.Value.(notePayload).Text)
				return s, nil
			},
			StateCodec: extension.JSONStateCodec[noteState]{},
		}
	}
	return extension.ModuleDescriptor{Source: extension.SourceTwilight, ID: "k", Streams: noteStreams(),
		Events:      []extension.EventDefinition{{Type: typ, Stream: noteDomain, Codecs: map[extension.PayloadVersion]extension.PayloadCodec{1: extension.JSONCodec[notePayload]{}}}},
		Projections: []extension.ProjectionDefinition{mk(alphaID), mk(betaID)}}
}

type cacheFixture struct {
	store    session.Store
	registry *extension.Registry
	cache    *extension.MemoryProjectionCache
	counter  *applyCounter
}

func newCacheFixture(t testing.TB) *cacheFixture {
	t.Helper()
	f := &cacheFixture{store: filestoretest.Store(t), cache: extension.NewMemoryProjectionCache(), counter: newApplyCounter()}
	registry, err := extension.BuildRegistry(cacheModule(f.counter))
	if err != nil {
		t.Fatal(err)
	}
	f.registry = registry
	if _, err := f.store.Create(context.Background(), session.CreateRequest{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *cacheFixture) open(t testing.TB, cfg WritersConfig) Writer {
	t.Helper()
	w, err := openWriter(context.Background(), f.store, f.registry, Admission{}, "s", session.OpenOptions{}, cfg)
	if err != nil {
		t.Fatalf("open writer: %v", err)
	}
	return w
}

// commit appends one commit; each text becomes one event.
func (f *cacheFixture) commit(t testing.TB, w Writer, id string, texts ...string) {
	t.Helper()
	res, err := w.Commit(context.Background(), func(View) (*SemanticGroup, error) {
		g := &SemanticGroup{CommitID: session.CommitID(id)}
		var events []TypedEvent
		for _, tx := range texts {
			events = append(events, TypedEvent{Type: tpfx("k") + "row", Value: notePayload{Text: tx}})
		}
		g.Batches = noteBatch(events...)
		return g, nil
	})
	if err != nil {
		t.Fatalf("commit %s: %v", id, err)
	}
	if res.Outcome != CommitApplied {
		t.Fatalf("commit %s: outcome %s (%s)", id, res.Outcome, res.Detail)
	}
}

func (f *cacheFixture) notes(t testing.TB, w Writer, id extension.ProjectionID) []string {
	t.Helper()
	state, _, err := w.Projections().Load(context.Background(), "s", id, 1)
	if err != nil {
		t.Fatalf("load %s: %v", id, err)
	}
	return state.(noteState).Notes
}

// commits reads the committed log, which is where the digests a cache entry
// must record come from.
func (f *cacheFixture) commits(t *testing.T) []session.Commit {
	t.Helper()
	page, err := f.store.ReadCommits(context.Background(), session.CommitReadRequest{SessionID: "s"})
	if err != nil {
		t.Fatal(err)
	}
	return page.Commits
}

// encodeState builds the cached form of a projection state.
func (f *cacheFixture) encodeState(t *testing.T, notes ...string) jsonstable.Value {
	t.Helper()
	v, err := extension.JSONStateCodec[noteState]{}.Encode(noteState{Notes: notes})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func sameNotes(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestWriterCachesAtCloseAndResumesEverything: a clean Close leaves an entry at
// the end of the log, so reopening folds nothing.
func TestWriterCachesAtCloseAndResumesEverything(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	w := f.open(t, WritersConfig{Cache: f.cache, CachePolicy: extension.CacheEvery(0).AtClose()})
	f.commit(t, w, "c1", "n1")
	f.commit(t, w, "c2", "n2")
	f.commit(t, w, "c3", "n3")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []extension.ProjectionID{alphaID, betaID} {
		_, through, ok, err := f.cache.Load(ctx, "s", id, 1)
		if err != nil || !ok {
			t.Fatalf("%s: entry after Close: ok=%v err=%v", id, ok, err)
		}
		if through.Next != 3 {
			t.Fatalf("%s: Close left the entry at %d, want 3", id, through.Next)
		}
	}

	f.counter.reset()
	reopened := f.open(t, WritersConfig{Cache: f.cache, CachePolicy: extension.CacheEvery(0).AtClose()})
	for _, id := range []extension.ProjectionID{alphaID, betaID} {
		if n := f.counter.get(id); n != 0 {
			t.Errorf("%s folded %d events, want 0 when the entry covers the whole log", id, n)
		}
		if got := f.notes(t, reopened, id); !sameNotes(got, []string{"n1", "n2", "n3"}) {
			t.Errorf("%s notes = %v, want [n1 n2 n3]", id, got)
		}
	}
}

// TestWriterResumesOnlyTheUncoveredTail is the point of the mechanism: an entry
// behind the head means only the remainder is folded.
func TestWriterResumesOnlyTheUncoveredTail(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	w := f.open(t, WritersConfig{Cache: f.cache, CachePolicy: extension.CacheEvery(0).AtClose()})
	f.commit(t, w, "c1", "n1")
	f.commit(t, w, "c2", "n2")
	f.commit(t, w, "c3", "n3")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	commits := f.commits(t)
	if len(commits) != 3 {
		t.Fatalf("log has %d commits, want 3", len(commits))
	}
	// Rewind alpha's entry to cover the first two commits only.
	// A stale entry stands in for a process that ended before its last
	// refresh; Save is monotonic, so the current entry is dropped first.
	f.cache.Delete("s", alphaID, 1)
	if err := f.cache.Save(ctx, "s", alphaID, 1, f.encodeState(t, "n1", "n2"), session.Head{Next: 2}); err != nil {
		t.Fatal(err)
	}

	f.counter.reset()
	reopened := f.open(t, WritersConfig{Cache: f.cache})
	if n := f.counter.get(alphaID); n != 1 {
		t.Errorf("alpha folded %d events, want 1 (only the covered tail)", n)
	}
	if n := f.counter.get(betaID); n != 0 {
		t.Errorf("beta folded %d events, want 0", n)
	}
	if got := f.notes(t, reopened, alphaID); !sameNotes(got, []string{"n1", "n2", "n3"}) {
		t.Errorf("alpha notes = %v, want [n1 n2 n3]", got)
	}
}

// TestWriterRejectsUnusableCacheEntries: every entry that cannot be trusted
// falls back to folding the whole log, and never fails the open.
func TestWriterRejectsUnusableCacheEntries(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	w := f.open(t, WritersConfig{Cache: f.cache})
	f.commit(t, w, "c1", "n1", "n2")
	f.commit(t, w, "c2", "n3")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	commits := f.commits(t)
	if len(commits) != 2 {
		t.Fatalf("fixture log is not shaped as expected: %+v", commits)
	}
	garbage, err := jsonstable.FromValue(map[string]any{"notes": 7})
	if err != nil {
		t.Fatal(err)
	}

	cases := map[string]struct {
		through session.Head
		state   jsonstable.Value
	}{
		"ahead of the log":  {through: session.Head{Next: 9}},
		"empty head":        {through: session.Head{}},
		"undecodable state": {through: session.Head{Next: 2}, state: garbage},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			state := tc.state
			if state.IsZero() {
				state = f.encodeState(t, "n1", "n2", "n3")
			}
			if err := f.cache.Save(ctx, "s", alphaID, 1, state, tc.through); err != nil {
				t.Fatal(err)
			}
			f.counter.reset()
			reopened, err := openWriter(ctx, f.store, f.registry, Admission{}, "s", session.OpenOptions{Takeover: true}, WritersConfig{Cache: f.cache})
			if err != nil {
				t.Fatalf("open with an unusable entry: %v", err)
			}
			// A rejected entry means the whole log is folded again: three events.
			if n := f.counter.get(alphaID); n != 3 {
				t.Errorf("alpha folded %d events, want 3 (a full fold)", n)
			}
			if got := f.notes(t, reopened, alphaID); !sameNotes(got, []string{"n1", "n2", "n3"}) {
				t.Errorf("alpha notes = %v, want [n1 n2 n3]", got)
			}
			if err := reopened.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestWriterCachePolicyGovernsWritingButNotReading separates the two halves of
// the contract: a policy only decides who writes an entry, while a Writer
// always starts from an entry it finds.
func TestWriterCachePolicyGovernsWritingButNotReading(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	// A policy that declines alpha and defers for beta.
	policy := extension.CacheEvery(1).AtClose().Exclude(alphaID)

	w := f.open(t, WritersConfig{Cache: f.cache, CachePolicy: policy})
	f.commit(t, w, "c1", "n1")
	f.commit(t, w, "c2", "n2")
	if _, _, ok, _ := f.cache.Load(ctx, "s", alphaID, 1); ok {
		t.Error("alpha has an entry although the policy declines it")
	}
	_, through, ok, err := f.cache.Load(ctx, "s", betaID, 1)
	if err != nil || !ok || through.Next != 2 {
		t.Fatalf("beta entry: ok=%v through=%d err=%v, want an entry at 2", ok, through.Next, err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := f.cache.Load(ctx, "s", alphaID, 1); ok {
		t.Error("alpha has an entry after Close although the policy declines it")
	}

	// An entry alpha's owner wrote is still used, policy or not.
	if err := f.cache.Save(ctx, "s", alphaID, 1, f.encodeState(t, "n1", "n2"), session.Head{Next: 2}); err != nil {
		t.Fatal(err)
	}
	f.counter.reset()
	reopened := f.open(t, WritersConfig{Cache: f.cache, CachePolicy: policy})
	if n := f.counter.get(alphaID); n != 0 {
		t.Errorf("alpha folded %d events, want 0: a declined projection is still started from", n)
	}
	if got := f.notes(t, reopened, alphaID); !sameNotes(got, []string{"n1", "n2"}) {
		t.Errorf("alpha notes = %v, want [n1 n2]", got)
	}
}

// TestCacheEveryBoundsHowFarBehindAnEntryFalls pins the invariant a deployment
// relies on: between refreshes a projection's entry is at most n commits behind.
func TestCacheEveryBoundsHowFarBehindAnEntryFalls(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	w := f.open(t, WritersConfig{Cache: f.cache, CachePolicy: extension.CacheEvery(3)})
	// The first two commits are inside the interval: nothing is written yet.
	f.commit(t, w, "c1", "n1")
	f.commit(t, w, "c2", "n2")
	if _, _, ok, _ := f.cache.Load(ctx, "s", alphaID, 1); ok {
		t.Error("an entry was written before the interval elapsed")
	}
	f.commit(t, w, "c3", "n3")
	_, through, ok, err := f.cache.Load(ctx, "s", alphaID, 1)
	if err != nil || !ok || through.Next != 3 {
		t.Fatalf("entry after three commits: ok=%v through=%d err=%v, want an entry at 3", ok, through.Next, err)
	}
	// Close refreshes regardless of the interval.
	f.commit(t, w, "c4", "n4")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	// Close is no exception to the interval: the entry lags by one commit,
	// which the next Writer folds, instead of the whole state being written
	// once more (EXT-PRJ-7).
	_, through, _, err = f.cache.Load(ctx, "s", alphaID, 1)
	if err != nil || through.Next != 3 {
		t.Fatalf("entry after Close: through=%d err=%v, want 3 (the interval governs Close too)", through.Next, err)
	}
	f.counter.reset()
	reopened := f.open(t, WritersConfig{Cache: f.cache, CachePolicy: extension.CacheEvery(3)})
	if n := f.counter.get(alphaID); n != 1 {
		t.Errorf("alpha folded %d events on reopen, want 1 (the commit past the entry)", n)
	}
	if got := f.notes(t, reopened, alphaID); !sameNotes(got, []string{"n1", "n2", "n3", "n4"}) {
		t.Errorf("alpha notes = %v", got)
	}
}

// A Save that arrives after a later one never moves an entry back: the
// cache IO runs outside the Writer's critical section (EXT-PRJ-7).
func TestCacheSaveIsMonotonic(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	if err := f.cache.Save(ctx, "s", alphaID, 1, f.encodeState(t, "n1", "n2"), session.Head{Next: 2}); err != nil {
		t.Fatal(err)
	}
	if err := f.cache.Save(ctx, "s", alphaID, 1, f.encodeState(t, "n1"), session.Head{Next: 1}); err != nil {
		t.Fatal(err)
	}
	_, through, ok, err := f.cache.Load(ctx, "s", alphaID, 1)
	if err != nil || !ok || through.Next != 2 {
		t.Fatalf("entry after a late save: through=%d ok=%v err=%v, want 2", through.Next, ok, err)
	}
}

// TestWriterWithoutCacheFoldsEverything is the unchanged deployment: no cache
// configured means no entry is written and none is started from.
func TestWriterWithoutCacheFoldsEverything(t *testing.T) {
	ctx := context.Background()
	f := newCacheFixture(t)
	w := f.open(t, WritersConfig{})
	f.commit(t, w, "c1", "n1")
	f.commit(t, w, "c2", "n2")
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := f.cache.Load(ctx, "s", alphaID, 1); ok {
		t.Error("an entry was written although no cache was configured")
	}
	f.counter.reset()
	reopened := f.open(t, WritersConfig{})
	if n := f.counter.get(alphaID); n != 2 {
		t.Errorf("alpha folded %d events, want 2", n)
	}
	if got := f.notes(t, reopened, alphaID); !sameNotes(got, []string{"n1", "n2"}) {
		t.Errorf("alpha notes = %v, want [n1 n2]", got)
	}
}

// TestCoversCommit pins the validation that keeps an entry recorded at a head
// the log does not have, or at a commit the tip inherits, from being started
// from (EXT-PRJ-3). The commit is the atomic unit: every commit boundary is a
// fold boundary, but only the tip's own boundaries were folded under its
// inheritance policy.
func TestCoversCommit(t *testing.T) {
	commits := []session.Commit{{Seq: 0}, {Seq: 1}, {Seq: 2}}
	root := session.SegmentHeader{ID: "h"}
	// A tip that inherits the first two commits and wrote the third.
	child := session.SegmentHeader{ID: "c", Parent: &session.LedgerRef{Segment: "h", Seq: 1}}
	cases := map[string]struct {
		header  session.SegmentHeader
		through session.Head
		want    bool
	}{
		"first commit":            {root, session.Head{Next: 1}, true},
		"mid log":                 {root, session.Head{Next: 2}, true},
		"end of the log":          {root, session.Head{Next: 3}, true},
		"empty":                   {root, session.Head{}, false},
		"past the log":            {root, session.Head{Next: 4}, false},
		"seq does not match head": {root, session.Head{Next: 99}, false},
		"inherited commit":        {child, session.Head{Next: 1}, false},
		"inherited boundary":      {child, session.Head{Next: 2}, false},
		"tip's own commit":        {child, session.Head{Next: 3}, true},
	}
	at := func(seq session.CommitSeq) (session.Commit, bool) {
		for _, c := range commits {
			if c.Seq == seq {
				return c, true
			}
		}
		return session.Commit{}, false
	}
	for name, tc := range cases {
		if got := coversCommit(tc.header, tc.through, at); got != tc.want {
			t.Errorf("%s: coversCommit = %v, want %v", name, got, tc.want)
		}
	}
}
