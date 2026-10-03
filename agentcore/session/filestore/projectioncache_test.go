package filestore_test

import (
	"context"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore"
	"github.com/felinics/twilight/agentcore/session/writer"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// This file covers the durable side of EXT-PRJ-3: the cache entries a Writer
// leaves in the session directory, and a fresh process resuming from them.

const projectID = module.ProjectionID("twilight/z/rows")

type rowPayload struct {
	Text string `json:"text"`
}

type rowState struct {
	Rows []string `json:"rows"`
}

type foldCounter struct {
	mu    sync.Mutex
	calls int
}

func (c *foldCounter) inc() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
}

func (c *foldCounter) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *foldCounter) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = 0
}

// counterModule is a one-projection module whose Apply counts the events it
// folds, which is how these tests tell a resume from a full fold.
func counterModule(c *foldCounter) module.ModuleDescriptor {
	const typ ledger.EventType = "twilight/z/row"
	return module.ModuleDescriptor{Source: module.SourceTwilight, ID: "z",
		Streams: []module.StreamDefinition{{Domain: "z", Inheritance: module.Inherited}},
		Events: []module.EventDefinition{{Type: typ, Domain: "z",
			Codecs: map[module.PayloadVersion]module.PayloadCodec{1: module.JSONCodec[rowPayload]{}}}},
		Projections: []module.ProjectionDefinition{{
			ID: projectID, Version: 1, Consumes: []ledger.EventType{typ},
			Initial: func() (any, error) { return rowState{}, nil },
			Apply: func(state any, e module.DecodedEvent) (any, error) {
				c.inc()
				s := state.(rowState)
				return rowState{Rows: append(append([]string(nil), s.Rows...), e.Value.(rowPayload).Text)}, nil
			},
			StateCodec: module.JSONStateCodec[rowState]{},
		}}}
}

func mustRegistry(t *testing.T, c *foldCounter) *module.Registry {
	t.Helper()
	r, err := module.BuildRegistry(counterModule(c))
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// entryPath is the layout the adapter must keep: a projection ID contains
// slashes, so it is percent-encoded like a Session ID.
func entryPath(root string, sid session.SessionID, v module.ProjectionVersion) string {
	return filepath.Join(root, "sessions", string(sid), "projections", "twilight%2Fz%2Frows", "1.json")
}

func TestProjectionCacheRoundTrip(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	cache := store.ProjectionCache()
	state, err := module.JSONStateCodec[rowState]{}.Encode(rowState{Rows: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}

	if _, _, ok, err := cache.Load(ctx, "s", projectID, 1); ok || err != nil {
		t.Fatalf("absent entry: ok=%v err=%v, want a miss with no error", ok, err)
	}
	want := ledger.Head{Next: 4}
	if err := cache.Save(ctx, "s", projectID, 1, state, want); err != nil {
		t.Fatal(err)
	}
	got, through, ok, err := cache.Load(ctx, "s", projectID, 1)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if through != want {
		t.Errorf("through = %+v, want %+v", through, want)
	}
	if string(got.Bytes()) != string(state.Bytes()) {
		t.Errorf("state = %s, want %s", got.Bytes(), state.Bytes())
	}
}

func TestProjectionCacheIgnoresCorruption(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	store, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	cache := store.ProjectionCache()
	state, err := module.JSONStateCodec[rowState]{}.Encode(rowState{Rows: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := cache.Save(ctx, "s", projectID, 1, state, ledger.Head{Next: 1}); err != nil {
		t.Fatal(err)
	}
	path := entryPath(root, "s", 1)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("entry not at the expected path %s: %v", path, err)
	}

	// A damaged entry is derived data: a miss, never an error.
	for name, raw := range map[string]string{"truncated": `{"through":`, "not json": `nonsense`, "bad state": `{"through":{"next":1,"digest":"d"},"state":{`} {
		if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, _, ok, err := cache.Load(ctx, "s", projectID, 1); ok || err != nil {
			t.Errorf("%s: ok=%v err=%v, want a miss with no error", name, ok, err)
		}
	}
}

// TestProjectionCacheSurvivesRestart is the whole point: a second process over
// the same directory resumes instead of folding the log again.
func TestProjectionCacheSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const sid session.SessionID = "restart"
	counter := &foldCounter{}

	// First process: three commits, then a clean Close.
	first, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	writers := writer.NewWriters(first, mustRegistry(t, counter), writer.Admission{}, session.OpenOptions{},
		writer.WritersConfig{Cache: first.ProjectionCache(), CachePolicy: module.CacheEvery(0).AtClose()})
	w, err := writers.Writer(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	for i, text := range []string{"a", "b", "c"} {
		res, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
			return &writer.SemanticGroup{CommitID: ledger.CommitID(string(rune('1' + i))),
				Batches: []writer.TypedBatch{{Domain: ledger.Domain{Name: "z"},
					Events: []writer.TypedEvent{{Type: "twilight/z/row", Value: rowPayload{Text: text}}}}}}, nil
		})
		if err != nil || res.Outcome != writer.CommitApplied {
			t.Fatalf("commit %d: outcome=%s err=%v", i, res.Outcome, err)
		}
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if counter.get() != 3 {
		t.Fatalf("first process folded %d events, want 3", counter.get())
	}

	// Second process: a fresh Store over the same directory.
	second, err := filestore.New(root)
	if err != nil {
		t.Fatal(err)
	}
	counter.reset()
	writers2 := writer.NewWriters(second, mustRegistry(t, counter), writer.Admission{}, session.OpenOptions{},
		writer.WritersConfig{Cache: second.ProjectionCache()})
	w2, err := writers2.Writer(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if n := counter.get(); n != 0 {
		t.Errorf("the reopened Writer folded %d events, want 0: the entry was not resumed", n)
	}
	state, _, err := w2.Projections().Load(ctx, sid, projectID, 1)
	if err != nil {
		t.Fatal(err)
	}
	got := state.(rowState).Rows
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Errorf("resumed state = %v, want [a b c]", got)
	}
}
