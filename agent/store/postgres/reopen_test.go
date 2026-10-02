package postgres_test

import (
	"context"
	"github.com/felinics/twilight/agent/store/postgres"
	"github.com/felinics/twilight/agent/store/postgres/postgrestest"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/writer"
	"sync"
	"testing"
	"testing/fstest"
)

const rowsProjection = module.ProjectionID("twilight/z/rows")

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

func (c *foldCounter) get() int { c.mu.Lock(); defer c.mu.Unlock(); return c.calls }
func (c *foldCounter) reset()   { c.mu.Lock(); c.calls = 0; c.mu.Unlock() }

func counterRegistry(t testing.TB, c *foldCounter) *module.Registry {
	t.Helper()
	const typ ledger.EventType = "twilight/z/row"
	r, err := module.BuildRegistry(module.ModuleDescriptor{Source: module.SourceTwilight, ID: "z",
		Streams: []module.StreamDefinition{{Domain: "z", Inheritance: module.Inherited}},
		Events: []module.EventDefinition{{Type: typ, Domain: "z",
			Codecs: map[module.PayloadVersion]module.PayloadCodec{module.Pre(1): module.JSONCodec[rowPayload]{}}}},
		Projections: []module.ProjectionDefinition{{
			ID: rowsProjection, Version: 1, Consumes: []ledger.EventType{typ},
			Initial: func() (any, error) { return rowState{}, nil },
			Apply: func(state any, e module.DecodedEvent) (any, error) {
				c.mu.Lock()
				c.calls++
				c.mu.Unlock()
				// Amortized append: the fold is linear and the earlier state
				// keeps its own length, so the benchmark measures the kernel
				// and the store, not a copy of the state per event.
				s := state.(rowState)
				return rowState{Rows: append(s.Rows, e.Value.(rowPayload).Text)}, nil
			},
			StateCodec: module.JSONStateCodec[rowState]{},
		}}})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// A Session reopened by a second handle over the database (another
// replica) starts its projections from the saved state and folds only the
// commits past it: the cost of an activation is the tail, not the history
// (EXT-PRJ-3, APP-ACT). The saved state is dropped with the root.
func TestReopenFoldsOnlyTheTail(t *testing.T) {
	ctx := context.Background()
	dsn := postgrestest.Schema(t)
	first := postgrestest.OpenDSN(t, dsn).Sessions()
	const sid session.SessionID = "restart"
	counter := &foldCounter{}
	if _, err := first.Create(ctx, session.CreateRequest{SessionID: sid}); err != nil {
		t.Fatal(err)
	}
	commit := func(w writer.Writer, id string, text string) {
		t.Helper()
		res, err := w.Commit(ctx, func(writer.View) (*writer.SemanticGroup, error) {
			return &writer.SemanticGroup{CommitID: ledger.CommitID(id),
				Batches: []writer.TypedBatch{{Domain: ledger.Domain{Name: "z"},
					Events: []writer.TypedEvent{{Type: "twilight/z/row", Value: rowPayload{Text: text}}}}}}, nil
		})
		if err != nil || res.Outcome != writer.CommitApplied {
			t.Fatalf("commit %s: outcome=%s err=%v", id, res.Outcome, err)
		}
	}
	writers := writer.NewWriters(first, counterRegistry(t, counter), writer.Admission{}, session.OpenOptions{},
		writer.WritersConfig{Cache: first.ProjectionCache(), CachePolicy: module.CacheEvery(0).AtClose()})
	w, err := writers.Writer(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	for i, text := range []string{"a", "b", "c"} {
		commit(w, string(rune('1'+i)), text)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if n := counter.get(); n != 3 {
		t.Fatalf("first process folded %d events, want 3", n)
	}
	// The second replica: nothing to fold on open, one event per new commit.
	second := postgrestest.OpenDSN(t, dsn).Sessions()
	counter.reset()
	writers2 := writer.NewWriters(second, counterRegistry(t, counter), writer.Admission{}, session.OpenOptions{},
		writer.WritersConfig{Cache: second.ProjectionCache(), CachePolicy: module.CacheEvery(0).AtClose()})
	w2, err := writers2.Writer(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if n := counter.get(); n != 0 {
		t.Fatalf("second process folded %d events on open, want 0 (saved state covered the history)", n)
	}
	commit(w2, "4", "d")
	if n := counter.get(); n != 1 {
		t.Fatalf("second process folded %d events after one commit, want 1", n)
	}
	state, _, err := w2.Projections().Load(ctx, sid, rowsProjection, 1)
	if err != nil || len(state.(rowState).Rows) != 4 {
		t.Fatalf("projection after reopen = %+v %v, want four rows", state, err)
	}
	if err := w2.Close(ctx); err != nil {
		t.Fatal(err)
	}
	cache := second.ProjectionCache()
	if _, through, ok, err := cache.Load(ctx, sid, rowsProjection, 1); err != nil || !ok || through.Next != 4 {
		t.Fatalf("saved state = through:%+v ok:%v %v, want through 4", through, ok, err)
	}
	if _, err := second.Delete(ctx, sid); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, err := cache.Load(ctx, sid, rowsProjection, 1); err != nil || ok {
		t.Fatalf("saved state after delete = ok:%v %v, want gone", ok, err)
	}
}

// Migration versions come from the file names, contiguous from 1.
func TestMigrationVersionsFromFileNames(t *testing.T) {
	cases := []struct {
		name  string
		files []string
		want  []int64
		ok    bool
	}{
		{name: "ordered", files: []string{"0001_init.sql", "0002_b.sql", "0010_c.sql"}, ok: false},
		{name: "contiguous out of listing order", files: []string{"0002_b.sql", "0001_init.sql", "0003_c.sql"}, want: []int64{1, 2, 3}, ok: true},
		{name: "no version prefix", files: []string{"init.sql"}},
		{name: "duplicate version", files: []string{"0001_a.sql", "0001_b.sql"}},
		{name: "does not start at one", files: []string{"0002_a.sql"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			files := fstest.MapFS{}
			for _, f := range tc.files {
				files["migrations/"+f] = &fstest.MapFile{Data: []byte("SELECT 1;")}
			}
			versions, err := postgres.MigrationVersions(files)
			if (err == nil) != tc.ok {
				t.Fatalf("versions = %v %v, want ok=%v", versions, err, tc.ok)
			}
			if tc.ok && len(versions) != len(tc.want) {
				t.Fatalf("versions = %v, want %v", versions, tc.want)
			}
			for i := range tc.want {
				if tc.ok && versions[i] != tc.want[i] {
					t.Fatalf("versions = %v, want %v", versions, tc.want)
				}
			}
		})
	}
}
