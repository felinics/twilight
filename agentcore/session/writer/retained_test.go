package writer

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
)

// countState is O(1) on purpose: a projection whose state grows with the log
// would hide what the Writer itself retains behind the folded result.
type countState struct{ N int }

type countPayload struct {
	Text string `json:"text"`
}

func countModule() extension.ModuleDescriptor {
	typ := tpfx("q") + "row"
	return extension.ModuleDescriptor{Source: extension.SourceTwilight, ID: "q", Streams: noteStreams(),
		Events: []extension.EventDefinition{{Type: typ, Stream: noteDomain,
			Codecs: map[extension.PayloadVersion]extension.PayloadCodec{1: extension.JSONCodec[countPayload]{}}}},
		Projections: []extension.ProjectionDefinition{{
			ID: extension.ProjectionID(string(typ) + "s"), Version: 1, Consumes: []session.EventType{typ},
			Initial: func() (any, error) { return countState{}, nil },
			Apply: func(state any, _ extension.DecodedEvent) (any, error) {
				return countState{N: state.(countState).N + 1}, nil
			},
			StateCodec: extension.JSONStateCodec[countState]{},
		}}}
}

// sinkWriter keeps the measured Writer reachable across the GC.
var sinkWriter Writer

// TestWriterRetainsNoHistory guards SES-REP-3/4. A Writer answers idempotency
// from the kernel's index, so what a reopened Writer retains must not depend on
// how long the log is. It used to: every entry of the Writer's own index held
// the group's rows, each a subslice of the one slice the rebuild read, so the
// whole parsed log stayed alive for the lifetime of the Writer -- measured at
// 2.2 MB for 6400 rows, and identical with or without a cache entry. The
// sizes are kept small because every commit is an fsync'd file write.
func TestWriterRetainsNoHistory(t *testing.T) {
	small := retainedOnReopen(t, 200)
	large := retainedOnReopen(t, 1600)
	t.Logf("retained on reopen: 200 commits = %.3f MB, 1600 commits = %.3f MB", mib(small), mib(large))
	// Eight times the commits must not cost a linear amount: a returning index
	// would put the larger session past half a megabyte.
	if large > 256<<10 {
		t.Fatalf("reopening a 1600-commit session retained %.3f MB; the Writer holds no per-commit state", mib(large))
	}
}

func mib(n uint64) float64 { return float64(n) / (1 << 20) }

// retainedOnReopen reports the heap an opened Writer keeps alive, taking the
// worse of the fold-everything and resume-from-cache paths.
func retainedOnReopen(t *testing.T, commits int) uint64 {
	t.Helper()
	ctx := context.Background()
	store := filestoretest.Store(t)
	reg, err := extension.BuildRegistry(countModule())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Create(ctx, session.CreateRequest{SessionID: "s"}); err != nil {
		t.Fatal(err)
	}
	cache := extension.NewMemoryProjectionCache()
	w, err := openWriter(ctx, store, reg, Admission{}, "s", session.OpenOptions{}, WritersConfig{})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < commits; i++ {
		group := &SemanticGroup{CommitID: session.CommitID(fmt.Sprintf("c%d", i)),
			Batches: noteBatch(TypedEvent{Type: tpfx("q") + "row", Value: countPayload{Text: fmt.Sprintf("t%d", i)}})}
		if _, err := w.Commit(ctx, func(View) (*SemanticGroup, error) { return group, nil }); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	primed, err := openWriter(ctx, store, reg, Admission{}, "s", session.OpenOptions{Takeover: true}, WritersConfig{Cache: cache})
	if err != nil {
		t.Fatal(err)
	}
	if err := primed.Close(ctx); err != nil {
		t.Fatal(err)
	}
	measure := func(cfg WritersConfig) uint64 {
		runtime.GC()
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		got, err := openWriter(ctx, store, reg, Admission{}, "s", session.OpenOptions{Takeover: true}, cfg)
		if err != nil {
			t.Fatal(err)
		}
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&after)
		sinkWriter = got
		// Opening can also retire garbage the baseline still counted, so the
		// difference is clamped rather than allowed to underflow.
		if after.HeapAlloc <= before.HeapAlloc {
			return 0
		}
		return after.HeapAlloc - before.HeapAlloc
	}
	folded := measure(WritersConfig{})
	cached := measure(WritersConfig{Cache: cache})
	if cached > folded {
		return cached
	}
	return folded
}
