package writer

import (
	"context"
	"fmt"
	"github.com/felinics/twilight/agentcore/session"
	"testing"
)

// BenchmarkWriterOpen measures EXT-WRT-1/PRJ-5 against the same log twice: with
// no cache the Writer folds every group, with a warm cache entry it folds only
// what the entry does not cover. full-fold is the before, from-cache the after.
func BenchmarkWriterOpen(b *testing.B) {
	ctx := context.Background()
	for _, n := range []int{400, 800, 1600, 3200, 6400} {
		f := newCacheFixture(b)
		w := f.open(b, WritersConfig{})
		for i := 0; i < n; i++ {
			f.commit(b, w, fmt.Sprintf("c%d", i), fmt.Sprintf("n%d", i))
		}
		if err := w.Close(ctx); err != nil {
			b.Fatal(err)
		}
		// Prime a cache entry at the end of the log, so the from-cache case
		// starts from a complete entry rather than folding once.
		primed := f.open(b, WritersConfig{Cache: f.cache})
		if err := primed.Close(ctx); err != nil {
			b.Fatal(err)
		}

		b.Run(fmt.Sprintf("full-fold/%d", n), func(b *testing.B) {
			benchmarkOpen(b, f, WritersConfig{})
		})
		b.Run(fmt.Sprintf("from-cache/%d", n), func(b *testing.B) {
			benchmarkOpen(b, f, WritersConfig{Cache: f.cache})
		})
	}
}

// benchmarkOpen times OpenWriter alone. Takeover lets the loop reopen without a
// Close, whose cache flush would otherwise land inside the measurement.
func benchmarkOpen(b *testing.B, f *cacheFixture, cfg WritersConfig) {
	b.Helper()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := openWriter(ctx, f.store, f.registry, Admission{}, "s", session.OpenOptions{Takeover: true}, cfg); err != nil {
			b.Fatal(err)
		}
	}
}
