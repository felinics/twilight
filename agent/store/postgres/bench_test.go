package postgres_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/felinics/twilight/agent/store/postgres"
	"github.com/felinics/twilight/agent/store/postgres/postgrestest"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// The benchmarks measure the reopen path the activation model turns into a
// per-Turn cost (APP-ACT-5), against a Postgres a run is given with
// -postgres.dsn; without one they are skipped. Sizes are commits per
// Session: 1k, 10k, 100k.
//
//	go test ./agent/store/postgres/ -run xxx -bench . -benchtime 3x -args -postgres.dsn=...

var sizes = []int{1_000, 10_000, 100_000}

// seedSession creates a Session whose tip holds n commits of one small
// event each, loaded in bulk, and returns its tip segment.
func seedSession(b *testing.B, store *postgres.SessionStore, dbh *postgres.DB, sid session.SessionID, n int) session.SegmentID {
	b.Helper()
	ctx := context.Background()
	// Payloads carry the module's version envelope, as a Writer writes them,
	// so a reopening Writer decodes and folds them.
	registry := counterRegistry(b, &foldCounter{})
	header, err := store.Create(ctx, session.CreateRequest{SessionID: sid, CreatedAtUnixMilli: 1})
	if err != nil {
		b.Fatal(err)
	}
	const chunk = 10_000
	for from := 0; from < n; from += chunk {
		to := min(from+chunk, n)
		commits := make([]session.Commit, 0, to-from)
		for i := from; i < to; i++ {
			commits = append(commits, session.Commit{Seq: session.CommitSeq(i), CommitID: session.CommitID(fmt.Sprintf("c%08d", i)), //nolint:gosec // G115: bounded by n
				Batches: []session.StreamBatch{{Stream: session.StreamRef{Domain: "z"}, Events: []session.Event{{
					Type: "twilight/z/row", RecordedAtUnixMilli: 1, Payload: encodeRow(b, registry, i)}}}}})
		}
		if err := dbh.SeedSegmentCommits(ctx, header.ID, commits); err != nil {
			b.Fatal(err)
		}
	}
	return header.ID
}

// The kernel Open: the CommitIndex scan of the tip plus the root's
// ownership write. This is the floor of every reopen.
func BenchmarkSessionOpen(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("commits=%d", n), func(b *testing.B) {
			dbh := postgrestest.Open(b)
			store := dbh.Sessions()
			seedSession(b, store, dbh, "s", n)
			ctx := context.Background()
			b.ResetTimer()
			for range b.N {
				h, err := store.Open(ctx, "s", session.OpenOptions{})
				if err != nil {
					b.Fatal(err)
				}
				if err := h.Close(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// The index read alone, without the ownership write.
func BenchmarkCommitIndexScan(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("commits=%d", n), func(b *testing.B) {
			dbh := postgrestest.Open(b)
			store := dbh.Sessions()
			seg := seedSession(b, store, dbh, "s", n)
			ctx := context.Background()
			b.ResetTimer()
			for range b.N {
				idx, _, err := store.IndexOf(ctx, seg)
				if err != nil || len(idx.Entries) != n {
					b.Fatalf("index = %d entries, %v", len(idx.Entries), err)
				}
			}
		})
	}
}

// A Writer open with a projection whose state grows with the history
// (the chatlog surface's shape): cold folds the whole log, warm starts
// from a saved state at most CacheEvery behind and folds the tail.
func BenchmarkWriterOpen(b *testing.B) {
	for _, n := range sizes {
		for _, warm := range []bool{false, true} {
			b.Run(fmt.Sprintf("commits=%d/warm=%v", n, warm), func(b *testing.B) {
				dbh := postgrestest.Open(b)
				store := dbh.Sessions()
				seedSession(b, store, dbh, "s", n)
				ctx := context.Background()
				counter := &foldCounter{}
				registry := counterRegistry(b, counter)
				var cache extension.ProjectionCache
				if warm {
					cache = store.ProjectionCache()
					// One Writer open under the default interval leaves the
					// entry at most CacheEvery commits behind the head.
					ws := writer.NewWriters(store, registry, writer.Admission{}, session.OpenOptions{}, writer.WritersConfig{Cache: cache})
					w, err := ws.Writer(ctx, "s")
					if err != nil {
						b.Fatal(err)
					}
					// Force one save at the current head: the state is what a
					// clean Close under AtClose would have left.
					if err := cache.Save(ctx, "s", rowsProjection, 1, mustEncodeRows(b, n-64), session.Head{Next: session.CommitSeq(n - 64)}); err != nil { //nolint:gosec // G115: bounded by n
						b.Fatal(err)
					}
					if err := w.Close(ctx); err != nil {
						b.Fatal(err)
					}
				}
				counter.reset()
				b.ResetTimer()
				for range b.N {
					ws := writer.NewWriters(store, registry, writer.Admission{}, session.OpenOptions{}, writer.WritersConfig{Cache: cache})
					w, err := ws.Writer(ctx, "s")
					if err != nil {
						b.Fatal(err)
					}
					if err := w.Close(ctx); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(counter.get())/float64(b.N), "folded/op")
			})
		}
	}
}

func encodeRow(b *testing.B, registry *extension.Registry, i int) jsonstable.Value {
	b.Helper()
	v, err := registry.Encode("twilight/z/row", rowPayload{Text: fmt.Sprintf("row %d", i)})
	if err != nil {
		b.Fatal(err)
	}
	return v
}

func mustEncodeRows(b *testing.B, n int) jsonstable.Value {
	b.Helper()
	rows := make([]string, n)
	for i := range rows {
		rows[i] = fmt.Sprintf("row %d", i)
	}
	v, err := extension.JSONStateCodec[rowState]{}.Encode(rowState{Rows: rows})
	if err != nil {
		b.Fatal(err)
	}
	return v
}

// One projection cache Save of a state with n rows: the write the interval
// amortizes.
func BenchmarkProjectionCacheSave(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("rows=%d", n), func(b *testing.B) {
			dbh := postgrestest.Open(b)
			cache := dbh.Sessions().ProjectionCache()
			ctx := context.Background()
			state := mustEncodeRows(b, n)
			b.SetBytes(int64(len(state.Bytes())))
			b.ResetTimer()
			for i := range b.N {
				if err := cache.Save(ctx, "s", rowsProjection, 1, state, session.Head{Next: session.CommitSeq(i + 1)}); err != nil { //nolint:gosec // G115: bounded by b.N
					b.Fatal(err)
				}
			}
		})
	}
}

// One page of claims under a sparse identity rule (one in sixteen
// matches) over n claims of one owner.
func BenchmarkClaimPaging(b *testing.B) {
	for _, n := range sizes {
		b.Run(fmt.Sprintf("claims=%d", n), func(b *testing.B) {
			dbh := postgrestest.Open(b)
			ctx := context.Background()
			bindings := dbh.Bindings()
			ledger := dbh.Ledger(nil)
			key, integrity := artifact.CASKey([]byte("body"))
			ref := artifact.Ref{Scheme: artifact.SchemeCAS, Authority: "a", Key: key, Integrity: &integrity, MediaType: "text/plain", Durability: artifact.Pinned}
			bid := artifact.BindingID("b1")
			digest, _ := artifact.DigestBinding(bid, ref)
			if _, err := bindings.CreateBinding(ctx, artifact.Binding{ID: bid, Ref: ref, Digest: digest}); err != nil {
				b.Fatal(err)
			}
			set, err := artifact.SetBuilder{Resolver: bindings}.Build(ctx, []artifact.BindingID{bid})
			if err != nil {
				b.Fatal(err)
			}
			var identities []string
			for i := range n {
				owner := fmt.Sprintf("u%04d", i%16)
				if _, err := ledger.Activate(ctx, artifact.ClaimID(fmt.Sprintf("claim-%08d", i)), artifact.ClaimOwner{Kind: "k", Authority: "s", Identity: owner}, set); err != nil {
					b.Fatal(err)
				}
			}
			identities = []string{"u0007"}
			q := artifact.ClaimOwnerQuery{Kind: "k", Authority: "s", Identities: identities, Limit: 64}
			b.ResetTimer()
			for range b.N {
				page, err := ledger.ClaimsByOwner(ctx, q, artifact.ClaimCursor{})
				if err != nil || len(page.Items) == 0 {
					b.Fatalf("page = %d items, %v", len(page.Items), err)
				}
			}
		})
	}
}
