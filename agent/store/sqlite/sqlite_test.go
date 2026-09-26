package sqlite_test

import (
	"context"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/store/sqlite"
	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/session/filestore"
)

// The SQLite bindings and ledger run the artifact conformance suite next to
// the file-backed cas store.
func TestArtifactConformance(t *testing.T) {
	artifacttest.Run(t, func(t *testing.T) artifacttest.Fixture {
		var mu sync.Mutex
		now := time.Unix(1_000_000, 0)
		clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
		db := sqlitetest.Open(t)
		bindings := db.Bindings()
		root := t.TempDir()
		return artifacttest.Fixture{
			Bindings: bindings,
			Ledger:   db.Ledger(artifact.SetBuilder{Resolver: bindings}),
			NewContent: func(t *testing.T, authority artifact.Authority) artifact.ContentStore {
				store, err := filestore.NewContentStore(root, authority, filestore.ContentStoreOptions{Now: clock, EphemeralTTL: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				return store
			},
			Advance: func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() },
		}
	})
}

// The SQLite execution store runs the execution store suite with two
// database handles over one file: the second handle stands for a second
// process.
func TestExecutionStoreConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Fixture {
		var mu sync.Mutex
		now := time.Unix(2_000_000, 0)
		clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
		path := filepath.Join(t.TempDir(), "records.db")
		open := func(t *testing.T) executionstore.Store {
			db, err := sqlite.Open(path, sqlite.Options{Now: clock})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			return db.Executions()
		}
		return storetest.Fixture{Store: open(t), Reopen: open, Advance: func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }}
	})
}

// List folds every ledger; it is the adapter's operator view, not part of
// the contract.
func TestExecutionStoreList(t *testing.T) {
	ctx := context.Background()
	s := sqlitetest.Open(t).Executions()
	for _, id := range []run.EffectID{"e1", "e2"} {
		a := effect.Assignment{Session: "s", RunID: "r", StepID: "step", Effect: id, Body: effect.ModelAssignment{Model: "m", RequestDigest: "sha256:req"}}
		if err := s.Seed(ctx, executionstore.Execution{ExecutionState: executionstore.ExecutionState{Assignment: a, State: effect.ExecutionAccepted}}); err != nil {
			t.Fatal(err)
		}
	}
	list, err := s.List(ctx)
	if err != nil || len(list) != 2 {
		t.Fatalf("list = %d %v, want 2", len(list), err)
	}
}
