package filestore

import (
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
)

// The file-backed cas store runs the artifact conformance suite next to the
// SQLite bindings and ledger; a fresh root per fixture.
func TestContentStoreConformance(t *testing.T) {
	artifacttest.Run(t, func(t *testing.T) artifacttest.Fixture {
		var mu sync.Mutex
		now := time.Unix(1_000_000, 0)
		clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
		bindings, ledger := artifacttest.Stores(t)
		root := t.TempDir()
		return artifacttest.Fixture{
			Bindings: bindings,
			Ledger:   ledger,
			NewContent: func(t *testing.T, authority artifact.Authority) artifact.ContentStore {
				store, err := NewContentStore(root, authority, ContentStoreOptions{Now: clock, EphemeralTTL: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				return store
			},
			Advance: func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() },
		}
	})
}

func TestContentStorePutLimit(t *testing.T) {
	artifacttest.PutLimit(t, func(t *testing.T, maxBytes int64) artifact.ContentStore {
		store, err := NewContentStore(t.TempDir(), "a", ContentStoreOptions{MaxBytes: maxBytes})
		if err != nil {
			t.Fatal(err)
		}
		return store
	})
}
