package artifacttest_test

import (
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/session/filestore"
)

// The reference bindings and ledger run the suite next to the file-backed
// cas store, the kernel's own content store.
func TestMapConformance(t *testing.T) {
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
