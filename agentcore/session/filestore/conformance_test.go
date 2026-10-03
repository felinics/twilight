package filestore_test

import (
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agentcore/run/sessionstore/storetest"
	"github.com/felinics/twilight/agentcore/session/filestore"
	"github.com/felinics/twilight/agentcore/session/sessiontest"
)

func newStore(t testing.TB) *filestore.Store {
	t.Helper()
	store, err := filestore.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func TestKernelConformance(t *testing.T) {
	sessiontest.Run(t, func(t *testing.T) sessiontest.Fixture {
		now := time.Unix(1_700_000_000, 0)
		var mu sync.Mutex
		clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
		store, err := filestore.NewWithClock(t.TempDir(), clock)
		if err != nil {
			t.Fatal(err)
		}
		return sessiontest.Fixture{Store: store, Now: clock, Advance: func(d time.Duration) { mu.Lock(); now = now.Add(d); mu.Unlock() }}
	})
}

func TestRuntimeConformance(t *testing.T) {
	storetest.Run(t, func(t testing.TB) storetest.Fixture {
		return storetest.Fixture{Store: newStore(t)}
	})
}
