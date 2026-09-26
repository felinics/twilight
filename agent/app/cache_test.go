package app_test

import (
	"context"
	"sync"
	"testing"

	"github.com/felinics/twilight/agent/app"
	agentinput "github.com/felinics/twilight/agent/input"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// countingCache wraps the in-memory cache to count writes, so a test can see
// whether the Host's interval reached the Writer (APP-MEM-2, EXT-PRJ-7).
type countingCache struct {
	inner *extension.MemoryProjectionCache
	mu    sync.Mutex
	saves int
}

func newCountingCache() *countingCache {
	return &countingCache{inner: extension.NewMemoryProjectionCache()}
}

func (c *countingCache) Load(ctx context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (jsonstable.Value, session.Head, bool, error) {
	return c.inner.Load(ctx, sid, id, v)
}

func (c *countingCache) Save(ctx context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion, state jsonstable.Value, through session.Head) error {
	c.mu.Lock()
	c.saves++
	c.mu.Unlock()
	return c.inner.Save(ctx, sid, id, v, state, through)
}

func (c *countingCache) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.saves
}

// TestHostCacheEveryIsConfigurable pins the deployment's knob all the way to
// the Writer: a small interval writes entries as the log grows, a large one
// leaves the log uncached until Close, and Close writes regardless.
func TestHostCacheEveryIsConfigurable(t *testing.T) {
	ctx := context.Background()
	for name, tc := range map[string]struct {
		every      session.CommitSeq
		wantBefore bool
	}{
		"an interval of one commit writes after the first commit": {every: 1, wantBefore: true},
		"a large interval writes nothing, at Close included":      {every: 1 << 40, wantBefore: false},
	} {
		t.Run(name, func(t *testing.T) {
			cache := newCountingCache()
			h := newHost(t, app.Config{Cache: cache, CacheEvery: tc.every}, nil)
			const sid session.SessionID = "s-interval"
			if err := h.EnsureSession(ctx, sid); err != nil {
				t.Fatal(err)
			}
			owned, err := h.Owner.Open(ctx, sid)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := h.Owner.Chatlog.Submit(ctx, owned.Writer(), "in-1", agentinput.Text("hello")); err != nil {
				t.Fatal(err)
			}
			if got := cache.count() > 0; got != tc.wantBefore {
				t.Fatalf("wrote entries after one commit = %v, want %v (every=%d)", got, tc.wantBefore, tc.every)
			}
			// Close is always a refresh point, whatever the interval.
			if err := h.Close(ctx); err != nil {
				t.Fatal(err)
			}
			// Close obeys the same interval (EXT-PRJ-7): a large one never
			// writes a whole state per Turn, and the next owner folds the tail.
			if got := cache.count() > 0; got != tc.wantBefore {
				t.Errorf("wrote entries by Close = %v, want %v (every=%d)", got, tc.wantBefore, tc.every)
			}
		})
	}
}

// TestHostNeverCachesTheMachineProjection is the other half of APP-MEM-2:
// the Writer leaves the machine projection to the Runtime's SnapshotPolicy, so
// no entry appears for it however small the interval is.
func TestHostNeverCachesTheMachineProjection(t *testing.T) {
	ctx := context.Background()
	cache := newCountingCache()
	h := newHost(t, app.Config{Cache: cache, CacheEvery: 1}, nil)
	const sid session.SessionID = "s-machine"
	if err := h.EnsureSession(ctx, sid); err != nil {
		t.Fatal(err)
	}
	owned, err := h.Owner.Open(ctx, sid)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.Owner.Chatlog.Submit(ctx, owned.Writer(), "in-1", agentinput.Text("hello")); err != nil {
		t.Fatal(err)
	}
	if err := h.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if cache.count() == 0 {
		t.Fatal("no projection was cached at all, so the check below proves nothing")
	}
	machine := extension.ProjectionID("twilight/run/machine")
	if _, _, ok, err := cache.Load(ctx, sid, machine, 1); err != nil || ok {
		t.Errorf("machine projection entry: ok=%v err=%v, want absent", ok, err)
	}
}
