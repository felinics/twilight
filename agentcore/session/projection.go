package session

import (
	"context"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
)

// ProjectionReader loads a projection state together with the stream head it
// covers. It is the public read model, in the owner process and in observers
// alike: it takes no ownership. The Writer keeps its own transactional
// projections for the commit critical section.
type ProjectionReader interface {
	Load(ctx context.Context, sid SessionID, id module.ProjectionID, v module.ProjectionVersion) (state any, through ledger.Head, err error)
}

// ProjectionCache is the optional derived cache of EXT-PRJ-3. Entries may be
// lost or stale at any time; readers verify Through against the stream.
//
// An Authoritative projection's entry is a checkpoint the Writer plans the
// next commit against (EXT-PRJ-10): its state bytes travel with the digest
// the Writer computed when it saved them, in the same Value (see
// module.SealCheckpoint), and a reader that finds the digest missing or
// wrong treats the entry as absent. A derived projection's entry carries the
// bare state; a wrong one costs a wrong read model until the next refold.
type ProjectionCache interface {
	Load(ctx context.Context, sid SessionID, id module.ProjectionID, v module.ProjectionVersion) (state jsonstable.Value, through ledger.Head, ok bool, err error)
	Save(ctx context.Context, sid SessionID, id module.ProjectionID, v module.ProjectionVersion, state jsonstable.Value, through ledger.Head) error
}

// ProjectionCacheProvider is implemented by a Store adapter that can back its
// projection cache durably, so assembly code can pick it without
// knowing the adapter. A Store that does not implement it gets an in-memory
// cache or none.
type ProjectionCacheProvider interface {
	ProjectionCache() ProjectionCache
}

// MemoryProjectionCache is the in-process ProjectionCache.
type MemoryProjectionCache struct {
	mu      sync.Mutex
	entries map[cacheKey]cacheEntry
}

type cacheKey struct {
	sid SessionID
	id  module.ProjectionID
	v   module.ProjectionVersion
}
type cacheEntry struct {
	state   jsonstable.Value
	through ledger.Head
}

func NewMemoryProjectionCache() *MemoryProjectionCache {
	return &MemoryProjectionCache{entries: make(map[cacheKey]cacheEntry)}
}

func (c *MemoryProjectionCache) Load(_ context.Context, sid SessionID, id module.ProjectionID, v module.ProjectionVersion) (jsonstable.Value, ledger.Head, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[cacheKey{sid, id, v}]
	return e.state, e.through, ok, nil
}

// Save keeps the entry monotonic: a write that reaches the cache after a
// later one (Save runs outside the Writer's critical section, EXT-PRJ-7)
// never moves the entry back.
func (c *MemoryProjectionCache) Save(_ context.Context, sid SessionID, id module.ProjectionID, v module.ProjectionVersion, state jsonstable.Value, through ledger.Head) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	k := cacheKey{sid, id, v}
	if e, ok := c.entries[k]; ok && e.through.Next >= through.Next {
		return nil
	}
	c.entries[k] = cacheEntry{state, through}
	return nil
}

// Delete drops one entry; tests use it to prove the cache is discardable.
func (c *MemoryProjectionCache) Delete(sid SessionID, id module.ProjectionID, v module.ProjectionVersion) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, cacheKey{sid, id, v})
}

// SaveProjection encodes state with the projection's StateCodec and stores it
// in cache covering through.
func SaveProjection(ctx context.Context, cache ProjectionCache, registry *module.Registry, sid SessionID, id module.ProjectionID, v module.ProjectionVersion, state any, through ledger.Head) error {
	if cache == nil {
		return nil
	}
	def, _, ok := registry.LookupProjection(id, v)
	if !ok {
		return ledger.NewInvalid("", fmt.Sprintf("unknown projection %q v%d", id, v))
	}
	encoded, err := def.StateCodec.Encode(state)
	if err != nil {
		return err
	}
	if def.Authoritative {
		if encoded, err = module.SealCheckpoint(id, v, through, encoded); err != nil {
			return err
		}
	}
	return cache.Save(ctx, sid, id, v, encoded, through)
}

// LoadProjectionEntry reads a cache entry for def, unsealing and verifying
// an authoritative one (EXT-PRJ-10); an entry that does not verify is a
// miss.
func LoadProjectionEntry(ctx context.Context, cache ProjectionCache, def *module.ProjectionDefinition, sid SessionID) (jsonstable.Value, ledger.Head, bool, error) {
	encoded, through, ok, err := cache.Load(ctx, sid, def.ID, def.Version)
	if err != nil || !ok {
		return jsonstable.Value{}, ledger.Head{}, false, err
	}
	if def.Authoritative {
		state, verified := module.OpenCheckpoint(def.ID, def.Version, through, encoded)
		if !verified {
			return jsonstable.Value{}, ledger.Head{}, false, nil
		}
		return state, through, true, nil
	}
	return encoded, through, true, nil
}

type storeReader struct {
	store    Store
	registry *module.Registry
	cache    ProjectionCache
}

// NewProjectionReader reads projections from the Store: a cache entry (when
// it is a prefix of the stream) plus the tail commits, or a full fold. It is
// the public read model, in the owner process and in observers alike: it
// takes no ownership. Writer.Projections() is the owner's transactional
// view inside a commit's critical section.
func NewProjectionReader(store Store, registry *module.Registry, cache ProjectionCache) ProjectionReader {
	return &storeReader{store: store, registry: registry, cache: cache}
}

func (r *storeReader) Load(ctx context.Context, sid SessionID, id module.ProjectionID, v module.ProjectionVersion) (any, ledger.Head, error) {
	scope, err := r.registry.ScopeFor(id, v)
	if err != nil {
		return nil, ledger.Head{}, err
	}
	state, from, err := r.startState(ctx, sid, scope)
	if err != nil {
		return nil, ledger.Head{}, err
	}
	page, err := r.store.ReadCommits(ctx, CommitReadRequest{SessionID: sid, From: from.Next})
	if err != nil {
		return nil, ledger.Head{}, err
	}
	if from.Next > 0 && !ledger.OwnBoundary(page.Header.Seed(), from) {
		// The tip moved between the two reads and the entry's
		// boundary is inherited by the new tip: it was folded under the old
		// tip's inheritance policy, so the whole log is folded under this
		// read's header instead.
		if state, err = scope.Def.Initial(); err != nil {
			return nil, ledger.Head{}, err
		}
		if page, err = r.store.ReadCommits(ctx, CommitReadRequest{SessionID: sid}); err != nil {
			return nil, ledger.Head{}, err
		}
	}
	state, err = r.registry.FoldFrom(scope, state, page.Commits, page.Header.Seed())
	if err != nil {
		return nil, ledger.Head{}, err
	}
	return state, page.Head, nil
}

// startState returns the cached state when its Through is a prefix of the
// stream; otherwise the projection's initial state and the empty head.
func (r *storeReader) startState(ctx context.Context, sid SessionID, scope *module.ProjectionScope) (any, ledger.Head, error) {
	if r.cache != nil {
		encoded, through, ok, err := LoadProjectionEntry(ctx, r.cache, &scope.Def, sid)
		if err != nil {
			return nil, ledger.Head{}, err
		}
		if ok && through.Next > 0 && r.isPrefix(ctx, sid, through) {
			if state, err := scope.Def.StateCodec.Decode(encoded); err == nil {
				return state, through, nil
			}
		}
	}
	state, err := scope.Def.Initial()
	return state, ledger.Head{}, err
}

// isPrefix checks that the commit at through.Next-1 is the commit the entry
// recorded and one the tip segment wrote itself.
func (r *storeReader) isPrefix(ctx context.Context, sid SessionID, through ledger.Head) bool {
	page, err := r.store.ReadCommits(ctx, CommitReadRequest{SessionID: sid, From: through.Next - 1, Limit: 1})
	if err != nil || len(page.Commits) != 1 {
		return false
	}
	return ledger.OwnBoundary(page.Header.Seed(), through) && ledger.CommitAt(page.Commits[0], through)
}
