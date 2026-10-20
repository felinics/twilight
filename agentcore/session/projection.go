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

// ProjectionCache is the optional derived cache. Entries may be
// lost or stale at any time; readers verify Through against the stream.
// A required-for-commit projection's entry is a checkpoint the Writer plans the
// next commit: its state bytes travel with the digest
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
// later one (Save runs outside the Writer's critical section)
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
		return &ledger.Error{Code: ledger.CodeInvalid, Detail: fmt.Sprintf("unknown projection %q v%d", id, v)}
	}
	encoded, err := def.StateCodec.Encode(state)
	if err != nil {
		return err
	}
	if def.RequiredForCommit {
		if encoded, err = module.SealCheckpoint(id, v, through, encoded); err != nil {
			return err
		}
	}
	return cache.Save(ctx, sid, id, v, encoded, through)
}

// LoadProjectionEntry reads a cache entry for def, unsealing and verifying
// a required-for-commit one; an entry that does not verify is a
// miss.
func LoadProjectionEntry(ctx context.Context, cache ProjectionCache, def *module.ProjectionDefinition, sid SessionID) (jsonstable.Value, ledger.Head, bool, error) {
	encoded, through, ok, err := cache.Load(ctx, sid, def.ID, def.Version)
	if err != nil || !ok {
		return jsonstable.Value{}, ledger.Head{}, false, err
	}
	if def.RequiredForCommit {
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
// takes no ownership. Writer.Projections is the owner's transactional
// view inside a commit's critical section.
func NewProjectionReader(store Store, registry *module.Registry, cache ProjectionCache) ProjectionReader {
	return &storeReader{store: store, registry: registry, cache: cache}
}

// ProjectionRef identifies one projection version in a grouped read.
type ProjectionRef struct {
	ID      module.ProjectionID
	Version module.ProjectionVersion
}

// ProjectionState carries a detached projection and its observed ledger head.
type ProjectionState struct {
	State   any
	Through ledger.Head
}

// LoadProjections shares a log read when the reader supports grouped reads.
// Other readers retain their ordinary Load semantics, including distinct heads.
func LoadProjections(ctx context.Context, reader ProjectionReader, sid SessionID, refs ...ProjectionRef) ([]ProjectionState, error) {
	if grouped, ok := reader.(interface {
		LoadMany(context.Context, SessionID, []ProjectionRef) ([]ProjectionState, error)
	}); ok {
		return grouped.LoadMany(ctx, sid, refs)
	}
	out := make([]ProjectionState, len(refs))
	for i, ref := range refs {
		state, through, err := reader.Load(ctx, sid, ref.ID, ref.Version)
		if err != nil {
			return nil, err
		}
		out[i] = ProjectionState{State: state, Through: through}
	}
	return out, nil
}

func (r *storeReader) Load(ctx context.Context, sid SessionID, id module.ProjectionID, v module.ProjectionVersion) (any, ledger.Head, error) {
	states, err := r.LoadMany(ctx, sid, []ProjectionRef{{ID: id, Version: v}})
	if err != nil {
		return nil, ledger.Head{}, err
	}
	return states[0].State, states[0].Through, nil
}

// LoadMany decodes each checkpoint independently, then reads the earliest
// boundary and shared tail once. The boundary is included in that same read:
// validation and replay therefore use the same tip and observed head. Nothing
// is retained between calls or shared with a Writer's transactional state.
func (r *storeReader) LoadMany(ctx context.Context, sid SessionID, refs []ProjectionRef) ([]ProjectionState, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	type start struct {
		scope   *module.ProjectionScope
		state   any
		through ledger.Head
		cached  bool
	}
	starts := make([]start, len(refs))
	from := ^ledger.CommitSeq(0)
	for i, ref := range refs {
		scope, err := r.registry.ScopeFor(ref.ID, ref.Version)
		if err != nil {
			return nil, err
		}
		s := &starts[i]
		s.scope = scope
		if r.cache != nil {
			encoded, through, ok, err := LoadProjectionEntry(ctx, r.cache, &scope.Def, sid)
			if err != nil {
				return nil, err
			}
			if ok && through.Next > 0 {
				if state, err := scope.Def.StateCodec.Decode(encoded); err == nil {
					s.state, s.through, s.cached = state, through, true
				}
			}
		}
		next := ledger.CommitSeq(0)
		if s.cached {
			next = s.through.Next - 1
		}
		from = min(from, next)
	}
	page, err := r.store.ReadCommits(ctx, CommitReadRequest{SessionID: sid, From: from})
	if err != nil {
		return nil, err
	}
	for i := range starts {
		s := &starts[i]
		if !s.cached {
			continue
		}
		boundary := s.through.Next - 1
		offset := IndexWithin(boundary-from, len(page.Commits))
		if !ledger.OwnBoundary(page.Header.Seed(), s.through) || offset >= len(page.Commits) || !ledger.CommitAt(page.Commits[offset], s.through) {
			s.cached = false
		}
	}
	// A stale, inherited or ahead-of-log checkpoint needs the full history.
	// Validate all other checkpoints again under the replacement page's tip.
	if from > 0 {
		for i := range starts {
			if !starts[i].cached {
				page, err = r.store.ReadCommits(ctx, CommitReadRequest{SessionID: sid})
				if err != nil {
					return nil, err
				}
				from = 0
				break
			}
		}
	}
	out := make([]ProjectionState, len(refs))
	for i := range starts {
		s := &starts[i]
		next := ledger.CommitSeq(0)
		if s.cached {
			boundary := s.through.Next - 1
			offset := IndexWithin(boundary-from, len(page.Commits))
			s.cached = ledger.OwnBoundary(page.Header.Seed(), s.through) && offset < len(page.Commits) && ledger.CommitAt(page.Commits[offset], s.through)
		}
		if s.cached {
			next = s.through.Next
		} else {
			s.state, err = s.scope.Def.Initial()
			if err != nil {
				return nil, err
			}
		}
		offset := IndexWithin(next-from, len(page.Commits))
		state, err := r.registry.FoldFrom(s.scope, s.state, page.Commits[offset:], page.Header.Seed())
		if err != nil {
			return nil, err
		}
		out[i] = ProjectionState{State: state, Through: page.Head}
	}
	return out, nil
}
