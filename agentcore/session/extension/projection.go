package extension

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/session"
)

// ProjectionDefinition is a pure fold over decoded events (EXT-PRJ-1).
type ProjectionDefinition struct {
	ID         ProjectionID
	Version    ProjectionVersion
	Consumes   []session.EventType
	Initial    func() (any, error)
	Apply      func(any, DecodedEvent) (any, error)
	StateCodec PayloadCodec
	// Inherits decides, per logical stream, what the fold takes from the
	// commits a fork inherits (EXT-PRJ-8). nil follows the lineage each
	// stream's domain declared: inherited batches of LineageSession domains
	// are folded and those of LineageSegment domains are skipped, so a
	// child never interprets its parent's execution history as its own. A
	// projection whose content lives in another module's segment-lineage
	// facts declares InheritAll, or names the domains it takes with
	// InheritStreams.
	Inherits InheritPolicy
	// Authoritative marks a projection commands plan against on the Writer's
	// View and whose fold guards its stream's invariants (the run machine,
	// the turn and chatlog surfaces): a provisional commit it cannot fold is
	// refused. Every other projection is a derived read model: a fold
	// failure marks it unhealthy for this Writer's lifetime and never blocks
	// the facts (EXT-PRJ-9). Write-time invariants belong to the Parts of a
	// unit of work, not to projections; an authoritative fold failing is a
	// defect, not a business rejection.
	Authoritative bool
}

// InheritPolicy decides whether a projection folds the batches of one
// logical stream from a fork's inherited prefix.
type InheritPolicy func(session.StreamRef) bool

// InheritAll folds every batch of inherited commits.
func InheritAll(session.StreamRef) bool { return true }

// InheritStreams folds the listed stream domains of inherited commits.
func InheritStreams(domains ...string) InheritPolicy {
	return func(stream session.StreamRef) bool {
		for _, d := range domains {
			if stream.Domain == d {
				return true
			}
		}
		return false
	}
}

// inherits applies the definition's policy; nil follows the lineage the
// stream's domain declared, and a domain no module declared is not
// inherited.
func (r *Registry) inherits(d *ProjectionDefinition, stream session.StreamRef) bool {
	if d.Inherits != nil {
		return d.Inherits(stream)
	}
	_, def, ok := r.LookupStream(stream.Domain)
	return ok && def.Lineage == session.LineageSession
}

// ProjectionScope is a definition bound to its module scope: the modules
// whose unknown events the fold must not silently skip.
type ProjectionScope struct {
	Def      ProjectionDefinition
	consumes map[session.EventType]struct{}
	modules  map[ModuleKey]struct{}
}

// ScopeFor resolves a projection definition together with the module scope its
// fold must honour (EXT-PRJ-2). It is the entry point of the fold engine: both
// the Writer and a ProjectionReader start from a Scope.
func (r *Registry) ScopeFor(id ProjectionID, v ProjectionVersion) (*ProjectionScope, error) {
	def, module, ok := r.LookupProjection(id, v)
	if !ok {
		return nil, &Error{Code: ErrInvalid, Detail: fmt.Sprintf("unknown projection %q v%d", id, v)}
	}
	s := &ProjectionScope{Def: def, consumes: make(map[session.EventType]struct{}, len(def.Consumes)), modules: r.scopeOf(module)}
	for _, t := range def.Consumes {
		s.consumes[t] = struct{}{}
	}
	return s, nil
}

// Fold applies whole commits to state, event by event in CommitSeq order
// (EXT-PRJ-1/2). commits must be contiguous from the ledger and complete: the
// kernel never exposes a torn commit (SES-APP-2). A fold reads every stream:
// the commits carry their own stream attribution, and a projection whose
// Consumes spans modules sees their events wherever the commits placed them.
// It is pure with respect to the Registry: the same Scope and commits always
// fold the same. The Store assigns only Seq inside Append, so nothing a
// projection can read differs between the Writer's fold of the proposal and
// a reader's fold of the stored commit.
func (r *Registry) Fold(s *ProjectionScope, state any, commits []session.Commit) (any, error) {
	return r.FoldFrom(s, state, commits, session.SegmentHeader{})
}

// FoldFrom folds commits under the inheritance policy of the projection
// (EXT-PRJ-8): header is the Session's tip header, whose Parent edge marks
// the inherited prefix; commits at or below Parent.Seq contribute only the
// batches the policy admits, by default those of LineageSession domains. A
// header without a Parent (a root Session, or a caller folding tip commits
// only) inherits nothing and folds everything.
func (r *Registry) FoldFrom(s *ProjectionScope, state any, commits []session.Commit, header session.SegmentHeader) (any, error) {
	for i := range commits {
		inherited := header.Parent != nil && commits[i].Seq <= header.Parent.Seq
		// index numbers every event of the commit in batch order, skipped
		// batches included, so an event's Position does not depend on the
		// projection folding it.
		var index uint32
		for j := range commits[i].Batches {
			b := &commits[i].Batches[j]
			if inherited && !r.inherits(&s.Def, b.Stream) {
				index += session.Limit32(uint64(len(b.Events)))
				continue
			}
			for _, e := range b.Events {
				var err error
				pos := session.Position{Commit: commits[i].Seq, Index: index}
				index++
				state, err = r.applyEvent(s, state, pos, b.Stream, e)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return state, nil
}

func (r *Registry) applyEvent(s *ProjectionScope, state any, pos session.Position, stream session.StreamRef, e session.Event) (any, error) {
	seq := pos.Commit
	entry, registered := r.events[e.Type]
	if _, want := s.consumes[e.Type]; !want {
		if registered {
			return state, nil // known type of some module, not consumed here
		}
		module, known := r.ModuleOf(e.Type)
		if _, inScope := s.modules[module]; known && inScope {
			// The registry is the only authority on Ignorable, and an
			// unregistered type has no entry to consult: an in-scope event
			// the registry does not know is an error (EXT-PRJ-2).
			return nil, &Error{Code: ErrUnknownEvent, Type: e.Type, Detail: fmt.Sprintf("projection %q: unregistered event of module %s/%s at commit %d", s.Def.ID, module.Source, module.ID, seq)}
		}
		return state, nil
	}
	decoded, err := r.Decode(e)
	if err != nil {
		return nil, err
	}
	decoded.Stream, decoded.Position = stream, pos
	if decoded.Unknown {
		if entry.def.Ignorable {
			return state, nil
		}
		return nil, &Error{Code: ErrUnknownEvent, Type: e.Type, Detail: fmt.Sprintf("projection %q cannot decode v%d at commit %d", s.Def.ID, decoded.Version, seq)}
	}
	next, err := s.Def.Apply(state, decoded)
	if err != nil {
		return nil, fmt.Errorf("projection %s: commit %d: %w", s.Def.ID, seq, err)
	}
	return next, nil
}

// ProjectionReader loads a projection state together with the stream head it
// covers.
type ProjectionReader interface {
	Load(ctx context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion) (state any, through session.Head, err error)
}

// ProjectionCache is the optional derived cache of EXT-PRJ-3. Entries may be
// lost or stale at any time; readers verify Through against the stream.
type ProjectionCache interface {
	Load(ctx context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion) (state jsonstable.Value, through session.Head, ok bool, err error)
	Save(ctx context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion, state jsonstable.Value, through session.Head) error
}

// ProjectionCacheProvider is implemented by a Store adapter that can back its
// projection cache durably, so assembly code can pick it without
// knowing the adapter. A Store that does not implement it gets an in-memory
// cache or none.
type ProjectionCacheProvider interface {
	ProjectionCache() ProjectionCache
}

// DefaultCacheEvery is the commit gap a projection's cached state may fall
// behind the head when the deployment chooses no other policy. It bounds
// the work a reopening Writer repeats: at most this many commits are
// refolded, after a clean Close as after an abrupt end, since Close obeys
// the same interval (a deployment wanting none after Close wraps the policy
// in AtClose). It also bounds the write side: a projection's whole state is
// saved at most once per this many commits.
const DefaultCacheEvery session.CommitSeq = 64

// CachePolicy decides whether the Writer refreshes one projection's entry in
// the ProjectionCache. The Writer asks it after every applied commit, and once
// more with closing set when it is closed, so a policy can treat the last
// question differently from a routine one.
//
// cached is the head the projection's cache entry already reflects, or the zero
// Head when the cache holds no entry for it; head is where the fold itself now
// stands, so the pair is "how far the stream has come" against "how far the
// cached copy reaches". A policy only governs *writing*: a Writer always uses
// whatever entry it finds, whoever wrote it, because a stale or hostile entry is
// rejected when it is validated against the stream.
type CachePolicy func(id ProjectionID, v ProjectionVersion, head, cached session.Head, closing bool) bool

// CacheEvery refreshes a projection once the head has moved n commits past
// the entry the cache already covers, at Close as at any other time: the
// entry may lag the head by up to n commits, and a Writer reopening folds
// at most that many. Writing a large projection's whole state on every
// Close would cost the state's size per Turn (EXT-PRJ-7); a deployment
// that wants a clean Close to leave nothing to fold wraps the policy in
// AtClose. n <= 0 means DefaultCacheEvery.
func CacheEvery(n session.CommitSeq) CachePolicy {
	if n <= 0 {
		n = DefaultCacheEvery
	}
	return func(_ ProjectionID, _ ProjectionVersion, head, cached session.Head, _ bool) bool {
		return head.Next >= cached.Next+n
	}
}

// AtClose refreshes every entry that lags the head when the Writer closes,
// and defers to p otherwise.
func (p CachePolicy) AtClose() CachePolicy {
	return func(id ProjectionID, v ProjectionVersion, head, cached session.Head, closing bool) bool {
		if closing {
			return head.Next > cached.Next
		}
		return p(id, v, head, cached, closing)
	}
}

// Exclude declines the named projections and defers to p for the rest. An
// assembly uses it for a projection whose owning component refreshes the cache
// itself at checkpoint points the Writer must not preempt.
func (p CachePolicy) Exclude(ids ...ProjectionID) CachePolicy {
	return func(id ProjectionID, v ProjectionVersion, head, cached session.Head, closing bool) bool {
		for _, excluded := range ids {
			if id == excluded {
				return false
			}
		}
		return p(id, v, head, cached, closing)
	}
}

// MemoryProjectionCache is the in-process ProjectionCache.
type MemoryProjectionCache struct {
	mu      sync.Mutex
	entries map[cacheKey]cacheEntry
}

type cacheKey struct {
	sid session.SessionID
	id  ProjectionID
	v   ProjectionVersion
}
type cacheEntry struct {
	state   jsonstable.Value
	through session.Head
}

func NewMemoryProjectionCache() *MemoryProjectionCache {
	return &MemoryProjectionCache{entries: make(map[cacheKey]cacheEntry)}
}

func (c *MemoryProjectionCache) Load(_ context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion) (jsonstable.Value, session.Head, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[cacheKey{sid, id, v}]
	return e.state, e.through, ok, nil
}

// Save keeps the entry monotonic: a write that reaches the cache after a
// later one (Save runs outside the Writer's critical section, EXT-PRJ-7)
// never moves the entry back.
func (c *MemoryProjectionCache) Save(_ context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion, state jsonstable.Value, through session.Head) error {
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
func (c *MemoryProjectionCache) Delete(sid session.SessionID, id ProjectionID, v ProjectionVersion) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, cacheKey{sid, id, v})
}

// SaveProjection encodes state with the projection's StateCodec and stores it
// in cache covering through.
func SaveProjection(ctx context.Context, cache ProjectionCache, registry *Registry, sid session.SessionID, id ProjectionID, v ProjectionVersion, state any, through session.Head) error {
	if cache == nil {
		return nil
	}
	def, _, ok := registry.LookupProjection(id, v)
	if !ok {
		return &Error{Code: ErrInvalid, Detail: fmt.Sprintf("unknown projection %q v%d", id, v)}
	}
	encoded, err := def.StateCodec.Encode(state)
	if err != nil {
		return err
	}
	return cache.Save(ctx, sid, id, v, encoded, through)
}

type storeReader struct {
	store    session.Store
	registry *Registry
	cache    ProjectionCache
}

// NewProjectionReader reads projections from the Store: a cache entry (when
// it is a prefix of the stream) plus the tail commits, or a full fold. It is
// the public read model, in the owner process and in observers alike: it
// takes no ownership. Writer.Projections() is the owner's transactional
// view inside a commit's critical section.
func NewProjectionReader(store session.Store, registry *Registry, cache ProjectionCache) ProjectionReader {
	return &storeReader{store: store, registry: registry, cache: cache}
}

func (r *storeReader) Load(ctx context.Context, sid session.SessionID, id ProjectionID, v ProjectionVersion) (any, session.Head, error) {
	scope, err := r.registry.ScopeFor(id, v)
	if err != nil {
		return nil, session.Head{}, err
	}
	state, from, err := r.startState(ctx, sid, scope)
	if err != nil {
		return nil, session.Head{}, err
	}
	page, err := r.store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid, From: from.Next})
	if err != nil {
		return nil, session.Head{}, err
	}
	if from.Next > 0 && !OwnBoundary(page.Header, from) {
		// The tip moved between the two reads and the entry's
		// boundary is inherited by the new tip: it was folded under the old
		// tip's inheritance policy, so the whole log is folded under this
		// read's header instead.
		if state, err = scope.Def.Initial(); err != nil {
			return nil, session.Head{}, err
		}
		if page, err = r.store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid}); err != nil {
			return nil, session.Head{}, err
		}
	}
	state, err = r.registry.FoldFrom(scope, state, page.Commits, page.Header)
	if err != nil {
		return nil, session.Head{}, err
	}
	return state, page.Head, nil
}

// startState returns the cached state when its Through is a prefix of the
// stream; otherwise the projection's initial state and the empty head.
func (r *storeReader) startState(ctx context.Context, sid session.SessionID, scope *ProjectionScope) (any, session.Head, error) {
	if r.cache != nil {
		encoded, through, ok, err := r.cache.Load(ctx, sid, scope.Def.ID, scope.Def.Version)
		if err != nil {
			return nil, session.Head{}, err
		}
		if ok && through.Next > 0 && r.isPrefix(ctx, sid, through) {
			if state, err := scope.Def.StateCodec.Decode(encoded); err == nil {
				return state, through, nil
			}
		}
	}
	state, err := scope.Def.Initial()
	return state, session.Head{}, err
}

// isPrefix checks that the commit at through.Next-1 is the commit the entry
// recorded and one the tip segment wrote itself.
func (r *storeReader) isPrefix(ctx context.Context, sid session.SessionID, through session.Head) bool {
	page, err := r.store.ReadCommits(ctx, session.CommitReadRequest{SessionID: sid, From: through.Next - 1, Limit: 1})
	if err != nil || len(page.Commits) != 1 {
		return false
	}
	return OwnBoundary(page.Header, through) && CommitAt(page.Commits[0], through)
}

// CommitAt reports whether c is the commit through records: the commit at
// through.Next-1. History is append-only, so the position names the commit;
// it is the one head-alignment predicate of EXT-PRJ-3, shared by the Writer
// and the Store reader so a cache entry is judged the same way on both
// paths. The commit is the atomic unit of the ledger: there is no finer
// boundary to check.
func CommitAt(c session.Commit, through session.Head) bool {
	return through.Next > 0 && c.Seq == through.Next-1
}

// OwnBoundary reports whether through is a commit boundary of the tip
// segment itself: the commit before through.Next is one the tip wrote, not
// one it inherits. A projection state was folded under the inheritance
// policy of the tip that was current when it was recorded (EXT-PRJ-8); a
// fork makes every earlier commit inherited, so
// an entry ending on an inherited boundary is not started from (EXT-PRJ-3)
// and the fold restarts from the initial state until the tip holds a commit
// of its own. It is the second head-alignment predicate of EXT-PRJ-3, shared
// by the Writer and the Store reader like CommitAt.
func OwnBoundary(header session.SegmentHeader, through session.Head) bool {
	return through.Next > session.LedgerSeed(header).Next
}

// JSONStateCodec is a StateCodec for projection states that marshal to JSON.
type JSONStateCodec[T any] struct{}

func (JSONStateCodec[T]) Validate(value any) error {
	if _, ok := value.(T); !ok {
		var zero T
		return fmt.Errorf("state is %T, want %T", value, zero)
	}
	return nil
}
func (c JSONStateCodec[T]) Encode(value any) (jsonstable.Value, error) {
	if err := c.Validate(value); err != nil {
		return jsonstable.Value{}, err
	}
	return jsonstable.FromValue(value)
}
func (JSONStateCodec[T]) Decode(wire jsonstable.Value) (any, error) {
	var v T
	if wire.IsZero() {
		return nil, errors.New("empty projection state")
	}
	if err := StrictDecode(wire, &v); err != nil {
		return nil, err
	}
	return v, nil
}
