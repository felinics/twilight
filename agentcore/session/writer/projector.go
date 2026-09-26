package writer

import (
	"context"
	"fmt"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// projectionKey names one projection version in the projector's maps.
type projectionKey struct {
	id      extension.ProjectionID
	version extension.ProjectionVersion
}

// projector is the projection stage of the commit pipeline: it holds the
// transactional state of every registered projection folded to the Writer's
// head (EXT-WRT-1), pre-folds each provisional commit so an invalid group is
// refused before anything is persisted, and refreshes the projection cache
// per the deployment's policy (EXT-PRJ-3, EXT-PRJ-7). It knows nothing of
// ownership, admission or observers.
type projector struct {
	registry *extension.Registry
	sid      session.SessionID
	states   map[projectionKey]any
	scopes   map[projectionKey]*extension.ProjectionScope
	// unhealthy records derived projections that failed to fold an applied
	// commit (EXT-PRJ-9): their state stays at the last good commit, reads
	// report the failure and the cache is not refreshed for them.
	unhealthy map[projectionKey]error
	// cache, policy and cached carry EXT-PRJ-3: cached records the head each
	// projection's cache entry already reflects, which is what a policy
	// measures the next refresh against.
	cache  extension.ProjectionCache
	policy extension.CachePolicy
	cached map[projectionKey]session.Head
	// starts is the CommitSeq each projection resumes folding from after
	// prepare: its cache entry's Next, or 0 for a full fold.
	starts map[projectionKey]session.CommitSeq
}

func newProjector(registry *extension.Registry, sid session.SessionID, cache extension.ProjectionCache, policy extension.CachePolicy) *projector {
	if policy == nil {
		policy = extension.CacheEvery(extension.DefaultCacheEvery)
	}
	return &projector{registry: registry, sid: sid, states: make(map[projectionKey]any), unhealthy: make(map[projectionKey]error),
		scopes: make(map[projectionKey]*extension.ProjectionScope), cache: cache, policy: policy, cached: make(map[projectionKey]session.Head),
		starts: make(map[projectionKey]session.CommitSeq)}
}

// commitAt reads the commit at one stitched position of the Session, for
// judging a cache entry's boundary (EXT-PRJ-3); ok=false when there is none.
type commitAt func(session.CommitSeq) (session.Commit, bool)

// prepare chooses each registered projection's starting state: its cache
// entry when the entry ends on a commit boundary the tip segment wrote
// itself and still decodes, the initial state otherwise (EXT-PRJ-3). It
// returns the earliest stitched CommitSeq any projection must fold from,
// which is how much of the log the Writer reads: a clean Close leaves every
// entry at the head and the read is empty (EXT-PRJ-5).
func (p *projector) prepare(ctx context.Context, header session.SegmentHeader, at commitAt) (session.CommitSeq, error) {
	from := ^session.CommitSeq(0)
	for _, def := range p.registry.Projections() {
		k := projectionKey{def.ID, def.Version}
		scope, err := p.registry.ScopeFor(def.ID, def.Version)
		if err != nil {
			return 0, err
		}
		p.scopes[k] = scope
		state, through, ok := p.startState(ctx, scope, header, at)
		start := session.CommitSeq(0)
		if ok {
			p.cached[k] = through
			start = through.Next
		} else if state, err = scope.Def.Initial(); err != nil {
			return 0, err
		}
		p.states[k] = state
		p.starts[k] = start
		if start < from {
			from = start
		}
	}
	return from, nil
}

// resume folds the commits read from from into every projection, each from
// its own start (EXT-PRJ-3, EXT-PRJ-9).
func (p *projector) resume(page *session.CommitPage, from session.CommitSeq) error {
	for k, scope := range p.scopes {
		start := p.starts[k]
		if start < from {
			start = from
		}
		off := session.IndexWithin(start-from, len(page.Commits))
		if off >= len(page.Commits) {
			continue
		}
		folded, err := p.registry.FoldFrom(scope, p.states[k], page.Commits[off:], page.Header)
		if err != nil {
			if scope.Def.Authoritative {
				return err
			}
			// A derived projection that cannot fold the log does not keep
			// the Session from opening (EXT-PRJ-9): it stops at its last
			// good commit and stays unhealthy until a registry that folds
			// it reopens the Session.
			folded, err = p.lastGood(scope, p.states[k], page.Commits[off:], page.Header)
			p.unhealthy[k] = err
		}
		p.states[k] = folded
	}
	return nil
}

// lastGood folds commits one at a time and returns the state before the
// first commit the projection cannot fold, with that failure.
func (p *projector) lastGood(scope *extension.ProjectionScope, state any, commits []session.Commit, header session.SegmentHeader) (any, error) {
	for i := range commits {
		next, err := p.registry.FoldFrom(scope, state, commits[i:i+1], header)
		if err != nil {
			return state, err
		}
		state = next
	}
	return state, nil
}

// startState returns the state this projection should begin folding from: the
// cached one when its entry covers a commit boundary the tip segment wrote
// itself and still decodes, otherwise nothing. Anything unusable -- absent,
// corrupt, ahead of the log, recorded at a digest the log does not have, or
// ending on an inherited commit -- falls back to a full fold, so a stale or
// damaged cache only costs time (EXT-PRJ-3). It is the Writer's counterpart
// of the store reader's startState. A fork, and a Session whose tip just
// advanced, fold their inherited prefix on first open and cache the result
// once they hold a commit of their own.
func (p *projector) startState(ctx context.Context, scope *extension.ProjectionScope, header session.SegmentHeader, at commitAt) (any, session.Head, bool) {
	if p.cache == nil {
		return nil, session.Head{}, false
	}
	encoded, through, ok, err := p.cache.Load(ctx, p.sid, scope.Def.ID, scope.Def.Version)
	if err != nil || !ok || !coversCommit(header, through, at) {
		return nil, session.Head{}, false
	}
	state, err := scope.Def.StateCodec.Decode(encoded)
	if err != nil {
		return nil, session.Head{}, false
	}
	return state, through, true
}

// coversCommit reports whether through names the commit before its Next -- a
// commit boundary of this log that the tip header describes wrote itself --
// with the digest the entry recorded. It reads that one commit.
func coversCommit(header session.SegmentHeader, through session.Head, at commitAt) bool {
	if through.Next == 0 || !extension.OwnBoundary(header, through) {
		return false
	}
	c, ok := at(through.Next - 1)
	return ok && extension.CommitAt(c, through)
}

// folded is what fold produced: the next states and, for derived
// projections that could not fold the commit, the failure to record once
// the commit is durable.
type folded struct {
	next   map[projectionKey]any
	failed map[projectionKey]error
}

// fold pre-folds one provisional commit into every projection without
// installing anything. An authoritative projection that refuses the commit
// refuses it for the Writer; a derived one keeps its last good state and is
// marked unhealthy once the commit lands (EXT-PRJ-9). A projection already
// unhealthy is not folded further.
func (p *projector) fold(provisional session.Commit) (folded, error) {
	out := folded{next: make(map[projectionKey]any, len(p.states))}
	for k, scope := range p.scopes {
		if _, down := p.unhealthy[k]; down {
			continue
		}
		state, err := p.registry.Fold(scope, p.states[k], []session.Commit{provisional})
		if err != nil {
			if scope.Def.Authoritative {
				return folded{}, err
			}
			if out.failed == nil {
				out.failed = make(map[projectionKey]error)
			}
			out.failed[k] = err
			continue
		}
		out.next[k] = state
	}
	return out, nil
}

// advance installs what fold produced for a commit that is now durable.
func (p *projector) advance(f folded) {
	for k, s := range f.next {
		p.states[k] = s
	}
	for k, err := range f.failed {
		p.unhealthy[k] = err
	}
}

// detached returns a copy of one projection state that the caller owns.
func (p *projector) detached(id extension.ProjectionID, ver extension.ProjectionVersion) (any, error) {
	k := projectionKey{id, ver}
	state, ok := p.states[k]
	if !ok {
		return nil, &extension.Error{Code: extension.ErrInvalid, Detail: fmt.Sprintf("unknown projection %q v%d", id, ver)}
	}
	if err, down := p.unhealthy[k]; down {
		return nil, &extension.Error{Code: extension.ErrProjectionUnhealthy, Detail: fmt.Sprintf("projection %q v%d: %v", id, ver, err)}
	}
	codec := p.scopes[k].Def.StateCodec
	encoded, err := codec.Encode(state)
	if err != nil {
		return nil, err
	}
	return codec.Decode(encoded)
}

// cacheWrite is one projection entry the policy asked to refresh: the state
// and the head it covers, captured under the lock and written outside it.
type cacheWrite struct {
	key   projectionKey
	state any
	head  session.Head
}

// planRefresh selects the entries the deployment's policy wants refreshed and
// records them as covering head. The caller holds the Writer's lock. The
// writes themselves happen outside it (saveRefresh): a cache entry is derived
// data with no ordering constraint against later commits (EXT-PRJ-7), and the
// captured states are immutable (EXT-PRJ-1), so nothing in the critical
// section depends on the IO.
func (p *projector) planRefresh(head session.Head, closing bool) []cacheWrite {
	if p.cache == nil {
		return nil
	}
	var writes []cacheWrite
	for k := range p.scopes {
		if _, down := p.unhealthy[k]; down {
			continue // its state is behind head; a cache entry would lie
		}
		if !p.policy(k.id, k.version, head, p.cached[k], closing) {
			continue
		}
		writes = append(writes, cacheWrite{key: k, state: p.states[k], head: head})
		p.cached[k] = head
	}
	return writes
}

// saveRefresh performs planned writes. Best effort and never fatal: the cache
// is derived data, so a failed Save only means a later Writer folds more
// (EXT-PRJ-3); the policy then asks again at its next threshold.
func (p *projector) saveRefresh(ctx context.Context, writes []cacheWrite) {
	for _, cw := range writes {
		_ = extension.SaveProjection(ctx, p.cache, p.registry, p.sid, cw.key.id, cw.key.version, cw.state, cw.head)
	}
}

// memoryReader reads the Writer's transactional projections (EXT-PRJ-4).
type memoryReader struct{ w *sessionWriter }

func (r memoryReader) Load(_ context.Context, sid session.SessionID, id extension.ProjectionID, v extension.ProjectionVersion) (any, session.Head, error) {
	if sid != r.w.sid {
		return nil, session.Head{}, &extension.Error{Code: extension.ErrInvalid, Detail: "writer projections are session-local"}
	}
	r.w.mu.Lock()
	defer r.w.mu.Unlock()
	state, err := r.w.projections.detached(id, v)
	return state, r.w.head, err
}
