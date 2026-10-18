package module

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/ledger"

	"github.com/felinics/twilight/agentcore/jsonstable"
)

// ProjectionDefinition is a pure fold over decoded events.
type ProjectionDefinition struct {
	ID         ProjectionID
	Version    ProjectionVersion
	Consumes   []ledger.EventType
	Initial    func() (any, error)
	Apply      func(any, DecodedEvent) (any, error)
	StateCodec PayloadCodec
	// InheritanceOverride decides which batches from an inherited prefix this
	// projection folds. A nil override follows the owning stream domain's
	// Inheritance declaration. InheritAll and InheritNone override it for every
	// domain.
	InheritanceOverride InheritancePolicy
	// RequiredForCommit marks a projection whose fold must accept a
	// provisional commit before anything is persisted. Replay failures also
	// prevent the Writer from opening. Only trusted core modules may set it.
	// Every other projection is a derived read model: a fold failure marks it
	// Writer's lifetime and never blocks the facts. Write-time invariants
	// belong to the Writer's decision, not to projections; a required fold
	// failing is a defect, not a business rejection.
	RequiredForCommit bool
}

// InheritancePolicy selects how a projection reads an inherited prefix for a
// particular stream domain. A nil policy means the domain's declaration is
// used instead.
type InheritancePolicy func(ledger.Domain) bool

// InheritAll folds every batch of inherited commits.
func InheritAll(ledger.Domain) bool { return true }

// InheritNone skips every batch of inherited commits.
func InheritNone(ledger.Domain) bool { return false }

// inherits applies the projection policy. A missing policy follows the
// inheritance declared by the stream's owning module.
func (r *Registry) inherits(d *ProjectionDefinition, stream ledger.Domain) bool {
	if d.InheritanceOverride != nil {
		return d.InheritanceOverride(stream)
	}
	_, def, ok := r.LookupStream(stream.Name)
	return ok && def.Inheritance == Inherited
}

// ProjectionScope is a definition bound to its module scope: the modules
// whose unknown events the fold must not silently skip.
type ProjectionScope struct {
	Def      ProjectionDefinition
	consumes map[ledger.EventType]struct{}
	modules  map[ModuleKey]struct{}
}

// ScopeFor resolves a projection definition together with the module scope
// its fold must honour. It is the entry point of the fold
// engine: every fold starts from a Scope.
func (r *Registry) ScopeFor(id ProjectionID, v ProjectionVersion) (*ProjectionScope, error) {
	def, module, ok := r.LookupProjection(id, v)
	if !ok {
		return nil, &ledger.Error{Code: ledger.CodeInvalid, Detail: fmt.Sprintf("unknown projection %q v%d", id, v)}
	}
	s := &ProjectionScope{Def: def, consumes: make(map[ledger.EventType]struct{}, len(def.Consumes)), modules: r.scopeOf(module)}
	for _, t := range def.Consumes {
		s.consumes[t] = struct{}{}
	}
	return s, nil
}

// Fold applies whole commits to state, event by event in CommitSeq order
// commits must be contiguous from the ledger and complete:
// the store never exposes a torn commit. A fold reads every stream: the
// commits carry their own stream attribution, and a projection whose
// Consumes spans modules sees their events wherever the commits placed
// them. It is pure with respect to the Registry: the same Scope and commits
// always fold the same.
func (r *Registry) Fold(s *ProjectionScope, state any, commits []ledger.Commit) (any, error) {
	return r.FoldFrom(s, state, commits, ledger.Head{})
}

// FoldFrom folds commits under the inheritance policy of the projection
// header is the tip segment's header, whose Parent edge marks
// the inherited prefix; commits at or below Parent.Seq contribute only the
// batches the policy admits, by default those of Inherited domains. A
// header without a Parent (a root, or a caller folding tip commits only)
// inherits nothing and folds everything.
func (r *Registry) FoldFrom(s *ProjectionScope, state any, commits []ledger.Commit, seed ledger.Head) (any, error) {
	for i := range commits {
		inherited := commits[i].Seq < seed.Next
		// index numbers every event of the commit in batch order, skipped
		// batches included, so an event's Position does not depend on the
		// projection folding it.
		var index uint32
		for j := range commits[i].Batches {
			b := &commits[i].Batches[j]
			if inherited && !r.inherits(&s.Def, b.Domain) {
				index += ledger.Limit32(uint64(len(b.Events)))
				continue
			}
			for _, e := range b.Events {
				var err error
				pos := ledger.Position{Commit: commits[i].Seq, Index: index}
				index++
				state, err = r.applyEvent(s, state, pos, b.Domain, e)
				if err != nil {
					return nil, err
				}
			}
		}
	}
	return state, nil
}

func (r *Registry) applyEvent(s *ProjectionScope, state any, pos ledger.Position, stream ledger.Domain, e ledger.Event) (any, error) {
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
			// the registry does not know is an error.
			return nil, &ledger.Error{Code: ledger.CodeUnknownEvent, Type: e.Type, Detail: fmt.Sprintf("projection %q: unregistered event of module %s/%s at commit %d", s.Def.ID, module.Source, module.ID, seq)}
		}
		return state, nil
	}
	decoded, err := r.Decode(e)
	if err != nil {
		return nil, err
	}
	decoded.Domain, decoded.Position = stream, pos
	if decoded.Unknown {
		if entry.def.Ignorable {
			return state, nil
		}
		return nil, &ledger.Error{Code: ledger.CodeUnknownEvent, Type: e.Type, Detail: fmt.Sprintf("projection %q cannot decode v%d at commit %d", s.Def.ID, decoded.Version, seq)}
	}
	next, err := s.Def.Apply(state, decoded)
	if err != nil {
		return nil, fmt.Errorf("projection %s: commit %d: %w", s.Def.ID, seq, err)
	}
	return next, nil
}

// checkpointDomain namespaces the digest of a required-for-commit checkpoint.
const checkpointDomain = "twilight/projection-checkpoint"

// sealedCheckpoint is the cache Value of a required-for-commit projection: the
// encoded state and a digest over (projection, version, through, state).
type sealedCheckpoint struct {
	State  json.RawMessage   `json:"state"`
	Digest jsonstable.Digest `json:"digest"`
}

// checkpointDigest is the digest a required-for-commit entry must carry.
func checkpointDigest(id ProjectionID, v ProjectionVersion, through ledger.Head, state jsonstable.Value) jsonstable.Digest {
	preimage, _ := jsonstable.EncodeTypedPayload(1, checkpointDomain, []string{string(id), fmt.Sprint(uint64(v)), fmt.Sprint(uint64(through.Next)), string(state.Bytes())})
	return jsonstable.DigestBytes(preimage)
}

// SealCheckpoint wraps a required-for-commit projection's encoded state with its
// digest for the cache.
func SealCheckpoint(id ProjectionID, v ProjectionVersion, through ledger.Head, state jsonstable.Value) (jsonstable.Value, error) {
	return jsonstable.FromValue(sealedCheckpoint{State: state.Bytes(), Digest: checkpointDigest(id, v, through, state)})
}

// OpenCheckpoint unwraps a sealed required-for-commit entry, verifying its digest;
// ok is false when the entry is not a checkpoint of this projection at
// through, and the caller treats it as absent.
func OpenCheckpoint(id ProjectionID, v ProjectionVersion, through ledger.Head, sealed jsonstable.Value) (jsonstable.Value, bool) {
	var c sealedCheckpoint
	if err := json.Unmarshal(sealed.Bytes(), &c); err != nil || len(c.State) == 0 || c.Digest == "" {
		return jsonstable.Value{}, false
	}
	state, err := jsonstable.Parse(c.State)
	if err != nil {
		return jsonstable.Value{}, false
	}
	if checkpointDigest(id, v, through, state) != c.Digest {
		return jsonstable.Value{}, false
	}
	return state, true
}

// DefaultCacheEvery is the commit gap a projection's cached state may fall
// behind the head when the deployment chooses no other policy. It bounds
// the work a reopening Writer repeats: at most this many commits are
// refolded, after a clean Close as after an abrupt end, since Close obeys
// the same interval (a deployment wanting none after Close wraps the policy
// in AtClose). It also bounds the write side: a projection's whole state is
// saved at most once per this many commits.
const DefaultCacheEvery ledger.CommitSeq = 64

// CachePolicy decides whether the Writer refreshes one projection's entry
// in the projection cache. The Writer asks it after every applied commit,
// and once more with closing set when it is closed, so a policy can treat
// the last question differently from a routine one.
// cached is the head the projection's cache entry already reflects, or the
// zero Head when the cache holds no entry for it; head is where the fold
// itself now stands, so the pair is "how far the stream has come" against
// "how far the cached copy reaches". A policy only governs *writing*: a
// Writer always uses whatever entry it finds, whoever wrote it, because a
// stale or hostile entry is rejected when it is validated against the
// stream.
type CachePolicy func(id ProjectionID, v ProjectionVersion, head, cached ledger.Head, closing bool) bool

// CacheEvery refreshes a projection once the head has moved n commits past
// the entry the cache already covers, at Close as at any other time: the
// entry may lag the head by up to n commits, and a Writer reopening folds
// at most that many. Writing a large projection's whole state on every
// Close would cost the state's size per Turn; a deployment
// that wants a clean Close to leave nothing to fold wraps the policy in
// AtClose. n <= 0 means DefaultCacheEvery.
func CacheEvery(n ledger.CommitSeq) CachePolicy {
	if n <= 0 {
		n = DefaultCacheEvery
	}
	return func(_ ProjectionID, _ ProjectionVersion, head, cached ledger.Head, _ bool) bool {
		return head.Next >= cached.Next+n
	}
}

// AtClose refreshes every entry that lags the head when the Writer closes,
// and defers to p otherwise.
func (p CachePolicy) AtClose() CachePolicy {
	return func(id ProjectionID, v ProjectionVersion, head, cached ledger.Head, closing bool) bool {
		if closing {
			return head.Next > cached.Next
		}
		return p(id, v, head, cached, closing)
	}
}

// Exclude declines the named projections and defers to p for the rest. An
// assembly uses it for a projection whose owning component refreshes the
// cache itself at checkpoint points the Writer must not preempt.
func (p CachePolicy) Exclude(ids ...ProjectionID) CachePolicy {
	return func(id ProjectionID, v ProjectionVersion, head, cached ledger.Head, closing bool) bool {
		for _, excluded := range ids {
			if id == excluded {
				return false
			}
		}
		return p(id, v, head, cached, closing)
	}
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
