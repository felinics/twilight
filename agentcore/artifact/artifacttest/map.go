package artifacttest

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/felinics/twilight/agentcore/artifact"
)

// MapBindings is artifact.BindingStore over a Go map (ART-BND-1): the
// reference the suite is written against and the store the kernel's own
// tests bind through. The zero value is ready.
type MapBindings struct {
	mu       sync.Mutex
	bindings map[artifact.BindingID]artifact.Binding
}

var _ artifact.BindingStore = (*MapBindings)(nil)

func (s *MapBindings) CreateBinding(_ context.Context, b artifact.Binding) (artifact.Binding, error) { //nolint:gocritic // hugeParam: BindingStore contract takes the Binding by value
	want, err := artifact.DigestBinding(b.ID, b.Ref)
	if err != nil {
		return artifact.Binding{}, err
	}
	if b.Digest != want {
		return artifact.Binding{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: "create_binding", Identity: string(b.ID), Detail: "binding digest mismatch"}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.bindings[b.ID]; ok {
		if existing.Digest != b.Digest {
			return artifact.Binding{}, &artifact.Error{Code: artifact.ErrConflict, Operation: "create_binding", Identity: string(b.ID)}
		}
		return existing, nil
	}
	if s.bindings == nil {
		s.bindings = make(map[artifact.BindingID]artifact.Binding)
	}
	s.bindings[b.ID] = b
	return b, nil
}

func (s *MapBindings) LookupBinding(_ context.Context, id artifact.BindingID) (artifact.Binding, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bindings[id]
	return b, ok, nil
}

func (s *MapBindings) ResolveBinding(ctx context.Context, id artifact.BindingID) (artifact.Binding, error) {
	b, ok, err := s.LookupBinding(ctx, id)
	if err != nil {
		return artifact.Binding{}, err
	}
	if !ok {
		return artifact.Binding{}, &artifact.Error{Code: artifact.ErrNotFound, Operation: "resolve_binding", Identity: string(id)}
	}
	return b, nil
}

// MapLedger is artifact.RetentionLedger over a Go map (ART-RET-2,
// ART-RET-3). Builder, when set, verifies every activated set against the
// bindings it was built from.
type MapLedger struct {
	Builder artifact.BindingSetBuilder

	mu     sync.Mutex
	claims map[artifact.ClaimID]artifact.RetentionClaim
}

var _ artifact.RetentionLedger = (*MapLedger)(nil)

const opActivate = "activate"

func (l *MapLedger) Activate(ctx context.Context, id artifact.ClaimID, owner artifact.ClaimOwner, set artifact.BindingSet) (artifact.RetentionClaim, error) {
	if id == "" || owner.Kind == "" || owner.Identity == "" {
		return artifact.RetentionClaim{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opActivate, Identity: string(id), Detail: "empty claim identity or owner"}
	}
	if len(set.BindingIDs) == 0 || set.RefSetDigest == "" {
		return artifact.RetentionClaim{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opActivate, Identity: string(id), Detail: "empty binding set"}
	}
	if l.Builder != nil {
		rebuilt, err := l.Builder.Build(ctx, set.BindingIDs)
		if err != nil {
			return artifact.RetentionClaim{}, err
		}
		if !sameSet(rebuilt, set) {
			return artifact.RetentionClaim{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: opActivate, Identity: string(id), Detail: "binding set does not verify"}
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if existing, ok := l.claims[id]; ok {
		if existing.State != artifact.ClaimActive || existing.Owner != owner || !sameSet(existing.BindingSet, set) {
			return artifact.RetentionClaim{}, &artifact.Error{Code: artifact.ErrConflict, Operation: opActivate, Identity: string(id)}
		}
		return existing, nil
	}
	if l.claims == nil {
		l.claims = make(map[artifact.ClaimID]artifact.RetentionClaim)
	}
	claim := artifact.RetentionClaim{ID: id, Owner: owner, BindingSet: set, State: artifact.ClaimActive}
	l.claims[id] = claim
	return claim, nil
}

func (l *MapLedger) LookupClaim(_ context.Context, id artifact.ClaimID) (artifact.RetentionClaim, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.claims[id]
	return c, ok, nil
}

func (l *MapLedger) ReleaseActive(_ context.Context, id artifact.ClaimID) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.claims[id]
	if !ok {
		return &artifact.Error{Code: artifact.ErrNotFound, Operation: "release", Identity: string(id)}
	}
	c.State = artifact.ClaimReleased
	l.claims[id] = c
	return nil
}

// ClaimsByOwner pages the owner's claims in ClaimID order under a watermark
// cursor (ART-RET-3): a first page fixes its watermark at the last matching
// id, so claims activated while paging stay out of the enumeration.
func (l *MapLedger) ClaimsByOwner(_ context.Context, q artifact.ClaimOwnerQuery, cursor artifact.ClaimCursor) (artifact.ClaimPage, error) {
	if q.Kind == "" || q.Authority == "" {
		return artifact.ClaimPage{}, &artifact.Error{Code: artifact.ErrInvalid, Operation: "claims_by_owner", Detail: "empty owner kind or authority"}
	}
	limit := q.Limit
	if limit <= 0 {
		limit = artifact.DefaultClaimPageSize
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var matching []artifact.RetentionClaim
	for _, c := range l.claims {
		if c.Owner.Kind == q.Kind && c.Owner.Authority == q.Authority && matchesIdentity(q, c.Owner.Identity) {
			matching = append(matching, c)
		}
	}
	sort.Slice(matching, func(i, j int) bool { return matching[i].ID < matching[j].ID })
	watermark := cursor.Watermark
	if watermark == "" && len(matching) > 0 {
		watermark = matching[len(matching)-1].ID
	}
	var items []artifact.RetentionClaim
	for _, c := range matching {
		if c.ID <= cursor.After || c.ID > watermark {
			continue
		}
		if len(items) == limit {
			return artifact.ClaimPage{Items: items, Next: &artifact.ClaimCursor{Watermark: watermark, After: items[len(items)-1].ID}}, nil
		}
		items = append(items, c)
	}
	return artifact.ClaimPage{Items: items}, nil
}

// matchesIdentity is ART-RET-3: nil selects every identity, an explicit
// list selects its members, and an empty identity never matches.
func matchesIdentity(q artifact.ClaimOwnerQuery, identity string) bool {
	if identity == "" {
		return false
	}
	if q.Identities == nil {
		return true
	}
	for _, want := range q.Identities {
		if want != "" && want == identity {
			return true
		}
	}
	return false
}

func sameSet(a, b artifact.BindingSet) bool {
	if a.RefSetDigest != b.RefSetDigest || len(a.BindingIDs) != len(b.BindingIDs) {
		return false
	}
	for i := range a.BindingIDs {
		if a.BindingIDs[i] != b.BindingIDs[i] {
			return false
		}
	}
	return true
}

// Stores returns a fresh reference binding store and the retention ledger
// that verifies sets against it: what a test that needs bindings but is not
// about them asks for.
func Stores(t testing.TB) (artifact.BindingStore, artifact.RetentionLedger) {
	t.Helper()
	bindings := &MapBindings{}
	return bindings, &MapLedger{Builder: artifact.SetBuilder{Resolver: bindings}}
}
