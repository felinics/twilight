// Package artifact is the Artifact Core (docs/design/agent-artifact.md):
// Ref, Binding and the two-state RetentionLedger. The ledger
// persists itself; claims are activated before the owner fact is appended and
// orphans are released by the pre-collection reconciliation (ART-RET-3).
package artifact

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/felinics/twilight/agentcore/es"
)

// Operation names and shared details of artifact errors.
const (
	opRef                   = "ref"
	opActivate              = "activate"
	opPut                   = "put"
	opPromote               = "promote"
	opRegistry              = "registry"
	opVerify                = "verify"
	detailUnknownDurability = "unknown durability"
)

type (
	WireVersion   uint16
	Scheme        string
	Authority     string
	Key           string
	BindingID     string
	BindingDigest string
	ClaimID       string
	RefSetDigest  string
)

const WireVersion1 WireVersion = 1

type Durability string

const (
	Ephemeral  Durability = "ephemeral"
	EventBound Durability = "event_bound"
	Pinned     Durability = "pinned"
)

// Rank orders durabilities: Ephemeral < EventBound < Pinned.
func (d Durability) Rank() int {
	switch d {
	case Ephemeral:
		return 0
	case EventBound:
		return 1
	case Pinned:
		return 2
	default:
		return -1
	}
}

type Integrity struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"value"`
}

// Ref locates and verifies immutable content (ART-REF-1).
type Ref struct {
	Scheme             Scheme     `json:"scheme"`
	Authority          Authority  `json:"authority"`
	Key                Key        `json:"key"`
	MediaType          string     `json:"mediaType,omitempty"`
	SizeBytes          *uint64    `json:"sizeBytes,omitempty"`
	Integrity          *Integrity `json:"integrity,omitempty"`
	Durability         Durability `json:"durability"`
	ExpiresAtUnixMilli *int64     `json:"expiresAtUnixMilli,omitempty"`
}

// Validate applies ART-ID-1 and ART-REF-2.
func (r Ref) Validate() error {
	if r.Scheme == "" || r.Authority == "" || r.Key == "" {
		return &Error{Code: ErrInvalid, Operation: opRef, Detail: "empty locator component"}
	}
	if r.Durability.Rank() < 0 {
		return &Error{Code: ErrInvalid, Operation: opRef, Detail: detailUnknownDurability}
	}
	if r.Scheme == "cas" && r.Integrity == nil {
		return &Error{Code: ErrInvalid, Operation: opRef, Detail: "cas ref requires integrity"}
	}
	if r.ExpiresAtUnixMilli != nil && r.Durability != Ephemeral {
		return &Error{Code: ErrInvalid, Operation: opRef, Detail: "only ephemeral refs may expire"}
	}
	return nil
}

// Identity is the versioned canonical wire identity of the complete Ref.
func (r Ref) Identity() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	raw, err := es.EncodeTypedPayload(uint16(WireVersion1), "twilight/artifact/ref", r)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// Binding maps a stable BindingID to an immutable Ref (ART-BND-1).
type Binding struct {
	ID     BindingID     `json:"id"`
	Ref    Ref           `json:"ref"`
	Digest BindingDigest `json:"digest"`
}

// DigestBinding covers the domain, BindingID and full RefWireIdentity.
func DigestBinding(id BindingID, ref Ref) (BindingDigest, error) {
	identity, err := ref.Identity()
	if err != nil {
		return "", err
	}
	raw, err := es.EncodeTypedPayload(uint16(WireVersion1), "twilight/artifact/binding", struct {
		ID       BindingID `json:"id"`
		Identity string    `json:"identity"`
	}{id, identity})
	if err != nil {
		return "", err
	}
	return BindingDigest(es.DigestBytes(raw)), nil
}

// NewBinding builds a Binding with its digest.
func NewBinding(id BindingID, ref Ref) (Binding, error) {
	if id == "" {
		return Binding{}, &Error{Code: ErrInvalid, Operation: "binding", Detail: "empty BindingID"}
	}
	d, err := DigestBinding(id, ref)
	if err != nil {
		return Binding{}, err
	}
	return Binding{ID: id, Ref: ref, Digest: d}, nil
}

type BindingResolver interface {
	ResolveBinding(context.Context, BindingID) (Binding, error)
}

type BindingStore interface {
	BindingResolver
	CreateBinding(context.Context, Binding) (Binding, error)
	LookupBinding(context.Context, BindingID) (Binding, bool, error)
}

// --- retention ledger --------------------------------------------------------

type ClaimOwner struct {
	Kind      string `json:"kind"`
	Authority string `json:"authority"`
	Identity  string `json:"identity"`
}

// ClaimOwnerScope selects every owner of one Kind under one Authority.
type ClaimOwnerScope struct {
	Kind      string
	Authority string
}

type ClaimState string

const (
	ClaimActive   ClaimState = "active"
	ClaimReleased ClaimState = "released"
)

// BindingSet is a canonical, resolved retention set (ART-RET-1).
type BindingSet struct {
	BindingIDs   []BindingID  `json:"bindingIds"`
	RefSetDigest RefSetDigest `json:"refSetDigest"`
}

type RetentionClaim struct {
	ID         ClaimID    `json:"id"`
	Owner      ClaimOwner `json:"owner"`
	BindingSet BindingSet `json:"bindingSet"`
	State      ClaimState `json:"state"`
}

// BindingSetBuilder is the only way to construct a BindingSet.
type BindingSetBuilder interface {
	Build(context.Context, []BindingID) (BindingSet, error)
}

// SetBuilder resolves every binding and digests the sorted (ID, Digest) pairs.
type SetBuilder struct{ Resolver BindingResolver }

func (b SetBuilder) Build(ctx context.Context, ids []BindingID) (BindingSet, error) {
	if b.Resolver == nil {
		return BindingSet{}, errors.New("artifact: set builder: nil resolver")
	}
	sorted := SortedUniqueBindingIDs(ids)
	type pair struct {
		ID     BindingID     `json:"id"`
		Digest BindingDigest `json:"digest"`
	}
	pairs := make([]pair, 0, len(sorted))
	for _, id := range sorted {
		binding, err := b.Resolver.ResolveBinding(ctx, id)
		if err != nil {
			return BindingSet{}, err
		}
		if binding.Ref.Durability.Rank() < EventBound.Rank() {
			return BindingSet{}, &Error{Code: ErrInvalid, Operation: "build_set", Identity: string(id), Detail: "ephemeral binding cannot be claimed"}
		}
		pairs = append(pairs, pair{id, binding.Digest})
	}
	raw, err := es.EncodeTypedPayload(uint16(WireVersion1), "twilight/artifact/ref-set", pairs)
	if err != nil {
		return BindingSet{}, err
	}
	return BindingSet{BindingIDs: sorted, RefSetDigest: RefSetDigest(es.DigestBytes(raw))}, nil
}

func SortedUniqueBindingIDs(ids []BindingID) []BindingID {
	out := append([]BindingID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	n := 0
	for i := range out {
		if n == 0 || out[i] != out[n-1] {
			out[n] = out[i]
			n++
		}
	}
	if n == 0 {
		return nil
	}
	return out[:n]
}

// ClaimOwnerQuery selects claims by owner (ART-RET-3). Identities nil selects
// every owner of the Kind under the Authority; an explicit list selects those
// owners only, and an empty identity in it matches nothing. Limit is the page
// size, 0 meaning DefaultClaimPageSize.
type ClaimOwnerQuery struct {
	Kind       string
	Authority  string
	Identities []string
	Limit      int
}

// ClaimCursor pages ClaimsByOwner. Watermark is the highest ClaimID the
// enumeration covers, fixed on the first page so claims activated while paging
// are excluded; After is the last ClaimID returned. The zero cursor starts an
// enumeration.
type ClaimCursor struct {
	Watermark ClaimID
	After     ClaimID
}

// ClaimPage is one page of ClaimsByOwner; Next is nil once exhausted.
type ClaimPage struct {
	Items []RetentionClaim
	Next  *ClaimCursor
}

// DefaultClaimPageSize is the ClaimsByOwner page size when the query gives none.
const DefaultClaimPageSize = 256

// RetentionLedger keeps Active/Released claims (ART-RET-2). It persists
// itself; Activate returns only once the claim is durable.
type RetentionLedger interface {
	Activate(context.Context, ClaimID, ClaimOwner, BindingSet) (RetentionClaim, error)
	LookupClaim(context.Context, ClaimID) (RetentionClaim, bool, error)
	ReleaseActive(context.Context, ClaimID) error
	// ClaimsByOwner enumerates the matching claims of every state in stable
	// ClaimID order under a watermark cursor (ART-RET-3).
	ClaimsByOwner(context.Context, ClaimOwnerQuery, ClaimCursor) (ClaimPage, error)
}

// ActiveClaims drains ClaimsByOwner for the scope and keeps the Active claims,
// in ClaimID order.
func ActiveClaims(ctx context.Context, ledger RetentionLedger, scope ClaimOwnerScope) ([]RetentionClaim, error) {
	var out []RetentionClaim
	cursor := ClaimCursor{}
	for {
		page, err := ledger.ClaimsByOwner(ctx, ClaimOwnerQuery{Kind: scope.Kind, Authority: scope.Authority}, cursor)
		if err != nil {
			return nil, err
		}
		for _, c := range page.Items {
			if c.State == ClaimActive {
				out = append(out, c)
			}
		}
		if page.Next == nil {
			return out, nil
		}
		cursor = *page.Next
	}
}

// OwnerVerifier is supplied by the owner's host: does the owner fact exist?
type OwnerVerifier interface {
	OwnerExists(context.Context, ClaimOwner) (bool, error)
}

// Reconcile releases Active claims in scope whose owner no longer exists
// (ART-RET-3). The caller guarantees no owner write in scope is in flight.
func Reconcile(ctx context.Context, ledger RetentionLedger, scope ClaimOwnerScope, verifier OwnerVerifier) (int, error) {
	claims, err := ActiveClaims(ctx, ledger, scope)
	if err != nil {
		return 0, err
	}
	released := 0
	for _, c := range claims {
		exists, err := verifier.OwnerExists(ctx, c.Owner)
		if err != nil {
			return released, err
		}
		if exists {
			continue
		}
		if err := ledger.ReleaseActive(ctx, c.ID); err != nil {
			return released, err
		}
		released++
	}
	return released, nil
}

// --- errors --------------------------------------------------------------------

type ErrorCode string

const (
	ErrInvalid      ErrorCode = "invalid"
	ErrNotFound     ErrorCode = "not_found"
	ErrConflict     ErrorCode = "conflict"
	ErrUnauthorized ErrorCode = "unauthorized"
	ErrExpired      ErrorCode = "expired"
	ErrCorrupt      ErrorCode = "corrupt"
	ErrUnsupported  ErrorCode = "unsupported"
	ErrUnavailable  ErrorCode = "unavailable"
)

type Error struct {
	Code      ErrorCode
	Operation string
	Identity  string
	Detail    string
}

func (e *Error) Error() string {
	s := fmt.Sprintf("artifact: %s: %s", e.Operation, e.Code)
	if e.Identity != "" {
		s += " " + e.Identity
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}
