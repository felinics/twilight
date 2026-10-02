package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// SchemeCAS is the standard content-addressed scheme (ART-CAP-2): Key is the
// content digest in the form <algorithm>:<hex> and equals Integrity.
const SchemeCAS Scheme = "cas"

// IntegritySHA256 is the one integrity algorithm the reference store speaks.
const IntegritySHA256 = "sha256"

// Info is what a Resolver knows about a Ref's content (ART-BND-2).
type Info struct {
	MediaType  string
	SizeBytes  *uint64
	Integrity  *Integrity
	Durability Durability
}

// PutRequest stores new content. Reader is consumed to EOF.
type PutRequest struct {
	MediaType  string
	Reader     io.Reader
	Durability Durability
}

// PromoteRequest asks for a Ref of the same content at TargetScheme and
// TargetAuthority with at least the durability of the source (ART-REF-2).
type PromoteRequest struct {
	TargetScheme    Scheme
	TargetAuthority Authority
	Durability      Durability
}

// Resolver reads content behind a Ref and verifies it against the Ref's
// declared size and integrity (ART-BND-2). Failures are classified by
// ErrorCode (ART-CAP-1): ErrNotFound, ErrExpired, ErrUnauthorized (a key of
// another Authority), ErrCorrupt (stored bytes disagree with the Ref) and
// ErrUnsupported (a scheme this resolver does not speak).
type Resolver interface {
	Stat(context.Context, Ref) (Info, error)
	Open(context.Context, Ref) (io.ReadCloser, Info, error)
}

// Store writes content and returns its Ref only once the write is durable.
// A repeated Put of identical bytes is idempotent (ART-BND-2).
type Store interface {
	Put(context.Context, PutRequest) (Ref, error)
}

// Promoter produces a new Ref for existing content at the requested target
// and durability; it never lowers durability and never rewrites a Binding
// (ART-BND-2, ART-REF-2).
type Promoter interface {
	Promote(context.Context, Ref, PromoteRequest) (Ref, error)
}

// ContentStore is one Authority's full capability set.
type ContentStore interface {
	Resolver
	Store
	Promoter
}

// DefaultMaxContentBytes bounds a Put when a store's options give no cap
// (ART-CAP-1, size amplification).
const DefaultMaxContentBytes int64 = 64 << 20

// CASKey is the cas Key and Integrity value of data.
func CASKey(data []byte) (Key, Integrity) {
	sum := sha256.Sum256(data)
	hexSum := hex.EncodeToString(sum[:])
	return Key(IntegritySHA256 + ":" + hexSum), Integrity{Algorithm: IntegritySHA256, Value: hexSum}
}

// CopyPromoter promotes across stores: it resolves the source Ref, writes the
// bytes into Target at the requested durability and returns the new Ref,
// verifying that both sides name the same content.
type CopyPromoter struct {
	Source Resolver
	Target Store
}

func (p CopyPromoter) Promote(ctx context.Context, ref Ref, req PromoteRequest) (Ref, error) {
	if p.Source == nil || p.Target == nil {
		return Ref{}, errors.New("artifact: copy promoter: nil source or target")
	}
	if req.Durability.Rank() < ref.Durability.Rank() {
		return Ref{}, &Error{Code: ErrInvalid, Operation: opPromote, Identity: string(ref.Key), Detail: fmt.Sprintf("cannot lower durability from %s to %s", ref.Durability, req.Durability)}
	}
	rc, info, err := p.Source.Open(ctx, ref)
	if err != nil {
		return Ref{}, err
	}
	defer rc.Close()
	out, err := p.Target.Put(ctx, PutRequest{MediaType: info.MediaType, Reader: rc, Durability: req.Durability})
	if err != nil {
		return Ref{}, err
	}
	if out.Scheme != req.TargetScheme || out.Authority != req.TargetAuthority {
		return Ref{}, &Error{Code: ErrUnsupported, Operation: opPromote, Identity: string(ref.Key), Detail: "target store does not serve the requested scheme and authority"}
	}
	if ref.Integrity != nil && out.Integrity != nil && *ref.Integrity != *out.Integrity {
		return Ref{}, &Error{Code: ErrCorrupt, Operation: opPromote, Identity: string(ref.Key), Detail: "target content integrity differs from the source"}
	}
	return out, nil
}
