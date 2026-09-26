package runmod

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/run/wire"
)

// FrozenAuthority is the cas Authority of frozen run bodies: the run layer's
// frozen.Store is one Authority of the artifact ContentStore, not a
// second content-addressed store (RUN-WIR-4).
const FrozenAuthority artifact.Authority = frozen.Authority

// FrozenMediaType is the media type frozen bodies are stored under; the cas
// store binds it to the key, so every Put and Get of a body agrees on it.
const FrozenMediaType = "application/vnd.twilight.frozen+json"

// frozenValues realizes frozen.Store over a cas ContentStore and the
// BindingStore the Session Writers admit against. A body's digest is the
// SHA-256 of its bytes (run.EncodeFrozen*), which is exactly the cas Key, so
// no index maps digests to refs: Get rebuilds the Ref from the digest alone.
// Put stores the body and registers its Binding in one call, so a fact that
// names the digest is admissible as soon as Put returns and no caller has to
// coordinate two stores before a commit (RUN-WIR-4, EXT-WRT-3). Bodies are
// EventBound content whose retention root is the fact that names them; the
// adapter offers no Delete.
type frozenValues struct {
	store    artifact.ContentStore
	bindings artifact.BindingStore
}

// FrozenValues adapts a cas ContentStore serving FrozenAuthority and the
// Writers' BindingStore to the run layer's frozen.Store port. bindings
// must be the store the Writers' Admission resolves against. A content store
// of another Authority answers every Get with ErrUnauthorized, which surfaces
// as an error rather than a miss.
func FrozenValues(store artifact.ContentStore, bindings artifact.BindingStore) (frozen.Store, error) {
	if store == nil || bindings == nil {
		return nil, errors.New("runmod: frozen values require a content store and a binding store")
	}
	return &frozenValues{store: store, bindings: bindings}, nil
}

func (f *frozenValues) Put(ctx context.Context, digest run.Digest, val []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ref, err := FrozenRef(digest)
	if err != nil {
		return err
	}
	if key, _ := artifact.CASKey(val); key != ref.Key {
		return fmt.Errorf("agent: frozen values: body digests to %s, not to its name %s", key, digest)
	}
	if _, err := f.store.Put(ctx, artifact.PutRequest{MediaType: FrozenMediaType, Reader: bytes.NewReader(val), Durability: artifact.EventBound}); err != nil {
		return err
	}
	binding, err := FrozenBinding(digest)
	if err != nil {
		return err
	}
	// An identical Binding is already registered on a replay; only a
	// differing Ref under the same BindingID conflicts (ART-BND-1).
	_, err = f.bindings.CreateBinding(ctx, binding)
	return err
}

func (f *frozenValues) Get(ctx context.Context, digest run.Digest) (body []byte, found bool, err error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	if digest == "" {
		return nil, false, nil
	}
	ref, err := FrozenRef(digest)
	if err != nil {
		return nil, false, err
	}
	rc, _, err := f.store.Open(ctx, ref)
	if err != nil {
		var aerr *artifact.Error
		if errors.As(err, &aerr) && aerr.Code == artifact.ErrNotFound {
			return nil, false, nil
		}
		return nil, false, err
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		return nil, false, err
	}
	return raw, true, nil
}

// FrozenRef is the cas Ref of the body named by digest: Key and Integrity are
// both the digest, MediaType and Durability are the adapter's constants. The
// Run protocol knows only the digest; this derivation is the adapter's
// (RUN-WIR-4).
func FrozenRef(digest run.Digest) (artifact.Ref, error) {
	algorithm, val, ok := strings.Cut(string(digest), ":")
	if !ok || algorithm != artifact.IntegritySHA256 || val == "" {
		return artifact.Ref{}, fmt.Errorf("agent: frozen values: digest %q is not sha256:<hex>", digest)
	}
	return artifact.Ref{Scheme: artifact.SchemeCAS, Authority: FrozenAuthority, Key: artifact.Key(digest), MediaType: FrozenMediaType,
		Integrity: &artifact.Integrity{Algorithm: algorithm, Value: val}, Durability: artifact.EventBound}, nil
}

// FrozenBindingID is the BindingID under which a frozen body's Ref is
// registered: derived from the digest alone, so the fact that names the body
// and the binding admission agree without an index.
func FrozenBindingID(digest run.Digest) artifact.BindingID {
	return artifact.BindingID(frozen.Authority + "/" + string(digest))
}

// FrozenBinding is the Binding a fact naming digest references. Its Ref is
// FrozenRef(digest), so registering it is idempotent across replays.
func FrozenBinding(digest run.Digest) (artifact.Binding, error) {
	ref, err := FrozenRef(digest)
	if err != nil {
		return artifact.Binding{}, err
	}
	return artifact.NewBinding(FrozenBindingID(digest), ref)
}

// frozenRefs is the BindingExtractor of the fact types that name a frozen
// body: the one BindingID the Writer admits and claims for the commit
// (EXT-REF-2, EXT-WRT-3).
func frozenRefs(val any) ([]artifact.BindingID, error) {
	ev, ok := val.(Event)
	if !ok {
		return nil, fmt.Errorf("frozen refs: unexpected %T", val)
	}
	var digest run.Digest
	switch f := ev.Fact.(type) {
	case run.ModelStepPrepared:
		digest = f.RequestDigest
	case run.ModelStepCompleted:
		digest = f.ResultDigest
	case run.ToolCallCompleted:
		digest = f.OutputDigest
	case run.ToolCallAnswered:
		digest = f.ResponseDigest
	default:
		return nil, fmt.Errorf("frozen refs: %s names no frozen body", wire.FactType(ev.Fact))
	}
	if digest == "" {
		return nil, fmt.Errorf("frozen refs: %s has an empty digest", wire.FactType(ev.Fact))
	}
	return []artifact.BindingID{FrozenBindingID(digest)}, nil
}

// --- materialization ---------------------------------------------------------------

// Content resolves the bodies facts name by digest (RUN-WIR-4) for the layer
// that renders a structural projection into a usable representation. It is
// the materialization port: projections stay pure folds over facts and never
// touch it.
type Content struct {
	Frozen frozen.Store
}

// NewContent builds the materializer over a frozen.Store.
func NewContent(store frozen.Store) *Content { return &Content{Frozen: store} }

func (c *Content) raw(ctx context.Context, what string, digest run.Digest) ([]byte, error) {
	if err := runtime.CheckContext(ctx); err != nil {
		return nil, err
	}
	if digest == "" {
		return nil, fmt.Errorf("runmod: empty %s digest", what)
	}
	raw, ok, err := c.Frozen.Get(ctx, digest)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: %s %s", frozen.ErrMissing, what, digest)
	}
	return raw, nil
}

// ModelResult returns the body ModelStepCompleted.ResultDigest names.
func (c *Content) ModelResult(ctx context.Context, digest run.Digest) (model.ModelResult, error) {
	raw, err := c.raw(ctx, "model result", digest)
	if err != nil {
		return model.ModelResult{}, err
	}
	return frozen.DecodeModelResult(raw, digest)
}

// ToolOutput returns the body ToolCallCompleted.OutputDigest names.
func (c *Content) ToolOutput(ctx context.Context, digest run.Digest) (run.CanonicalJSON, error) {
	raw, err := c.raw(ctx, "tool output", digest)
	if err != nil {
		return run.CanonicalJSON{}, err
	}
	return frozen.DecodeToolOutput(raw, digest)
}

// ToolResponse returns the body ToolCallAnswered.ResponseDigest names.
func (c *Content) ToolResponse(ctx context.Context, digest run.Digest) (run.CanonicalJSON, error) {
	raw, err := c.raw(ctx, "tool response", digest)
	if err != nil {
		return run.CanonicalJSON{}, err
	}
	return frozen.DecodeToolResponse(raw, digest)
}

// ModelRequest returns the body ModelStepPrepared.RequestDigest names.
func (c *Content) ModelRequest(ctx context.Context, digest run.Digest) (model.ModelRequest, error) {
	raw, err := c.raw(ctx, "model request", digest)
	if err != nil {
		return model.ModelRequest{}, err
	}
	return frozen.DecodeRequest(raw, digest)
}
