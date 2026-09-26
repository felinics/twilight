package runmod

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/model/sdkconv"
	"github.com/felinics/twilight/agentcore/run/schema"
	"github.com/felinics/twilight/agentcore/session/filestore"
	"github.com/felinics/twilight/sdk"
)

// frozenBody freezes one request and renders the bytes the store keeps.
func frozenBody(t *testing.T, text string) (run.Digest, []byte) {
	t.Helper()
	req, err := sdkconv.FreezeModelRequest(sdk.Request{Model: "m-1", Messages: []sdk.Message{sdk.UserMessage(text)}})
	if err != nil {
		t.Fatal(err)
	}
	digest, err := schema.Canonical().DigestRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := schema.Bodies().EncodeRequest(&req, digest)
	if err != nil {
		t.Fatal(err)
	}
	return digest, raw
}

func fileFrozen(t *testing.T, root string) frozen.Store {
	t.Helper()
	store, err := filestore.NewContentStore(root, FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fz, err := FrozenValues(store, &artifacttest.MapBindings{})
	if err != nil {
		t.Fatal(err)
	}
	return fz
}

// The frozen.Store adapter over the cas ContentStore (RUN-WIR-4): the
// digest is the SHA-256 of the stored bytes, so Put and Get need no index; a
// body under a name it does not digest to is refused; and a second instance
// over the same root reads what the first wrote -- the path a restarted
// process takes to replay an interrupted ModelStep.
func TestFrozenValuesOverContentStores(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name string
		open func(t *testing.T, root string) frozen.Store
		file bool
	}{
		{"file", fileFrozen, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			fv := tc.open(t, root)
			digest, raw := frozenBody(t, "hello")
			if err := fv.Put(ctx, digest, raw); err != nil {
				t.Fatalf("put: %v", err)
			}
			if err := fv.Put(ctx, digest, raw); err != nil {
				t.Fatalf("idempotent re-put: %v", err)
			}
			other, otherRaw := frozenBody(t, "other")
			if err := fv.Put(ctx, digest, otherRaw); err == nil {
				t.Fatal("a body under a digest it does not hash to must be refused")
			}
			got, ok, err := fv.Get(ctx, digest)
			if err != nil || !ok || string(got) != string(raw) {
				t.Fatalf("get = %q %v %v", got, ok, err)
			}
			if req, err := frozen.DecodeRequest(got, digest); err != nil || req.Model != "m-1" {
				t.Fatalf("decode = %+v %v", req, err)
			}
			if _, ok, err := fv.Get(ctx, other); err != nil || ok {
				t.Fatalf("missing digest = %v %v, want (false, nil)", ok, err)
			}
			if _, _, err := fv.Get(ctx, "not-a-digest"); err == nil {
				t.Fatal("a malformed digest must be an error, not a miss")
			}
			if tc.file {
				got2, ok, err := tc.open(t, root).Get(ctx, digest)
				if err != nil || !ok || string(got2) != string(raw) {
					t.Fatalf("second instance get = %q %v %v", got2, ok, err)
				}
			}
		})
	}
}

// A store serving another Authority answers with unauthorized: the adapter
// surfaces it as an error rather than a miss.
func TestFrozenValuesRejectsForeignAuthority(t *testing.T) {
	ctx := context.Background()
	store, err := filestore.NewContentStore(t.TempDir(), "someone-else", filestore.ContentStoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	fv, err := FrozenValues(store, &artifacttest.MapBindings{})
	if err != nil {
		t.Fatal(err)
	}
	digest, raw := frozenBody(t, "hello")
	if err := fv.Put(ctx, digest, raw); err != nil {
		t.Fatalf("the store accepts the bytes; authority is checked on read: %v", err)
	}
	_, _, err = fv.Get(ctx, digest)
	var aerr *artifact.Error
	if !errors.As(err, &aerr) || aerr.Code != artifact.ErrUnauthorized {
		t.Fatalf("get from a foreign authority = %v, want unauthorized", err)
	}
}
