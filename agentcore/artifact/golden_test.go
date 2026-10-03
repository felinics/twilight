package artifact

import (
	"context"
	"testing"
)

func freezeArtifact(t *testing.T, name, got, want string) {
	t.Helper()
	if got == want {
		return
	}
	if want == "" {
		t.Errorf("UNSET %s = %s", name, got)
		return
	}
	t.Errorf("golden %s drifted — an intentional wire change must update this fixture and agent-artifact.md:\n got: %s\nwant: %s", name, got, want)
}

// TestArtifactWireGolden freezes the Ref wire identity, the binding digest and
// the ref-set digest of WireVersion 1.
func TestArtifactWireGolden(t *testing.T) {
	size := uint64(3)
	ref := Ref{
		Scheme:     "cas",
		Authority:  "store-1",
		Key:        "k-1",
		MediaType:  "text/plain",
		SizeBytes:  &size,
		Integrity:  &Integrity{Algorithm: "sha256", Value: "abc"},
		Durability: EventBound,
	}
	identity, err := ref.Identity()
	if err != nil {
		t.Fatal(err)
	}
	freezeArtifact(t, "ref identity", identity, `v1:21:twilight/artifact/ref:{"authority":"store-1","durability":"event_bound","integrity":{"algorithm":"sha256","value":"abc"},"key":"k-1","mediaType":"text/plain","scheme":"cas","sizeBytes":3}`)

	bd, err := DigestBinding("b-1", ref)
	if err != nil {
		t.Fatal(err)
	}
	freezeArtifact(t, "binding digest", string(bd), "sha256:cfa91e37ef2aa6917f0df953f083b69dd41897a780af5f030eb967c316afaf76")

	store := goldenBindings{}
	ctx := context.Background()
	for _, id := range []BindingID{"b-1", "b-2"} {
		r := ref
		r.Key = Key("k-" + string(id))
		b, err := NewBinding(id, r)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.CreateBinding(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	set, err := SetBuilder{Resolver: store}.Build(ctx, []BindingID{"b-2", "b-1"})
	if err != nil {
		t.Fatal(err)
	}
	freezeArtifact(t, "ref set digest", string(set.RefSetDigest), "sha256:ef0d0d7eb84023a539bedc1ea0a2d198efeeed389e94a5ed7601dbbca07bf29c")
}

// goldenBindings is the smallest BindingResolver the golden needs: an
// in-test map, since the package has no memory store.
type goldenBindings map[BindingID]Binding

func (g goldenBindings) CreateBinding(_ context.Context, b Binding) (Binding, error) {
	g[b.ID] = b
	return b, nil
}

func (g goldenBindings) LookupBinding(_ context.Context, id BindingID) (Binding, bool, error) {
	b, ok := g[id]
	return b, ok, nil
}

func (g goldenBindings) ResolveBinding(ctx context.Context, id BindingID) (Binding, error) {
	b, ok, _ := g.LookupBinding(ctx, id)
	if !ok {
		return Binding{}, &Error{Code: ErrNotFound, Operation: "resolve_binding", Identity: string(id)}
	}
	return b, nil
}
