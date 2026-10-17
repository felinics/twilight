package effect

import (
	"encoding/json"
	"testing"
)

func TestAssignmentOwnerNamespaces(t *testing.T) {
	a := Assignment{Session: "s", OwnerKind: OwnerTurn, OwnerID: "same", Effect: "same", Body: ModelAssignment{Model: "test", RequestDigest: "sha256:test"}}
	b := a
	b.OwnerKind = OwnerCompaction
	if a.Key() == b.Key() {
		t.Fatal("execution owners collided")
	}
	ad, err := a.Digest()
	if err != nil {
		t.Fatal(err)
	}
	bd, err := b.Digest()
	if err != nil || ad == bd {
		t.Fatalf("owner not part of immutable assignment: %s %s %v", ad, bd, err)
	}
	for _, want := range []Assignment{a, b} {
		raw, err := json.Marshal(want)
		if err != nil {
			t.Fatal(err)
		}
		var got Assignment
		if err := json.Unmarshal(raw, &got); err != nil {
			t.Fatal(err)
		}
		if got.Key() != want.Key() || got.Kind() != want.Kind() {
			t.Fatalf("roundtrip = %+v, want %+v", got, want)
		}
		if err := got.Key().Validate(); err != nil {
			t.Fatal(err)
		}
	}
	for _, key := range []AssignmentKey{{}, {Session: "s", OwnerKind: OwnerTurn, OwnerID: "same"}, {Session: "s", OwnerKind: "unknown", OwnerID: "same", Effect: "same"}} {
		if key.Validate() == nil {
			t.Fatalf("invalid key accepted: %+v", key)
		}
	}
}
