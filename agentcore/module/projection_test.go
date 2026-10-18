package module

import (
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
)

func TestProjectionInheritanceOverride(t *testing.T) {
	const (
		inheritedEvent ledger.EventType = "twilight/history/inherited"
		ownEvent       ledger.EventType = "twilight/history/own"
	)
	r, err := BuildRegistry(ModuleDescriptor{
		Source: SourceTwilight,
		ID:     "history",
		Streams: []StreamDefinition{
			{Domain: "inherited", Inheritance: Inherited},
			{Domain: "own", Inheritance: Own},
		},
		Events: []EventDefinition{
			{Type: inheritedEvent, Domain: "inherited", Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[struct{}]{}}},
			{Type: ownEvent, Domain: "own", Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[struct{}]{}}},
		},
		Projections: []ProjectionDefinition{{
			ID:       "twilight/history/projection",
			Version:  1,
			Consumes: []ledger.EventType{inheritedEvent, ownEvent},
			Initial:  func() (any, error) { return []string{}, nil },
			Apply: func(state any, event DecodedEvent) (any, error) {
				return append(state.([]string), event.Domain.Name), nil
			},
			StateCodec: JSONStateCodec[[]string]{},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	scope, err := r.ScopeFor("twilight/history/projection", 1)
	if err != nil {
		t.Fatal(err)
	}

	inheritedPayload, err := r.Encode(inheritedEvent, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	inherited := ledger.Event{Type: inheritedEvent, Payload: inheritedPayload}
	ownPayload, err := r.Encode(ownEvent, struct{}{})
	if err != nil {
		t.Fatal(err)
	}
	own := ledger.Event{Type: ownEvent, Payload: ownPayload}
	commits := []ledger.Commit{{
		Seq: 0,
		Batches: []ledger.EventBatch{
			{Domain: ledger.Domain{Name: "inherited"}, Events: []ledger.Event{inherited}},
			{Domain: ledger.Domain{Name: "own"}, Events: []ledger.Event{own}},
		},
	}}

	for _, tc := range []struct {
		name     string
		policy   InheritancePolicy
		expected []string
	}{
		{name: "domain defaults", expected: []string{"inherited"}},
		{name: "inherit all", policy: InheritAll, expected: []string{"inherited", "own"}},
		{name: "inherit none", policy: InheritNone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scope.Def.InheritanceOverride = tc.policy
			state, err := r.FoldFrom(scope, []string{}, commits, ledger.Head{Next: 1})
			if err != nil {
				t.Fatal(err)
			}
			got := state.([]string)
			if len(got) != len(tc.expected) {
				t.Fatalf("folded domains = %v, want %v", got, tc.expected)
			}
			for i := range got {
				if got[i] != tc.expected[i] {
					t.Fatalf("folded domains = %v, want %v", got, tc.expected)
				}
			}
		})
	}
}
