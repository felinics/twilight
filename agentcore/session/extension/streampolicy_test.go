package extension

import (
	"errors"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
)

type policyPayload struct {
	Note string `json:"note"`
}

// policyModule declares streams and one event that names the domain
// eventStream.
func policyModule(streams []StreamDefinition, eventStream string) ModuleDescriptor {
	return ModuleDescriptor{
		Source: "polsrc", ID: "pol", Streams: streams,
		Events: []EventDefinition{{
			Type: "polsrc/pol/note", Stream: eventStream,
			Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[policyPayload]{}},
		}},
	}
}

// TestBuildRegistryValidatesStreamDeclarations: stream declarations and the
// domains events name are assembly errors, caught before any write
// (EXT-STR-1).
func TestBuildRegistryValidatesStreamDeclarations(t *testing.T) {
	singleton := StreamDefinition{Domain: "pol", Lineage: session.LineageSession}
	keyed := StreamDefinition{Domain: "polrun", Key: func(any) (string, error) { return "r1", nil }, Lineage: session.LineageSegment}
	other := ModuleDescriptor{Source: "polsrc", ID: "other", Streams: []StreamDefinition{{Domain: "other", Lineage: session.LineageSession}}}
	cases := map[string]struct {
		streams []StreamDefinition
		event   string
		others  []ModuleDescriptor
		detail  string
	}{
		"valid singleton":                  {streams: []StreamDefinition{singleton}, event: "pol"},
		"valid keyed":                      {streams: []StreamDefinition{keyed}, event: "polrun"},
		"event without a domain":           {streams: []StreamDefinition{singleton}, detail: "no stream domain"},
		"event of an undeclared domain":    {streams: []StreamDefinition{singleton}, event: "polrun", detail: "does not declare"},
		"event of another module's domain": {streams: []StreamDefinition{singleton}, event: "other", others: []ModuleDescriptor{other}, detail: "does not declare"},
		"empty domain":                     {streams: []StreamDefinition{{Lineage: session.LineageSession}}, event: "pol", detail: "stream domain is empty"},
		"domain with a separator":          {streams: []StreamDefinition{{Domain: "pol/x", Lineage: session.LineageSession}}, event: "pol", detail: `contains "/"`},
		"duplicate domain":                 {streams: []StreamDefinition{singleton, singleton}, event: "pol", detail: "duplicate stream domain"},
		"domain declared by two modules": {streams: []StreamDefinition{singleton}, event: "pol", detail: "duplicate stream domain",
			others: []ModuleDescriptor{{Source: "polsrc", ID: "other", Streams: []StreamDefinition{singleton}}}},
		"missing lineage": {streams: []StreamDefinition{{Domain: "pol"}}, event: "pol", detail: "lineage is empty"},
		"unknown lineage": {streams: []StreamDefinition{{Domain: "pol", Lineage: "branch"}}, event: "pol", detail: "unknown stream lineage"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			modules := append([]ModuleDescriptor{policyModule(tc.streams, tc.event)}, tc.others...)
			_, err := BuildRegistry(modules...)
			if tc.detail == "" {
				if err != nil {
					t.Fatalf("BuildRegistry = %v, want success", err)
				}
				return
			}
			var eerr *Error
			if !errors.As(err, &eerr) || !strings.Contains(eerr.Detail, tc.detail) {
				t.Fatalf("BuildRegistry = %v, want detail containing %q", err, tc.detail)
			}
		})
	}
}
