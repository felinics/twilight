package writer

import (
	"errors"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// TestCheckStreamAffinity: every branch of the writer-side stream attribution
// check (EXT-STR-1), table-driven.
func TestCheckStreamAffinity(t *testing.T) {
	type runValue struct{ RunID string }
	keyed := extension.StreamDefinition{Domain: "run", Lineage: session.LineageSegment, Key: func(v any) (string, error) {
		rv, ok := v.(runValue)
		if !ok {
			return "", errors.New("not a run value")
		}
		return rv.RunID, nil
	}}
	singleton := extension.StreamDefinition{Domain: "chat", Lineage: session.LineageSession}
	runStream := keyed.Ref("r1")
	chatStream := singleton.Ref("")
	cases := map[string]struct {
		stream  session.StreamRef
		def     extension.StreamDefinition
		value   any
		verdict string // "" means accepted
	}{
		"singleton event in its stream":     {chatStream, singleton, "note", ""},
		"keyed event in its stream":         {runStream, keyed, runValue{"r1"}, ""},
		"singleton event in another domain": {runStream, singleton, "note", `belongs to stream domain "chat"`},
		"keyed event in another domain":     {chatStream, keyed, runValue{"r1"}, `belongs to stream domain "run"`},
		"singleton batch with an ID":        {session.StreamRef{Domain: "chat", ID: "x"}, singleton, "note", "is a singleton"},
		"keyed batch without an ID":         {session.StreamRef{Domain: "run"}, keyed, runValue{"r1"}, "names no stream ID"},
		"id mismatch":                       {runStream, keyed, runValue{"r9"}, "but the batch is"},
		"id empty":                          {runStream, keyed, runValue{""}, "names no stream"},
		"value of another type":             {runStream, keyed, "note", "not a run value"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := checkStreamAffinity(tc.stream, tc.def, tc.value)
			if tc.verdict == "" {
				if got != "" {
					t.Fatalf("verdict = %q, want accepted", got)
				}
				return
			}
			if got == "" || !strings.Contains(got, tc.verdict) {
				t.Fatalf("verdict = %q, want containing %q", got, tc.verdict)
			}
		})
	}
}
