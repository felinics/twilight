package chatlog_test

import (
	"testing"

	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/chatlog"
)

// pairEntries builds a context with one tool call pair: input, assistant
// with call c1, the result of c1, and a final plain assistant.
func pairEntries() []chatlog.Entry {
	in := chatlog.Input{ID: "in1", Digest: "sha256:in1"}
	a1 := chatlog.Assistant{ID: "a1", TurnID: "t1", StepID: "a1", ResultDigest: "sha256:a1r", CallIDs: []chatlog.CallID{"c1"}, Digest: "sha256:a1"}
	r1 := chatlog.ToolResult{ID: "r1", TurnID: "t1", CallID: "c1", Status: chatlog.ToolSuccess, Source: chatlog.SourceToolOutput, OutputDigest: "sha256:r1o", Digest: "sha256:r1"}
	a2 := chatlog.Assistant{ID: "a2", TurnID: "t1", StepID: "a2", ResultDigest: "sha256:a2r", Digest: "sha256:a2"}
	return []chatlog.Entry{
		{Kind: chatlog.EntryInput, ID: "in1", Digest: in.Digest, Position: session.Position{Commit: 1}, Input: &in},
		{Kind: chatlog.EntryAssistant, ID: "a1", Digest: a1.Digest, Position: session.Position{Commit: 2}, Assistant: &a1},
		{Kind: chatlog.EntryToolResult, ID: "r1", Digest: r1.Digest, Position: session.Position{Commit: 3}, ToolResult: &r1},
		{Kind: chatlog.EntryAssistant, ID: "a2", Digest: a2.Digest, Position: session.Position{Commit: 4}, Assistant: &a2},
	}
}

// CheckRetainClosure rejects a retained set that splits a tool pair in either
// direction and accepts closed sets (APP-CKP-2).
func TestCheckRetainClosure(t *testing.T) {
	entries := pairEntries()
	pair := func(i int) chatlog.EntryDigestPair { return entries[i].Pair() }
	cases := []struct {
		name    string
		retain  []chatlog.EntryDigestPair
		wantErr bool
	}{
		{"closed pair", []chatlog.EntryDigestPair{pair(1), pair(2)}, false},
		{"plain suffix", []chatlog.EntryDigestPair{pair(3)}, false},
		{"result without assistant", []chatlog.EntryDigestPair{pair(2)}, true},
		{"assistant without result", []chatlog.EntryDigestPair{pair(1)}, true},
		{"empty", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := chatlog.CheckRetainClosure(entries, tc.retain)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
	// An assistant whose call has no result yet must be retained, whatever
	// else is (APP-CKP-2): the result will land after the compaction.
	open := openEntries()
	openPair := func(i int) chatlog.EntryDigestPair { return open[i].Pair() }
	openCases := []struct {
		name    string
		retain  []chatlog.EntryDigestPair
		wantErr bool
	}{
		{"open assistant retained", []chatlog.EntryDigestPair{openPair(3)}, false},
		{"open assistant dropped", nil, true},
		{"open assistant dropped, others kept", []chatlog.EntryDigestPair{openPair(1), openPair(2)}, true},
	}
	for _, tc := range openCases {
		t.Run(tc.name, func(t *testing.T) {
			err := chatlog.CheckRetainClosure(open, tc.retain)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

// openEntries is pairEntries with the last assistant holding a call whose
// result is not in the context yet.
func openEntries() []chatlog.Entry {
	entries := pairEntries()
	a2 := *entries[3].Assistant
	a2.CallIDs = []chatlog.CallID{"c2"}
	entries[3].Assistant = &a2
	return entries
}
