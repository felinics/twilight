package chatlog

import (
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/session"
)

// APP-CKP-1: compaction identifiers are a function of the Session, the base
// context digest and the summary text, so a Compaction retried over the same
// base replays the same CommitID.
func TestCompactionIDsAreDerived(t *testing.T) {
	base := es.DigestBytes([]byte("base"))
	ckpt, sum, err := compactionIDs("s", base, "summary")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(ckpt), "ckpt-") || !strings.HasPrefix(string(sum), "sum-") ||
		strings.TrimPrefix(string(ckpt), "ckpt-") != strings.TrimPrefix(string(sum), "sum-") {
		t.Fatalf("ids = %s, %s; want one derived suffix behind both prefixes", ckpt, sum)
	}
	cases := []struct {
		name    string
		sid     session.SessionID
		base    es.Digest
		summary string
		same    bool
	}{
		{"same inputs", "s", base, "summary", true},
		{"other session", "s2", base, "summary", false},
		{"other base", "s", es.DigestBytes([]byte("other")), "summary", false},
		{"other summary", "s", base, "summary 2", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, _, err := compactionIDs(tc.sid, tc.base, tc.summary)
			if err != nil {
				t.Fatal(err)
			}
			if (got == ckpt) != tc.same {
				t.Fatalf("compactionIDs = %s; equal to %s is %v, want %v", got, ckpt, got == ckpt, tc.same)
			}
		})
	}
}
