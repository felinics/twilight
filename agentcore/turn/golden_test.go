package turn_test

import (
	"testing"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/turn"
)

func freezeTurn(t *testing.T, name, got, want string) {
	t.Helper()
	if got == want {
		return
	}
	if want == "" {
		t.Errorf("UNSET %s = %s", name, got)
		return
	}
	t.Errorf("golden %s drifted — an intentional wire change must update this fixture and agent-turn.md:\n got: %s\nwant: %s", name, got, want)
}

// TestTurnDerivationGolden freezes the Turn's derived identities (TRN-ID-1/2).
func TestTurnDerivationGolden(t *testing.T) {
	plan := turn.PlanDigest("turn-1", jsonstable.Digest("sha256:aa"), []chatlog.InputID{"in-1", "in-2"})
	freezeTurn(t, "plan digest", string(plan), "sha256:5194f3d5319f06e2362c816ca4dded69e8b9c30298c067c4508248f33a8d94a4")

	op := turn.StartOperationDigest("sess-1", "turn-1", plan)
	freezeTurn(t, "start operation digest", string(op), "sha256:26e6b4f4a9c416dac8a6dcb4d24eb40b3c707d1f0b5a6e0fd9de3a1bbbf4ea8f")

	freezeTurn(t, "run id", string(turn.DeriveRunID("sess-1", "turn-1")), "sha256:46a41c9b32ce837aa0d4910bbc47a86150a0c27fd120a7432ed5558a3f854601")
}
