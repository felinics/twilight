package turn_test

import (
	"testing"

	"github.com/felinics/twilight/agentcore/es"
	"github.com/felinics/twilight/agentcore/session/chatlog"
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
	plan := turn.PlanDigest("turn-1", es.Digest("sha256:aa"), []chatlog.InputID{"in-1", "in-2"})
	freezeTurn(t, "plan digest", string(plan), "sha256:5194f3d5319f06e2362c816ca4dded69e8b9c30298c067c4508248f33a8d94a4")

	op := turn.StartOperationDigest("sess-1", "turn-1", plan)
	freezeTurn(t, "start operation digest", string(op), "sha256:26e6b4f4a9c416dac8a6dcb4d24eb40b3c707d1f0b5a6e0fd9de3a1bbbf4ea8f")

	freezeTurn(t, "run id attempt 1", string(turn.DeriveRunID("sess-1", "turn-1", 1)), "sha256:fdf7b80ce8c610086353146e4ca239374f6cbb249724cca6bc772895404ad682")
	freezeTurn(t, "run id attempt 2", string(turn.DeriveRunID("sess-1", "turn-1", 2)), "sha256:9ad90f0fefdd75e7622a3966defc5f637a477bdf43214de5de461fbf57301e6f")
}
