package canonical

import (
	"testing"

	"github.com/felinics/twilight/agentcore/run"
)

// TestDerivedIdentityGolden freezes every derived identity of RUN-WIR-4 for
// one fixed input set. A drift with a non-empty want means the derivation
// preimage changed: intentional protocol changes must update this fixture and
// agent-run.md; anything else is accidental drift.
func TestDerivedIdentityGolden(t *testing.T) {
	const (
		runID  run.RunID    = "run-1"
		step   run.StepID   = "step-1"
		call   run.CallID   = "call-1"
		effect run.EffectID = "effect-1"
	)
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"model request command", string((Identity{}).DeriveModelRequestCommandID(runID, 7)), "7e42f75006e3271ce7420647345f238ee59e9603d596e7907cb2ccadc281763d"},
		{"model step", string((Identity{}).DeriveModelStepID(runID, "cmd-1")), "883c5ef2aae151247361c311c41884dab6621d880aac6e3c285dcc2ad5ffa8e6"},
		{"tool call", string((Identity{}).DeriveCallID(step, 0)), "cca7fd2e5a02a565155747fd8c126ac497a425d2f627eb2c77605dc0551aea11"},
		{"tool step", string((Identity{}).DeriveToolStepID(step)), "6f62a482a83239b9c324a73c21b0b671ef2cad0af94a30177ed62929467d419d"},
		{"response id", string((Identity{}).DeriveResponseID(runID, step, call, run.ResponseApproval)), "819924b49cc24df66e97f5821ac80bf286b983e600f20e25f045142c4bc0e77e"},
		{"response command", string((Identity{}).DeriveResponseCommandID(runID, step, call, "resp-1")), "aacf7d247a13e4a696bff6f881ec517a6ad95ae262ec862d7dcf84b80c6bb32b"},
		{"input command", string((Identity{}).DeriveInputCommandID(runID, "in-1")), "ce57abe29d2da34e02cf8fbfb7e4a25086b352096977094e9bed7eb66dd16aad"},
		{"withdraw command", string((Identity{}).DeriveWithdrawCommandID(runID, step)), "582ffed8e0d762e83d68e0eef5c90f123e03787aad8462766667133d15961a8c"},
		{"model effect", string((Identity{}).DeriveEffectID(runID, step, "", 1)), "553f030a642d4f74a09c2b11620e464f56f98711a97b522f82cc15771ccc3394"},
		{"tool effect", string((Identity{}).DeriveEffectID(runID, step, call, 0)), "20fd10fde6bbb31634d80836d50906afc234e3676cfefb00a9abdb0fb996be78"},
		{"start command", string((Identity{}).DeriveStartCommandID(effect)), "5fbd4523f88a2e24369724c7829e378883a83851f4164b78d0cd5d07c790fd3b"},
		{"settlement command", string((Identity{}).DeriveSettlementCommandID(effect)), "6e05abc3045f404dfe76216a9bd84ec8b6ca0294177d60e9695ebd354446a01d"},
		{"recovery command", string((Identity{}).DeriveRecoveryCommandID(effect)), "3059f690e0e819d6f1f08bdfe09f22c4012d1d284ee90c02c3e50175102d6735"},
		{"decline command", string((Identity{}).DeriveDeclineCommandID(runID, step, call)), "c5eab65b0a9a2b828d18aed6ecc3655eff37cf55b275eb5fd15f26367637d49a"},
	}
	for _, c := range cases {
		if c.got == c.want {
			continue
		}
		if c.want == "" {
			t.Errorf("UNSET %s = %s", c.name, c.got)
			continue
		}
		t.Errorf("golden %s drifted — an intentional derivation change must update this fixture and agent-run.md:\n got: %s\nwant: %s", c.name, c.got, c.want)
	}
}
