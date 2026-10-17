package turn

import (
	"github.com/felinics/twilight/agentcore/preset/ref"
	"testing"

	"github.com/felinics/twilight/agentcore/ledger"
)

func mustNewTurn(t testing.TB, id TurnID, cause ledger.CausationID) NewTurn {
	t.Helper()
	run, err := BuildNewTurn(id, cause, ref.Ref{ID: "test", Digest: "sha256:test"})
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func TestNewRunValidation(t *testing.T) {
	created := mustNewTurn(t, "run-1", "session-1")
	if created.TurnID != "run-1" {
		t.Fatalf("run id = %q", created.TurnID)
	}
	for _, candidate := range []NewTurn{
		{},
		{TurnID: TurnID(string([]byte{0xff}))},
		{TurnID: "run-1", CausationID: ledger.CausationID(string([]byte{0xff}))},
	} {
		if err := ValidateNewTurn(candidate); err == nil {
			t.Fatalf("invalid NewTurn accepted: %+v", candidate)
		}
	}
}
