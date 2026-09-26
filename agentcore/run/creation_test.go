package run

import (
	"testing"

	"github.com/felinics/twilight/agentcore/es"
)

func mustNewRun(t testing.TB, id RunID, cause es.CausationID) NewRun {
	t.Helper()
	run, err := BuildNewRun(id, cause)
	if err != nil {
		t.Fatal(err)
	}
	return run
}

func TestNewRunValidation(t *testing.T) {
	created := mustNewRun(t, "run-1", "session-1")
	if created.RunID != "run-1" {
		t.Fatalf("run id = %q", created.RunID)
	}
	for _, candidate := range []NewRun{
		{},
		{RunID: RunID(string([]byte{0xff}))},
		{RunID: "run-1", CausationID: es.CausationID(string([]byte{0xff}))},
	} {
		if err := ValidateNewRun(candidate); err == nil {
			t.Fatalf("invalid NewRun accepted: %+v", candidate)
		}
	}
}
