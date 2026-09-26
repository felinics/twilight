package turn

import (
	attemptmod "github.com/felinics/twilight/agentcore/session/attempt"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
)

// TRN-PRJ-1: run_ended settles the attempt attempt/started registered. A
// completed Run completes the Turn, any other end leaves it attempt_failed; a
// Run no Turn of the Session owns is ignored; a second end of one attempt is
// a fold error rather than a silent no-op.
func TestSurfaceSettlementNamesAnAttempt(t *testing.T) {
	started := StartedPayload{TurnID: "t1"}
	attempt := attemptmod.StartedPayload{TurnID: "t1", RunID: "r1", Attempt: 1}
	ended := func(runID run.RunID, end run.RunEnd) runmod.Event {
		return runmod.Event{RunID: runID, Fact: run.RunEnded{End: end}}
	}
	completed, failed := run.RunCompletedEnd{}, run.RunFailedEnd{Reason: "provider"}
	cases := []struct {
		name       string
		events     []any
		wantStatus TurnStatus
		wantEnded  bool
		wantErr    string
	}{
		{"completed ends the active attempt", []any{started, attempt, ended("r1", completed)}, TurnCompleted, true, ""},
		{"failure ends the active attempt", []any{started, attempt, ended("r1", failed)}, TurnAttemptFailed, true, ""},
		{"a foreign run is ignored", []any{started, attempt, ended("r9", completed)}, TurnActive, false, ""},
		{"failure twice", []any{started, attempt, ended("r1", failed), ended("r1", failed)}, "", false, "ended twice"},
		{"completed after the attempt failed", []any{started, attempt, ended("r1", failed), ended("r1", completed)}, "", false, "ended twice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, err := SurfaceProjection.Initial()
			if err != nil {
				t.Fatal(err)
			}
			for i, ev := range tc.events {
				if state, err = applySurface(state, extension.DecodedEvent{Value: ev}); err != nil {
					if i != len(tc.events)-1 {
						t.Fatalf("event %d: %v", i, err)
					}
					break
				}
			}
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("fold error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			v := state.(TurnSurface).Turns["t1"]
			if v.Status != tc.wantStatus || len(v.Attempts) != 1 || (v.Attempts[0].End != nil) != tc.wantEnded || (v.ActiveRun == "") != tc.wantEnded {
				t.Fatalf("view = %+v, want status %s ended=%v", v, tc.wantStatus, tc.wantEnded)
			}
		})
	}
}
