package turn

import (
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"strings"
	"testing"
)

// TRN-PRJ-1: run_ended settles the Turn whose started named the Run. A
// completed Run completes the Turn, any other end fails it; a Run no Turn of
// the Session names is ignored; a second end of the Run is a fold error
// rather than a silent no-op.
func TestSurfaceSettlesFromRunEnded(t *testing.T) {
	started := StartedPayload{TurnID: "t1", RunID: "r1"}
	ended := func(runID run.RunID, end run.RunEnd) sessionstore.Event {
		return sessionstore.Event{RunID: runID, Fact: run.RunEnded{End: end}}
	}
	completed, failed := run.RunCompletedEnd{}, run.RunFailedEnd{Reason: "provider"}
	cases := []struct {
		name       string
		events     []any
		wantStatus TurnStatus
		wantEnded  bool
		wantErr    string
	}{
		{"completed completes the turn", []any{started, ended("r1", completed)}, TurnCompleted, true, ""},
		{"failure fails the turn", []any{started, ended("r1", failed)}, TurnFailed, true, ""},
		{"a foreign run is ignored", []any{started, ended("r9", completed)}, TurnActive, false, ""},
		{"failure twice", []any{started, ended("r1", failed), ended("r1", failed)}, "", false, "ended twice"},
		{"completed after failure", []any{started, ended("r1", failed), ended("r1", completed)}, "", false, "ended twice"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, err := SurfaceProjection.Initial()
			if err != nil {
				t.Fatal(err)
			}
			for i, ev := range tc.events {
				if state, err = applySurface(state, module.DecodedEvent{Value: ev}); err != nil {
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
			if v.Status != tc.wantStatus || v.RunID != "r1" || (v.End != nil) != tc.wantEnded {
				t.Fatalf("view = %+v, want status %s ended=%v", v, tc.wantStatus, tc.wantEnded)
			}
		})
	}
}
