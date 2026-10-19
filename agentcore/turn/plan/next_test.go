package plan_test

import (
	"testing"

	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/plan"
)

// A fresh activation must respect capacity occupied by earlier activations.
func TestToolSchedulingCountsExecutingCalls(t *testing.T) {
	for _, tc := range []struct {
		name       string
		scheduling turn.ToolScheduling
		executing  int
		want       int
	}{
		{"sequential busy", turn.ToolScheduling{Mode: turn.ToolScheduleSequential}, 1, 0},
		{"sequential free", turn.ToolScheduling{Mode: turn.ToolScheduleSequential}, 0, 1},
		{"parallel full", turn.ToolScheduling{MaxParallel: 2}, 2, 0},
		{"parallel one slot", turn.ToolScheduling{MaxParallel: 2}, 1, 1},
		{"unbounded", turn.ToolScheduling{}, 2, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := []turn.ToolCallState{{CallID: "waiting", Status: turn.ToolWaiting}}
			for range tc.executing {
				calls = append(calls, turn.ToolCallState{Status: turn.ToolExecuting})
			}
			for _, id := range []turn.CallID{"a", "b", "c"} {
				calls = append(calls, turn.ToolCallState{CallID: id, Status: turn.ToolPending})
			}
			action, err := plan.Next(turn.MachineState{Status: turn.TurnActive, Current: turn.ToolStep{Calls: calls, Scheduling: tc.scheduling}})
			if err != nil {
				t.Fatal(err)
			}
			if tc.want == 0 {
				if _, ok := action.(plan.Idle); !ok {
					t.Fatalf("got %T, want Idle", action)
				}
				return
			}
			start, ok := action.(plan.StartToolCalls)
			if !ok || len(start.CallIDs) != tc.want || start.CallIDs[0] != "a" {
				t.Fatalf("action = %+v", action)
			}
		})
	}
}
