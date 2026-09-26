package turn

import (
	"errors"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	runmod "github.com/felinics/twilight/agentcore/session/run"
)

// guardView is a writer.View that answers the surface projection with state
// and, when set, the machine projection with machine.
type guardView struct {
	state   any
	err     error
	machine any
}

func (guardView) Head() session.Head                                     { return session.Head{} }
func (guardView) Epoch() session.Epoch                                   { return 0 }
func (guardView) Schema() extension.PayloadVersion                       { return 1 }
func (guardView) Header() session.SegmentHeader                          { return session.SegmentHeader{} }
func (guardView) Committed(session.CommitID) bool                        { return false }
func (guardView) StreamHead(session.StreamRef) (session.StreamSeq, bool) { return 0, false }
func (guardView) LookupCommit(session.CommitID) (session.Commit, bool, error) {
	return session.Commit{}, false, nil
}
func (v guardView) Projection(id extension.ProjectionID, _ extension.ProjectionVersion) (any, error) {
	if id == runmod.MachineProjectionID {
		return v.machine, v.err
	}
	return v.state, v.err
}

func activeSurface(runID run.RunID) TurnSurface {
	return TurnSurface{Order: []TurnID{"t1"}, Turns: map[TurnID]TurnView{"t1": {TurnID: "t1", Status: TurnActive, ActiveRun: runID}}}
}

func machineWith(current run.Current) runmod.Machine {
	return runmod.Machine{Active: map[run.RunID]run.MachineState{"r1": {RunID: "r1", Status: run.RunActive, Current: current}}}
}

// APP-CKP-1: the quiescent guard admits a Session between Turns and a Turn
// between steps, and refuses one inside a step.
func TestRequireQuiescentRun(t *testing.T) {
	executing := run.ToolStep{Calls: []run.ToolCallState{{CallID: "c1", Status: run.ToolExecuting}}}
	pending := run.ToolStep{Calls: []run.ToolCallState{{CallID: "c1", Status: run.ToolPending}, {CallID: "c2", Status: run.ToolWaiting}}}
	cases := map[string]struct {
		view guardView
		ok   bool
	}{
		"no turns":                          {view: guardView{state: TurnSurface{}}, ok: true},
		"completed turn":                    {view: guardView{state: surfaceWith(TurnCompleted)}, ok: true},
		"active turn, run open":             {view: guardView{state: activeSurface("r1"), machine: machineWith(run.Open{})}, ok: true},
		"active turn, no live run":          {view: guardView{state: activeSurface("r1"), machine: runmod.Machine{}}, ok: true},
		"tool step without executing calls": {view: guardView{state: activeSurface("r1"), machine: machineWith(pending)}, ok: true},
		"tool step with an executing call":  {view: guardView{state: activeSurface("r1"), machine: machineWith(executing)}},
		"model step prepared":               {view: guardView{state: activeSurface("r1"), machine: machineWith(run.ModelStep{Status: run.ModelPrepared})}},
		"model step executing":              {view: guardView{state: activeSurface("r1"), machine: machineWith(run.ModelStep{Status: run.ModelExecuting})}},
	}
	for name, tc := range cases {
		err := RequireQuiescentRun(tc.view)
		if tc.ok && err != nil {
			t.Errorf("%s: err = %v, want nil", name, err)
		}
		if !tc.ok && !errors.Is(err, ErrConflict) {
			t.Errorf("%s: err = %v, want conflict", name, err)
		}
	}
}

func surfaceWith(status TurnStatus) TurnSurface {
	return TurnSurface{Order: []TurnID{"t1"}, Turns: map[TurnID]TurnView{"t1": {TurnID: "t1", Status: status}}}
}

func TestRequireNoActiveTurn(t *testing.T) {
	boom := errors.New("projection unavailable")
	cases := map[string]struct {
		view    guardView
		wantErr error  // errors.Is
		mention string // substring of the error
	}{
		"no turns":          {view: guardView{state: TurnSurface{}}},
		"completed turn":    {view: guardView{state: surfaceWith(TurnCompleted)}},
		"active turn":       {view: guardView{state: surfaceWith(TurnActive)}, wantErr: ErrConflict, mention: "t1"},
		"projection failed": {view: guardView{err: boom}, wantErr: boom},
		"foreign state":     {view: guardView{state: "nope"}, mention: "string"},
	}
	for name, tc := range cases {
		err := RequireNoActiveTurn(tc.view)
		if tc.wantErr == nil && tc.mention == "" {
			if err != nil {
				t.Errorf("%s: err = %v, want nil", name, err)
			}
			continue
		}
		if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
			t.Errorf("%s: err = %v, want %v", name, err, tc.wantErr)
		}
		if tc.mention != "" && (err == nil || !strings.Contains(err.Error(), tc.mention)) {
			t.Errorf("%s: err = %v, want one mentioning %q", name, err, tc.mention)
		}
	}
}
