package sessionkernel

import (
	"errors"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
	"strings"
	"testing"
)

// guardView is a writer.View that answers the surface projection with state
// and, when set, the machine projection with machine.
type guardView struct {
	state   any
	err     error
	machine any
}

func (guardView) Head() ledger.Head                                 { return ledger.Head{} }
func (guardView) Epoch() ledger.Epoch                               { return 0 }
func (guardView) Schema() module.PayloadVersion                     { return 1 }
func (guardView) Header() session.SegmentHeader                     { return session.SegmentHeader{} }
func (guardView) Committed(ledger.CommitID) (bool, error)           { return false, nil }
func (guardView) StreamHead(ledger.Domain) (ledger.StreamSeq, bool) { return 0, false }
func (guardView) LookupCommit(ledger.CommitID) (ledger.Commit, bool, error) {
	return ledger.Commit{}, false, nil
}
func (v guardView) Projection(id module.ProjectionID, _ module.ProjectionVersion) (any, error) {
	if id == sessionstore.MachineProjectionID {
		return v.machine, v.err
	}
	return v.state, v.err
}

func activeSurface(turnID turn.TurnID) sessionstore.TurnSurface {
	return sessionstore.TurnSurface{Order: []turn.TurnID{turnID}, Turns: map[turn.TurnID]sessionstore.TurnView{turnID: {TurnID: turnID, Status: turn.TurnActive}}}
}

func machineWith(current turn.Current) sessionstore.Machine {
	return sessionstore.Machine{Active: map[turn.TurnID]turn.MachineState{"r1": {TurnID: "r1", Status: turn.TurnActive, Current: current}}}
}

// The quiescent guard admits a Session between Turns and a Turn between
// steps, and refuses one inside a step.
func TestRequireQuiescentTurn(t *testing.T) {
	executing := turn.ToolStep{Calls: []turn.ToolCallState{{CallID: "c1", Status: turn.ToolExecuting}}}
	pending := turn.ToolStep{Calls: []turn.ToolCallState{{CallID: "c1", Status: turn.ToolPending}, {CallID: "c2", Status: turn.ToolWaiting}}}
	cases := map[string]struct {
		view guardView
		ok   bool
	}{
		"no turns":                          {view: guardView{state: sessionstore.TurnSurface{}}, ok: true},
		"completed turn":                    {view: guardView{state: surfaceWith(turn.TurnCompleted)}, ok: true},
		"active turn, run open":             {view: guardView{state: activeSurface("r1"), machine: machineWith(turn.Open{})}, ok: true},
		"active turn, no live run":          {view: guardView{state: activeSurface("r1"), machine: sessionstore.Machine{}}, ok: true},
		"tool step without executing calls": {view: guardView{state: activeSurface("r1"), machine: machineWith(pending)}, ok: true},
		"tool step with an executing call":  {view: guardView{state: activeSurface("r1"), machine: machineWith(executing)}},
		"model step prepared":               {view: guardView{state: activeSurface("r1"), machine: machineWith(turn.ModelStep{Status: turn.ModelPrepared})}},
		"model step executing":              {view: guardView{state: activeSurface("r1"), machine: machineWith(turn.ModelStep{Status: turn.ModelExecuting})}},
	}
	for name, tc := range cases {
		err := RequireQuiescentTurn(tc.view)
		if tc.ok && err != nil {
			t.Errorf("%s: err = %v, want nil", name, err)
		}
		if !tc.ok && !errors.Is(err, turn.ErrConflict) {
			t.Errorf("%s: err = %v, want conflict", name, err)
		}
	}
}

func surfaceWith(status turn.TurnStatus) sessionstore.TurnSurface {
	return sessionstore.TurnSurface{Order: []turn.TurnID{"t1"}, Turns: map[turn.TurnID]sessionstore.TurnView{"t1": {TurnID: "t1", Status: status}}}
}

func TestRequireNoActiveTurn(t *testing.T) {
	boom := errors.New("projection unavailable")
	cases := map[string]struct {
		view    guardView
		wantErr error  // errors.Is
		mention string // substring of the error
	}{
		"no turns":          {view: guardView{state: sessionstore.TurnSurface{}}},
		"completed turn":    {view: guardView{state: surfaceWith(turn.TurnCompleted)}},
		"active turn":       {view: guardView{state: surfaceWith(turn.TurnActive)}, wantErr: turn.ErrConflict, mention: "t1"},
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
