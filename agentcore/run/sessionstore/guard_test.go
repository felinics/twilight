package sessionstore

import (
	"errors"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"strings"
	"testing"
)

// guardView is a writer.View whose only answer is the machine projection.
type guardView struct {
	state any
	err   error
}

func (guardView) Head() ledger.Head                                 { return ledger.Head{} }
func (guardView) Epoch() ledger.Epoch                               { return 0 }
func (guardView) Schema() module.PayloadVersion                     { return module.Pre(1) }
func (guardView) Header() session.SegmentHeader                     { return session.SegmentHeader{} }
func (guardView) Committed(ledger.CommitID) (bool, error)           { return false, nil }
func (guardView) StreamHead(ledger.Domain) (ledger.StreamSeq, bool) { return 0, false }
func (guardView) LookupCommit(ledger.CommitID) (ledger.Commit, bool, error) {
	return ledger.Commit{}, false, nil
}
func (v guardView) Projection(module.ProjectionID, module.ProjectionVersion) (any, error) {
	return v.state, v.err
}

func TestRequireNoActiveRun(t *testing.T) {
	boom := errors.New("projection unavailable")
	active := newMachine()
	active.Active["r-1"] = run.MachineState{}
	cases := map[string]struct {
		view    guardView
		wantErr error  // errors.Is
		mention string // substring of the error
	}{
		"idle":              {view: guardView{state: newMachine()}},
		"active run":        {view: guardView{state: active}, mention: "r-1"},
		"projection failed": {view: guardView{err: boom}, wantErr: boom},
		"foreign state":     {view: guardView{state: "nope"}, mention: "string"},
	}
	for name, tc := range cases {
		err := RequireNoActiveRun(tc.view)
		switch {
		case tc.wantErr == nil && tc.mention == "":
			if err != nil {
				t.Errorf("%s: err = %v, want nil", name, err)
			}
		case tc.wantErr != nil:
			if !errors.Is(err, tc.wantErr) {
				t.Errorf("%s: err = %v, want %v", name, err, tc.wantErr)
			}
		default:
			if err == nil || !strings.Contains(err.Error(), tc.mention) {
				t.Errorf("%s: err = %v, want one mentioning %q", name, err, tc.mention)
			}
		}
	}
}
