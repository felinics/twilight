package runmod

import (
	"errors"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// guardView is a writer.View whose only answer is the machine projection.
type guardView struct {
	state any
	err   error
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
func (v guardView) Projection(extension.ProjectionID, extension.ProjectionVersion) (any, error) {
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
