package loop_test

import (
	"testing"

	"github.com/felinics/twilight/agentcore/turn"
)

func TestApprovalWaitsThenResumes(t *testing.T) {
	f := newFeature(t)
	f.Tool("echo", turn.ApprovalRequired)
	f.Model(ToolCalls("echo", "c1"), Text("after"))
	f.Run()
	f.RequireWaiting(turn.ResponseApproval)
	f.RequireNotRan("echo")
	w := f.Waiting()
	if err := f.TryCommit(turn.ApproveToolCall{
		StepID: w.StepID, CallID: w.CallID, ResponseID: w.ID, ResponseDigest: "sha256:bad",
	}); err == nil {
		t.Fatal("approval with bad response digest accepted")
	}
	f.Approve()
	f.RequireCallPending("c1")
	f.Run()
	f.RequireCompleted("after")
	f.RequireRan("echo")
}

func TestApprovalRejectIsPermissionDenied(t *testing.T) {
	f := newFeature(t)
	f.Tool("echo", turn.ApprovalRequired)
	f.Model(ToolCalls("echo", "c1"))
	f.Run()
	f.RequireWaiting(turn.ResponseApproval)
	f.Reject("no")
	f.RequireActive()
	f.RequireOpen()
	f.RequireFailureClass(turn.FailurePermissionDenied)
	f.RequireNotRan("echo")
}

func TestApprovalYieldsAfterDirectExecution(t *testing.T) {
	f := newFeature(t)
	f.Tool("ask", turn.ApprovalRequired)
	f.Tool("work", turn.DirectExecution)
	f.Model(Calls(Call("ask", "cA"), Call("work", "cB")), Text("after"))
	f.Run()
	f.RequireWaiting(turn.ResponseApproval)
	f.RequireWaitingProvider("cA")
	f.RequireRan("work")
	f.RequireNotRan("ask")
	f.Approve()
	f.Run()
	f.RequireCompleted("after")
	f.RequireRan("ask")
}
