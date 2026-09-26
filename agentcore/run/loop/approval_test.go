package loop_test

import (
	"testing"

	"github.com/felinics/twilight/agentcore/run"
)

func TestApprovalWaitsThenResumes(t *testing.T) {
	f := newFeature(t)
	f.Tool("echo", run.ApprovalRequired)
	f.Model(ToolCalls("echo", "c1"), Text("after"))
	f.Run()
	f.RequireWaiting(run.ResponseApproval)
	f.RequireNotRan("echo")
	w := f.Waiting()
	if err := f.TryCommit(run.ApproveToolCall{
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
	f.Tool("echo", run.ApprovalRequired)
	f.Model(ToolCalls("echo", "c1"))
	f.Run()
	f.RequireWaiting(run.ResponseApproval)
	f.Reject("no")
	f.RequireActive()
	f.RequireOpen()
	f.RequireFailureClass(run.FailurePermissionDenied)
	f.RequireNotRan("echo")
}

func TestApprovalYieldsAfterDirectExecution(t *testing.T) {
	f := newFeature(t)
	f.Tool("ask", run.ApprovalRequired)
	f.Tool("work", run.DirectExecution)
	f.Model(Calls(Call("ask", "cA"), Call("work", "cB")), Text("after"))
	f.Run()
	f.RequireWaiting(run.ResponseApproval)
	f.RequireWaitingProvider("cA")
	f.RequireRan("work")
	f.RequireNotRan("ask")
	f.Approve()
	f.Run()
	f.RequireCompleted("after")
	f.RequireRan("ask")
}
