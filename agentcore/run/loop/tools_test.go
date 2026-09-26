package loop_test

import (
	"testing"

	"github.com/felinics/twilight/agentcore/run"
)

func TestModelCallCompletes(t *testing.T) {
	f := newFeature(t)
	f.Model(Text("hello"))
	f.Run()
	f.RequireCompleted("hello")
}

func TestToolRoundTripCompletes(t *testing.T) {
	f := newFeature(t)
	f.Tool("echo", run.DirectExecution)
	f.Model(ToolCalls("echo", "c1"), Text("done"))
	f.Run()
	f.RequireCompleted("done")
	f.RequireRan("echo")
	f.RequireModelCalls(2)
	f.RequireUsage(3)
	f.RequireFactOpened()
	f.RequireBuilderSawTool("c1", `{"x":1}`)
}

func TestKnownToolFailureContinues(t *testing.T) {
	f := newFeature(t)
	f.KnownFailure("echo", run.FailureExecution)
	f.Model(ToolCalls("echo", "c1"), Text("recovered"))
	f.Run()
	f.RequireCompleted("recovered")
	f.RequireFailureClass(run.FailureExecution)
}

func TestUnknownToolRefContinues(t *testing.T) {
	f := newFeature(t)
	f.Model(ToolCalls("ghost", "c1"), Text("moved on"))
	f.Run()
	f.RequireCompleted("moved on")
	f.RequireFailureClass(run.FailureToolLookup)
}
