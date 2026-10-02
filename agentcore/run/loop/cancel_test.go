package loop_test

import (
	"testing"

	"github.com/felinics/twilight/agentcore/run"
)

func TestCancelStopsIdleRun(t *testing.T) {
	f := newFeature(t)
	f.Cancel()
	f.RequireStopped()
	f.RequireNoUncertain()
	f.Run()
	f.RequireStopped()
}

func TestCancelProjectsExecutingTool(t *testing.T) {
	f := newFeature(t)
	f.Tool("echo", run.DirectExecution)
	f.ExecutingTool("echo", "c1")
	f.Cancel()
	f.RequireStopped()
	f.RequireUncertainCall("c1")
}

func TestCancelProjectsExecutingModel(t *testing.T) {
	f := newFeature(t)
	f.ExecutingModel()
	f.Cancel()
	f.RequireStopped()
	f.RequireUncertainModel()
}
