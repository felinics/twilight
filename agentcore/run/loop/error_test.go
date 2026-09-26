package loop_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agentcore/run/loop"
)

func TestCatalogResolveErrorLeavesRunActive(t *testing.T) {
	missing := errors.New("missing provider")
	f := newFeature(t)
	f.ModelResolveError(missing)
	// Validate finds the missing model before the start barrier: no start or
	// recovery fact, the step stays Prepared for a later drive (RUN-EXE-5).
	f.RunError(loop.ErrModelUnavailable)
	f.RequireActive()
	f.RequirePrepared()
}

func TestContextCancelBeforeRunLeavesRunActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	f := newFeature(t)
	f.Model(Text("resumed"))
	f.Context(ctx)
	f.RunError(context.Canceled)
	f.RequireActive()
	f.Context(context.Background())
	f.Run()
	f.RequireCompleted("resumed")
}
