package executor

import (
	"context"

	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/turn/effect"
)

var _ effect.OutstandingPort = (*Worker)(nil)

// ListOutstanding exposes the store's durable management directory without
// inspecting backends or changing executions. A store lacking Directory is
// an explicit capability failure, never an apparently successful empty scan.
func (w *Worker) ListOutstanding(ctx context.Context, after effect.AssignmentKey, limit int) (effect.OutstandingPage, error) {
	directory, ok := w.store.(executionstore.Directory)
	if !ok {
		return effect.OutstandingPage{}, effect.ErrOutstandingUnsupported
	}
	return directory.ListOutstanding(ctx, after, limit)
}
