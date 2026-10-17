package store

import (
	"context"

	"github.com/felinics/twilight/agentcore/turn/effect"
)

// Directory is the optional durable management index of executions. It is
// separate from Store's per-key ledger/lease operations and has precisely the
// semantics of effect.OutstandingPort: tuple-sorted exclusive keyset pages,
// all nonterminal acceptances and terminal unacknowledged Outcomes, excluding
// aborted and acknowledged records. Each summary joins the ledger head and
// current lease consistently; expiry never removes an execution from the
// directory. Implementations must retain the Outcome until safe ACK.
type Directory interface {
	ListOutstanding(ctx context.Context, after effect.AssignmentKey, limit int) (effect.OutstandingPage, error)
}
