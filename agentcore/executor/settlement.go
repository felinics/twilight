package executor

import (
	"context"

	"github.com/felinics/twilight/agentcore/executor/notice"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// DefaultSettlementWindow is the number of settlements a Worker keeps for
// late subscribers.
const DefaultSettlementWindow = notice.DefaultWindow

// SettlementHub is the Worker's settlement notice log (RUN-EXE-17): the
// notice.Ring of effect.Settlement, the SettlementPort the Worker and the
// HTTP Server expose. One hub per Worker incarnation; every settlement the
// Worker records, whoever dispatched it, goes through it.
type SettlementHub struct {
	ring *notice.Ring[effect.Settlement]
}

// NewSettlementHub starts a hub for one Worker incarnation.
func NewSettlementHub(epoch string, window int) *SettlementHub {
	return &SettlementHub{ring: notice.NewRing[effect.Settlement](epoch, window)}
}

// Epoch names the incarnation.
func (h *SettlementHub) Epoch() string { return h.ring.Epoch() }

// Record announces that key settled.
func (h *SettlementHub) Record(key effect.AssignmentKey) {
	if h == nil {
		return
	}
	h.ring.Record(func(epoch string, sequence uint64) effect.Settlement {
		return effect.Settlement{Key: key, Epoch: epoch, Sequence: sequence}
	})
}

// Close ends the hub's subscriptions.
func (h *SettlementHub) Close() {
	if h != nil {
		h.ring.Close()
	}
}

// Settlements implements effect.SettlementPort: an announcement of the
// hub's epoch and head first (a Settlement with a zero Key), then every
// settled key (RUN-EXE-17).
func (h *SettlementHub) Settlements(ctx context.Context, epoch string, after uint64, fn func(effect.Settlement) bool) error {
	return h.ring.SubscribeAnnounced(ctx, epoch, after, func(e string, head uint64) effect.Settlement {
		return effect.Settlement{Epoch: e, Sequence: head}
	}, fn)
}

var _ effect.SettlementPort = (*SettlementHub)(nil)
