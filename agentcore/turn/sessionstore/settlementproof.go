package sessionstore

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/store"
)

var _ store.EffectDispositionReader = (*bound)(nil)

// EffectDisposition reads effect-naming closing facts from the durable Turn
// stream. It also finds dispositions in CancelTurn commits, whose CommandID is
// not an effect-derived settlement ID. Active projections and retained local
// CommitResults are never consulted; terminal Turns remain provable.
func (b *bound) EffectDisposition(ctx context.Context, turnID turn.TurnID, effectID turn.EffectID) ([]turn.Fact, bool, error) {
	if err := store.CheckContext(ctx); err != nil {
		return nil, false, err
	}
	if turnID == "" || effectID == "" {
		return nil, false, errors.New("sessionstore: effect proof requires TurnID and EffectID")
	}
	var from ledger.StreamSeq
	for {
		// This proof reads module.Own even though history is inherited:
		// ancestor dispositions cannot prove this Session's execution.
		page, err := b.s.cfg.Store.ReadStream(ctx, session.StreamReadRequest{SessionID: b.w.SessionID(), Domain: Stream(turnID), Inheritance: module.Own, From: from, Limit: 128})
		if err != nil {
			return nil, false, err
		}
		for i := range page.Events {
			decoded, err := b.s.cfg.Registry.Decode(page.Events[i])
			if err != nil {
				return nil, false, err
			}
			ev, ok := decoded.Value.(Event)
			if !ok || ev.TurnID != turnID {
				return nil, false, errors.New("sessionstore: effect proof event does not belong to the Turn")
			}
			if store.DispositionEffect(ev.Fact) == effectID {
				return []turn.Fact{ev.Fact}, true, nil
			}
		}
		if !page.HasMore {
			return nil, false, nil
		}
		if len(page.Events) == 0 {
			return nil, false, errors.New("sessionstore: effect proof stream page did not advance")
		}
		from += ledger.StreamSeq(len(page.Events))
	}
}
