package sessionstore

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/schema"
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
	lookup, indexed := b.w.(interface {
		LookupCommit(ledger.CommitID) (ledger.Commit, bool, error)
	})
	if indexed {
		// Most effects settle under a derived command identity. The Writer's
		// commit index locates those durable facts without replaying the Turn's
		// entire history. A lookup hit alone is never proof: check local lineage,
		// stream attribution, TurnID and the effect-naming closing fact.
		c, ok, err := lookup.LookupCommit(ledger.CommitID(schema.Identity().DeriveSettlementCommandID(effectID)))
		if err != nil {
			return nil, false, err
		}
		if ok && c.Seq >= b.w.Header().Seed().Next {
			facts, found, err := b.dispositionIn(c, turnID, effectID)
			if err != nil || found {
				return facts, found, err
			}
		}
		// CancelTurn and recovery can use other command identities. A verified
		// local start bounds that scan; immutable history before the start cannot
		// contain this execution's disposition. Without a usable start, retain the
		// full own-stream scan for older/imported histories.
		c, ok, err = lookup.LookupCommit(ledger.CommitID(schema.Identity().DeriveStartCommandID(effectID)))
		if err != nil {
			return nil, false, err
		}
		if ok && c.Seq >= b.w.Header().Seed().Next {
			started, err := b.startsEffect(c, turnID, effectID)
			if err != nil {
				return nil, false, err
			}
			if started {
				return b.dispositionAfter(ctx, turnID, effectID, c.Seq)
			}
		}
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

// dispositionIn checks authoritative facts, never a cached projection or a
// retained local command result.
func (b *bound) dispositionIn(c ledger.Commit, turnID turn.TurnID, effectID turn.EffectID) ([]turn.Fact, bool, error) {
	for _, batch := range c.Batches {
		if batch.Domain != Stream(turnID) {
			continue
		}
		for _, event := range batch.Events {
			decoded, err := b.s.cfg.Registry.Decode(event)
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
	}
	return nil, false, nil
}

func (b *bound) startsEffect(c ledger.Commit, turnID turn.TurnID, effectID turn.EffectID) (bool, error) {
	for _, batch := range c.Batches {
		if batch.Domain != Stream(turnID) {
			continue
		}
		for _, event := range batch.Events {
			decoded, err := b.s.cfg.Registry.Decode(event)
			if err != nil {
				return false, err
			}
			ev, ok := decoded.Value.(Event)
			if !ok || ev.TurnID != turnID {
				return false, errors.New("sessionstore: effect proof start does not belong to the Turn")
			}
			switch fact := ev.Fact.(type) {
			case turn.ModelStepStarted:
				if fact.Effect == effectID {
					return true, nil
				}
			case turn.ToolCallStarted:
				if fact.Effect == effectID {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

func (b *bound) dispositionAfter(ctx context.Context, turnID turn.TurnID, effectID turn.EffectID, from ledger.CommitSeq) ([]turn.Fact, bool, error) {
	for {
		page, err := b.s.cfg.Store.ReadCommits(ctx, session.CommitReadRequest{SessionID: b.w.SessionID(), From: from, Limit: 128})
		if err != nil {
			return nil, false, err
		}
		for _, c := range page.Commits {
			if c.Seq < page.Header.Seed().Next {
				continue
			}
			facts, found, err := b.dispositionIn(c, turnID, effectID)
			if err != nil || found {
				return facts, found, err
			}
		}
		if !page.HasMore {
			return nil, false, nil
		}
		if len(page.Commits) == 0 || page.Commits[len(page.Commits)-1].Seq < from {
			return nil, false, errors.New("sessionstore: effect proof commit page did not advance")
		}
		from = page.Commits[len(page.Commits)-1].Seq + 1
	}
}
