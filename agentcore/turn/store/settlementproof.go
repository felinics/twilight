package store

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agentcore/turn"
)

// ErrEffectProofUnsupported means a TurnStore cannot prove durable effect
// disposition. It must not be interpreted as absence of a settlement.
var ErrEffectProofUnsupported = errors.New("agent: turn store: durable effect proof unsupported")

// EffectDispositionReader is an optional read-only capability next to
// TurnStore. It speaks only Turn types. EffectDisposition returns committed
// closing facts naming exactly this Turn and EffectID, from the durable Turn
// ledger or an index of it, never from an active-state projection. found is
// false only for proven absence; read failures are errors, not absence. Facts
// are detached and proof survives a terminal Turn leaving the projection.
//
// Proof is by effect fact, not only a derived settlement CommandID: CancelTurn
// can close several effects in one differently named commit. A recovery or
// cancellation disposition does not prove execution terminated, so callers
// must also close/probe the execution before allowing Outcome collection.
type EffectDispositionReader interface {
	EffectDisposition(context.Context, turn.TurnID, turn.EffectID) (facts []turn.Fact, found bool, err error)
}

// DispositionEffect names the effect a closing fact settles or recovers.
// Starts, TurnEnded, and facts about effects that never started return empty.
func DispositionEffect(fact turn.Fact) turn.EffectID {
	switch f := fact.(type) {
	case turn.ModelStepCompleted:
		return f.Effect
	case turn.ModelStepFailed:
		return f.Effect
	case turn.ModelStepRejected:
		return f.Effect
	case turn.ModelStepRecovered:
		return f.Effect
	case turn.ToolCallCompleted:
		return f.Effect
	case turn.ToolCallFailed:
		return f.Effect
	default:
		return ""
	}
}
