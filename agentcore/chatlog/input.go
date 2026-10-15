package chatlog

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agentcore/session/unit"
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn"
)

// SubmitInput contributes input admission to a cross-module unit, for example
// an agent's atomic command receipt. Its caller supplies the stable operation
// identity; this module alone constructs the chatlog fact. It starts no Turn.
func SubmitInput(id turn.InputID, content turn.CanonicalJSON) unit.Part {
	return SubmitInputWithPlacement(id, content, PlacementNextTurn)
}

// SubmitInputWithPlacement contributes a placement-aware input admission to a unit.
func SubmitInputWithPlacement(id turn.InputID, content turn.CanonicalJSON, placement Placement) unit.Part {
	return unit.PartFunc(func(_ context.Context, _ writer.View, now int64) ([]writer.TypedBatch, error) {
		if id == "" || content.IsZero() || !placement.Valid() {
			return nil, errors.New("chatlog: input identity, content and valid placement are required")
		}
		return []writer.TypedBatch{{Domain: Stream, Events: []writer.TypedEvent{{
			Type: TypeInputSubmitted, RecordedAtUnixMilli: now,
			Value: InputSubmittedPayload{InputID: InputID(id), Content: content, Placement: placement, SubmittedAtUnixMilli: now},
		}}}}, nil
	})
}
