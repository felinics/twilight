package chatlog

import (
	"context"
	"errors"
	"fmt"

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
	return unit.PartFunc(func(_ context.Context, view writer.View, now int64) ([]writer.TypedBatch, error) {
		if id == "" || content.IsZero() || !placement.Valid() {
			return nil, errors.New("chatlog: input identity, content and valid placement are required")
		}

		state, err := view.Projection(SurfaceProjectionID, SurfaceProjection.Version)
		if err != nil {
			return nil, err
		}
		surface, ok := state.(Surface)
		if !ok {
			return nil, fmt.Errorf("chatlog: surface projection is %T", state)
		}
		if old, ok := surface.Inputs.Get(InputID(id)); ok {
			digest, err := DigestInput(InputID(id), content)
			if err != nil {
				return nil, err
			}
			if old.Input.Digest != digest || old.Input.Placement.Effective() != placement {
				return nil, fmt.Errorf("%w: %s", ErrInputConflict, id)
			}
			return nil, nil
		}
		return []writer.TypedBatch{{Domain: Stream, Events: []writer.TypedEvent{{
			Type: TypeInputSubmitted, RecordedAtUnixMilli: now,
			Value: InputSubmittedPayload{InputID: InputID(id), Content: content, Placement: placement, SubmittedAtUnixMilli: now},
		}}}}, nil
	})
}

// WithdrawInput contributes a withdrawal to an atomic application command.
func WithdrawInput(id turn.InputID, reason string) unit.Part {
	return unit.PartFunc(func(_ context.Context, v writer.View, at int64) ([]writer.TypedBatch, error) {
		state, err := v.Projection(SurfaceProjectionID, SurfaceProjection.Version)
		if err != nil {
			return nil, err
		}
		surface, ok := state.(Surface)
		if !ok {
			return nil, fmt.Errorf("chatlog: surface projection is %T", state)
		}
		in, ok := surface.Inputs.Get(InputID(id))
		if !ok || in.Status != InputSubmitted {
			return nil, ErrNotSubmitted
		}
		return []writer.TypedBatch{{Domain: Stream, Events: []writer.TypedEvent{{Type: TypeInputWithdrawn, RecordedAtUnixMilli: at, Value: InputWithdrawnPayload{InputID: InputID(id), Reason: reason}}}}}, nil
	})
}
