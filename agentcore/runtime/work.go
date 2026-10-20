package runtime

import (
	"context"
	"fmt"

	"github.com/felinics/twilight/agentcore/chatlog"
	"github.com/felinics/twilight/agentcore/compaction"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/agentcore/turn/effect"
	"github.com/felinics/twilight/agentcore/turn/plan"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
)

// Work is a reconstructible read model, not an Activation's durable state.
// Ready requires immediate computation. Pending requires an independent probe
// or external wakeup. Human responses alone require neither a busy loop nor
// a permanently owned Session.
type Work struct {
	Revision   uint64
	Ready      bool
	Pending    bool
	Executions []effect.AssignmentKey
	Responses  []turn.ResponseRequest
}

// Inspect uses lease-free reads. Projections can be read at different heads:
// the minimum observed head is the conservative discovery watermark, never
// the maximum (which could acknowledge work a stale projection has not seen).
// Hosts re-scan; only the Writer's transactional view decides semantic changes.
func Inspect(ctx context.Context, reader session.ProjectionReader, sid session.SessionID) (Work, error) {
	states, err := session.LoadProjections(ctx, reader, sid,
		session.ProjectionRef{ID: sessionstore.MachineProjectionID, Version: sessionstore.MachineProjection.Version},
		session.ProjectionRef{ID: chatlog.PendingProjectionID, Version: chatlog.PendingProjection.Version},
		session.ProjectionRef{ID: compaction.ProjectionID, Version: compaction.Projection.Version},
	)
	if err != nil {
		return Work{}, err
	}
	value, head := states[0].State, states[0].Through
	machine, ok := value.(sessionstore.Machine)
	if !ok {
		return Work{}, fmt.Errorf("runtime: machine projection is %T", value)
	}
	work := Work{Revision: uint64(head.Next)}
	value, head = states[1].State, states[1].Through
	work.Revision = min(work.Revision, uint64(head.Next))
	chat, ok := value.(chatlog.Pending)
	if !ok {
		return Work{}, fmt.Errorf("runtime: pending input projection is %T", value)
	}
	work.Ready = len(machine.Active) == 0 && chat.Inputs.Len() > 0
	for id := range machine.Active {
		state := machine.Active[id]
		action, err := plan.Next(state)
		if err != nil {
			return Work{}, err
		}
		if _, idle := action.(plan.Idle); !idle {
			work.Ready = true
		}
		for _, target := range plan.RecoveryTargets(&state) {
			work.Executions = append(work.Executions, effect.AssignmentKey{Session: turn.Scope(sid), OwnerKind: effect.OwnerTurn, OwnerID: string(id), Effect: target.Effect})
			work.Pending = true
		}
		work.Responses = append(work.Responses, turn.WaitingCalls(state)...)
	}
	value, head = states[2].State, states[2].Through
	work.Revision = min(work.Revision, uint64(head.Next))
	maintenance, ok := value.(compaction.State)
	if !ok {
		return Work{}, fmt.Errorf("runtime: compaction projection is %T", value)
	}
	for id := range maintenance.Operations {
		op := maintenance.Operations[id]
		if op.Status == compaction.Pending {
			work.Pending = true
			work.Ready = false // ordinary preparation remains gated
			work.Executions = append(work.Executions, op.Request.Key(sid))
		} else if !op.Acknowledged {
			work.Ready = true
		}
	}
	return work, nil
}
