package workspace

import (
	"context"

	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// Resolver is the application's loop.TargetResolver (APP-TGT-1, RUN-LOP-9):
// a workspace-placed tool effect gets the Session's bound Workspace as its
// target, own or inherited; every other effect gets none. A Session with no
// binding yields no target, and the workspace backend then refuses the call
// as a Known failure.
type Resolver struct {
	Projections extension.ProjectionReader
}

func (r *Resolver) ResolveTarget(ctx context.Context, ec loop.EffectContext) (*run.TargetRef, error) {
	if ec.Kind != loop.AssignmentTool || ec.Placement != run.PlacementWorkspace {
		return nil, nil
	}
	b, err := Read(ctx, r.Projections, session.SessionID(ec.Session))
	if err != nil {
		return nil, err
	}
	if !b.Bound {
		return nil, nil
	}
	return &run.TargetRef{Kind: TargetKind, ID: string(b.Workspace)}, nil
}
