package sessionstore

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/felinics/twilight/agentcore/run/store"
	"github.com/felinics/twilight/agentcore/session/writer"
)

// ActiveRuns reads the snapshot of every active Run of the Session from the
// Writer's projection, ordered by RunID. A terminal Run has left the
// projection and is not listed. It is a read: nothing is committed and no
// ownership is taken.
func (s *SessionRunStore) ActiveRuns(ctx context.Context, w writer.Writer) ([]store.Snapshot, error) {
	if err := store.CheckContext(ctx); err != nil {
		return nil, err
	}
	if w == nil {
		return nil, errors.New("sessionstore: listing the active runs requires the session's writer")
	}
	state, _, err := w.Projections().Load(ctx, w.SessionID(), MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return nil, err
	}
	m, ok := state.(Machine)
	if !ok {
		return nil, fmt.Errorf("sessionstore: machine projection is %T", state)
	}
	ids := slices.Sorted(maps.Keys(m.Active))
	snapshots := make([]store.Snapshot, 0, len(ids))
	for _, runID := range ids {
		snapshot, _ := m.snapshot(runID)
		snapshots = append(snapshots, snapshot)
	}
	return snapshots, nil
}
