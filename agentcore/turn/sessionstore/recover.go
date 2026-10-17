package sessionstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/turn/store"
)

// Reconciler decides the takeover disposition of one Turn's Executing targets
// reconcile.Reconciler is the implementation.
type Reconciler interface {
	Reconcile(ctx context.Context, st store.TurnStore, snapshot *store.Snapshot) (int, error)
}

// RecoverInterrupted is the Session-level takeover: every active
// Turn of the Session is reconciled. Each recovery command is identified by
// the effect it disposes, so an owner that repeats the takeover,
// or a later owner, replays the same commands idempotently. The host calls it
// once after opening the Writer and before driving any Turn. The Reconciler is
// required: a caller that holds no executor and wants every Executing target
// disposed says so with reconcile.Reconciler{Abandon: true}; nothing is
// disposed for want of an executor to ask. It returns the number
// of accepted recovery commands.
func (s *SessionTurnStore) RecoverInterrupted(ctx context.Context, w writer.Writer, rec Reconciler) (int, error) {
	if err := store.CheckContext(ctx); err != nil {
		return 0, err
	}
	if w == nil {
		return 0, errors.New("sessionstore: recovery requires the session's writer")
	}
	if rec == nil {
		return 0, errors.New("sessionstore: recovery requires a reconciler; reconcile.Reconciler{Abandon: true} disposes without an executor")
	}
	sid := w.SessionID()
	state, _, err := w.Projections().Load(ctx, sid, MachineProjectionID, MachineProjection.Version)
	if err != nil {
		return 0, err
	}
	m, ok := state.(Machine)
	if !ok {
		return 0, fmt.Errorf("sessionstore: machine projection is %T", state)
	}
	st := s.Bind(w)
	n := 0
	for turnID := range m.Active {
		snapshot, _ := m.snapshot(turnID)
		accepted, err := rec.Reconcile(ctx, st, &snapshot)
		n += accepted
		if err != nil {
			return n, err
		}
	}
	return n, nil
}
