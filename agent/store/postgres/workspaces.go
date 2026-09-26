package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/felinics/twilight/agent/store/postgres/internal/db"
	"github.com/felinics/twilight/agent/workspace"
)

// WorkspaceStore is workspace.Store over the workspaces tables.
type WorkspaceStore struct{ d *DB }

var _ workspace.Store = (*WorkspaceStore)(nil)

// Workspaces is the workspace.Store over this database.
func (d *DB) Workspaces() *WorkspaceStore { return &WorkspaceStore{d: d} }

func readWorkspace(ctx context.Context, q *db.Queries, id workspace.ID) (workspace.Workspace, error) {
	raw, err := q.Workspace(ctx, string(id))
	if noRows(err) {
		return workspace.Workspace{}, workspace.ErrNotFound
	}
	if err != nil {
		return workspace.Workspace{}, err
	}
	var w workspace.Workspace
	if err := json.Unmarshal([]byte(raw), &w); err != nil {
		return workspace.Workspace{}, fmt.Errorf("postgres: workspace %s: %w", id, err)
	}
	return w, nil
}

func writeWorkspace(ctx context.Context, q *db.Queries, w *workspace.Workspace, insert bool) error {
	raw, err := json.Marshal(w)
	if err != nil {
		return err
	}
	if insert {
		err := q.InsertWorkspace(ctx, db.InsertWorkspaceParams{ID: string(w.ID), Record: string(raw)})
		if isUniqueViolation(err) {
			return workspace.ErrExists
		}
		return err
	}
	return q.UpdateWorkspace(ctx, db.UpdateWorkspaceParams{Record: string(raw), ID: string(w.ID)})
}

func (s *WorkspaceStore) Create(ctx context.Context, w workspace.Workspace) error { //nolint:gocritic // hugeParam: Store contract takes the value
	return s.d.tx(ctx, "workspace:"+string(w.ID), func(q *db.Queries) error {
		if _, err := readWorkspace(ctx, q, w.ID); err == nil {
			return workspace.ErrExists
		} else if err != workspace.ErrNotFound { //nolint:errorlint // sentinel returned unwrapped by readWorkspace
			return err
		}
		return writeWorkspace(ctx, q, &w, true)
	})
}

func (s *WorkspaceStore) Get(ctx context.Context, id workspace.ID) (workspace.Workspace, error) {
	return readWorkspace(ctx, s.d.q, id)
}

func (s *WorkspaceStore) Put(ctx context.Context, w workspace.Workspace) error { //nolint:gocritic // hugeParam: Store contract takes the value
	return s.d.tx(ctx, "workspace:"+string(w.ID), func(q *db.Queries) error {
		if _, err := readWorkspace(ctx, q, w.ID); err != nil {
			return err
		}
		return writeWorkspace(ctx, q, &w, false)
	})
}

func (s *WorkspaceStore) UpdateRuntime(ctx context.Context, id workspace.ID, expected uint64, binding workspace.RuntimeBinding) error {
	return s.d.tx(ctx, "workspace:"+string(id), func(q *db.Queries) error {
		w, err := readWorkspace(ctx, q, id)
		if err != nil {
			return err
		}
		var current uint64
		if w.Runtime != nil {
			current = w.Runtime.Generation
		}
		if current != expected {
			return workspace.ErrGenerationConflict
		}
		w.Runtime = &binding
		return writeWorkspace(ctx, q, &w, false)
	})
}

func readSnapshot(ctx context.Context, q *db.Queries, ref workspace.SnapshotRef) (workspace.Snapshot, error) {
	raw, err := q.WorkspaceSnapshot(ctx, string(ref))
	if noRows(err) {
		return workspace.Snapshot{}, workspace.ErrNotFound
	}
	if err != nil {
		return workspace.Snapshot{}, err
	}
	var snap workspace.Snapshot
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		return workspace.Snapshot{}, fmt.Errorf("postgres: snapshot %s: %w", ref, err)
	}
	return snap, nil
}

func (s *WorkspaceStore) PutSnapshot(ctx context.Context, snap workspace.Snapshot) error {
	raw, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	return s.d.tx(ctx, "workspace:"+string(snap.Workspace), func(q *db.Queries) error {
		if _, err := readWorkspace(ctx, q, snap.Workspace); err != nil {
			return err
		}
		return q.UpsertWorkspaceSnapshot(ctx, db.UpsertWorkspaceSnapshotParams{Ref: string(snap.Ref), Workspace: string(snap.Workspace), Record: string(raw)})
	})
}

func (s *WorkspaceStore) GetSnapshot(ctx context.Context, ref workspace.SnapshotRef) (workspace.Snapshot, error) {
	return readSnapshot(ctx, s.d.q, ref)
}

func (s *WorkspaceStore) UpdateSnapshot(ctx context.Context, id workspace.ID, ref workspace.SnapshotRef) error {
	return s.d.tx(ctx, "workspace:"+string(id), func(q *db.Queries) error {
		w, err := readWorkspace(ctx, q, id)
		if err != nil {
			return err
		}
		snap, err := readSnapshot(ctx, q, ref)
		if err != nil {
			return err
		}
		if snap.Workspace != id {
			return workspace.ErrNotFound
		}
		w.Snapshot = &ref
		return writeWorkspace(ctx, q, &w, false)
	})
}

func (s *WorkspaceStore) Fork(ctx context.Context, f workspace.Fork) (workspace.Workspace, error) {
	var out workspace.Workspace
	err := s.d.tx(ctx, "workspace:"+string(f.Destination), func(q *db.Queries) error {
		src, err := readWorkspace(ctx, q, f.Source)
		if err != nil {
			return err
		}
		snap, err := readSnapshot(ctx, q, f.Snapshot)
		if err != nil {
			return err
		}
		if snap.Workspace != f.Source {
			return workspace.ErrNotFound
		}
		if _, err := readWorkspace(ctx, q, f.Destination); err == nil {
			return workspace.ErrExists
		} else if err != workspace.ErrNotFound { //nolint:errorlint // sentinel returned unwrapped by readWorkspace
			return err
		}
		base := f.Base
		if base == "" {
			base = src.Base
		}
		ref := f.Snapshot
		out = workspace.Workspace{ID: f.Destination, Project: src.Project, Base: base, Snapshot: &ref}
		return writeWorkspace(ctx, q, &out, true)
	})
	if err != nil {
		return workspace.Workspace{}, err
	}
	return out, nil
}
