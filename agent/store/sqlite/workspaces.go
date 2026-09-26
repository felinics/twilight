package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/workspace"
)

// WorkspaceStore is workspace.Store over the workspaces and
// workspace_snapshots tables: one JSON record per Workspace, the runtime
// generation checked and replaced inside one transaction.
type WorkspaceStore struct{ db *sql.DB }

var _ workspace.Store = (*WorkspaceStore)(nil)

// Workspaces is the workspace.Store over this database.
func (d *DB) Workspaces() *WorkspaceStore { return &WorkspaceStore{db: d.db} }

func readWorkspace(ctx context.Context, q querier, id workspace.ID) (workspace.Workspace, error) {
	var raw string
	err := q.QueryRowContext(ctx, `SELECT record FROM workspaces WHERE id = ?`, string(id)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return workspace.Workspace{}, workspace.ErrNotFound
	}
	if err != nil {
		return workspace.Workspace{}, err
	}
	var w workspace.Workspace
	if err := json.Unmarshal([]byte(raw), &w); err != nil {
		return workspace.Workspace{}, fmt.Errorf("sqlite: workspace %s: %w", id, err)
	}
	return w, nil
}

func writeWorkspace(ctx context.Context, t *sql.Tx, w *workspace.Workspace, insert bool) error {
	raw, err := json.Marshal(w)
	if err != nil {
		return err
	}
	if insert {
		_, err = t.ExecContext(ctx, `INSERT INTO workspaces (id, record) VALUES (?, ?)`, string(w.ID), string(raw))
		return err
	}
	_, err = t.ExecContext(ctx, `UPDATE workspaces SET record = ? WHERE id = ?`, string(raw), string(w.ID))
	return err
}

func (s *WorkspaceStore) Create(ctx context.Context, w workspace.Workspace) error { //nolint:gocritic // hugeParam: Store contract takes the value
	return tx(ctx, s.db, func(t *sql.Tx) error {
		if _, err := readWorkspace(ctx, t, w.ID); err == nil {
			return workspace.ErrExists
		} else if !errors.Is(err, workspace.ErrNotFound) {
			return err
		}
		return writeWorkspace(ctx, t, &w, true)
	})
}

func (s *WorkspaceStore) Get(ctx context.Context, id workspace.ID) (workspace.Workspace, error) {
	return readWorkspace(ctx, s.db, id)
}

func (s *WorkspaceStore) Put(ctx context.Context, w workspace.Workspace) error { //nolint:gocritic // hugeParam: Store contract takes the value
	return tx(ctx, s.db, func(t *sql.Tx) error {
		if _, err := readWorkspace(ctx, t, w.ID); err != nil {
			return err
		}
		return writeWorkspace(ctx, t, &w, false)
	})
}

func (s *WorkspaceStore) UpdateRuntime(ctx context.Context, id workspace.ID, expected uint64, binding workspace.RuntimeBinding) error {
	return tx(ctx, s.db, func(t *sql.Tx) error {
		w, err := readWorkspace(ctx, t, id)
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
		return writeWorkspace(ctx, t, &w, false)
	})
}

func (s *WorkspaceStore) UpdateSnapshot(ctx context.Context, id workspace.ID, ref workspace.SnapshotRef) error {
	return tx(ctx, s.db, func(t *sql.Tx) error {
		w, err := readWorkspace(ctx, t, id)
		if err != nil {
			return err
		}
		snap, err := readSnapshot(ctx, t, ref)
		if err != nil {
			return err
		}
		if snap.Workspace != id {
			return workspace.ErrNotFound
		}
		w.Snapshot = &ref
		return writeWorkspace(ctx, t, &w, false)
	})
}

func readSnapshot(ctx context.Context, q querier, ref workspace.SnapshotRef) (workspace.Snapshot, error) {
	var raw string
	err := q.QueryRowContext(ctx, `SELECT record FROM workspace_snapshots WHERE ref = ?`, string(ref)).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return workspace.Snapshot{}, workspace.ErrNotFound
	}
	if err != nil {
		return workspace.Snapshot{}, err
	}
	var snap workspace.Snapshot
	if err := json.Unmarshal([]byte(raw), &snap); err != nil {
		return workspace.Snapshot{}, fmt.Errorf("sqlite: snapshot %s: %w", ref, err)
	}
	return snap, nil
}

func (s *WorkspaceStore) PutSnapshot(ctx context.Context, snap workspace.Snapshot) error {
	raw, err := json.Marshal(snap)
	if err != nil {
		return err
	}
	return tx(ctx, s.db, func(t *sql.Tx) error {
		if _, err := readWorkspace(ctx, t, snap.Workspace); err != nil {
			return err
		}
		_, err := t.ExecContext(ctx, `INSERT INTO workspace_snapshots (ref, workspace, record) VALUES (?, ?, ?)
			ON CONFLICT(ref) DO UPDATE SET workspace = excluded.workspace, record = excluded.record`, string(snap.Ref), string(snap.Workspace), string(raw))
		return err
	})
}

func (s *WorkspaceStore) GetSnapshot(ctx context.Context, ref workspace.SnapshotRef) (workspace.Snapshot, error) {
	return readSnapshot(ctx, s.db, ref)
}

func (s *WorkspaceStore) Fork(ctx context.Context, f workspace.Fork) (workspace.Workspace, error) {
	var out workspace.Workspace
	err := tx(ctx, s.db, func(t *sql.Tx) error {
		src, err := readWorkspace(ctx, t, f.Source)
		if err != nil {
			return err
		}
		snap, err := readSnapshot(ctx, t, f.Snapshot)
		if err != nil {
			return err
		}
		if snap.Workspace != f.Source {
			return workspace.ErrNotFound
		}
		if _, err := readWorkspace(ctx, t, f.Destination); err == nil {
			return workspace.ErrExists
		} else if !errors.Is(err, workspace.ErrNotFound) {
			return err
		}
		base := f.Base
		if base == "" {
			base = src.Base
		}
		ref := f.Snapshot
		out = workspace.Workspace{ID: f.Destination, Project: src.Project, Base: base, Snapshot: &ref}
		return writeWorkspace(ctx, t, &out, true)
	})
	if err != nil {
		return workspace.Workspace{}, err
	}
	return out, nil
}
