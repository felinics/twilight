-- name: Workspace :one
SELECT record FROM workspaces WHERE id = $1;

-- name: InsertWorkspace :exec
INSERT INTO workspaces (id, record) VALUES ($1, $2);

-- name: UpdateWorkspace :exec
UPDATE workspaces SET record = $1 WHERE id = $2;

-- name: WorkspaceSnapshot :one
SELECT record FROM workspace_snapshots WHERE ref = $1;

-- name: UpsertWorkspaceSnapshot :exec
INSERT INTO workspace_snapshots (ref, workspace, record) VALUES ($1, $2, $3)
ON CONFLICT (ref) DO UPDATE SET workspace = EXCLUDED.workspace, record = EXCLUDED.record;
