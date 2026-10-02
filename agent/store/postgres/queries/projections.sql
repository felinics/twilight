-- name: ProjectionEntry :one
SELECT state, through FROM projection_cache WHERE session = $1 AND projection = $2 AND version = $3;

-- name: UpsertProjectionEntry :exec
INSERT INTO projection_cache (session, projection, version, state, through) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (session, projection, version) DO UPDATE SET state = EXCLUDED.state, through = EXCLUDED.through
WHERE projection_cache.through < EXCLUDED.through;

-- name: DeleteProjectionEntries :exec
DELETE FROM projection_cache WHERE session = $1;
