-- name: InboxEntry :one
SELECT seq, command_id, kind, payload, enqueued_at, status, reason, resolved_at
FROM inbox WHERE session = $1 AND command_id = $2;

-- name: InboxPending :many
SELECT seq, command_id, kind, payload, enqueued_at, status, reason, resolved_at
FROM inbox WHERE session = $1 AND status = '' ORDER BY seq;

-- name: InboxNextSeq :one
SELECT (COALESCE(MAX(seq), -1) + 1)::bigint AS next_seq FROM inbox WHERE session = $1;

-- name: InsertInboxEntry :exec
INSERT INTO inbox (session, seq, command_id, kind, payload, enqueued_at) VALUES ($1, $2, $3, $4, $5, $6);

-- name: ResolveInboxEntry :execrows
UPDATE inbox SET status = $1, reason = $2, resolved_at = $3 WHERE session = $4 AND seq = $5 AND status = '';

-- name: InboxSessions :many
SELECT DISTINCT session FROM inbox WHERE status = '' ORDER BY session LIMIT $1;
