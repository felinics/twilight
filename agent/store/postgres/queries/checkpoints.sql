-- name: Checkpoint :one
SELECT next FROM checkpoints WHERE consumer = $1 AND ledger = $2;

-- name: UpsertCheckpoint :exec
INSERT INTO checkpoints (consumer, ledger, next) VALUES ($1, $2, $3)
ON CONFLICT (consumer, ledger) DO UPDATE SET next = EXCLUDED.next;
