-- name: ProcessCommitsFrom :many
SELECT seq, body FROM process_commits WHERE key = $1 AND seq >= $2 ORDER BY seq;

-- name: ProcessHead :one
SELECT COALESCE(MAX(seq), -1)::bigint AS last_seq FROM process_commits WHERE key = $1;

-- name: ProcessCommitSeq :one
SELECT seq FROM process_commits WHERE key = $1 AND commit_id = $2;

-- name: ProcessEpoch :one
SELECT epoch FROM processes WHERE key = $1;

-- name: InsertProcess :exec
INSERT INTO processes (key, assignment_key, epoch) VALUES ($1, $2, $3);

-- name: RaiseProcessEpoch :exec
UPDATE processes SET epoch = GREATEST(epoch, $1) WHERE key = $2;

-- name: InsertProcessCommit :exec
INSERT INTO process_commits (key, seq, commit_id, body) VALUES ($1, $2, $3, $4);
