-- name: Segment :one
SELECT header FROM session_segments WHERE id = $1;

-- name: SegmentIDs :many
SELECT id FROM session_segments ORDER BY id;

-- name: InsertSegment :exec
INSERT INTO session_segments (id, header) VALUES ($1, $2);

-- name: DeleteSegment :exec
DELETE FROM session_segments WHERE id = $1;

-- name: SegmentCommitsFrom :many
SELECT seq, body FROM session_commits WHERE segment = $1 AND seq >= $2 ORDER BY seq LIMIT $3;

-- name: SegmentIndex :many
SELECT seq, commit_id FROM session_commits WHERE segment = $1 ORDER BY seq;

-- name: SegmentIndexSummary :one
SELECT COUNT(*)::bigint AS entries, COALESCE(MIN(seq), -1)::bigint AS first_seq, COALESCE(MAX(seq), -1)::bigint AS last_seq FROM session_commits WHERE segment = $1;

-- name: SegmentStreamCounts :many
SELECT seq, domain, stream_id, events FROM session_commit_streams WHERE segment = $1 ORDER BY seq, domain, stream_id;

-- name: SegmentStreamHead :one
SELECT COALESCE(SUM(events), 0)::bigint AS events FROM session_commit_streams WHERE segment = $1 AND domain = $2 AND stream_id = $3 AND seq < $4;

-- name: InsertSegmentCommitStream :exec
INSERT INTO session_commit_streams (segment, seq, domain, stream_id, events) VALUES ($1, $2, $3, $4, $5);

-- name: DeleteSegmentCommitStreamsAbove :exec
DELETE FROM session_commit_streams WHERE segment = $1 AND seq > $2;

-- name: DeleteSegmentCommitStreams :exec
DELETE FROM session_commit_streams WHERE segment = $1;

-- name: SegmentHead :one
SELECT COALESCE(MAX(seq), -1)::bigint AS last_seq FROM session_commits WHERE segment = $1;

-- name: SegmentCommitSeq :one
SELECT seq FROM session_commits WHERE segment = $1 AND commit_id = $2;

-- name: SegmentCommitByID :one
SELECT body FROM session_commits WHERE segment = $1 AND commit_id = $2;

-- name: InsertSegmentCommit :exec
INSERT INTO session_commits (segment, seq, commit_id, body) VALUES ($1, $2, $3, $4);

-- name: DeleteSegmentCommitsAbove :exec
DELETE FROM session_commits WHERE segment = $1 AND seq > $2;

-- name: DeleteSegmentCommits :exec
DELETE FROM session_commits WHERE segment = $1;

-- name: SessionRoot :one
SELECT id, tip, created_at, epoch, owned, owner, lease_until, failed FROM session_roots WHERE id = $1;

-- name: SessionRoots :many
SELECT id, tip, created_at, epoch, owned, owner, lease_until, failed FROM session_roots ORDER BY id;

-- name: HeldSessionRoots :many
SELECT id, tip, created_at, epoch, owned, owner, lease_until, failed FROM session_roots WHERE owned ORDER BY id;

-- name: ExpiredSessionRoots :many
SELECT id, tip, created_at, epoch, owned, owner, lease_until, failed FROM session_roots
WHERE owned AND lease_until > 0 AND lease_until <= $1 ORDER BY lease_until, id LIMIT $2;

-- name: InsertSessionRoot :exec
INSERT INTO session_roots (id, tip, created_at) VALUES ($1, $2, $3);

-- name: UpdateSessionOwnership :exec
UPDATE session_roots SET epoch = $1, owned = $2, owner = $3, lease_until = $4, failed = $5 WHERE id = $6;

-- name: DeleteSessionRoot :exec
DELETE FROM session_roots WHERE id = $1;
