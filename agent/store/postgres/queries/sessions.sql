-- name: Segment :one
SELECT header FROM session_segments WHERE id = $1;

-- name: SegmentIDs :many
SELECT id FROM session_segments ORDER BY id;

-- name: InsertSegment :exec
INSERT INTO session_segments (id, header, parent_segment) VALUES ($1, $2, $3);

-- name: SegmentReferenced :one
SELECT (EXISTS (SELECT 1 FROM session_roots WHERE tip = $1 AND NOT deleted)
    OR EXISTS (SELECT 1 FROM session_segments WHERE parent_segment = $1))::boolean AS referenced;

-- name: DeleteSegment :exec
DELETE FROM session_segments WHERE id = $1;

-- name: SegmentCommitsFrom :many
SELECT seq, body FROM session_commits WHERE segment = $1 AND seq >= $2 ORDER BY seq LIMIT $3;

-- name: SegmentStreamCommits :many
SELECT c.seq, c.body FROM session_commits c
JOIN session_commit_streams s ON s.segment = c.segment AND s.seq = c.seq
WHERE c.segment = $1 AND s.domain = $2 AND s.stream_id = $3 AND c.seq >= $4 AND s.events > 0
ORDER BY c.seq LIMIT $5;

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

-- name: SegmentCommitAt :one
SELECT seq FROM session_commits WHERE segment = $1 AND seq = $2;

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
SELECT id, tip, created_at, epoch, owned, owner, lease_until, failed, deleted FROM session_roots WHERE id = $1;

-- name: SessionRoots :many
SELECT id, tip, created_at, epoch, owned, owner, lease_until, failed, deleted FROM session_roots WHERE NOT deleted ORDER BY id;

-- name: HeldSessionRoots :many
SELECT id, tip, created_at, epoch, owned, owner, lease_until, failed, deleted FROM session_roots WHERE owned AND NOT deleted ORDER BY id;

-- name: ExpiredSessionRoots :many
SELECT id, tip, created_at, epoch, owned, owner, lease_until, failed, deleted FROM session_roots
WHERE owned AND NOT deleted AND lease_until > 0 AND lease_until <= @before ORDER BY lease_until, id LIMIT @row_limit;

-- name: InsertSessionRoot :exec
INSERT INTO session_roots (id, tip, created_at) VALUES ($1, $2, $3);

-- name: InsertPathSpan :exec
INSERT INTO session_path_spans (session, ordinal, segment, from_seq, through_seq) VALUES ($1, $2, $3, $4, $5);

-- name: SessionPathSpans :many
SELECT ordinal, segment, from_seq, through_seq FROM session_path_spans WHERE session = $1 ORDER BY ordinal;

-- name: LivePathSpans :many
SELECT s.session, s.ordinal, s.segment, s.from_seq, s.through_seq
FROM session_path_spans s
JOIN session_roots r ON r.id = s.session
WHERE NOT r.deleted
ORDER BY s.session, s.ordinal;

-- name: DeleteSessionPathSpans :exec
DELETE FROM session_path_spans WHERE session = $1;

-- name: DeleteOrphanPathSpans :exec
DELETE FROM session_path_spans AS s
WHERE NOT EXISTS (
	SELECT 1 FROM session_roots AS r
	WHERE r.id = s.session AND NOT r.deleted
);

-- name: SegmentSpanBound :one
SELECT through_seq FROM session_path_spans WHERE segment = $1 ORDER BY through_seq DESC NULLS FIRST LIMIT 1;

-- name: UpdateSessionOwnership :exec
UPDATE session_roots SET epoch = $1, owned = $2, owner = $3, lease_until = $4, failed = $5 WHERE id = $6;

-- name: TombstoneSessionRoot :exec
UPDATE session_roots SET deleted = TRUE, owned = FALSE, owner = '', lease_until = 0, failed = '' WHERE id = $1;
