-- name: AdvisoryLock :exec
SELECT pg_advisory_xact_lock(hashtext(@key::text));

-- name: SchemaVersion :one
SELECT COALESCE(MAX(version), 0)::bigint AS version FROM twilight_schema;

-- name: RecordSchemaVersion :exec
INSERT INTO twilight_schema (version, applied_at) VALUES ($1, $2);
