-- The schema-version table Migrate creates in code before any migration
-- runs; declared here so sqlc can type the queries over it. Not a
-- migration: it is not under migrations/ and Migrate never applies it.
CREATE TABLE twilight_schema (
	version    BIGINT PRIMARY KEY,
	applied_at BIGINT NOT NULL
);
