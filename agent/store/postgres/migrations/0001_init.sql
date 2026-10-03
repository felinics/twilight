-- 0001_init: every shared store of the reference agent in one schema.
-- Bodies are JSON as TEXT, so the wire form of each ledger is the one the
-- kernel writes; sequences and epochs are BIGINT holding the kernel's uint64.

CREATE TABLE executions (
	key            TEXT PRIMARY KEY,
	assignment_key TEXT NOT NULL
);
CREATE TABLE execution_commits (
	key       TEXT NOT NULL,
	seq       BIGINT NOT NULL,
	commit_id TEXT NOT NULL,
	body      TEXT NOT NULL,
	PRIMARY KEY (key, seq),
	UNIQUE (key, commit_id)
);
CREATE TABLE execution_leases (
	key         TEXT PRIMARY KEY,
	owner       TEXT NOT NULL,
	epoch       BIGINT NOT NULL,
	lease_until BIGINT NOT NULL
);
CREATE INDEX execution_leases_by_owner ON execution_leases (owner, key);

CREATE TABLE processes (
	key            TEXT PRIMARY KEY,
	assignment_key TEXT NOT NULL,
	epoch          BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE process_commits (
	key       TEXT NOT NULL,
	seq       BIGINT NOT NULL,
	commit_id TEXT NOT NULL,
	body      TEXT NOT NULL,
	PRIMARY KEY (key, seq),
	UNIQUE (key, commit_id)
);

CREATE TABLE checkpoints (
	consumer TEXT NOT NULL,
	ledger   TEXT NOT NULL,
	next     BIGINT NOT NULL,
	PRIMARY KEY (consumer, ledger)
);

CREATE TABLE bindings (
	id      TEXT PRIMARY KEY,
	digest  TEXT NOT NULL,
	binding TEXT NOT NULL
);
CREATE TABLE claims (
	id              TEXT PRIMARY KEY,
	owner_kind      TEXT NOT NULL,
	owner_authority TEXT NOT NULL,
	owner_identity  TEXT NOT NULL,
	state           TEXT NOT NULL,
	claim           TEXT NOT NULL
);
CREATE INDEX claims_by_owner ON claims (owner_kind, owner_authority, id);

CREATE TABLE inbox (
	session     TEXT NOT NULL,
	seq         BIGINT NOT NULL,
	command_id  TEXT NOT NULL,
	kind        TEXT NOT NULL,
	payload     TEXT NOT NULL DEFAULT '',
	enqueued_at BIGINT NOT NULL,
	status      TEXT NOT NULL DEFAULT '',
	reason      TEXT NOT NULL DEFAULT '',
	resolved_at BIGINT NOT NULL DEFAULT 0,
	PRIMARY KEY (session, seq),
	UNIQUE (session, command_id)
);
CREATE INDEX inbox_pending ON inbox (status, session, seq);

CREATE TABLE workspaces (
	id     TEXT PRIMARY KEY,
	record TEXT NOT NULL
);
CREATE TABLE workspace_snapshots (
	ref       TEXT PRIMARY KEY,
	workspace TEXT NOT NULL,
	record    TEXT NOT NULL
);

-- parent_segment repeats the header's Parent edge as a foreign key, so a
-- segment a fork still reaches cannot be removed and a fork cannot be
-- created on a parent that is gone (SES-GC-4): the database, not a
-- process-wide lock, arbitrates Create against Collect across replicas.
CREATE TABLE session_segments (
	id             TEXT PRIMARY KEY,
	header         TEXT NOT NULL,
	parent_segment TEXT REFERENCES session_segments (id)
);
CREATE INDEX session_segments_by_parent ON session_segments (parent_segment) WHERE parent_segment IS NOT NULL;
CREATE TABLE session_commits (
	segment   TEXT NOT NULL,
	seq       BIGINT NOT NULL,
	commit_id TEXT NOT NULL,
	body      TEXT NOT NULL,
	PRIMARY KEY (segment, seq),
	UNIQUE (segment, commit_id)
);
-- The CommitIndex's stream counts (SES-REP-3/5): one row per commit per
-- stream, written with the commit, so StreamHead is a sum over one stream's
-- rows and Index derives its entries without decoding a body.
CREATE TABLE session_commit_streams (
	segment   TEXT   NOT NULL,
	seq       BIGINT NOT NULL,
	domain    TEXT   NOT NULL,
	stream_id TEXT   NOT NULL,
	events    BIGINT NOT NULL,
	PRIMARY KEY (segment, seq, domain, stream_id)
);
CREATE INDEX session_commit_streams_by_stream ON session_commit_streams (segment, domain, stream_id, seq);
-- A deleted Session keeps its row with deleted set (SES-GC-1): the id is
-- never reused. tip is not a foreign key: a tombstone keeps naming a
-- segment Collect may remove; RemoveSegment checks live roots itself.
CREATE TABLE session_roots (
	id          TEXT PRIMARY KEY,
	tip         TEXT NOT NULL,
	created_at  BIGINT NOT NULL,
	epoch       BIGINT NOT NULL DEFAULT 0,
	owned       BOOLEAN NOT NULL DEFAULT FALSE,
	owner       TEXT NOT NULL DEFAULT '',
	lease_until BIGINT NOT NULL DEFAULT 0,
	failed      TEXT NOT NULL DEFAULT '',
	deleted     BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE INDEX session_roots_owned ON session_roots (owned);
-- The activation scan's read (APP-ACT-3): held leases by expiry.
CREATE INDEX session_roots_expiry ON session_roots (lease_until) WHERE owned;
-- A root's path is the reference relation: one span per segment that
-- session retains. through_seq NULL is an open end. path_by_segment
-- answers the greatest remaining reference for a segment. segment
-- references the node, so a span still naming it keeps RemoveSegment from
-- deleting it. UNIQUE (session, segment) keeps a segment on a path once.
-- The session foreign key waits until commit: CreateSession inserts the
-- spans before the root row, and a span whose session was never written
-- rolls the transaction back.
CREATE TABLE session_path_spans (
	session     TEXT NOT NULL,
	ordinal     BIGINT NOT NULL,
	segment     TEXT NOT NULL REFERENCES session_segments (id),
	from_seq    BIGINT NOT NULL,
	through_seq BIGINT,
	PRIMARY KEY (session, ordinal),
	CONSTRAINT session_path_spans_session_segment UNIQUE (session, segment),
	CONSTRAINT session_path_spans_session_fkey FOREIGN KEY (session)
		REFERENCES session_roots (id)
		DEFERRABLE INITIALLY DEFERRED
);
CREATE INDEX path_by_segment
	ON session_path_spans (segment, through_seq DESC NULLS FIRST);

-- Folded projection states (EXT-PRJ-3), so a Session reopened on another
-- replica folds only the tail.
CREATE TABLE projection_cache (
	session    TEXT   NOT NULL,
	projection TEXT   NOT NULL,
	version    BIGINT NOT NULL,
	state      TEXT   NOT NULL,
	through    BIGINT NOT NULL,
	PRIMARY KEY (session, projection, version)
);

CREATE TABLE content (
	authority  TEXT NOT NULL,
	key        TEXT NOT NULL,
	data       BYTEA NOT NULL,
	media_type TEXT NOT NULL,
	durability TEXT NOT NULL,
	PRIMARY KEY (authority, key)
);
