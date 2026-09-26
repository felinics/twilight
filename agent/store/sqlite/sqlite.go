// Package sqlite is the reference agent's deployment adapter for the
// small-row ports that sit next to the Session ledger and the cas content
// files: the Worker's execution records (executor/store.Store), the dispatch
// ledger (process.Store), the artifact BindingStore and RetentionLedger, and
// the checkpoint.Store, the Session command inbox (inbox.Store) and the
// workspace records (workspace.Store). One
// SQLite file holds the tables, so one deployment
// has one transaction boundary and one cross-process lock for all of them;
// the Session ledger stays in its JSONL segments and cas bodies stay files.
//
// It lives outside agentcore on purpose: the kernel's contracts are defined
// and tested in agentcore against the reference stores of their xxxtest
// packages (storetest, processtest, artifacttest, checkpointtest), and this
// package proves itself by running those same conformance suites.
//
// The database runs in WAL mode with a busy timeout, and every
// read-modify-write of a lease runs in an immediate transaction, so two
// Workers over one file serialize on the store, which remains the fencing
// authority (RUN-EXE-6).
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/felinics/twilight/agentcore/artifact"

	// The pure-Go SQLite driver, registered as "sqlite".
	_ "modernc.org/sqlite"
)

// Options tune a DB.
type Options struct {
	// Now is the lease clock of the execution records; nil selects time.Now.
	// It must agree with the Worker's clock (WorkerOptions.Clock).
	Now func() time.Time
	// BusyTimeout is how long a statement waits for another connection's
	// lock before failing; zero selects DefaultBusyTimeout.
	BusyTimeout time.Duration
}

// DefaultBusyTimeout is the lock wait applied when Options give none.
const DefaultBusyTimeout = 5 * time.Second

// DB is one open SQLite file holding the three stores.
type DB struct {
	db  *sql.DB
	now func() time.Time
}

const schema = `
CREATE TABLE IF NOT EXISTS executions (
	key            TEXT PRIMARY KEY,
	assignment_key TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS execution_commits (
	key       TEXT NOT NULL,
	seq       INTEGER NOT NULL,
	commit_id TEXT NOT NULL,
	body      TEXT NOT NULL,
	PRIMARY KEY (key, seq),
	UNIQUE (key, commit_id)
);
CREATE TABLE IF NOT EXISTS execution_leases (
	key         TEXT PRIMARY KEY,
	owner       TEXT NOT NULL,
	epoch       INTEGER NOT NULL,
	lease_until INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS execution_leases_by_owner ON execution_leases (owner, key);
CREATE TABLE IF NOT EXISTS processes (
	key            TEXT PRIMARY KEY,
	assignment_key TEXT NOT NULL,
	epoch          INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS process_commits (
	key       TEXT NOT NULL,
	seq       INTEGER NOT NULL,
	commit_id TEXT NOT NULL,
	body      TEXT NOT NULL,
	PRIMARY KEY (key, seq),
	UNIQUE (key, commit_id)
);
CREATE TABLE IF NOT EXISTS checkpoints (
	consumer TEXT NOT NULL,
	ledger   TEXT NOT NULL,
	next     INTEGER NOT NULL,
	PRIMARY KEY (consumer, ledger)
);
CREATE TABLE IF NOT EXISTS bindings (
	id      TEXT PRIMARY KEY,
	digest  TEXT NOT NULL,
	binding TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS claims (
	id              TEXT PRIMARY KEY,
	owner_kind      TEXT NOT NULL,
	owner_authority TEXT NOT NULL,
	owner_identity  TEXT NOT NULL,
	state           TEXT NOT NULL,
	claim           TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS claims_by_owner ON claims (owner_kind, owner_authority, id);
CREATE TABLE IF NOT EXISTS inbox (
	session     TEXT NOT NULL,
	seq         INTEGER NOT NULL,
	command_id  TEXT NOT NULL,
	kind        TEXT NOT NULL,
	payload     TEXT,
	enqueued_at INTEGER NOT NULL,
	status      TEXT NOT NULL DEFAULT '',
	reason      TEXT,
	resolved_at INTEGER,
	PRIMARY KEY (session, seq),
	UNIQUE (session, command_id)
);
CREATE INDEX IF NOT EXISTS inbox_pending ON inbox (status, session, seq);
CREATE TABLE IF NOT EXISTS workspaces (
	id     TEXT PRIMARY KEY,
	record TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS workspace_snapshots (
	ref       TEXT PRIMARY KEY,
	workspace TEXT NOT NULL,
	record    TEXT NOT NULL
);
`

// Open opens or creates the database at path and ensures its schema.
func Open(path string, options ...Options) (*DB, error) {
	if path == "" {
		return nil, errors.New("sqlite: empty database path")
	}
	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	busy := opts.BusyTimeout
	if busy <= 0 {
		busy = DefaultBusyTimeout
	}
	q := url.Values{}
	q.Set("_txlock", "immediate")
	q.Add("_pragma", "busy_timeout("+strconv.FormatInt(busy.Milliseconds(), 10)+")")
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "synchronous(NORMAL)")
	q.Add("_pragma", "foreign_keys(ON)")
	db, err := sql.Open("sqlite", "file:"+path+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %s: %w", path, err)
	}
	if _, err := db.ExecContext(context.Background(), schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: create schema in %s: %w", path, err)
	}
	return &DB{db: db, now: now}, nil
}

// Close closes the database. Stores obtained from it stop working.
func (d *DB) Close() error { return d.db.Close() }

// Executions is the Worker's execution record store over this database.
func (d *DB) Executions() *ExecutionStore { return &ExecutionStore{db: d.db, now: d.now} }

// Bindings is the artifact BindingStore over this database.
func (d *DB) Bindings() *BindingStore { return &BindingStore{db: d.db} }

// Ledger is the artifact RetentionLedger over this database. builder, when
// set, rebuilds and verifies every incoming BindingSet (ART-RET-1); the
// usual choice is artifact.SetBuilder over Bindings.
func (d *DB) Ledger(builder artifact.BindingSetBuilder) *RetentionLedger {
	return &RetentionLedger{db: d.db, builder: builder}
}

// tx runs fn inside an immediate write transaction and commits it when fn
// returns nil.
func tx(ctx context.Context, db *sql.DB, fn func(*sql.Tx) error) error {
	t, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(t); err != nil {
		_ = t.Rollback()
		return err
	}
	return t.Commit()
}
