// Package stores opens the durable database a component's document names
// (config.Store) and presents it as the narrow store contracts the
// component composes over. The adapters (agent/store/sqlite,
// agent/store/postgres) stay behind it, so a component neither imports
// them nor knows which one it runs on.
package stores

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/config"
	"github.com/felinics/twilight/agent/store/postgres"
	"github.com/felinics/twilight/agent/store/sqlite"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/checkpoint"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/session"
)

// Handle is the set of small-row stores every durable database provides.
type Handle interface {
	Executions() executionstore.Store
	Processes() process.Store
	Checkpoints() checkpoint.Store
	Inbox() inbox.Store
	Bindings() artifact.BindingStore
	Ledger(artifact.BindingSetBuilder) artifact.RetentionLedger
	Workspaces() workspace.Store
	Close() error
}

// Shared is a Handle that also holds the Session ledger and the cas
// content: a database every replica reaches (CLD-STO-1). A SQLite file is
// not one; its process keeps those two in filestore directories.
type Shared interface {
	Handle
	Sessions() session.Store
	Content(artifact.Authority) (artifact.ContentStore, error)
}

// Open opens the database cfg names. The result is a Shared when the
// database is Postgres.
func Open(ctx context.Context, cfg config.Store) (Handle, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if cfg.Postgres != nil {
		dsn, err := cfg.Postgres.Resolve()
		if err != nil {
			return nil, err
		}
		db, err := postgres.Open(ctx, dsn)
		if err != nil {
			return nil, fmt.Errorf("stores: postgres: %w", err)
		}
		return pgHandle{db}, nil
	}
	db, err := sqlite.Open(cfg.SQLite)
	if err != nil {
		return nil, fmt.Errorf("stores: sqlite: %w", err)
	}
	return sqliteHandle{db}, nil
}

// OpenShared opens cfg and requires a Shared database.
func OpenShared(ctx context.Context, cfg config.Store) (Shared, error) {
	h, err := Open(ctx, cfg)
	if err != nil {
		return nil, err
	}
	shared, ok := h.(Shared)
	if !ok {
		_ = h.Close()
		return nil, errors.New("stores: a shared database (postgres) is required")
	}
	return shared, nil
}

type sqliteHandle struct{ db *sqlite.DB }

func (h sqliteHandle) Executions() executionstore.Store { return h.db.Executions() }
func (h sqliteHandle) Processes() process.Store         { return h.db.Processes() }
func (h sqliteHandle) Checkpoints() checkpoint.Store    { return h.db.Checkpoints() }
func (h sqliteHandle) Inbox() inbox.Store               { return h.db.Inbox() }
func (h sqliteHandle) Bindings() artifact.BindingStore  { return h.db.Bindings() }
func (h sqliteHandle) Ledger(b artifact.BindingSetBuilder) artifact.RetentionLedger {
	return h.db.Ledger(b)
}
func (h sqliteHandle) Workspaces() workspace.Store { return h.db.Workspaces() }
func (h sqliteHandle) Close() error                { return h.db.Close() }

type pgHandle struct{ db *postgres.DB }

func (h pgHandle) Executions() executionstore.Store { return h.db.Executions() }
func (h pgHandle) Processes() process.Store         { return h.db.Processes() }
func (h pgHandle) Checkpoints() checkpoint.Store    { return h.db.Checkpoints() }
func (h pgHandle) Inbox() inbox.Store               { return h.db.Inbox() }
func (h pgHandle) Bindings() artifact.BindingStore  { return h.db.Bindings() }
func (h pgHandle) Ledger(b artifact.BindingSetBuilder) artifact.RetentionLedger {
	return h.db.Ledger(b)
}
func (h pgHandle) Workspaces() workspace.Store { return h.db.Workspaces() }
func (h pgHandle) Sessions() session.Store     { return h.db.Sessions() }
func (h pgHandle) Content(authority artifact.Authority) (artifact.ContentStore, error) {
	return h.db.Content(authority, postgres.ContentStoreOptions{})
}
func (h pgHandle) Close() error { return h.db.Close() }
