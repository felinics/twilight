package postgres_test

import (
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/store/postgres"
	"github.com/felinics/twilight/agent/store/postgres/postgrestest"
	"github.com/felinics/twilight/agent/workspace"
	"github.com/felinics/twilight/agent/workspace/workspacetest"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/artifact/artifacttest"
	"github.com/felinics/twilight/agentcore/checkpoint"
	"github.com/felinics/twilight/agentcore/checkpoint/checkpointtest"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/executor/store/storetest"
	"github.com/felinics/twilight/agentcore/inbox"
	"github.com/felinics/twilight/agentcore/inbox/inboxtest"
	"github.com/felinics/twilight/agentcore/process"
	"github.com/felinics/twilight/agentcore/process/processtest"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/session/run/runtimetest"
	"github.com/felinics/twilight/agentcore/session/sessiontest"
	"github.com/felinics/twilight/agentcore/turn/turntest"
)

// clock is a settable store clock shared by the handles of one fixture.
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// Two handles over one schema stand for two processes.
func TestExecutionStoreConformance(t *testing.T) {
	storetest.Run(t, func(t *testing.T) storetest.Fixture {
		c := &clock{now: time.Unix(2_000_000, 0)}
		dsn := postgrestest.Schema(t)
		open := func(t *testing.T) executionstore.Store {
			return postgrestest.OpenDSN(t, dsn, postgres.Options{Now: c.Now}).Executions()
		}
		return storetest.Fixture{Store: open(t), Reopen: open, Advance: c.Advance}
	})
}

func TestProcessStoreConformance(t *testing.T) {
	processtest.Run(t, func(t *testing.T) process.Store { return postgrestest.Open(t).Processes() })
}

func TestCheckpointStoreConformance(t *testing.T) {
	checkpointtest.Run(t, func(t *testing.T) checkpoint.Store { return postgrestest.Open(t).Checkpoints() })
}

func TestInboxStoreConformance(t *testing.T) {
	inboxtest.Run(t, func(t *testing.T) inbox.Store { return postgrestest.Open(t).Inbox() })
}

func TestWorkspaceStoreConformance(t *testing.T) {
	workspacetest.Run(t, func(t *testing.T) workspace.Store { return postgrestest.Open(t).Workspaces() })
}

// The bindings, the retention ledger and the cas content store all live in
// the database; the content store's expiry follows the DB clock.
func TestArtifactConformance(t *testing.T) {
	artifacttest.Run(t, func(t *testing.T) artifacttest.Fixture {
		c := &clock{now: time.Unix(1_000_000, 0)}
		db := postgrestest.Open(t, postgres.Options{Now: c.Now})
		bindings := db.Bindings()
		return artifacttest.Fixture{
			Bindings: bindings,
			Ledger:   db.Ledger(artifact.SetBuilder{Resolver: bindings}),
			NewContent: func(t *testing.T, authority artifact.Authority) artifact.ContentStore {
				store, err := db.Content(authority, postgres.ContentStoreOptions{EphemeralTTL: time.Hour})
				if err != nil {
					t.Fatal(err)
				}
				return store
			},
			Advance: c.Advance,
		}
	})
}

func TestContentPutLimit(t *testing.T) {
	db := postgrestest.Open(t)
	artifacttest.PutLimit(t, func(t *testing.T, maxBytes int64) artifact.ContentStore {
		store, err := db.Content(runmod.FrozenAuthority, postgres.ContentStoreOptions{MaxBytes: maxBytes})
		if err != nil {
			t.Fatal(err)
		}
		return store
	})
}

// The Session store runs the kernel, runtime and turn suites: every rule of
// the Session ledger over Postgres tables.
func TestSessionKernelConformance(t *testing.T) {
	sessiontest.Run(t, func(t *testing.T) sessiontest.Fixture {
		return sessiontest.Fixture{Store: postgrestest.Open(t).Sessions()}
	})
}

func TestSessionRuntimeConformance(t *testing.T) {
	runtimetest.Run(t, func(t testing.TB) runtimetest.Fixture {
		return runtimetest.Fixture{Store: postgrestest.Open(t).Sessions()}
	})
}

func TestSessionTurnConformance(t *testing.T) {
	turntest.Run(t, func(t testing.TB) turntest.Fixture {
		return turntest.Fixture{Store: postgrestest.Open(t).Sessions()}
	})
}
