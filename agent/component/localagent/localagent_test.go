package localagent_test

import (
	"context"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/component/localagent"
	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	rt "github.com/felinics/twilight/agentcore/runtime"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/sessionkernel"
)

// A Compose that fails after the Worker exists releases it: the Worker's
// settlement hub is closed, so a subscription ends instead of waiting.
func TestComposeRollsBackOnFailure(t *testing.T) {
	p, err := app.NewPresetFromDefinitions("model", nil)
	if err != nil {
		t.Fatal(err)
	}
	hub := executor.NewSettlementHub("compose-test", 8)
	bindings, ledger := sqlitetest.Artifacts(t)
	db := sqlitetest.Open(t)
	cfg := localagent.Config{
		Config: app.Config{
			Kernel: sessionkernel.Ports{
				Store:     filestoretest.Store(t),
				Content:   filestoretest.Content(t, sessionstore.FrozenAuthority),
				Artifacts: sessionkernel.Artifacts{Bindings: bindings, Ledger: ledger},
			},
			Execution: rt.ExecutionConfig{Redispatches: db.Redispatches()},
			Presets:   []app.Preset{{ID: "", Value: p}},
		},
		Models:     map[run.ModelRef]local.ModelInvoker{},
		Executions: db.Executions(),
		Worker:     executor.WorkerOptions{Settlements: hub},
	}
	if _, err := localagent.Compose(cfg); err == nil {
		t.Fatal("Compose with a nameless preset succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := hub.Settlements(ctx, "", 0, func(effect.Settlement) bool { return true }); err != nil {
		t.Fatalf("the Worker of a failed Compose is still running: %v", err)
	}
}
