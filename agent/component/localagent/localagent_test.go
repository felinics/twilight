package localagent_test

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/component/localagent"
	envlocal "github.com/felinics/twilight/agent/environment/local"
	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
	"github.com/felinics/twilight/agent/workspace/workspacetest"
	"github.com/felinics/twilight/agentcore/execution"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/preset"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
)

// baseConfig is a complete product Config over temporary durable stores.
func baseConfig(t *testing.T) app.Config {
	t.Helper()
	bindings, ledger := sqlitetest.Artifacts(t)
	return app.Config{
		Sessions: app.SessionPorts{
			Store:     filestoretest.Store(t),
			Content:   filestoretest.Content(t, sessionstore.FrozenAuthority),
			Artifacts: app.Artifacts{Bindings: bindings, Ledger: ledger},
		},
		Execution: execution.Config{Redispatches: sqlitetest.Open(t).Redispatches()},
	}
}

// A Compose that fails after the Worker exists releases it: the Worker's
// settlement hub is closed, so a subscription ends instead of waiting.
func TestComposeRollsBackOnFailure(t *testing.T) {
	p, err := app.NewPresetFromDefinitions("model", nil)
	if err != nil {
		t.Fatal(err)
	}
	hub := executor.NewSettlementHub("compose-test", 8)
	cfg := localagent.Config{
		Config:     baseConfig(t),
		Models:     map[run.ModelRef]local.ModelInvoker{},
		Executions: sqlitetest.Open(t).Executions(),
		Worker:     executor.WorkerOptions{Settlements: hub},
	}
	cfg.Presets = []app.Preset{{ID: "", Value: p}}
	if _, err := localagent.Compose(cfg); err == nil {
		t.Fatal("Compose with a nameless preset succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := hub.Settlements(ctx, "", 0, func(effect.Settlement) bool { return true }); err != nil {
		t.Fatalf("the Worker of a failed Compose is still running: %v", err)
	}
}

// countingPort is an effect port that records what reaches it and answers
// every dispatch with a settled model result, so a Compose over it can be
// driven to the end of a Turn.
type countingPort struct {
	mu         sync.Mutex
	dispatched []effect.Assignment
	settled    map[effect.AssignmentKey]effect.Outcome
}

func (p *countingPort) Validate(context.Context, effect.Assignment) (*run.ToolFailure, error) {
	return nil, nil
}
func (p *countingPort) Dispatch(_ context.Context, a effect.Assignment) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.dispatched = append(p.dispatched, a)
	if p.settled == nil {
		p.settled = make(map[effect.AssignmentKey]effect.Outcome)
	}
	p.settled[a.Key()] = effect.Outcome{Key: a.Key(), Result: effect.ModelSucceeded{Result: model.ModelResult{Text: "ok", FinishReason: model.FinishReasonStop}}}
	return nil
}
func (p *countingPort) Attach(context.Context, effect.AssignmentKey) (effect.Attachment, error) {
	return effect.Attachment{State: effect.AttachmentActive, Execution: effect.ExecutionRunning}, nil
}
func (p *countingPort) Abort(context.Context, effect.AssignmentKey) (effect.Attachment, error) {
	return effect.Attachment{State: effect.AttachmentAborted, Execution: effect.ExecutionAborted}, nil
}
func (p *countingPort) GetStatus(context.Context, effect.AssignmentKey) (effect.ExecutionStatus, error) {
	return effect.ExecutionRunning, nil
}
func (p *countingPort) GetOutcome(_ context.Context, key effect.AssignmentKey) (effect.Outcome, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if out, ok := p.settled[key]; ok {
		return out, nil
	}
	return effect.Outcome{}, effect.ErrExecutionNotFound
}
func (p *countingPort) Cancel(context.Context, effect.AssignmentKey) error { return nil }

func (p *countingPort) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.dispatched)
}

// The Config decides the effect port and refuses to be second-guessed: a
// prefilled Execution.Executor is an error, and so is an execution store
// nothing would read.
func TestComposeRefusesConflictingConfig(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*localagent.Config)
		want string
	}{
		{"prefilled executor", func(c *localagent.Config) {
			c.Execution.Executor = effect.PortsOf(&countingPort{})
			c.Port = &countingPort{}
		}, "Execution.Executor"},
		{"executions without a worker", func(c *localagent.Config) {
			c.Port = &countingPort{}
			c.Executions = sqlitetest.Open(t).Executions()
		}, "no Worker is composed"},
		{"worker without executions", func(c *localagent.Config) {
			c.Models = map[run.ModelRef]local.ModelInvoker{}
		}, "Config.Executions"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := localagent.Config{Config: baseConfig(t)}
			tc.mut(&cfg)
			_, err := localagent.Compose(cfg)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Compose = %v, want an error naming %q", err, tc.want)
			}
		})
	}
}

// A Port beside a Sandbox is the Worker's default route: a model effect,
// which no workspace route claims, reaches the Port through the Worker.
func TestComposePortBesideSandboxRoutesToPort(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	provider, err := envlocal.New(filepath.Join(t.TempDir(), "envs"))
	if err != nil {
		t.Fatal(err)
	}
	port := &countingPort{}
	cfg := localagent.Config{
		Config:     baseConfig(t),
		Port:       port,
		Executions: sqlitetest.Open(t).Executions(),
		Sandbox:    &localagent.SandboxConfig{Provider: provider, Backend: envlocal.Backend},
	}
	cfg.Workspaces = &app.WorkspaceConfig{Store: &workspacetest.Map{}}
	ag, err := localagent.Compose(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ag.Close(ctx) }()
	if ag.Worker == nil {
		t.Fatal("a Port beside a Sandbox composed no Worker")
	}
	ref, err := ag.RegisterPreset("p", mustPreset(t, "m-1"))
	if err != nil {
		t.Fatal(err)
	}
	s, err := ag.OpenSession(ctx, "s-port", app.SessionOptions{Preset: ref})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Send(ctx, "hi"); err != nil {
		t.Fatal(err)
	}
	if n := port.count(); n != 1 {
		t.Fatalf("the Port received %d dispatches, want the model effect", n)
	}
	if _, ok := port.dispatched[0].Model(); !ok {
		t.Fatalf("the Port received %+v, want a model assignment", port.dispatched[0])
	}
}

// Compose reads its Config and writes nothing back: composing the same
// Config twice yields two agents, each snapshotting through its own backend.
func TestComposeLeavesTheConfigAlone(t *testing.T) {
	provider, err := envlocal.New(filepath.Join(t.TempDir(), "envs"))
	if err != nil {
		t.Fatal(err)
	}
	workspaces := &app.WorkspaceConfig{Store: &workspacetest.Map{}}
	cfg := localagent.Config{
		Config:     baseConfig(t),
		Models:     map[run.ModelRef]local.ModelInvoker{},
		Executions: sqlitetest.Open(t).Executions(),
		Sandbox:    &localagent.SandboxConfig{Provider: provider, Backend: envlocal.Backend},
	}
	cfg.Workspaces = workspaces
	first, err := localagent.Compose(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if workspaces.Snapshots != nil {
		t.Fatal("Compose wrote the sandbox backend into the caller's WorkspaceConfig")
	}
	if err := first.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	// The second agent must not snapshot through the first's closed backend.
	cfg.Executions = sqlitetest.Open(t).Executions()
	cfg.Sessions.Store = filestoretest.Store(t)
	second, err := localagent.Compose(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = second.Close(context.Background()) }()
	if second.Worker == first.Worker {
		t.Fatal("the second Compose reused the first's Worker")
	}
}

// Close releases every component Compose created, in order: the
// application, the Worker, the sandbox backend.
func TestCloseReleasesWorkerAndSandbox(t *testing.T) {
	provider, err := envlocal.New(filepath.Join(t.TempDir(), "envs"))
	if err != nil {
		t.Fatal(err)
	}
	hub := executor.NewSettlementHub("close-test", 8)
	cfg := localagent.Config{
		Config:     baseConfig(t),
		Models:     map[run.ModelRef]local.ModelInvoker{},
		Executions: sqlitetest.Open(t).Executions(),
		Worker:     executor.WorkerOptions{Settlements: hub},
		Sandbox:    &localagent.SandboxConfig{Provider: provider, Backend: envlocal.Backend},
	}
	cfg.Workspaces = &app.WorkspaceConfig{Store: &workspacetest.Map{}}
	ag, err := localagent.Compose(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := ag.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := hub.Settlements(ctx, "", 0, func(effect.Settlement) bool { return true }); err != nil || errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("the Worker is still running after Close: %v", err)
	}
}

func mustPreset(t *testing.T, model run.ModelRef) preset.AgentPreset {
	t.Helper()
	p, err := app.NewPreset(model, nil)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
