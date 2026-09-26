package app_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/store/sqlite"
	"github.com/felinics/twilight/agent/store/sqlite/sqlitetest"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/agentcore/run/runtime"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/turn"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
	"path/filepath"
)

// newHost builds a colocated application for tests: a LocalExecutor over the
// given models and tools; the Runtime still writes request bodies to
// cfg.Content (RUN-WIR-4) and the executor never reads them back (RUN-EXE-7).
func newHost(t testing.TB, cfg app.Config, models map[run.ModelRef]loop.ModelInvoker, tools ...loop.ExecutableTool) *app.Application {
	t.Helper()
	cfg = durablePorts(t, cfg)
	cfg.Executor = app.ExecutorConfig{Models: models, Tools: tools}
	a, err := app.Build(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// durablePorts fills every store the Config leaves nil with a fresh durable
// one under t.TempDir(): the JSONL Session ledger, the file cas store for
// frozen bodies, and one SQLite file for the binding index, the retention
// ledger and the Worker's execution records. Build itself has no defaults.
func durablePorts(t testing.TB, cfg app.Config) app.Config {
	t.Helper()
	if cfg.Store == nil {
		cfg.Store = filestoretest.Store(t)
	}
	if cfg.Content == nil {
		cfg.Content = durableContent(t)
	}
	if cfg.Artifacts.Bindings == nil {
		cfg.Artifacts.Bindings, cfg.Artifacts.Ledger = sqlitetest.Artifacts(t)
	}
	if cfg.Executions == nil {
		db := sqlitetest.Open(t)
		cfg.Executions = db.Executions()
		if cfg.Processes == nil {
			cfg.Processes = db.Processes()
		}
	}
	return cfg
}

// runState reads a Run's committed state by SessionID: the lease-free read
// (OWN-HDL-2), so a test observes without owning.
func runState(a *app.Application, sid session.SessionID, runID run.RunID) (runtime.Snapshot, error) {
	record, err := a.Owner.Runs.Record(context.Background(), sid, runID)
	if err != nil {
		return runtime.Snapshot{}, err
	}
	return record.Snapshot, nil
}

// exampleStores builds the durable ports of one process under root for the
// Example functions, which have no testing.TB: the JSONL ledger and the cas
// content store are shared by every process over the root, the binding index
// and retention ledger live in one SQLite file, and each process keeps its
// own execution record file so that a process the example abandons without
// closing does not keep the next one's Worker waiting on its live lease.
func exampleStores(root, worker string) app.Config {
	store, err := filestore.New(filepath.Join(root, "ledger"))
	if err != nil {
		panic(err)
	}
	content, err := filestore.NewContentStore(filepath.Join(root, "content"), runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		panic(err)
	}
	artifacts, err := sqlite.Open(filepath.Join(root, "artifacts.db"))
	if err != nil {
		panic(err)
	}
	records, err := sqlite.Open(filepath.Join(root, worker+"-records.db"))
	if err != nil {
		panic(err)
	}
	bindings := artifacts.Bindings()
	return app.Config{Store: store, Content: content, Executions: records.Executions(),
		Artifacts: owner.Artifacts{Bindings: bindings, Ledger: artifacts.Ledger(artifact.SetBuilder{Resolver: bindings})}}
}

// buildHost is newHost for the Example functions: cfg is complete and a
// failure is a panic.
func buildHost(cfg app.Config, models map[run.ModelRef]loop.ModelInvoker, tools ...loop.ExecutableTool) *app.Application {
	cfg.Executor = app.ExecutorConfig{Models: models, Tools: tools}
	a, err := app.Build(cfg)
	if err != nil {
		panic(err)
	}
	return a
}

// durableContent is a fresh file cas store under the frozen authority.
func durableContent(t testing.TB) artifact.ContentStore {
	t.Helper()
	return filestoretest.Content(t, runmod.FrozenAuthority)
}

// mustPreset builds the one-model AgentPreset the tests register.
func mustPreset(model run.ModelRef, tools []loop.ExecutableTool, opts ...app.PresetOption) turn.AgentPreset {
	p, err := app.NewPreset(model, tools, opts...)
	if err != nil {
		panic(err)
	}
	return p
}

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// scriptedRequests records every request the model saw and answers from a
// script: tool call first, then text.
type scriptedRequests struct {
	mu      sync.Mutex
	seen    []sdk.Request
	answers []sdk.ModelResult
}

func (m *scriptedRequests) Generate(_ context.Context, req sdk.Request) (sdk.ModelResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.seen = append(m.seen, req)
	if len(m.answers) == 0 {
		return sdk.ModelResult{Text: "done", FinishReason: sdk.FinishReasonStop, Usage: sdk.Usage{TotalTokens: 1}}, nil
	}
	next := m.answers[0]
	m.answers = m.answers[1:]
	return next, nil
}

func (m *scriptedRequests) requests() []sdk.Request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]sdk.Request(nil), m.seen...)
}

func toolCallAnswer() sdk.ModelResult {
	return sdk.ModelResult{FinishReason: sdk.FinishReasonToolCalls, Usage: sdk.Usage{TotalTokens: 1},
		ToolCalls: []sdk.ToolCall{{ToolCallID: "c1", ToolName: "lookup", Input: sdk.ParseToolArguments(`{"q":"weather"}`)}}}
}

// gateTool blocks each execution until released, so tests can act mid-step.
type gateTool struct {
	started chan struct{}
	release chan struct{}
}

func (t *gateTool) Ref() run.ToolRef { return "lookup" }
func (t *gateTool) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: "lookup", Parameters: &jsonschema.Schema{Type: "object"}}
}
func (t *gateTool) ResponsePolicy() run.ResponsePolicy        { return run.DirectExecution }
func (t *gateTool) Replay() run.ReplayPolicy                  { return run.ReplayUnknown }
func (t *gateTool) Placement() run.ToolPlacement              { return run.PlacementProcess }
func (t *gateTool) ValidateArguments(run.CanonicalJSON) error { return nil }
func (t *gateTool) Execute(_ context.Context, req loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	t.started <- struct{}{}
	<-t.release
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: req.Arguments}}
}

func messageText(m sdk.Message) string {
	var b strings.Builder
	for _, part := range m.Content {
		if t, ok := part.(sdk.TextPart); ok {
			b.WriteString(t.Text)
		}
	}
	return b.String()
}

func roles(msgs []sdk.Message) []string {
	out := make([]string, len(msgs))
	for i, m := range msgs {
		out[i] = string(m.Role)
	}
	return out
}

func waitUntil(cond func() bool) {
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			panic("condition not reached")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func errorsIsOwnershipLost(err error) string {
	if err == nil {
		return "no error"
	}
	if errors.Is(err, runtime.ErrOwnershipLost) {
		return "ownership lost"
	}
	return err.Error()
}
