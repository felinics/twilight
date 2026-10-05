package localagent

import (
	"context"
	"errors"
	"fmt"
	stdhttp "net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/felinics/twilight/agent/app"
	apphttp "github.com/felinics/twilight/agent/app/http"
	"github.com/felinics/twilight/agent/config"
	"github.com/felinics/twilight/agent/environment/local"
	"github.com/felinics/twilight/agent/models"
	"github.com/felinics/twilight/agent/secrets"
	"github.com/felinics/twilight/agent/store/sqlite"
	"github.com/felinics/twilight/agent/tools"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/executor"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	rt "github.com/felinics/twilight/agentcore/runtime"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore"
	"github.com/felinics/twilight/agentcore/sessionkernel"
)

// FileConfig is the user-facing configuration of a single-process local
// agent. It intentionally describes the product, not its internal Worker,
// model-backend, and tool-backend processes.
type FileConfig struct {
	// Listen is the only network address in local mode: the application's
	// command API. It defaults to 127.0.0.1:8080.
	Listen string `json:"listen,omitempty"`
	// Root contains the local database, Session ledger, CAS and workspaces.
	// It defaults to ./var/twilight.
	Root string `json:"root,omitempty"`
	// Catalog is an agent/models.File. It defaults to ./models.json.
	Catalog string `json:"catalog,omitempty"`
	// Secrets is a directory containing one file per model credential.
	Secrets string `json:"secrets,omitempty"`
	// Model is the logical model reference used by the default preset.
	Model        run.ModelRef `json:"model"`
	SystemPrompt string       `json:"systemPrompt,omitempty"`
	// WorkspaceTools enables the built-in local workspace tools. It defaults
	// to true; set it to false to run a model-only local agent.
	WorkspaceTools *bool `json:"workspaceTools,omitempty"`
	// Lease and InboxPoll tune local ownership. They are optional because
	// local mode has one owner and the useful defaults are safe.
	Lease     config.Duration `json:"lease,omitempty"`
	InboxPoll config.Duration `json:"inboxPoll,omitempty"`
}

const (
	defaultListen         = "127.0.0.1:8080"
	defaultRoot           = "./var/twilight"
	defaultCatalog        = "./models.json"
	defaultSecrets        = "./var/secrets"
	defaultLease          = 5 * time.Minute
	defaultInboxPoll      = time.Second
	defaultIdleRelease    = 30 * time.Second
	defaultActivationScan = 5 * time.Second
)

func (c FileConfig) defaults() FileConfig {
	if c.Listen == "" {
		c.Listen = defaultListen
	}
	if c.Root == "" {
		c.Root = defaultRoot
	}
	if c.Catalog == "" {
		c.Catalog = defaultCatalog
	}
	if c.Secrets == "" {
		c.Secrets = defaultSecrets
	}
	if c.Lease.Std() == 0 {
		c.Lease = config.Duration(defaultLease)
	}
	if c.InboxPoll.Std() == 0 {
		c.InboxPoll = config.Duration(defaultInboxPoll)
	}
	if c.WorkspaceTools == nil {
		enabled := true
		c.WorkspaceTools = &enabled
	}
	return c
}

func (c FileConfig) validate() error {
	if c.Model == "" {
		return errors.New("localagent: model is required")
	}
	if c.Root == "" || c.Catalog == "" || c.Secrets == "" {
		return errors.New("localagent: root, catalog and secrets must not be empty")
	}
	if c.Lease.Std() <= 0 || c.InboxPoll.Std() <= 0 {
		return errors.New("localagent: lease and inboxPoll must be positive")
	}
	return nil
}

// Service is the runnable local agent. All execution components are
// colocated; only the application command face is exposed over HTTP.
type Service struct {
	*Agent
	server *apphttp.Server
	db     *sqlite.DB
}

// ComposeFile builds a local agent from one user-facing configuration.
func ComposeFile(ctx context.Context, raw FileConfig) (*Service, error) {
	cfg := raw.defaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Root, 0o750); err != nil {
		return nil, fmt.Errorf("localagent: root: %w", err)
	}
	if err := os.MkdirAll(cfg.Secrets, 0o750); err != nil {
		return nil, fmt.Errorf("localagent: secrets: %w", err)
	}

	entries, err := models.LoadFile(cfg.Catalog)
	if err != nil {
		return nil, fmt.Errorf("localagent: catalog: %w", err)
	}
	catalog, err := models.Build(ctx, entries, secrets.Dir(cfg.Secrets))
	if err != nil {
		return nil, fmt.Errorf("localagent: models: %w", err)
	}

	db, err := sqlite.Open(filepath.Join(cfg.Root, "agent.db"))
	if err != nil {
		return nil, fmt.Errorf("localagent: database: %w", err)
	}
	closeDB := func(e error) (*Service, error) {
		_ = db.Close()
		return nil, e
	}

	ledger, err := filestore.New(filepath.Join(cfg.Root, "sessions"))
	if err != nil {
		return closeDB(fmt.Errorf("localagent: sessions: %w", err))
	}
	content, err := filestore.NewContentStore(filepath.Join(cfg.Root, "content"), sessionstore.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		return closeDB(fmt.Errorf("localagent: content: %w", err))
	}
	provider, err := local.New(filepath.Join(cfg.Root, "workspaces"))
	if err != nil {
		return closeDB(fmt.Errorf("localagent: workspaces: %w", err))
	}

	defs, err := app.WorkspaceTools(nil)
	if err != nil {
		return closeDB(fmt.Errorf("localagent: workspace tools: %w", err))
	}
	presetOpts := []app.PresetOption{app.WithSystemPrompt(cfg.SystemPrompt)}
	if cfg.WorkspaceTools != nil && *cfg.WorkspaceTools {
		presetOpts = append(presetOpts, app.WithTools(defs...))
	}
	preset, err := app.NewPresetFromDefinitions(cfg.Model, nil, presetOpts...)
	if err != nil {
		return closeDB(fmt.Errorf("localagent: preset: %w", err))
	}

	bindings := db.Bindings()
	appCfg := app.Config{
		Kernel: sessionkernel.Ports{
			Store:   ledger,
			Content: content,
			Artifacts: sessionkernel.Artifacts{
				Bindings: bindings,
				Ledger:   db.Ledger(artifact.SetBuilder{Resolver: bindings}),
			},
			Ownership: session.OpenOptions{Owner: "local", LeaseDuration: cfg.Lease.Std()},
		},
		Execution: rt.ExecutionConfig{Redispatches: db.Redispatches()},
		Inbox:     db.Inbox(),
		Presets:   []app.Preset{{ID: "default", Value: preset}},
		Activation: &app.Activation{
			Preset:      "default",
			IdleRelease: defaultIdleRelease,
			Scan:        defaultActivationScan,
			Options:     app.SessionOptions{InboxPoll: cfg.InboxPoll.Std()},
		},
		Workspaces: &app.WorkspaceConfig{Store: db.Workspaces(), SnapshotAfterTurn: true},
	}

	ag, err := Compose(Config{
		Config:     appCfg,
		Models:     catalog.Invokers(),
		Executions: db.Executions(),
		Worker:     executor.WorkerOptions{ID: "local", LeaseDuration: cfg.Lease.Std()},
		Sandbox:    &SandboxConfig{Provider: provider, Backend: local.Backend, Tools: tools.Default()},
	})
	if err != nil {
		return closeDB(fmt.Errorf("localagent: compose: %w", err))
	}
	return &Service{Agent: ag, server: &apphttp.Server{App: ag.Application, Options: app.SessionOptions{InboxPoll: cfg.InboxPoll.Std()}}, db: db}, nil
}

// Handler serves the single local command API.
func (s *Service) Handler() stdhttp.Handler { return s.server.Handler() }

// Close stops the application and its colocated execution components before
// closing the shared SQLite handle.
func (s *Service) Close(ctx context.Context) error {
	err := s.Agent.Close(ctx)
	if dbErr := s.db.Close(); err == nil {
		err = dbErr
	}
	return err
}
