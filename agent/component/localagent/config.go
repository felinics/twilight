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
	"github.com/felinics/twilight/agentcore/session/writer"
	"github.com/felinics/twilight/agentcore/sessionkernel"
)

// FileConfig is the user-facing configuration of a single-process local
// agent. It intentionally describes the product, not its internal Worker,
// model-backend, and tool-backend processes.
type FileConfig struct {
	Server    ServerConfig    `json:"server,omitempty"`
	Workspace WorkspaceConfig `json:"workspace,omitempty"`
	Storage   StorageConfig   `json:"storage,omitempty"`
	Models    ModelsConfig    `json:"models,omitempty"`
	Agent     AgentConfig     `json:"agent,omitempty"`
}

// ServerConfig controls the one HTTP command API exposed by local mode.
type ServerConfig struct {
	Listen string `json:"listen,omitempty"`
}

// WorkspaceConfig selects the directory operated on by workspace tools. An
// empty Path means the process working directory.
type WorkspaceConfig struct {
	Path  string `json:"path,omitempty"`
	Tools *bool  `json:"tools,omitempty"`
}

// StorageConfig contains Twilight-owned state and never the user workspace.
type StorageConfig struct {
	DataDir string `json:"dataDir,omitempty"`
	// Debug writes a human-readable sidecar trace while retaining the
	// canonical ledger and content-addressed state for recovery.
	Debug bool `json:"debug,omitempty"`
}

// ModelsConfig selects the model catalog and its credential directory. Local
// mode falls back to an environment variable named by the catalog entry when
// the credential file is absent.
type ModelsConfig struct {
	Catalog    string       `json:"catalog,omitempty"`
	SecretsDir string       `json:"secretsDir,omitempty"`
	Default    run.ModelRef `json:"default"`
}

// AgentConfig contains conversation and local ownership behavior.
type AgentConfig struct {
	SystemPrompt string          `json:"systemPrompt,omitempty"`
	Lease        config.Duration `json:"lease,omitempty"`
	InboxPoll    config.Duration `json:"inboxPoll,omitempty"`
}

const (
	defaultListen         = "127.0.0.1:8080"
	defaultLease          = 5 * time.Minute
	defaultInboxPoll      = time.Second
	defaultIdleRelease    = 30 * time.Second
	defaultActivationScan = 5 * time.Second
)

func (c FileConfig) defaults() (FileConfig, error) {
	paths, err := config.UserPaths()
	if err != nil {
		return FileConfig{}, err
	}
	if c.Server.Listen == "" {
		c.Server.Listen = defaultListen
	}
	if c.Storage.DataDir == "" {
		c.Storage.DataDir = paths.StateDir
	}
	if c.Workspace.Path == "" {
		c.Workspace.Path = "."
	}
	if c.Models.Catalog == "" {
		c.Models.Catalog = paths.Models
	}
	if c.Models.SecretsDir == "" {
		c.Models.SecretsDir = paths.Secrets
	}
	if c.Agent.Lease.Std() == 0 {
		c.Agent.Lease = config.Duration(defaultLease)
	}
	if c.Agent.InboxPoll.Std() == 0 {
		c.Agent.InboxPoll = config.Duration(defaultInboxPoll)
	}
	if c.Workspace.Tools == nil {
		enabled := true
		c.Workspace.Tools = &enabled
	}
	return c, nil
}

// ListenAddr returns the local command API address after applying its
// process-local default.
func (c FileConfig) ListenAddr() string {
	if c.Server.Listen == "" {
		return defaultListen
	}
	return c.Server.Listen
}

func (c FileConfig) validate() error {
	if c.Models.Default == "" {
		return errors.New("localagent: models.default is required")
	}
	if c.Storage.DataDir == "" || c.Workspace.Path == "" || c.Models.Catalog == "" || c.Models.SecretsDir == "" {
		return errors.New("localagent: storage.dataDir, workspace.path, models.catalog and models.secretsDir must not be empty")
	}
	if c.Agent.Lease.Std() <= 0 || c.Agent.InboxPoll.Std() <= 0 {
		return errors.New("localagent: agent.lease and agent.inboxPoll must be positive")
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
	cfg, err := raw.defaults()
	if err != nil {
		return nil, err
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.Storage.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("localagent: storage.dataDir: %w", err)
	}
	if err := os.MkdirAll(cfg.Models.SecretsDir, 0o700); err != nil {
		return nil, fmt.Errorf("localagent: models.secretsDir: %w", err)
	}

	entries, err := models.LoadFile(cfg.Models.Catalog)
	if err != nil {
		return nil, fmt.Errorf("localagent: catalog: %w", err)
	}
	catalog, err := models.Build(ctx, entries, secrets.Fallback{secrets.Dir(cfg.Models.SecretsDir), secrets.Env{}})
	if err != nil {
		return nil, fmt.Errorf("localagent: models: %w", err)
	}

	db, err := sqlite.Open(filepath.Join(cfg.Storage.DataDir, "agent.db"))
	if err != nil {
		return nil, fmt.Errorf("localagent: database: %w", err)
	}
	closeDB := func(e error) (*Service, error) {
		_ = db.Close()
		return nil, e
	}

	ledger, err := filestore.New(cfg.Storage.DataDir)
	if err != nil {
		return closeDB(fmt.Errorf("localagent: sessions: %w", err))
	}
	content, err := filestore.NewContentStore(filepath.Join(cfg.Storage.DataDir, "content"), sessionstore.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		return closeDB(fmt.Errorf("localagent: content: %w", err))
	}
	var observers []writer.CommitObserver
	if cfg.Storage.Debug {
		debug, debugErr := newDebugObserver(filepath.Join(cfg.Storage.DataDir, "debug"), content)
		if debugErr != nil {
			return closeDB(debugErr)
		}
		observers = append(observers, debug)
	}
	workspaceRoot, err := filepath.Abs(cfg.Workspace.Path)
	if err != nil {
		return closeDB(fmt.Errorf("localagent: workspace: %w", err))
	}
	provider, err := local.NewWorkspace(workspaceRoot)
	if err != nil {
		return closeDB(fmt.Errorf("localagent: workspace: %w", err))
	}

	defs, err := app.WorkspaceTools(nil)
	if err != nil {
		return closeDB(fmt.Errorf("localagent: workspace tools: %w", err))
	}
	presetOpts := []app.PresetOption{app.WithSystemPrompt(cfg.Agent.SystemPrompt)}
	if cfg.Workspace.Tools != nil && *cfg.Workspace.Tools {
		presetOpts = append(presetOpts, app.WithTools(defs...))
	}
	preset, err := app.NewPresetFromDefinitions(cfg.Models.Default, nil, presetOpts...)
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
			Ownership: session.OpenOptions{Owner: "local", LeaseDuration: cfg.Agent.Lease.Std()},
			Observers: observers,
		},
		Execution: rt.ExecutionConfig{Redispatches: db.Redispatches()},
		Inbox:     db.Inbox(),
		Presets:   []app.Preset{{ID: "default", Value: preset}},
		Activation: &app.Activation{
			Preset:      "default",
			IdleRelease: defaultIdleRelease,
			Scan:        defaultActivationScan,
			Options:     app.SessionOptions{InboxPoll: cfg.Agent.InboxPoll.Std()},
		},
		Workspaces: &app.WorkspaceConfig{Store: db.Workspaces(), SnapshotAfterTurn: false},
	}

	ag, err := Compose(Config{
		Config:     appCfg,
		Models:     catalog.Invokers(),
		Executions: db.Executions(),
		Worker:     executor.WorkerOptions{ID: "local", LeaseDuration: cfg.Agent.Lease.Std()},
		Sandbox:    &SandboxConfig{Provider: provider, Backend: local.Backend, Tools: tools.Default()},
	})
	if err != nil {
		return closeDB(fmt.Errorf("localagent: compose: %w", err))
	}
	return &Service{Agent: ag, server: &apphttp.Server{App: ag.Application, Options: app.SessionOptions{InboxPoll: cfg.Agent.InboxPoll.Std()}}, db: db}, nil
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
