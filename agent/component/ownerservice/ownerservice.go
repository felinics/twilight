// Package ownerservice composes the owner service component (CLD-OWN-1):
// an app.Application over the shared stores, driving effects through the
// worker's ExecutionPort client, exposed through the command face. The
// process holds Session leases and the Writers' projections; every durable
// fact is in the stores.
package ownerservice

import (
	"context"
	"errors"
	"fmt"
	stdhttp "net/http"
	"time"

	"github.com/felinics/twilight/agent/app"
	ownerhttp "github.com/felinics/twilight/agent/app/http"
	"github.com/felinics/twilight/agent/component/stores"
	"github.com/felinics/twilight/agent/config"
	wshttp "github.com/felinics/twilight/agent/workspace/http"
	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/owner"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/filestore"
	runmod "github.com/felinics/twilight/agentcore/session/run"
	"github.com/felinics/twilight/agentcore/turn"
)

// Config is the owner service's document.
type Config struct {
	// Identity is the lease owner name (SES-OWN-1) a gateway routes by; a
	// pod name through the downward API in a cluster.
	Identity config.Identity `json:"identity"`
	Listen   string          `json:"listen"`
	// Sessions is the Session ledger root (filestore) when Stores is a
	// SQLite file; a Postgres database holds the ledger itself and leaves
	// it unset.
	Sessions Filestore `json:"sessions,omitempty"`
	// Content is the frozen bodies' cas root (filestore), under the same
	// rule as Sessions.
	Content Filestore `json:"content,omitempty"`
	// Stores is the durable database: artifact bindings and claims,
	// dispatch ledger, inbox, workspace records, and with Postgres the
	// Session ledger and cas content too (CLD-STO-1).
	Stores config.Store `json:"stores"`
	// Executor is the worker's ExecutionPort endpoint.
	Executor string `json:"executor"`
	// ToolBackend is the tool backend's endpoint, for workspace snapshots;
	// empty leaves snapshots unavailable.
	ToolBackend string `json:"toolBackend,omitempty"`
	// Lease is the Session lease duration; zero means until Release.
	Lease config.Duration `json:"lease,omitempty"`
	// Takeover makes every Open take over a live lease (CLD-CTL-2); a
	// deployment sets it on the owner the controller hands a Session to.
	Takeover bool `json:"takeover,omitempty"`
	// Presets are the decision identities registered at start.
	Presets []Preset `json:"presets"`
	// SnapshotAfterTurn snapshots the bound workspace after each settled
	// Turn (APP-WSP-7); needs ToolBackend.
	SnapshotAfterTurn bool `json:"snapshotAfterTurn,omitempty"`
	// InboxPoll is the applier's poll interval (APP-INB-3).
	InboxPoll config.Duration `json:"inboxPoll,omitempty"`
	// Activation makes this owner hold Sessions for active work only
	// (APP-ACT): a command reaching it opens the Session, quiescence
	// releases it, and a scan picks up Sessions left by a dead owner. It
	// requires Lease, so a dead owner's Sessions expire.
	Activation *Activation `json:"activation,omitempty"`
}

// Activation is the owner's activation model (APP-ACT).
type Activation struct {
	// Preset is the decision identity an activation opens with.
	Preset turn.PresetID `json:"preset"`
	// IdleRelease is the quiescence after which ownership is released;
	// zero keeps activated Sessions open.
	IdleRelease config.Duration `json:"idleRelease,omitempty"`
	// Scan is the period of the scan over pending inboxes and expired
	// leases; zero disables it.
	Scan config.Duration `json:"scan,omitempty"`
	// ScanLimit bounds each scan's page from each source; zero selects
	// app.DefaultScanLimit.
	ScanLimit int `json:"scanLimit,omitempty"`
}

// Filestore names a file-backed store root.
type Filestore struct {
	Root string `json:"root"`
}

// Preset is one decision identity of the document.
type Preset struct {
	ID           turn.PresetID `json:"id"`
	Model        run.ModelRef  `json:"model"`
	SystemPrompt string        `json:"systemPrompt,omitempty"`
	// WorkspaceTools adds the workspace tool set (APP-WSP-3).
	WorkspaceTools bool `json:"workspaceTools,omitempty"`
}

// Component is the composed owner service.
type Component struct {
	App    *app.Application
	server *ownerhttp.Server
	closes []func() error
}

// New composes the owner service over a built Application.
func New(a *app.Application, sessionOptions app.SessionOptions) *Component {
	return &Component{App: a, server: &ownerhttp.Server{App: a, Options: sessionOptions}}
}

// Compose builds the owner service its Config describes.
func Compose(ctx context.Context, cfg Config) (*Component, error) { //nolint:gocritic // hugeParam: Config is a by-value document read once
	id, err := cfg.Identity.Resolve()
	if err != nil {
		return nil, err
	}
	switch {
	case cfg.Executor == "":
		return nil, errors.New("ownerservice: executor endpoint is required")
	case len(cfg.Presets) == 0:
		return nil, errors.New("ownerservice: at least one preset is required")
	case cfg.Activation != nil && cfg.Activation.Preset == "":
		return nil, errors.New("ownerservice: activation.preset is required")
	case cfg.Activation != nil && cfg.Lease.Std() <= 0:
		return nil, errors.New("ownerservice: activation requires a lease duration, so a dead owner's sessions expire")
	}
	var activation *app.Activation
	if cfg.Activation != nil {
		activation = &app.Activation{Preset: cfg.Activation.Preset, IdleRelease: cfg.Activation.IdleRelease.Std(),
			Scan: cfg.Activation.Scan.Std(), ScanLimit: cfg.Activation.ScanLimit, Options: app.SessionOptions{InboxPoll: cfg.InboxPoll.Std()}}
	}
	db, err := stores.Open(ctx, cfg.Stores)
	if err != nil {
		return nil, err
	}
	store, content, err := sessionStores(&cfg, db)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	bindings := db.Bindings()
	wsCfg := &app.WorkspaceConfig{Store: db.Workspaces(), SnapshotAfterTurn: cfg.SnapshotAfterTurn}
	if cfg.ToolBackend != "" {
		wsCfg.Snapshots = &wshttp.Client{BaseURL: cfg.ToolBackend}
	}
	presets, err := presetsOf(cfg.Presets)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	a, err := app.Build(app.Config{
		Store:      store,
		Content:    content,
		Artifacts:  owner.Artifacts{Bindings: bindings, Ledger: db.Ledger(artifact.SetBuilder{Resolver: bindings})},
		Processes:  db.Processes(),
		Inbox:      db.Inbox(),
		Executor:   app.ExecutorConfig{Mode: app.ExecutorRemote, Endpoint: cfg.Executor},
		Workspaces: wsCfg,
		Ownership:  session.OpenOptions{Owner: id, LeaseDuration: cfg.Lease.Std(), Takeover: cfg.Takeover},
		Presets:    presets,
		Activation: activation,
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	c := New(a, app.SessionOptions{InboxPoll: cfg.InboxPoll.Std()})
	c.closes = append(c.closes, db.Close)
	return c, nil
}

// sessionStores is the Session ledger and the cas content: the shared
// database's own when it is one, filestore directories beside a SQLite
// file otherwise.
func sessionStores(cfg *Config, db stores.Handle) (session.Store, artifact.ContentStore, error) {
	if shared, ok := db.(stores.Shared); ok {
		if cfg.Sessions.Root != "" || cfg.Content.Root != "" {
			return nil, nil, errors.New("ownerservice: sessions.root and content.root are not used with stores.postgres")
		}
		content, err := shared.Content(runmod.FrozenAuthority)
		if err != nil {
			return nil, nil, err
		}
		return shared.Sessions(), content, nil
	}
	if cfg.Sessions.Root == "" || cfg.Content.Root == "" {
		return nil, nil, errors.New("ownerservice: sessions.root and content.root are required with stores.sqlite")
	}
	store, err := filestore.New(cfg.Sessions.Root)
	if err != nil {
		return nil, nil, err
	}
	content, err := filestore.NewContentStore(cfg.Content.Root, runmod.FrozenAuthority, filestore.ContentStoreOptions{})
	if err != nil {
		return nil, nil, err
	}
	return store, content, nil
}

func presetsOf(defs []Preset) ([]app.Preset, error) {
	out := make([]app.Preset, 0, len(defs))
	for _, d := range defs {
		if d.ID == "" || d.Model == "" {
			return nil, fmt.Errorf("ownerservice: preset %q needs an id and a model", d.ID)
		}
		opts := []app.PresetOption{}
		if d.SystemPrompt != "" {
			opts = append(opts, app.WithSystemPrompt(d.SystemPrompt))
		}
		if d.WorkspaceTools {
			defs, err := app.WorkspaceTools(nil)
			if err != nil {
				return nil, err
			}
			opts = append(opts, app.WithPublicTools(defs...))
		}
		p, err := app.NewPreset(d.Model, nil, opts...)
		if err != nil {
			return nil, fmt.Errorf("ownerservice: preset %s: %w", d.ID, err)
		}
		out = append(out, app.Preset{ID: d.ID, Value: p})
	}
	return out, nil
}

// Handler is the command face.
func (c *Component) Handler() stdhttp.Handler { return c.server.Handler() }

// Close releases every Session this owner holds and the stores.
func (c *Component) Close(ctx context.Context) error {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
	}
	err := c.App.Close(ctx)
	for _, close := range c.closes {
		if cerr := close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}
