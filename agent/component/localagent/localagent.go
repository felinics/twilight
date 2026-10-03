// Package localagent composes the local agent: the colocated model and
// tool executor, the execution Worker that owns the records of the effects
// dispatched here, the workspace sandbox backend when the workspace layer
// runs in this process -- and the application over them. It is the
// composition root of the single-process deployment; a host that serves
// the application remotely composes app.Application without it.
package localagent

import (
	"context"
	"errors"

	"github.com/felinics/twilight/agent/app"
	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agent/executor/local"
	"github.com/felinics/twilight/agent/executor/sandbox"
	"github.com/felinics/twilight/agent/tools"
	"github.com/felinics/twilight/agentcore/executor"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/effect"
)

// Config composes the local agent: the product assembly (Kernel, Execution
// policies, presets, inbox, activation, workspaces) plus this deployment's
// choices -- the effect implementation, the Worker over it and the sandbox
// backend.
type Config struct {
	app.Config

	// Port, when set, is the effect port the application drives: a custom
	// transport or a test double. Without extra routes (no Sandbox) it is
	// driven directly; with them, the Worker routes to them and to the Port
	// as its default backend. Nil composes a LocalExecutor from Models and
	// Tools behind the Worker.
	Port effect.ExecutionPort
	// Models and Tools build the LocalExecutor when Port is nil.
	Models map[run.ModelRef]local.ModelInvoker
	Tools  []local.ExecutableTool

	// Executions is the execution record store of the Worker (RUN-EXE-8):
	// required whenever a Worker is composed (always with a LocalExecutor;
	// with a Port when the Sandbox adds a route). Like every store it is
	// durable (OWN-PRT-3); a record that did not survive a restart would
	// let the Owner dispose an execution that is still running.
	Executions executionstore.Store
	// Worker tunes the composed Worker (lease, id, reconcile loop, clock).
	Worker executor.WorkerOptions

	// Sandbox, when set, composes the workspace sandbox backend beside the
	// Worker and serves the workspace-placed tool calls (APP-WSP-3).
	Sandbox *SandboxConfig
}

// SandboxConfig composes the workspace sandbox backend.
type SandboxConfig struct {
	// Provider materializes and attaches environments.
	Provider environment.Provider
	// Backend is the Provider's identity in RuntimeBindings.
	Backend environment.Backend
	// Tools are the workspace-placed tools the backend serves; nil selects
	// tools.Default.
	Tools []tools.Tool
}

func (c *SandboxConfig) tools() []tools.Tool {
	if c.Tools == nil {
		return tools.Default()
	}
	return c.Tools
}

// Agent is the composed local agent: the application plus the deployment
// components around it.
type Agent struct {
	*app.Application
	// Worker owns the execution records of the effects dispatched here.
	Worker *executor.Worker
	// sandbox is the composed workspace backend, if any.
	sandbox *sandbox.Backend
}

// Compose builds the local agent: the sandbox backend and the Worker when
// they apply, then the application over the decided effect port.
func Compose(c Config) (*Agent, error) { //nolint:gocritic // hugeParam: Config is a by-value options struct read once
	// One progress hub serves the Worker and every backend it routes to
	// (RUN-EXE-12).
	if c.Worker.Progress == nil {
		c.Worker.Progress = executor.NewProgressHub(0)
	}
	var routes []executor.Route
	ag := &Agent{}
	if c.Sandbox != nil {
		if c.Workspaces == nil || c.Workspaces.Store == nil {
			return nil, errors.New("localagent: Sandbox requires Config.Workspaces with a Store")
		}
		backend, err := sandbox.New(sandbox.Options{Workspaces: c.Workspaces.Store, Provider: c.Sandbox.Provider,
			Backend: c.Sandbox.Backend, Tools: c.Sandbox.tools(), Progress: c.Worker.Progress})
		if err != nil {
			return nil, err
		}
		ag.sandbox = backend
		routes = append(routes, sandbox.Route(backend))
		if c.Workspaces.Snapshots == nil {
			c.Workspaces.Snapshots = backend
		}
	}
	port := c.Port
	if port == nil {
		catalog, err := local.NewCatalog(c.Models, c.Tools...)
		if err != nil {
			ag.closeBackends()
			return nil, err
		}
		backend, err := local.NewLocalExecutor(catalog, catalog, c.Worker.Progress, true)
		if err != nil {
			ag.closeBackends()
			return nil, err
		}
		routes = append(routes, local.Route(backend))
	} else if len(routes) == 0 {
		c.Execution.Executor = port
	}
	if c.Execution.Executor == nil {
		// A Worker owns the execution records (RUN-EXE-8): the local
		// executor always, a Port with routes beside it.
		if c.Executions == nil {
			ag.closeBackends()
			return nil, errors.New("localagent: an execution record store (Config.Executions) is required when a Worker is composed")
		}
		w, err := executor.NewWorker(context.Background(), c.Executions, routes, c.Worker)
		if err != nil {
			ag.closeBackends()
			return nil, err
		}
		ag.Worker = w
		c.Execution.Executor = w
	}
	application, err := app.New(c.Config)
	if err != nil {
		ag.closeBackends()
		return nil, err
	}
	ag.Application = application
	return ag, nil
}

// closeBackends releases the deployment components Compose created: the
// Worker before the sandbox backend (the Worker's routes may still be
// settling through it).
func (ag *Agent) closeBackends() {
	if ag.Worker != nil {
		ag.Worker.Close()
	}
	if ag.sandbox != nil {
		_ = ag.sandbox.Close(context.Background())
	}
}

// Close closes the application, then the Worker and the sandbox backend:
// the drives may still be settling outcomes through them (RUN-EXE-8).
func (ag *Agent) Close(ctx context.Context) error {
	err := ag.Application.Close(ctx)
	if ag.Worker != nil {
		ag.Worker.Close()
	}
	if ag.sandbox != nil {
		if cerr := ag.sandbox.Close(ctx); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}
