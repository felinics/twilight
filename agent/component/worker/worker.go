// Package worker composes the executor worker component (CLD-EXE-1): an
// executor.Worker over the execution record store, routing model calls to
// the model backend and workspace-placed tool calls to the tool backend
// over the Backend protocol, exposed through the ExecutionPort's HTTP
// binding. The process holds no provider credential and no tool
// implementation.
package worker

import (
	"context"
	"errors"
	"fmt"
	stdhttp "net/http"
	"time"

	"github.com/felinics/twilight/agent/component/stores"
	"github.com/felinics/twilight/agent/config"
	"github.com/felinics/twilight/agent/executor/backendhttp"
	executorhttp "github.com/felinics/twilight/agent/executor/http"
	"github.com/felinics/twilight/agent/executor/sandbox"
	"github.com/felinics/twilight/agentcore/executor"
	executionstore "github.com/felinics/twilight/agentcore/executor/store"
)

// Config is the worker's document.
type Config struct {
	// Identity is this incarnation's WorkerOptions.ID; it must not be reused
	// while an older incarnation may be alive.
	Identity config.Identity `json:"identity"`
	Listen   string          `json:"listen"`
	// Executions is the execution record store.
	Executions config.Store `json:"executions"`
	// Lease is the record lease duration; zero selects the Worker's default.
	Lease config.Duration `json:"lease,omitempty"`
	// Backends are the Backend protocol endpoints: Model takes model calls,
	// Tool takes workspace-placed tool calls. Either may be empty, in which
	// case such calls have no route and are refused at Dispatch.
	Backends Backends `json:"backends"`
	// Retry is the re-dispatch budget after a Known retryable failure
	// (RUN-EXE-11); zero disables retries.
	Retry Retry `json:"retry,omitempty"`
}

// Backends are the worker's backend endpoints.
type Backends struct {
	Model string `json:"model,omitempty"`
	Tool  string `json:"tool,omitempty"`
}

// Retry is executor.RetryBudget in the document.
type Retry struct {
	MaxAttempts int             `json:"maxAttempts,omitempty"`
	Backoff     config.Duration `json:"backoff,omitempty"`
}

// Component is the composed worker.
type Component struct {
	Worker *executor.Worker
	server *executorhttp.Server
	closes []func() error
}

// Options compose a Component from ready dependencies; Compose builds them
// from a Config.
type Options struct {
	ID         string
	Executions executionstore.Store
	// Routes are the Worker's routes, first match wins.
	Routes []executor.Route
	Lease  time.Duration
	Retry  executor.RetryBudget
	// Clock is the lease clock; nil selects time.Now. It must agree with
	// the execution store's.
	Clock func() time.Time
}

// New composes a worker from ready dependencies.
func New(ctx context.Context, opts Options) (*Component, error) {
	if opts.ID == "" || opts.Executions == nil {
		return nil, errors.New("worker: an identity and an execution store are required")
	}
	if len(opts.Routes) == 0 {
		return nil, errors.New("worker: at least one route is required")
	}
	w, err := executor.NewWorker(ctx, opts.Executions, opts.Routes, executor.WorkerOptions{ID: opts.ID, LeaseDuration: opts.Lease, Retry: opts.Retry, Clock: opts.Clock})
	if err != nil {
		return nil, err
	}
	return &Component{Worker: w, server: &executorhttp.Server{Worker: w}}, nil
}

// Compose builds the worker its Config describes.
func Compose(ctx context.Context, cfg Config) (*Component, error) {
	id, err := cfg.Identity.Resolve()
	if err != nil {
		return nil, err
	}
	db, err := stores.Open(ctx, cfg.Executions)
	if err != nil {
		return nil, fmt.Errorf("worker: executions: %w", err)
	}
	c, err := New(ctx, Options{ID: id, Executions: db.Executions(), Routes: Routes(cfg.Backends), Lease: cfg.Lease.Std(),
		Retry: executor.RetryBudget{MaxAttempts: cfg.Retry.MaxAttempts, Backoff: cfg.Retry.Backoff.Std()}})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	c.closes = append(c.closes, db.Close)
	return c, nil
}

// Routes are the Worker routes to the configured backends: model calls by
// kind, tool calls by their workspace placement (RUN-LOP-9).
func Routes(b Backends) []executor.Route {
	var routes []executor.Route
	if b.Model != "" {
		routes = append(routes, executor.Route{Provider: "model", Match: executor.MatchModel, Backend: &backendhttp.Client{BaseURL: b.Model}})
	}
	if b.Tool != "" {
		routes = append(routes, sandbox.Route(&backendhttp.Client{BaseURL: b.Tool}))
	}
	return routes
}

// Handler is the ExecutionPort's HTTP binding.
func (c *Component) Handler() stdhttp.Handler { return c.server.Handler() }

// Close stops the Worker; its records keep their leases until they expire
// and the next incarnation adopts them (RUN-EXE-8).
func (c *Component) Close(context.Context) error {
	c.Worker.Close()
	var first error
	for _, close := range c.closes {
		if err := close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}
