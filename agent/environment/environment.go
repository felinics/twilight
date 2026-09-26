// Package environment defines the provider-neutral physical materialization
// boundary for logical workspaces. It deliberately does not depend on E2B,
// Docker, a VM SDK, or Agent Core effect types.
package environment

import (
	"context"
	"errors"
)

// Errors of the provider contract.
var (
	// ErrNotFound: the EnvironmentRef names no environment the provider
	// still has; the workspace must be materialized again.
	ErrNotFound = errors.New("environment: not found")
	// ErrUnsupported: the provider does not offer the operation (a Restore
	// without snapshot support, an Exec on a provider that only stores
	// files).
	ErrUnsupported = errors.New("environment: operation unsupported by this provider")
	// ErrOutsideRoot: a path escapes the environment's root.
	ErrOutsideRoot = errors.New("environment: path escapes the environment")
)

// Backend identifies an implementation of the runtime provider contract.
type Backend string

// EnvironmentRef identifies a provider environment. It is not an execution
// or tool-call identity.
type EnvironmentRef string

// StateRef identifies durable provider state that can materialize a new environment.
type StateRef string

// Binding is the durable association between a logical workspace and its
// current physical environment.
type Binding struct {
	Backend        Backend        `json:"backend"`
	EnvironmentRef EnvironmentRef `json:"environmentRef"`
	Generation     uint64         `json:"generation"`
}

// Spec is enough for a provider to materialize an environment without
// exposing provider-specific configuration to the workspace or Agent Core.
type Spec struct {
	Subject string
	Base    string
}

// RestoreSpec supplies a durable snapshot and the destination environment spec.
type RestoreSpec struct {
	State       StateRef
	Destination Spec
}

// Environment is a provider-owned materialized runtime. Execution operations
// remain in the executor/provider adapter; this interface only models the
// lifecycle needed for attach, restore, and replacement.
type Environment interface {
	Ref() EnvironmentRef
	Close(context.Context) error
}

// Provider creates, restores, and adopts physical environments. Implementations
// may use a local process, container, VM, or a cloud sandbox.
type Provider interface {
	Create(context.Context, Spec) (Environment, error)
	Restore(context.Context, RestoreSpec) (Environment, error)
	Attach(context.Context, EnvironmentRef) (Environment, error)
}

// --- capabilities ------------------------------------------------------------

// Executor is the capability of an Environment that runs commands. An
// Environment offers it by implementing the interface; a tool that needs it
// asserts for it and reports the environment as unavailable otherwise.
type Executor interface {
	Exec(context.Context, ExecSpec) (ExecResult, error)
}

// ExecSpec is one command inside the environment. Cwd is relative to the
// environment's root; MaxOutputBytes bounds each captured stream, zero
// selecting DefaultMaxOutputBytes.
type ExecSpec struct {
	Argv           []string
	Cwd            string
	Env            map[string]string
	Stdin          []byte
	MaxOutputBytes int64
}

// DefaultMaxOutputBytes bounds a captured stream when ExecSpec gives no cap.
const DefaultMaxOutputBytes int64 = 64 << 10

// ExecResult is what a finished command left: its exit code and the
// captured streams, cut at MaxOutputBytes when Truncated.
type ExecResult struct {
	ExitCode  int
	Stdout    string
	Stderr    string
	Truncated bool
}

// FS is the capability of an Environment that exposes its files. Paths are
// relative to the environment's root and may not escape it (ErrOutsideRoot).
type FS interface {
	ReadFile(ctx context.Context, path string) ([]byte, error)
	// WriteFile creates or replaces the file and its missing parents.
	WriteFile(ctx context.Context, path string, data []byte) error
	ReadDir(ctx context.Context, path string) ([]DirEntry, error)
}

// Snapshotter is the capability of an Environment whose provider can take a
// durable snapshot of it: Snapshot returns the StateRef a later Restore
// materializes from, independent of this environment's lifetime.
type Snapshotter interface {
	Snapshot(context.Context) (StateRef, error)
}

// DirEntry is one entry of ReadDir.
type DirEntry struct {
	Name string `json:"name"`
	Dir  bool   `json:"dir"`
	Size int64  `json:"size,omitempty"`
}
