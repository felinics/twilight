// Package tools holds the workspace-placed tools of the reference agent:
// tools that run inside the Environment the Session's workspace is
// materialized in (run.PlacementWorkspace). A Tool declares itself like a
// loop.ExecutableTool but executes against an Environment the sandbox
// backend resolves from the Assignment's target; it never sees the host.
package tools

import (
	"context"
	"errors"
	"fmt"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/sdk"
)

// Tool is one workspace-placed tool.
type Tool interface {
	Ref() run.ToolRef
	Definition() sdk.ToolDefinition
	ResponsePolicy() run.ResponsePolicy
	// Replay declares whether Run may happen again for the same call after
	// an earlier execution was lost (RUN-EXE-9).
	Replay() run.ReplayPolicy
	// ValidateArguments runs before the start barrier and touches no
	// environment.
	ValidateArguments(run.CanonicalJSON) error
	// Run executes the call inside env.
	Run(ctx context.Context, env environment.Environment, req *loop.ToolExecutionRequest) loop.ToolExecutionOutcome
}

// Default is the reference agent's workspace tool set.
func Default() []Tool { return []Tool{Shell{}, ReadFile{}, WriteFile{}, ListDir{}} }

func decode[T any](args run.CanonicalJSON, into *T) error {
	if args.IsZero() {
		return errors.New("arguments are required")
	}
	return args.Decode(into)
}

func succeed(v any) loop.ToolExecutionOutcome {
	out, err := run.CanonicalJSONFromValue(v)
	if err != nil {
		return fail(run.FailureInternal, err.Error(), run.RetryNever)
	}
	return loop.ToolExecutionSucceeded{Result: run.ToolExecutionResult{Output: out}}
}

func fail(class, message string, retry run.RetryDisposition) loop.ToolExecutionOutcome {
	return loop.ToolExecutionFailed{Failure: run.ToolFailure{Class: class, Message: message}, Retry: retry}
}

// fsOf returns the environment's FS capability or the failure a tool
// reports without it.
func fsOf(env environment.Environment) (fsys environment.FS, failure loop.ToolExecutionOutcome) {
	fs, ok := env.(environment.FS)
	if !ok {
		return nil, fail(run.FailureUnavailable, fmt.Sprintf("environment %s exposes no filesystem", env.Ref()), run.RetryNever)
	}
	return fs, nil
}

// fsFailure classifies an FS error.
func fsFailure(op, path string, err error) loop.ToolExecutionOutcome {
	switch {
	case errors.Is(err, environment.ErrOutsideRoot):
		return fail(run.FailureInvalidInput, err.Error(), run.RetryNever)
	case isNotExist(err):
		return fail(run.FailureNotFound, fmt.Sprintf("%s: %s does not exist", op, path), run.RetryNever)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return fail(run.FailureCancelled, err.Error(), run.RetryNever)
	default:
		return fail(run.FailureExecution, fmt.Sprintf("%s %s: %v", op, path, err), run.RetryUnknown)
	}
}
