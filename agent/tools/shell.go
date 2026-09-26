package tools

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/jsonschema-go/jsonschema"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/sdk"
)

// Shell runs a command line with the environment's shell in the workspace.
// A non-zero exit is a result the model reads; a run may not be replayed
// after it was lost, since the command may have had its effect.
type Shell struct{}

// ShellRef is the tool's ToolRef.
const ShellRef run.ToolRef = "shell"

// DefaultShellTimeout bounds a command whose arguments give no timeout.
const DefaultShellTimeout = 2 * time.Minute

type shellArgs struct {
	Command   string `json:"command"`
	Cwd       string `json:"cwd,omitempty"`
	TimeoutMS int64  `json:"timeout_ms,omitempty"`
}

func (Shell) Ref() run.ToolRef { return ShellRef }
func (Shell) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{
		Name:        string(ShellRef),
		Description: "Run a shell command in the workspace and return its exit code, stdout and stderr.",
		Parameters: &jsonschema.Schema{
			Type: "object",
			Properties: map[string]*jsonschema.Schema{
				"command":    {Type: "string", Description: "The command line, run with sh -c."},
				"cwd":        {Type: "string", Description: "Working directory relative to the workspace root; default is the root."},
				"timeout_ms": {Type: "integer", Description: "Kill the command after this many milliseconds."},
			},
			Required:             []string{"command"},
			AdditionalProperties: &jsonschema.Schema{Not: &jsonschema.Schema{}},
		},
	}
}
func (Shell) ResponsePolicy() run.ResponsePolicy { return run.DirectExecution }
func (Shell) Replay() run.ReplayPolicy           { return run.ReplayForbidden }
func (Shell) ValidateArguments(args run.CanonicalJSON) error {
	var a shellArgs
	if err := decode(args, &a); err != nil {
		return err
	}
	if a.Command == "" {
		return errors.New("command is required")
	}
	if a.TimeoutMS < 0 {
		return errors.New("timeout_ms must not be negative")
	}
	return nil
}

func (Shell) Run(ctx context.Context, env environment.Environment, req *loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	var a shellArgs
	if err := decode(req.Arguments, &a); err != nil {
		return fail(run.FailureInvalidArguments, err.Error(), run.RetryNever)
	}
	exe, ok := env.(environment.Executor)
	if !ok {
		return fail(run.FailureUnavailable, fmt.Sprintf("environment %s cannot run commands", env.Ref()), run.RetryNever)
	}
	timeout := DefaultShellTimeout
	if a.TimeoutMS > 0 {
		timeout = time.Duration(a.TimeoutMS) * time.Millisecond
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	res, err := exe.Exec(runCtx, environment.ExecSpec{Argv: []string{"sh", "-c", a.Command}, Cwd: a.Cwd})
	switch {
	case err == nil:
	case errors.Is(err, environment.ErrOutsideRoot):
		return fail(run.FailureInvalidInput, err.Error(), run.RetryNever)
	case errors.Is(runCtx.Err(), context.DeadlineExceeded) && ctx.Err() == nil:
		return fail(run.FailureTimeout, fmt.Sprintf("command exceeded %s", timeout), run.RetryNever)
	case ctx.Err() != nil:
		return fail(run.FailureCancelled, ctx.Err().Error(), run.RetryNever)
	default:
		return fail(run.FailureExecution, err.Error(), run.RetryUnknown)
	}
	return succeed(struct {
		ExitCode  int    `json:"exitCode"`
		Stdout    string `json:"stdout"`
		Stderr    string `json:"stderr"`
		Truncated bool   `json:"truncated,omitempty"`
	}{res.ExitCode, res.Stdout, res.Stderr, res.Truncated})
}
