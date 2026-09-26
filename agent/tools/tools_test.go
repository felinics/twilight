package tools_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agent/environment/local"
	"github.com/felinics/twilight/agent/tools"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
)

func newEnv(t *testing.T) environment.Environment {
	t.Helper()
	p, err := local.New(filepath.Join(t.TempDir(), "envs"))
	if err != nil {
		t.Fatal(err)
	}
	env, err := p.Create(context.Background(), environment.Spec{Subject: "ws"})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

func runTool(t *testing.T, tool tools.Tool, env environment.Environment, args string) loop.ToolExecutionOutcome {
	t.Helper()
	a := run.MustParseCanonicalJSON(args)
	if err := tool.ValidateArguments(a); err != nil {
		t.Fatalf("%s validate %s = %v", tool.Ref(), args, err)
	}
	return tool.Run(context.Background(), env, &loop.ToolExecutionRequest{RunID: "r", StepID: "s", CallID: "c", ToolRef: tool.Ref(), Arguments: a})
}

func output(t *testing.T, out loop.ToolExecutionOutcome) string {
	t.Helper()
	ok, is := out.(loop.ToolExecutionSucceeded)
	if !is {
		t.Fatalf("outcome = %#v, want success", out)
	}
	return ok.Result.Output.String()
}

func failure(t *testing.T, out loop.ToolExecutionOutcome) run.ToolFailure {
	t.Helper()
	f, is := out.(loop.ToolExecutionFailed)
	if !is {
		t.Fatalf("outcome = %#v, want failure", out)
	}
	return f.Failure
}

// The four tools declare workspace placement through their backend, run
// against the environment's capabilities and classify their failures.
func TestWorkspaceTools(t *testing.T) {
	env := newEnv(t)
	for _, tool := range tools.Default() {
		if tool.Definition().Name != string(tool.Ref()) {
			t.Fatalf("%s definition name = %q", tool.Ref(), tool.Definition().Name)
		}
		if err := tool.ValidateArguments(run.CanonicalJSON{}); err == nil && tool.Ref() != tools.ListDirRef {
			t.Fatalf("%s accepted empty arguments", tool.Ref())
		}
	}
	if got := output(t, runTool(t, tools.WriteFile{}, env, `{"path":"a/b.txt","content":"one\ntwo"}`)); !strings.Contains(got, `"bytes":7`) {
		t.Fatalf("write = %s", got)
	}
	if got := output(t, runTool(t, tools.ReadFile{}, env, `{"path":"a/b.txt"}`)); !strings.Contains(got, `"content":"one\ntwo"`) {
		t.Fatalf("read = %s", got)
	}
	if got := output(t, runTool(t, tools.ListDir{}, env, `{"path":"a"}`)); !strings.Contains(got, `"name":"b.txt"`) {
		t.Fatalf("list = %s", got)
	}
	if got := output(t, runTool(t, tools.ListDir{}, env, `{}`)); !strings.Contains(got, `"name":"a"`) {
		t.Fatalf("list root = %s", got)
	}
	if got := output(t, runTool(t, tools.Shell{}, env, `{"command":"wc -l < b.txt; exit 2","cwd":"a"}`)); !strings.Contains(got, `"exitCode":2`) || !strings.Contains(got, `1`) {
		t.Fatalf("shell = %s", got)
	}
	cases := []struct {
		name  string
		tool  tools.Tool
		args  string
		class string
	}{
		{"read missing", tools.ReadFile{}, `{"path":"nope.txt"}`, run.FailureNotFound},
		{"read outside", tools.ReadFile{}, `{"path":"../x"}`, run.FailureInvalidInput},
		{"list missing", tools.ListDir{}, `{"path":"nope"}`, run.FailureNotFound},
		{"shell timeout", tools.Shell{}, `{"command":"sleep 5","timeout_ms":50}`, run.FailureTimeout},
		{"shell outside", tools.Shell{}, `{"command":"true","cwd":".."}`, run.FailureInvalidInput},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if f := failure(t, runTool(t, tc.tool, env, tc.args)); f.Class != tc.class {
				t.Fatalf("failure = %+v, want class %s", f, tc.class)
			}
		})
	}
	// An environment without the capability is unavailable, never a crash.
	bare := bareEnvironment{}
	if f := failure(t, tools.Shell{}.Run(context.Background(), bare, &loop.ToolExecutionRequest{Arguments: run.MustParseCanonicalJSON(`{"command":"true"}`)})); f.Class != run.FailureUnavailable {
		t.Fatalf("shell without an executor = %+v", f)
	}
	if f := failure(t, tools.ReadFile{}.Run(context.Background(), bare, &loop.ToolExecutionRequest{Arguments: run.MustParseCanonicalJSON(`{"path":"x"}`)})); f.Class != run.FailureUnavailable {
		t.Fatalf("read without a filesystem = %+v", f)
	}
}

type bareEnvironment struct{}

func (bareEnvironment) Ref() environment.EnvironmentRef { return "bare" }
func (bareEnvironment) Close(context.Context) error     { return nil }
