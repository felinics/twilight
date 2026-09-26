package tools

import (
	"context"
	"errors"
	"io/fs"

	"github.com/google/jsonschema-go/jsonschema"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/run/loop"
	"github.com/felinics/twilight/sdk"
)

// The tools' ToolRefs.
const (
	ReadFileRef  run.ToolRef = "read_file"
	WriteFileRef run.ToolRef = "write_file"
	ListDirRef   run.ToolRef = "list_dir"
)

func isNotExist(err error) bool { return errors.Is(err, fs.ErrNotExist) }

func pathSchema(desc string, required bool, extra map[string]*jsonschema.Schema) *jsonschema.Schema {
	props := map[string]*jsonschema.Schema{"path": {Type: "string", Description: desc}}
	for k, v := range extra {
		props[k] = v
	}
	s := &jsonschema.Schema{Type: "object", Properties: props, AdditionalProperties: &jsonschema.Schema{Not: &jsonschema.Schema{}}}
	if required {
		s.Required = []string{"path"}
	}
	return s
}

type pathArgs struct {
	Path string `json:"path"`
}

// ReadFile returns a file's content; reading again after a lost execution
// has no second effect.
type ReadFile struct{}

func (ReadFile) Ref() run.ToolRef { return ReadFileRef }
func (ReadFile) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: string(ReadFileRef), Description: "Read a file of the workspace.",
		Parameters: pathSchema("Path relative to the workspace root.", true, nil)}
}
func (ReadFile) ResponsePolicy() run.ResponsePolicy { return run.DirectExecution }
func (ReadFile) Replay() run.ReplayPolicy           { return run.ReplayAllowed }
func (ReadFile) ValidateArguments(args run.CanonicalJSON) error {
	var a pathArgs
	if err := decode(args, &a); err != nil {
		return err
	}
	if a.Path == "" {
		return errors.New("path is required")
	}
	return nil
}
func (ReadFile) Run(ctx context.Context, env environment.Environment, req *loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	var a pathArgs
	if err := decode(req.Arguments, &a); err != nil {
		return fail(run.FailureInvalidArguments, err.Error(), run.RetryNever)
	}
	fsys, failure := fsOf(env)
	if failure != nil {
		return failure
	}
	data, err := fsys.ReadFile(ctx, a.Path)
	if err != nil {
		return fsFailure("read", a.Path, err)
	}
	return succeed(struct {
		Path    string `json:"path"`
		Content string `json:"content"`
	}{a.Path, string(data)})
}

// WriteFile creates or replaces a file; a lost execution may have written
// it, so it is not replayed.
type WriteFile struct{}

type writeArgs struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

func (WriteFile) Ref() run.ToolRef { return WriteFileRef }
func (WriteFile) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: string(WriteFileRef), Description: "Create or replace a file of the workspace with the given content.",
		Parameters: &jsonschema.Schema{Type: "object",
			Properties: map[string]*jsonschema.Schema{
				"path":    {Type: "string", Description: "Path relative to the workspace root; missing directories are created."},
				"content": {Type: "string", Description: "The whole new content of the file."},
			},
			Required: []string{"path", "content"}, AdditionalProperties: &jsonschema.Schema{Not: &jsonschema.Schema{}}}}
}
func (WriteFile) ResponsePolicy() run.ResponsePolicy { return run.DirectExecution }
func (WriteFile) Replay() run.ReplayPolicy           { return run.ReplayForbidden }
func (WriteFile) ValidateArguments(args run.CanonicalJSON) error {
	var a writeArgs
	if err := decode(args, &a); err != nil {
		return err
	}
	if a.Path == "" {
		return errors.New("path is required")
	}
	return nil
}
func (WriteFile) Run(ctx context.Context, env environment.Environment, req *loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	var a writeArgs
	if err := decode(req.Arguments, &a); err != nil {
		return fail(run.FailureInvalidArguments, err.Error(), run.RetryNever)
	}
	fsys, failure := fsOf(env)
	if failure != nil {
		return failure
	}
	if err := fsys.WriteFile(ctx, a.Path, []byte(a.Content)); err != nil {
		return fsFailure("write", a.Path, err)
	}
	return succeed(struct {
		Path  string `json:"path"`
		Bytes int    `json:"bytes"`
	}{a.Path, len(a.Content)})
}

// ListDir lists a directory; a repeated listing has no second effect.
type ListDir struct{}

func (ListDir) Ref() run.ToolRef { return ListDirRef }
func (ListDir) Definition() sdk.ToolDefinition {
	return sdk.ToolDefinition{Name: string(ListDirRef), Description: "List the entries of a workspace directory.",
		Parameters: pathSchema("Directory relative to the workspace root; default is the root.", false, nil)}
}
func (ListDir) ResponsePolicy() run.ResponsePolicy { return run.DirectExecution }
func (ListDir) Replay() run.ReplayPolicy           { return run.ReplayAllowed }
func (ListDir) ValidateArguments(args run.CanonicalJSON) error {
	var a pathArgs
	if args.IsZero() {
		return nil
	}
	return args.Decode(&a)
}
func (ListDir) Run(ctx context.Context, env environment.Environment, req *loop.ToolExecutionRequest) loop.ToolExecutionOutcome {
	var a pathArgs
	if !req.Arguments.IsZero() {
		if err := req.Arguments.Decode(&a); err != nil {
			return fail(run.FailureInvalidArguments, err.Error(), run.RetryNever)
		}
	}
	fsys, failure := fsOf(env)
	if failure != nil {
		return failure
	}
	entries, err := fsys.ReadDir(ctx, a.Path)
	if err != nil {
		return fsFailure("list", a.Path, err)
	}
	if entries == nil {
		entries = []environment.DirEntry{}
	}
	return succeed(struct {
		Path    string                 `json:"path"`
		Entries []environment.DirEntry `json:"entries"`
	}{a.Path, entries})
}
