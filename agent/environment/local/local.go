// Package local is the reference environment.Provider: one directory per
// environment under a root, commands run with os/exec in that directory,
// files read and written through the host filesystem, snapshots as copies
// of the directory under root/.snapshots. It serves one process;
// Attach from another host cannot see its directories, so a deployment with
// several sandbox backend replicas needs a provider whose environments are
// reachable from every replica.
package local

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/felinics/twilight/agent/environment"
)

// Backend is the provider's identity in RuntimeBindings.
const Backend environment.Backend = "local"

// Provider materializes environments as directories under Root.
type Provider struct{ root string }

var _ environment.Provider = (*Provider)(nil)

// New returns a Provider over root, creating it when missing.
func New(root string) (*Provider, error) {
	if root == "" {
		return nil, errors.New("local: empty root")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o750); err != nil {
		return nil, err
	}
	return &Provider{root: abs}, nil
}

// Create materializes an empty directory. A Spec with a Base is
// ErrUnsupported: this provider has no notion of a revision to check out.
func (p *Provider) Create(_ context.Context, spec environment.Spec) (environment.Environment, error) {
	if spec.Base != "" {
		return nil, fmt.Errorf("%w: local provider cannot materialize base %q", environment.ErrUnsupported, spec.Base)
	}
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return nil, err
	}
	ref := environment.EnvironmentRef("env-" + hex.EncodeToString(b[:]))
	dir := filepath.Join(p.root, string(ref))
	if err := os.Mkdir(dir, 0o750); err != nil {
		return nil, err
	}
	return &Environment{ref: ref, dir: dir}, nil
}

// snapshotsDir holds the snapshots; its name is not a valid environment ref.
const snapshotsDir = ".snapshots"

// Restore materializes a new directory from the snapshot State names; an
// unknown State is ErrNotFound. Destination.Base is ignored: the snapshot
// already carries the workspace's files.
func (p *Provider) Restore(ctx context.Context, spec environment.RestoreSpec) (environment.Environment, error) {
	src, err := p.snapshotPath(spec.State)
	if err != nil {
		return nil, err
	}
	created, err := p.Create(ctx, environment.Spec{Subject: spec.Destination.Subject})
	if err != nil {
		return nil, err
	}
	env, ok := created.(*Environment)
	if !ok {
		return nil, fmt.Errorf("local: create returned %T", created)
	}
	if err := copyTree(src, env.dir); err != nil {
		return nil, fmt.Errorf("local: restore %s: %w", spec.State, err)
	}
	return env, nil
}

func (p *Provider) snapshotPath(state environment.StateRef) (string, error) {
	name := string(state)
	if name == "" || name != filepath.Base(name) || strings.HasPrefix(name, ".") {
		return "", fmt.Errorf("%w: malformed snapshot state %q", environment.ErrNotFound, state)
	}
	dir := filepath.Join(p.root, snapshotsDir, name)
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return "", fmt.Errorf("%w: snapshot %s", environment.ErrNotFound, state)
	}
	return dir, nil
}

// copyTree copies the files and directories under src into dst, which is
// empty; execute bits are kept, symbolic links are copied as the files they
// point to.
func copyTree(src, dst string) error {
	return os.CopyFS(dst, os.DirFS(src))
}

// Attach adopts the directory ref names; a missing one is ErrNotFound.
func (p *Provider) Attach(_ context.Context, ref environment.EnvironmentRef) (environment.Environment, error) {
	name := string(ref)
	if name == "" || name != filepath.Base(name) || strings.HasPrefix(name, ".") {
		return nil, fmt.Errorf("%w: malformed environment ref %q", environment.ErrNotFound, ref)
	}
	dir := filepath.Join(p.root, name)
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil, fmt.Errorf("%w: %s", environment.ErrNotFound, ref)
	}
	return &Environment{ref: ref, dir: dir}, nil
}

// Environment is one directory. It offers environment.Executor and
// environment.FS.
type Environment struct {
	ref environment.EnvironmentRef
	dir string
}

var (
	_ environment.Environment = (*Environment)(nil)
	_ environment.Executor    = (*Environment)(nil)
	_ environment.FS          = (*Environment)(nil)
	_ environment.Snapshotter = (*Environment)(nil)
)

func (e *Environment) Ref() environment.EnvironmentRef { return e.ref }

// Dir is the environment's directory on the host.
func (e *Environment) Dir() string { return e.dir }

// Close keeps the directory: the workspace outlives any one attachment.
func (e *Environment) Close(context.Context) error { return nil }

// Snapshot copies the directory into the provider's snapshot area
// (environment.Snapshotter); the StateRef is the copy's name.
func (e *Environment) Snapshot(context.Context) (environment.StateRef, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	state := "snap-" + hex.EncodeToString(b[:])
	dir := filepath.Join(filepath.Dir(e.dir), snapshotsDir, state)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	if err := copyTree(e.dir, dir); err != nil {
		return "", fmt.Errorf("local: snapshot %s: %w", e.ref, err)
	}
	return environment.StateRef(state), nil
}

// resolve maps a relative path into the directory, refusing escapes.
func (e *Environment) resolve(rel string) (string, error) {
	if rel == "" {
		rel = "."
	}
	if filepath.IsAbs(rel) {
		return "", fmt.Errorf("%w: %q is absolute", environment.ErrOutsideRoot, rel)
	}
	clean := filepath.Clean(rel)
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %q", environment.ErrOutsideRoot, rel)
	}
	return filepath.Join(e.dir, clean), nil
}

// limitedBuffer keeps the first n bytes and records that more arrived.
type limitedBuffer struct {
	buf       bytes.Buffer
	limit     int64
	truncated bool
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	room := b.limit - int64(b.buf.Len())
	if room <= 0 {
		b.truncated = len(p) > 0 || b.truncated
		return len(p), nil
	}
	if int64(len(p)) > room {
		b.buf.Write(p[:room])
		b.truncated = true
		return len(p), nil
	}
	return b.buf.Write(p)
}

// Exec runs the command in the directory (environment.Executor). A non-zero
// exit is a result, not an error; a command that cannot start is an error.
func (e *Environment) Exec(ctx context.Context, spec environment.ExecSpec) (environment.ExecResult, error) {
	if len(spec.Argv) == 0 {
		return environment.ExecResult{}, errors.New("local: exec requires argv")
	}
	cwd, err := e.resolve(spec.Cwd)
	if err != nil {
		return environment.ExecResult{}, err
	}
	limit := spec.MaxOutputBytes
	if limit <= 0 {
		limit = environment.DefaultMaxOutputBytes
	}
	cmd := exec.CommandContext(ctx, spec.Argv[0], spec.Argv[1:]...) //nolint:gosec // G204: the workspace tool's purpose is to run the model's command inside the environment
	cmd.Dir = cwd
	cmd.Env = os.Environ()
	for k, v := range spec.Env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if len(spec.Stdin) > 0 {
		cmd.Stdin = bytes.NewReader(spec.Stdin)
	}
	stdout, stderr := &limitedBuffer{limit: limit}, &limitedBuffer{limit: limit}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	err = cmd.Run()
	res := environment.ExecResult{Stdout: stdout.buf.String(), Stderr: stderr.buf.String(), Truncated: stdout.truncated || stderr.truncated}
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		res.ExitCode = exit.ExitCode()
	default:
		if ctx.Err() != nil {
			return res, ctx.Err()
		}
		return res, err
	}
	if ctx.Err() != nil {
		return res, ctx.Err()
	}
	return res, nil
}

func (e *Environment) ReadFile(_ context.Context, path string) ([]byte, error) {
	full, err := e.resolve(path)
	if err != nil {
		return nil, err
	}
	return os.ReadFile(full)
}

func (e *Environment) WriteFile(_ context.Context, path string, data []byte) error {
	full, err := e.resolve(path)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
		return err
	}
	return os.WriteFile(full, data, 0o644) //nolint:gosec // G306: workspace files are the model's working files
}

func (e *Environment) ReadDir(_ context.Context, path string) ([]environment.DirEntry, error) {
	full, err := e.resolve(path)
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(full)
	if err != nil {
		return nil, err
	}
	out := make([]environment.DirEntry, 0, len(entries))
	for _, d := range entries {
		de := environment.DirEntry{Name: d.Name(), Dir: d.IsDir()}
		if !d.IsDir() {
			if info, err := d.Info(); err == nil {
				de.Size = info.Size()
			}
		}
		out = append(out, de)
	}
	return out, nil
}
