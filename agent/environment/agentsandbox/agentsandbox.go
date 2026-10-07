// Package agentsandbox adapts Kubernetes SIGs Agent Sandbox to Twilight's
// provider-neutral environment contract. It is a Kubernetes development
// provider: the controller manages one sandbox Pod per workspace and the
// sandboxd runtime serves process and filesystem operations.
//
// The default k3d runtime is runc and is not a hostile-code security boundary.
// Use a stronger RuntimeClass or a provider such as E2B for production.
package agentsandbox

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/felinics/twilight/agent/environment"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	sandboxclient "sigs.k8s.io/agent-sandbox/clients/go/sandbox"
)

// Backend is the provider identity stored in Workspace RuntimeBindings.
const Backend environment.Backend = "agent-sandbox"

// Config configures the Agent Sandbox client.
type Config struct {
	Namespace string
	WarmPool  string
}

type sandboxAPI interface {
	CreateSandbox(context.Context, string, string) (*sandboxclient.Sandbox, error)
	GetSandbox(context.Context, string, string) (*sandboxclient.Sandbox, error)
}

// Provider creates and attaches Agent Sandbox claims.
type Provider struct {
	client    sandboxAPI
	namespace string
	warmPool  string
}

var _ environment.Provider = (*Provider)(nil)

// New creates an in-cluster Agent Sandbox provider. The Agent Sandbox
// controller, SandboxTemplate and SandboxWarmPool must already exist.
func New(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.Namespace == "" {
		return nil, errors.New("agentsandbox: namespace is required")
	}
	if cfg.WarmPool == "" {
		return nil, errors.New("agentsandbox: warm pool is required")
	}
	client, err := sandboxclient.NewClient(ctx, sandboxclient.Options{
		Namespace:    cfg.Namespace,
		Runtime:      sandboxclient.RuntimeSandboxd,
		Connectivity: sandboxclient.ConnectivityPortForward,
	})
	if err != nil {
		return nil, fmt.Errorf("agentsandbox: client: %w", err)
	}
	return &Provider{client: client, namespace: cfg.Namespace, warmPool: cfg.WarmPool}, nil
}

func (p *Provider) Create(ctx context.Context, spec environment.Spec) (environment.Environment, error) {
	if spec.Base != "" {
		return nil, fmt.Errorf("%w: agent sandbox does not materialize base %q", environment.ErrUnsupported, spec.Base)
	}
	sb, err := p.client.CreateSandbox(ctx, p.warmPool, p.namespace)
	if err != nil {
		return nil, fmt.Errorf("agentsandbox: create %s: %w", spec.Subject, err)
	}
	return &Environment{sandbox: sb, ref: ref(p.namespace, sb.ClaimName())}, nil
}

func (p *Provider) Restore(context.Context, environment.RestoreSpec) (environment.Environment, error) {
	return nil, fmt.Errorf("%w: agent sandbox snapshots are not configured", environment.ErrUnsupported)
}

func (p *Provider) Attach(ctx context.Context, raw environment.EnvironmentRef) (environment.Environment, error) {
	namespace, claim, err := parseRef(raw)
	if err != nil {
		return nil, err
	}
	sb, err := p.client.GetSandbox(ctx, claim, namespace)
	if err != nil {
		if k8serrors.IsNotFound(err) || errors.Is(err, sandboxclient.ErrSandboxDeleted) {
			return nil, fmt.Errorf("agentsandbox: attach %s: %w: %w", raw, environment.ErrNotFound, err)
		}
		return nil, fmt.Errorf("agentsandbox: attach %s: %w", raw, err)
	}
	return &Environment{sandbox: sb, ref: raw}, nil
}

func ref(namespace, claim string) environment.EnvironmentRef {
	return environment.EnvironmentRef("agentsandbox/" + namespace + "/" + claim)
}

func parseRef(raw environment.EnvironmentRef) (string, string, error) {
	parts := strings.Split(string(raw), "/")
	if len(parts) != 3 || parts[0] != "agentsandbox" || parts[1] == "" || parts[2] == "" {
		return "", "", fmt.Errorf("%w: malformed Agent Sandbox ref %q", environment.ErrNotFound, raw)
	}
	return parts[1], parts[2], nil
}

// Environment is an attached sandboxd runtime.
type Environment struct {
	sandbox *sandboxclient.Sandbox
	ref     environment.EnvironmentRef
}

var (
	_ environment.Environment = (*Environment)(nil)
	_ environment.Executor    = (*Environment)(nil)
	_ environment.FS          = (*Environment)(nil)
)

func (e *Environment) Ref() environment.EnvironmentRef { return e.ref }

// Close disconnects without deleting the SandboxClaim. The durable workspace
// binding can therefore be re-attached after tool-backend restarts.
func (e *Environment) Close(ctx context.Context) error { return e.sandbox.Disconnect(ctx) }

func (e *Environment) Exec(ctx context.Context, spec environment.ExecSpec) (environment.ExecResult, error) {
	if len(spec.Argv) == 0 {
		return environment.ExecResult{}, errors.New("agentsandbox: exec requires argv")
	}
	if len(spec.Stdin) != 0 {
		return environment.ExecResult{}, fmt.Errorf("%w: Agent Sandbox Exec does not accept stdin yet", environment.ErrUnsupported)
	}
	cwd, err := relativePath(spec.Cwd)
	if err != nil {
		return environment.ExecResult{}, err
	}
	command := commandString(spec.Argv)
	if cwd != "." {
		command = "cd -- " + shellQuote(cwd) + " && " + command
	}
	if len(spec.Env) > 0 {
		var prefix []string
		for key, value := range spec.Env {
			if key == "" || strings.ContainsAny(key, "=\x00") {
				return environment.ExecResult{}, fmt.Errorf("agentsandbox: invalid environment key %q", key)
			}
			prefix = append(prefix, key+"="+shellQuote(value))
		}
		command = "env " + strings.Join(prefix, " ") + " " + command
	}
	result, err := e.sandbox.Run(ctx, command)
	if err != nil {
		return environment.ExecResult{}, err
	}
	out := environment.ExecResult{ExitCode: result.ExitCode, Stdout: result.Stdout, Stderr: result.Stderr}
	limit := spec.MaxOutputBytes
	if limit <= 0 {
		limit = environment.DefaultMaxOutputBytes
	}
	out.Stdout, out.Truncated = truncate(out.Stdout, limit)
	var truncated bool
	out.Stderr, truncated = truncate(out.Stderr, limit)
	out.Truncated = out.Truncated || truncated
	return out, nil
}

func (e *Environment) ReadFile(ctx context.Context, path string) ([]byte, error) {
	path, err := relativePath(path)
	if err != nil {
		return nil, err
	}
	return e.sandbox.Read(ctx, path)
}

func (e *Environment) WriteFile(ctx context.Context, path string, data []byte) error {
	path, err := relativePath(path)
	if err != nil {
		return err
	}
	return e.sandbox.Write(ctx, path, data)
}

func (e *Environment) ReadDir(ctx context.Context, path string) ([]environment.DirEntry, error) {
	path, err := relativePath(path)
	if err != nil {
		return nil, err
	}
	entries, err := e.sandbox.List(ctx, path)
	if err != nil {
		return nil, err
	}
	out := make([]environment.DirEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, environment.DirEntry{
			Name: entry.Name,
			Dir:  entry.Type == sandboxclient.FileTypeDirectory,
			Size: entry.Size,
		})
	}
	return out, nil
}

func relativePath(path string) (string, error) {
	if path == "" {
		return ".", nil
	}
	if filepath.IsAbs(path) {
		return "", fmt.Errorf("%w: %q is absolute", environment.ErrOutsideRoot, path)
	}
	clean := filepath.ToSlash(filepath.Clean(path))
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("%w: %q", environment.ErrOutsideRoot, path)
	}
	return clean, nil
}

func commandString(argv []string) string {
	if len(argv) == 3 && argv[0] == "sh" && argv[1] == "-c" {
		return argv[2]
	}
	parts := make([]string, len(argv))
	for i, arg := range argv {
		parts[i] = shellQuote(arg)
	}
	return strings.Join(parts, " ")
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func truncate(value string, limit int64) (string, bool) {
	if int64(len(value)) <= limit {
		return value, false
	}
	return value[:limit], true
}
