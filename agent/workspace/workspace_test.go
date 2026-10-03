package workspace_test

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agent/workspace"
)

func TestWorkspaceTargetIsLogical(t *testing.T) {
	w := workspace.Workspace{ID: "ws-123", Project: "repo/foo", Base: "abc123"}
	target := w.Target()
	if target.Kind != "workspace" || target.ID != "ws-123" {
		t.Fatalf("target = %+v", target)
	}
}

type snapshotProvider struct {
	state    environment.StateRef
	restored environment.RestoreSpec
}

var _ environment.Provider = (*snapshotProvider)(nil)

type restoredEnvironment struct{ ref environment.EnvironmentRef }

func (e restoredEnvironment) Ref() environment.EnvironmentRef { return e.ref }
func (restoredEnvironment) Close(context.Context) error       { return nil }

func (*snapshotProvider) Create(context.Context, environment.Spec) (environment.Environment, error) {
	return nil, errors.New("test provider: create unavailable")
}

func (*snapshotProvider) Attach(context.Context, environment.EnvironmentRef) (environment.Environment, error) {
	return nil, errors.New("test provider: source environment removed")
}

func (p *snapshotProvider) Restore(_ context.Context, spec environment.RestoreSpec) (environment.Environment, error) {
	if spec.State != p.state {
		return nil, errors.New("test provider: snapshot unavailable")
	}
	p.restored = spec
	return restoredEnvironment{ref: "restored-environment"}, nil
}

// The contract carries durable state and destination independently of an old environment.
func TestSnapshotRestoresIntoAnotherWorkspace(t *testing.T) {
	snapshot := workspace.Snapshot{Ref: "snapshot", Workspace: "source", Backend: "test", StateRef: "durable-state"}
	provider := &snapshotProvider{state: snapshot.StateRef}
	const old environment.EnvironmentRef = "deleted-environment"
	if _, err := provider.Attach(context.Background(), old); err == nil {
		t.Fatal("source environment still exists")
	}
	spec := environment.RestoreSpec{State: snapshot.StateRef, Destination: environment.Spec{Subject: "fork", Base: "base-revision"}}
	env, err := provider.Restore(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if env.Ref() == old || provider.restored != spec {
		t.Fatalf("restore = %q %+v", env.Ref(), provider.restored)
	}
}
