package workspacetest

import (
	"context"
	"errors"
	"testing"

	"github.com/felinics/twilight/agent/workspace"
)

// Factory builds a fresh, empty Store for one subtest.
type Factory func(t *testing.T) workspace.Store

// Run executes the suite.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	t.Run("records", func(t *testing.T) { testRecords(t, factory(t)) })
	t.Run("runtime", func(t *testing.T) { testRuntime(t, factory(t)) })
	t.Run("fork", func(t *testing.T) { testFork(t, factory(t)) })
}

const renamedProject = "repo-renamed"

func testRecords(t *testing.T, s workspace.Store) {
	ctx := context.Background()
	if _, err := s.Get(ctx, "ws-1"); !errors.Is(err, workspace.ErrNotFound) {
		t.Fatalf("get of an unknown workspace = %v, want not found", err)
	}
	w := workspace.Workspace{ID: "ws-1", Project: "repo", Base: "rev-1"}
	if err := s.Create(ctx, w); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, w); !errors.Is(err, workspace.ErrExists) {
		t.Fatalf("create twice = %v, want exists", err)
	}
	got, err := s.Get(ctx, "ws-1")
	if err != nil || got.ID != "ws-1" || got.Project != "repo" || got.Base != "rev-1" || got.Runtime != nil || got.Snapshot != nil {
		t.Fatalf("get = %+v %v", got, err)
	}
	w.Project = renamedProject
	if err := s.Put(ctx, w); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, "ws-1"); err != nil || got.Project != renamedProject {
		t.Fatalf("get after put = %+v %v", got, err)
	}
	if err := s.Put(ctx, workspace.Workspace{ID: "ws-absent"}); !errors.Is(err, workspace.ErrNotFound) {
		t.Fatalf("put of an unknown workspace = %v, want not found", err)
	}
	if err := s.PutSnapshot(ctx, workspace.Snapshot{Ref: "snap-x", Workspace: "ws-absent", Backend: "b", StateRef: "st"}); !errors.Is(err, workspace.ErrNotFound) {
		t.Fatalf("snapshot of an unknown workspace = %v, want not found", err)
	}
	snap := workspace.Snapshot{Ref: "snap-1", Workspace: "ws-1", Backend: "local", StateRef: "state-1"}
	if err := s.PutSnapshot(ctx, snap); err != nil {
		t.Fatal(err)
	}
	if got, err := s.GetSnapshot(ctx, "snap-1"); err != nil || got != snap {
		t.Fatalf("get snapshot = %+v %v", got, err)
	}
	if _, err := s.GetSnapshot(ctx, "snap-absent"); !errors.Is(err, workspace.ErrNotFound) {
		t.Fatalf("get of an unknown snapshot = %v, want not found", err)
	}
	// UpdateSnapshot is a partial write: it moves the latest snapshot and
	// leaves a runtime written in between where it is.
	if err := s.UpdateRuntime(ctx, "ws-1", 0, workspace.RuntimeBinding{Backend: "local", EnvironmentRef: "env-1", Generation: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateSnapshot(ctx, "ws-1", "snap-1"); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, "ws-1"); err != nil || got.Snapshot == nil || *got.Snapshot != "snap-1" || got.Runtime == nil || got.Runtime.Generation != 1 || got.Project != renamedProject {
		t.Fatalf("after update snapshot = %+v %v, want snapshot set and runtime kept", got, err)
	}
	if err := s.UpdateSnapshot(ctx, "ws-1", "snap-absent"); !errors.Is(err, workspace.ErrNotFound) {
		t.Fatalf("update to an unknown snapshot = %v, want not found", err)
	}
	if err := s.UpdateSnapshot(ctx, "ws-absent", "snap-1"); !errors.Is(err, workspace.ErrNotFound) {
		t.Fatalf("update of an unknown workspace = %v, want not found", err)
	}
	if err := s.PutSnapshot(ctx, workspace.Snapshot{Ref: "snap-other", Workspace: "ws-1", Backend: "local", StateRef: "st"}); err != nil {
		t.Fatal(err)
	}
	if err := s.Create(ctx, workspace.Workspace{ID: "ws-2"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateSnapshot(ctx, "ws-2", "snap-other"); !errors.Is(err, workspace.ErrNotFound) {
		t.Fatalf("update to another workspace's snapshot = %v, want not found", err)
	}
}

// UpdateRuntime is a conditional write on the binding generation: the first
// materialization expects 0, a replacement expects the current generation,
// and a stale expectation writes nothing.
func testRuntime(t *testing.T, s workspace.Store) {
	ctx := context.Background()
	if err := s.Create(ctx, workspace.Workspace{ID: "ws-1"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateRuntime(ctx, "ws-absent", 0, workspace.RuntimeBinding{Backend: "local", EnvironmentRef: "e", Generation: 1}); !errors.Is(err, workspace.ErrNotFound) {
		t.Fatalf("runtime of an unknown workspace = %v, want not found", err)
	}
	first := workspace.RuntimeBinding{Backend: "local", EnvironmentRef: "env-1", Generation: 1}
	if err := s.UpdateRuntime(ctx, "ws-1", 0, first); err != nil {
		t.Fatal(err)
	}
	// The loser of a materialization race sees the winner's binding.
	if err := s.UpdateRuntime(ctx, "ws-1", 0, workspace.RuntimeBinding{Backend: "local", EnvironmentRef: "env-other", Generation: 1}); !errors.Is(err, workspace.ErrGenerationConflict) {
		t.Fatalf("second materialization from 0 = %v, want generation conflict", err)
	}
	got, err := s.Get(ctx, "ws-1")
	if err != nil || got.Runtime == nil || *got.Runtime != first {
		t.Fatalf("runtime after the race = %+v %v, want the first binding", got.Runtime, err)
	}
	second := workspace.RuntimeBinding{Backend: "local", EnvironmentRef: "env-2", Generation: 2}
	if err := s.UpdateRuntime(ctx, "ws-1", 1, second); err != nil {
		t.Fatal(err)
	}
	if got, err := s.Get(ctx, "ws-1"); err != nil || got.Runtime == nil || *got.Runtime != second {
		t.Fatalf("runtime after replacement = %+v %v", got.Runtime, err)
	}
}

// Fork creates the destination from a snapshot of the source with no runtime
// binding of its own.
func testFork(t *testing.T, s workspace.Store) {
	ctx := context.Background()
	if err := s.Create(ctx, workspace.Workspace{ID: "src", Project: "repo", Base: "rev-1", Runtime: &workspace.RuntimeBinding{Backend: "local", EnvironmentRef: "env-src", Generation: 3}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Fork(ctx, workspace.Fork{Source: "src", Destination: "dst", Snapshot: "snap-absent"}); !errors.Is(err, workspace.ErrNotFound) {
		t.Fatalf("fork from an unknown snapshot = %v, want not found", err)
	}
	if err := s.PutSnapshot(ctx, workspace.Snapshot{Ref: "snap-1", Workspace: "src", Backend: "local", StateRef: "state-1"}); err != nil {
		t.Fatal(err)
	}
	dst, err := s.Fork(ctx, workspace.Fork{Source: "src", Destination: "dst", Snapshot: "snap-1"})
	if err != nil || dst.ID != "dst" || dst.Project != "repo" || dst.Base != "rev-1" || dst.Snapshot == nil || *dst.Snapshot != "snap-1" || dst.Runtime != nil {
		t.Fatalf("fork = %+v %v", dst, err)
	}
	if got, err := s.Get(ctx, "dst"); err != nil || got.Runtime != nil || got.Snapshot == nil {
		t.Fatalf("forked workspace = %+v %v", got, err)
	}
	if _, err := s.Fork(ctx, workspace.Fork{Source: "src", Destination: "dst", Snapshot: "snap-1"}); !errors.Is(err, workspace.ErrExists) {
		t.Fatalf("fork onto an existing destination = %v, want exists", err)
	}
	if _, err := s.Fork(ctx, workspace.Fork{Source: "absent", Destination: "dst2", Snapshot: "snap-1"}); !errors.Is(err, workspace.ErrNotFound) {
		t.Fatalf("fork from an unknown source = %v, want not found", err)
	}
	if forked, err := s.Fork(ctx, workspace.Fork{Source: "src", Destination: "dst3", Snapshot: "snap-1", Base: "rev-9"}); err != nil || forked.Base != "rev-9" {
		t.Fatalf("fork with its own base = %+v %v", forked, err)
	}
}
