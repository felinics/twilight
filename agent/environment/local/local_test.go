package local_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agent/environment/local"
)

func TestLocalProvider(t *testing.T) {
	ctx := context.Background()
	p, err := local.New(filepath.Join(t.TempDir(), "envs"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Create(ctx, environment.Spec{Subject: "ws-1", Base: "rev-1"}); !errors.Is(err, environment.ErrUnsupported) {
		t.Fatalf("create with a base = %v, want unsupported", err)
	}
	if _, err := p.Restore(ctx, environment.RestoreSpec{State: "snap-missing"}); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("restore of an unknown snapshot = %v, want not found", err)
	}
	env, err := p.Create(ctx, environment.Spec{Subject: "ws-1"})
	if err != nil {
		t.Fatal(err)
	}
	// The directory survives Close and is adopted again by its ref; a ref
	// the provider never issued, or one that escapes the root, is not found.
	if err := env.Close(ctx); err != nil {
		t.Fatal(err)
	}
	again, err := p.Attach(ctx, env.Ref())
	if err != nil || again.Ref() != env.Ref() {
		t.Fatalf("attach = %v %v", again, err)
	}
	for _, ref := range []environment.EnvironmentRef{"env-missing", "../etc", "", ".hidden"} {
		if _, err := p.Attach(ctx, ref); !errors.Is(err, environment.ErrNotFound) {
			t.Fatalf("attach %q = %v, want not found", ref, err)
		}
	}
	fsys := again.(environment.FS)
	exe := again.(environment.Executor)
	if err := fsys.WriteFile(ctx, "src/hello.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	data, err := fsys.ReadFile(ctx, "src/hello.txt")
	if err != nil || string(data) != "hello" {
		t.Fatalf("read = %q %v", data, err)
	}
	entries, err := fsys.ReadDir(ctx, "src")
	if err != nil || len(entries) != 1 || entries[0].Name != "hello.txt" || entries[0].Dir || entries[0].Size != 5 {
		t.Fatalf("readdir = %+v %v", entries, err)
	}
	for _, bad := range []string{"../outside", "/etc/passwd", "src/../../x"} {
		if _, err := fsys.ReadFile(ctx, bad); !errors.Is(err, environment.ErrOutsideRoot) {
			t.Fatalf("read %q = %v, want outside root", bad, err)
		}
	}
	res, err := exe.Exec(ctx, environment.ExecSpec{Argv: []string{"sh", "-c", "cat hello.txt; echo err >&2; exit 3"}, Cwd: "src"})
	if err != nil || res.ExitCode != 3 || res.Stdout != "hello" || res.Stderr != "err\n" || res.Truncated {
		t.Fatalf("exec = %+v %v", res, err)
	}
	res, err = exe.Exec(ctx, environment.ExecSpec{Argv: []string{"sh", "-c", "printf 0123456789"}, MaxOutputBytes: 4})
	if err != nil || res.Stdout != "0123" || !res.Truncated {
		t.Fatalf("truncated exec = %+v %v", res, err)
	}
	if _, err := exe.Exec(ctx, environment.ExecSpec{Argv: []string{"true"}, Cwd: ".."}); !errors.Is(err, environment.ErrOutsideRoot) {
		t.Fatalf("exec outside the root = %v", err)
	}
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := exe.Exec(short, environment.ExecSpec{Argv: []string{"sleep", "5"}}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed out exec = %v, want deadline exceeded", err)
	}
	if _, err := os.Stat(filepath.Join(again.(*local.Environment).Dir(), "src", "hello.txt")); err != nil {
		t.Fatalf("file on the host = %v", err)
	}
	// A snapshot is a copy: later writes do not reach it, and Restore
	// materializes a new environment from it.
	state, err := again.(environment.Snapshotter).Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := fsys.WriteFile(ctx, "src/hello.txt", []byte("changed")); err != nil {
		t.Fatal(err)
	}
	restored, err := p.Restore(ctx, environment.RestoreSpec{State: state, Destination: environment.Spec{Subject: "ws-2"}})
	if err != nil {
		t.Fatal(err)
	}
	if restored.Ref() == again.Ref() {
		t.Fatal("restore reused the source environment")
	}
	if data, err := restored.(environment.FS).ReadFile(ctx, "src/hello.txt"); err != nil || string(data) != "hello" {
		t.Fatalf("restored file = %q %v, want the snapshot's content", data, err)
	}
	if _, err := p.Attach(ctx, environment.EnvironmentRef(".snapshots")); !errors.Is(err, environment.ErrNotFound) {
		t.Fatalf("attach of the snapshot area = %v", err)
	}
}
