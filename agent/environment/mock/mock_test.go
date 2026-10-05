package mock_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/felinics/twilight/agent/environment"
	"github.com/felinics/twilight/agent/environment/mock"
)

func TestProviderExercisesWorkspaceCapabilities(t *testing.T) {
	root := t.TempDir()
	provider, err := mock.New(filepath.Join(root, "envs"))
	if err != nil {
		t.Fatal(err)
	}
	env, err := provider.Create(context.Background(), environment.Spec{Subject: "test"})
	if err != nil {
		t.Fatal(err)
	}
	fs := env.(environment.FS)
	if err := fs.WriteFile(context.Background(), "hello.txt", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	got, err := fs.ReadFile(context.Background(), "hello.txt")
	if err != nil || string(got) != "hello" {
		t.Fatalf("read = %q, %v", got, err)
	}
	exec := env.(environment.Executor)
	result, err := exec.Exec(context.Background(), environment.ExecSpec{Argv: []string{"sh", "-c", "cat hello.txt"}})
	if err != nil || result.ExitCode != 0 || result.Stdout != "hello" {
		t.Fatalf("exec = %#v, %v", result, err)
	}
	if _, err := os.Stat(filepath.Join(root, "envs", string(env.Ref()), "hello.txt")); err != nil {
		t.Fatal(err)
	}
}
