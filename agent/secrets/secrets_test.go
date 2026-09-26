package secrets_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/felinics/twilight/agent/secrets"
)

// Dir reads a mounted Kubernetes Secret: one file per name, a Kubernetes
// key (dots and leading dots included) is a valid name, anything that is
// not one path element is not, and only one trailing newline leaves the
// value. Static answers from memory. Both report ErrNotFound the same way.
func TestResolvers(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	for name, content := range map[string]string{
		"openai":      "k-from-volume\n",
		".dockercfg":  "dotted\r\n",
		"raw":         "  padded  ",
		"two-lines":   "a\n\n",
		"config.json": "{}",
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var resolver secrets.Resolver = secrets.Dir(dir)
	for name, want := range map[string]string{
		"openai":      "k-from-volume",
		".dockercfg":  "dotted",
		"raw":         "  padded  ",
		"two-lines":   "a\n",
		"config.json": "{}",
	} {
		if v, err := resolver.Lookup(ctx, name); err != nil || v != want {
			t.Fatalf("Dir.Lookup(%q) = %q, %v; want %q", name, v, err, want)
		}
	}
	for _, name := range []string{"missing", "../openai", "sub/openai", ".", "..", ""} {
		if _, err := resolver.Lookup(ctx, name); !errors.Is(err, secrets.ErrNotFound) {
			t.Fatalf("Dir.Lookup(%q) = %v, want not found", name, err)
		}
	}
	resolver = secrets.Static{"openai": "k"}
	if v, err := resolver.Lookup(ctx, "openai"); err != nil || v != "k" {
		t.Fatalf("Static.Lookup = %q, %v", v, err)
	}
	if _, err := resolver.Lookup(ctx, "missing"); !errors.Is(err, secrets.ErrNotFound) {
		t.Fatalf("Static.Lookup(missing) = %v, want not found", err)
	}
}
