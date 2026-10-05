package agentsandbox

import (
	"testing"

	"github.com/felinics/twilight/agent/environment"
)

func TestRelativePathRejectsEscape(t *testing.T) {
	for _, path := range []string{"../secret", "a/../../secret", "/workspace/file"} {
		if _, err := relativePath(path); err == nil {
			t.Fatalf("relativePath(%q) accepted an escaping path", path)
		}
	}
}

func TestRelativePathNormalizes(t *testing.T) {
	got, err := relativePath("dir/../file")
	if err != nil {
		t.Fatal(err)
	}
	if got != "file" {
		t.Fatalf("got %q, want file", got)
	}
}

func TestCommandStringPreservesShellScript(t *testing.T) {
	if got := commandString([]string{"sh", "-c", "printf '%s' \"hello world\""}); got != "printf '%s' \"hello world\"" {
		t.Fatalf("got %q", got)
	}
}

func TestParseRef(t *testing.T) {
	namespace, claim, err := parseRef(environment.EnvironmentRef("agentsandbox/twilight/claim-1"))
	if err != nil {
		t.Fatal(err)
	}
	if namespace != "twilight" || claim != "claim-1" {
		t.Fatalf("got %q/%q", namespace, claim)
	}
}
