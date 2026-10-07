package agentsandbox

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/felinics/twilight/agent/environment"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	sandboxclient "sigs.k8s.io/agent-sandbox/clients/go/sandbox"
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

func TestAttachMapsOnlyMissingSandboxesToNotFound(t *testing.T) {
	notFound := k8serrors.NewNotFound(schema.GroupResource{
		Group:    "extensions.agents.x-k8s.io",
		Resource: "sandboxclaims",
	}, "claim-1")
	ordinary := errors.New("connection refused")

	tests := []struct {
		name         string
		err          error
		wantNotFound bool
	}{
		{name: "Kubernetes NotFound", err: fmt.Errorf("get claim: %w", notFound), wantNotFound: true},
		{name: "deleted while attaching", err: fmt.Errorf("wait: %w", sandboxclient.ErrSandboxDeleted), wantNotFound: true},
		{name: "ordinary error", err: ordinary},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			provider := &Provider{client: &attachClient{err: tt.err}}
			_, err := provider.Attach(context.Background(), "agentsandbox/twilight/claim-1")
			if err == nil {
				t.Fatal("Attach returned nil error")
			}
			if got := errors.Is(err, environment.ErrNotFound); got != tt.wantNotFound {
				t.Fatalf("errors.Is(ErrNotFound) = %v, want %v: %v", got, tt.wantNotFound, err)
			}
			if !errors.Is(err, tt.err) {
				t.Fatalf("Attach did not preserve source error: %v", err)
			}
		})
	}
}

type attachClient struct {
	err error
}

func (c *attachClient) CreateSandbox(context.Context, string, string) (*sandboxclient.Sandbox, error) {
	return nil, errors.New("unexpected CreateSandbox call")
}

func (c *attachClient) GetSandbox(context.Context, string, string) (*sandboxclient.Sandbox, error) {
	return nil, c.err
}
