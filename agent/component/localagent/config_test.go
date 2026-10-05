package localagent_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/felinics/twilight/agent/component/localagent"
)

func TestComposeFileBuildsOneProcessAgent(t *testing.T) {
	root := t.TempDir()
	secrets := filepath.Join(root, "secrets")
	if err := os.MkdirAll(secrets, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(secrets, "key"), []byte("test-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	catalog := filepath.Join(root, "models.json")
	if err := os.WriteFile(catalog, []byte(`{"models":[{"ref":"chat","kind":"openai","model":"test","baseURL":"http://127.0.0.1:1","apiKeySecret":"key"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	ag, err := localagent.ComposeFile(context.Background(), localagent.FileConfig{
		Root:    filepath.Join(root, "data"),
		Catalog: catalog,
		Secrets: secrets,
		Model:   "chat",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ag.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
