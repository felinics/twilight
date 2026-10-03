// Package sessionstoretest builds the Run session store's ports over throwaway durable
// stores for tests.
package sessionstoretest

import (
	"testing"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/run/sessionstore"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
)

// Frozen is the run layer's frozen.Store over a fresh file-backed content
// store for sessionstore.FrozenAuthority and the given binding store.
func Frozen(t testing.TB, bindings artifact.BindingStore) frozen.Store {
	t.Helper()
	fz, err := sessionstore.FrozenValues(filestoretest.Content(t, sessionstore.FrozenAuthority), bindings)
	if err != nil {
		t.Fatal(err)
	}
	return fz
}
