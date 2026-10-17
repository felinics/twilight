// Package sessionstoretest builds the Turn session store's ports over throwaway durable
// stores for tests.
package sessionstoretest

import (
	"testing"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	"github.com/felinics/twilight/agentcore/turn/frozen"
	"github.com/felinics/twilight/agentcore/turn/sessionstore"
)

// Frozen is the Turn layer's frozen.Store over a fresh file-backed content
// store for sessionstore.FrozenAuthority and the given binding store.
func Frozen(t testing.TB, bindings artifact.BindingStore) frozen.Store {
	t.Helper()
	fz, err := sessionstore.FrozenValues(filestoretest.Content(t, sessionstore.FrozenAuthority), bindings)
	if err != nil {
		t.Fatal(err)
	}
	return fz
}
