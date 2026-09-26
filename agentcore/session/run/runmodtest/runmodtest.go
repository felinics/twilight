// Package runmodtest builds the run module's ports over throwaway durable
// stores for tests.
package runmodtest

import (
	"testing"

	"github.com/felinics/twilight/agentcore/artifact"
	"github.com/felinics/twilight/agentcore/run/frozen"
	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
	runmod "github.com/felinics/twilight/agentcore/session/run"
)

// Frozen is the run layer's frozen.Store over a fresh file-backed content
// store for runmod.FrozenAuthority and the given binding store.
func Frozen(t testing.TB, bindings artifact.BindingStore) frozen.Store {
	t.Helper()
	fz, err := runmod.FrozenValues(filestoretest.Content(t, runmod.FrozenAuthority), bindings)
	if err != nil {
		t.Fatal(err)
	}
	return fz
}
