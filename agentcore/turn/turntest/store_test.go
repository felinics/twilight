package turntest

import (
	"testing"

	"github.com/felinics/twilight/agentcore/session/filestore/filestoretest"
)

// The kernel's own Session store runs the turn suite; adapters elsewhere run
// it through Run with their Factory.
func TestFileStoreConformance(t *testing.T) {
	Run(t, func(t testing.TB) Fixture { return Fixture{Store: filestoretest.Store(t)} })
}
