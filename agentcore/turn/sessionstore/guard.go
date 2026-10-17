package sessionstore

import (
	"fmt"

	"github.com/felinics/twilight/agentcore/session/writer"
)

// RequireNoActiveTurn is the Turn module's quiescence precondition for a
// Session-level operation evaluated inside the Writer's critical section:
// an error while any Turn of the Session is active. Terminal Turns leave
// the machine projection, so an
// empty Active set is the whole answer: no Turn is executing, awaiting an
// effect or holding an unresolved outcome.
func RequireNoActiveTurn(view writer.View) error {
	m, err := loadMachine(view)
	if err != nil {
		return err
	}
	for id := range m.Active {
		return fmt.Errorf("sessionstore: turn %s is active", id)
	}
	return nil
}
