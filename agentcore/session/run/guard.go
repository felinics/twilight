package runmod

import (
	"fmt"

	"github.com/felinics/twilight/agentcore/session/writer"
)

// RequireNoActiveRun is the Run module's quiescence precondition for a
// Session-level operation evaluated inside the Writer's critical section:
// an error while any Run of the Session is active. Terminal Runs leave the machine projection (RUN-CMT-2), so an
// empty Active set is the whole answer: no Run is executing, awaiting an
// effect or holding an unresolved outcome.
func RequireNoActiveRun(view writer.View) error {
	m, err := loadMachine(view)
	if err != nil {
		return err
	}
	for id := range m.Active {
		return fmt.Errorf("runmod: run %s is active", id)
	}
	return nil
}
