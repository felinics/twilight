package run

import (
	"errors"
	"unicode/utf8"

	"github.com/felinics/twilight/agentcore/es"
)

// NewRun is the immutable creation data for a Run. RunID is caller-supplied
// so retries retain a stable identity. Which Turn the Run serves, and as
// which attempt, is the attempt module's fact (twilight/attempt/started),
// written in the same commit; the Run itself is one execution.
type NewRun struct {
	RunID       RunID          `json:"runId"`
	CausationID es.CausationID `json:"causationId,omitempty"`
}

// BuildNewRun constructs a Run creation value.
func BuildNewRun(runID RunID, causationID es.CausationID) (NewRun, error) {
	run := NewRun{RunID: runID, CausationID: causationID}
	if err := ValidateNewRun(run); err != nil {
		return NewRun{}, err
	}
	return run, nil
}

// ValidateNewRun verifies textual identity encoding.
func ValidateNewRun(run NewRun) error {
	if run.RunID == "" {
		return errors.New("agent: new run: empty RunID")
	}
	if !utf8.ValidString(string(run.RunID)) {
		return errors.New("agent: new run: RunID is not valid UTF-8")
	}
	if !utf8.ValidString(string(run.CausationID)) {
		return errors.New("agent: new run: CausationID is not valid UTF-8")
	}
	return nil
}
