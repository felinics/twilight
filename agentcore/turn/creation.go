package turn

import (
	"errors"
	"github.com/felinics/twilight/agentcore/preset/ref"
	"unicode/utf8"

	"github.com/felinics/twilight/agentcore/ledger"
)

// NewTurn is immutable Turn creation data. The caller supplies TurnID so
// retries retain a stable identity; Preset fixes the configuration throughout
// the Turn, including recovery and ownership changes.
type NewTurn struct {
	TurnID      TurnID             `json:"turnId"`
	Preset      ref.Ref            `json:"preset"`
	CausationID ledger.CausationID `json:"causationId,omitempty"`
}

// BuildNewTurn constructs a Turn creation value.
func BuildNewTurn(turnID TurnID, causationID ledger.CausationID, preset ref.Ref) (NewTurn, error) {
	value := NewTurn{TurnID: turnID, CausationID: causationID, Preset: preset}
	if err := ValidateNewTurn(value); err != nil {
		return NewTurn{}, err
	}
	return value, nil
}

// ValidateNewTurn verifies the preset reference and textual identity encoding.
func ValidateNewTurn(value NewTurn) error {
	if value.Preset.ID == "" || value.Preset.Digest == "" {
		return errors.New("turn: preset identity and digest are required")
	}
	if value.TurnID == "" {
		return errors.New("agent: new turn: empty TurnID")
	}
	if !utf8.ValidString(string(value.TurnID)) {
		return errors.New("agent: new turn: TurnID is not valid UTF-8")
	}
	if !utf8.ValidString(string(value.CausationID)) {
		return errors.New("agent: new turn: CausationID is not valid UTF-8")
	}
	return nil
}
