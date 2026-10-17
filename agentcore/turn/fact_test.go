package turn

import (
	"encoding/json"
	"testing"
)

// TurnEnded validates its tagged terminal value on the way to the wire: no
// end, an incomplete end or an end variant the schema does not know is
// refused by MarshalJSON, so no fact encoder can persist it.
func TestTurnEndedTaggedUnionRejectsInvalidValues(t *testing.T) {
	for name, fact := range map[string]TurnEnded{
		"nil end":                {},
		"stopped without reason": {End: TurnStoppedEnd{}},
		"failed without class":   {End: TurnFailedEnd{Reason: ReasonProviderFailure}},
		"unknown end variant":    {End: fakeTurnEnd{}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := json.Marshal(fact); err == nil {
				t.Fatal("invalid tagged terminal value was accepted")
			}
		})
	}
}

type fakeTurnEnd struct{}

func (fakeTurnEnd) turnEnd() {}
