package run

import (
	"encoding/json"
	"testing"
)

// RunEnded validates its tagged terminal value on the way to the wire: no
// end, an incomplete end or an end variant the schema does not know is
// refused by MarshalJSON, so no fact encoder can persist it.
func TestRunEndedTaggedUnionRejectsInvalidValues(t *testing.T) {
	for name, fact := range map[string]RunEnded{
		"nil end":                {},
		"stopped without reason": {End: RunStoppedEnd{}},
		"failed without class":   {End: RunFailedEnd{Reason: ReasonProviderFailure}},
		"unknown end variant":    {End: fakeRunEnd{}},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := json.Marshal(fact); err == nil {
				t.Fatal("invalid tagged terminal value was accepted")
			}
		})
	}
}

type fakeRunEnd struct{}

func (fakeRunEnd) runEnd() {}
