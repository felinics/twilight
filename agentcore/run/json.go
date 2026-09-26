package run

import (
	"github.com/felinics/twilight/agentcore/jsonstable"
)

// CanonicalJSON is an immutable, agent-owned canonical JSON value. It can only
// be built by parsing external bytes through ParseCanonicalJSON or by
// marshaling a Go JSON-shaped value through CanonicalJSONFromValue.
type CanonicalJSON = jsonstable.Value

func ParseCanonicalJSON(raw []byte) (CanonicalJSON, error) {
	return jsonstable.Parse(raw)
}

func CanonicalJSONFromValue(v any) (CanonicalJSON, error) {
	return jsonstable.FromValue(v)
}

func MustParseCanonicalJSON(raw string) CanonicalJSON {
	return jsonstable.MustParse(raw)
}
