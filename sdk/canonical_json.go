package sdk

import (
	"encoding/json"
	"fmt"

	"github.com/felinics/twilight/internal/canonicaljson"
)

// ErrInvalidJSON reports bytes that are not one JSON document, or a document
// with a repeated object member or an escaped lone surrogate.
var ErrInvalidJSON = canonicaljson.ErrInvalid

// CanonicalJSON re-encodes one JSON document in its RFC 8785 (JCS) form, so
// that equal documents are equal bytes in every language that implements the
// RFC: object members sorted by UTF-16 code units, no insignificant
// whitespace, minimal string escaping, and numbers in their IEEE-754
// binary64 form (2.0 becomes 2, 1e2 becomes 100). Numbers therefore have
// binary64 semantics: an identifier or quantity that must stay exact belongs
// in a JSON string, not a number.
//
// Input that is not valid UTF-8, not exactly one JSON value, an object with
// a repeated member, or a string with an escaped lone surrogate is
// ErrInvalidJSON.
func CanonicalJSON(raw []byte) (json.RawMessage, error) {
	canonical, err := canonicaljson.Transform(raw)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(canonical), nil
}

// canonicalMarshal encodes v with encoding/json and canonicalizes the result,
// so a value that embeds raw JSON (a json.RawMessage field, a map of them)
// encodes canonically too.
func canonicalMarshal(v any) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return CanonicalJSON(raw)
}

// CanonicalProviderOptions returns options with every value re-encoded by
// CanonicalJSON, so a Request that carries them marshals to the same bytes
// for the same options however they were written. Neither the map nor its
// values are modified; a nil or empty map is returned as is.
func CanonicalProviderOptions(options map[string]json.RawMessage) (map[string]json.RawMessage, error) {
	if len(options) == 0 {
		return options, nil
	}
	out := make(map[string]json.RawMessage, len(options))
	for namespace, raw := range options {
		canonical, err := CanonicalJSON(raw)
		if err != nil {
			return nil, fmt.Errorf("provider options for %q: %w", namespace, err)
		}
		out[namespace] = canonical
	}
	return out, nil
}
