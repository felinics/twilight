// Package jsonstable provides immutable RFC 8785 (JCS) JSON values for agent
// wire protocols. External bytes are parsed and canonicalized once at the
// boundary, under the SDK's rule (sdk.CanonicalJSON); after that Value is
// safe to store in commands, facts, and MachineState, and its bytes are the
// digest preimage.
package jsonstable

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/felinics/twilight/sdk"
)

// Value is an immutable canonical JSON value. The zero value represents an
// absent value for omitzero fields and marshals as JSON null when required.
type Value struct {
	raw []byte
}

// Parse validates raw JSON and stores its RFC 8785 canonical representation.
// A nil slice returns the zero Value; an empty but non-nil slice is invalid
// JSON.
//
// JCS gives every I-JSON value one cross-language representation. JSON
// numbers therefore have IEEE-754 binary64 semantics. Exact identifiers or
// arbitrary-precision quantities must be represented as JSON strings, not
// JSON numbers.
func Parse(raw []byte) (Value, error) {
	if raw == nil {
		return Value{}, nil
	}
	canonical, err := Canonicalize(raw)
	if err != nil {
		return Value{}, err
	}
	return Value{raw: append([]byte(nil), canonical...)}, nil
}

// MustParse is a convenience for tests and package-level constants.
func MustParse(raw string) Value {
	v, err := Parse([]byte(raw))
	if err != nil {
		panic(err)
	}
	return v
}

// FromValue marshals a Go JSON-shaped value and stores its canonical form.
func FromValue(v any) (Value, error) {
	if existing, ok := v.(Value); ok {
		return existing, nil
	}
	if raw, ok := v.(json.RawMessage); ok {
		return Parse(raw)
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return Value{}, err
	}
	return Parse(raw)
}

// Bytes returns a detached canonical byte slice. The zero Value returns null.
func (v Value) Bytes() []byte {
	if len(v.raw) == 0 {
		return []byte("null")
	}
	return append([]byte(nil), v.raw...)
}

// RawMessage returns a detached json.RawMessage view of the canonical bytes.
func (v Value) RawMessage() json.RawMessage {
	return json.RawMessage(v.Bytes())
}

func (v Value) String() string { return string(v.Bytes()) }

// IsZero reports whether v is absent. It is used by encoding/json's omitzero
// tag; a present JSON null is not zero because its raw bytes are "null".
func (v Value) IsZero() bool { return len(v.raw) == 0 }

func (v Value) Equal(other Value) bool { return bytes.Equal(v.Bytes(), other.Bytes()) }

func (v Value) Decode(dst any) error {
	dec := json.NewDecoder(bytes.NewReader(v.Bytes()))
	dec.UseNumber()
	return dec.Decode(dst)
}

func (v Value) Any() (any, error) {
	dec := json.NewDecoder(bytes.NewReader(v.Bytes()))
	dec.UseNumber()
	var out any
	if err := dec.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func (v Value) MarshalJSON() ([]byte, error) { return v.Bytes(), nil }

func (v *Value) UnmarshalJSON(raw []byte) error {
	parsed, err := Parse(raw)
	if err != nil {
		return err
	}
	*v = parsed
	return nil
}

// Canonicalize transforms JSON into RFC 8785 (JCS) bytes. The rule is the
// SDK's (sdk.CanonicalJSON): the SDK is the layer that first carries raw
// JSON, and there is one canonical form in the module, so bytes the SDK
// produced re-canonicalize to themselves here and a PostgreSQL JSONB round
// trip remains digest-stable after canonicalization.
func Canonicalize(raw []byte) ([]byte, error) {
	canonical, err := sdk.CanonicalJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("agent: canonical: %w", err)
	}
	return []byte(canonical), nil
}

// MarshalCanonical marshals a Go value and canonicalizes its JSON wire form.
// This is the single path from protocol values to digest input.
func MarshalCanonical(v any) ([]byte, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("agent: canonical: %w", err)
	}
	return Canonicalize(raw)
}
