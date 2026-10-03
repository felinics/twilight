package sdk

import (
	"bytes"
	"encoding/json"
	"fmt"
)

// JSONObject is a JSON object that keeps its members in the order they were
// declared.
//
// Go's encoding/json marshals maps with their keys sorted, so a
// map[string]any silently reorders an object on its way to the wire. That is
// harmless for most APIs, but a System One model reads the object as content:
// the order of Choice options and of the fields of a structured state is part
// of what it is shown. JSONObject keeps the order the caller wrote.
//
// A Go struct already marshals in field-declaration order, so a struct state
// needs no special handling. Reach for JSONObject when the object is built at
// runtime and a map would be the obvious alternative.
//
//	state := sdk.JSONObject{}.
//		Set("ticket", ticket).
//		Set("order", order).
//		Set("refund_policy", policy)
type JSONObject []JSONMember

// JSONMember is one key/value pair of a [JSONObject].
type JSONMember struct {
	Key   string
	Value any
}

// Set appends a member and returns the extended object, so calls can be
// chained. It does not replace an existing member with the same key; marshaling
// an object with duplicate keys is an error.
func (o JSONObject) Set(key string, value any) JSONObject {
	return append(o, JSONMember{Key: key, Value: value})
}

// Get returns the value of the first member with the given key.
func (o JSONObject) Get(key string) (any, bool) {
	for _, m := range o {
		if m.Key == key {
			return m.Value, true
		}
	}
	return nil, false
}

// Keys returns the member keys, in order.
func (o JSONObject) Keys() []string {
	keys := make([]string, len(o))
	for i, m := range o {
		keys[i] = m.Key
	}
	return keys
}

// MarshalJSON implements json.Marshaler. Members are emitted in order.
// Duplicate keys are rejected: the resulting object would be ambiguous, and
// for Choice options it would collapse two options into one.
func (o JSONObject) MarshalJSON() ([]byte, error) {
	if o == nil {
		return []byte("null"), nil
	}

	seen := make(map[string]struct{}, len(o))
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, m := range o {
		if _, dup := seen[m.Key]; dup {
			return nil, fmt.Errorf("twilightai: duplicate JSON key %q", m.Key)
		}
		seen[m.Key] = struct{}{}

		if i > 0 {
			buf.WriteByte(',')
		}
		key, err := json.Marshal(m.Key)
		if err != nil {
			return nil, fmt.Errorf("twilightai: marshal JSON key %q: %w", m.Key, err)
		}
		buf.Write(key)
		buf.WriteByte(':')

		value, err := json.Marshal(m.Value)
		if err != nil {
			return nil, fmt.Errorf("twilightai: marshal JSON value for key %q: %w", m.Key, err)
		}
		buf.Write(value)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

// UnmarshalJSON implements json.Unmarshaler, preserving member order at every
// level: a nested object is decoded into a JSONObject too, and numbers are
// decoded as json.Number so they survive a round trip unchanged.
func (o *JSONObject) UnmarshalJSON(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()

	value, err := decodeOrderedJSON(dec)
	if err != nil {
		return err
	}
	switch v := value.(type) {
	case nil:
		*o = nil
	case JSONObject:
		*o = v
	default:
		return fmt.Errorf("twilightai: cannot unmarshal %s into a JSONObject", data)
	}
	return nil
}

// decodeOrderedJSON reads one JSON value from dec, decoding objects into
// JSONObject so their member order survives.
func decodeOrderedJSON(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}

	delim, ok := tok.(json.Delim)
	if !ok {
		return tok, nil
	}

	switch delim {
	case '{':
		obj := JSONObject{}
		for dec.More() {
			keyTok, err := dec.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyTok.(string)
			if !ok {
				return nil, fmt.Errorf("twilightai: JSON object key is not a string: %v", keyTok)
			}
			value, err := decodeOrderedJSON(dec)
			if err != nil {
				return nil, err
			}
			obj = append(obj, JSONMember{Key: key, Value: value})
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		return obj, nil
	case '[':
		arr := []any{}
		for dec.More() {
			value, err := decodeOrderedJSON(dec)
			if err != nil {
				return nil, err
			}
			arr = append(arr, value)
		}
		if _, err := dec.Token(); err != nil {
			return nil, err
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("twilightai: unexpected JSON delimiter %q", delim)
	}
}

var (
	_ json.Marshaler   = JSONObject(nil)
	_ json.Unmarshaler = (*JSONObject)(nil)
)
