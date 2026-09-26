package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ToolArguments is what the model supplied as a tool call's arguments. A model
// emits argument text; when that text is a JSON document it is kept in JSON
// in canonical form (CanonicalJSON, RFC 8785), and the call can be executed. When it is not -- a
// truncated or malformed call, which chat-completions style APIs deliver as
// a plain string, or an object with a repeated member -- the text is kept
// verbatim in Text and JSON is nil: the call is still reported, so the
// caller can answer the model with an invalid-arguments result and let it
// correct itself; Valid tells the caller not to run it. Exactly one of the
// two fields is set. The constructors keep the canonical form; a literal
// ToolArguments{JSON: raw} carries whatever bytes it was given.
type ToolArguments struct {
	JSON json.RawMessage `json:"json,omitempty"`
	Text string          `json:"text,omitempty"`
}

// ErrInvalidToolArguments reports arguments that are not a JSON document.
var ErrInvalidToolArguments = errors.New("twilightai: tool arguments are not valid JSON")

// ParseToolArguments classifies the argument text a provider received. Empty
// text is the empty object, the value providers substitute when a model calls
// a tool that takes no arguments. A JSON document is kept in canonical form;
// text that is not valid UTF-8, not a JSON document, or an object with a
// repeated member is kept as Text.
func ParseToolArguments(text string) ToolArguments {
	if text == "" {
		return ToolArguments{JSON: json.RawMessage(`{}`)}
	}
	canonical, err := CanonicalJSON([]byte(text))
	if err != nil {
		return ToolArguments{Text: text}
	}
	return ToolArguments{JSON: canonical}
}

// ToolArgumentsJSON encodes v as the arguments of a tool call, in canonical
// form.
func ToolArgumentsJSON(v any) (ToolArguments, error) {
	raw, err := canonicalMarshal(v)
	if err != nil {
		return ToolArguments{}, fmt.Errorf("twilightai: encode tool arguments: %w", err)
	}
	return ToolArguments{JSON: raw}, nil
}

// Valid reports whether the arguments are a JSON document a tool can decode:
// a document, or the zero value standing for the empty object.
func (a ToolArguments) Valid() bool { return a.Text == "" }

// IsZero reports arguments that carry neither a document nor text.
func (a ToolArguments) IsZero() bool { return a.JSON == nil && a.Text == "" }

// Unmarshal decodes the JSON document into v; invalid arguments are
// ErrInvalidToolArguments.
func (a ToolArguments) Unmarshal(v any) error {
	if !a.Valid() {
		return ErrInvalidToolArguments
	}
	if a.JSON == nil {
		return json.Unmarshal([]byte(`{}`), v)
	}
	return json.Unmarshal(a.JSON, v)
}

// String is the argument text as the model produced it: the JSON document, or
// the invalid text verbatim. It is what a string-typed wire field carries.
func (a ToolArguments) String() string {
	if !a.Valid() {
		return a.Text
	}
	if a.JSON == nil {
		return "{}"
	}
	return string(a.JSON)
}

// Object is the arguments as a JSON value for an object-typed wire field: the
// document itself, or the empty object when the arguments were invalid.
func (a ToolArguments) Object() json.RawMessage {
	if a.Valid() && a.JSON != nil {
		return append(json.RawMessage(nil), a.JSON...)
	}
	return json.RawMessage(`{}`)
}

func (a ToolArguments) clone() ToolArguments {
	if a.JSON != nil {
		a.JSON = append(json.RawMessage(nil), a.JSON...)
	}
	return a
}
