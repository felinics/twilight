package sdk

import (
	"encoding/json"
	"fmt"
)

// ToolOutput is what a tool returned, as the model will read it: plain text,
// or a JSON document in canonical form (CanonicalJSON). Exactly one of the
// two fields is set; the zero value is an empty text output. The
// constructors keep the canonical form; a literal ToolOutput{JSON: raw}
// carries whatever bytes it was given.
type ToolOutput struct {
	Text string          `json:"text,omitempty"`
	JSON json.RawMessage `json:"json,omitempty"`
}

// TextOutput is a text tool output.
func TextOutput(text string) ToolOutput { return ToolOutput{Text: text} }

// JSONOutput encodes v as a JSON tool output, in canonical form.
func JSONOutput(v any) (ToolOutput, error) {
	raw, err := canonicalMarshal(v)
	if err != nil {
		return ToolOutput{}, fmt.Errorf("twilightai: encode tool output: %w", err)
	}
	return ToolOutput{JSON: raw}, nil
}

// RawJSONOutput wraps an already encoded JSON document, re-encoded in
// canonical form; bytes that are not one JSON document are ErrInvalidJSON.
func RawJSONOutput(raw json.RawMessage) (ToolOutput, error) {
	canonical, err := CanonicalJSON(raw)
	if err != nil {
		return ToolOutput{}, fmt.Errorf("twilightai: tool output: %w", err)
	}
	return ToolOutput{JSON: canonical}, nil
}

// String is the output as text: the text itself, or the JSON document.
func (o ToolOutput) String() string {
	if o.JSON != nil {
		return string(o.JSON)
	}
	return o.Text
}

// IsJSON reports whether the output is a JSON document.
func (o ToolOutput) IsJSON() bool { return o.JSON != nil }
