package model

import (
	"encoding/json"

	"github.com/felinics/twilight/agentcore/jsonstable"
)

// ToolArguments mirrors sdk.ToolArguments: the arguments a model supplied for
// a tool call. JSON holds the canonical document when the model produced one;
// Text holds the verbatim text when it did not, so the invalid_arguments
// result the model receives quotes what it wrote. Exactly one field is set;
// the zero value stands for the empty object.
type ToolArguments struct {
	JSON jsonstable.Value `json:"json,omitzero"`
	Text string           `json:"text,omitempty"`
}

// Valid reports whether the arguments are a JSON document a tool can decode.
func (a ToolArguments) Valid() bool { return a.Text == "" }

// Canonical is the binding form of the arguments (RUN-MCH-2): the document
// itself, the empty object for the zero value, or the invalid text as a JSON
// string so the binding digest still covers what the model wrote and
// validation fails as invalid_arguments.
func (a ToolArguments) Canonical() jsonstable.Value {
	if !a.Valid() {
		raw, _ := json.Marshal(a.Text) //nolint:errchkjson // a string always marshals
		return jsonstable.MustParse(string(raw))
	}
	if a.JSON.IsZero() {
		return jsonstable.MustParse("{}")
	}
	return a.JSON
}

// ToolOutput mirrors sdk.ToolOutput: what a tool returned, as text or as a
// canonical JSON document. Exactly one field is set; the zero value is an
// empty text output.
type ToolOutput struct {
	Text string           `json:"text,omitempty"`
	JSON jsonstable.Value `json:"json,omitzero"`
}
