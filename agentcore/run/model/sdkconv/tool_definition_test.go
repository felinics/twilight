package sdkconv

import (
	"encoding/json"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// The definition digest covers the canonical JSON of the frozen schema. A
// schema that is thawed into the SDK type and frozen again has to produce the
// same bytes, or a replayed Request would carry a definition whose digest no
// longer matches the frozen ToolSpec.
func TestToolDefinitionRoundTripKeepsCanonicalBytes(t *testing.T) {
	f := 1.5
	v := any("v")
	cases := []struct {
		name   string
		schema *jsonschema.Schema
	}{
		{"nil schema", nil},
		{"empty object", &jsonschema.Schema{Type: "object"}},
		{"properties required enum", &jsonschema.Schema{
			Type: "object",
			Properties: map[string]*jsonschema.Schema{
				"task": {Type: "string", Description: "What to do."},
				"mode": {Type: "string", Enum: []any{"spawn", "fork"}},
				"n":    {Type: "number", Minimum: &f},
			},
			Required: []string{"task"},
		}},
		{"additionalProperties false", &jsonschema.Schema{
			Type: "object", AdditionalProperties: &jsonschema.Schema{Not: &jsonschema.Schema{}},
		}},
		{"array items and multiple types", &jsonschema.Schema{
			Type:  "array",
			Items: &jsonschema.Schema{Types: []string{"string", "null"}},
		}},
		{"default const and extra keyword", &jsonschema.Schema{
			Type:    "object",
			Default: json.RawMessage(`{"a":[1,2]}`),
			Properties: map[string]*jsonschema.Schema{
				"k": {Const: &v},
			},
			Extra: map[string]any{"x-vendor": map[string]any{"hint": true}},
		}},
		{"property order", &jsonschema.Schema{
			Type:          "object",
			Properties:    map[string]*jsonschema.Schema{"b": {Type: "string"}, "a": {Type: "string"}},
			PropertyOrder: []string{"b", "a"},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			first, err := FreezeToolDefinition(sdk.ToolDefinition{Name: "t", Parameters: tc.schema})
			if err != nil {
				t.Fatal(err)
			}
			thawed, err := ToolDefinition(first)
			if err != nil {
				t.Fatal(err)
			}
			second, err := FreezeToolDefinition(thawed)
			if err != nil {
				t.Fatal(err)
			}
			if !first.Parameters.Equal(second.Parameters) {
				t.Fatalf("schema drifted across freeze/thaw/freeze:\n first: %s\nsecond: %s", first.Parameters, second.Parameters)
			}
			if (tc.schema == nil) != (thawed.Parameters == nil) {
				t.Fatalf("nil-ness changed: in %v, out %v", tc.schema == nil, thawed.Parameters == nil)
			}
		})
	}
}

// A schema authored as a JSON document is normalized by the library on the
// way in: boolean schemas become {"not":true} / {} and back, and integer
// keywords are re-rendered. The cases here pin what the normalization does to
// the canonical bytes, so a schema ported from a literal is compared
// knowingly.
func TestSchemaDocumentNormalization(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string // canonical bytes after Unmarshal + Marshal
	}{
		{"plain object", `{"type":"object","properties":{"q":{"type":"string"}}}`, `{"properties":{"q":{"type":"string"}},"type":"object"}`},
		{"additionalProperties false", `{"type":"object","additionalProperties":false}`, `{"additionalProperties":false,"type":"object"}`},
		{"additionalProperties true", `{"type":"object","additionalProperties":true}`, `{"additionalProperties":true,"type":"object"}`},
		{"unknown keyword kept", `{"type":"object","x-vendor":{"hint":1}}`, `{"type":"object","x-vendor":{"hint":1}}`},
		{"integer keywords", `{"type":"string","minLength":1,"maxLength":10}`, `{"maxLength":10,"minLength":1,"type":"string"}`},
		{"empty schema becomes true", `{}`, `true`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var s jsonschema.Schema
			if err := json.Unmarshal([]byte(tc.in), &s); err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(&s)
			if err != nil {
				t.Fatal(err)
			}
			got, err := jsonstable.Parse(raw)
			if err != nil {
				t.Fatal(err)
			}
			if got.String() != tc.want {
				t.Fatalf("Unmarshal+Marshal(%s) = %s, want %s", tc.in, got, tc.want)
			}
		})
	}
}
