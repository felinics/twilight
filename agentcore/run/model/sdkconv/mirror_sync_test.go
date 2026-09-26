package sdkconv

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/run/model"
	"github.com/felinics/twilight/sdk"
	"github.com/google/jsonschema-go/jsonschema"
)

// The agent tier persists its own mirror of the SDK's model-call input, and the
// mirror's canonical JSON is what the run digest covers. A field that exists on
// one side and not the other silently leaves the identity, so the field sets,
// the JSON keys and the omit behaviour have to move together.
//
// The mirror is not a copy: sdk.Request carries open SDK interfaces and
// `any` payloads, while the mirror carries canonical JSON (see
// TestMirrorCanonicalFields), and sdk.Message.Content holds sdk.MessagePart
// values where the mirror holds a closed MessagePart union (see
// TestMirrorCoversEveryMessagePartType). Those three differences are the only
// ones allowed; anything else fails here.
var mirroredTypes = []struct {
	name   string
	sdk    any
	mirror any
}{
	{"Request", sdk.Request{}, model.ModelRequest{}},
	{"ToolDefinition", sdk.ToolDefinition{}, model.ToolDefinition{}},
	{"ResponseFormat", sdk.ResponseFormat{}, model.ResponseFormat{}},
	{"ToolChoice", sdk.ToolChoice{}, model.ToolChoice{}},
	{"CacheControl", sdk.CacheControl{}, model.CacheControl{}},
	{"Usage", sdk.Usage{}, model.Usage{}},
	{"Message", sdk.Message{}, model.Message{}},
	{"ModelResult", sdk.ModelResult{}, model.ModelResult{}},
	{"ToolCall", sdk.ToolCall{}, model.ModelToolCall{}},
	{"ToolArguments", sdk.ToolArguments{}, model.ToolArguments{}},
	{"ToolOutput", sdk.ToolOutput{}, model.ToolOutput{}},
}

// expectedDifference names the fields that are deliberately absent from the
// mirror of an SDK type, with the reason. Every entry has to stay justified:
// a field dropped from the persisted mirror is a field the digest stops
// covering.
var expectedDifference = map[string]map[string]string{}

type frozenField struct {
	jsonName  string
	omitsZero bool
}

func frozenFields(t reflect.Type) map[string]frozenField {
	out := make(map[string]frozenField, t.NumField())
	for i := range t.NumField() {
		f := t.Field(i)
		tag := f.Tag.Get("json")
		if tag == "-" {
			continue
		}
		name, options, _ := strings.Cut(tag, ",")
		if name == "" {
			name = f.Name
		}
		out[f.Name] = frozenField{jsonName: name, omitsZero: strings.Contains(options, "omit")}
	}
	return out
}

// fieldType reports a named field's type, or nil when the type has no such
// field, so a renamed field fails loudly instead of panicking.
func fieldType(t reflect.Type, name string) reflect.Type {
	f, ok := t.FieldByName(name)
	if !ok {
		return nil
	}
	return f.Type
}

func TestMirrorFieldSetsMatchSDK(t *testing.T) {
	for _, tc := range mirroredTypes {
		t.Run(tc.name, func(t *testing.T) {
			sdkFields := frozenFields(reflect.TypeOf(tc.sdk))
			mirrorFields := frozenFields(reflect.TypeOf(tc.mirror))
			allowed := expectedDifference[tc.name]

			for name, want := range sdkFields {
				if reason, ok := allowed[name]; ok {
					if _, present := mirrorFields[name]; present {
						t.Errorf("run.%s.%s is back in the mirror, drop it from expectedDifference (%s)", tc.name, name, reason)
					}
					continue
				}
				got, ok := mirrorFields[name]
				if !ok {
					t.Errorf("run.%s is missing field %s, which sdk.%s freezes as %q", tc.name, name, tc.name, want.jsonName)
					continue
				}
				if got.jsonName != want.jsonName {
					t.Errorf("run.%s.%s freezes as %q, sdk.%s.%s freezes as %q", tc.name, name, got.jsonName, tc.name, name, want.jsonName)
				}
				if got.omitsZero != want.omitsZero {
					t.Errorf("run.%s.%s omits an empty value: %t, sdk.%s.%s: %t", tc.name, name, got.omitsZero, tc.name, name, want.omitsZero)
				}
			}
			for name := range mirrorFields {
				if _, ok := sdkFields[name]; !ok {
					if _, ok := allowed[name]; ok {
						continue
					}
					t.Errorf("run.%s has field %s, which sdk.%s does not", tc.name, name, tc.name)
				}
			}
		})
	}
}

// TestMirrorCanonicalFields pins the fields where the mirror stops carrying the
// SDK's open shape and starts carrying canonical JSON. Widening or narrowing
// either side changes what the digest covers, so it has to be a deliberate edit
// here as well as in the freeze function.
func TestMirrorCanonicalFields(t *testing.T) {
	canonical := reflect.TypeOf(jsonstable.Value{})
	cases := []struct {
		sdk, mirror reflect.Type
		field       string
		wantSDK     reflect.Type
		wantMirror  reflect.Type
	}{
		{
			sdk: reflect.TypeOf(sdk.ToolDefinition{}), mirror: reflect.TypeOf(model.ToolDefinition{}),
			field:   "Parameters",
			wantSDK: reflect.TypeOf(&jsonschema.Schema{}), wantMirror: canonical,
		},
		{
			sdk: reflect.TypeOf(sdk.ToolArguments{}), mirror: reflect.TypeOf(model.ToolArguments{}),
			field:   "JSON",
			wantSDK: reflect.TypeOf(json.RawMessage{}), wantMirror: canonical,
		},
		{
			sdk: reflect.TypeOf(sdk.ToolOutput{}), mirror: reflect.TypeOf(model.ToolOutput{}),
			field:   "JSON",
			wantSDK: reflect.TypeOf(json.RawMessage{}), wantMirror: canonical,
		},
		{
			sdk: reflect.TypeOf(sdk.Request{}), mirror: reflect.TypeOf(model.ModelRequest{}),
			field:   "ProviderOptions",
			wantSDK: reflect.TypeOf(map[string]json.RawMessage{}), wantMirror: reflect.TypeOf(map[string]jsonstable.Value{}),
		},
	}
	for _, tc := range cases {
		t.Run(tc.mirror.Name()+"."+tc.field, func(t *testing.T) {
			if got := fieldType(tc.sdk, tc.field); got != tc.wantSDK {
				t.Errorf("sdk.%s.%s is %s, want %s", tc.sdk.Name(), tc.field, got, tc.wantSDK)
			}
			if got := fieldType(tc.mirror, tc.field); got != tc.wantMirror {
				t.Errorf("run.%s.%s is %s, want %s", tc.mirror.Name(), tc.field, got, tc.wantMirror)
			}
		})
	}

	// sdk.ResponseFormat.JSONSchema is a *jsonschema.Schema on the SDK side and
	// canonical JSON on the mirror side; the field is the one place the SDK
	// hands the agent tier a builder type rather than resolved JSON.
	if got := fieldType(reflect.TypeOf(model.ResponseFormat{}), "JSONSchema"); got != canonical {
		t.Errorf("run.ResponseFormat.JSONSchema is %s, want %s", got, canonical)
	}
	// Provider metadata is string tokens on both sides: the mirror carries
	// the SDK shape unchanged, so nothing is canonicalized on the way in.
	if got, want := reflect.TypeOf(model.ProviderMetadata{}).Elem(), reflect.TypeOf(sdk.ProviderMetadata{}).Elem(); got != want {
		t.Errorf("run.ProviderMetadata holds %s, sdk.ProviderMetadata holds %s", got, want)
	}
}

// TestMirrorCoversEveryMessagePartType freezes one value of every SDK part type
// and asserts that the mirror's closed union carries the same discriminator the
// SDK reports, and that the frozen part converts back to the same SDK type.
// The two constant families are declared separately, so this is what keeps
// sdk.MessagePartTypeText and run.MessagePartTypeText from drifting apart.
func TestMirrorCoversEveryMessagePartType(t *testing.T) {
	parts := []sdk.MessagePart{
		sdk.TextPart{Text: "hello"},
		sdk.ImagePart{Image: "aGk=", MediaType: "image/png"},
		sdk.FilePart{Data: "aGk=", MediaType: "text/plain", Filename: "a.txt"},
		sdk.ReasoningPart{ID: "rs_1", Text: "why", Model: "m"},
		sdk.ToolCallPart{ToolCallID: "call_1", ToolName: "f", Input: sdk.ParseToolArguments(`{"a":1}`)},
		sdk.ToolResultPart{ToolCallID: "call_1", ToolName: "f", Result: sdk.TextOutput("ok")},
	}

	for _, part := range parts {
		frozen, err := FreezeMessagePart(part)
		if err != nil {
			t.Errorf("FreezeMessagePart(%T): %v", part, err)
			continue
		}
		if got, want := string(frozen.Type), string(part.PartType()); got != want {
			t.Errorf("FreezeMessagePart(%T) wrote type %q, but the SDK calls it %q", part, got, want)
		}
		back, err := MessagePart(frozen)
		if err != nil {
			t.Errorf("MessagePart.SDK() for %T: %v", part, err)
			continue
		}
		if reflect.TypeOf(back) != reflect.TypeOf(part) {
			t.Errorf("%T froze and came back as %T", part, back)
			continue
		}
		// Parts whose payload the freeze step canonicalizes are compared by
		// shape above; parts that carry no open payload have to survive intact.
		switch part.(type) {
		case sdk.TextPart, sdk.ImagePart, sdk.FilePart:
			if !reflect.DeepEqual(back, part) {
				t.Errorf("%T did not survive the freeze round trip:\n got %#v\nwant %#v", part, back, part)
			}
		}
	}
}
