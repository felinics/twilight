package module

import (
	"testing"
)

type goldenPayload struct {
	B int `json:"b"`
}

// TestEncodeWireGolden freezes the framework's payload wire: the canonical
// bytes after the `v` field is injected (EXT-COD-2). A drift with a non-empty
// want is an intentional wire change or an accident — update the fixture and
// agent-session-md only for the former.
func TestEncodeWireGolden(t *testing.T) {
	reg, err := BuildRegistry(ModuleDescriptor{
		Source:  "goldsrc",
		ID:      "gold",
		Streams: []StreamDefinition{{Domain: "gold", Inheritance: Inherited}},
		Events: []EventDefinition{{
			Type:   "goldsrc/gold/sample",
			Domain: "gold",
			Codecs: map[PayloadVersion]PayloadCodec{1: JSONCodec[goldenPayload]{}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	wire, err := reg.Encode("goldsrc/gold/sample", goldenPayload{B: 1})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if got, want := wire.String(), `{"b":1,"v":1}`; got != want {
		t.Fatalf("golden encoded payload drifted:\n got: %s\nwant: %s", got, want)
	}
}
