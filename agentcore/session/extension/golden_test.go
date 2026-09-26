package extension_test

import (
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
	"testing"
)

type goldenPayload struct {
	B int `json:"b"`
}

// TestEncodeWireGolden freezes the framework's payload wire: the canonical
// bytes after the `v` field is injected (EXT-COD-2). A drift with a non-empty
// want is an intentional wire change or an accident — update the fixture and
// agent-session-extension.md only for the former.
func TestEncodeWireGolden(t *testing.T) {
	reg, err := extension.BuildRegistry(extension.ModuleDescriptor{
		Source:  "goldsrc",
		ID:      "gold",
		Streams: []extension.StreamDefinition{{Domain: "gold", Lineage: session.LineageSession}},
		Events: []extension.EventDefinition{{
			Type:   "goldsrc/gold/sample",
			Stream: "gold",
			Codecs: map[extension.PayloadVersion]extension.PayloadCodec{1: extension.JSONCodec[goldenPayload]{}},
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
