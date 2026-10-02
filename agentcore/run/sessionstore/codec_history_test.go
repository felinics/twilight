package sessionstore

import (
	"errors"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/run"
)

// withdrawnV1 stands in for a superseded stable wire shape of
// model_step_withdrawn in which the step was named "step": it decodes that
// shape and upcasts to the current run.ModelStepWithdrawn. Encode is never
// used for a superseded version.
type withdrawnV1 struct{}

func (withdrawnV1) Validate(any) error { return errors.New("version 1 is superseded") }
func (withdrawnV1) Encode(any) (jsonstable.Value, error) {
	return jsonstable.Value{}, errors.New("version 1 is superseded")
}
func (withdrawnV1) Decode(w jsonstable.Value) (any, error) {
	var body struct {
		RunID run.RunID  `json:"runId"`
		Step  run.StepID `json:"step"`
	}
	if err := w.Decode(&body); err != nil {
		return nil, err
	}
	return Event{RunID: body.RunID, Fact: run.ModelStepWithdrawn{StepID: body.Step}}, nil
}

// EXT-REG-2: once released, a fact type keeps the codec of every stable
// payload version it ever wrote. With a superseded stable version in its
// history the type encodes at the next stable version and still decodes the
// old wire to the current fact.
func TestFactCodecHistoryDecodesEveryVersion(t *testing.T) {
	const name = "model_step_withdrawn"
	codecs, current := factCodecs(name, map[module.PayloadVersion]module.PayloadCodec{module.Stable(1): withdrawnV1{}})
	if current != module.Stable(2) || len(codecs) != 2 {
		t.Fatalf("history = %d codecs at version %s, want 2 at 2", len(codecs), current)
	}
	mod := module.ModuleDescriptor{Source: module.SourceTwilight, ID: ModuleID,
		Streams: []module.StreamDefinition{streamDefinition}, Events: []module.EventDefinition{eventDefinition(name, codecs, current)}}
	reg, err := module.BuildRegistry(mod)
	if err != nil {
		t.Fatal(err)
	}
	typ := Type(name)
	want := Event{RunID: "r1", Fact: run.ModelStepWithdrawn{StepID: "s1"}}

	wire, err := reg.Encode(typ, want)
	if err != nil || !strings.Contains(wire.String(), `"v":"2"`) {
		t.Fatalf("encode = %s %v, want the current stable version 2", wire, err)
	}
	cases := []struct {
		name    string
		payload string
		version module.PayloadVersion
	}{
		{"superseded wire", `{"runId":"r1","step":"s1","v":"1"}`, module.Stable(1)},
		{"current wire", wire.String(), module.Stable(2)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, err := reg.Decode(ledger.Event{Type: typ, Payload: jsonstable.MustParse(tc.payload)})
			if err != nil || d.Unknown || d.Version != tc.version || d.Value != want {
				t.Fatalf("decode = %+v %v, want version %s value %+v", d, err, tc.version, want)
			}
		})
	}
}

// Before the release every fact type is written at the module's one
// prerelease Version with no history: a shape that changes is changed in
// place and the number moves. A payload of an earlier prerelease shape is
// not read.
func TestPrereleaseFactsHaveNoHistory(t *testing.T) {
	if !Version.Prerelease {
		t.Skip("released: fact types may carry a stable codec history")
	}
	for _, def := range Module.Events {
		if def.Version != Version || len(def.Codecs) != 1 || def.Codecs[Version] == nil {
			t.Fatalf("%s: version %s with %d codecs, want %s with one codec", def.Type, def.Version, len(def.Codecs), Version)
		}
	}
	reg, err := module.BuildRegistry(Module)
	if err != nil {
		t.Fatal(err)
	}
	typ := Type("model_step_withdrawn")
	earlier := module.Pre(Version.Number - 1)
	if earlier.IsZero() {
		earlier = module.Pre(Version.Number + 1)
	}
	d, err := reg.Decode(ledger.Event{Type: typ, Payload: jsonstable.MustParse(`{"runId":"r1","stepId":"s1","v":"` + earlier.String() + `"}`)})
	if err != nil || !d.Unknown {
		t.Fatalf("decode of another prerelease shape = %+v %v, want Unknown", d, err)
	}
}

// A history with a gap is caught when the module is built.
func TestFactCodecHistoryMustBeContiguous(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a history without stable version 1 built")
		}
	}()
	factCodecs("model_step_withdrawn", map[module.PayloadVersion]module.PayloadCodec{module.Stable(2): withdrawnV1{}})
}
