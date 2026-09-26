package runmod

import (
	"errors"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/run"
	"github.com/felinics/twilight/agentcore/session"
	"github.com/felinics/twilight/agentcore/session/extension"
)

// withdrawnV1 stands in for a superseded wire shape of model_step_withdrawn
// in which the step was named "step": it decodes that shape and upcasts to
// the current run.ModelStepWithdrawn. Encode is never used for a superseded
// version.
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

// EXT-REG-2: a fact type keeps the codec of every payload version it ever
// wrote. With a superseded version in its history the type encodes at the
// next version and still decodes the old wire to the current fact.
func TestFactCodecHistoryDecodesEveryVersion(t *testing.T) {
	const name = "model_step_withdrawn"
	codecs, current := factCodecs(name, map[extension.PayloadVersion]extension.PayloadCodec{1: withdrawnV1{}})
	if current != 2 || len(codecs) != 2 {
		t.Fatalf("history = %d codecs at version %d, want 2 at 2", len(codecs), current)
	}
	module := extension.ModuleDescriptor{Source: extension.SourceTwilight, ID: ModuleID,
		Streams: []extension.StreamDefinition{streamDefinition}, Events: []extension.EventDefinition{eventDefinition(name, codecs, current)}}
	reg, err := extension.BuildRegistry(module)
	if err != nil {
		t.Fatal(err)
	}
	typ := Type(name)
	want := Event{RunID: "r1", Fact: run.ModelStepWithdrawn{StepID: "s1"}}

	wire, err := reg.Encode(typ, want)
	if err != nil || !strings.Contains(wire.String(), `"v":2`) {
		t.Fatalf("encode = %s %v, want the current version 2", wire, err)
	}
	cases := []struct {
		name    string
		payload string
		version extension.PayloadVersion
	}{
		{"superseded wire", `{"runId":"r1","step":"s1","v":1}`, 1},
		{"current wire", wire.String(), 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, err := reg.Decode(session.Event{Type: typ, Payload: jsonstable.MustParse(tc.payload)})
			if err != nil || d.Unknown || d.Version != tc.version || d.Value != want {
				t.Fatalf("decode = %+v %v, want version %d value %+v", d, err, tc.version, want)
			}
		})
	}
}

// The production module has no history yet: every fact type is at version 1
// with one codec. A fact whose wire shape changes must add its previous
// codec to olderFactCodecs rather than edit the current one in place.
func TestEveryFactIsAtVersionOne(t *testing.T) {
	for _, def := range Module.Events {
		if def.Version != 1 || len(def.Codecs) != 1 || def.Codecs[1] == nil {
			t.Fatalf("%s: version %d with %d codecs, want version 1 with one codec", def.Type, def.Version, len(def.Codecs))
		}
	}
}

// A history with a gap is caught when the module is built.
func TestFactCodecHistoryMustBeContiguous(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("a history without version 1 built")
		}
	}()
	factCodecs("model_step_withdrawn", map[extension.PayloadVersion]extension.PayloadCodec{2: withdrawnV1{}})
}
