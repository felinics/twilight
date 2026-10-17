package sessionstore

import (
	"errors"
	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/felinics/twilight/agentcore/module"
	"github.com/felinics/twilight/agentcore/preset/ref"
	"github.com/felinics/twilight/agentcore/turn"
	"reflect"
	"strings"
	"testing"
)

// withdrawnV1 stands in for a superseded wire shape of model_step_withdrawn
// in which the step was named "step": it decodes that shape and upcasts to
// the current turn.ModelStepWithdrawn. Encode is never used for a superseded
// version.
type withdrawnV1 struct{}

func (withdrawnV1) Validate(any) error { return errors.New("version 1 is superseded") }
func (withdrawnV1) Encode(any) (jsonstable.Value, error) {
	return jsonstable.Value{}, errors.New("version 1 is superseded")
}
func (withdrawnV1) Decode(w jsonstable.Value) (any, error) {
	var body struct {
		TurnID turn.TurnID `json:"turnId"`
		Step   turn.StepID `json:"step"`
	}
	if err := w.Decode(&body); err != nil {
		return nil, err
	}
	return Event{TurnID: body.TurnID, Fact: turn.ModelStepWithdrawn{StepID: body.Step}}, nil
}

// a fact type keeps the codec of every payload version it ever
// wrote. With a superseded version in its history the type encodes at the
// next version and still decodes the old wire to the current fact.
func TestFactCodecHistoryDecodesEveryVersion(t *testing.T) {
	const name = "model_step_withdrawn"
	codecs, current := factCodecs(name, map[module.PayloadVersion]module.PayloadCodec{1: withdrawnV1{}})
	if current != 2 || len(codecs) != 2 {
		t.Fatalf("history = %d codecs at version %d, want 2 at 2", len(codecs), current)
	}
	mod := module.ModuleDescriptor{Source: module.SourceTwilight, ID: ModuleID,
		Streams: []module.StreamDefinition{streamDefinition}, Events: []module.EventDefinition{eventDefinition(name, codecs, current)}}
	reg, err := module.BuildRegistry(mod)
	if err != nil {
		t.Fatal(err)
	}
	typ := Type(name)
	want := Event{TurnID: "r1", Fact: turn.ModelStepWithdrawn{StepID: "s1"}}

	wire, err := reg.Encode(typ, want)
	if err != nil || !strings.Contains(wire.String(), `"v":2`) {
		t.Fatalf("encode = %s %v, want the current version 2", wire, err)
	}
	cases := []struct {
		name    string
		payload string
		version module.PayloadVersion
	}{
		{"superseded wire", `{"turnId":"r1","step":"s1","v":1}`, 1},
		{"current wire", wire.String(), 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, err := reg.Decode(ledger.Event{Type: typ, Payload: jsonstable.MustParse(tc.payload)})
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
	factCodecs("model_step_withdrawn", map[module.PayloadVersion]module.PayloadCodec{2: withdrawnV1{}})
}

func TestTurnStartAndTerminalSurfaceRoundtrip(t *testing.T) {
	preset := ref.Ref{ID: "p", Digest: "sha256:p"}
	started := turn.TurnStarted{TurnID: "t", Preset: preset}
	codec := factCodec{local: "turn_started"}
	raw, err := codec.Encode(Event{TurnID: "t", Fact: started})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := codec.Decode(raw)
	if err != nil || !reflect.DeepEqual(decoded, Event{TurnID: "t", Fact: started}) {
		t.Fatalf("start roundtrip = %+v %v", decoded, err)
	}
	for _, end := range []turn.TurnEnd{turn.TurnCompletedEnd{}, turn.TurnFailedEnd{Reason: turn.ReasonProviderFailure, Failure: turn.TurnFailure{Class: turn.FailureProvider}}, turn.TurnStoppedEnd{Reason: turn.ReasonCancelled, Message: "requested"}} {
		want := TurnSurface{Order: []turn.TurnID{"t"}, Turns: map[turn.TurnID]TurnView{"t": {TurnID: "t", Preset: preset, End: &turn.TurnEnded{End: end}}}}
		raw, err := SurfaceProjection.StateCodec.Encode(want)
		if err != nil {
			t.Fatal(err)
		}
		got, err := SurfaceProjection.StateCodec.Decode(raw)
		if err != nil || !reflect.DeepEqual(got, want) {
			t.Fatalf("surface %T roundtrip = %+v %v", end, got, err)
		}
	}
	for _, ev := range []Event{{TurnID: "other", Fact: started}, {TurnID: "t", Fact: turn.TurnStarted{TurnID: "t"}}} {
		if _, err := codec.Encode(ev); err == nil {
			t.Fatalf("invalid start encoded: %+v", ev)
		}
	}
	if _, err := codec.Decode(jsonstable.MustParse("{\"turnId\":\"t\"}")); err == nil {
		t.Fatal("start without preset decoded")
	}
}
