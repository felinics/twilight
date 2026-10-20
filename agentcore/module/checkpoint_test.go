package module

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
)

func TestCheckpointEncodingMatchesLegacyPreimage(t *testing.T) {
	for _, raw := range []string{`null`, `{"x":"<>&\u2028中文","n":1.25}`, `{"zero":0,"empty":[],"nested":{"a":true}}`} {
		state := jsonstable.MustParse(raw)
		id, version, head := ProjectionID("test/checkpoint"), ProjectionVersion(2), ledger.Head{Next: 123}
		preimage, err := jsonstable.EncodeTypedPayload(1, checkpointDomain, []string{string(id), fmt.Sprint(uint64(version)), fmt.Sprint(uint64(head.Next)), string(state.Bytes())})
		if err != nil {
			t.Fatal(err)
		}
		want := jsonstable.DigestBytes(preimage)
		legacy, err := jsonstable.FromValue(sealedCheckpoint{State: state.Bytes(), Digest: want})
		if err != nil {
			t.Fatal(err)
		}
		got, err := SealCheckpoint(id, version, head, state)
		if err != nil || !got.Equal(legacy) {
			t.Fatalf("seal = %s, %v; want %s", got, err, legacy)
		}
		opened, ok := OpenCheckpoint(id, version, head, got)
		if !ok || !opened.Equal(state) {
			t.Fatalf("open = %s, %v", opened, ok)
		}
		for _, bad := range []struct {
			id      ProjectionID
			version ProjectionVersion
			head    ledger.Head
		}{
			{id + "other", version, head}, {id, version + 1, head}, {id, version, ledger.Head{Next: head.Next + 1}},
		} {
			if _, ok := OpenCheckpoint(bad.id, bad.version, bad.head, got); ok {
				t.Fatal("accepted wrong checkpoint identity")
			}
		}
		wire := map[string]any{"state": json.RawMessage(state.Bytes()), "digest": want, "extension": "allowed"}
		extended, err := jsonstable.FromValue(wire)
		if err != nil {
			t.Fatal(err)
		}
		if v, ok := OpenCheckpoint(id, version, head, extended); !ok || !v.Equal(state) {
			t.Fatal("unknown envelope fields changed behavior")
		}
	}
	for _, raw := range []string{`null`, `[]`, `{}`, `{"state":{},"digest":1}`, `{"state":{},"digest":"bad"}`} {
		if _, ok := OpenCheckpoint("test/checkpoint", 2, ledger.Head{Next: 123}, jsonstable.MustParse(raw)); ok {
			t.Fatalf("accepted invalid checkpoint %s", raw)
		}
	}
}
