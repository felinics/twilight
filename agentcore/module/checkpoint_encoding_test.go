package module

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/felinics/twilight/agentcore/jsonstable"
	"github.com/felinics/twilight/agentcore/ledger"
	"github.com/gowebpki/jcs"
)

func legacyCheckpointDigest(t testing.TB, id ProjectionID, v ProjectionVersion, through ledger.Head, state jsonstable.Value) jsonstable.Digest {
	t.Helper()
	raw, err := json.Marshal([]string{string(id), fmt.Sprint(uint64(v)), fmt.Sprint(uint64(through.Next)), state.String()})
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := jcs.Transform(raw)
	if err != nil {
		t.Fatal(err)
	}
	return jsonstable.DigestBytes(append([]byte("v1:30:twilight/projection-checkpoint:"), canonical...))
}

func legacyOpenCheckpoint(id ProjectionID, v ProjectionVersion, through ledger.Head, sealed jsonstable.Value) (jsonstable.Value, bool) {
	var c sealedCheckpoint
	if err := sealed.Decode(&c); err != nil || len(c.State) == 0 || c.Digest == "" {
		return jsonstable.Value{}, false
	}
	state, err := jsonstable.Parse(c.State)
	if err != nil {
		return jsonstable.Value{}, false
	}
	// Use the original generic encoding, independently of the optimized shortcut.
	canonical, err := jsonstable.MarshalCanonical([]string{string(id), fmt.Sprint(uint64(v)), fmt.Sprint(uint64(through.Next)), state.String()})
	if err != nil {
		return jsonstable.Value{}, false
	}
	digest := jsonstable.DigestBytes(append([]byte("v1:30:twilight/projection-checkpoint:"), canonical...))
	if digest != c.Digest {
		return jsonstable.Value{}, false
	}
	return state, true
}

func TestCheckpointEncodingLegacyParity(t *testing.T) {
	const id ProjectionID = "twilight/test/中文<>&\u2028"
	through := ledger.Head{Next: 123}
	states := []jsonstable.Value{
		{}, jsonstable.MustParse("null"), jsonstable.MustParse(`{}`), jsonstable.MustParse(`[]`),
		jsonstable.MustParse(`{"z":1e2,"a":"\u0000\b\f\n\r\t<>&😀\\\""}`),
		jsonstable.MustParse(strings.Repeat("[", 100) + "null" + strings.Repeat("]", 100)),
	}
	large, err := jsonstable.FromValue(map[string]any{"history": strings.Repeat("hello<>&😀\n", 10000)})
	if err != nil {
		t.Fatal(err)
	}
	states = append(states, large)
	for i, state := range states {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			digest := legacyCheckpointDigest(t, id, 2, through, state)
			legacyRaw, err := json.Marshal(sealedCheckpoint{State: state.Bytes(), Digest: digest})
			if err != nil {
				t.Fatal(err)
			}
			want, err := jcs.Transform(legacyRaw)
			if err != nil {
				t.Fatal(err)
			}
			sealed, err := SealCheckpoint(id, 2, through, state)
			if err != nil {
				t.Fatal(err)
			}
			if sealed.String() != string(want) {
				t.Fatal("sealed wire bytes changed")
			}
			opened, ok := OpenCheckpoint(id, 2, through, sealed)
			if !ok || !opened.Equal(state) {
				t.Fatal("valid legacy seal rejected")
			}
			for _, binding := range []struct {
				id   ProjectionID
				v    ProjectionVersion
				head ledger.Head
			}{
				{id, 2, through}, {"other", 2, through}, {id, 3, through}, {id, 2, ledger.Head{Next: 124}},
			} {
				got, ok := OpenCheckpoint(binding.id, binding.v, binding.head, sealed)
				old, oldOK := legacyOpenCheckpoint(binding.id, binding.v, binding.head, sealed)
				if ok != oldOK || !got.Equal(old) {
					t.Fatal("binding acceptance changed")
				}
				if ok != (binding.id == id && binding.v == 2 && binding.head == through) {
					t.Fatal("incorrect checkpoint binding")
				}
			}
		})
	}
}

func TestOpenCheckpointEnvelopeLegacyParity(t *testing.T) {
	const id ProjectionID = "twilight/test/projection"
	through := ledger.Head{Next: 8}
	state := jsonstable.MustParse(`{"a":1}`)
	digest := legacyCheckpointDigest(t, id, 1, through, state)
	nullDigest := legacyCheckpointDigest(t, id, 1, through, jsonstable.Value{})
	cases := []struct {
		raw   string
		valid bool
	}{
		{`null`, false}, {`{}`, false}, {`[]`, false}, {`1`, false}, {`"bad"`, false},
		{`{"state":{}}`, false}, {`{"digest":"x"}`, false},
		{`{"state":{},"digest":null}`, false}, {`{"state":{},"digest":12}`, false},
		{`{"state":{},"digest":""}`, false},
		{fmt.Sprintf(`{"state":{"a":2},"digest":%q}`, digest), false},
		{fmt.Sprintf(`{"state":{"a":1},"digest":%q,"unknown":true}`, digest), true},
		{fmt.Sprintf(`{"STATE":{"a":1},"DIGEST":%q}`, digest), true},
		{fmt.Sprintf(`{"state":null,"digest":%q}`, nullDigest), true},
	}
	for i, tc := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			sealed := jsonstable.MustParse(tc.raw)
			got, ok := OpenCheckpoint(id, 1, through, sealed)
			old, oldOK := legacyOpenCheckpoint(id, 1, through, sealed)
			if ok != oldOK || !got.Equal(old) || ok != tc.valid {
				t.Fatalf("acceptance changed: got %v old %v want %v", ok, oldOK, tc.valid)
			}
		})
	}
	if _, ok := OpenCheckpoint(id, 1, through, jsonstable.Value{}); ok {
		t.Fatal("zero envelope accepted")
	}
}

func BenchmarkOpenCheckpoint(b *testing.B) {
	const id ProjectionID = "twilight/test/projection"
	through := ledger.Head{Next: 128}
	state, err := jsonstable.FromValue(map[string]any{"history": strings.Repeat(`hello <>&😀`, 3000)})
	if err != nil {
		b.Fatal(err)
	}
	sealed, err := SealCheckpoint(id, 1, through, state)
	if err != nil {
		b.Fatal(err)
	}
	for _, legacy := range []bool{true, false} {
		b.Run(fmt.Sprint("legacy=", legacy), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				var ok bool
				if legacy {
					_, ok = legacyOpenCheckpoint(id, 1, through, sealed)
				} else {
					_, ok = OpenCheckpoint(id, 1, through, sealed)
				}
				if !ok {
					b.Fatal("checkpoint rejected")
				}
			}
		})
	}
}
