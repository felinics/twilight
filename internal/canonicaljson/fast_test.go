package canonicaljson

import (
	"bytes"
	"encoding/json"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/gowebpki/jcs"
)

func TestCanonicalScannerMatchesJCS(t *testing.T) {
	cases := []string{
		`null`, `true`, `false`, `0`, `1`, `-1`, `9007199254740991`, `-9007199254740991`,
		`{"a":[true,false,null,"中😀\u0000\u001f\b\f\n\r\t\\\""],"b":{}}`,
		`{"":1,"a":2}`, `[]`, `{}`, `"é\u0001"`,
		``, `-0`, `00`, `01`, `1.0`, `1e2`, `9007199254740992`, `18446744073709551616`,
		`{"b":1,"a":2}`, `{"a":1,"a":2}`, `{"a":1,"\u0061":2}`,
		`{"😀":1,"\ufffd":2}`, `{"a":1} garbage`, `[1,]`, `[00]`,
		`"\ud800"`, `"\udc00"`, `"\ud83d\ude00"`, `"\u000a"`, `"\u001F"`, `"\/"`,
		`"\u00e9"`, `"\x00"`, `"\u0020"`, `"\u0008"`, `"\u0009"`, `"\u000c"`, `"\u000d"`,
		`"` + string([]byte{0xff}) + `"`, strings.Repeat("[", 140) + "0" + strings.Repeat("]", 140),
	}
	for _, raw := range cases {
		fast := utf8.ValidString(raw) && isCanonical([]byte(raw))
		want, oldErr := jcs.Transform([]byte(raw))
		if fast && (oldErr != nil || !bytes.Equal(want, []byte(raw))) {
			t.Fatalf("unsafe fast acceptance %q: JCS=%q, %v", raw, want, oldErr)
		}
		got, err := Transform([]byte(raw))
		invalid := !utf8.ValidString(raw) || rejectEscapedLoneSurrogates([]byte(raw)) != nil || oldErr != nil
		if invalid {
			if err == nil {
				t.Errorf("invalid input accepted: %q", raw)
			}
			continue
		}
		if err != nil || !bytes.Equal(got, want) {
			t.Errorf("changed JCS bytes for %q: %q vs %q, %v", raw, got, want, err)
		}
	}
}

func TestCanonicalScannerRandomizedDifferential(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 37))
	var value func(int) any
	value = func(depth int) any {
		kind := rng.IntN(7)
		if depth > 3 {
			kind = rng.IntN(5)
		}
		switch kind {
		case 0:
			return nil
		case 1:
			return rng.IntN(2) == 0
		case 2:
			return rng.Int64N(1<<54) - (1 << 53)
		case 3:
			return math.Float64frombits(rng.Uint64() & 0xffefffffffffffff)
		case 4:
			parts := []string{"abc", "中", "😀", "\u0000", "\b\f\n\r\t", "\\\"/", "<&>"}
			return parts[rng.IntN(len(parts))]
		case 5:
			out := make([]any, rng.IntN(5))
			for i := range out {
				out[i] = value(depth + 1)
			}
			return out
		default:
			out := map[string]any{}
			keys := []string{"a", "z", "ab", "", "中", "😀", "\ufffd", "\n"}
			for range rng.IntN(6) {
				out[keys[rng.IntN(len(keys))]] = value(depth + 1)
			}
			return out
		}
	}
	for range 5000 {
		raw, err := json.Marshal(value(0))
		if err != nil {
			t.Fatal(err)
		}
		want, err := jcs.Transform(raw)
		if err != nil {
			t.Fatal(err)
		}
		for _, input := range [][]byte{raw, want} {
			if isCanonical(input) && !bytes.Equal(input, want) {
				t.Fatalf("unsafe scanner acceptance: %s != %s", input, want)
			}
			got, err := Transform(input)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("changed JCS result: %s, want %s: %v", got, want, err)
			}
		}
	}
}

func TestTransformReturnsDetachedBytes(t *testing.T) {
	for _, raw := range []string{`{"a":1}`, `{ "b":2,"a":1 }`} {
		input := []byte(raw)
		got, err := Transform(input)
		if err != nil {
			t.Fatal(err)
		}
		copyOfResult := bytes.Clone(got)
		input[0] = '!'
		if !bytes.Equal(got, copyOfResult) {
			t.Fatal("result aliases input")
		}
	}
}

func FuzzCanonicalScanner(f *testing.F) {
	for _, seed := range []string{`{"a":[1,true,null]}`, `{"a":1,"a":2}`, `"\ud800"`, `[1,]`, `9007199254740991`, `"\u001f"`} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(t *testing.T, raw []byte) {
		if !utf8.Valid(raw) || !isCanonical(raw) {
			return
		}
		want, err := jcs.Transform(raw)
		if err != nil || !bytes.Equal(raw, want) {
			t.Fatalf("unsafe scanner acceptance %q: %q, %v", raw, want, err)
		}
	})
}

func BenchmarkCanonicalStoredProjection(b *testing.B) {
	raw := []byte(`{"active":{"turn":{"attempt":1,"context":{"entries":[{"kind":"assistant","text":"hello"},{"kind":"tool","text":"result"}]},"state":"waiting"}},"completed":[],"revision":123}`)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := Transform(raw); err != nil {
			b.Fatal(err)
		}
	}
}
