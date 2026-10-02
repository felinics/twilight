package jsonstable

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestCanonicalizeJCS pins RFC 8785 (JCS) output for the shapes the protocol
// depends on. These bytes are digest input, so a change here moves every
// digest in every layer.
func TestCanonicalizeJCS(t *testing.T) {
	cases := map[string]string{
		// Object keys sort by UTF-16 code unit and insignificant whitespace
		// disappears.
		`{"b":2,"a":1}`:                          `{"a":1,"b":2}`,
		`{ "a" : 1 }`:                            `{"a":1}`,
		`{"nested":{"z":1,"a":[true,null,"x"]}}`: `{"nested":{"a":[true,null,"x"],"z":1}}`,
		`[]`:                                     `[]`,
		`{}`:                                     `{}`,
		`[[1,2],[3]]`:                            `[[1,2],[3]]`,
		// Strings: non-ASCII stays raw UTF-8, escapes that JSON requires stay
		// escaped.
		`"\u00e9"`:       `"é"`,
		`"a\"b"`:         `"a\"b"`,
		`"a\nb"`:         `"a\nb"`,
		`"\u0001"`:       `"\u0001"`,
		`"\ud83d\ude00"`: `"😀"`, // a valid surrogate pair is one code point
	}
	for in, want := range cases {
		got, err := Canonicalize([]byte(in))
		if err != nil {
			t.Errorf("Canonicalize(%s) error: %v", in, err)
			continue
		}
		if string(got) != want {
			t.Errorf("Canonicalize(%s) = %s, want %s", in, got, want)
		}
	}
}

// TestNumberSemantics pins ECMAScript/IEEE-754 binary64 number formatting and
// the precision limit the package doc warns about: an exact identifier or a
// quantity beyond binary64 must be a JSON string, because a JSON number is
// silently rounded before it is ever digested.
func TestNumberSemantics(t *testing.T) {
	cases := map[string]string{
		`1e2`:  `100`,
		`1E+2`: `100`,
		`1.0`:  `1`,
		`1.5`:  `1.5`,
		`0.1`:  `0.1`,
		`-0`:   `0`,
		`1e21`: `1e+21`,
		`1e-7`: `1e-7`,
		// 30 significant digits collapse to the nearest binary64 value.
		`123456789012345678901234567890`: `1.2345678901234568e+29`,
	}
	for in, want := range cases {
		got, err := Canonicalize([]byte(in))
		if err != nil {
			t.Errorf("Canonicalize(%s) error: %v", in, err)
			continue
		}
		if string(got) != want {
			t.Errorf("Canonicalize(%s) = %s, want %s", in, got, want)
		}
	}
	// Beyond binary64 is an error rather than an infinity in the digest input.
	if _, err := Canonicalize([]byte(`1e309`)); err == nil {
		t.Fatal("a number outside binary64 was accepted")
	}
}

// TestCanonicalizeRejects pins the inputs that must never reach a digest.
func TestCanonicalizeRejects(t *testing.T) {
	cases := map[string]string{
		"duplicate key":       `{"a":1,"a":2}`,
		"trailing data":       `{"a":1} trailing`,
		"empty input":         ``,
		"malformed array":     `[1,]`,
		"invalid utf-8":       "\"\xff\"",
		"lone high surrogate": `"\ud800"`,
		"lone low surrogate":  `"\udc00"`,
	}
	for name, in := range cases {
		if _, err := Canonicalize([]byte(in)); err == nil {
			t.Errorf("%s (%q) was accepted", name, in)
		}
	}
	// The surrogate check is the SDK's, not the JCS parser's: it stops two
	// distinct invalid wires from merging into one digest input.
	if _, err := Canonicalize([]byte(`"\ud800"`)); err == nil || !strings.Contains(err.Error(), "lone surrogate") {
		t.Errorf("lone surrogate error = %v", err)
	}
}

// TestCanonicalizeIdempotent pins that canonical output is a fixed point, which
// is what lets a stored canonical row be re-digested without drift.
func TestCanonicalizeIdempotent(t *testing.T) {
	for _, in := range []string{`{ "b" : [1, 2.0, {"y":null,"x":"é"}] }`, `1.0`, `"\u0001"`} {
		once, err := Canonicalize([]byte(in))
		if err != nil {
			t.Fatal(err)
		}
		twice, err := Canonicalize(once)
		if err != nil {
			t.Fatal(err)
		}
		if string(once) != string(twice) {
			t.Errorf("Canonicalize is not a fixed point for %s: %s != %s", in, once, twice)
		}
	}
}

// TestValueCanonicalByConstruction pins the invariant the kernel relies on:
// every Value holds canonical bytes, so SES-WIR-1's "Payload must be canonical
// JSON" is a property of the type rather than a check each writer must
// remember. Every construction path therefore canonicalizes.
func TestValueCanonicalByConstruction(t *testing.T) {
	const nonCanonical = `{ "b" : 2, "a" : 1 }`
	const canonical = `{"a":1,"b":2}`

	parsed, err := Parse([]byte(nonCanonical))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.String() != canonical {
		t.Fatalf("Parse kept the input form: %s", parsed)
	}
	if MustParse(nonCanonical).String() != canonical {
		t.Fatal("MustParse kept the input form")
	}
	fromValue, err := FromValue(map[string]int{"b": 2, "a": 1})
	if err != nil {
		t.Fatal(err)
	}
	if fromValue.String() != canonical {
		t.Fatalf("FromValue = %s", fromValue)
	}
	var unmarshalled Value
	if err := json.Unmarshal([]byte(nonCanonical), &unmarshalled); err != nil {
		t.Fatal(err)
	}
	if unmarshalled.String() != canonical {
		t.Fatalf("UnmarshalJSON kept the input form: %s", unmarshalled)
	}
	// Parse is idempotent on its own output and agrees with Canonicalize.
	again, err := Parse([]byte(parsed.String()))
	if err != nil {
		t.Fatal(err)
	}
	if !parsed.Equal(again) {
		t.Fatal("Parse is not idempotent")
	}
	want, err := Canonicalize([]byte(nonCanonical))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.String() != string(want) {
		t.Fatalf("Parse = %s, Canonicalize = %s", parsed, want)
	}
}

// TestValueAccessors covers the read and marshal surface, including that Bytes
// hands back a detached copy so a caller cannot mutate stored canonical bytes.
func TestValueAccessors(t *testing.T) {
	v := MustParse(`{"a":1}`)

	b := v.Bytes()
	b[0] = 'X'
	if v.String() != `{"a":1}` {
		t.Fatalf("Bytes is not detached: %s", v)
	}

	var zero Value
	if !zero.IsZero() || zero.String() != "null" {
		t.Fatalf("zero Value = %q isZero=%v", zero.String(), zero.IsZero())
	}
	if v.IsZero() {
		t.Fatal("a parsed Value reported zero")
	}
	if parsed, err := Parse(nil); err != nil || !parsed.IsZero() {
		t.Fatalf("Parse(nil) = %v %v", parsed, err)
	}

	if !v.Equal(MustParse(`{ "a" : 1 }`)) || v.Equal(MustParse(`{"a":2}`)) {
		t.Fatal("Equal ignores canonical form")
	}

	var decoded struct {
		A int `json:"a"`
	}
	if err := v.Decode(&decoded); err != nil || decoded.A != 1 {
		t.Fatalf("Decode = %+v %v", decoded, err)
	}
	anyValue, err := v.Any()
	if err != nil {
		t.Fatal(err)
	}
	// Numbers decode as json.Number, not float64: a caller that reads a
	// canonical value back must not reintroduce binary64 rounding.
	m, ok := anyValue.(map[string]any)
	if !ok {
		t.Fatalf("Any = %#v, want a map", anyValue)
	}
	if num, ok := m["a"].(json.Number); !ok || num.String() != "1" {
		t.Fatalf("Any number = %#v (%T), want json.Number \"1\"", m["a"], m["a"])
	}

	marshalled, err := json.Marshal(v)
	if err != nil || string(marshalled) != `{"a":1}` {
		t.Fatalf("MarshalJSON = %s %v", marshalled, err)
	}
	if string(v.RawMessage()) != `{"a":1}` {
		t.Fatalf("RawMessage = %s", v.RawMessage())
	}
}

// TestMarshalCanonical pins the single path from protocol values to digest
// input: a Go value is marshalled and then canonicalized.
func TestMarshalCanonical(t *testing.T) {
	type structWithUnorderedFields struct {
		B int `json:"b"`
		A int `json:"a"`
	}
	got, err := MarshalCanonical(structWithUnorderedFields{B: 2, A: 1})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != `{"a":1,"b":2}` {
		t.Fatalf("MarshalCanonical = %s, want sorted keys", got)
	}
	if _, err := MarshalCanonical(func() {}); err == nil {
		t.Fatal("an unmarshallable value was accepted")
	}
	if _, err := FromValue(func() {}); err == nil {
		t.Fatal("FromValue accepted an unmarshallable value")
	}
	// A Value passes through FromValue unchanged, so a canonical value is not
	// re-encoded on its way to a digest.
	v := MustParse(`{"a":1}`)
	roundTripped, err := FromValue(v)
	if err != nil || !roundTripped.Equal(v) {
		t.Fatalf("FromValue(Value) = %s %v", roundTripped, err)
	}
}

// TestParseRejectsInvalid keeps the construction paths failing loudly rather
// than storing a differently-shaped value.
func TestParseRejectsInvalid(t *testing.T) {
	for _, in := range []string{`{"a":1,"a":2}`, `[1,]`, `nope`} {
		if _, err := Parse([]byte(in)); err == nil {
			t.Errorf("Parse(%s) was accepted", in)
		}
	}
	var v Value
	if err := json.Unmarshal([]byte(`{ "a" : 1, "a" : 2 }`), &v); err == nil {
		t.Fatal("UnmarshalJSON accepted a duplicate key")
	}
	// Decode reports a shape mismatch instead of storing a partial value.
	var n int
	if err := MustParse(`{"a":1}`).Decode(&n); err == nil {
		t.Fatal("Decode accepted an object into an int")
	}
}
