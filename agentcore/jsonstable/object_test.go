package jsonstable

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"unicode/utf8"
)

func checkExtractMember(t testing.TB, value Value, name string) {
	t.Helper()
	before := value.String()
	var object map[string]json.RawMessage
	err := json.Unmarshal(value.Bytes(), &object)
	raw, exists := object[name]
	wantOK := err == nil && exists && utf8.ValidString(name)
	member, rest, ok := value.ExtractMember(name)
	if ok != wantOK {
		t.Fatalf("ExtractMember(%s, %q): ok=%v, want %v", before, name, ok, wantOK)
	}
	if !ok {
		if !member.IsZero() || !rest.IsZero() {
			t.Fatal("unsuccessful extraction returned present Values")
		}
		return
	}
	delete(object, name)
	wantRest, err := MarshalCanonical(object)
	if err != nil {
		t.Fatal(err)
	}
	wantMember, err := Canonicalize(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(member.Bytes(), wantMember) || !bytes.Equal(rest.Bytes(), wantRest) {
		t.Fatalf("ExtractMember(%s, %q): member=%s rest=%s, want %s and %s", before, name, member, rest, wantMember, wantRest)
	}
	// Accessors detach bytes; decoding into an alias replaces its storage.
	for _, got := range []Value{value, member, rest} {
		detached := got.Bytes()
		detached[0] = '!'
		encoded, err := got.MarshalJSON()
		if err != nil {
			t.Fatal(err)
		}
		encoded[0] = '!'
		alias := got
		if err := alias.UnmarshalJSON([]byte(`{"replacement":true}`)); err != nil {
			t.Fatal(err)
		}
	}
	if value.String() != before || !bytes.Equal(member.Bytes(), wantMember) || !bytes.Equal(rest.Bytes(), wantRest) {
		t.Fatal("extraction Values were mutated through aliases")
	}
}

func TestExtractMemberReference(t *testing.T) {
	cases := []struct{ raw, name string }{
		{`{"a":1,"b":2,"c":3}`, "a"},
		{`{"a":1,"b":2,"c":3}`, "b"},
		{`{"a":1,"b":2,"c":3}`, "c"},
		{`{"v":null}`, "v"},
		{`{"": [null,true,false,1.00,1e30],"z":{}}`, ""},
		{`{"a":{"v":2},"v":[{"nested":[{},[],"}],[{,:\\\""]}],"z":"tail"}`, "v"},
		{`{"v":1,"x":"[{}],:\"\\\n\u0000"}`, "v"},
		{`{"\u0076":2,"x":-0}`, "v"},
		{`{"\"\\\n\u0000": {"a":[0]},"v":2}`, "\"\\\n\x00"},
		{`{"😀":true,"\ue000":false,"é":null,"v":2}`, "😀"},
		{`{"😀":true,"\ue000":false,"é":null,"v":2}`, "\ue000"},
		{`{"<>&/\u2028\u2029":1,"v":2}`, "<>&/\u2028\u2029"},
		{`{"a":1}`, "missing"}, {`{}`, "v"}, {`[]`, "v"},
		{`null`, "v"}, {`"v"`, "v"}, {`true`, "v"}, {`1`, "v"},
		{`{"v":1}`, string([]byte{0xff})},
		{`{"` + strings.Repeat("long😀", 64) + `":123,"v":2}`, strings.Repeat("long😀", 64)},
		{`{"a":` + strings.Repeat("[", 256) + `0` + strings.Repeat("]", 256) + `,"v":65535}`, "v"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { checkExtractMember(t, MustParse(c.raw), c.name) })
	}
	checkExtractMember(t, Value{}, "v")
}

func FuzzExtractMemberReference(f *testing.F) {
	for _, raw := range []string{
		`{"a":1,"v":2,"z":[{"v":"\\\""},null]}`,
		`{"":true,"\"\\\n":{},"😀":false,"v":1}`, `null`, `[]`, `{}`,
	} {
		for _, key := range []string{"v", "", "a", "😀", "\"\\\n"} {
			f.Add(raw, key)
		}
	}
	f.Fuzz(func(t *testing.T, raw, name string) {
		value, err := Parse([]byte(raw))
		if err != nil {
			return
		}
		checkExtractMember(t, value, name)
		// Exercise extraction of every existing key, even when the fuzz key misses.
		var object map[string]json.RawMessage
		if json.Unmarshal(value.Bytes(), &object) == nil {
			for key := range object {
				checkExtractMember(t, value, key)
			}
		}
	})
}
