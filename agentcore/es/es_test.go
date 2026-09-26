package es

import (
	"strings"
	"testing"
)

// TestEncodeTypedPayload pins the domain separator every Twilight digest is
// built on (SES-WIR-2, RUN-WIR-4): the exact byte shape, and the fact that the
// schema version, the type name and the canonical payload each change the
// preimage. A change here moves every digest in every protocol version, so it
// is a wire change and not a refactor.
func TestEncodeTypedPayload(t *testing.T) {
	raw, err := EncodeTypedPayload(1, "twilight/x/a", map[string]any{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	// v<schemaVersion>:<len(type)>:<type>:<canonical payload>
	if want := `v1:12:twilight/x/a:{"a":1}`; string(raw) != want {
		t.Fatalf("domain separator shape:\n got: %s\nwant: %s", raw, want)
	}

	again, err := EncodeTypedPayload(1, "twilight/x/a", map[string]any{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(raw) {
		t.Fatal("the same input must produce the same preimage")
	}

	// Schema version, type name and payload are three independent inputs.
	other, err := EncodeTypedPayload(2, "twilight/x/a", map[string]any{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	if string(other) == string(raw) {
		t.Error("schema version must separate the preimage: a new ProtocolVersion cannot reuse v1 digests")
	}
	other, err = EncodeTypedPayload(1, "twilight/x/b", map[string]any{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	if string(other) == string(raw) {
		t.Error("type must separate the preimage: two event types cannot share a digest domain")
	}
	other, err = EncodeTypedPayload(1, "twilight/x/a", map[string]any{"a": 2})
	if err != nil {
		t.Fatal(err)
	}
	if string(other) == string(raw) {
		t.Error("payload must separate the preimage")
	}

	// A type name is length-prefixed, so a type that merely extends another
	// cannot alias its domain.
	short, err := EncodeTypedPayload(1, "a", map[string]any{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	long, err := EncodeTypedPayload(1, "ab", map[string]any{"a": 1})
	if err != nil {
		t.Fatal(err)
	}
	if string(short) == string(long) {
		t.Error("length prefix does not separate a type from its extension")
	}
	if !strings.HasPrefix(string(short), "v1:1:a:") || !strings.HasPrefix(string(long), "v1:2:ab:") {
		t.Errorf("length prefix encoding: got %s and %s", short, long)
	}
}

// TestDigestBytes pins the digest value shape: SHA-256 over exactly the bytes
// handed in, hex encoded behind a "sha256:" tag. Canonicalization is the
// caller's obligation (DigestBytes does not re-encode).
func TestDigestBytes(t *testing.T) {
	d := DigestBytes([]byte(`{"a":1}`))
	if !strings.HasPrefix(string(d), "sha256:") {
		t.Fatalf("digest tag: %s", d)
	}
	if got := len(string(d)); got != len("sha256:")+64 {
		t.Fatalf("digest length %d, want %d", got, len("sha256:")+64)
	}
	if again := DigestBytes([]byte(`{"a":1}`)); again != d {
		t.Fatal("DigestBytes is not deterministic")
	}
	if other := DigestBytes([]byte(`{"a":2}`)); other == d {
		t.Fatal("different bytes must not share a digest")
	}
	// Byte-exact: a non-canonical form is a different preimage.
	if padded := DigestBytes([]byte(`{"a": 1}`)); padded == d {
		t.Fatal("DigestBytes must not canonicalize on the caller's behalf")
	}
}

// TestCanonicalDigest covers canonical identity and duplicate-key rejection.
func TestCanonicalDigest(t *testing.T) {
	left, err := DigestCanonical(map[string]any{"b": 2, "a": 1})
	if err != nil {
		t.Fatal(err)
	}
	right, err := DigestCanonical(map[string]any{"a": 1, "b": 2})
	if err != nil {
		t.Fatal(err)
	}
	if left != right {
		t.Fatalf("canonical digests differ: %s != %s", left, right)
	}
	if _, err := Canonicalize([]byte(`{"a":1,"a":2}`)); err == nil {
		t.Fatal("duplicate JSON object key accepted")
	}
	if _, err := Canonicalize([]byte(`{"a":1} trailing`)); err == nil {
		t.Fatal("trailing data accepted")
	}
}

// TestDecodeStrict covers the one-value contract: canonical or not, a value
// decodes; unknown fields, trailing data and duplicate keys are rejected.
func TestDecodeStrict(t *testing.T) {
	type shape struct {
		A int `json:"a"`
	}
	cases := map[string]struct {
		raw  string
		want int
		fail bool
	}{
		"canonical":     {raw: `{"a":1}`, want: 1},
		"non-canonical": {raw: "{ \"a\" : 2 }", want: 2},
		"unknown field": {raw: `{"a":1,"b":2}`, fail: true},
		"trailing data": {raw: `{"a":1}{}`, fail: true},
		"duplicate key": {raw: `{"a":1,"a":2}`, fail: true},
	}
	for name, tc := range cases {
		var got shape
		err := DecodeStrict([]byte(tc.raw), &got)
		if tc.fail {
			if err == nil {
				t.Errorf("%s: accepted", name)
			}
			continue
		}
		if err != nil || got.A != tc.want {
			t.Errorf("%s: got %+v, %v; want a=%d", name, got, err, tc.want)
		}
	}
}
