package sdk

import (
	"errors"
	"testing"
)

// CanonicalJSON is RFC 8785: equal documents are equal bytes and numbers take
// their binary64 form; anything that is not one document is ErrInvalidJSON.
func TestCanonicalJSON(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string // "" means ErrInvalidJSON
	}{
		{"whitespace and member order", " { \"b\" : [ 1 , 2 ] ,\n\"a\" : { \"d\" : null , \"c\" : true } } ", `{"a":{"c":true,"d":null},"b":[1,2]}`},
		{"same document, other spelling", `{"a":{"c":true,"d":null},"b":[1,2]}`, `{"a":{"c":true,"d":null},"b":[1,2]}`},
		{"numbers take their binary64 form", `[2, 2.0, 1e2, 1E2, -0, 0.10]`, `[2,2,100,100,0,0.1]`},
		{"strings are minimally escaped", `{"s":"a<b>&é😀\n"}`, "{\"s\":\"a<b>&é😀\\n\"}"},
		{"members sort by UTF-16 code units", `{"b":1,"B":2,"aa":3,"a":4,"é":5}`, `{"B":2,"a":4,"aa":3,"b":1,"é":5}`},
		{"scalar document", ` "x" `, `"x"`},
		{"empty object", `{ } `, `{}`},
		{"empty array", `[ ]`, `[]`},
		{"repeated member", `{"a":1,"a":1}`, ""},
		{"nested repeated member", `{"o":{"a":1,"b":2,"a":3}}`, ""},
		{"escaped lone surrogate", `{"s":"\ud800"}`, ""},
		{"truncated", `{"a":`, ""},
		{"trailing data", `{} {}`, ""},
		{"empty input", ``, ""},
		{"invalid utf-8", "{\"a\":\"\xff\"}", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := CanonicalJSON([]byte(tc.in))
			if tc.want == "" {
				if !errors.Is(err, ErrInvalidJSON) {
					t.Fatalf("CanonicalJSON(%q) = %s, %v; want ErrInvalidJSON", tc.in, got, err)
				}
				return
			}
			if err != nil || string(got) != tc.want {
				t.Fatalf("CanonicalJSON(%q) = %s, %v; want %s", tc.in, got, err, tc.want)
			}
			again, err := CanonicalJSON(got)
			if err != nil || string(again) != tc.want {
				t.Fatalf("CanonicalJSON is not idempotent: %s -> %s, %v", got, again, err)
			}
		})
	}
}
